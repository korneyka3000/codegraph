"""M4 T4: scan-diff engine -- the pure-function foundation `codegraph index
--incremental` (T5/T7) is built on. Two independent, unrelated questions live here:

`service_delta` answers "which of THIS service's files actually changed on disk since
the last staged run" -- a per-relpath comparison of the previous run's staged
snapshot (`Staging.files_for_service`'s `(relpath, sha256)` rows, already exactly the
shape needed here, turned into a dict by the caller -- see below) against a fresh
`scan_service` result. It is deliberately a pure function over two already-
materialized snapshots, never touching `Staging`/sqlite itself: T5's incremental
`analyze_service` wrapper is the one place that knows how to fetch both sides
(`dict(staging.files_for_service(svc.name))` for `staged`, `scan_service(svc.path,
svc.exclude)`'s rows for `scanned`) and feed them in. Kept decoupled this way,
`service_delta` is trivially unit-testable with plain dicts/lists -- no tmp_path
fixtures, no sqlite file, no filesystem at all.

No `files_snapshot` wrapper is added to `Staging` for this: `files_for_service`
already returns exactly `(relpath, sha256)` pairs, sorted by relpath (see its own
one-line body in stores/staging.py) -- precisely the pairs a `dict(...)` call turns
into this module's `staged` shape. Adding a second method that does nothing but
`dict(self.files_for_service(service))` would be pure duplication for a one-line
transform; the brief's own condition for adding it ("if `files_for_service` doesn't
already serve") isn't met, so T4 leaves `Staging` untouched.

`config_fingerprint` answers a different question entirely: not "did files change"
but "did this SERVICE'S OWN CONFIGURATION change in a way that makes any prior
scan/analyze result untrustworthy, independent of what the files say". An idiom edit
(a producer/consumer/http_client pattern added, changed, or removed), an exclude-list
edit (a previously-scanned file starts or stops being ignored), or a schema_version
bump (the staged on-disk layout itself changed shape) must all force a full
re-analyze even when `service_delta` would otherwise report zero file changes -- e.g.
a freshly-added kafka idiom needs every file re-extracted by the new producer/
consumer pattern even though not one file's bytes moved. `svc.path` is deliberately
EXCLUDED from the fingerprint payload: relocating a service's checkout to a new
filesystem path (a workspace move, a fresh clone elsewhere) is not a config change
and must not force a full re-analyze on its own.

Both `exclude` (a plain list, insertion order is meaningful in YAML but not
semantically) and `active_idioms` (a frozenset, inherently unordered and, for str
members, order-dependent on the interpreter's per-process hash seed) are sorted
before going into the payload -- without that, reordering a service's exclude list in
codegraph.yaml, or simply re-running in a fresh interpreter process with a different
PYTHONHASHSEED, would flip the fingerprint despite the configuration being
semantically identical, defeating the whole point of a STABLE cache key.
`ServiceIdioms.model_dump(mode="json")` (pydantic v2) already yields plain,
JSON-safe nested dicts/lists for the idiom DSL's own nested models (ValueSpec,
ChannelSpec, ...) -- no separate canonicalization needed there beyond the outer
`json.dumps(..., sort_keys=True)`, which sorts every dict's keys (including nested
ones) but never reorders list elements, which is exactly why `exclude`/`active` need
their own explicit `sorted()` first.

Neither function writes anything: `config_fingerprint`'s result is persisted by the
CALLER via `staging.set_meta(f"svc_fingerprint:{name}", fp)` -- T5 (read it back to
decide skip/incremental eligibility) and T7 (CLI wiring) own that; T4 only provides
the pure computation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from codegraph.config.models import ServiceConfig, ServiceIdioms
from codegraph.core.schema import SCHEMA_VERSION


@dataclass(frozen=True)
class ServiceDelta:
    """Result of `service_delta`: one bucket per relpath's fate between the staged
    snapshot and the fresh scan, each a sorted tuple (mirrors `scan_service`'s own
    relpath-sorted-output convention -- the single source of determinism downstream
    stages rely on, see pipeline/scan.py's module docstring)."""

    added: tuple[str, ...]
    changed: tuple[str, ...]
    deleted: tuple[str, ...]
    unchanged: tuple[str, ...]

    @property
    def empty(self) -> bool:
        """True when there is nothing an incremental run would need to act on --
        `unchanged` deliberately does NOT participate: a service whose every file is
        unchanged is exactly the "nothing to do" case this property exists to
        detect."""
        return not (self.added or self.changed or self.deleted)


def service_delta(
    staged: dict[str, str], scanned: list[tuple[str, str, int]]
) -> ServiceDelta:
    """`staged` -- previous run's relpath -> sha256 snapshot (e.g.
    `dict(staging.files_for_service(svc.name))`). `scanned` -- this run's fresh
    `scan_service` rows, `(relpath, sha256, size)` (size is carried in the parameter
    type for symmetry with `scan_service`'s own row shape but unused here -- the
    delta is computed on relpath + sha256 alone)."""
    scanned_sha_by_path = {relpath: sha for relpath, sha, _ in scanned}

    added = []
    changed = []
    unchanged = []
    for relpath, sha in scanned_sha_by_path.items():
        prior_sha = staged.get(relpath)
        if prior_sha is None:
            added.append(relpath)
        elif prior_sha != sha:
            changed.append(relpath)
        else:
            unchanged.append(relpath)
    deleted = [relpath for relpath in staged if relpath not in scanned_sha_by_path]

    return ServiceDelta(
        added=tuple(sorted(added)),
        changed=tuple(sorted(changed)),
        deleted=tuple(sorted(deleted)),
        unchanged=tuple(sorted(unchanged)),
    )


def config_fingerprint(
    svc: ServiceConfig, idioms: ServiceIdioms, active_idioms: frozenset[str]
) -> str:
    """sha256 hex digest of the canonical JSON payload `{"exclude": sorted(svc.
    exclude), "idioms": idioms.model_dump(mode="json"), "active":
    sorted(active_idioms), "schema": SCHEMA_VERSION}` (`json.dumps(...,
    sort_keys=True)`). `idioms` is the caller's EFFECTIVE per-service idioms (e.g.
    `config.loader.effective_idioms(cfg, svc)` -- service idioms merged with active
    builtins), not `svc.idioms` alone -- callers decide which idiom view makes a
    result untrustworthy; this function just hashes whatever it's handed. See this
    module's own docstring for why `svc.path` is excluded and why `exclude`/
    `active_idioms` are sorted first."""
    payload = {
        "exclude": sorted(svc.exclude),
        "idioms": idioms.model_dump(mode="json"),
        "active": sorted(active_idioms),
        "schema": SCHEMA_VERSION,
    }
    canonical = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()
