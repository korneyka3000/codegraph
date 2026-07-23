"""M7 T3 (OPEN R1): env->service map, harvested from `WorkspaceConfig.env_sources`
(helm-values-shaped YAML files) -- an ADDITIVE fallback for linking/http_routes.py's
anchoring tier 1, consulted only when the pre-existing `ServiceConfig.http.
base_url_env` registry (the PRIMARY source) finds no owner for a claim's
`base_url_env`. See that module's own docstring for the full tiering contract.

Root cause this closes (docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md
R1): a client's target service is often only knowable through a deterministic, but
until now unmodeled, chain -- `self.host` <- a Settings field <- an env var (SCREAMING_
SNAKE, e.g. `SERVICE_VERIFICATION_REQUESTS_URL`) <- a helm values file mapping that SAME
env var name to a concrete cluster hostname (`verification-requests.kyc.svc.cluster.
local`) whose FIRST DNS label is the service's own name in the workspace. This module
is the last link: env-var-name -> DNS-label -> workspace service name.

Harvest shape (per this task's own brief -- "код-first, helm-optional"): a plain,
NESTED yaml mapping, walked recursively (helm conventions vary -- envs may sit flat at
the top level, or nested under an `env:` block, or deeper) -- every (key, value) pair
where `value` is itself a STRING is a harvest candidate. The KEY is used VERBATIM as
the env-var name (helm env blocks already spell keys in SCREAMING_SNAKE -- no case
transform is applied, unlike the env_prefix-derivation `class_attrs.py` does for
pydantic Settings fields, a DIFFERENT, unrelated derivation). The VALUE is checked for
a URL shape (`urlparse` finds a `hostname`) -- a non-URL string (plain text, a bare
number/bool that YAML already parsed as non-str, ...) is silently NOT a service-mapping
candidate (harvested for nothing else here, this module has no other use for it).

Matching: `hostname.split(".")[0]` (the FIRST DNS label, e.g. "verification-requests"
from "verification-requests.kyc.svc.cluster.local") is looked up by EXACT string
equality against the caller-given `service_names` set -- NO fuzzy matching (a near-miss
is honestly absent from the resulting map, never guessed at) -- `.hostname` (unlike
`.netloc`) is already lowercased and port-stripped by `urlparse` itself, so no manual
cleanup is needed for either. One `urlparse` quirk worth naming (M7 T3 review Minor-4):
a SCHEME-LESS `host:port` value (e.g. `verification-requests.kyc:8000`, no `http://`)
parses with `hostname=None` (the text before ":" reads as a URL *scheme*, not a host),
so such values are silently absent from the map -- a safe degradation (the claim just
stays unanchored/unmapped, never mis-anchored).

Defensive at read time (unlike `config/loader.py`'s own load-time validation of
`env_sources` path existence, which is the PRIMARY, loud-failure guard for a real
`codegraph index` run): a missing file here is silently skipped, and a file with
MALFORMED YAML (M7 T3 review Important-1 -- the realistic shape is an UNRENDERED helm
template: `{{ .Values.host }}` is invalid YAML) is skipped with a `logger.warning`
naming the file and the parse error, not a crash -- `build_env_service_map` runs
INSIDE S7's `link()`, i.e. AFTER every service's expensive analyze work has already
completed, so an uncaught parse error here would lose the whole index run at its very
last stage over one bad optional-input file. This also keeps the function safely
callable from unit tests (and any future in-memory caller) that construct a
`WorkspaceConfig` directly, bypassing `config/loader.py`'s own validation entirely.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)


def _walk_strings(node: object) -> list[tuple[str, str]]:
    """Recursively walks a nested dict/list structure, yielding (key, value) for
    every dict entry whose OWN value is a plain string -- the key comes from the
    IMMEDIATELY enclosing dict (a list element itself contributes no key of its
    own, only further (key, value) pairs found by continuing to walk it -- helm
    values don't put env-var mappings inside a list, but this stays defensively
    correct if one ever does, e.g. a list of per-environment override dicts)."""
    out: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str):
                out.append((key, value))
            else:
                out.extend(_walk_strings(value))
    elif isinstance(node, list):
        for item in node:
            out.extend(_walk_strings(item))
    return out


def _hostname_label(value: str) -> str | None:
    """First DNS label of a URL-shaped string's hostname, or None when `value`
    doesn't parse as a URL with a network location at all (a non-URL string is
    honestly ignored for service-mapping, never guessed at -- see module
    docstring). `urlparse(...).hostname` is already lowercased and port-stripped
    by the stdlib itself, so no extra cleanup is needed here for either."""
    hostname = urlparse(value).hostname
    return hostname.split(".")[0] if hostname else None


def build_env_service_map(env_sources: list[Path], service_names: set[str]) -> dict[str, str]:
    """env-var-name -> workspace service name, for every (key, URL-value) pair found
    across all `env_sources` files whose hostname's first DNS label EXACTLY matches a
    name in `service_names`. Files are read in list order; a key harvested from a
    LATER file overwrites an earlier one on collision (plain dict-assignment
    last-wins, same "later claim wins" precedent `class_attrs.build_class_attr_index`
    already established for its own claim-merge). A missing file is silently skipped;
    a malformed-YAML file is skipped with a warning (see module docstring's own
    "defensive at read time" note for both)."""
    result: dict[str, str] = {}
    for path in env_sources:
        if not path.is_file():
            continue
        try:
            data = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as e:
            # M7 T3 review Important-1: warn-and-skip (stores/falkordb/batch.py's own
            # "skipping bad row" logger.warning precedent) -- see module docstring.
            logger.warning("env_sources: skipping malformed YAML %s: %s", path, e)
            continue
        for key, value in _walk_strings(data):
            label = _hostname_label(value)
            if label is not None and label in service_names:
                result[key] = label
    return result
