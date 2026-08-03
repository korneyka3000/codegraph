"""S6 join: SCIP refs × tree-sitter call-sites → CALLS edges.

Per call-site (`CallFact.callee_start_byte`), find the SCIP ref occupying that exact
byte position (exact dict hit) or, failing that, the ref whose [start_byte, end_byte)
span contains it (sorted-list bisect fallback — covers small span-conversion drift).

First-party vs. external (M5 Task 1 -- pilot Bug B, docs/superpowers/reports/
2026-07-18-m4-pilot.md §7.2): a non-local ref symbol is first-party iff it has a
STAGED DEF somewhere in this service's own `scip_defs` (`def_symbols`, hoisted once
per service by the caller -- see `analyze.py`) -- NOT, as before, iff
`parsed.package == service`. The package-tag criterion is REMOVED, not supplemented:
a dual criterion would still wrongly join a third-party symbol whose package happens
to collide with a first-party one. Root cause: `resolvers/scip/runner.py::ScipRunner.
run` invokes `scip-python index . --project-name <service> ...`, and `--project-name`
makes scip-python stamp `package=<service>` on EVERY symbol it fully resolves --
first-party AND third-party alike, as long as the callee is resolvable at all (which,
for a service with a real installed venv, includes sqlalchemy/pydantic/fastapi/etc).
A bare package-tag comparison therefore cannot distinguish "defined in this service"
from "some library this service's venv happens to have installed" once a real venv is
in play -- measured on a real repo during the M4 pilot: of 5345 staged CALLS edges,
only 2916 (54.6%) had a valid dst at load time (`load.py`'s S9); the other 2429
(45.4%) were third-party calls masquerading as first-party "joined" edges (94% of
those pointing at obviously third-party prefixes -- sqlalchemy/pydantic/fastapi/
blockkit/slack_sdk/etc) whose dst node is never staged (defs only ever exist for a
service's own scanned files), so they were silently dropped at load, with no signal
in `calls_joined`/`pct_unresolved_calls` that anything was wrong. Def-existence is
exact and package-name-independent: a def only ever gets staged for a symbol this
service's own S3/S4 (scip run + reader) actually found a DEFINITION occurrence for,
regardless of what package string scip-python happened to attach to it.

A local symbol (`local N`) is always first-party -- unless `local_defs_for_file` is
given and the symbol has no def in that same file, in which case it is unresolved
(pyright degrades unresolved 3rd-party refs to 'local N', see m1a-task-10-report §2);
`local_defs_for_file` is a SEPARATE lookup from `def_symbols` (local defs are
per-file-scoped `scip_defs` rows, `local_def_symbols`, not the service-wide
`def_symbols` set) and this branch is untouched by this task. Calls that match no ref
at all (fully dynamic) are likewise counted unresolved. Matched first-party calls are
aggregated per (src, dst) into one CALLS edge with a callsite_count and evidence from
the first call site encountered.

M10 T1 (pilot §5, docs/superpowers/reports/2026-08-03-mcp-pilot.md): module-level
singleton method-call resolution -- `registry = _DBRegistry(config.database.dsn)` at
module level, `registry.session()` call-sites elsewhere, previously resolved (when
resolved at all) to a node-less `module.attr` symbol and got silently dropped at S9
load (79/149 [53%] of the pilot's dropped CALLS, all this ONE pattern -- scip cannot
connect an attribute access on a module-level INSTANCE back to its class's own
method). The new OPTIONAL `singleton_index` parameter (default None, every
pre-existing caller unaffected) is consulted as a FALLBACK, tried ONLY when the
normal ref-based resolution above does not already point at a real, callable-shaped,
staged node -- an unresolved call (no ref at all), a local ref with no matching local
def, a non-local ref with no staged def (external), or -- the exact shape the pilot
diagnosed -- a non-local ref that DOES have a staged def but whose descriptors are
NOT function/method-shaped (`ids.structural_descriptor`'s own construction: every
Class/Function node's descriptors end in "#" or "().", so anything else, e.g. a bare
`` `mod`/name. `` module-attribute term, can never correspond to a real callable node
python_core.py ever stages). `_try_singleton` (below) resolves a call's OWN
`receiver_text`/`callee_name` (facts.py's M8 T1 fields) against the service-wide
`SingletonIndex` (parsing/module_singletons.py) and VERIFIES the candidate against
`def_symbols` before ever using it -- never overriding an already-good resolution,
never guessing when the candidate can't be verified (the ORIGINAL, pre-M10 path is
then taken completely unchanged, including staying "dangling" when that was already
going to happen). A successful redirect's resolution/confidence come from the
SingletonIndex entry's OWN tier (static/1.0 or heuristic/0.6 -- see that module's
docstring), not from this function's `resolution`/`confidence` parameters, and its
edge carries `props["mechanism"] = "singleton_dispatch"` (existing edges' props stay
exactly `{"callsite_count": N}`, unchanged)."""

from __future__ import annotations

import bisect
from collections.abc import Callable
from dataclasses import dataclass

from codegraph.core import ids
from codegraph.core.schema import EdgeRec
from codegraph.extractors.python_core import nesting_chain
from codegraph.parsing.facts import CallFact, FileFacts
from codegraph.parsing.module_singletons import (
    SingletonDispatch,
    SingletonIndex,
    resolve_singleton_call,
)
from codegraph.resolvers.base import RefRow
from codegraph.resolvers.scip.symbols import ParsedSymbol, parse_symbol, symbol_to_node_id
from codegraph.stores.staging import Staging

_EXTRACTOR = "calls"


@dataclass(frozen=True)
class JoinStats:
    calls_joined: int
    calls_unresolved: int
    calls_external: int


def _caller_id(service, relpath, facts, enclosing, lookup):
    if enclosing is None:
        return ids.node_id(service, ids.module_descriptor(ids.relpath_to_module(relpath)))
    d = facts.defs[enclosing]
    sym = lookup(relpath, d.name_start_byte)
    if sym is not None:
        return symbol_to_node_id(service, relpath, sym)
    return ids.node_id(
        service,
        ids.structural_descriptor(ids.relpath_to_module(relpath), nesting_chain(facts.defs, d)),
    )


def _find_ref(
    callee_start: int,
    by_start: dict[int, RefRow],
    sorted_refs: list[RefRow],
    starts: list[int],
) -> RefRow | None:
    exact = by_start.get(callee_start)
    if exact is not None:
        return exact
    i = bisect.bisect_right(starts, callee_start) - 1
    if i < 0:
        return None
    cand = sorted_refs[i]
    if cand.start_byte <= callee_start < cand.end_byte:
        return cand
    return None


def _is_callable_descriptor(parsed: ParsedSymbol) -> bool:
    """Every node python_core.py ever stages for a Class/Function has descriptors
    ending in "#" (class) or "()." (function/method) -- `ids.structural_descriptor`'s
    own construction, mirrored by every def-id path (`_def_id`, resolvers/fallback.py's
    `_symbol`). A resolved+staged symbol whose descriptors do NOT end in "()." can
    therefore never correspond to a real function/method node -- exactly the
    "module.attr symbol without a node" shape the M10 pilot diagnosed (a bare
    module-level term, `` `mod`/name. ``, one level deep, no "#"/"()." at all)."""
    return parsed.descriptors is not None and parsed.descriptors.endswith("().")


def _try_singleton(
    call: CallFact,
    singleton_index: SingletonIndex | None,
    def_symbols_set: set[str],
    service: str,
) -> SingletonDispatch | None:
    """M10 T1: try a module-level-singleton redirect for `call` -- None whenever
    there is nothing to try (no index wired) or the receiver isn't a bare (dot-free)
    name (mirrors `idiom_match._match_receiver`'s own "receiver -- ПРОСТОЕ имя [без
    точек]" convention: `self.registry.session()`/`mod.registry.session()` are never
    attempted, only this task's own claimed shape -- a bare module-level binding)."""
    if singleton_index is None:
        return None
    receiver = call.receiver_text
    if receiver is None or "." in receiver:
        return None
    return resolve_singleton_call(
        singleton_index, receiver, call.callee_name, def_symbols_set, service,
    )


def build_calls(
    service: str,
    staging: Staging,
    facts_by_file: dict[str, FileFacts],
    def_symbol_lookup: Callable[[str, int], str | None],
    def_symbols: set[str] | Callable[[], set[str]],
    local_defs_for_file: Callable[[str], set[str]] | None = None,
    resolution: str = "static",
    confidence: float = 1.0,
    singleton_index: SingletonIndex | None = None,
) -> JoinStats:
    calls_joined = 0
    calls_unresolved = 0
    calls_external = 0
    # Resolved ONCE regardless of whether the caller passed an already-materialized
    # set (analyze.py's own hoisted-per-service query, see its module docstring) or a
    # zero-arg callable (tests' preferred style, mirroring local_defs_for_file's own
    # lambda pattern) -- never re-queried per file or per call-site either way.
    def_symbols_set = def_symbols() if callable(def_symbols) else def_symbols
    # (src, dst) -> {"count", "file", "line", "resolution", "confidence", "mechanism"}
    # — evidence AND classification are the FIRST call site encountered for that pair
    # (dict only set once, per key). "resolution"/"confidence"/"mechanism" (M10 T1)
    # default to this call's own (function-parameter) resolution/confidence and
    # mechanism=None; a successful singleton redirect overrides all three with the
    # SingletonDispatch's own values -- see build_calls' own module-docstring addendum.
    agg: dict[tuple[str, str], dict] = {}

    for relpath, facts in facts_by_file.items():
        refs = staging.refs_for_file(service, relpath)  # already sorted by start_byte
        by_start = {r.start_byte: r for r in refs}
        starts = [r.start_byte for r in refs]

        for call in facts.calls:
            ref = _find_ref(call.callee_start_byte, by_start, refs, starts)
            dispatch: SingletonDispatch | None = None

            if ref is None:
                dispatch = _try_singleton(call, singleton_index, def_symbols_set, service)
                if dispatch is None:
                    calls_unresolved += 1
                    continue
            else:
                parsed = parse_symbol(ref.symbol)
                if parsed.is_local:
                    # ВНИМАНИЕ (M1b): pyright деградирует нерезолвленные 3rd-party в 'local N' —
                    # такие "first-party" локалы без def-occurrence в том же документе на деле
                    # unresolved (см. m1a-task-10-report §2). Unaffected by M5 Task 1 below --
                    # local-ness is decided by SCIP's own symbol shape, not by def_symbols.
                    if (local_defs_for_file is not None
                            and ref.symbol not in local_defs_for_file(relpath)):
                        dispatch = _try_singleton(
                            call, singleton_index, def_symbols_set, service,
                        )
                        if dispatch is None:
                            calls_unresolved += 1
                            continue
                # M5 Task 1 (pilot Bug B, see module docstring): first-party for a
                # non-local symbol is decided by EXISTENCE OF A STAGED DEF, not by
                # `parsed.package == service` -- --project-name makes that comparison
                # unreliable (it stamps package=service on every resolved symbol,
                # including third-party library calls resolvable via this service's own
                # venv). `parsed.package`/`.descriptors` are deliberately not consulted
                # here at all any more.
                elif ref.symbol not in def_symbols_set:
                    dispatch = _try_singleton(call, singleton_index, def_symbols_set, service)
                    if dispatch is None:
                        calls_external += 1
                        continue
                elif not _is_callable_descriptor(parsed):
                    # M10 T1: resolved + staged def, but NOT function/method-shaped
                    # (the exact "module.attr" shape the pilot diagnosed) -- try the
                    # redirect; on failure dst below still falls back to the
                    # ORIGINAL (dangling) symbol, unchanged from pre-M10 behavior
                    # (still "joined", still silently dropped at load -- this task
                    # narrows, never widens, what gets dropped).
                    dispatch = _try_singleton(call, singleton_index, def_symbols_set, service)

            dst_id = (
                dispatch.dst_id if dispatch is not None
                else symbol_to_node_id(service, relpath, ref.symbol)
            )
            src_id = _caller_id(service, relpath, facts, call.enclosing_def, def_symbol_lookup)

            key = (src_id, dst_id)
            entry = agg.get(key)
            if entry is None:
                entry = {
                    "count": 0, "file": relpath, "line": call.start_line,
                    "resolution": dispatch.resolution if dispatch is not None else resolution,
                    "confidence": dispatch.confidence if dispatch is not None else confidence,
                    "mechanism": "singleton_dispatch" if dispatch is not None else None,
                }
                agg[key] = entry
            entry["count"] += 1
            calls_joined += 1

    edges = [
        EdgeRec(
            src=src, dst=dst, type="CALLS",
            resolution=entry["resolution"], confidence=entry["confidence"],
            extractor=_EXTRACTOR,
            evidence_file=entry["file"], evidence_line=entry["line"],
            props=(
                {"callsite_count": entry["count"], "mechanism": entry["mechanism"]}
                if entry["mechanism"] is not None
                else {"callsite_count": entry["count"]}
            ),
        )
        for (src, dst), entry in agg.items()
    ]
    # origin_service=service (M2 final review fix): S6's own upsert_edges call ("build_calls
    # пишет сам" -- it writes its own batch, separate from analyze.py's S5 batch above) must
    # tag its CALLS edges with the SAME service too, or an untagged (origin_service=None)
    # batch would never be found by ANY begin_service() call. A same-(src,dst) CALLS edge
    # re-emitted on the next analyze just replaces its own row either way (INSERT OR
    # REPLACE on the (src,dst,type) PK), but a call site REMOVED from source between two
    # analyze runs would leave its now-stale CALLS edge undeletable forever -- the exact
    # same "survives re-index" symptom this whole fix batch targets, just for CALLS instead
    # of HANDLES/CONTAINS (see Staging.upsert_edges/begin_service docstrings).
    staging.upsert_edges(edges, origin_service=service)

    return JoinStats(
        calls_joined=calls_joined,
        calls_unresolved=calls_unresolved,
        calls_external=calls_external,
    )
