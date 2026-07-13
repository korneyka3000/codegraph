"""S6 join: SCIP refs × tree-sitter call-sites → CALLS edges.

Per call-site (`CallFact.callee_start_byte`), find the SCIP ref occupying that exact
byte position (exact dict hit) or, failing that, the ref whose [start_byte, end_byte)
span contains it (sorted-list bisect fallback — covers small span-conversion drift).
A global ref symbol from a different package than the current service is external
(counted, not joined: cross-service edges are forbidden by staging, see
`Staging.upsert_edges`); a local symbol is always first-party. Calls that match no ref
at all (fully dynamic/unresolved) are counted unresolved. Matched first-party calls are
aggregated per (src, dst) into one CALLS edge with a callsite_count and evidence from
the first call site encountered.
"""

from __future__ import annotations

import bisect
from collections.abc import Callable
from dataclasses import dataclass

from codegraph.core import ids
from codegraph.core.schema import EdgeRec
from codegraph.extractors.python_core import nesting_chain
from codegraph.parsing.facts import FileFacts
from codegraph.resolvers.base import RefRow
from codegraph.resolvers.scip.symbols import parse_symbol, symbol_to_node_id
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


def build_calls(
    service: str,
    staging: Staging,
    facts_by_file: dict[str, FileFacts],
    def_symbol_lookup: Callable[[str, int], str | None],
    resolution: str = "static",
    confidence: float = 1.0,
) -> JoinStats:
    calls_joined = 0
    calls_unresolved = 0
    calls_external = 0
    # (src, dst) -> {"count": int, "file": str, "line": int} — evidence is the FIRST
    # call site encountered for that pair (dict only set once, per key).
    agg: dict[tuple[str, str], dict] = {}

    for relpath, facts in facts_by_file.items():
        refs = staging.refs_for_file(service, relpath)  # already sorted by start_byte
        by_start = {r.start_byte: r for r in refs}
        starts = [r.start_byte for r in refs]

        for call in facts.calls:
            ref = _find_ref(call.callee_start_byte, by_start, refs, starts)
            if ref is None:
                calls_unresolved += 1
                continue

            parsed = parse_symbol(ref.symbol)
            # ВНИМАНИЕ (M1b): pyright деградирует нерезолвленные 3rd-party в 'local N' — такие
            # "first-party" локалы без def-occurrence в том же документе на деле unresolved
            # (см. m1a-task-10-report §2).
            if not parsed.is_local and parsed.package != service:
                calls_external += 1
                continue

            dst_id = symbol_to_node_id(service, relpath, ref.symbol)
            src_id = _caller_id(service, relpath, facts, call.enclosing_def, def_symbol_lookup)

            key = (src_id, dst_id)
            entry = agg.get(key)
            if entry is None:
                entry = {"count": 0, "file": relpath, "line": call.start_line}
                agg[key] = entry
            entry["count"] += 1
            calls_joined += 1

    edges = [
        EdgeRec(
            src=src, dst=dst, type="CALLS",
            resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
            evidence_file=entry["file"], evidence_line=entry["line"],
            props={"callsite_count": entry["count"]},
        )
        for (src, dst), entry in agg.items()
    ]
    staging.upsert_edges(edges)

    return JoinStats(
        calls_joined=calls_joined,
        calls_unresolved=calls_unresolved,
        calls_external=calls_external,
    )
