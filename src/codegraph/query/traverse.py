"""trace_process/find_paths algorithms (M2 T8): pure Python graph walks over a
GraphStore's get_nodes()/neighbors() primitives -- no Cypher here (see Global
Constraint in the M2 plan: "Весь Cypher — stores/falkordb/"), no store_factory/
error-dict/clamping concerns either (that's query.api.GraphQuery's job -- these
two functions take an already-constructed store and already-validated/clamped
parameters, and are tested directly against a fake store in test_traverse.py).

trace_process's segment model: a "segment" is a same-service run of the code call
graph (CALLS/DEPENDS_ON/INVOKES_ACTIVITY out-edges, downstream-only in M2), walked
breadth-first from an entry node up to _SEGMENT_MAX_DEPTH hops / _SEGMENT_MAX_BRANCH
fan-out per node. A segment ends where the code crosses a channel boundary
(PRODUCES/CALLS_HTTP out-edges) -- those become `exits`, not `steps`.

Cross-channel resolution (which node(s) start the NEXT segment) does NOT re-derive
linking/segments.py's PRODUCES/CONSUMES/CONTAINS/HANDLES pairing logic here --
that would duplicate a linker that already ran once, at index time, and stored its
answer as a NEXT_SEGMENT edge (props.via_channel_id) on the very node this walk is
already standing on (the producer/CALLS_HTTP-caller). So `exits` resolution is
purely the "fast path" the plan describes: look up this node's own NEXT_SEGMENT
out-edges, keep the ones whose via_channel_id matches the channel just reached.
This one lookup transparently covers every cross-channel shape the plan's rules
table enumerates (event<-CONSUMES, topic-consumer via CONTAINS, http->HANDLES) --
segments.derive() already collapsed all of them into the same via_channel_id-keyed
edge shape (see linking/segments.py's own docstring: PRODUCES/CALLS_HTTP treated
identically as "producer side", CONSUMES/HANDLES as "consumer side", containment
folded into the exact-channel pairing's via_channel_id too), so there is nothing
left for traverse.py to special-case per channel kind.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import Any

from codegraph.core.schema import ROLE_KINDS

NodeOut = dict[str, Any]

_INTRA_EDGE_TYPES = ("CALLS", "DEPENDS_ON", "INVOKES_ACTIVITY")
_EXIT_EDGE_TYPES = ("PRODUCES", "CALLS_HTTP")
_SEGMENT_EDGE_TYPES = [*_INTRA_EDGE_TYPES, *_EXIT_EDGE_TYPES]

_SEGMENT_MAX_DEPTH = 15  # hops from a segment's entry; a node reached AT this depth
# is recorded as a step but not itself expanded (see _walk_segment). Whether that
# non-expansion counts as truncation is decided honestly (T8 review fix): one extra
# neighbors-peek on the capped node -- truncated only if it actually HAS onward
# edges (above min_confidence) the walk would have processed. A COMPLETE 15-hop
# chain, whose last node merely sits at the cap with nothing beyond it, reads
# truncated=False.
_SEGMENT_MAX_BRANCH = 8  # per-node fan-out cap, across intra+exit edges combined
_NEIGHBOR_FETCH_LIMIT = 50  # generous per-node store.neighbors() cap; confidence
# filtering happens client-side (store.neighbors has no confidence predicate), so
# this must comfortably exceed _SEGMENT_MAX_BRANCH for the "over 8 -> truncated"
# check below to see the true count rather than an already-clipped one.

_FIND_PATHS_NEIGHBOR_LIMIT = 50  # per-node cap for find_paths' BFS, analogous to
# GraphQuery.who_calls' _DEFAULT_CALLER_LIMIT -- bounds work on a hub node without
# affecting correctness for the fixture-scale graphs this project targets.


def _sorted_hops(hops: list, min_confidence: float) -> list:
    """min_confidence фильтрует рёбра (шаги и переходы): drop any hop whose
    edge_props["confidence"] is below the floor (missing confidence -- e.g. a fake
    store in a test that didn't set one -- is never filtered out). Sorted by
    (edge_type, neighbor_id) for deterministic step/exit ordering regardless of
    store iteration order (FalkorDB's own row order is not contractually stable)."""
    kept = [h for h in hops if h[1].get("confidence", 1.0) >= min_confidence]
    kept.sort(key=lambda h: (h[0], h[2].get("id") or ""))
    return kept


def _walk_segment(store: Any, entry_id: str, min_confidence: float) -> dict:
    """BFS from entry_id over _SEGMENT_EDGE_TYPES; intra-edge hops become `steps`
    (and get expanded further, depth/visited permitting), exit-edge hops become
    raw (channel_id -> {producer node ids, channel node}) bookkeeping consumed by
    `_resolve_exits` below. Cycle-safe: a node is only ever enqueued for expansion
    once (matches query.api.GraphQuery.expand_neighbors' own visited-set pattern),
    though a repeat hop INTO an already-visited node is still recorded as a step
    (it's a real edge in the graph, just not a new place to keep walking from).

    M5 T5: also returns `step_parents` (parallel to `steps` -- the id of the node
    each step's edge was walked FROM) and `exit_producer_ids` (the flat union of
    every node id that produced at least one of this segment's own exits) -- pure
    bookkeeping alongside the two places that already record this information
    (`steps.append`/`exit_producers.setdefault`), consumed by `_compact_steps`
    post-processing in `trace_process` below. Nothing about the walk itself
    (order, cycle/depth/branch handling) changes."""
    steps: list[dict] = []
    step_parents: list[str] = []
    confidences: list[float] = []
    exit_channel_nodes: dict[str, NodeOut] = {}
    exit_producers: dict[str, set[str]] = {}
    exit_producer_ids: set[str] = set()
    truncated = False

    visited = {entry_id}
    frontier: list[tuple[str, int]] = [(entry_id, 0)]
    idx = 0
    while idx < len(frontier):
        node_id, depth = frontier[idx]
        idx += 1
        if depth >= _SEGMENT_MAX_DEPTH:
            # Honest truncation (T8 review fix): reaching the cap only truncated
            # something if this node actually has onward edges the walk would have
            # processed -- ANY of _SEGMENT_EDGE_TYPES (a cut-off exit is a missing
            # channel/next segment, as real a loss as a missing step), above the
            # min_confidence floor (a below-floor edge would have been dropped by
            # _sorted_hops anyway, cap or no cap). Peek skipped once truncated is
            # already True -- the flag can't get any truer.
            if not truncated:
                peek = store.neighbors(node_id, _SEGMENT_EDGE_TYPES, "out", _NEIGHBOR_FETCH_LIMIT)
                if any(h[1].get("confidence", 1.0) >= min_confidence for h in peek):
                    truncated = True
            continue

        raw_hops = store.neighbors(node_id, _SEGMENT_EDGE_TYPES, "out", _NEIGHBOR_FETCH_LIMIT)
        hops = _sorted_hops(raw_hops, min_confidence)
        if len(hops) > _SEGMENT_MAX_BRANCH:
            truncated = True
            hops = hops[:_SEGMENT_MAX_BRANCH]

        for edge_type, edge_props, neighbor, _hop_direction in hops:
            neighbor_id = neighbor.get("id")
            confidence = edge_props.get("confidence")
            if confidence is not None:
                confidences.append(confidence)

            if edge_type in _EXIT_EDGE_TYPES:
                if neighbor_id is not None:
                    exit_channel_nodes[neighbor_id] = neighbor
                    exit_producers.setdefault(neighbor_id, set()).add(node_id)
                    exit_producer_ids.add(node_id)
                continue

            steps.append(
                {
                    "edge_type": edge_type,
                    "props": edge_props,
                    "node": neighbor,
                    "direction": "out",
                }
            )
            step_parents.append(node_id)
            if neighbor_id is not None and neighbor_id not in visited:
                visited.add(neighbor_id)
                frontier.append((neighbor_id, depth + 1))

    exits = _resolve_exits(store, exit_channel_nodes, exit_producers, min_confidence, confidences)
    return {
        "steps": steps,
        "step_parents": step_parents,
        "exit_producer_ids": exit_producer_ids,
        "exits": exits,
        "truncated": truncated,
        "confidences": confidences,
    }


def _resolve_exits(
    store: Any,
    exit_channel_nodes: dict[str, NodeOut],
    exit_producers: dict[str, set[str]],
    min_confidence: float,
    confidences: list[float],
) -> list[dict]:
    """Fast path: for each channel this segment produced/called into, look up the
    NEXT_SEGMENT out-edges of every node that did so, keep the ones whose
    via_channel_id matches THIS channel (a producer node can have NEXT_SEGMENT
    edges to several different channels' consumers -- via_channel_id disambiguates
    which ones belong here), collect their destination ids (see module docstring
    for why this one lookup already covers every cross-channel shape)."""
    exits = []
    for channel_id in sorted(exit_channel_nodes):
        next_ids: set[str] = set()
        for producer_id in sorted(exit_producers[channel_id]):
            hops = store.neighbors(producer_id, ["NEXT_SEGMENT"], "out", _NEIGHBOR_FETCH_LIMIT)
            for _edge_type, edge_props, neighbor, _direction in hops:
                if edge_props.get("via_channel_id") != channel_id:
                    continue
                confidence = edge_props.get("confidence")
                if confidence is not None and confidence < min_confidence:
                    continue
                if confidence is not None:
                    confidences.append(confidence)
                neighbor_id = neighbor.get("id")
                if neighbor_id is not None:
                    next_ids.add(neighbor_id)
        exits.append(
            {
                "channel": exit_channel_nodes[channel_id],
                "next_entry_ids": sorted(next_ids),
            }
        )
    return exits


# M5 T5 (pilot §7.3 -- a single-service repo's trace dumps a flat, undifferentiated
# segment, 80 steps on the real pilot corpus): compact-mode post-processing over an
# ALREADY-BUILT segment's steps, applied in trace_process below -- _walk_segment's
# own BFS logic (order, cycle/depth/branch handling) is untouched by any of this.
_COMPACT_STEP_GATE = 15  # segment-level gate: _compact_steps is a strict no-op
# (returns the SAME list object) at or under this count -- fixture-scale segments
# (M2 gate's golden traces, every unit-test store in this module) never exceed it,
# so their output stays byte-identical whether compact=True (the new default) or not.
_COMPACT_RUN_HEAD = 3
_COMPACT_RUN_TAIL = 2
# A run only gets replaced by a `{"collapsed": N}` marker once it has at least 2
# interior steps to hide (run > HEAD+TAIL+1): a run of HEAD+TAIL (5) or fewer
# would show MORE entries collapsed than plain (head + 1 marker + tail = 6 > 5),
# and a run of exactly HEAD+TAIL+1 (6) merely breaks even on display count
# (6 == 6) while still destroying one real step's identity -- collapse must
# strictly SHRINK the display, never merely match it (M5 T5 review fix).


def _compact_steps(
    steps: list[dict],
    step_parents: Sequence[str | None],
    exit_producer_ids: Collection[str],
) -> list[dict]:
    """Collapse maximal consecutive (in `steps`' own BFS-discovery order -- see the
    NOTE below) runs of "boring" steps once the segment has more than
    _COMPACT_STEP_GATE steps: each qualifying run (>= 2 interior steps to hide --
    see the break-even analysis at the constants above) keeps its first
    _COMPACT_RUN_HEAD and last _COMPACT_RUN_TAIL steps, with a single
    `{"collapsed": N}` marker (N = the run's own hidden interior length, always
    >= 2) in between.

    A step is "boring" (collapsible) unless its node:
      - carries one of core.schema.ROLE_KINDS (RouteHandler/MessageConsumer/
        MessageProducer/TemporalWorkflow/TemporalActivity/TemporalSignalHandler --
        the check itself reads ROLE_KINDS generically, so M7 T4's new role was
        covered the moment schema.py grew it; only this prose list needed updating)
        -- collapsing a role-bearing hop would hide exactly the service/
        channel-boundary-shaped step a reader most needs to see;
      - has more than one OUTGOING step within this SAME segment (`step_parents`,
        parallel to `steps` -- see _walk_segment) -- a node that itself calls >1
        further step here is a fan-out/branch point, never safe to fold into an
        ellipsis;
      - produced at least one of this segment's own exits (`exit_producer_ids`,
        also from _walk_segment) -- PRODUCES/CALLS_HTTP transitions are the
        cross-channel handoff points the whole trace exists to show.
    A protected ("interesting") step is never moved, reordered, or altered -- it
    always interrupts (flushes) whatever run was accumulating around it, and is
    then appended to the output as-is.

    NOTE (BFS-order caveat, a deliberate simplification -- see this task's report):
    `steps` is BFS DISCOVERY order, not necessarily a single caller->callee chain's
    own sequential order once real branching is involved (siblings at the same BFS
    depth land next to each other in the list, interleaved with any OTHER sibling
    chain's own next hop) -- collapsing therefore groups "whatever this segment's
    walk visited next that also happened to be boring", not strictly "everything
    downstream of one specific caller". This matches the feature's own motivating
    shape (pilot §7.3's single-service flat call spine, substantially linear) and
    degrades gracefully -- still correct (nothing dropped, protected steps never
    touched), just a coarser grouping -- on a genuinely bushy segment."""
    if len(steps) <= _COMPACT_STEP_GATE:
        return steps

    outgoing_count: dict[str, int] = {}
    for parent_id in step_parents:
        if parent_id is not None:
            outgoing_count[parent_id] = outgoing_count.get(parent_id, 0) + 1

    def _protected(step: dict) -> bool:
        node = step.get("node") or {}
        node_id = node.get("id")
        if any(r in ROLE_KINDS for r in (node.get("roles") or ())):
            return True
        if node_id is None:
            return False
        return outgoing_count.get(node_id, 0) > 1 or node_id in exit_producer_ids

    result: list[dict] = []
    run: list[dict] = []

    def _flush_run() -> None:
        # `> HEAD+TAIL+1`, not `> HEAD+TAIL` -- collapse must strictly shrink the
        # display (interior >= 2); see the break-even analysis at the constants above.
        if len(run) > _COMPACT_RUN_HEAD + _COMPACT_RUN_TAIL + 1:
            interior = len(run) - _COMPACT_RUN_HEAD - _COMPACT_RUN_TAIL
            result.extend(run[:_COMPACT_RUN_HEAD])
            result.append({"collapsed": interior})
            result.extend(run[-_COMPACT_RUN_TAIL:])
        else:
            result.extend(run)
        run.clear()

    for step in steps:
        if _protected(step):
            _flush_run()
            result.append(step)
        else:
            run.append(step)
    _flush_run()
    return result


def trace_process(
    store: Any,
    entrypoint_id: str,
    max_segments: int,
    min_confidence: float,
    compact: bool = True,
) -> dict:
    """Downstream-only (M2) multi-segment trace from entrypoint_id. Segment-level
    BFS: visited_entries prevents both re-visiting a segment and infinite process
    cycles (segment N's exit looping back to an earlier entry); max_segments caps
    the OUTPUT list (truncated=True if more entries were pending once the cap
    hit). Aggregate confidence is the minimum confidence over every edge actually
    walked (steps + NEXT_SEGMENT transitions) -- a chain is only as trustworthy as
    its weakest link; 1.0 if the trace contains no edges at all (single node, no
    steps/exits -- nothing to doubt).

    compact (M5 T5, default True): post-process each segment's steps through
    _compact_steps above -- collapses long boring runs (see its own docstring),
    a no-op for any segment at or under _COMPACT_STEP_GATE steps. compact=False
    (CLI `--full`, MCP callers who pass it explicitly) restores the pre-M5 always-
    full behavior."""
    if not store.get_nodes([entrypoint_id]):
        return {"error": f"entrypoint not found: {entrypoint_id}"}

    segments: list[dict] = []
    all_confidences: list[float] = []
    visited_entries = {entrypoint_id}
    queue = [entrypoint_id]
    truncated = False

    while queue and len(segments) < max_segments:
        entry_id = queue.pop(0)
        entry_nodes = store.get_nodes([entry_id])
        if not entry_nodes:
            continue  # dangling NEXT_SEGMENT target -- skip gracefully, not a hard error
        entry_node = entry_nodes[0]

        walked = _walk_segment(store, entry_id, min_confidence)
        steps = walked["steps"]
        if compact:
            steps = _compact_steps(steps, walked["step_parents"], walked["exit_producer_ids"])
        segments.append(
            {
                "service": entry_node.get("service", ""),
                "entry": entry_node,
                "steps": steps,
                "exits": walked["exits"],
                "truncated": walked["truncated"],
            }
        )
        all_confidences.extend(walked["confidences"])
        if walked["truncated"]:
            truncated = True

        for exit_ in walked["exits"]:
            for next_id in exit_["next_entry_ids"]:
                if next_id not in visited_entries:
                    visited_entries.add(next_id)
                    queue.append(next_id)

    if queue:  # more entries were pending than max_segments allowed
        truncated = True

    confidence = min(all_confidences) if all_confidences else 1.0
    return {"segments": segments, "confidence": confidence, "truncated": truncated}


def _reconstruct_path(visited: dict[str, tuple], to_id: str) -> list[dict]:
    chain = []
    node_id: str | None = to_id
    while node_id is not None:
        parent_id, edge_type, direction, node_dict = visited[node_id]
        chain.append({"node": node_dict, "edge_type": edge_type, "direction": direction})
        node_id = parent_id
    chain.reverse()
    return chain


def find_paths(
    store: Any,
    from_id: str,
    to_id: str,
    max_hops: int,
    edge_types: Sequence[str] | None,
) -> dict:
    """BFS from from_id over neighbors(direction="both") -- edge direction is
    ignored for reachability (both in- and out-edges expand the frontier), so this
    finds a shortest connection between two nodes regardless of which way its
    edges point; each path element after the first carries the edge_type/direction
    of the hop that reached it (direction is neighbors()'s own true per-hop
    direction, same semantics as GraphQuery.expand_neighbors). First-found via BFS
    is guaranteed shortest by hop count. Not found (or from_id doesn't exist) ->
    {"path": None} (a normal outcome, not an error -- see query.api.GraphQuery."""
    if from_id == to_id:
        nodes = store.get_nodes([from_id])
        if not nodes:
            return {"path": None}
        return {"path": [{"node": nodes[0], "edge_type": None, "direction": None}]}

    start_nodes = store.get_nodes([from_id])
    if not start_nodes:
        return {"path": None}

    visited: dict[str, tuple] = {from_id: (None, None, None, start_nodes[0])}
    frontier = [from_id]
    for _ in range(max_hops):
        next_frontier: list[str] = []
        for node_id in frontier:
            # _sorted_hops with min_confidence=0.0 -- pure deterministic ordering,
            # NO confidence filtering (find_paths' contract has no min_confidence
            # parameter; 0.0 keeps every hop). Sorting mirrors trace_process: with
            # several equal-length paths, the (edge_type, neighbor_id) tie-break
            # decides which one wins, so the winner must not depend on the store's
            # own row order (T8 review fix -- FalkorDB row order isn't stable).
            hops = _sorted_hops(
                store.neighbors(node_id, edge_types, "both", _FIND_PATHS_NEIGHBOR_LIMIT),
                min_confidence=0.0,
            )
            for edge_type, _edge_props, neighbor, hop_direction in hops:
                neighbor_id = neighbor.get("id")
                if neighbor_id is None or neighbor_id in visited:
                    continue
                visited[neighbor_id] = (node_id, edge_type, hop_direction, neighbor)
                if neighbor_id == to_id:
                    return {"path": _reconstruct_path(visited, to_id)}
                next_frontier.append(neighbor_id)
        frontier = next_frontier
        if not frontier:
            break
    return {"path": None}
