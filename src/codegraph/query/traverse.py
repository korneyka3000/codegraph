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

from collections.abc import Sequence
from typing import Any

NodeOut = dict[str, Any]

_INTRA_EDGE_TYPES = ("CALLS", "DEPENDS_ON", "INVOKES_ACTIVITY")
_EXIT_EDGE_TYPES = ("PRODUCES", "CALLS_HTTP")
_SEGMENT_EDGE_TYPES = [*_INTRA_EDGE_TYPES, *_EXIT_EDGE_TYPES]

_SEGMENT_MAX_DEPTH = 15  # hops from a segment's entry; a node reached AT this depth
# is recorded as a step but not itself expanded (see _walk_segment) -- conservatively
# marks the segment truncated even if that specific node had no further edges (no
# cheap way to know without querying past the cap, which would defeat having one).
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
    (it's a real edge in the graph, just not a new place to keep walking from)."""
    steps: list[dict] = []
    confidences: list[float] = []
    exit_channel_nodes: dict[str, NodeOut] = {}
    exit_producers: dict[str, set[str]] = {}
    truncated = False

    visited = {entry_id}
    frontier: list[tuple[str, int]] = [(entry_id, 0)]
    idx = 0
    while idx < len(frontier):
        node_id, depth = frontier[idx]
        idx += 1
        if depth >= _SEGMENT_MAX_DEPTH:
            truncated = True  # see _SEGMENT_MAX_DEPTH docstring above
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
                continue

            steps.append(
                {
                    "edge_type": edge_type,
                    "props": edge_props,
                    "node": neighbor,
                    "direction": "out",
                }
            )
            if neighbor_id is not None and neighbor_id not in visited:
                visited.add(neighbor_id)
                frontier.append((neighbor_id, depth + 1))

    exits = _resolve_exits(store, exit_channel_nodes, exit_producers, min_confidence, confidences)
    return {"steps": steps, "exits": exits, "truncated": truncated, "confidences": confidences}


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


def trace_process(
    store: Any,
    entrypoint_id: str,
    max_segments: int,
    min_confidence: float,
) -> dict:
    """Downstream-only (M2) multi-segment trace from entrypoint_id. Segment-level
    BFS: visited_entries prevents both re-visiting a segment and infinite process
    cycles (segment N's exit looping back to an earlier entry); max_segments caps
    the OUTPUT list (truncated=True if more entries were pending once the cap
    hit). Aggregate confidence is the minimum confidence over every edge actually
    walked (steps + NEXT_SEGMENT transitions) -- a chain is only as trustworthy as
    its weakest link; 1.0 if the trace contains no edges at all (single node, no
    steps/exits -- nothing to doubt)."""
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
        segments.append(
            {
                "service": entry_node.get("service", ""),
                "entry": entry_node,
                "steps": walked["steps"],
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
            hops = store.neighbors(node_id, edge_types, "both", _FIND_PATHS_NEIGHBOR_LIMIT)
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
