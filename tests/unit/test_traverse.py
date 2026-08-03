"""Юниты query.traverse (M2 T8) на fake store: trace_process (сегмент-обход) +
find_paths (BFS). Живой FalkorDB не нужен -- контракт MCP-схем/сети живёт в
tests/integration/test_mcp_contract.py (marker falkordb).

Мини-граф из плана (§Task 8 test bullet): route(create_order, RouteHandler)
-CALLS-> save_order -PRODUCES-> chan:event_type:OrderCreated <-CONSUMES-
handle_order_created(MessageConsumer) -CALLS(mechanism=temporal_start)->
KycWorkflow.run -INVOKES_ACTIVITY-> verify_documents -CALLS_HTTP->
chan:http:doc-mgmt:GET /documents/{id} -HANDLES-> get_document(RouteHandler).
Three segments (orders-api / kyc-worker / doc-mgmt), two channel crossings (one
event, one http); segments.py's own NEXT_SEGMENT derivation is NOT re-run here --
the fake store's NEXT_SEGMENT edges are seeded directly, exactly as T7's
segments.derive() would have produced them (via_channel_id keyed on the producer
side's own edge target -- see linking/segments.py docstring), since traverse.py's
job is to CONSUME that pre-derived fast path, not re-derive it (see traverse.py
module docstring)."""

from __future__ import annotations

import pytest

from codegraph.core import schema
from codegraph.query import traverse


class FakeStore:
    """Duck-typed GraphStore subset traverse.py actually calls: get_nodes/neighbors
    only (same shape as tests/unit/test_query_api.py's FakeStore -- traverse.py
    never touches upsert/schema/stats/search_fulltext, it's a read-only walker)."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict] = {}
        self.edges: list[tuple[str, str, dict, str]] = []  # (src, edge_type, props, dst)

    def add_node(self, node_id: str, **props) -> None:
        self.nodes[node_id] = {"id": node_id, **props}

    def add_edge(self, src: str, edge_type: str, dst: str, **edge_props) -> None:
        edge_props.setdefault("confidence", 1.0)
        edge_props.setdefault("resolution", "static")
        self.edges.append((src, edge_type, edge_props, dst))

    def get_nodes(self, ids):
        return [self.nodes[i] for i in ids if i in self.nodes]

    def neighbors(self, node_id, edge_types, direction, limit):
        # trace_process only ever calls with direction="out" (downstream-only, M2);
        # find_paths calls with direction="both". Mirrors FalkorStore.neighbors'
        # documented both=out+in-merge semantics over a plain in-memory edge list.
        out = [
            (et, dict(ep), self.nodes[d], "out")
            for (s, et, ep, d) in self.edges
            if s == node_id and (not edge_types or et in edge_types)
        ]
        inn = [
            (et, dict(ep), self.nodes[s], "in")
            for (s, et, ep, d) in self.edges
            if d == node_id and (not edge_types or et in edge_types)
        ]
        merged = out if direction == "out" else inn if direction == "in" else out + inn
        return merged[:limit]


def _three_segment_store() -> FakeStore:
    store = FakeStore()
    # -- segment 0: orders-api --
    store.add_node(
        "create_order",
        service="orders-api",
        kind="Function",
        name="create_order",
        roles=["RouteHandler"],
    )
    store.add_node("save_order", service="orders-api", kind="Function", name="save_order")
    store.add_node(
        "chan:event_type:OrderCreated",
        kind="Channel",
        name="OrderCreated",
        channel_kind="event_type",
    )
    store.add_edge("create_order", "CALLS", "save_order")
    store.add_edge("save_order", "PRODUCES", "chan:event_type:OrderCreated")

    # -- segment 1: kyc-worker --
    store.add_node(
        "handle_order_created",
        service="kyc-worker",
        kind="Function",
        name="handle_order_created",
        roles=["MessageConsumer"],
    )
    store.add_node("KycWorkflow.run", service="kyc-worker", kind="Function", name="run")
    store.add_node(
        "verify_documents",
        service="kyc-worker",
        kind="Function",
        name="verify_documents",
        roles=["TemporalActivity"],
    )
    store.add_node(
        "chan:http:doc-mgmt:GET /documents/{id}",
        kind="Channel",
        name="GET /documents/{id}",
        channel_kind="http_route",
    )
    store.add_edge("handle_order_created", "CONSUMES", "chan:event_type:OrderCreated")
    store.add_edge(
        "handle_order_created",
        "CALLS",
        "KycWorkflow.run",
        mechanism="temporal_start",
        resolution="dynamic",
    )
    store.add_edge("KycWorkflow.run", "INVOKES_ACTIVITY", "verify_documents")
    store.add_edge("verify_documents", "CALLS_HTTP", "chan:http:doc-mgmt:GET /documents/{id}")

    # -- segment 2: doc-mgmt --
    store.add_node(
        "get_document",
        service="doc-mgmt",
        kind="Function",
        name="get_document",
        roles=["RouteHandler"],
    )
    store.add_edge("chan:http:doc-mgmt:GET /documents/{id}", "HANDLES", "get_document")

    # -- fast-path NEXT_SEGMENT edges, exactly as linking/segments.derive() would
    # produce them: src = the node with the actual PRODUCES/CALLS_HTTP edge. --
    store.add_edge(
        "save_order",
        "NEXT_SEGMENT",
        "handle_order_created",
        via_channel_id="chan:event_type:OrderCreated",
        derived=True,
        confidence=1.0,
        resolution="static",
    )
    store.add_edge(
        "verify_documents",
        "NEXT_SEGMENT",
        "get_document",
        via_channel_id="chan:http:doc-mgmt:GET /documents/{id}",
        derived=True,
        confidence=1.0,
        resolution="static",
    )
    return store


# -- happy path: 3 segments, 2 channel crossings, temporal_start step --


def test_three_segments_via_event_and_http_channel():
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)

    assert "error" not in result
    assert len(result["segments"]) == 3
    seg0, seg1, seg2 = result["segments"]

    assert seg0["service"] == "orders-api"
    assert seg0["entry"]["id"] == "create_order"
    assert [s["node"]["id"] for s in seg0["steps"]] == ["save_order"]
    assert seg0["steps"][0]["edge_type"] == "CALLS"
    assert seg0["steps"][0]["direction"] == "out"
    assert len(seg0["exits"]) == 1
    assert seg0["exits"][0]["channel"]["id"] == "chan:event_type:OrderCreated"
    assert seg0["exits"][0]["next_entry_ids"] == ["handle_order_created"]
    assert seg0["truncated"] is False

    assert seg1["service"] == "kyc-worker"
    assert seg1["entry"]["id"] == "handle_order_created"
    step_ids = {s["node"]["id"] for s in seg1["steps"]}
    assert step_ids == {"KycWorkflow.run", "verify_documents"}
    assert seg1["exits"][0]["channel"]["id"] == "chan:http:doc-mgmt:GET /documents/{id}"
    assert seg1["exits"][0]["next_entry_ids"] == ["get_document"]

    assert seg2["service"] == "doc-mgmt"
    assert seg2["entry"]["id"] == "get_document"
    assert seg2["steps"] == []
    assert seg2["exits"] == []

    assert result["truncated"] is False
    assert 0.0 < result["confidence"] <= 1.0


def test_temporal_start_call_appears_as_intra_segment_step_with_mechanism_prop():
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    seg1 = result["segments"][1]
    step = next(s for s in seg1["steps"] if s["node"]["id"] == "KycWorkflow.run")
    assert step["edge_type"] == "CALLS"
    assert step["props"]["mechanism"] == "temporal_start"


def test_invokes_activity_step_present_in_same_segment_as_temporal_start():
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    seg1 = result["segments"][1]
    step = next(s for s in seg1["steps"] if s["node"]["id"] == "verify_documents")
    assert step["edge_type"] == "INVOKES_ACTIVITY"


def test_depends_on_edge_appears_as_intra_segment_step():
    # T8 review fast-follow: DEPENDS_ON is the third intra-segment edge type
    # (same walk path as CALLS/INVOKES_ACTIVITY) -- pin it explicitly so a future
    # edit to _INTRA_EDGE_TYPES can't silently drop FastAPI DI edges from traces.
    store = FakeStore()
    store.add_node("handler", service="a", kind="Function", roles=["RouteHandler"])
    store.add_node("get_db", service="a", kind="Function")
    store.add_edge("handler", "DEPENDS_ON", "get_db", via="depends")

    result = traverse.trace_process(store, "handler", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert [(s["edge_type"], s["node"]["id"]) for s in seg["steps"]] == [("DEPENDS_ON", "get_db")]
    assert seg["steps"][0]["props"]["via"] == "depends"
    assert seg["steps"][0]["direction"] == "out"


# -- entrypoint not found --


def test_entrypoint_not_found_returns_error_dict():
    store = FakeStore()
    result = traverse.trace_process(store, "does-not-exist", max_segments=12, min_confidence=0.3)
    assert "error" in result
    assert "does-not-exist" in result["error"]


# -- min_confidence filters steps and cross-segment transitions --


def test_min_confidence_filters_low_confidence_step():
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node("strong", service="a", kind="Function")
    store.add_node("weak", service="a", kind="Function")
    store.add_edge("entry", "CALLS", "strong", confidence=0.9)
    store.add_edge("entry", "CALLS", "weak", confidence=0.1)

    high = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    assert {s["node"]["id"] for s in high["segments"][0]["steps"]} == {"strong"}

    low = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.0)
    assert {s["node"]["id"] for s in low["segments"][0]["steps"]} == {"strong", "weak"}


def test_min_confidence_filters_next_segment_transition():
    store = FakeStore()
    store.add_node("producer", service="a", kind="Function")
    store.add_node("chan:event_type:E", kind="Channel", channel_kind="event_type")
    store.add_node("consumer", service="b", kind="Function")
    store.add_edge("producer", "PRODUCES", "chan:event_type:E")
    store.add_edge(
        "producer",
        "NEXT_SEGMENT",
        "consumer",
        via_channel_id="chan:event_type:E",
        derived=True,
        confidence=0.1,
        resolution="heuristic",
    )

    result = traverse.trace_process(store, "producer", max_segments=12, min_confidence=0.3)
    assert len(result["segments"]) == 1  # low-confidence NEXT_SEGMENT not followed
    assert result["segments"][0]["exits"][0]["next_entry_ids"] == []


# -- M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): external
# exit-hops (CALLS_HTTP/PRODUCES flagged external=True -- a documented boundary
# outside the workspace, see linking/http_routes.py) are EXCLUDED from the
# trace/segment aggregate confidence floor -- honest knowledge of a boundary must
# not drag a trace down the same way a genuine modeling gap does, even though the
# EDGE itself still (honestly) carries heuristic/0.5.
#
# M10 T4 (linking/http_routes.py's own module docstring, "SHARED-CHANNEL PROPS"
# section): `external`/`external_host` moved from the channel NODE's own props to
# the CALLS_HTTP/PRODUCES EDGE's -- the fixtures below set them on `add_edge`, not
# `add_node`, and exit assertions read `exits[i]["external"]`/`["external_host"]`
# (sibling keys of `exits[i]["channel"]` now), not `exits[i]["channel"]["external"]`.


def test_external_exit_hop_confidence_excluded_from_aggregate_stays_1_0():
    """A trace whose ONLY weak link is an external exit keeps confidence 1.0."""
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node("strong_step", service="a", kind="Function")
    store.add_node(
        "chan:http:?:GET /external",
        kind="Channel", name="GET /external", channel_kind="http_route", unresolved=True,
    )
    store.add_edge("entry", "CALLS", "strong_step", confidence=1.0, resolution="static")
    store.add_edge(
        "entry", "CALLS_HTTP", "chan:http:?:GET /external",
        confidence=0.5, resolution="heuristic",
        external=True, external_host="api-gateway.prod.svc.cluster.local",
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)

    assert result["confidence"] == 1.0
    # the external exit itself is still present in the trace, at its own honest
    # heuristic/0.5 edge confidence -- EXCLUDED from the aggregate, never hidden.
    exits = result["segments"][0]["exits"]
    assert len(exits) == 1
    assert exits[0]["external"] is True
    assert exits[0]["external_host"] == "api-gateway.prod.svc.cluster.local"
    assert "external" not in exits[0]["channel"]  # the node itself carries neither prop
    # M9 T1 review Important: the machine-readable top-level signal -- a
    # programmatic/MCP consumer reading confidence=1.0 alone would conclude
    # "fully traced" for a trace that actually stops at a workspace boundary (a
    # human sees the external leg in the render; the machine needs this count --
    # same precedent as the `truncated` field, which exists for exactly this).
    assert result["external_exit_count"] == 1


def test_external_exclusion_preserves_min_over_remaining_edges():
    """M9 T1 review Minor pin (3-way MIN): external/0.5 exit (excluded) + a real
    INTERNAL heuristic/0.6 step (kept) + a static/1.0 step (kept) -> aggregate is
    exactly 0.6 -- exclusion removes ONLY the external hop's contribution, never
    disturbing the minimum over every remaining edge."""
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node("strong_step", service="a", kind="Function")
    store.add_node("weak_step", service="a", kind="Function")
    store.add_node(
        "chan:http:?:GET /external",
        kind="Channel", name="GET /external", channel_kind="http_route", unresolved=True,
    )
    store.add_edge("entry", "CALLS", "strong_step", confidence=1.0, resolution="static")
    store.add_edge("entry", "CALLS", "weak_step", confidence=0.6, resolution="heuristic")
    store.add_edge(
        "entry", "CALLS_HTTP", "chan:http:?:GET /external",
        confidence=0.5, resolution="heuristic",
        external=True, external_host="api-gateway.prod.svc.cluster.local",
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)

    assert result["confidence"] == 0.6
    assert result["external_exit_count"] == 1


def test_external_exit_count_zero_for_fully_internal_trace():
    """0 = полностью внутренний трейс -- the three-segment happy path (one event
    channel + one RESOLVED http channel, both in-workspace) reports zero."""
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    assert result["external_exit_count"] == 0


def test_external_exit_count_zero_for_plain_unresolved_exit():
    """A generic (non-external) unresolved dead-end exit does NOT count -- the
    counter reports documented BOUNDARIES only, never plain modeling gaps."""
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node(
        "chan:http:?:GET /unresolved",
        kind="Channel", name="GET /unresolved", channel_kind="http_route", unresolved=True,
    )
    store.add_edge(
        "entry", "CALLS_HTTP", "chan:http:?:GET /unresolved",
        confidence=0.5, resolution="heuristic",
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    assert result["external_exit_count"] == 0


def test_external_exit_count_sums_across_segments():
    """Two segments, each with its own external exit -> count == 2 (a per-exit
    counter over the WHOLE trace, not a per-segment or boolean signal)."""
    store = FakeStore()
    store.add_node("entryA", service="a", kind="Function")
    store.add_node("entryB", service="b", kind="Function")
    store.add_node("chan:event_type:E", kind="Channel", channel_kind="event_type")
    store.add_node(
        "chan:http:?:GET /ext-a", kind="Channel", channel_kind="http_route", unresolved=True,
    )
    store.add_node(
        "chan:http:?:GET /ext-b", kind="Channel", channel_kind="http_route", unresolved=True,
    )
    store.add_edge(
        "entryA", "CALLS_HTTP", "chan:http:?:GET /ext-a", confidence=0.5,
        external=True, external_host="gw-a.prod",
    )
    store.add_edge("entryA", "PRODUCES", "chan:event_type:E")
    store.add_edge(
        "entryA", "NEXT_SEGMENT", "entryB", via_channel_id="chan:event_type:E", derived=True
    )
    store.add_edge(
        "entryB", "CALLS_HTTP", "chan:http:?:GET /ext-b", confidence=0.5,
        external=True, external_host="gw-b.prod",
    )

    result = traverse.trace_process(store, "entryA", max_segments=12, min_confidence=0.3)
    assert result["external_exit_count"] == 2


def test_non_external_exit_hop_confidence_still_counts_toward_aggregate():
    """Contrast case: a plain (non-external) low-confidence exit -- e.g. the
    pre-existing generic-unresolved heuristic/0.5 HTTP channel -- still drags the
    trace's aggregate confidence down exactly as before this task; only
    external=True exits are excluded."""
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node("strong_step", service="a", kind="Function")
    store.add_node(
        "chan:http:?:GET /unresolved",
        kind="Channel", name="GET /unresolved", channel_kind="http_route", unresolved=True,
    )
    store.add_edge("entry", "CALLS", "strong_step", confidence=1.0, resolution="static")
    store.add_edge(
        "entry", "CALLS_HTTP", "chan:http:?:GET /unresolved",
        confidence=0.5, resolution="heuristic",
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)

    assert result["confidence"] == 0.5


def test_external_exit_with_no_other_edges_at_all_is_the_trivial_1_0_case():
    """A degenerate single-hop trace (no steps, ONE external exit): all_confidences
    ends up empty after exclusion -- falls to the SAME "no edges to doubt" 1.0
    default `trace_process` already uses for a truly edge-less trace."""
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node(
        "chan:http:?:GET /external",
        kind="Channel", name="GET /external", channel_kind="http_route", unresolved=True,
    )
    store.add_edge(
        "entry", "CALLS_HTTP", "chan:http:?:GET /external",
        confidence=0.5, resolution="heuristic",
        external=True, external_host="api-gateway.prod.svc.cluster.local",
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    assert result["confidence"] == 1.0


# -- max_segments truncation --


def test_max_segments_truncates_and_sets_flag():
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=2, min_confidence=0.3)
    assert len(result["segments"]) == 2
    assert result["truncated"] is True


def test_max_segments_not_hit_is_not_truncated_by_segment_count():
    store = _three_segment_store()
    result = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    assert result["truncated"] is False


# -- cycles terminate --


def test_intra_segment_call_cycle_does_not_hang():
    store = FakeStore()
    store.add_node("a", service="s", kind="Function")
    store.add_node("b", service="s", kind="Function")
    store.add_edge("a", "CALLS", "b")
    store.add_edge("b", "CALLS", "a")  # cycle

    result = traverse.trace_process(store, "a", max_segments=12, min_confidence=0.3)
    assert len(result["segments"]) == 1
    step_ids = [s["node"]["id"] for s in result["segments"][0]["steps"]]
    assert step_ids.count("b") == 1  # recorded once, not re-expanded infinitely
    assert step_ids.count("a") == 1  # b->a hop recorded too (a already visited, not re-queued)


def test_segment_level_cycle_does_not_hang_and_visits_each_entry_once():
    store = FakeStore()
    store.add_node("entryA", service="a", kind="Function")
    store.add_node("entryB", service="b", kind="Function")
    store.add_node("chan:event_type:E1", kind="Channel", channel_kind="event_type")
    store.add_node("chan:event_type:E2", kind="Channel", channel_kind="event_type")
    store.add_edge("entryA", "PRODUCES", "chan:event_type:E1")
    store.add_edge(
        "entryA", "NEXT_SEGMENT", "entryB", via_channel_id="chan:event_type:E1", derived=True
    )
    store.add_edge("entryB", "PRODUCES", "chan:event_type:E2")
    store.add_edge(
        "entryB", "NEXT_SEGMENT", "entryA", via_channel_id="chan:event_type:E2", derived=True
    )  # cycle back to entryA

    result = traverse.trace_process(store, "entryA", max_segments=12, min_confidence=0.3)
    assert {s["entry"]["id"] for s in result["segments"]} == {"entryA", "entryB"}
    assert len(result["segments"]) == 2


# -- topic-level consumer reached via containment (T7-derived NEXT_SEGMENT) --


def test_exit_includes_topic_level_consumer_reached_via_containment():
    # segments.derive()'s containment pairing keys via_channel_id on the EVENT the
    # producer targets directly, never the topic (see linking/segments.py
    # docstring) -- traverse.py doesn't re-derive containment itself, it just
    # follows whatever NEXT_SEGMENT the linker already produced, so this proves
    # the fast path surfaces a topic-level consumer transparently.
    store = FakeStore()
    store.add_node("entry", service="a", kind="Function")
    store.add_node("chan:event_type:E", kind="Channel", channel_kind="event_type")
    store.add_node("topic-consumer", service="b", kind="Function", roles=["MessageConsumer"])
    store.add_edge("entry", "PRODUCES", "chan:event_type:E")
    store.add_edge(
        "entry", "NEXT_SEGMENT", "topic-consumer", via_channel_id="chan:event_type:E", derived=True
    )

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    assert {s["entry"]["id"] for s in result["segments"]} == {"entry", "topic-consumer"}
    exits = result["segments"][0]["exits"]
    assert len(exits) == 1
    assert exits[0]["channel"]["id"] == "chan:event_type:E"
    assert exits[0]["next_entry_ids"] == ["topic-consumer"]


# -- branching cap (<=8) --


def test_branch_cap_limits_steps_per_node_and_sets_truncated():
    store = FakeStore()
    store.add_node("hub", service="a", kind="Function")
    for i in range(10):
        store.add_node(f"leaf{i}", service="a", kind="Function")
        store.add_edge("hub", "CALLS", f"leaf{i}")

    result = traverse.trace_process(store, "hub", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert len(seg["steps"]) == 8
    assert seg["truncated"] is True
    assert result["truncated"] is True


# -- depth cap (<=15): honest truncation (T8 review fast-follow) --
# truncated must mean "edges the walk WOULD have processed were actually cut off",
# not merely "a node happened to sit exactly at the depth cap".


def _chain_store(hops: int) -> FakeStore:
    """n0 -CALLS-> n1 -CALLS-> ... -CALLS-> n{hops}: an exactly-hops-long chain."""
    store = FakeStore()
    for i in range(hops + 1):
        store.add_node(f"n{i}", service="s", kind="Function")
    for i in range(hops):
        store.add_edge(f"n{i}", "CALLS", f"n{i + 1}")
    return store


def test_complete_15_hop_chain_is_not_truncated():
    # Reviewer's exact probe: the last node of a COMPLETE 15-hop chain sits exactly
    # AT the depth cap -- it has nothing further to walk, so nothing was cut off.
    store = _chain_store(15)
    result = traverse.trace_process(store, "n0", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert len(seg["steps"]) == 15  # every edge of the chain IS in the output
    assert seg["truncated"] is False
    assert result["truncated"] is False


def test_16_hop_chain_is_truncated_and_walk_stops_at_the_cap():
    store = _chain_store(16)
    result = traverse.trace_process(store, "n0", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert len(seg["steps"]) == 15  # the n15->n16 edge was NOT walked...
    assert {s["node"]["id"] for s in seg["steps"]} == {f"n{i}" for i in range(1, 16)}
    assert seg["truncated"] is True  # ...and the flag says so
    assert result["truncated"] is True


def test_capped_node_with_only_subthreshold_edge_is_not_truncated():
    # The peek respects min_confidence: an edge below the floor would have been
    # dropped by the walk anyway (cap or no cap) -- it can't be "cut off".
    store = _chain_store(15)
    store.add_node("weak", service="s", kind="Function")
    store.add_edge("n15", "CALLS", "weak", confidence=0.1)
    result = traverse.trace_process(store, "n0", max_segments=12, min_confidence=0.3)
    assert result["segments"][0]["truncated"] is False


def test_capped_node_with_exit_edge_is_truncated():
    # An exit (PRODUCES/CALLS_HTTP) cut off at the cap is a missing channel -- and
    # potentially a whole missing next segment -- as much a truncation as a missing
    # step: the peek uses the walk's full edge set, not just the intra types.
    store = _chain_store(15)
    store.add_node("chan:event_type:E", kind="Channel", channel_kind="event_type")
    store.add_edge("n15", "PRODUCES", "chan:event_type:E")
    result = traverse.trace_process(store, "n0", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert seg["exits"] == []  # the exit itself is NOT recorded (cap stopped the walk)...
    assert seg["truncated"] is True  # ...which is exactly why the flag must be honest


# -- determinism: repeated calls over the same store produce identical output --


def test_trace_process_is_deterministic_across_repeated_calls():
    store = _three_segment_store()
    first = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    second = traverse.trace_process(store, "create_order", max_segments=12, min_confidence=0.3)
    assert first == second


# ============================ M5 T5: compact ==============================
# pilot §7.3: a single-service repo's trace dumps a flat, undifferentiated
# segment (the real pilot saw 80 steps) -- trace_process(compact=True, the new
# default) post-processes an ALREADY-BUILT segment's steps (BFS walk itself is
# untouched): a segment with MORE than 15 steps gets its maximal runs of
# "boring" (role-free/non-branching/non-exit-producing) CONSECUTIVE steps
# collapsed to their first 3 + a `{"collapsed": N}` marker + their last 2.
#
# `_compact_steps` is tested DIRECTLY (not only through trace_process) on
# hand-built ("synthetic", per the brief's own Step 1 wording) steps/parents --
# _walk_segment's own pre-existing _SEGMENT_MAX_DEPTH=15 cap makes a genuine
# SINGLE-PATH chain longer than 15 hops structurally unreachable through a real
# BFS walk (any one root-to-leaf path is capped at 15 hops = 15 steps), so the
# brief's own "40 линейных шагов" scenario can only be exercised by hand-
# building the (steps, step_parents) pair the walk would have produced, exactly
# as if 40 hops nothing lay downstream of a 15-deep cap -- see
# test_trace_process_compact_* below for the real-walk wiring/gate proof
# instead (using a branching topology that legitimately exceeds 15 TOTAL steps
# without any single path exceeding the depth cap).


def _step(node_id: str, roles: list[str] | None = None) -> dict:
    node = {"id": node_id, "name": node_id}
    if roles:
        node["roles"] = roles
    return {"edge_type": "CALLS", "props": {}, "node": node, "direction": "out"}


def _chain_steps(
    ids_: list[str], roles_by_id: dict[str, list[str]] | None = None
) -> tuple[list[dict], list[str]]:
    """A synthetic (steps, step_parents) pair for a straight chain: each id's step
    is CALLS-reached from the PREVIOUS id (or "entry" for the first) -- every node
    has exactly one outgoing step within the segment (never a branch point) unless
    the test itself appends more."""
    roles_by_id = roles_by_id or {}
    steps = [_step(i, roles=roles_by_id.get(i)) for i in ids_]
    parents = ["entry", *ids_[:-1]]
    return steps, parents


# -- _compact_steps: gate boundary (<=15 untouched, byte-identical) --


def test_compact_steps_segment_at_gate_boundary_15_is_returned_unchanged():
    steps, parents = _chain_steps([f"n{i}" for i in range(1, 16)])  # exactly 15
    result = traverse._compact_steps(steps, parents, set())
    assert result is steps  # no-op short-circuit -- same list object, not a copy


def test_compact_steps_segment_of_10_is_untouched():
    steps, parents = _chain_steps([f"n{i}" for i in range(1, 11)])
    result = traverse._compact_steps(steps, parents, set())
    assert result is steps
    assert [s["node"]["id"] for s in result] == [f"n{i}" for i in range(1, 11)]


# -- _compact_steps: collapsing a long boring run --


def test_compact_steps_40_linear_steps_collapse_to_head_marker_tail():
    ids_ = [f"n{i}" for i in range(1, 41)]
    steps, parents = _chain_steps(ids_)
    result = traverse._compact_steps(steps, parents, set())
    assert len(result) == 6  # 3 head + 1 marker + 2 tail
    assert [s["node"]["id"] for s in result[:3]] == ["n1", "n2", "n3"]
    assert result[3] == {"collapsed": 35}  # 40 - 3 - 2
    assert [s["node"]["id"] for s in result[-2:]] == ["n39", "n40"]


def test_compact_steps_16_steps_just_above_gate_collapses():
    ids_ = [f"n{i}" for i in range(1, 17)]
    steps, parents = _chain_steps(ids_)
    result = traverse._compact_steps(steps, parents, set())
    assert len(result) == 6
    assert result[3] == {"collapsed": 11}  # 16 - 3 - 2


def test_compact_steps_run_of_exactly_head_plus_tail_is_left_as_is():
    # a run of HEAD(3)+TAIL(2)=5 or fewer: collapsing would show >= as many
    # entries as leaving it alone (3 + marker + 2 = 6 > 5) -- never collapsed,
    # even though the SEGMENT total (20) is well past the >15 gate.
    ids_ = [f"n{i}" for i in range(1, 21)]
    steps, parents = _chain_steps(ids_)
    # r1 splits the 20-chain into a leading run of 5 (n1..n5) and a trailing run
    # of 14 (n6..n19) -- the leading run must survive verbatim.
    roles_by_id = {"n6": ["RouteHandler"]}
    steps, parents = _chain_steps(ids_, roles_by_id=roles_by_id)
    result = traverse._compact_steps(steps, parents, set())
    assert [s["node"]["id"] for s in result[:5]] == ["n1", "n2", "n3", "n4", "n5"]
    assert "collapsed" not in result[4]
    assert result[5]["node"]["id"] == "n6"  # the role step, untouched, right after


def test_compact_steps_run_of_6_single_interior_is_left_uncollapsed():
    # M5 T5 review fix (Important): collapsing a run of exactly HEAD+TAIL+1 (=6)
    # would display 3 + marker + 2 = 6 entries -- ZERO display savings -- while
    # still destroying one real step's identity. Break-even is excluded: only a
    # run with >= 2 interior steps to hide ever collapses.
    ids_ = [f"n{i}" for i in range(1, 21)]  # segment total 20 > the 15 gate
    steps, parents = _chain_steps(ids_, roles_by_id={"n7": ["RouteHandler"]})
    result = traverse._compact_steps(steps, parents, set())
    # leading run n1..n6 (exactly 6) survives verbatim, then the n7 role step
    assert [s.get("node", {}).get("id") for s in result[:7]] == [f"n{i}" for i in range(1, 8)]
    assert all("collapsed" not in s for s in result[:7])


def test_compact_steps_run_of_7_two_interior_collapses_marker_shows_2():
    # The other side of the same boundary (review sweep: break-even at run>=7):
    # run=7 (interior 2) is the SMALLEST run that genuinely shrinks the display
    # (6 entries shown for 7 steps) -- pins against overcorrecting the gate.
    ids_ = [f"n{i}" for i in range(1, 21)]
    steps, parents = _chain_steps(ids_, roles_by_id={"n8": ["RouteHandler"]})
    result = traverse._compact_steps(steps, parents, set())
    assert [s["node"]["id"] for s in result[:3]] == ["n1", "n2", "n3"]
    assert result[3] == {"collapsed": 2}
    assert [s["node"]["id"] for s in result[4:6]] == ["n6", "n7"]
    assert result[6]["node"]["id"] == "n8"  # the role step, right after the run


# -- _compact_steps: role-bearing step breaks the run in two --


def test_compact_steps_role_bearing_step_in_the_middle_breaks_the_collapse():
    ids_ = [f"n{i}" for i in range(1, 41)]
    steps, parents = _chain_steps(ids_, roles_by_id={"n20": ["MessageConsumer"]})
    result = traverse._compact_steps(steps, parents, set())
    markers = [s["collapsed"] for s in result if "collapsed" in s]
    assert markers == [14, 15]  # run n1..n19 (19 -> interior 14), n21..n40 (20 -> interior 15)
    role_step = next(s for s in result if s.get("node", {}).get("id") == "n20")
    assert role_step["node"]["roles"] == ["MessageConsumer"]
    assert "collapsed" not in role_step


@pytest.mark.parametrize("role", sorted(schema.ROLE_KINDS))
def test_compact_steps_every_canonical_role_kind_is_protected(role):
    # core.schema.ROLE_KINDS is exactly the brief's own enumerated role list
    # (RouteHandler/MessageConsumer/MessageProducer/TemporalWorkflow/
    # TemporalActivity) -- reused here, not re-typed, so the two can't drift.
    ids_ = [f"n{i}" for i in range(1, 41)]
    steps, parents = _chain_steps(ids_, roles_by_id={"n20": [role]})
    result = traverse._compact_steps(steps, parents, set())
    protected = next(s for s in result if s.get("node", {}).get("id") == "n20")
    assert "collapsed" not in protected


# -- _compact_steps: a branch point (>1 outgoing step in this segment) never collapses --


def test_compact_steps_branching_step_never_collapses():
    ids_ = [f"n{i}" for i in range(1, 41)]
    steps, parents = _chain_steps(ids_)
    steps.append(_step("branch-leaf"))
    parents.append("n20")  # n20 now has TWO outgoing steps here (n21 AND branch-leaf)
    result = traverse._compact_steps(steps, parents, set())
    n20_hits = [s for s in result if s.get("node", {}).get("id") == "n20"]
    assert len(n20_hits) == 1
    assert "collapsed" not in n20_hits[0]


# -- _compact_steps: an exit-producing step never collapses --


def test_compact_steps_exit_producer_step_never_collapses():
    ids_ = [f"n{i}" for i in range(1, 41)]
    steps, parents = _chain_steps(ids_)
    result = traverse._compact_steps(steps, parents, {"n20"})
    n20_hits = [s for s in result if s.get("node", {}).get("id") == "n20"]
    assert len(n20_hits) == 1
    assert "collapsed" not in n20_hits[0]


# -- trace_process wiring: compact defaults True, --full-equivalent disables it,
# gated on a REAL BFS walk (branching topology, no single path over 15 hops) --


def _wide_chain_store(prefixes: list[str], depth: int) -> FakeStore:
    """entry branches into len(prefixes) SEPARATE linear chains, each exactly
    `depth` hops (<= _SEGMENT_MAX_DEPTH, so no single root-to-leaf path is itself
    depth-capped) -- the only store topology that can legitimately make a
    SEGMENT's total step count exceed 15 through the real walk (see this test
    module's own section docstring above)."""
    store = FakeStore()
    store.add_node("entry", service="s", kind="Function")
    for prefix in prefixes:
        prev = "entry"
        for i in range(1, depth + 1):
            node_id = f"{prefix}{i}"
            store.add_node(node_id, service="s", kind="Function")
            store.add_edge(prev, "CALLS", node_id)
            prev = node_id
    return store


def test_trace_process_compact_defaults_true_and_collapses_a_real_over_threshold_walk():
    store = _wide_chain_store(["a", "b"], 15)  # 2 * 15 = 30 steps total
    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    assert len(seg["steps"]) < 30
    markers = [s["collapsed"] for s in seg["steps"] if "collapsed" in s]
    assert markers  # at least one collapse happened
    visible = len([s for s in seg["steps"] if "collapsed" not in s])
    assert sum(markers) + visible == 30  # every original step accounted for


def test_trace_process_compact_false_disables_collapsing_over_threshold():
    store = _wide_chain_store(["a", "b"], 15)
    result = traverse.trace_process(
        store, "entry", max_segments=12, min_confidence=0.3, compact=False
    )
    seg = result["segments"][0]
    assert len(seg["steps"]) == 30
    assert all("collapsed" not in s for s in seg["steps"])


def test_trace_process_segment_at_or_under_gate_is_byte_identical_regardless_of_compact():
    store = _wide_chain_store(["a", "b"], 7)  # 14 steps total, <= 15 gate
    compact_result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    full_result = traverse.trace_process(
        store, "entry", max_segments=12, min_confidence=0.3, compact=False
    )
    assert compact_result == full_result


def test_trace_process_real_walk_keeps_exit_producer_step_visible_in_compact_mode():
    # M5 T5 review coverage pin: exit_producer_ids protection through a REAL BFS
    # walk (the unit-level test above hands _compact_steps a hand-built id set;
    # this one derives it from an actual PRODUCES edge in the store) -- a
    # mid-chain node that produces a channel must survive visible in an
    # otherwise-collapsed over-threshold segment.
    store = _wide_chain_store(["a", "b"], 15)  # 30 steps total, > the 15 gate
    store.add_node("chan:event_type:E", kind="Channel", channel_kind="event_type")
    store.add_edge("a8", "PRODUCES", "chan:event_type:E")

    result = traverse.trace_process(store, "entry", max_segments=12, min_confidence=0.3)
    seg = result["segments"][0]
    # non-vacuity: the walk really did record a8's exit (so a8 IS an exit producer)
    assert [ex["channel"]["id"] for ex in seg["exits"]] == ["chan:event_type:E"]
    visible_ids = {s["node"]["id"] for s in seg["steps"] if "collapsed" not in s}
    assert "a8" in visible_ids
    markers = [s["collapsed"] for s in seg["steps"] if "collapsed" in s]
    assert markers  # collapsing still happened around the protected step
    assert sum(markers) + len(visible_ids) == 30  # every original step accounted for


# ============================== find_paths ==============================


def test_find_paths_finds_path_through_next_segment_edge():
    store = _three_segment_store()
    result = traverse.find_paths(
        store, "create_order", "handle_order_created", max_hops=8, edge_types=None
    )
    path = result["path"]
    assert path is not None
    node_ids = [step["node"]["id"] for step in path]
    assert node_ids[0] == "create_order"
    assert node_ids[-1] == "handle_order_created"
    assert path[0]["edge_type"] is None
    assert path[0]["direction"] is None
    assert path[-1]["edge_type"] == "NEXT_SEGMENT"


def test_find_paths_returns_shortest_path_by_hop_count():
    store = FakeStore()
    store.add_node("a", kind="Function")
    store.add_node("b", kind="Function")
    store.add_node("c", kind="Function")
    store.add_node("d", kind="Function")
    store.add_edge("a", "CALLS", "d")  # direct, 1 hop
    store.add_edge("a", "CALLS", "b")
    store.add_edge("b", "CALLS", "c")
    store.add_edge("c", "CALLS", "d")  # longer, 3 hops

    result = traverse.find_paths(store, "a", "d", max_hops=8, edge_types=None)
    node_ids = [step["node"]["id"] for step in result["path"]]
    assert node_ids == ["a", "d"]


def test_find_paths_deterministic_tie_break_between_equal_length_paths():
    # T8 review fast-follow: two equal-length paths a->b->d and a->c->d -- the hop
    # sorting (edge_type, neighbor id) must pick the SAME winner regardless of the
    # store's own row order (FalkorDB row order is not contractually stable; the
    # fake store returns edges in insertion order, so the reversed-insertion store
    # below would win with a->c->d if BFS took hops unsorted).
    def diamond(edge_order):
        store = FakeStore()
        for n in "abcd":
            store.add_node(n, kind="Function")
        for src, dst in edge_order:
            store.add_edge(src, "CALLS", dst)
        return store

    forward = diamond([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")])
    reversed_ = diamond([("a", "c"), ("a", "b"), ("c", "d"), ("b", "d")])

    path_fwd = traverse.find_paths(forward, "a", "d", max_hops=8, edge_types=None)["path"]
    path_rev = traverse.find_paths(reversed_, "a", "d", max_hops=8, edge_types=None)["path"]
    assert [s["node"]["id"] for s in path_fwd] == ["a", "b", "d"]
    assert [s["node"]["id"] for s in path_rev] == ["a", "b", "d"]


def test_find_paths_not_found_returns_null_path():
    store = FakeStore()
    store.add_node("a", kind="Function")
    store.add_node("b", kind="Function")  # no edge between them
    result = traverse.find_paths(store, "a", "b", max_hops=8, edge_types=None)
    assert result == {"path": None}


def test_find_paths_missing_from_id_returns_null_path():
    store = FakeStore()
    result = traverse.find_paths(store, "ghost", "also-ghost", max_hops=8, edge_types=None)
    assert result == {"path": None}


def test_find_paths_same_node_returns_trivial_single_node_path():
    store = FakeStore()
    store.add_node("a", kind="Function")
    result = traverse.find_paths(store, "a", "a", max_hops=8, edge_types=None)
    assert result["path"] == [{"node": store.nodes["a"], "edge_type": None, "direction": None}]


def test_find_paths_respects_edge_types_filter():
    store = FakeStore()
    store.add_node("a", kind="Function")
    store.add_node("b", kind="Function")
    store.add_edge("a", "CONTAINS", "b")
    result = traverse.find_paths(store, "a", "b", max_hops=8, edge_types=["CALLS"])
    assert result == {"path": None}
    result_unfiltered = traverse.find_paths(store, "a", "b", max_hops=8, edge_types=None)
    assert result_unfiltered["path"] is not None


def test_find_paths_direction_field_reflects_true_hop_direction():
    # a -CALLS-> b: walking FROM b (both-direction expansion) reaches a via an
    # "in" hop -- direction must reflect that, not a hardcoded "out".
    store = FakeStore()
    store.add_node("a", kind="Function")
    store.add_node("b", kind="Function")
    store.add_edge("a", "CALLS", "b")
    result = traverse.find_paths(store, "b", "a", max_hops=8, edge_types=None)
    assert result["path"][-1]["direction"] == "in"
    assert result["path"][-1]["edge_type"] == "CALLS"


def test_find_paths_max_hops_limits_search_depth():
    store = FakeStore()
    names = [f"n{i}" for i in range(6)]
    for n in names:
        store.add_node(n, kind="Function")
    for i in range(len(names) - 1):
        store.add_edge(names[i], "CALLS", names[i + 1])  # n0->n1->...->n5, 5 hops

    assert traverse.find_paths(store, "n0", "n5", max_hops=5, edge_types=None)["path"] is not None
    assert traverse.find_paths(store, "n0", "n5", max_hops=4, edge_types=None) == {"path": None}
