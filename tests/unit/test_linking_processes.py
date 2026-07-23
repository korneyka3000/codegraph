"""M2 T7: linking.processes.materialize -- BusinessProcess anchors from cfg.processes
(HTTP-route selector or qualified-name selector) plus one auto-anchor per staged
TemporalWorkflow-role node; PART_OF_PROCESS traces each anchor's reachable segment
entries via a lightweight BFS over the (already-derived) NEXT_SEGMENT edges, order 0..N,
order 0 always being the anchor's own entrypoint."""

from __future__ import annotations

from codegraph.config.models import ProcessDecl, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking import processes
from codegraph.stores.staging import Staging


def _cfg(*decls: ProcessDecl) -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="g", services=[ServiceConfig(name="svc", path=__file__)],
        processes=list(decls),
    )


def _fn(id_: str, service: str, name: str, qualified_name: str, roles: tuple = ()) -> NodeRec:
    return NodeRec(id=id_, kind="Function", service=service, name=name,
                    qualified_name=qualified_name, roles=roles)


def _edge(src, dst, type_, resolution="static", confidence=1.0, **props) -> EdgeRec:
    return EdgeRec(src=src, dst=dst, type=type_, resolution=resolution,
                   confidence=confidence, extractor="test", props=props)


# -- config selector: http-route form --


def test_http_route_selector_resolves_channel_handles_to_handler(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    handler = _fn("sym:orders-api:create_order", "orders-api", "create_order",
                   "app.routes.orders.create_order")
    st.upsert_nodes([chan, handler])
    st.upsert_edges([_edge(chan.id, handler.id, "HANDLES")])

    cfg = _cfg(ProcessDecl(name="Order KYC onboarding", entrypoint="orders-api:POST /orders"))
    stats = processes.materialize(cfg, st)

    assert stats["processes"] == 1
    procs = [n for n in st.iter_nodes() if n.kind == "BusinessProcess"]
    assert len(procs) == 1
    proc = procs[0]
    assert proc.id == "proc:order-kyc-onboarding"
    assert proc.name == "Order KYC onboarding"
    assert proc.props["entrypoint_id"] == handler.id
    assert proc.props["source"] == "config"

    part_of = [e for e in st.iter_edges() if e.type == "PART_OF_PROCESS"]
    assert len(part_of) == 1
    e = part_of[0]
    assert (e.src, e.dst) == (handler.id, proc.id)
    assert e.props["order"] == 0
    assert e.resolution == "static" and e.confidence == 1.0


def test_http_route_selector_unresolved_route_is_skipped(tmp_path):
    st = Staging(tmp_path / "s.db")
    cfg = _cfg(ProcessDecl(name="Nope", entrypoint="orders-api:POST /missing"))
    stats = processes.materialize(cfg, st)
    assert stats["processes"] == 0
    assert stats["processes_unresolved"] == 1
    assert list(st.iter_nodes()) == []


# -- config selector: qualified form --


def test_qualified_selector_resolves_via_service_and_qualified_name(tmp_path):
    st = Staging(tmp_path / "s.db")
    workflow = NodeRec(id="sym:kyc-worker:KycWorkflow", kind="Class", service="kyc-worker",
                        name="KycWorkflow", qualified_name="app.workflows.kyc.KycWorkflow")
    st.upsert_nodes([workflow])

    cfg = _cfg(ProcessDecl(name="KYC flow", entrypoint="kyc-worker:app.workflows.kyc.KycWorkflow"))
    stats = processes.materialize(cfg, st)

    assert stats["processes"] == 1
    proc = next(n for n in st.iter_nodes() if n.kind == "BusinessProcess")
    assert proc.props["entrypoint_id"] == workflow.id
    assert proc.props["source"] == "config"


def test_qualified_selector_unresolved_name_is_skipped(tmp_path):
    st = Staging(tmp_path / "s.db")
    cfg = _cfg(ProcessDecl(name="Nope", entrypoint="kyc-worker:app.nope.Nothing"))
    stats = processes.materialize(cfg, st)
    assert stats["processes"] == 0
    assert stats["processes_unresolved"] == 1


# -- auto temporal anchors --


def test_temporal_workflow_role_gets_auto_anchor(tmp_path):
    st = Staging(tmp_path / "s.db")
    workflow = NodeRec(id="sym:kyc-worker:KycWorkflow", kind="Class", service="kyc-worker",
                        name="KycWorkflow", qualified_name="app.workflows.kyc.KycWorkflow",
                        roles=("TemporalWorkflow",))
    st.upsert_nodes([workflow])

    stats = processes.materialize(_cfg(), st)

    assert stats["processes"] == 1
    proc = next(n for n in st.iter_nodes() if n.kind == "BusinessProcess")
    assert proc.props["entrypoint_id"] == workflow.id
    assert proc.props["source"] == "temporal"
    # service-qualified slug -- avoids collisions between same-named workflow classes
    # in different services (see processes.py docstring).
    assert proc.id == "proc:kyc-worker-kycworkflow"


def test_no_temporal_workflow_roles_no_auto_anchor(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn("sym:a:f", "a", "f", "m.f")])  # no roles
    stats = processes.materialize(_cfg(), st)
    assert stats["processes"] == 0


def test_config_and_temporal_anchors_coexist(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    handler = _fn("sym:orders-api:create_order", "orders-api", "create_order", "q.create_order")
    workflow = NodeRec(id="sym:kyc-worker:KycWorkflow", kind="Class", service="kyc-worker",
                        name="KycWorkflow", qualified_name="app.workflows.kyc.KycWorkflow",
                        roles=("TemporalWorkflow",))
    st.upsert_nodes([chan, handler, workflow])
    st.upsert_edges([_edge(chan.id, handler.id, "HANDLES")])

    cfg = _cfg(ProcessDecl(name="Order KYC onboarding", entrypoint="orders-api:POST /orders"))
    stats = processes.materialize(cfg, st)
    assert stats["processes"] == 2


# -- PART_OF_PROCESS: BFS over NEXT_SEGMENT, order 0..N --


def test_part_of_process_bfs_assigns_increasing_order(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry")
    mid = _fn("sym:kyc-worker:mid", "kyc-worker", "mid", "q.mid")
    tail = _fn("sym:doc-mgmt:tail", "doc-mgmt", "tail", "q.tail")
    st.upsert_nodes([chan, entry, mid, tail])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(entry.id, mid.id, "NEXT_SEGMENT", resolution="heuristic", confidence=0.42,
              via_channel_id="chan:x", derived=True),
        _edge(mid.id, tail.id, "NEXT_SEGMENT", resolution="static", confidence=0.5,
              via_channel_id="chan:y", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Order KYC onboarding", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    proc = next(n for n in st.iter_nodes() if n.kind == "BusinessProcess")
    part_of = {e.src: e for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert set(part_of) == {entry.id, mid.id, tail.id}
    assert part_of[entry.id].props["order"] == 0
    assert part_of[entry.id].resolution == "static" and part_of[entry.id].confidence == 1.0
    assert part_of[mid.id].props["order"] == 1
    assert part_of[mid.id].resolution == "heuristic"
    assert abs(part_of[mid.id].confidence - 0.42) < 1e-9
    assert part_of[tail.id].props["order"] == 2
    assert part_of[tail.id].resolution == "static"
    assert abs(part_of[tail.id].confidence - 0.5) < 1e-9
    assert all(e.dst == proc.id for e in part_of.values())


def test_part_of_process_bfs_cycle_safe(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry")
    a = _fn("sym:kyc-worker:a", "kyc-worker", "a", "q.a")
    st.upsert_nodes([chan, entry, a])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(entry.id, a.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
        _edge(a.id, entry.id, "NEXT_SEGMENT", via_channel_id="chan:y", derived=True),  # cycle
    ])

    cfg = _cfg(ProcessDecl(name="Order KYC onboarding", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)  # must terminate

    part_of = {e.src: e.props["order"] for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert part_of == {entry.id: 0, a.id: 1}


def test_part_of_process_bfs_fan_out_diamond_reconvergence_is_deterministic(tmp_path):
    """Regression pin (M2 final review, item 6): a node with 2 OUTGOING NEXT_SEGMENT
    edges (fan-out: entry -> mid_a, entry -> mid_b) whose branches reconverge on the
    SAME tail node (diamond: mid_a -> tail, mid_b -> tail) must --
      (a) assign BOTH fan-out children the SAME order (parent order + 1) -- order comes
          from the PARENT's already-fixed order, not a running counter, so this holds
          regardless of which branch is visited first within their shared BFS layer;
      (b) produce exactly ONE PART_OF_PROCESS entry for the reconverging tail, not two
          and not zero ("if edge.dst not in seen" claims a node exactly once);
      (c) do so DETERMINISTICALLY across runs: _next_segment_adjacency sorts each
          node's outgoing edges by dst id, so whichever branch's id sorts first always
          "wins" the tail's recorded via_edge (and therefore its own resolution/
          confidence) -- pinned here by giving the two branch->tail edges
          distinguishable confidence values and asserting the lexicographically-first
          branch (mid_a < mid_b) is the one recorded, every time.
    """
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry")
    mid_a = _fn("sym:kyc-worker:mid_a", "kyc-worker", "mid_a", "q.mid_a")
    mid_b = _fn("sym:kyc-worker:mid_b", "kyc-worker", "mid_b", "q.mid_b")
    tail = _fn("sym:doc-mgmt:tail", "doc-mgmt", "tail", "q.tail")
    assert mid_a.id < mid_b.id  # load-bearing for the determinism claim below
    st.upsert_nodes([chan, entry, mid_a, mid_b, tail])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(entry.id, mid_a.id, "NEXT_SEGMENT", via_channel_id="chan:entry-a", derived=True),
        _edge(entry.id, mid_b.id, "NEXT_SEGMENT", via_channel_id="chan:entry-b", derived=True),
        _edge(mid_a.id, tail.id, "NEXT_SEGMENT", resolution="static", confidence=0.9,
              via_channel_id="chan:from-a", derived=True),
        _edge(mid_b.id, tail.id, "NEXT_SEGMENT", resolution="heuristic", confidence=0.1,
              via_channel_id="chan:from-b", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Fan-out flow", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    part_of = {e.src: e for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert set(part_of) == {entry.id, mid_a.id, mid_b.id, tail.id}  # tail: exactly once

    assert part_of[entry.id].props["order"] == 0
    assert part_of[mid_a.id].props["order"] == 1
    assert part_of[mid_b.id].props["order"] == 1

    assert part_of[tail.id].props["order"] == 2
    # deterministic reconvergence winner: mid_a's edge to tail, not mid_b's.
    assert part_of[tail.id].resolution == "static"
    assert abs(part_of[tail.id].confidence - 0.9) < 1e-9


def test_part_of_process_entry_with_no_next_segment_is_order_zero_only(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry")
    st.upsert_nodes([chan, entry])
    st.upsert_edges([_edge(chan.id, entry.id, "HANDLES")])

    cfg = _cfg(ProcessDecl(name="Solo", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    part_of = [e for e in st.iter_edges() if e.type == "PART_OF_PROCESS"]
    assert len(part_of) == 1
    assert part_of[0].src == entry.id and part_of[0].props["order"] == 0


# -- no-op --


def test_no_processes_configured_and_no_temporal_roles_is_noop(tmp_path):
    st = Staging(tmp_path / "s.db")
    stats = processes.materialize(_cfg(), st)
    assert stats["processes"] == 0
    assert list(st.iter_nodes()) == []
    assert list(st.iter_edges()) == []


# -- resolve_selector: public wrapper reused by CLI `trace` (M2 T8) -- same
# parser/resolution logic materialize() uses internally, exposed standalone so
# cli.py doesn't reimplement the "<service>:<METHOD> <path>" / "<service>:qualified"
# selector grammar a second time (see linking/processes.py module docstring and
# cli.py's `trace` command). --


def test_resolve_selector_http_route_form(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    handler = _fn("sym:orders-api:create_order", "orders-api", "create_order",
                   "app.routes.orders.create_order")
    st.upsert_nodes([chan, handler])
    st.upsert_edges([_edge(chan.id, handler.id, "HANDLES")])

    assert processes.resolve_selector(st, "orders-api:POST /orders") == handler.id


def test_resolve_selector_qualified_form(tmp_path):
    st = Staging(tmp_path / "s.db")
    workflow = NodeRec(id="sym:kyc-worker:KycWorkflow", kind="Class", service="kyc-worker",
                        name="KycWorkflow", qualified_name="app.workflows.kyc.KycWorkflow")
    st.upsert_nodes([workflow])

    assert (
        processes.resolve_selector(st, "kyc-worker:app.workflows.kyc.KycWorkflow")
        == workflow.id
    )


def test_resolve_selector_unresolved_returns_none(tmp_path):
    st = Staging(tmp_path / "s.db")
    assert processes.resolve_selector(st, "orders-api:POST /missing") is None
    assert processes.resolve_selector(st, "kyc-worker:app.nope.Nothing") is None


def test_resolve_selector_malformed_selector_without_colon_returns_none(tmp_path):
    st = Staging(tmp_path / "s.db")
    assert processes.resolve_selector(st, "not-a-selector") is None


# -- _entry_of: climb reverse-intra-edges (CALLS/DEPENDS_ON/INVOKES_ACTIVITY) up to a
# RouteHandler/MessageConsumer/TemporalWorkflow-tagged node (+ TemporalSignalHandler,
# M7 T4), or a node with no incoming intra edge at all (M3 T2) --


def test_entry_of_returns_node_itself_when_it_has_no_incoming_intra_edges():
    assert processes._entry_of("lonely", {}, {}, set()) == "lonely"


def test_entry_of_returns_node_itself_when_it_already_carries_an_entry_role():
    roles_by_id = {"handler": ("RouteHandler",)}
    # even though "handler" HAS a predecessor, the role check short-circuits the climb.
    intra_reverse_adj = {"handler": ["caller"]}
    assert processes._entry_of("handler", intra_reverse_adj, roles_by_id, set()) == "handler"


def test_entry_of_climbs_single_hop_to_role_bearing_predecessor():
    intra_reverse_adj = {"leaf": ["entry"]}
    roles_by_id = {"entry": ("RouteHandler",)}
    assert processes._entry_of("leaf", intra_reverse_adj, roles_by_id, set()) == "entry"


def test_entry_of_climbs_multiple_hops_through_role_less_intermediates():
    # leaf <- mid <- mid2 <- entry(MessageConsumer) -- mirrors the KYC fixture's real
    # shape (client method <- verify_documents <- KycWorkflow.run <- handle_order_created).
    intra_reverse_adj = {"leaf": ["mid"], "mid": ["mid2"], "mid2": ["entry"]}
    roles_by_id = {"entry": ("MessageConsumer",)}
    assert processes._entry_of("leaf", intra_reverse_adj, roles_by_id, set()) == "entry"


def test_entry_of_stops_at_temporal_signal_handler_role():
    """M7 T4 (OPEN R3) review finding: a @workflow.signal/@workflow.update handler is
    an externally-invoked segment entry (Temporal server wakes it -- exactly like
    Kafka wakes a MessageConsumer or HTTP wakes a RouteHandler), so the climb must
    stop AT it even when it also has a local same-service caller. Without
    TemporalSignalHandler in _ENTRY_ROLES the climb walked PAST the handler to its
    caller -- misattributing PART_OF_PROCESS entries for the local-caller/relay
    pattern AND, worse, keying the handler's own onward NEXT_SEGMENT edges under the
    wrong entry so the signal->downstream chain broke in _trace_segments' BFS (the
    BFS reaches the handler as a dst but its outgoing edges sit under another key)."""
    roles_by_id = {"handler": ("TemporalSignalHandler",)}
    # the handler HAS a local caller -- the role check must short-circuit the climb
    # (the no-predecessor fallback alone would NOT cover this shape).
    intra_reverse_adj = {"handler": ["local_caller"]}
    assert processes._entry_of("handler", intra_reverse_adj, roles_by_id, set()) == "handler"


def test_entry_of_stops_at_first_node_with_no_predecessor_when_no_role_ever_found():
    intra_reverse_adj = {"leaf": ["root"]}  # "root" has no further predecessors, no role
    assert processes._entry_of("leaf", intra_reverse_adj, {}, set()) == "root"


def test_entry_of_cycle_terminates_via_visited_instead_of_looping_forever():
    intra_reverse_adj = {"a": ["b"], "b": ["a"]}  # a <-> b cycle, no role anywhere
    assert processes._entry_of("a", intra_reverse_adj, {}, set()) == "a"


def test_entry_of_ambiguous_predecessors_picks_deterministic_sorted_first():
    intra_reverse_adj = {"leaf": ["caller-b", "caller-a"]}
    ambiguous = [0]
    result = processes._entry_of("leaf", intra_reverse_adj, {}, set(), ambiguous)
    assert result == "caller-a"  # sorted first, regardless of input list order
    assert ambiguous[0] == 1


def test_entry_of_unambiguous_predecessor_does_not_bump_counter():
    intra_reverse_adj = {"leaf": ["only-caller"]}
    ambiguous = [0]
    processes._entry_of("leaf", intra_reverse_adj, {}, set(), ambiguous)
    assert ambiguous[0] == 0


def test_entry_of_ambiguous_counter_is_optional_and_defaults_to_no_tracking():
    intra_reverse_adj = {"leaf": ["caller-b", "caller-a"]}
    # no ambiguous arg passed -- must not raise, must still resolve deterministically.
    assert processes._entry_of("leaf", intra_reverse_adj, {}, set()) == "caller-a"


# -- _entry_graph: builds an entry->entry adjacency from NEXT_SEGMENT edges, keyed by
# the CLIMBED entry of each edge's src (not the raw src itself) -- (M3 T2) --


def test_entry_graph_keys_adjacency_by_climbed_entry_not_raw_next_segment_src(tmp_path):
    st = Staging(tmp_path / "s.db")
    entry = _fn("sym:a:entry", "a", "entry", "q.entry", roles=("RouteHandler",))
    mid = _fn("sym:a:mid", "a", "mid", "q.mid")
    producer = _fn("sym:a:producer", "a", "producer", "q.producer")
    consumer = _fn("sym:b:consumer", "b", "consumer", "q.consumer", roles=("MessageConsumer",))
    st.upsert_nodes([entry, mid, producer, consumer])
    st.upsert_edges([
        _edge(entry.id, mid.id, "CALLS"),
        _edge(mid.id, producer.id, "CALLS"),
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    entry_adj, ambiguous = processes._entry_graph(st)

    assert ambiguous == 0
    assert entry.id in entry_adj
    assert [e.dst for e in entry_adj[entry.id]] == [consumer.id]
    assert producer.id not in entry_adj  # raw NEXT_SEGMENT.src never appears as a key


def test_entry_graph_dst_is_used_as_is_never_climbed(tmp_path):
    """T7's own construction guarantee: NEXT_SEGMENT.dst is ALREADY a segment entry
    (a CONSUMES src or a HANDLES dst), so _entry_graph must never apply _entry_of to
    it -- even when dst itself has intra-edge predecessors that would otherwise climb
    it somewhere else entirely."""
    st = Staging(tmp_path / "s.db")
    producer = _fn("sym:a:producer", "a", "producer", "q.producer")
    consumer = _fn("sym:b:consumer", "b", "consumer", "q.consumer", roles=("MessageConsumer",))
    decoy_caller = _fn("sym:b:decoy", "b", "decoy", "q.decoy", roles=("RouteHandler",))
    st.upsert_nodes([producer, consumer, decoy_caller])
    st.upsert_edges([
        _edge(decoy_caller.id, consumer.id, "CALLS"),  # would climb consumer -> decoy_caller
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    entry_adj, _ = processes._entry_graph(st)

    assert [e.dst for e in entry_adj[producer.id]] == [consumer.id]  # untouched, not decoy_caller


def test_entry_graph_ambiguous_counts_propagate_from_entry_of(tmp_path):
    st = Staging(tmp_path / "s.db")
    producer = _fn("sym:a:producer", "a", "producer", "q.producer")
    caller_a = _fn("sym:a:caller-a", "a", "caller-a", "q.caller-a")
    caller_b = _fn("sym:a:caller-b", "a", "caller-b", "q.caller-b")
    consumer = _fn("sym:b:consumer", "b", "consumer", "q.consumer", roles=("MessageConsumer",))
    st.upsert_nodes([producer, caller_a, caller_b, consumer])
    st.upsert_edges([
        _edge(caller_a.id, producer.id, "CALLS"),
        _edge(caller_b.id, producer.id, "CALLS"),  # producer has TWO callers -- ambiguous
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    _, ambiguous = processes._entry_graph(st)

    assert ambiguous == 1


# -- PART_OF_PROCESS climbing through intra edges: synthetic real-shape regression
# anchor (M3 T2) -- mirrors the actual shape verified live against the fixtures'
# "Order KYC onboarding" chain with real scip-python (see
# tests/integration/test_processes_real_shape.py + m3-task-2-report.md): a
# NEXT_SEGMENT.src is a producer/client node buried 2 intra-CALLS hops under its
# segment's TRUE entry, not the entry itself. Runs in the default suite (no
# scip/falkordb marker) -- this is what actually executes in CI to prove the BFS is
# no longer inert (pre-fix: order was ALWAYS 0, full stop, on every real graph -- see
# the M2 final review finding this task starts from). --


def test_part_of_process_climbs_through_intra_edges_to_reach_max_order_two(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry", roles=("RouteHandler",))
    mid = _fn("sym:orders-api:mid", "orders-api", "mid", "q.mid")
    producer = _fn("sym:orders-api:producer", "orders-api", "producer", "q.producer")
    consumer = _fn("sym:kyc-worker:consumer", "kyc-worker", "consumer", "q.consumer",
                    roles=("MessageConsumer",))
    mid2 = _fn("sym:kyc-worker:mid2", "kyc-worker", "mid2", "q.mid2")
    producer2 = _fn("sym:kyc-worker:producer2", "kyc-worker", "producer2", "q.producer2")
    tail = _fn("sym:doc-mgmt:tail", "doc-mgmt", "tail", "q.tail", roles=("RouteHandler",))
    st.upsert_nodes([chan, entry, mid, producer, consumer, mid2, producer2, tail])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(entry.id, mid.id, "CALLS"),
        _edge(mid.id, producer.id, "CALLS"),
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
        _edge(consumer.id, mid2.id, "CALLS"),
        _edge(mid2.id, producer2.id, "INVOKES_ACTIVITY"),
        _edge(producer2.id, tail.id, "NEXT_SEGMENT", via_channel_id="chan:y", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Order KYC onboarding", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    part_of = {e.src: e.props["order"] for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert part_of == {entry.id: 0, consumer.id: 1, tail.id: 2}
    assert max(part_of.values()) == 2  # regression pin: pre-fix this was always 0


def test_part_of_process_climb_uses_depends_on_edges_too(tmp_path):
    """DEPENDS_ON (fastapi Depends()-injection edges) is one of the three intra-edge
    types _entry_of climbs over, alongside CALLS/INVOKES_ACTIVITY."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry", roles=("RouteHandler",))
    producer = _fn("sym:orders-api:producer", "orders-api", "producer", "q.producer")
    consumer = _fn("sym:kyc-worker:consumer", "kyc-worker", "consumer", "q.consumer",
                    roles=("MessageConsumer",))
    st.upsert_nodes([chan, entry, producer, consumer])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(entry.id, producer.id, "DEPENDS_ON"),
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Depends flow", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    part_of = {e.src: e.props["order"] for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert part_of == {entry.id: 0, consumer.id: 1}


def test_part_of_process_ambiguous_climb_counted_in_materialize_stats(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry", roles=("RouteHandler",))
    caller_a = _fn("sym:orders-api:caller-a", "orders-api", "caller-a", "q.caller-a")
    caller_b = _fn("sym:orders-api:caller-b", "orders-api", "caller-b", "q.caller-b")
    producer = _fn("sym:orders-api:producer", "orders-api", "producer", "q.producer")
    consumer = _fn("sym:kyc-worker:consumer", "kyc-worker", "consumer", "q.consumer",
                    roles=("MessageConsumer",))
    st.upsert_nodes([chan, entry, caller_a, caller_b, producer, consumer])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES"),
        _edge(caller_a.id, producer.id, "CALLS"),
        _edge(caller_b.id, producer.id, "CALLS"),  # ambiguous: 2 callers of "producer"
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Ambiguous flow", entrypoint="orders-api:POST /orders"))
    stats = processes.materialize(cfg, st)

    assert stats["part_of_process_ambiguous"] == 1


def test_part_of_process_safety_cap_stops_at_100_nodes(tmp_path):
    """Safety cap (M3 T2 brief: "максимум узлов 100"): a long NEXT_SEGMENT chain (no
    cycle -- BFS would otherwise legitimately keep growing order forever) must stop
    materializing PART_OF_PROCESS members once 100 nodes have been claimed, rather
    than growing without bound on a pathological/huge real graph."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:a:n0", "a", "n0", "q.n0", roles=("RouteHandler",))
    nodes = [entry]
    edges = [_edge(chan.id, entry.id, "HANDLES")]
    n = 150
    for i in range(1, n):
        node = _fn(f"sym:a:n{i}", "a", f"n{i}", f"q.n{i}", roles=("MessageConsumer",))
        nodes.append(node)
        prev_id = f"sym:a:n{i - 1}"
        edges.append(_edge(prev_id, node.id, "NEXT_SEGMENT", via_channel_id=f"chan:{i}",
                            derived=True))
    st.upsert_nodes([chan, *nodes])
    st.upsert_edges(edges)

    cfg = _cfg(ProcessDecl(name="Long chain", entrypoint="orders-api:POST /orders"))
    processes.materialize(cfg, st)

    part_of = [e for e in st.iter_edges() if e.type == "PART_OF_PROCESS"]
    assert len(part_of) <= 100
