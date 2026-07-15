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
