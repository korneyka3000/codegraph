"""M2 T7: linking.workspace.link_workspace -- the S7 orchestrator: clear_workspace_layer
-> temporal_start marking -> http_routes.link -> segments.derive -> processes.materialize,
in that exact order (each stage's INPUT depends on the previous stage's OUTPUT -- see the
order-dependency test below, which fails if any two stages are swapped)."""

from __future__ import annotations

from codegraph.config.models import ProcessDecl, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking.workspace import link_workspace
from codegraph.stores.staging import Staging


def _cfg(*decls: ProcessDecl) -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="g", services=[ServiceConfig(name="svc", path=__file__)],
        processes=list(decls),
    )


def _fn(id_: str, service: str, name: str, qualified_name: str) -> NodeRec:
    return NodeRec(id=id_, kind="Function", service=service, name=name,
                    qualified_name=qualified_name)


def _edge(
    src, dst, type_, resolution="static", confidence=1.0, extractor="test", **props
) -> EdgeRec:
    return EdgeRec(src=src, dst=dst, type=type_, resolution=resolution,
                   confidence=confidence, extractor=extractor, props=props)


# -- temporal_start_mark: create-vs-update --


def test_temporal_start_mark_creates_calls_edge_when_absent(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn("sym:a:src", "a", "src", "q.src"), _fn("sym:a:dst", "a", "dst", "q.dst")])
    st.add_claims("a", "app/x.py", "temporal_start_mark",
                  [{"src_id": "sym:a:src", "dst_id": "sym:a:dst", "evidence_line": 7}])

    link_workspace(_cfg(), st)

    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(calls) == 1
    e = calls[0]
    assert (e.src, e.dst) == ("sym:a:src", "sym:a:dst")
    assert e.resolution == "dynamic"
    assert e.confidence == 0.9
    assert e.extractor == "linking"
    assert e.props == {"mechanism": "temporal_start"}
    assert e.evidence_file == "app/x.py"
    assert e.evidence_line == 7


def test_temporal_start_mark_updates_existing_calls_edge_instead_of_duplicating(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([_edge("sym:a:src", "sym:a:dst", "CALLS", resolution="static",
                            confidence=1.0, extractor="calls", callsite_count=1)])
    st.add_claims("a", "app/x.py", "temporal_start_mark",
                  [{"src_id": "sym:a:src", "dst_id": "sym:a:dst", "evidence_line": 7}])

    link_workspace(_cfg(), st)

    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(calls) == 1  # updated in place, not duplicated
    e = calls[0]
    # a pre-existing (e.g. future static-path) CALLS edge is TAGGED, not replaced: its
    # own resolution/confidence/extractor survive, only props gain the mechanism tag.
    assert e.resolution == "static" and e.confidence == 1.0 and e.extractor == "calls"
    assert e.props == {"callsite_count": 1, "mechanism": "temporal_start"}


def test_marks_counter_counts_claims_processed(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "x.py", "temporal_start_mark",
                  [{"src_id": "sym:a:1", "dst_id": "sym:a:2", "evidence_line": 1}])
    st.add_claims("b", "y.py", "temporal_start_mark",
                  [{"src_id": "sym:b:1", "dst_id": "sym:b:2", "evidence_line": 2}])
    report = link_workspace(_cfg(), st)
    assert report["marks"] == 2


def test_no_temporal_start_claims_marks_zero_and_no_calls_edges(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = link_workspace(_cfg(), st)
    assert report["marks"] == 0
    assert not any(e.type == "CALLS" for e in st.iter_edges())


# -- clear runs first: proven by a differentiating props scenario --


def test_clear_workspace_layer_runs_before_temporal_marking(tmp_path):
    """A stale extractor="linking" CALLS edge (as if left over from a PRIOR
    link_workspace run) carries an extra prop that must NOT survive: if clear ran AFTER
    marking (wrong order), update_edge_props would merge onto the stale edge and then
    clear would delete it outright (extractor="linking" either way) -- final state would
    have NO edge at all. If clear runs FIRST (correct), the stale edge is wiped, then
    marking re-creates a fresh one with ONLY the mechanism prop. Observing the edge
    PRESENT with exactly {"mechanism": "temporal_start"} therefore pins the order.
    """
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([_edge("sym:a:src", "sym:a:dst", "CALLS", resolution="dynamic",
                            confidence=0.9, extractor="linking",
                            mechanism="temporal_start", stale_marker=True)])
    st.add_claims("a", "app/x.py", "temporal_start_mark",
                  [{"src_id": "sym:a:src", "dst_id": "sym:a:dst", "evidence_line": 1}])

    link_workspace(_cfg(), st)

    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(calls) == 1
    assert calls[0].props == {"mechanism": "temporal_start"}  # no stale_marker leaked through


def test_clear_workspace_layer_removes_stale_business_process(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([NodeRec(id="proc:stale", kind="BusinessProcess", service="",
                              name="Stale", qualified_name="proc:stale")])
    link_workspace(_cfg(), st)
    assert not any(n.kind == "BusinessProcess" and n.id == "proc:stale" for n in st.iter_nodes())


def test_clear_workspace_layer_does_not_remove_referenced_channel_nodes(tmp_path):
    """A Channel node that's still referenced by an edge (the normal, live case) must
    survive the full link_workspace pipeline -- not blanket-deleted by
    clear_workspace_layer (T7's own fix, see its docstring) NOR swept by the M2 final
    review's end-of-pipeline gc_orphan_channels (which only targets Channel nodes with
    ZERO referencing edges -- see test_link_workspace_gc_removes_unreferenced_channel_
    node below for that complementary case)."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("kafka_topic", name="orders.events")
    producer = _fn("sym:a:producer", "a", "producer", "q.producer")
    st.upsert_nodes([chan, producer])
    st.upsert_edges([_edge(producer.id, chan.id, "PRODUCES")])
    link_workspace(_cfg(), st)
    assert any(n.id == chan.id for n in st.iter_nodes())


# -- M2 final review: gc_orphan_channels, run at the end of link_workspace --


def test_link_workspace_gc_removes_unreferenced_channel_node(tmp_path):
    """The regression this fix targets: a Channel node with NO referencing edge at all
    (e.g. the OLD id left behind by a route/topic rename -- its own edges were already
    retired by this run's origin_service-scoped begin_service, see Staging.begin_service's
    docstring, but the Channel NODE itself has no per-service home to be swept by
    begin_service) must be gone after a full link_workspace pass, and counted in the
    returned report."""
    st = Staging(tmp_path / "s.db")
    orphan = make_channel_node("kafka_topic", name="stale.orphan.topic")
    st.upsert_nodes([orphan])
    report = link_workspace(_cfg(), st)
    assert not any(n.id == orphan.id for n in st.iter_nodes())
    assert report["channels_gc"] == 1


def test_link_workspace_gc_runs_before_http_routes_link_so_stale_channel_is_not_rematched(
    tmp_path,
):
    """Ordering pin for the M2 final review fix (see gc_orphan_channels' own docstring):
    GC must run BEFORE http_routes.link, or a stale Channel -- edge-less as of
    clear_workspace_layer, e.g. the old id left behind by a renamed route -- would
    still be visible to http_routes.link's route-table scan and get incorrectly
    re-matched by an unrelated claim, keeping it "referenced" and therefore immune to
    a LATER GC pass (the exact ordering bug this fix's own double-run regression test
    caught when GC was first, wrongly, placed at the end of link_workspace)."""
    st = Staging(tmp_path / "s.db")
    # Edge-less BEFORE link_workspace starts -- stands in for "the old Channel id from
    # a renamed route, whose own HANDLES edge begin_service already correctly retired
    # this run" (see Staging.begin_service's docstring); not re-derived by anything in
    # this scenario, so it should never become referenced again.
    stale = make_channel_node("http_route", owner_service="svc", method="GET",
                               template="/x", http_method="GET", path_template="/x")
    st.upsert_nodes([stale])
    st.add_claims("caller", "app/client.py", "http_call", [{
        "src_id": "sym:caller:client", "verb": "GET", "path_template": "/x",
        "base_url_env": None, "resolution_hint": "static", "evidence_line": 3,
    }])

    report = link_workspace(_cfg(), st)

    assert report["channels_gc"] == 1
    assert not any(n.id == stale.id for n in st.iter_nodes())
    # the claim falls back to UNRESOLVED (a brand new owner="?" channel), instead of
    # silently re-matching the (correctly removed) stale route.
    assert report["calls_http_unresolved"] == 1
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.dst != stale.id


def test_link_workspace_gc_does_not_remove_channel_gaining_a_fresh_edge_this_run(tmp_path):
    """Complements the ordering test above: a Channel that legitimately gains its FIRST
    edge as part of THIS run's own S5 data (HANDLES, staged together with the Channel in
    the same upsert_edges batch analyze_service always uses -- see analyze.py) must
    survive, since GC only ever sees Channels that are ALREADY edge-less once
    clear_workspace_layer has run, and this one never was."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="svc", method="GET", template="/x",
                              http_method="GET", path_template="/x")
    handler = _fn("sym:svc:handler", "svc", "handler", "q.handler")
    st.upsert_nodes([chan, handler])
    st.upsert_edges([_edge(chan.id, handler.id, "HANDLES", extractor="fastapi")],
                     origin_service="svc")

    report = link_workspace(_cfg(), st)

    assert report["channels_gc"] == 0
    assert any(n.id == chan.id for n in st.iter_nodes())


# -- full pipeline ordering: each stage's input depends on the previous stage's output --


def test_link_workspace_pipeline_order_produces_full_derivation_chain(tmp_path):
    """http_routes.link must run before segments.derive (CALLS_HTTP must exist before
    the CALLS_HTTP/HANDLES pair can be found), and segments.derive must run before
    processes.materialize (PART_OF_PROCESS's BFS needs NEXT_SEGMENT edges to walk). This
    single scenario chains claim -> CALLS_HTTP -> NEXT_SEGMENT -> PART_OF_PROCESS(order=1)
    end to end; getting the order wrong anywhere drops a later assertion to empty."""
    st = Staging(tmp_path / "s.db")
    route_chan = make_channel_node("http_route", owner_service="callee", method="GET",
                                    template="/x", http_method="GET", path_template="/x")
    caller_route = make_channel_node("http_route", owner_service="caller", method="POST",
                                      template="/start", http_method="POST",
                                      path_template="/start")
    entry = _fn("sym:caller:entry", "caller", "entry", "q.entry")
    client = _fn("sym:caller:client", "caller", "client", "q.client")
    handler = _fn("sym:callee:handler", "callee", "handler", "q.handler")
    st.upsert_nodes([route_chan, caller_route, entry, client, handler])
    st.upsert_edges([
        _edge(caller_route.id, entry.id, "HANDLES", extractor="fastapi"),
        _edge(route_chan.id, handler.id, "HANDLES", extractor="fastapi"),
    ])
    st.add_claims("caller", "app/client.py", "http_call", [{
        "src_id": client.id, "verb": "GET", "path_template": "/x",
        "base_url_env": None, "resolution_hint": "static", "evidence_line": 3,
    }])

    cfg = _cfg(ProcessDecl(name="Start flow", entrypoint="caller:POST /start"))
    report = link_workspace(cfg, st)

    assert report["calls_http"] == 1 and report["calls_http_unresolved"] == 0
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert (calls_http.src, calls_http.dst) == (client.id, route_chan.id)

    assert report["next_segments"] == 1
    next_seg = next(e for e in st.iter_edges() if e.type == "NEXT_SEGMENT")
    assert (next_seg.src, next_seg.dst) == (client.id, handler.id)

    assert report["processes"] == 1
    # PART_OF_PROCESS reaches entry(order 0). "client" is NOT connected to "entry" by any
    # NEXT_SEGMENT (entry itself never produces/calls-http anything) -- this scenario
    # deliberately isolates the http_routes->segments dependency without conflating it
    # with the BFS-reachability assertion (that is covered by test_linking_processes.py).
    part_of = {e.src: e.props["order"] for e in st.iter_edges() if e.type == "PART_OF_PROCESS"}
    assert part_of == {entry.id: 0}


# -- M8 T1 (rerun-2 R4): router_prefix.link runs before http_routes.link --


def test_link_workspace_composes_router_prefix_before_http_routes_link(tmp_path):
    """router_prefix.link must run before http_routes.link -- that stage's own
    _route_table scan reads whatever Channel(http_route) nodes are ALREADY staged,
    and router_prefix.link is what stages them now (see linking/router_prefix.py's
    own module docstring). This single scenario chains route_decl+router_include+
    router_decl claims (the last: M8 review Important-1 -- the hop parent's own
    declared prefix, required for composition) -> composed Channel/HANDLES ->
    CALLS_HTTP end to end."""
    st = Staging(tmp_path / "s.db")
    handler = _fn("sym:worker:handler", "worker", "handler", "q.handler")
    client = _fn("sym:caller:client", "caller", "client", "q.client")
    st.upsert_nodes([handler, client])
    st.add_claims("worker", "app/routes/steps.py", "route_decl", [{
        "router_symbol": "sym:worker:router", "verb": "GET", "path": "/steps/{id}",
        "handler_node_id": handler.id, "prefix_local": "", "evidence_line": 5,
    }])
    st.add_claims("worker", "app/main.py", "router_include", [{
        "parent_symbol": "sym:worker:app", "child_symbol": "sym:worker:router",
        "prefix": "/api/v1",
    }])
    st.add_claims("worker", "app/main.py", "router_decl", [{
        "router_symbol": "sym:worker:app", "prefix_local": "",
    }])
    st.add_claims("caller", "app/client.py", "http_call", [{
        "src_id": client.id, "verb": "GET", "path_template": "/api/v1/steps/{id}",
        "base_url_env": None, "resolution_hint": "static", "evidence_line": 1,
    }])

    report = link_workspace(_cfg(), st)

    assert report["route_prefix_unresolved"] == 0
    assert report["calls_http"] == 1 and report["calls_http_unresolved"] == 0
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.dst == "chan:http:worker:GET /api/v1/steps/{id}"


def test_link_workspace_propagates_calls_http_external_count(tmp_path):
    """M9 T1: link_workspace's own returned dict must surface http_routes.link's
    tier-2a ("external") counter, not just calls_http/calls_http_unresolved -- same
    "propagates unchanged" contract as part_of_process_ambiguous below."""
    st = Staging(tmp_path / "s.db")
    st.add_claims("caller", "app/client.py", "http_call", [{
        "src_id": "sym:caller:client", "verb": "GET", "path_template": "/x",
        "base_url_env": "GATEWAY_URL", "resolution_hint": "static", "evidence_line": 3,
    }])
    helm = tmp_path / "values.yaml"
    helm.write_text('GATEWAY_URL: "http://api-gateway.prod.svc.cluster.local"\n')
    cfg = WorkspaceConfig(
        graph_name="g", services=[ServiceConfig(name="svc", path=__file__)],
        env_sources=[helm],
    )

    report = link_workspace(cfg, st)

    assert report["calls_http_external"] == 1
    assert report["calls_http_unresolved"] == 0
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.resolution == "heuristic" and calls_http.confidence == 0.5
    assert calls_http.dst == "chan:http:?:GET /x"


def test_link_workspace_returns_all_expected_counter_keys(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = link_workspace(_cfg(), st)
    assert report.keys() == {
        "calls_http", "calls_http_unresolved", "next_segments", "processes", "marks",
        "channels_gc", "part_of_process_ambiguous",
        # M8 T1 (rerun-2 R4): router_prefix.link's own honest-miss counter -- see
        # linking/router_prefix.py's own docstring for the four failure shapes it
        # counts.
        "route_prefix_unresolved",
        # M8 T2 (rerun-2 R5): linking.signal_send.link's own honest-miss counter --
        # see linking/signal_send.py's own docstring.
        "signal_send_unlinked",
        # M9 T1: http_routes.link's own tier-2a ("external") honest-miss counter --
        # see linking/http_routes.py's own module docstring.
        "calls_http_external",
    }


def test_link_workspace_propagates_part_of_process_ambiguous_count(tmp_path):
    """M3 T2: processes.materialize's ambiguous-climb counter (see
    linking/processes.py's `_entry_of` docstring) must survive the trip through
    link_workspace's own returned dict, not just materialize's own -- this is what
    `codegraph index`'s report actually surfaces to a controller."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("http_route", owner_service="orders-api", method="POST",
                              template="/orders", http_method="POST", path_template="/orders")
    entry = _fn("sym:orders-api:entry", "orders-api", "entry", "q.entry")
    caller_a = _fn("sym:orders-api:caller-a", "orders-api", "caller-a", "q.caller-a")
    caller_b = _fn("sym:orders-api:caller-b", "orders-api", "caller-b", "q.caller-b")
    producer = _fn("sym:orders-api:producer", "orders-api", "producer", "q.producer")
    consumer = _fn("sym:kyc-worker:consumer", "kyc-worker", "consumer", "q.consumer")
    st.upsert_nodes([chan, entry, caller_a, caller_b, producer, consumer])
    st.upsert_edges([
        _edge(chan.id, entry.id, "HANDLES", extractor="fastapi"),
        _edge(caller_a.id, producer.id, "CALLS", extractor="calls"),
        _edge(caller_b.id, producer.id, "CALLS", extractor="calls"),
        _edge(producer.id, consumer.id, "NEXT_SEGMENT", via_channel_id="chan:x", derived=True),
    ])

    cfg = _cfg(ProcessDecl(name="Ambiguous flow", entrypoint="orders-api:POST /orders"))
    report = link_workspace(cfg, st)

    assert report["part_of_process_ambiguous"] == 1


# -- M8 T2 (rerun-2 R5): linking.signal_send.link runs before segments.derive --


def test_link_workspace_links_signal_send_claim_and_feeds_segments_derive(tmp_path):
    """temporal_signal_send claim + a pre-staged CONSUMES(handler -> channel) edge
    (temporal_ext.py's own S5 handler-side emission) must produce a PRODUCES(sender
    -> channel) edge AND feed straight into segments.derive's own exact-channel
    pairing -- proving linking.signal_send.link runs BEFORE segments.derive, not
    just that it runs at all (see linking/signal_send.py's own module docstring for
    why this ordering is load-bearing)."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("temporal_signal", name="complete-survey")
    sender = _fn("sym:worker:notify", "worker", "notify", "q.notify")
    handler = _fn("sym:gateway:complete_survey", "gateway", "complete_survey", "q.cs")
    st.upsert_nodes([chan, sender, handler])
    st.upsert_edges(
        [_edge(handler.id, chan.id, "CONSUMES", extractor="temporal")],
        origin_service="gateway",
    )
    st.add_claims("worker", "app/consumers/doc.py", "temporal_signal_send", [{
        "src_id": sender.id, "method_symbol": handler.id, "evidence_line": 5,
    }])

    report = link_workspace(_cfg(), st)

    assert report["signal_send_unlinked"] == 0
    produces = next(e for e in st.iter_edges() if e.type == "PRODUCES")
    assert (produces.src, produces.dst) == (sender.id, chan.id)
    assert produces.resolution == "static" and produces.confidence == 1.0

    assert report["next_segments"] == 1
    next_seg = next(e for e in st.iter_edges() if e.type == "NEXT_SEGMENT")
    assert (next_seg.src, next_seg.dst) == (sender.id, handler.id)


def test_link_workspace_signal_send_unlinked_counter_propagates(tmp_path):
    st = Staging(tmp_path / "s.db")
    sender = _fn("sym:worker:notify", "worker", "notify", "q.notify")
    st.upsert_nodes([sender])
    st.add_claims("worker", "app/consumers/doc.py", "temporal_signal_send", [{
        "src_id": sender.id, "method_symbol": "sym:gateway:`app.x`/NotAHandler#m().",
        "evidence_line": 1,
    }])

    report = link_workspace(_cfg(), st)

    assert report["signal_send_unlinked"] == 1
    assert not any(e.type == "PRODUCES" for e in st.iter_edges())
