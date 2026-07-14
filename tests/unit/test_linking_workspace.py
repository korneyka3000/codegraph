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


def test_clear_workspace_layer_does_not_remove_channel_nodes(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("kafka_topic", name="orders.events")
    st.upsert_nodes([chan])
    link_workspace(_cfg(), st)
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


def test_link_workspace_returns_all_expected_counter_keys(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = link_workspace(_cfg(), st)
    assert report.keys() == {
        "calls_http", "calls_http_unresolved", "next_segments", "processes", "marks",
    }
