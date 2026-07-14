import pytest

from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.stores.staging import Staging


def _node(id_, svc, kind="Function"):
    return NodeRec(id=id_, kind=kind, service=svc, name="n", qualified_name="q")


def test_roundtrip_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_files("a", [("app/x.py", "abc", 10)])
    st.add_defs("a", [DefRow("app/x.py", "local 1", 5, 8, 1)])
    st.add_refs("a", [RefRow("app/x.py", "local 1", 20, 23, 2, 0)])
    st.upsert_nodes([_node("sym:a:`app.x`/f().", "a")])
    st.upsert_edges([EdgeRec("sym:a:`app.x`/f().", "sym:a:`app.x`/g().", "CALLS",
                             "static", 1.0, "calls")])
    c = st.counts()
    assert (c["files"], c["defs"], c["refs"], c["nodes"], c["edges"]) == (1, 1, 1, 1, 1)


def test_begin_service_wipes_only_that_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    for svc in ("a", "b"):
        st.begin_service(svc)
        st.add_files(svc, [("m.py", "h", 1)])
    st.begin_service("a")
    assert st.files_for_service("a") == []
    assert st.files_for_service("b") == [("m.py", "h")]


def test_def_symbol_at_and_refs_sorted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_defs("a", [DefRow("m.py", "SYM_F", 100, 103, 5)])
    st.add_refs("a", [RefRow("m.py", "R2", 50, 52, 3, 0), RefRow("m.py", "R1", 10, 12, 1, 0)])
    assert st.def_symbol_at("a", "m.py", 100) == "SYM_F"
    assert st.def_symbol_at("a", "m.py", 99) is None
    assert [r.symbol for r in st.refs_for_file("a", "m.py")] == ["R1", "R2"]


def test_cross_service_code_edge_forbidden(tmp_path):
    st = Staging(tmp_path / "s.db")
    with pytest.raises(InvariantError):
        st.upsert_edges([EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "CALLS",
                                 "static", 1.0, "calls")])


def test_edge_replace_on_pk(tmp_path):
    st = Staging(tmp_path / "s.db")
    e1 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0,
                 "calls", props={"callsite_count": 1})
    e2 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0,
                 "calls", props={"callsite_count": 3})
    st.upsert_edges([e1])
    st.upsert_edges([e2])
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].props["callsite_count"] == 3


def test_module_set(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_files("a", [("app/__init__.py", "h", 1), ("app/db/outbox.py", "h", 1)])
    assert st.module_set("a") == {"app", "app.db.outbox"}


def test_meta(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.set_meta("schema_version", "1")
    assert st.get_meta("schema_version") == "1"
    assert st.get_meta("nope") is None


def test_svc_to_foreign_sym_edge_forbidden(tmp_path):
    st = Staging(tmp_path / "s.db")
    with pytest.raises(InvariantError):
        st.upsert_edges([EdgeRec("svc:a", "sym:b:`m`/", "CONTAINS", "static", 1.0, "x")])


def test_def_symbol_at_deterministic_on_collision(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_defs("a", [DefRow("m.py", "SYM_B", 10, 12, 1), DefRow("m.py", "SYM_A", 10, 12, 1)])
    assert st.def_symbol_at("a", "m.py", 10) == "SYM_A"  # ORDER BY symbol


# -- M2 T4: ref_symbol_at (mirrors def_symbol_at, but over scip_refs -- sanctioned
# FileContext.ref_symbol_lookup extension needs a ref-occurrence lookup keyed by
# (service, relpath, start_byte), symmetric to the existing def-lookup) --


def test_ref_symbol_at_mirrors_def_symbol_at(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_refs("a", [RefRow("m.py", "SYM_F", 100, 103, 5, 0)])
    assert st.ref_symbol_at("a", "m.py", 100) == "SYM_F"
    assert st.ref_symbol_at("a", "m.py", 99) is None


def test_ref_symbol_at_deterministic_on_collision(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_refs("a", [RefRow("m.py", "SYM_B", 10, 12, 1, 0), RefRow("m.py", "SYM_A", 10, 12, 1, 0)])
    assert st.ref_symbol_at("a", "m.py", 10) == "SYM_A"  # ORDER BY symbol


def test_ref_symbol_at_scoped_by_service_and_relpath(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.begin_service("b")
    st.add_refs("a", [RefRow("m.py", "SYM_A", 10, 12, 1, 0)])
    st.add_refs("b", [RefRow("m.py", "SYM_B", 10, 12, 1, 0)])
    assert st.ref_symbol_at("a", "m.py", 10) == "SYM_A"
    assert st.ref_symbol_at("b", "m.py", 10) == "SYM_B"
    assert st.ref_symbol_at("a", "other.py", 10) is None


def test_schema_version_mismatch_raises(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.set_meta("schema_version", "999")
    st.close()
    with pytest.raises(InvariantError, match="schema_version"):
        Staging(tmp_path / "s.db")


def test_local_def_symbols(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_defs("a", [DefRow("m.py", "local 1", 0, 1, 1),
                      DefRow("m.py", "scip-python python a 0.1 `m`/f().", 5, 6, 1)])
    assert st.local_def_symbols("a", "m.py") == {"local 1"}


# -- M2: NodeRec.roles round-trip + validation --


def test_upsert_nodes_roles_round_trip_via_iter_nodes(tmp_path):
    st = Staging(tmp_path / "s.db")
    n = NodeRec(id="sym:a:`m`/f().", kind="Function", service="a", name="f",
                qualified_name="m.f", roles=("RouteHandler",))
    st.upsert_nodes([n])
    out = list(st.iter_nodes())
    assert len(out) == 1
    assert out[0].roles == ("RouteHandler",)


def test_upsert_nodes_no_roles_round_trips_empty_tuple(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("sym:a:`m`/f().", "a")])
    out = list(st.iter_nodes())
    assert out[0].roles == ()


def test_upsert_nodes_multiple_roles_round_trip_order_preserved(tmp_path):
    st = Staging(tmp_path / "s.db")
    n = NodeRec(id="sym:a:`m`/f().", kind="Function", service="a", name="f",
                qualified_name="m.f", roles=("MessageConsumer", "TemporalActivity"))
    st.upsert_nodes([n])
    out = list(st.iter_nodes())
    assert out[0].roles == ("MessageConsumer", "TemporalActivity")


def test_upsert_nodes_invalid_role_raises_invariant_error(tmp_path):
    st = Staging(tmp_path / "s.db")
    n = NodeRec(id="sym:a:`m`/f().", kind="Function", service="a", name="f",
                qualified_name="m.f", roles=("NotARole",))
    with pytest.raises(InvariantError):
        st.upsert_nodes([n])


# -- M2: upsert_edges invariant (chan:/proc: endpoints free; NEXT_SEGMENT exception) --


def test_next_segment_cross_service_allowed_with_via_channel_id(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.9,
                "linking", props={"via_channel_id": "chan:kafka_topic:orders"})
    st.upsert_edges([e])  # must not raise
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].type == "NEXT_SEGMENT"


def test_next_segment_cross_service_without_via_channel_id_forbidden(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.9,
                "linking")  # no via_channel_id prop
    with pytest.raises(InvariantError):
        st.upsert_edges([e])


def test_cross_service_edge_wrong_type_with_via_channel_id_still_forbidden(tmp_path):
    # via_channel_id alone doesn't grant a pass -- type must be exactly NEXT_SEGMENT.
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "CALLS", "static", 1.0,
                "calls", props={"via_channel_id": "chan:kafka_topic:orders"})
    with pytest.raises(InvariantError):
        st.upsert_edges([e])


def test_channel_endpoint_edge_no_cross_service_check(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "chan:kafka_topic:orders.created", "PRODUCES",
                "static", 1.0, "kafka")
    st.upsert_edges([e])  # must not raise despite a service-bearing endpoint
    assert len(list(st.iter_edges())) == 1


def test_process_endpoint_edge_no_cross_service_check(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("proc:place-order", "sym:a:`m`/f().", "PART_OF_PROCESS",
                "derived", 1.0, "linking")
    st.upsert_edges([e])  # must not raise
    assert len(list(st.iter_edges())) == 1


# -- M2: begin_service no longer wipes unrelated NULL-src edges globally --


def test_begin_service_does_not_wipe_other_services_null_src_edges(tmp_path):
    st = Staging(tmp_path / "s.db")
    # proc: src -> NULL src_service; must survive an unrelated service's
    # begin_service (old code deleted ALL null-src edges globally as a side
    # effect of ANY single service's begin_service call -- this is the fixed
    # regression: workspace-layer edges are now cleared ONLY by
    # clear_workspace_layer(), never as a side effect of begin_service).
    e = EdgeRec("proc:place-order", "sym:a:`m`/f().", "PART_OF_PROCESS",
                "derived", 1.0, "linking")
    st.upsert_edges([e])
    st.begin_service("b")  # unrelated service; never touched "a" or the process
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].type == "PART_OF_PROCESS"


# -- M2: claims --


def test_claims_round_trip_injects_service_and_relpath(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "app/producer.py", "kafka_producer",
                  [{"topic": "orders.created", "var": "producer"}])
    claims = st.claims_for("kafka_producer")
    assert len(claims) == 1
    assert claims[0]["topic"] == "orders.created"
    assert claims[0]["var"] == "producer"
    assert claims[0]["_service"] == "a"
    assert claims[0]["_relpath"] == "app/producer.py"


def test_claims_filtered_by_kind_and_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "x.py", "kafka_producer", [{"topic": "t1"}])
    st.add_claims("b", "y.py", "kafka_producer", [{"topic": "t2"}])
    st.add_claims("a", "x.py", "kafka_consumer", [{"topic": "t3"}])

    claims_a_producer = st.claims_for("kafka_producer", service="a")
    assert len(claims_a_producer) == 1 and claims_a_producer[0]["topic"] == "t1"

    claims_all_producers = st.claims_for("kafka_producer")
    assert {c["topic"] for c in claims_all_producers} == {"t1", "t2"}

    claims_a_consumer = st.claims_for("kafka_consumer")
    assert len(claims_a_consumer) == 1 and claims_a_consumer[0]["topic"] == "t3"


def test_claims_for_unknown_kind_returns_empty_list(tmp_path):
    st = Staging(tmp_path / "s.db")
    assert st.claims_for("nope") == []


def test_add_claims_multiple_payloads_one_call(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "x.py", "kafka_producer", [{"topic": "t1"}, {"topic": "t2"}])
    claims = st.claims_for("kafka_producer")
    assert {c["topic"] for c in claims} == {"t1", "t2"}


def test_begin_service_clears_own_claims(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "app/x.py", "kafka_producer", [{"topic": "orders"}])
    st.begin_service("a")
    assert st.claims_for("kafka_producer", service="a") == []


def test_begin_service_does_not_clear_other_services_claims(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.add_claims("a", "x.py", "kafka_producer", [{"topic": "t1"}])
    st.add_claims("b", "y.py", "kafka_producer", [{"topic": "t2"}])
    st.begin_service("a")
    assert st.claims_for("kafka_producer", service="b") != []
    assert st.claims_for("kafka_producer", service="a") == []


# -- M2 T7: clear_workspace_layer (narrowed contract) --
#
# T1 originally deleted kind IN ('Channel','BusinessProcess'). T7 narrows this to
# BusinessProcess ONLY (sanctioned T1-contract fix, see staging.py's clear_workspace_layer
# docstring): Channel nodes are now created by S5 extractors (fastapi_ext/kafka_ext),
# per-service, staged the same way code nodes are -- deleting kind='Channel' here would
# wipe EVERY service's channels workspace-wide even though begin_service only re-analyzes
# ONE service at a time, losing channels for services that weren't re-analyzed in this run.
# Channel ids are deterministic (ids.chan_kafka/chan_event/chan_http) and upsert_nodes is
# INSERT OR REPLACE, so re-emission is a no-op replace, not a duplicate -- explicit
# deletion here would be redundant defense with a real downside (data loss) and no upside.


def test_clear_workspace_layer_removes_only_business_process_nodes_and_linking_edges(
    tmp_path,
):
    st = Staging(tmp_path / "s.db")
    fn = NodeRec(id="sym:a:`m`/f().", kind="Function", service="a", name="f",
                 qualified_name="m.f")
    chan = NodeRec(id="chan:kafka_topic:orders", kind="Channel", service="",
                    name="orders", qualified_name="chan:kafka_topic:orders")
    proc = NodeRec(id="proc:place-order", kind="BusinessProcess", service="",
                    name="Place Order", qualified_name="proc:place-order")
    st.upsert_nodes([fn, chan, proc])

    code_edge = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/f().", "CALLS", "static", 1.0, "calls")
    linking_edge = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/f().", "NEXT_SEGMENT", "derived",
                           0.9, "linking", props={"via_channel_id": chan.id})
    st.upsert_edges([code_edge, linking_edge])  # same (src,dst), distinct type -> both kept

    st.clear_workspace_layer()

    # Channel survives (T7 fix); BusinessProcess is removed; the code node is untouched.
    remaining_ids = {n.id for n in st.iter_nodes()}
    assert remaining_ids == {fn.id, chan.id}
    remaining_edges = {(e.src, e.dst, e.type) for e in st.iter_edges()}
    assert remaining_edges == {(code_edge.src, code_edge.dst, code_edge.type)}


def test_clear_workspace_layer_survives_repeated_calls_without_deleting_channel(tmp_path):
    """Regression guard for the exact scenario the T7 fix addresses: calling
    clear_workspace_layer() a second time (as link_workspace does on every `codegraph
    index` run) must not progressively erode Channel nodes staged by an EARLIER
    analyze_service call that isn't part of THIS run's service loop."""
    st = Staging(tmp_path / "s.db")
    chan = NodeRec(id="chan:http:svc:GET /x", kind="Channel", service="",
                    name="GET /x", qualified_name="chan:http:svc:GET /x")
    st.upsert_nodes([chan])
    st.clear_workspace_layer()
    st.clear_workspace_layer()
    assert {n.id for n in st.iter_nodes()} == {chan.id}


# -- M2: update_edge_props --


def test_update_edge_props_merges_and_overwrites(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0, "calls",
                props={"callsite_count": 1, "keep": "me"})
    st.upsert_edges([e])
    ok = st.update_edge_props(e.src, e.dst, e.type, {"callsite_count": 5, "new_key": "v"})
    assert ok is True
    updated = next(iter(st.iter_edges()))
    assert updated.props == {"callsite_count": 5, "keep": "me", "new_key": "v"}


def test_update_edge_props_returns_false_when_edge_missing(tmp_path):
    st = Staging(tmp_path / "s.db")
    ok = st.update_edge_props("sym:a:x", "sym:a:y", "CALLS", {"k": "v"})
    assert ok is False
