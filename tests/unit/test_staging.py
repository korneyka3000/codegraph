import hashlib
import sqlite3

import pytest

from codegraph.chunking.splitter import ChunkRec
from codegraph.core.errors import InvariantError
from codegraph.core.schema import SCHEMA_VERSION, EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.stores.staging import Staging


def _node(id_, svc, kind="Function"):
    return NodeRec(id=id_, kind=kind, service=svc, name="n", qualified_name="q")


def _chunk(chunk_id, symbol_id, ord_, text="hello", start_line=1, end_line=1):
    return ChunkRec(
        chunk_id=chunk_id, symbol_id=symbol_id, ord=ord_, text=text,
        start_line=start_line, end_line=end_line,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


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


# -- M3 T1: version-check must run BEFORE ensure_schema's DDL, but only when a meta
# table already exists (a pre-existing staging.db) -- a naive unconditional swap would
# instead break the FRESH-file path (querying a meta table that doesn't exist yet raises
# its own raw sqlite3.OperationalError). See Staging.__init__'s docstring and
# SCHEMA_VERSION's history comment (core/schema.py) for the M2-final-review bug this
# closes: ensure_schema's DDL running first could itself raise a raw OperationalError
# (e.g. an index on a column only the CURRENT schema has) on an old-shaped table,
# before the actionable InvariantError ever got a chance to fire.


def test_v2_like_database_raises_invariant_error_not_operational_error(tmp_path):
    """Simulates re-opening a genuinely pre-v3 staging.db: a bare sqlite file created
    BY HAND (bypassing Staging entirely, exactly like an old on-disk file that predates
    the current DDL) with only a meta table recording schema_version='2'. Staging(path)
    must detect the mismatch from the meta table before ever running ensure_schema's
    DDL and raise the actionable InvariantError ("recreate") -- not let executescript()
    run first and risk surfacing a raw sqlite3.OperationalError instead."""
    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    raw.execute("INSERT INTO meta VALUES ('schema_version', '2')")
    raw.commit()
    raw.close()

    with pytest.raises(InvariantError, match="recreate") as exc_info:
        Staging(path)
    assert not isinstance(exc_info.value, sqlite3.OperationalError)


def test_v3_pre_t6_database_raises_invariant_error_not_operational_error(tmp_path):
    """M3 T6's own 3 -> 4 bump, pinned specifically (coordinator fix -- the _DDL
    comment block cites this test): a v3-shaped staging.db (pre-T6: `chunks` exists
    but has no `embedded_hash` column and no CHECK constraint) must fail with the
    loud, actionable InvariantError at Staging() construction -- via the
    version-check-BEFORE-DDL path -- never by letting any embedded_hash-referencing
    SQL reach the old table and raise a raw sqlite3.OperationalError ("no such
    column")."""
    path = tmp_path / "v3.db"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    raw.execute("INSERT INTO meta VALUES ('schema_version', '3')")
    # the v3-era chunks table shape (per T3's original DDL -- no embedded_hash, no
    # CHECK), so an unguarded embedded_hash query genuinely WOULD OperationalError
    raw.execute(
        "CREATE TABLE chunks(chunk_id TEXT PRIMARY KEY, symbol_id TEXT, service TEXT, "
        "relpath TEXT, ord INTEGER, text TEXT, start_line INTEGER, end_line INTEGER, "
        "content_hash TEXT, context_header TEXT, embedding BLOB, embed_model TEXT)"
    )
    raw.commit()
    raw.close()

    with pytest.raises(InvariantError, match="recreate") as exc_info:
        Staging(path)
    assert not isinstance(exc_info.value, sqlite3.OperationalError)


def test_fresh_staging_path_still_initializes_after_version_check_reorder(tmp_path):
    """The fresh-create path (no pre-existing file, no meta table at all) must keep
    working after the version-check-before-DDL reorder above -- pins the exact failure
    mode a NAIVE unconditional swap would introduce (querying meta before it exists)."""
    path = tmp_path / "fresh.db"
    assert not path.exists()
    st = Staging(path)  # must not raise
    assert st.get_meta("schema_version") == str(SCHEMA_VERSION)


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


def test_next_segment_parallel_channels_coexist(tmp_path):
    """M3 T1 PK migration: edges.PRIMARY KEY is now (src,dst,type,via_channel), not
    (src,dst,type) -- two NEXT_SEGMENT edges between the SAME (src,dst) pair, reached
    via two DIFFERENT channels (e.g. a producer that fans out over both an event topic
    AND a direct HTTP call to the same downstream handler), must both survive upsert
    and both come back out of iter_edges, instead of the second silently clobbering the
    first via INSERT OR REPLACE on a too-narrow key (the v2 bug)."""
    st = Staging(tmp_path / "s.db")
    e1 = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.9,
                 "linking", props={"via_channel_id": "chan:kafka_topic:orders"})
    e2 = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.72,
                 "linking", props={"via_channel_id": "chan:kafka_topic:shipping"})
    st.upsert_edges([e1, e2])
    edges = list(st.iter_edges())
    assert len(edges) == 2
    assert {e.props["via_channel_id"] for e in edges} == {
        "chan:kafka_topic:orders", "chan:kafka_topic:shipping",
    }


def test_next_segment_same_via_channel_replaces_on_full_pk(tmp_path):
    """Companion to the coexistence test above: re-upserting the SAME (src,dst,type,
    via_channel) key must still REPLACE (not duplicate) -- the PK widened by exactly
    one column, it didn't stop being a real dedup key."""
    st = Staging(tmp_path / "s.db")
    e1 = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.9,
                 "linking", props={"via_channel_id": "chan:kafka_topic:orders"})
    e2 = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.42,
                 "linking", props={"via_channel_id": "chan:kafka_topic:orders"})
    st.upsert_edges([e1])
    st.upsert_edges([e2])
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].confidence == 0.42


def test_edge_without_via_channel_id_still_dedups_as_before(tmp_path):
    """Edges that never carry via_channel_id in props (the overwhelming majority of
    types) get the column's DEFAULT '' on both writes -- PK behavior for them is
    unchanged from v2's (src,dst,type)."""
    st = Staging(tmp_path / "s.db")
    e1 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "PRODUCES", "static", 1.0, "kafka")
    e2 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "PRODUCES", "heuristic", 0.6, "kafka")
    st.upsert_edges([e1])
    st.upsert_edges([e2])
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].resolution == "heuristic"


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
    # proc: src, no origin_service (S7-linking-derived, the default) -- must survive an
    # unrelated service's begin_service (old M1b code deleted ALL null-src edges
    # globally as a side effect of ANY single service's begin_service call -- that
    # regression is fixed by scoping deletion to clear_workspace_layer() alone, never
    # a side effect of begin_service). See the origin_service-specific tests below
    # (M2 final review) for a second, narrower survival gap in the SAME edges-outlive-
    # a-re-index family: a chan:-src edge that DOES belong to one service's own S5 run.
    e = EdgeRec("proc:place-order", "sym:a:`m`/f().", "PART_OF_PROCESS",
                "derived", 1.0, "linking")
    st.upsert_edges([e])
    st.begin_service("b")  # unrelated service; never touched "a" or the process
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].type == "PART_OF_PROCESS"


# -- M2 final review: origin_service (replaces src_service as begin_service's deletion
# key -- see Staging.upsert_edges/begin_service docstrings for the stale-layer bug this
# closes: a chan:-src edge like HANDLES has no derivable _id_service(src) at all, so the
# OLD scheme could never find it on re-index no matter which service wrote it) --


def test_begin_service_deletes_chan_src_edge_tagged_with_its_own_origin_service(tmp_path):
    """The core regression proof: a HANDLES-shaped edge (src=chan:, a prefix
    _id_service can't attribute to any service -- see staging.py's _id_service) is
    still deleted by ITS OWN emitting service's begin_service, because origin_service
    is passed explicitly by the caller rather than derived from e.src's prefix."""
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("chan:http:a:GET /x", "sym:a:`m`/handler().", "HANDLES",
                "static", 1.0, "fastapi")
    st.upsert_edges([e], origin_service="a")
    st.begin_service("a")
    assert list(st.iter_edges()) == []


def test_begin_service_does_not_delete_chan_src_edge_of_a_different_origin_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("chan:http:b:GET /x", "sym:b:`m`/handler().", "HANDLES",
                "static", 1.0, "fastapi")
    st.upsert_edges([e], origin_service="b")
    st.begin_service("a")  # unrelated service
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].src == "chan:http:b:GET /x"


def test_begin_service_does_not_delete_edges_with_no_origin_service(tmp_path):
    """origin_service=None (the default -- S7/linking-derived batches, e.g.
    NEXT_SEGMENT/CALLS_HTTP/PART_OF_PROCESS) must be immune to EVERY service's
    begin_service, same contract as the pre-existing null-src test above, now proven
    directly against the new column instead of the retired src_service one."""
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "NEXT_SEGMENT", "derived", 0.9,
                "linking", props={"via_channel_id": "chan:kafka_topic:orders"})
    st.upsert_edges([e])  # origin_service defaults to None
    st.begin_service("a")
    st.begin_service("b")
    assert len(list(st.iter_edges())) == 1


def test_begin_service_deletes_regular_sym_src_edge_tagged_with_origin_service(tmp_path):
    """Sanity: the ordinary sym->sym case (e.g. a CALLS edge -- see extractors/calls.py)
    keeps working exactly as before under the new column; origin_service and the
    edge's own src-derived service happen to coincide here, but deletion now goes
    through origin_service, not the coincidence."""
    st = Staging(tmp_path / "s.db")
    e = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0, "calls")
    st.upsert_edges([e], origin_service="a")
    st.begin_service("a")
    assert list(st.iter_edges()) == []


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


def test_claims_for_injected_service_and_relpath_win_over_payload_collision(tmp_path):
    """claims_for's docstring has always claimed the injected "_service"/"_relpath"
    win over same-named keys already present in the payload itself ("staging-метаданные
    авторитетнее произвольного содержимого claim'а") -- documented since T1 but never
    actually tested (progress.md M2 T1 backlog). {**payload, "_service": svc,
    "_relpath": relpath} does put the injected keys last, so this should already pass;
    this test pins that contract directly instead of leaving it merely asserted in
    prose."""
    st = Staging(tmp_path / "s.db")
    st.add_claims("real-service", "real/path.py", "kafka_producer", [{
        "topic": "orders.created", "_service": "fake-service", "_relpath": "fake/path.py",
    }])
    claims = st.claims_for("kafka_producer")
    assert len(claims) == 1
    assert claims[0]["_service"] == "real-service"
    assert claims[0]["_relpath"] == "real/path.py"
    assert claims[0]["topic"] == "orders.created"


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


def test_update_edge_props_rejects_next_segment_type(tmp_path):
    """update_edge_props's (src, dst, type) key doesn't distinguish via_channel --
    unlike upsert_edges, whose real PK is (src, dst, type, via_channel) (M3 T1, see
    core/schema.py's SCHEMA_VERSION "2 -> 3" history). A NEXT_SEGMENT pair can
    legitimately have two rows sharing (src, dst, type) since the parallel-channel
    derive() fix (linking/segments.py) -- calling update_edge_props on that type would
    silently overwrite BOTH rows' props from whichever one SELECT happened to fetch
    first. The only real caller (linking/workspace.py's temporal-start marking) only
    ever passes type="CALLS", so this guard costs it nothing."""
    st = Staging(tmp_path / "s.db")
    with pytest.raises(InvariantError):
        st.update_edge_props("sym:a:x", "sym:b:y", "NEXT_SEGMENT", {"k": "v"})


# -- M3 T3: chunks (chunking.splitter.ChunkRec staged for T4/T6) --


def test_upsert_chunks_and_chunks_for_service_round_trip(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    c1 = _chunk("sym:a:m/f().#c0", "sym:a:m/f().", 0, text="def f(): pass",
                start_line=3, end_line=4)
    st.upsert_chunks("a", "app/m.py", [c1])

    rows = st.chunks_for_service("a")
    assert len(rows) == 1
    row = rows[0]
    assert row.chunk_id == c1.chunk_id
    assert row.symbol_id == c1.symbol_id
    assert row.service == "a"
    assert row.relpath == "app/m.py"
    assert row.ord == 0
    assert row.text == c1.text
    assert (row.start_line, row.end_line) == (3, 4)
    assert row.content_hash == c1.content_hash
    assert row.context_header is None
    assert row.embedding is None
    assert row.embed_model is None
    assert row.embedded_hash is None


def test_chunks_for_service_scoped_by_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("a", "m.py", [_chunk("ca#c0", "ca", 0)])
    st.upsert_chunks("b", "m.py", [_chunk("cb#c0", "cb", 0)])
    assert {r.chunk_id for r in st.chunks_for_service("a")} == {"ca#c0"}
    assert {r.chunk_id for r in st.chunks_for_service("b")} == {"cb#c0"}


def test_chunks_missing_embedding_filters_correctly(tmp_path):
    st = Staging(tmp_path / "s.db")
    c1, c2, c3 = _chunk("c1#c0", "c1", 0), _chunk("c2#c0", "c2", 0), _chunk("c3#c0", "c3", 0)
    st.upsert_chunks("a", "m.py", [c1, c2, c3])
    st.set_embeddings([
        ("c1#c0", b"\x00\x01", "model-a", c1.content_hash),
        ("c2#c0", b"\x02\x03", "model-old", c2.content_hash),
    ])

    # c1: embedded under the CURRENT model, hash matches (up to date) -- not missing.
    # c2: embedded, but under a DIFFERENT (stale) model -- missing.
    # c3: never embedded at all -- missing.
    missing = {r.chunk_id for r in st.chunks_missing_embedding("model-a")}
    assert missing == {"c2#c0", "c3#c0"}


def test_chunks_missing_embedding_flags_stale_embedded_hash(tmp_path):
    """M3 T6 carry (cache hardening): a chunk embedded under the CURRENT model, whose
    `embedded_hash` no longer matches its CURRENT `content_hash` (simulates a
    same-chunk_id content edit that landed between the embed and this check, e.g. a
    file changed within one staging session with no intervening `begin_service`), must
    still be flagged -- embedding presence + matching model alone are NOT enough."""
    st = Staging(tmp_path / "s.db")
    c1 = _chunk("c1#c0", "c1", 0, text="original")
    st.upsert_chunks("a", "m.py", [c1])
    st.set_embeddings([("c1#c0", b"\xaa", "model-a", c1.content_hash)])
    assert st.chunks_missing_embedding("model-a") == []  # up to date immediately after

    # re-upsert the SAME chunk_id with DIFFERENT text -- content_hash changes,
    # embedding/embed_model/embedded_hash survive untouched (upsert_chunks' own
    # ON CONFLICT contract).
    st.upsert_chunks("a", "m.py", [_chunk("c1#c0", "c1", 0, text="edited")])

    missing = {r.chunk_id for r in st.chunks_missing_embedding("model-a")}
    assert missing == {"c1#c0"}


def test_set_embeddings_and_set_context_headers(tmp_path):
    st = Staging(tmp_path / "s.db")
    c1 = _chunk("c1#c0", "c1", 0)
    st.upsert_chunks("a", "m.py", [c1])
    st.set_embeddings([("c1#c0", b"\x01\x02\x03", "model-a", c1.content_hash)])
    st.set_context_headers([("c1#c0", "file: m.py")])

    row = st.chunks_for_service("a")[0]
    assert row.embedding == b"\x01\x02\x03"
    assert row.embed_model == "model-a"
    assert row.embedded_hash == c1.content_hash
    assert row.context_header == "file: m.py"


def test_set_embeddings_noop_for_missing_chunk_id(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.set_embeddings([("nope#c0", b"\x00", "model-a", "somehash")])  # must not raise
    st.set_context_headers([("nope#c0", "header")])  # must not raise


def test_begin_service_clears_chunks(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("a", "m.py", [_chunk("ca#c0", "ca", 0)])
    st.upsert_chunks("b", "m.py", [_chunk("cb#c0", "cb", 0)])
    st.begin_service("a")
    assert st.chunks_for_service("a") == []
    assert len(st.chunks_for_service("b")) == 1


def test_counts_includes_chunks(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("a", "m.py", [_chunk("ca#c0", "ca", 0), _chunk("ca#c1", "ca", 1)])
    assert st.counts()["chunks"] == 2


def test_iter_chunks_across_services(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("a", "m.py", [_chunk("ca#c0", "ca", 0)])
    st.upsert_chunks("b", "n.py", [_chunk("cb#c0", "cb", 0)])
    assert {r.chunk_id for r in st.iter_chunks()} == {"ca#c0", "cb#c0"}


def test_upsert_chunks_preserves_embedding_on_conflict_same_content(tmp_path):
    """Re-upserting the SAME chunk_id (identical content) must not wipe an
    already-set embedding -- `ON CONFLICT DO UPDATE` only ever touches the
    content-derived columns, which is what makes T6's chunk_embed.run idempotent on
    a repeated call (see upsert_chunks' own docstring)."""
    st = Staging(tmp_path / "s.db")
    c1 = _chunk("c1#c0", "c1", 0, text="same text")
    st.upsert_chunks("a", "m.py", [c1])
    st.set_embeddings([("c1#c0", b"\xaa\xbb", "model-a", c1.content_hash)])

    st.upsert_chunks("a", "m.py", [c1])  # re-chunk, identical content
    row = st.chunks_for_service("a")[0]
    assert row.embedding == b"\xaa\xbb"
    assert row.embed_model == "model-a"
    assert row.embedded_hash == c1.content_hash


def test_upsert_chunks_updates_text_on_conflict_when_content_changes(tmp_path):
    """M3 T6 carry fix: this test used to document the OPPOSITE as an accepted
    limitation ("the now-stale embedding survives... not content_hash freshness") --
    embedded_hash now closes that gap, see chunks_missing_embedding's own docstring."""
    st = Staging(tmp_path / "s.db")
    c1 = _chunk("c1#c0", "c1", 0, text="old text")
    st.upsert_chunks("a", "m.py", [c1])
    st.set_embeddings([("c1#c0", b"\xaa", "model-a", c1.content_hash)])

    c1_edited = _chunk("c1#c0", "c1", 0, text="new text, edited")
    st.upsert_chunks("a", "m.py", [c1_edited])
    row = st.chunks_for_service("a")[0]
    assert row.text == "new text, edited"
    assert row.content_hash == c1_edited.content_hash
    # the OLD embedding blob itself still sits in the column (upsert_chunks never
    # touches it) -- but embedded_hash (still c1's ORIGINAL hash) now disagrees with
    # the fresh content_hash, so chunks_missing_embedding correctly flags this row for
    # re-embedding instead of silently serving a stale vector forever.
    assert row.embedding == b"\xaa"
    assert row.embedded_hash == c1.content_hash
    missing_ids = {r.chunk_id for r in st.chunks_missing_embedding("model-a")}
    assert "c1#c0" in missing_ids
