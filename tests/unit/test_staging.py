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
