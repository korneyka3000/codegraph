"""Интеграционный тест load.load_graph (S9) на живом FalkorDB (Step 1 брифа m1b-task-5).

Синтетический staging: 2 кодовых узла (Function x2, разные label-группы не нужны --
одного kind достаточно, чтобы доказать группировку по labels-набору; Module добавлять
не обязательно, т.к. labels-маппинг для {Module,Class,Function} идентичен по форме
("Sym", kind) -- разница только в самом kind) + Service-узел (третья, отдельная
label-группа ("Service",)) + CONTAINS (svc->a) + CALLS (a->b) + ребро с ghost-концом
(a->несуществующий id, тот же тип CALLS, чтобы dropped-счётчик по типам был ненулевым
именно для CALLS). load_graph пишет в `__t5__build`, swap_in переключает на `__t5__`;
проверяем: labels(n) через raw, props (None-ключи опущены, decorators/is_async живьём
как настоящий list/bool -- не json-строка, см. probe в отчёте), рёбра (dropped==1,
разбитый по типам), build-ключ исчезает после swap, finally-удаление обоих графов.
"""

from __future__ import annotations

import pytest

from codegraph.core.schema import EdgeRec, NodeRec, make_service_node
from codegraph.pipeline.load import load_graph
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.falkordb

GRAPH_NAME = "__t5__"
BUILD_NAME = f"{GRAPH_NAME}__build"

NODE_A_ID = "sym:t5svc:a"
NODE_B_ID = "sym:t5svc:b"
GHOST_ID = "sym:t5svc:ghost"

NODE_A = NodeRec(
    id=NODE_A_ID, kind="Function", service="t5svc", name="a", qualified_name="mod.a",
    relpath="mod.py", start_byte=0, end_byte=10, start_line=1, end_line=2,
    content_hash="hash-a",
    props={"signature": "def a():", "docstring": None, "is_async": False, "decorators": []},
)
NODE_B = NodeRec(
    id=NODE_B_ID, kind="Function", service="t5svc", name="b", qualified_name="mod.b",
    relpath="mod.py", start_byte=20, end_byte=30, start_line=5, end_line=6,
    content_hash="hash-b",
    props={"signature": "def b():", "docstring": "does b things", "is_async": True,
           "decorators": ["staticmethod", "cached"]},
)
SERVICE_NODE = make_service_node("t5svc")

EDGE_CONTAINS = EdgeRec(
    src=SERVICE_NODE.id, dst=NODE_A_ID, type="CONTAINS",
    resolution="static", confidence=1.0, extractor="python_core",
)
EDGE_CALLS = EdgeRec(
    src=NODE_A_ID, dst=NODE_B_ID, type="CALLS",
    resolution="static", confidence=1.0, extractor="calls",
    evidence_file="mod.py", evidence_line=1, props={"callsite_count": 1},
)
EDGE_CALLS_GHOST = EdgeRec(
    src=NODE_A_ID, dst=GHOST_ID, type="CALLS",
    resolution="static", confidence=1.0, extractor="calls",
    evidence_file="mod.py", evidence_line=2, props={"callsite_count": 1},
)


def _cleanup(cfg, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # swap_in уже мог унести build-ключ через RENAME


def _staging(tmp_path) -> Staging:
    st = Staging(tmp_path / "s.db")
    st.begin_service("t5svc")
    st.upsert_nodes([NODE_A, NODE_B, SERVICE_NODE])
    st.upsert_edges([EDGE_CONTAINS, EDGE_CALLS, EDGE_CALLS_GHOST])
    return st


def test_load_graph_writes_labels_edges_drops_ghost_and_swaps(falkordb_cfg, tmp_path):
    st = _staging(tmp_path)

    def store_factory(name: str) -> FalkorStore:
        return FalkorStore(falkordb_cfg, name)

    try:
        stats = load_graph(st, store_factory, GRAPH_NAME)

        # -- return dict: counts + by-type/by-label breakdowns --
        assert stats["nodes_written"] == 3
        assert stats["nodes_written_by_label"] == {"Sym:Function": 2, "Service": 1}
        assert stats["edges_written"] == 2
        assert stats["edges_written_by_type"] == {"CONTAINS": 1, "CALLS": 1}
        assert stats["edges_dropped_missing_endpoint"] == 1
        assert stats["edges_dropped_by_type"] == {"CONTAINS": 0, "CALLS": 1}

        # -- blue/green: build key gone, final graph present --
        db = connect(falkordb_cfg)
        graphs_after = db.list_graphs()
        assert BUILD_NAME not in graphs_after
        assert GRAPH_NAME in graphs_after

        final_store = FalkorStore(falkordb_cfg, GRAPH_NAME)

        # -- labels(n) via raw --
        rows = final_store.raw("MATCH (n) RETURN n.id, labels(n)").result_set
        labels_by_id = {row[0]: set(row[1]) for row in rows}
        assert labels_by_id == {
            NODE_A_ID: {"Sym", "Function"},
            NODE_B_ID: {"Sym", "Function"},
            SERVICE_NODE.id: {"Service"},
        }

        # -- props: None-valued keys omitted; list/bool props round-trip as real
        # list/bool (not json-string fallback) --
        nodes = {n["id"]: n for n in final_store.get_nodes(
            [NODE_A_ID, NODE_B_ID, SERVICE_NODE.id]
        )}
        a_props = nodes[NODE_A_ID]
        assert a_props["is_async"] is False
        assert a_props["decorators"] == []
        assert "docstring" not in a_props  # None omitted, not null

        b_props = nodes[NODE_B_ID]
        assert b_props["is_async"] is True
        assert b_props["decorators"] == ["staticmethod", "cached"]
        assert b_props["docstring"] == "does b things"

        svc_props = nodes[SERVICE_NODE.id]
        assert svc_props["kind"] == "Service"
        assert "relpath" not in svc_props
        assert "start_line" not in svc_props
        assert "content_hash" not in svc_props

        # -- edges: CALLS a->b written with props; ghost dropped, never reachable --
        out_hops = final_store.neighbors(NODE_A_ID, ["CALLS"], "out", limit=10)
        assert len(out_hops) == 1
        edge_type, edge_props, node_dict = out_hops[0]
        assert edge_type == "CALLS"
        assert edge_props["callsite_count"] == 1
        assert edge_props["resolution"] == "static"
        assert node_dict["id"] == NODE_B_ID

        contains_hops = final_store.neighbors(SERVICE_NODE.id, ["CONTAINS"], "out", limit=10)
        assert len(contains_hops) == 1
        assert contains_hops[0][2]["id"] == NODE_A_ID

        assert final_store.get_nodes([GHOST_ID]) == []
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)
