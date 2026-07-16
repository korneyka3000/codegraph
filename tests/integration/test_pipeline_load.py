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

import struct

import pytest

from codegraph.chunking.splitter import ChunkRec
from codegraph.core.schema import (
    SCHEMA_VERSION,
    EdgeRec,
    NodeRec,
    make_channel_node,
    make_process_node,
    make_service_node,
)
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
        # M3 T6: every load_graph run ALWAYS writes exactly one Meta node (label
        # ("Meta",), id "meta") -- see pipeline/load.py's module docstring -- so the
        # node counts here are +1 versus the pre-M3-T6 shape (3 -> 4), even though
        # this staging has zero chunks and no embedder was ever involved.
        assert stats["nodes_written"] == 4
        assert stats["nodes_written_by_label"] == {
            "Sym:Function": 2, "Service": 1, "Meta": 1,
        }
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
            "meta": {"Meta"},
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
        edge_type, edge_props, node_dict, direction = out_hops[0]
        assert edge_type == "CALLS"
        assert edge_props["callsite_count"] == 1
        assert edge_props["resolution"] == "static"
        assert node_dict["id"] == NODE_B_ID
        assert direction == "out"

        contains_hops = final_store.neighbors(SERVICE_NODE.id, ["CONTAINS"], "out", limit=10)
        assert len(contains_hops) == 1
        assert contains_hops[0][2]["id"] == NODE_A_ID
        assert contains_hops[0][3] == "out"

        assert final_store.get_nodes([GHOST_ID]) == []
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)


STALE_GRAPH = "__t5x__"
STALE_BUILD = f"{STALE_GRAPH}__build"


def test_load_graph_resets_stale_build_graph_from_crashed_run(falkordb_cfg, tmp_path):
    """Регрессия дыры корректности из первичного ревью T5: предыдущий прогон, упавший
    ПОСЛЕ частичной записи в build-граф, но ДО swap_in, оставляет мусор под build-ключом
    (RENAME не состоялся -- ключ жив). load_graph обязан начинать с чистого build-графа
    (build_store.delete_graph() первым делом), иначе мусор протекает в финальный граф
    после swap -- живьём воспроизведено до фикса (см. m1b-task-5-report §Fix)."""
    db = connect(falkordb_cfg)
    # симулируем упавший прогон: непустой build-ключ с посторонним узлом
    db.select_graph(STALE_BUILD).query(
        "MERGE (n:Junk {id: 'stale-from-crash'}) SET n.kind = 'Junk'"
    )

    st = _staging(tmp_path)
    try:
        stats = load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), STALE_GRAPH)

        final_store = FalkorStore(falkordb_cfg, STALE_GRAPH)
        # мусор упавшего прогона НЕ протёк в финальный граф
        assert final_store.get_nodes(["stale-from-crash"]) == []
        # а реальное содержимое staging -- протекло целиком и ровно оно (+1 always-on
        # Meta node, M3 T6 -- see the other test in this file for why)
        assert stats["nodes_written"] == 4
        rows = final_store.raw("MATCH (n) RETURN n.id").result_set
        assert {r[0] for r in rows} == {NODE_A_ID, NODE_B_ID, SERVICE_NODE.id, "meta"}
    finally:
        _cleanup(falkordb_cfg, STALE_BUILD, STALE_GRAPH)


ROLES_GRAPH = "__t5roles__"
ROLES_BUILD = f"{ROLES_GRAPH}__build"


def test_load_graph_writes_role_multilabel_and_channel_business_process_labels(
    falkordb_cfg, tmp_path,
):
    """M2: _labels_for_kind's новые ветки (roles-appended multi-label для кодовых
    узлов; Channel/BusinessProcess как однословные labels) против РЕАЛЬНОГО FalkorDB
    -- доказывает, что MERGE (n:Sym:Function:RouteHandler {...}) (3+ labels, не
    только 2, как в остальных тестах этого файла) действительно создаёт узел с
    ожидаемым набором labels(), а не молча падает/схлопывает часть меток."""
    st = Staging(tmp_path / "s.db")
    st.begin_service("t5rsvc")
    handler = NodeRec(
        id="sym:t5rsvc:`app`/handle().", kind="Function", service="t5rsvc",
        name="handle", qualified_name="app.handle", relpath="app.py",
        start_byte=0, end_byte=10, start_line=1, end_line=2, content_hash="h",
        roles=("RouteHandler",),
    )
    chan = make_channel_node("kafka_topic", "orders.created")
    proc = make_process_node(
        "place-order", "Place Order", entrypoint_id=handler.id, source="config",
    )
    st.upsert_nodes([handler, chan, proc])

    try:
        load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), ROLES_GRAPH)

        final_store = FalkorStore(falkordb_cfg, ROLES_GRAPH)
        rows = final_store.raw("MATCH (n) RETURN n.id, labels(n)").result_set
        labels_by_id = {row[0]: set(row[1]) for row in rows}
        assert labels_by_id[handler.id] == {"Sym", "Function", "RouteHandler"}
        assert labels_by_id[chan.id] == {"Channel"}
        assert labels_by_id[proc.id] == {"BusinessProcess"}

        chan_props = final_store.get_nodes([chan.id])[0]
        assert chan_props["name"] == "orders.created"
        proc_props = final_store.get_nodes([proc.id])[0]
        assert proc_props["entrypoint_id"] == handler.id
    finally:
        _cleanup(falkordb_cfg, ROLES_BUILD, ROLES_GRAPH)


# -- M3 T6: Chunk nodes (embedding via vecf32) + fulltext(text, context_header) +
# Meta node, against a REAL FalkorDB (this task's own self-review requirement: vecf32
# actually round-trips through db.idx.vector.queryNodes, not just doctor's own probe) --

CHUNK_GRAPH = "__t6chunks__"
CHUNK_BUILD = f"{CHUNK_GRAPH}__build"

CHUNK_1 = ChunkRec(
    chunk_id="sym:c:`app`/create_order().#c0", symbol_id="sym:c:`app`/create_order().",
    ord=0, text="def create_order(): ...", start_line=1, end_line=1,
    content_hash="hash-create-order",
)
CHUNK_2 = ChunkRec(
    chunk_id="sym:c:`app`/unrelated().#c0", symbol_id="sym:c:`app`/unrelated().",
    ord=0, text="def unrelated(): ...", start_line=3, end_line=3,
    content_hash="hash-unrelated",
)
CHUNK_3_NOT_EMBEDDED = ChunkRec(
    chunk_id="sym:c:`app`/not_embedded().#c0", symbol_id="sym:c:`app`/not_embedded().",
    ord=0, text="def not_embedded(): ...", start_line=5, end_line=5,
    content_hash="hash-not-embedded",
)

# Hand-constructed (not FakeEmbedder -- see its own docstring: its vectors carry no
# semantic relationship between similar texts, only a controlled numeric one is needed
# here) so cosine distance is fully predictable: QUERY_VEC is close to CHUNK_1's own
# vector and far from CHUNK_2's.
CHUNK_1_VEC = [1.0, 0.0, 0.0, 0.0]
CHUNK_2_VEC = [0.0, 1.0, 0.0, 0.0]
QUERY_VEC = [0.9, 0.05, 0.05, 0.0]


def _vec_blob(vec: list[float]) -> bytes:
    return struct.pack(f"<{len(vec)}f", *vec)


def _chunk_staging(tmp_path) -> Staging:
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("c", "app/orders.py", [CHUNK_1, CHUNK_2, CHUNK_3_NOT_EMBEDDED])
    st.set_embeddings([
        (CHUNK_1.chunk_id, _vec_blob(CHUNK_1_VEC), "test-model", CHUNK_1.content_hash),
        (CHUNK_2.chunk_id, _vec_blob(CHUNK_2_VEC), "test-model", CHUNK_2.content_hash),
    ])
    st.set_context_headers([
        (CHUNK_1.chunk_id, "symbol: app.create_order (Function) · roles: RouteHandler"),
        (CHUNK_2.chunk_id, "symbol: app.unrelated (Function)"),
        (CHUNK_3_NOT_EMBEDDED.chunk_id, "symbol: app.not_embedded (Function)"),
    ])
    st.set_meta("embed_model", "test-model")
    st.set_meta("embed_dim", "4")
    return st


def test_load_graph_writes_chunk_nodes_with_vecf32_embedding_and_meta_node(
    falkordb_cfg, tmp_path,
):
    st = _chunk_staging(tmp_path)
    try:
        stats = load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), CHUNK_GRAPH)

        # -- load_graph's own return dict: Chunk/Meta folded into the SAME
        # nodes_written/nodes_written_by_label accounting as every other node kind --
        assert stats["nodes_written"] == 4  # 3 chunks + 1 meta
        assert stats["nodes_written_by_label"] == {"Chunk": 3, "Meta": 1}

        final_store = FalkorStore(falkordb_cfg, CHUNK_GRAPH)

        # -- Chunk node props (brief's own field list) --
        chunk1_props = final_store.get_nodes([CHUNK_1.chunk_id])[0]
        # "kind": "Chunk" -- FalkorStore.stats()/get_nodes_by_kind group/filter by
        # this PROPERTY (Cypher labels can't be parameterized); a missing "kind"
        # here breaks `codegraph stats`'s sorted() the moment a graph has both a
        # kind-bearing node and a Chunk node (live-verified regression, now fixed).
        assert chunk1_props["kind"] == "Chunk"
        assert chunk1_props["symbol_id"] == CHUNK_1.symbol_id
        assert chunk1_props["service"] == "c"
        assert chunk1_props["relpath"] == "app/orders.py"
        assert chunk1_props["ord"] == 0
        assert chunk1_props["start_line"] == 1
        assert chunk1_props["end_line"] == 1
        assert chunk1_props["content_hash"] == CHUNK_1.content_hash
        assert chunk1_props["text"] == "def create_order(): ..."
        assert chunk1_props["context_header"].startswith("symbol: app.create_order")
        assert chunk1_props["embed_model"] == "test-model"

        # -- a chunk with NO embedding still loads -- no "embedding" property key at
        # all (never a null one: `vecf32(NULL)` doesn't error, live-verified, it just
        # silently never sets the property -- see pipeline/load.py's own module
        # docstring for why the two-call split is kept anyway, for row-shape clarity
        # and the embed_model-stripping it lines up with, not error avoidance) --
        chunk3_props = final_store.get_nodes([CHUNK_3_NOT_EMBEDDED.chunk_id])[0]
        assert "embedding" not in chunk3_props
        assert "embed_model" not in chunk3_props
        assert chunk3_props["text"] == "def not_embedded(): ..."

        # -- THE live sanity check this task's self-review calls for: vecf32-encoded
        # embedding actually round-trips through db.idx.vector.queryNodes, returning
        # the nearest inserted vector for a query vector close to it --
        res = final_store.raw(
            "CALL db.idx.vector.queryNodes('Chunk', 'embedding', 2, vecf32($v)) "
            "YIELD node, score RETURN node.id, score ORDER BY score",
            {"v": QUERY_VEC},
        )
        nearest_ids = [row[0] for row in res.result_set]
        assert nearest_ids[0] == CHUNK_1.chunk_id  # closest by cosine, not CHUNK_2

        # -- fulltext(text, context_header) finds a chunk by a context_header-only
        # term (CHUNK_1's own role annotation, not present in its "text" at all) --
        fulltext_res = final_store.raw(
            "CALL db.idx.fulltext.queryNodes('Chunk', 'RouteHandler') YIELD node "
            "RETURN node.id"
        )
        assert [row[0] for row in fulltext_res.result_set] == [CHUNK_1.chunk_id]

        # -- fulltext also finds the NOT-embedded chunk -- text/context_header are
        # independent of whether a chunk ever got an embedding --
        fulltext_res_3 = final_store.raw(
            "CALL db.idx.fulltext.queryNodes('Chunk', 'not_embedded') YIELD node "
            "RETURN node.id"
        )
        assert [row[0] for row in fulltext_res_3.result_set] == [
            CHUNK_3_NOT_EMBEDDED.chunk_id
        ]

        # -- Meta node: embed_model/dim (from staging meta) + schema_version (constant) --
        meta_props = final_store.get_nodes(["meta"])[0]
        assert meta_props["kind"] == "Meta"
        assert meta_props["embed_model"] == "test-model"
        assert meta_props["dim"] == 4
        assert meta_props["schema_version"] == SCHEMA_VERSION

        # -- stats() groups by n.kind: Chunk/Meta must show up as their OWN kind
        # buckets, never merged into a `None` bucket (regression: cli.py's `stats`
        # command sorts this dict's keys, which raises TypeError comparing None to a
        # real kind string the moment a graph has both -- true for every real
        # workspace, since Meta is always written) --
        stats = final_store.stats()
        assert stats["nodes"]["Chunk"] == 3
        assert stats["nodes"]["Meta"] == 1
        assert None not in stats["nodes"]
        sorted(stats["nodes"])  # must not raise TypeError (str vs NoneType)
    finally:
        _cleanup(falkordb_cfg, CHUNK_BUILD, CHUNK_GRAPH)


NO_EMBED_CHUNK_GRAPH = "__t6chunks_noembed__"
NO_EMBED_CHUNK_BUILD = f"{NO_EMBED_CHUNK_GRAPH}__build"


def test_load_graph_meta_omits_embed_model_and_dim_when_never_embedded(
    falkordb_cfg, tmp_path,
):
    """No embedder was ever run against this staging (embed_model/embed_dim meta keys
    were never set at all) -- Meta must still be written (schema_version only), and
    ensure_schema must not attempt to create a vector index with no dimension to size
    it (dim=None -- see ddl.ensure_schema's own docstring)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("c", "app/x.py", [CHUNK_3_NOT_EMBEDDED])
    st.set_context_headers([(CHUNK_3_NOT_EMBEDDED.chunk_id, "symbol: app.not_embedded")])
    try:
        load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), NO_EMBED_CHUNK_GRAPH)

        final_store = FalkorStore(falkordb_cfg, NO_EMBED_CHUNK_GRAPH)
        meta_props = final_store.get_nodes(["meta"])[0]
        assert "embed_model" not in meta_props
        assert "dim" not in meta_props
        assert meta_props["schema_version"] == SCHEMA_VERSION
    finally:
        _cleanup(falkordb_cfg, NO_EMBED_CHUNK_BUILD, NO_EMBED_CHUNK_GRAPH)
