"""Интеграционный тест ddl.py + batch.py на живом FalkorDB (Step 4, брифа m1b-task-2;
M2 T8 добавляет fulltext-индекс + store.search_fulltext поверх него)."""

from __future__ import annotations

import pytest

from codegraph.stores.falkordb.batch import upsert_nodes
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.ddl import ensure_schema
from codegraph.stores.falkordb.store import FalkorStore

pytestmark = pytest.mark.falkordb

GRAPH = "__codegraph_t2__"


def test_ensure_schema_idempotent_and_upsert_nodes_merges_by_id(falkordb_cfg):
    db = connect(falkordb_cfg)
    g = db.select_graph(GRAPH)
    try:
        ensure_schema(db, GRAPH)
        ensure_schema(db, GRAPH)  # второй вызов не должен падать -- идемпотентность

        written1 = upsert_nodes(g, ("Sym",), [{"id": "sym:a:1", "props": {"name": "first"}}])
        written2 = upsert_nodes(g, ("Sym",), [{"id": "sym:a:1", "props": {"name": "second"}}])
        assert written1 == 1
        assert written2 == 1

        res = g.query("MATCH (n:Sym {id: 'sym:a:1'}) RETURN count(n), n.name")
        count, name = res.result_set[0]
        assert count == 1  # MERGE -- один узел, не два
        assert name == "second"  # второй upsert реально применился (SET n += r.props)
    finally:
        g.delete()


# -- M2 T8: fulltext index (Sym.name/.qualified_name/.docstring) + search_fulltext --

FULLTEXT_GRAPH = "__codegraph_t8_fulltext__"


def test_ensure_schema_creates_fulltext_index_idempotently_and_search_fulltext_finds_nodes(
    falkordb_cfg,
):
    store = FalkorStore(falkordb_cfg, FULLTEXT_GRAPH)
    try:
        store.ensure_schema()
        store.ensure_schema()  # idempotent -- "already indexed"-class error swallowed

        store.upsert_nodes(("Sym", "Function"), [
            {
                "id": "sym:a:create_order",
                "props": {
                    "kind": "Function", "name": "create_order",
                    "qualified_name": "app.routes.orders.create_order",
                    "docstring": "Creates a new order and emits OrderCreated.",
                },
            },
            {
                "id": "sym:a:get_order",
                "props": {
                    "kind": "Function", "name": "get_order",
                    "qualified_name": "app.routes.orders.get_order",
                    "docstring": "Fetches an order by id.",
                },
            },
        ])

        results = store.search_fulltext("create order", k=5)
        assert any(r["id"] == "sym:a:create_order" for r in results)
        assert all("score" in r for r in results)

        # kinds filter (property, not label) narrows/empties results
        assert store.search_fulltext("create order", k=5, kinds=["Class"]) == []
        assert any(
            r["id"] == "sym:a:create_order"
            for r in store.search_fulltext("create order", k=5, kinds=["Function"])
        )

        # RediSearch special chars sanitized to space, not left to raise a syntax
        # error -- "orders-api" style dotted/hyphenated queries still find matches.
        assert any(
            r["id"] == "sym:a:create_order"
            for r in store.search_fulltext("app.routes.orders.create_order", k=5)
        )

        # sanitizes to "" -- short-circuits to [] rather than a RediSearch call
        assert store.search_fulltext("@{}~*", k=5) == []
    finally:
        store._g.delete()


# -- M3 T6: Chunk vector index (only when dim is given) + fulltext(text, context_header) --

VECTOR_GRAPH = "__codegraph_t6_vector__"


def test_ensure_schema_creates_chunk_vector_index_only_when_dim_given(falkordb_cfg):
    store = FalkorStore(falkordb_cfg, VECTOR_GRAPH)
    try:
        store.ensure_schema(dim=4)
        store.ensure_schema(dim=4)  # idempotent -- "already indexed"-class error swallowed

        vec = [0.1, 0.2, 0.3, 0.4]
        store._g.query(
            "MERGE (c:Chunk {id: 'chunk:1'}) SET c.embedding = vecf32($v)", {"v": vec}
        )
        res = store._g.query(
            "CALL db.idx.vector.queryNodes('Chunk', 'embedding', 1, vecf32($v)) "
            "YIELD node RETURN node.id",
            {"v": vec},
        )
        assert res.result_set == [["chunk:1"]]
    finally:
        store._g.delete()


NO_DIM_GRAPH = "__codegraph_t6_no_dim__"


def test_ensure_schema_without_dim_creates_no_vector_index(falkordb_cfg):
    """dim=None (embedder skipped this run, e.g. --no-embed) -- ensure_schema must not
    even attempt CREATE VECTOR INDEX; querying Chunk.embedding without one is a
    RediSearch/FalkorDB error, which is exactly the honest signal wanted here (no
    silently-empty vector index masquerading as "search ran, found nothing")."""
    store = FalkorStore(falkordb_cfg, NO_DIM_GRAPH)
    try:
        store.ensure_schema(dim=None)
        with pytest.raises(Exception):  # noqa: B017 -- exact FalkorDB error text not pinned
            store._g.query(
                "CALL db.idx.vector.queryNodes('Chunk', 'embedding', 1, vecf32($v)) "
                "YIELD node RETURN node.id",
                {"v": [0.1, 0.2, 0.3, 0.4]},
            )
    finally:
        store._g.delete()


CHUNK_FULLTEXT_GRAPH = "__codegraph_t6_chunk_fulltext__"


def test_ensure_schema_creates_chunk_fulltext_index_and_finds_by_context_header(
    falkordb_cfg,
):
    store = FalkorStore(falkordb_cfg, CHUNK_FULLTEXT_GRAPH)
    try:
        store.ensure_schema()
        store.ensure_schema()  # idempotent

        store.upsert_nodes(("Chunk",), [
            {
                "id": "chunk:1",
                "props": {
                    "text": "def create_order(): ...",
                    "context_header": "symbol: app.routes.orders.create_order (Function)",
                },
            },
        ])

        res = store._g.query(
            "CALL db.idx.fulltext.queryNodes('Chunk', 'create_order') YIELD node RETURN node.id"
        )
        assert res.result_set == [["chunk:1"]]
    finally:
        store._g.delete()


# -- M3 T7: store.search_vector_chunks / store.search_text_chunks -- the same Cypher
# shapes as the raw-query tests above, now promoted to proper store methods (retrieval.py's
# actual read path) -- score ordering (vector ASC=nearest-first / text DESC=most-relevant-
# first), the service filter's over-fetch, and the no-vector-index -> [] degradation.

VECTOR_METHOD_GRAPH = "__codegraph_t7_vector_method__"


def test_search_vector_chunks_orders_nearest_first_by_cosine_distance(falkordb_cfg):
    store = FalkorStore(falkordb_cfg, VECTOR_METHOD_GRAPH)
    try:
        store.ensure_schema(dim=4)
        store.upsert_nodes(
            ("Chunk",),
            [
                {"id": "near", "props": {"service": "svc-a"}},
                {"id": "far", "props": {"service": "svc-a"}},
            ],
            vector_props=("embedding",),
        )
        # upsert_nodes' vector_props path needs the vector INSIDE the row (not props) --
        # simplest here is a direct MERGE since it's just 2 hand-crafted vectors.
        store._g.query(
            "MERGE (c:Chunk {id: 'near'}) SET c.embedding = vecf32($v)",
            {"v": [0.9, 0.05, 0.05, 0.0]},
        )
        store._g.query(
            "MERGE (c:Chunk {id: 'far'}) SET c.embedding = vecf32($v)",
            {"v": [0.0, 1.0, 0.0, 0.0]},
        )

        results = store.search_vector_chunks([1.0, 0.0, 0.0, 0.0], k=2)
        assert [props["id"] for props, _score in results] == ["near", "far"]
        # score ASC (nearest first): "near"'s distance must be strictly smaller.
        assert results[0][1] < results[1][1]
    finally:
        store._g.delete()


def test_search_vector_chunks_service_filter_overfetches_and_trims_to_k(falkordb_cfg):
    store = FalkorStore(falkordb_cfg, VECTOR_METHOD_GRAPH)
    try:
        store.ensure_schema(dim=4)
        for cid, svc, vec in (
            ("a1", "svc-a", [1.0, 0.0, 0.0, 0.0]),
            ("a2", "svc-a", [0.9, 0.1, 0.0, 0.0]),
            ("b1", "svc-b", [0.95, 0.05, 0.0, 0.0]),  # closer than a2, but wrong service
        ):
            store._g.query(
                "MERGE (c:Chunk {id: $id}) SET c.service = $svc, c.embedding = vecf32($v)",
                {"id": cid, "svc": svc, "v": vec},
            )

        # k=2 with NO filter would naturally include b1 (2nd closest overall) -- the
        # service filter must still return BOTH real svc-a chunks despite that.
        results = store.search_vector_chunks([1.0, 0.0, 0.0, 0.0], k=2, service="svc-a")
        assert {props["id"] for props, _score in results} == {"a1", "a2"}
        assert len(results) == 2
    finally:
        store._g.delete()


NO_VECTOR_INDEX_METHOD_GRAPH = "__codegraph_t7_no_vector_method__"


def test_search_vector_chunks_returns_empty_list_not_exception_without_an_index(
    falkordb_cfg,
):
    """The degraded-graph contract this task's brief calls for validating live: a
    graph that has never been embedded (ensure_schema(dim=None), see
    test_ensure_schema_without_dim_creates_no_vector_index above for proof the RAW
    Cypher call raises) must make search_vector_chunks behave like an honest
    zero-result search, not an exception -- callers (retrieval.py) shouldn't need
    their own try/except around every vector search just to handle "not embedded yet"."""
    store = FalkorStore(falkordb_cfg, NO_VECTOR_INDEX_METHOD_GRAPH)
    try:
        store.ensure_schema(dim=None)
        assert store.search_vector_chunks([0.1, 0.2, 0.3, 0.4], k=5) == []
    finally:
        store._g.delete()


TEXT_METHOD_GRAPH = "__codegraph_t7_text_method__"


def test_search_text_chunks_orders_by_relevance_desc_and_respects_service_filter(
    falkordb_cfg,
):
    store = FalkorStore(falkordb_cfg, TEXT_METHOD_GRAPH)
    try:
        store.ensure_schema()
        store.upsert_nodes(("Chunk",), [
            {
                "id": "chunk:strong", "props": {
                    "service": "svc-a",
                    "text": "widget widget widget widget process order",
                },
            },
            {
                "id": "chunk:weak", "props": {
                    "service": "svc-a", "text": "widget helper utility",
                },
            },
            {
                "id": "chunk:other-service", "props": {
                    "service": "svc-b", "text": "widget widget widget widget widget",
                },
            },
        ])

        results = store.search_text_chunks("widget", k=5)
        ids = {props["id"] for props, _score in results}
        assert ids == {"chunk:strong", "chunk:weak", "chunk:other-service"}

        filtered = store.search_text_chunks("widget", k=5, service="svc-a")
        assert {props["id"] for props, _score in filtered} == {"chunk:strong", "chunk:weak"}
        # ORDER BY score DESC is real (RediSearch's default scorer normalizes by field
        # length -- empirically it favors "widget"'s higher TERM DENSITY in the
        # shorter "chunk:weak" text over "chunk:strong"'s higher raw COUNT, so this
        # deliberately doesn't hardcode which one wins, only that scores actually
        # come back sorted descending).
        assert len(filtered) == 2
        assert filtered[0][1] >= filtered[1][1]

        assert store.search_text_chunks("@{}~*", k=5) == []  # sanitizes to "" -- no call
    finally:
        store._g.delete()
