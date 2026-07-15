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
