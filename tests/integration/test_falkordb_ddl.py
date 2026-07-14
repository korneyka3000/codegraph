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
