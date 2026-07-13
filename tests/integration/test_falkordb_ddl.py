"""Интеграционный тест ddl.py + batch.py на живом FalkorDB (Step 4, брифа m1b-task-2)."""

from __future__ import annotations

import pytest

from codegraph.stores.falkordb.batch import upsert_nodes
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.ddl import ensure_schema

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
