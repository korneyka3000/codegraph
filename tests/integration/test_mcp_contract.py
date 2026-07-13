"""MCP-контракт (Step 1 брифа m1b-task-7, marker falkordb): реальный мини-граф
(Task 5 паттерн -- Staging + load_graph blue/green, см.
tests/integration/test_pipeline_load.py), build_server поверх него, in-memory
fastmcp.Client (`async with Client(build_server(cfg, graph_name)) as c: ...` --
уточнено по факту установленной fastmcp 2.14.7, см. m1b-task-7-report.md §fastmcp API
notes): list_tools == 4 имени; graph_stats/who_calls живьём (буквальное требование
брифа) + get_source/expand_neighbors живьём (бонус -- дешёвая проверка amendment 2's
path-safety/staleness логики против РЕАЛЬНЫХ dict'ов из FalkorStore.get_nodes(), не
только fake store из test_query_api.py) + один error-dict живьём (amendment 3:
структурированная ошибка, НЕ MCP-исключение, isError остаётся False).
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from fastmcp import Client

from codegraph.config.models import FalkorDBConfig, ServiceConfig, StorageConfig, WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_service_node
from codegraph.mcp.schemas import (
    ExpandNeighborsOutput,
    GetSourceOutput,
    GraphStatsOutput,
    WhoCallsOutput,
)
from codegraph.mcp.server import build_server
from codegraph.pipeline.load import load_graph
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.falkordb

GRAPH_NAME = "__t7__"
BUILD_NAME = f"{GRAPH_NAME}__build"
SERVICE = "t7svc"

NODE_A_ID = f"sym:{SERVICE}:a"
NODE_B_ID = f"sym:{SERVICE}:b"


def _cleanup(cfg: FalkorDBConfig, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # swap_in уже мог унести build-ключ через RENAME (см. T5 _cleanup)


def test_mcp_contract_list_tools_and_live_tool_calls(falkordb_cfg, tmp_path):
    # -- мини-граф: a() calls b(), Service CONTAINS a (Task 5 паттерн) --
    root = tmp_path / SERVICE
    content = b"def a():\n    b()\n"
    mod_path = root / "mod.py"
    mod_path.parent.mkdir(parents=True, exist_ok=True)
    mod_path.write_bytes(content)

    a_start, a_end = 0, len(b"def a():\n    b()")
    a_hash = hashlib.sha256(content[a_start:a_end]).hexdigest()

    node_a = NodeRec(
        id=NODE_A_ID, kind="Function", service=SERVICE, name="a", qualified_name="mod.a",
        relpath="mod.py", start_byte=a_start, end_byte=a_end, start_line=1, end_line=2,
        content_hash=a_hash,
    )
    node_b = NodeRec(
        id=NODE_B_ID, kind="Function", service=SERVICE, name="b", qualified_name="mod.b",
        relpath="mod.py", start_byte=13, end_byte=16, start_line=2, end_line=2,
        content_hash=hashlib.sha256(content[13:16]).hexdigest(),
    )
    service_node = make_service_node(SERVICE)
    edge_calls = EdgeRec(
        src=NODE_A_ID, dst=NODE_B_ID, type="CALLS", resolution="static", confidence=1.0,
        extractor="calls",
    )
    edge_contains = EdgeRec(
        src=service_node.id, dst=NODE_A_ID, type="CONTAINS", resolution="static",
        confidence=1.0, extractor="python_core",
    )

    st = Staging(tmp_path / "s.db")
    st.begin_service(SERVICE)
    st.upsert_nodes([node_a, node_b, service_node])
    st.upsert_edges([edge_calls, edge_contains])

    cfg = WorkspaceConfig(
        graph_name=GRAPH_NAME,
        storage=StorageConfig(falkordb=falkordb_cfg),
        services=[ServiceConfig(name=SERVICE, path=root)],
    )

    try:
        load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        server = build_server(cfg, GRAPH_NAME)

        async def _run() -> None:
            async with Client(server) as c:
                # -- список инструментов: ровно 4 имени (буквальное требование брифа) --
                tools = await c.list_tools()
                assert {t.name for t in tools} == {
                    "graph_stats",
                    "get_source",
                    "expand_neighbors",
                    "who_calls",
                }

                # -- graph_stats живьём (буквальное требование брифа) --
                stats_res = await c.call_tool("graph_stats", {})
                assert stats_res.is_error is False
                stats_out = GraphStatsOutput(**stats_res.data)  # схема валидна
                assert stats_out.nodes.get("Function") == 2
                assert stats_out.edges.get("CALLS") == 1
                assert stats_out.edges.get("CONTAINS") == 1

                # -- who_calls живьём (буквальное требование брифа): b вызывается a-й --
                who_res = await c.call_tool("who_calls", {"node_id": NODE_B_ID})
                assert who_res.is_error is False
                who_out = WhoCallsOutput(**who_res.data)  # схема валидна
                assert who_out.truncated is False
                assert {caller["id"] for caller in who_out.callers} == {NODE_A_ID}

                # -- get_source живьём: реальные relpath/start_byte/content_hash из
                # FalkorStore.get_nodes(), реальный файл на диске (amendment 2) --
                src_res = await c.call_tool("get_source", {"node_id": NODE_A_ID})
                assert src_res.is_error is False
                src_out = GetSourceOutput(**src_res.data)  # схема валидна
                assert src_out.stale is False
                assert src_out.source == "def a():\n    b()"

                # -- expand_neighbors живьём: a -CALLS-> b (direction=out) --
                expand_res = await c.call_tool(
                    "expand_neighbors", {"node_id": NODE_A_ID, "direction": "out"}
                )
                assert expand_res.is_error is False
                expand_out = ExpandNeighborsOutput(**expand_res.data)  # схема валидна
                assert {n["id"] for n in expand_out.nodes} == {NODE_B_ID}
                assert expand_out.truncated is False

                # -- error-dict живьём (amendment 3): структурированная ошибка, НЕ
                # MCP-исключение -- isError остаётся False, полезная нагрузка -- dict
                # с ключом "error", а не traceback/ToolError.
                missing_res = await c.call_tool(
                    "get_source", {"node_id": f"sym:{SERVICE}:does-not-exist"}
                )
                assert missing_res.is_error is False
                assert "error" in missing_res.data

        asyncio.run(_run())
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)
