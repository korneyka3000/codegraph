"""MCP-контракт (Step 1 брифа m1b-task-7, marker falkordb): реальный мини-граф
(Task 5 паттерн -- Staging + load_graph blue/green, см.
tests/integration/test_pipeline_load.py), build_server поверх него, in-memory
fastmcp.Client (`async with Client(build_server(cfg, graph_name)) as c: ...` --
уточнено по факту установленной fastmcp 2.14.7, см. m1b-task-7-report.md §fastmcp API
notes): list_tools == 8 имён (M2 T8: +trace_process/find_paths/list_processes/
find_entrypoint); graph_stats/who_calls живьём (буквальное требование брифа) +
get_source/expand_neighbors живьём (бонус -- дешёвая проверка amendment 2's
path-safety/staleness логики против РЕАЛЬНЫХ dict'ов из FalkorStore.get_nodes(), не
только fake store из test_query_api.py) + find_paths/list_processes/find_entrypoint
живьём на том же мини-графе + один error-dict живьём (amendment 3: структурированная
ошибка, НЕ MCP-исключение, isError остаётся False).

trace_process живьём -- отдельный тест ниже, на СВОЁМ мини-графе (3 сегмента через
event- и http-каналы, T8 test bullet: "route→calls→produce→event-chan→consumer→
invokes→activity→calls_http→http-chan→handles→handler2") -- тот же shape, что unit
на fake store в tests/unit/test_traverse.py, но здесь узлы/рёбра построены ВРУЧНУЮ
(T5-паттерн: Staging + load_graph, БЕЗ полного прогона extractors/linking -- это
T9's eval-gate) и прогнаны через РЕАЛЬНЫЙ FalkorDB, чтобы live-подтвердить: (a)
pipeline/load._node_props' "roles"-в-props фикс переживает полный staging ->
FalkorDB round trip (traverse.py читает node["roles"] из данных, которые реально
пришли через MERGE/RETURN, а не из staging-объекта в памяти теста); (b) сегменты/
каналы/exits/NEXT_SEGMENT fast-path работают против настоящего Cypher-результата,
не только in-memory fake store.
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from fastmcp import Client

from codegraph.config.models import FalkorDBConfig, ServiceConfig, StorageConfig, WorkspaceConfig
from codegraph.core.schema import (
    EdgeRec,
    NodeRec,
    make_channel_node,
    make_process_node,
    make_service_node,
)
from codegraph.mcp.schemas import (
    ErrorOutput,
    ExpandNeighborsOutput,
    FindEntrypointOutput,
    FindPathsOutput,
    GetSourceOutput,
    GraphStatsOutput,
    ListProcessesOutput,
    TraceProcessOutput,
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
                # -- список инструментов: ровно 8 имён (M2 T8 расширяет с 4 до 8) --
                tools = await c.list_tools()
                assert {t.name for t in tools} == {
                    "graph_stats",
                    "get_source",
                    "expand_neighbors",
                    "who_calls",
                    "trace_process",
                    "find_paths",
                    "list_processes",
                    "find_entrypoint",
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

                # -- M2 T8: find_paths живьём -- a -CALLS-> b, тот же мини-граф --
                fp_res = await c.call_tool(
                    "find_paths", {"from_id": NODE_A_ID, "to_id": NODE_B_ID}
                )
                assert fp_res.is_error is False
                fp_out = FindPathsOutput(**fp_res.data)  # схема валидна
                assert fp_out.path is not None
                assert [s.node["id"] for s in fp_out.path] == [NODE_A_ID, NODE_B_ID]
                assert fp_out.path[-1].edge_type == "CALLS"

                # -- M2 T8: list_processes живьём -- нет BusinessProcess-узлов в этом
                # мини-графе (тот сценарий -- отдельный тест ниже, на своём мини-графе
                # с process-якорем) -- честный пустой список, не ошибка.
                lp_res = await c.call_tool("list_processes", {})
                assert lp_res.is_error is False
                lp_out = ListProcessesOutput(**lp_res.data)  # схема валидна
                assert lp_out.processes == []

                # -- M2 T8: find_entrypoint живьём -- fulltext по Sym(qualified_name)
                # находит узел "a" (qualified_name="mod.a") реального fulltext-индекса,
                # созданного ensure_schema внутри load_graph выше.
                fe_res = await c.call_tool("find_entrypoint", {"query": "mod.a"})
                assert fe_res.is_error is False
                fe_out = FindEntrypointOutput(**fe_res.data)  # схема валидна
                assert any(r["id"] == NODE_A_ID for r in fe_out.results)

                # -- error-dict живьём (amendment 3): структурированная ошибка, НЕ
                # MCP-исключение -- isError остаётся False, полезная нагрузка -- dict
                # с ключом "error", а не traceback/ToolError.
                missing_res = await c.call_tool(
                    "get_source", {"node_id": f"sym:{SERVICE}:does-not-exist"}
                )
                assert missing_res.is_error is False
                missing_out = ErrorOutput(**missing_res.data)  # схема валидна
                assert "does-not-exist" in missing_out.error

        asyncio.run(_run())
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)


# -- M2 T8: trace_process live on a 3-segment mini-graph --

TRACE_GRAPH_NAME = "__t8trace__"
TRACE_BUILD_NAME = f"{TRACE_GRAPH_NAME}__build"
TRACE_SVC_A = "t8-orders-api"
TRACE_SVC_B = "t8-kyc-worker"
TRACE_SVC_C = "t8-doc-mgmt"


def test_mcp_contract_trace_process_live_on_three_segment_mini_graph(falkordb_cfg, tmp_path):
    # route(create_order, RouteHandler) -CALLS-> save_order -PRODUCES->
    # chan:event_type:OrderCreated <-CONSUMES- handle_order_created(MessageConsumer)
    # -CALLS(mechanism=temporal_start)-> KycWorkflow.run -INVOKES_ACTIVITY->
    # verify_documents -CALLS_HTTP-> chan:http:...GET /documents/{id} -HANDLES->
    # get_document(RouteHandler) -- hand-built nodes/edges (T5 pattern: Staging +
    # load_graph, NOT a full extractors/linking pipeline run -- see module docstring).
    create_order = NodeRec(
        id=f"sym:{TRACE_SVC_A}:create_order", kind="Function", service=TRACE_SVC_A,
        name="create_order", qualified_name="app.routes.create_order",
        roles=("RouteHandler",),
    )
    save_order = NodeRec(
        id=f"sym:{TRACE_SVC_A}:save_order", kind="Function", service=TRACE_SVC_A,
        name="save_order", qualified_name="app.routes.save_order",
    )
    chan_event = make_channel_node("event_type", "OrderCreated")
    handle_order_created = NodeRec(
        id=f"sym:{TRACE_SVC_B}:handle_order_created", kind="Function", service=TRACE_SVC_B,
        name="handle_order_created", qualified_name="app.consumers.handle_order_created",
        roles=("MessageConsumer",),
    )
    workflow_run = NodeRec(
        id=f"sym:{TRACE_SVC_B}:KycWorkflow.run", kind="Function", service=TRACE_SVC_B,
        name="run", qualified_name="app.workflows.KycWorkflow.run",
    )
    verify_documents = NodeRec(
        id=f"sym:{TRACE_SVC_B}:verify_documents", kind="Function", service=TRACE_SVC_B,
        name="verify_documents", qualified_name="app.activities.verify_documents",
        roles=("TemporalActivity",),
    )
    chan_http = make_channel_node(
        "http_route", owner_service=TRACE_SVC_C, method="GET", template="/documents/{id}",
        http_method="GET", path_template="/documents/{id}",
    )
    get_document = NodeRec(
        id=f"sym:{TRACE_SVC_C}:get_document", kind="Function", service=TRACE_SVC_C,
        name="get_document", qualified_name="app.routes.get_document",
        roles=("RouteHandler",),
    )
    service_nodes = [make_service_node(s) for s in (TRACE_SVC_A, TRACE_SVC_B, TRACE_SVC_C)]
    proc = make_process_node("order-kyc", "Order KYC onboarding", create_order.id, "config")

    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([
        create_order, save_order, chan_event, handle_order_created, workflow_run,
        verify_documents, chan_http, get_document, *service_nodes, proc,
    ])
    st.upsert_edges([
        EdgeRec(src=create_order.id, dst=save_order.id, type="CALLS", resolution="static",
                confidence=1.0, extractor="calls"),
        EdgeRec(src=save_order.id, dst=chan_event.id, type="PRODUCES", resolution="static",
                confidence=1.0, extractor="kafka"),
        EdgeRec(src=handle_order_created.id, dst=chan_event.id, type="CONSUMES",
                resolution="static", confidence=1.0, extractor="kafka"),
        EdgeRec(src=handle_order_created.id, dst=workflow_run.id, type="CALLS",
                resolution="dynamic", confidence=1.0, extractor="temporal",
                props={"mechanism": "temporal_start"}),
        EdgeRec(src=workflow_run.id, dst=verify_documents.id, type="INVOKES_ACTIVITY",
                resolution="static", confidence=1.0, extractor="temporal"),
        EdgeRec(src=verify_documents.id, dst=chan_http.id, type="CALLS_HTTP",
                resolution="static", confidence=1.0, extractor="http_client"),
        EdgeRec(src=chan_http.id, dst=get_document.id, type="HANDLES", resolution="static",
                confidence=1.0, extractor="fastapi"),
        # fast-path NEXT_SEGMENT edges, exactly as linking/segments.derive() would
        # produce them (src = the node with the actual PRODUCES/CALLS_HTTP edge).
        EdgeRec(src=save_order.id, dst=handle_order_created.id, type="NEXT_SEGMENT",
                resolution="static", confidence=1.0, extractor="linking",
                props={"via_channel_id": chan_event.id, "derived": True}),
        EdgeRec(src=verify_documents.id, dst=get_document.id, type="NEXT_SEGMENT",
                resolution="static", confidence=1.0, extractor="linking",
                props={"via_channel_id": chan_http.id, "derived": True}),
        EdgeRec(src=create_order.id, dst=proc.id, type="PART_OF_PROCESS", resolution="static",
                confidence=1.0, extractor="linking", props={"order": 0}),
    ])

    cfg = WorkspaceConfig(
        graph_name=TRACE_GRAPH_NAME, storage=StorageConfig(falkordb=falkordb_cfg),
        services=[
            ServiceConfig(name=s, path=tmp_path) for s in (TRACE_SVC_A, TRACE_SVC_B, TRACE_SVC_C)
        ],
    )

    try:
        load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), TRACE_GRAPH_NAME)
        server = build_server(cfg, TRACE_GRAPH_NAME)

        async def _run() -> None:
            async with Client(server) as c:
                trace_res = await c.call_tool(
                    "trace_process", {"entrypoint_id": create_order.id}
                )
                assert trace_res.is_error is False
                trace_out = TraceProcessOutput(**trace_res.data)  # схема валидна
                assert len(trace_out.segments) == 3
                seg0, seg1, seg2 = trace_out.segments

                assert seg0.service == TRACE_SVC_A
                # roles live-proof (self-review item): pipeline/load._node_props'
                # "roles"-в-props фикс должен пережить ПОЛНЫЙ staging -> FalkorDB
                # round trip -- это НЕ то же самое, что unit на fake store
                # (tests/unit/test_traverse.py), там roles кладутся в fake store
                # напрямую, минуя _node_props/MERGE/RETURN вовсе.
                assert seg0.entry.get("roles") == ["RouteHandler"]
                assert [s.edge_type for s in seg0.steps] == ["CALLS"]
                assert seg0.exits[0].channel["id"] == chan_event.id
                assert seg0.exits[0].next_entry_ids == [handle_order_created.id]

                assert seg1.service == TRACE_SVC_B
                assert seg1.entry.get("roles") == ["MessageConsumer"]
                step_types = {s.edge_type for s in seg1.steps}
                assert step_types == {"CALLS", "INVOKES_ACTIVITY"}
                temporal_step = next(s for s in seg1.steps if s.edge_type == "CALLS")
                assert temporal_step.props.get("mechanism") == "temporal_start"
                assert temporal_step.node.get("roles") is None  # KycWorkflow.run has no roles
                assert seg1.exits[0].channel["id"] == chan_http.id
                assert seg1.exits[0].next_entry_ids == [get_document.id]

                assert seg2.service == TRACE_SVC_C
                assert seg2.entry["id"] == get_document.id
                assert seg2.entry.get("roles") == ["RouteHandler"]
                assert seg2.steps == []
                assert seg2.exits == []

                assert trace_out.truncated is False
                assert 0.0 < trace_out.confidence <= 1.0

                # -- list_processes live: the config-sourced anchor from this graph --
                lp_res = await c.call_tool("list_processes", {})
                assert lp_res.is_error is False
                lp_out = ListProcessesOutput(**lp_res.data)
                assert {p.id for p in lp_out.processes} == {proc.id}
                found_proc = next(p for p in lp_out.processes if p.id == proc.id)
                assert found_proc.entrypoint_id == create_order.id
                assert found_proc.source == "config"

                # -- find_paths live: create_order -> get_document across all 3
                # segments (CALLS + NEXT_SEGMENT fast-path hops, direction="both") --
                fp_res = await c.call_tool(
                    "find_paths", {"from_id": create_order.id, "to_id": get_document.id}
                )
                assert fp_res.is_error is False
                fp_out = FindPathsOutput(**fp_res.data)
                assert fp_out.path is not None
                assert fp_out.path[0].node["id"] == create_order.id
                assert fp_out.path[-1].node["id"] == get_document.id

        asyncio.run(_run())
    finally:
        _cleanup(falkordb_cfg, TRACE_BUILD_NAME, TRACE_GRAPH_NAME)
