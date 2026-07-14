"""FastMCP сервер: 8 read-only инструментов (M1 v0: graph_stats/get_source/
expand_neighbors/who_calls; M2 T8: trace_process/find_paths/list_processes/
find_entrypoint), тонкая делегация в query.api.GraphQuery -- ни одного Cypher-
запроса и ни одного обращения к GraphStore.raw() в этом модуле (см. stores/graph.py:
GraphStore.raw() docstring -- "internal-only", не для MCP).

Локальная переменная -- `gq` (не `query`): find_entrypoint's собственный
параметр -- ИМЕННО `query: str` (см. mcp/schemas.py FindEntrypointInput /
query.api.GraphQuery.find_entrypoint) -- назови эту переменную `query`, и
find_entrypoint's тело закрыло бы её собственным строковым параметром
(shadowing), сделав GraphQuery-инстанс недостижимым внутри своего же tool-а.

Инструменты принимают ПЛОСКИЕ kwargs (не единственный pydantic-параметр): fastmcp
разворачивает функцию с одним BaseModel-параметром во ВЛОЖЕННЫЙ inputSchema
(`{"params": {node_id: ..., ...}}`), что заставляет MCP-клиента оборачивать аргументы --
живьём проверено на fastmcp 2.14.7 (см. m1b-task-7-report.md §fastmcp API notes).
Плоские kwargs дают плоский inputSchema (`{"node_id": ..., ...}`), как у любого обычного
MCP-инструмента; mcp/schemas.py остаётся источником истины для формы этих же полей.

Каждый инструмент -- голая передача в GraphQuery.<метод>(...) без своего try/except:
GraphQuery САМА отвечает за error-dict-контракт (amendment 3 -- node/file not found,
store unreachable -> {"error": ...}, никогда исключение), так что обёртке здесь нечего
перехватывать. Живьём проверено (см. отчёт): успешный dict и {"error": ...}-словарь оба
приходят в MCP-клиент как обычный (`isError=False`) результат с этим словарём в
structured_content/data -- то есть "структурированная ошибка вместо MCP-исключения" не
требует никакого специального кода на этом уровне, только "не мешать" GraphQuery.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastmcp import FastMCP

from codegraph.config.models import WorkspaceConfig
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.store import FalkorStore


def build_server(cfg: WorkspaceConfig, graph_name: str) -> FastMCP:
    """graph_name -- отдельный параметр (не cfg.graph_name напрямую), т.к. cli.serve
    поддерживает `--graph` override -- та же проводка, что _resolve_graph_name даёт
    index/load/stats (см. cli.py). service_paths строится из cfg.services -- пути уже
    абсолютны/резолвлены к этому моменту (config.loader.load_workspace резолвит
    ServiceConfig.path относительно расположения codegraph.yaml до того, как cfg сюда
    попадёт), так что GraphQuery.get_source не нуждается в собственном base-dir.

    store_factory -- `lambda: FalkorStore(cfg.storage.falkordb, graph_name)`: ФУНКЦИЯ,
    не готовый store (amendment 1/T3 stale-handle -- см. query.api.GraphQuery докстринг).
    Каждый tool-call этого сервера получает свежий FalkorStore и, значит, свежий
    falkordb.Graph -- сервер может жить неделями, переживая произвольное число
    `codegraph index` в других процессах, не рискуя декодировать данные через
    устаревший schema-кэш клиента.
    """
    service_paths: dict[str, Path] = {svc.name: svc.path for svc in cfg.services}
    gq = GraphQuery(
        store_factory=lambda: FalkorStore(cfg.storage.falkordb, graph_name),
        service_paths=service_paths,
    )

    mcp = FastMCP("codegraph")

    @mcp.tool
    def graph_stats() -> dict:
        """Счётчики узлов по kind и рёбер по type в текущем графе."""
        return gq.graph_stats()

    @mcp.tool
    def get_source(node_id: str, context_lines: int = 0) -> dict:
        """Исходный текст узла (+/- context_lines строк контекста) с флагом staleness
        (файл на диске изменился после индексации -- содержимое может не совпадать)."""
        return gq.get_source(node_id, context_lines=context_lines)

    @mcp.tool
    def expand_neighbors(
        node_id: str,
        edge_types: list[str] | None = None,
        direction: Literal["out", "in", "both"] = "both",
        depth: int = 1,
        limit: int = 50,
    ) -> dict:
        """BFS-обход соседей узла (depth клампится к 1..3), суммарно не более limit
        hops; truncated=true, если реальных соседей было больше. edge_types=None и
        edge_types=[] эквивалентны -- оба означают "без фильтра, любой тип ребра".
        Каждый hop несёт своё направление (direction: "out"|"in") -- в режиме
        direction="both" это различает исходные и обратные рёбра после слияния.
        Невалидный direction -> {"error": "invalid direction: ..."} (не исключение)."""
        return gq.expand_neighbors(
            node_id, edge_types=edge_types, direction=direction, depth=depth, limit=limit
        )

    @mcp.tool
    def who_calls(node_id: str, transitive: bool = False, max_depth: int = 3) -> dict:
        """Вызывающие узел через CALLS-рёбра: прямые (transitive=false) или BFS вверх
        по цепочке вызовов до max_depth (клампится к 1..5, transitive=true)."""
        return gq.who_calls(node_id, transitive=transitive, max_depth=max_depth)

    @mcp.tool
    def trace_process(
        entrypoint_id: str,
        direction: Literal["downstream", "upstream"] = "downstream",
        max_segments: int = 12,
        min_confidence: float = 0.3,
        include_source: bool = False,
    ) -> dict:
        """Трассировка бизнес-процесса от entrypoint_id вниз по цепочке вызовов и
        каналов (downstream-only в M2 -- direction="upstream" -> error dict, не
        исключение). max_segments клампится к 1..20, каждый сегмент -- до 15 хопов
        вглубь / 8 в ширину (truncated -- на сегменте и агрегированно). confidence --
        минимум по всем пройденным рёбрам (шагам и меж-сегментным переходам),
        отфильтрованным по min_confidence. include_source=true подмешивает исходный
        текст (get_source) в каждый узел трассы, best-effort (узлы без источника --
        напр. Channel -- остаются без "source", не ошибка)."""
        return gq.trace_process(
            entrypoint_id, direction=direction, max_segments=max_segments,
            min_confidence=min_confidence, include_source=include_source,
        )

    @mcp.tool
    def find_paths(
        from_id: str,
        to_id: str,
        max_hops: int = 8,
        edge_types: list[str] | None = None,
    ) -> dict:
        """BFS-путь между from_id и to_id по рёбрам в обе стороны (max_hops
        клампится к 1..12); путь -- список узлов с рёбрами, которыми до них дошли
        (edge_type/direction у самого from_id -- null). Путь не найден ->
        {"path": null} (не ошибка)."""
        return gq.find_paths(from_id, to_id, max_hops=max_hops, edge_types=edge_types)

    @mcp.tool
    def list_processes() -> dict:
        """Все BusinessProcess-узлы графа (id/name/entrypoint_id/source), отсортированы
        по id."""
        return gq.list_processes()

    @mcp.tool
    def find_entrypoint(query: str, kinds: list[str] | None = None, k: int = 5) -> dict:
        """Fulltext-поиск по Sym(name, qualified_name, docstring) (k клампится к
        1..20); kinds -- опциональный фильтр по n.kind. Пустой результат (в т.ч.
        запрос из одних RediSearch-спецсимволов) -- не ошибка, {"results": []}."""
        return gq.find_entrypoint(query, kinds=kinds, k=k)

    return mcp
