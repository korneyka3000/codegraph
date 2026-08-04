"""FastMCP сервер: 9 read-only инструментов (M1 v0: graph_stats/get_source/
expand_neighbors/who_calls; M2 T8: trace_process/find_paths/list_processes/
find_entrypoint; M3 T7: search_code + find_entrypoint становится гибридным), тонкая
делегация в query.api.GraphQuery -- ни одного Cypher-запроса и ни одного обращения к
GraphStore.raw() в этом модуле (см. stores/graph.py: GraphStore.raw() docstring --
"internal-only", не для MCP).

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

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from fastmcp import FastMCP

from codegraph.config.models import WorkspaceConfig
from codegraph.core.errors import CodegraphError
from codegraph.embedding.base import Embedder
from codegraph.embedding.factory import make_embedder
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.store import FalkorStore

logger = logging.getLogger(__name__)


def _default_embedder_factory(cfg: WorkspaceConfig) -> Callable[[], Embedder | None]:
    """M3 T7 default wiring for `build_server`'s new `embedder_factory` param: lazily
    build the workspace's configured embedder (`cfg.embedding`) the same
    catch-CodegraphError-and-degrade way `cli._make_embedder_or_warn` does for
    `codegraph index`'s S8 stage -- provider package not installed/API key missing
    degrades search_code/find_entrypoint to text-only (see query.retrieval's own
    docstring for that degradation path) instead of crashing the whole MCP server.
    Uses `logger.warning` (stderr), NOT `console.print`/any stdout write -- unlike
    cli.py's own yellow-warning variant, THIS factory runs inside a live MCP server
    process, typically talking to its client over stdio (see cli.py's `serve`
    docstring); writing anything to stdout here would corrupt that protocol stream."""

    def factory() -> Embedder | None:
        # M4 T2: logged immediately before the real (possibly multi-second --
        # sentence-transformers model load/download, or a first provider API
        # handshake) construction call, NOT after -- a user watching `codegraph
        # serve`'s stderr on their first search_code/find_entrypoint call sees WHY
        # the server is pausing instead of it looking hung. Fires exactly once per
        # GraphQuery lifetime in practice: GraphQuery._get_embedder() caches any
        # non-None result and never calls this factory again afterwards (see its own
        # docstring) -- this factory has no cache of its own, that's deliberate,
        # _get_embedder already owns caching policy.
        logger.info("loading embedding model %s (provider=%s)...", cfg.embedding.model,
                    cfg.embedding.provider)
        try:
            return make_embedder(cfg.embedding)
        except CodegraphError as e:
            logger.warning("search_code/find_entrypoint vector mode unavailable: %s", e)
            return None

    return factory


def build_server(
    cfg: WorkspaceConfig,
    graph_name: str,
    embedder_factory: Callable[[], Embedder | None] | None = None,
) -> FastMCP:
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

    embedder_factory (M3 T7, опционален): None (единственный способ вызвать эту
    функцию из cli.py serve -- см. её докстринг, интерфейс НЕ менялся) -> строится
    дефолтный `_default_embedder_factory(cfg)` внутри. Параметр существует ради
    тестов (contract-тест подменяет его на `lambda: FakeEmbedder(...)`, санкционировано
    планом T7 -- не нужно поднимать реальный sentence-transformers/OpenAI/Voyage только
    чтобы живьём проверить vector-режим search_code) -- ни cli.py, ни любой другой
    реальный вызывающий код НЕ передаёт этот параметр явно.
    """
    service_paths: dict[str, Path] = {svc.name: svc.path for svc in cfg.services}
    gq = GraphQuery(
        store_factory=lambda: FalkorStore(cfg.storage.falkordb, graph_name),
        service_paths=service_paths,
        embedder_factory=(
            embedder_factory if embedder_factory is not None else _default_embedder_factory(cfg)
        ),
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
        по цепочке вызовов до max_depth (клампится к 1..5, transitive=true). M10 T2
        (pilot §4.3): если у node_id роль TemporalActivity, дополнительно
        учитываются входящие INVOKES_ACTIVITY-рёбра (воркфлоу вызывает активность
        через execute_activity_method) на всех уровнях обхода -- такие callers несут
        доп. поле mechanism="invokes_activity"; обычные CALLS-callers его не несут.
        Для узлов без роли TemporalActivity поведение не меняется."""
        return gq.who_calls(node_id, transitive=transitive, max_depth=max_depth)

    @mcp.tool
    def trace_process(
        entrypoint_id: str,
        direction: Literal["downstream", "upstream"] = "downstream",
        max_segments: int = 12,
        min_confidence: float = 0.3,
        include_source: bool = False,
        compact: bool = True,
    ) -> dict:
        """Трассировка бизнес-процесса от entrypoint_id вниз по цепочке вызовов и
        каналов (downstream-only в M2 -- direction="upstream" -> error dict, не
        исключение). max_segments клампится к 1..20, каждый сегмент -- до 15 хопов
        вглубь / 8 в ширину (truncated -- на сегменте и агрегированно). confidence --
        минимум по всем пройденным рёбрам (шагам и меж-сегментным переходам),
        отфильтрованным по min_confidence. include_source=true подмешивает исходный
        текст (get_source) в каждый узел трассы, best-effort (узлы без источника --
        напр. Channel -- остаются без "source", не ошибка). compact=true (по
        умолчанию, M5 T5) схлопывает длинные линейные хвосты внутри сегмента
        (>15 шагов, см. query/traverse.py _compact_steps) в
        {"collapsed": N}-маркеры -- роли/ветвления/exit-шаги никогда не
        схлопываются; compact=false возвращает каждый шаг без исключений."""
        return gq.trace_process(
            entrypoint_id, direction=direction, max_segments=max_segments,
            min_confidence=min_confidence, include_source=include_source, compact=compact,
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
        """Гибридный (fulltext по Sym + vector по Chunk, RRF-фьюжн) поиск точки входа
        (k клампится к 1..20); kinds -- опциональный фильтр по n.kind, применяется
        ПОСЛЕ фьюжна. Нет доступного embedder'а для этого воркспейса (или граф ещё не
        эмбеден/эмбеден другой моделью) -- молчаливая деградация в чистый fulltext
        (mode_used="text" в ответе), НЕ ошибка. Пустой результат (в т.ч. запрос из
        одних RediSearch-спецсимволов) -- тоже не ошибка, {"results": [], ...}."""
        return gq.find_entrypoint(query, kinds=kinds, k=k)

    @mcp.tool
    def search_code(
        query: str,
        k: int = 8,
        service: str | None = None,
        mode: Literal["hybrid", "vector", "text"] = "hybrid",
    ) -> dict:
        """Поиск по коду (Chunk-узлам) -- text (fulltext по тексту/заголовку чанка),
        vector (по embedding'у) или hybrid (RRF-фьюжн обоих, по умолчанию); k --
        top-k РАЗНЫХ СИМВОЛОВ, не сырых чанков (M12 T1, см. ниже), клампится к 1..20;
        service -- опциональный фильтр по владеющему сервису. mode="vector" без
        доступного embedder'а (или при рассинхроне модели с той, которой граф
        эмбеден) -- {"error": ...}; mode="hybrid" в той же ситуации молчаливо
        деградирует в text-only (mode_used="text" в ответе). Результат:
        {"items": [{chunk_id, symbol_id, qualified_name, enclosing_symbol,
        chunk_kind, service, relpath, start_line, end_line, snippet, score,
        sibling_chunks}, ...], "mode_used": ...}. enclosing_symbol -- qualified_name
        владеющего чанком символа (то же значение, что qualified_name); chunk_kind --
        его kind ("Module"/"Class"/"Function") -- вместе показывают, покрывает ли
        чанк ровно один метод (chunk_kind="Function") или содержимое уровня класса
        (chunk_kind="Class": часть большого класса, либо класс целиком).

        Top-k различных символов (M12 T1, pilot §4.1): большие классы режутся на N
        чанков, часть которых делит один symbol_id (различаются лишь ##cN в
        chunk_id) -- раньше это позволяло sibling-чанкам ОДНОГО такого класса забить
        весь top-k, вытеснив чанки других, реально разных символов (напр. нужный
        метод-чанк). Теперь кандидаты агрегируются по symbol_id ПОСЛЕ RRF-фьюжна:
        каждый символ представлен ровно одним (лучшим по рангу) чанком, top-k -- это
        k РАЗНЫХ символов. sibling_chunks на каждом item -- сколько ЕЩЁ чанков ТОГО
        ЖЕ символа было в пуле кандидатов (0 -- символ был в пуле единственным
        чанком, типичный случай для класса, целиком влезающего в один чанк).

        Клиент vs сервер (pilot §4.2): у вопроса вида «где ОБРАБАТЫВАЕТСЯ X» (сервер,
        напр. HTTP/consumer-хендлер) и «кто ВЫЗЫВАЕТ X» (клиент) отвечают чанки из
        РАЗНЫХ сервисов -- без service-фильтра клиентские методы (их обычно больше,
        они чаще встречаются по всему workspace) склонны доминировать в топе даже
        когда вопрос о серверной стороне. Для «где обрабатывается» -- фильтруйте
        service=<серверный сервис> (владелец хендлера/обработчика); для «кто
        вызывает» -- service=<сервис-клиент> либо вовсе без фильтра."""
        return gq.search_code(query, k=k, service=service, mode=mode)

    return mcp
