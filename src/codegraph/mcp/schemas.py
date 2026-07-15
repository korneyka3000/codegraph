"""Pydantic-модели входов/выходов 4 MCP v0 инструментов -- стабильный контракт
(поля как в брифе m1b-task-7 §Interfaces), используемый:
  1) контракт-тестом (tests/integration/test_mcp_contract.py) для валидации формы
     живых ответов сервера (`Output(**result)` не должен падать);
  2) юнитами query.api (косвенно -- GraphQuery возвращает эти же поля как plain dict).

НЕ используются как buквальный тип параметра/аннотация возврата в mcp/server.py: MCP-
инструмент возвращает либо success-схему, либо `{"error": str}` (см. GraphQuery's
error-dict контракт, mcp/server.py делегирует его дословно) -- эти две формы не сводятся
к одной pydantic-модели без объединения-в-error, а по m1b-task-7 §Controller amendment 3
инструменты обязаны возвращать структурированную ошибку, а не кидать исключение (в т.ч.
pydantic ValidationError). Входные модели тоже не используются как buквальный тип
единственного параметра инструмента: fastmcp разворачивает pydantic-параметр в
ВЛОЖЕННЫЙ inputSchema (`{"params": {...}}`), что ломает плоский MCP tool-call контракт --
инструменты в server.py принимают эти же поля как плоские kwargs (см. server.py
докстринг), а модели здесь остаются контрактом для тестов/документации.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ErrorOutput(BaseModel):
    """Форма ответа ЛЮБОГО из 4 инструментов при ошибке (amendment 3): узел/файл не
    найден, путь убегает из корня сервиса, store недоступен."""

    error: str


# -- graph_stats --


class GraphStatsInput(BaseModel):
    """Без параметров -- модель пуста намеренно (полнота контракта: 4 инструмента x
    input+output, даже если для этого конкретного инструмента input тривиален)."""


class GraphStatsOutput(BaseModel):
    nodes: dict[str, int]
    edges: dict[str, int]


# -- get_source --


class GetSourceInput(BaseModel):
    node_id: str
    context_lines: int = 0


class GetSourceOutput(BaseModel):
    source: str
    file: str
    start_line: int
    end_line: int
    stale: bool


# -- expand_neighbors --


class ExpandNeighborsInput(BaseModel):
    node_id: str
    edge_types: list[str] | None = Field(
        default=None,
        description=(
            "Фильтр по типу ребра (например [\"CALLS\"]). None и [] эквивалентны -- "
            "оба означают «без фильтра, любой тип ребра» (см. "
            "query.api.GraphQuery.expand_neighbors)."
        ),
    )
    direction: Literal["out", "in", "both"] = "both"
    depth: int = 1  # клампится в GraphQuery к [1,3]
    limit: int = 50


class Hop(BaseModel):
    """Один шаг обхода: `node` -[edge_type]-> `neighbor` в направлении `direction`
    (M2: обязательное поле -- "out"|"in", ИСТИННОЕ направление ЭТОГО перехода; при
    вызове expand_neighbors(direction="both") каждый hop несёт своё, а не общее
    значение -- см. query.api.GraphQuery.expand_neighbors и stores/graph.py Hop)."""

    node: str
    edge_type: str
    edge_props: dict
    neighbor: str | None = None
    direction: Literal["out", "in"]


class ExpandNeighborsOutput(BaseModel):
    nodes: list[dict]
    hops: list[Hop]
    truncated: bool


# -- who_calls --


class WhoCallsInput(BaseModel):
    node_id: str
    transitive: bool = False
    max_depth: int = 3  # клампится в GraphQuery к [1,5]


class WhoCallsOutput(BaseModel):
    callers: list[dict]
    truncated: bool


# -- M2 T8: trace_process / find_paths / list_processes / find_entrypoint --


class TraceProcessInput(BaseModel):
    entrypoint_id: str
    direction: Literal["downstream", "upstream"] = "downstream"  # upstream -> error dict, M2
    max_segments: int = 12  # клампится в GraphQuery к [1,20]
    min_confidence: float = 0.3
    include_source: bool = False


class TraceStep(BaseModel):
    """Один intra-сегмент переход (CALLS/DEPENDS_ON/INVOKES_ACTIVITY; CALLS с
    props["mechanism"]=="temporal_start" -- тот же edge_type, просто с этим
    доп. props-ключом, см. query/traverse.py). direction всегда "out" в M2
    (downstream-only walk)."""

    edge_type: str
    props: dict
    node: dict
    direction: Literal["out"]


class TraceExit(BaseModel):
    """Выход сегмента через канал (PRODUCES/CALLS_HTTP): next_entry_ids -- entry-id
    следующих сегментов, восстановленные через NEXT_SEGMENT+via_channel_id
    (см. query/traverse.py модульный докстринг); пустой список -- канал без
    резолвленного потребителя (dead end), не ошибка."""

    channel: dict
    next_entry_ids: list[str]


class TraceSegment(BaseModel):
    service: str
    entry: dict
    steps: list[TraceStep]
    exits: list[TraceExit]
    truncated: bool  # рёбра ЭТОГО сегмента реально срезаны depth-капом 15 (за капом
    # есть непройденные рёбра -- полная 15-хоповая цепочка НЕ truncated) или branch-капом 8


class TraceProcessOutput(BaseModel):
    segments: list[TraceSegment]
    confidence: float  # min по всем пройденным рёбрам (шаги + переходы); 1.0 если рёбер нет
    truncated: bool  # true если truncated у любого сегмента ИЛИ max_segments срезал список


class FindPathsInput(BaseModel):
    from_id: str
    to_id: str
    max_hops: int = 8  # клампится в GraphQuery к [1,12]
    edge_types: list[str] | None = None


class PathStep(BaseModel):
    """Один узел пути; edge_type/direction -- ребро, которым СЮДА пришли (None у
    самого первого узла -- у него нет входящего в путь ребра)."""

    node: dict
    edge_type: str | None = None
    direction: Literal["out", "in"] | None = None


class FindPathsOutput(BaseModel):
    path: list[PathStep] | None  # None -- путь не найден (не ошибка)


class ListProcessesInput(BaseModel):
    """Без параметров -- как GraphStatsInput."""


class ProcessOut(BaseModel):
    id: str
    name: str
    entrypoint_id: str
    source: str


class ListProcessesOutput(BaseModel):
    processes: list[ProcessOut]


class FindEntrypointInput(BaseModel):
    query: str
    kinds: list[str] | None = None
    k: int = 5  # клампится в GraphQuery к [1,20]


class FindEntrypointOutput(BaseModel):
    """v2 (M3 T7): те же поля, что M2 ("results" -- node properties + "score"), плюс
    "mode_used" -- обратная совместимость по полям (см. query.retrieval.find_entrypoint
    докстринг): "hybrid" (Sym-fulltext + chunk-vector RRF-фьюжн) или "text" (нет
    usable embedder'а на этот вызов -- молчаливая деградация, НЕ ошибка, ровно
    M2-поведение)."""

    results: list[dict]  # каждый -- node properties + "score" (см. store.search_fulltext)
    mode_used: Literal["hybrid", "text"]


# -- M3 T7: search_code (9-й инструмент) --


class SearchCodeInput(BaseModel):
    query: str
    k: int = 8  # клампится в GraphQuery к [1,20]
    service: str | None = None
    mode: Literal["hybrid", "vector", "text"] = "hybrid"


class SearchCodeItem(BaseModel):
    chunk_id: str | None = None
    symbol_id: str | None = None
    service: str | None = None
    relpath: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    snippet: str  # chunk text, truncated to <=600 chars (see query.retrieval._snippet)
    score: float  # ВСЕГДА fused RRF-score, даже для mode="text"/"vector" (единая,
    # всегда-desc-лучше шкала независимо от режима -- см. query.retrieval.search_code)


class SearchCodeOutput(BaseModel):
    items: list[SearchCodeItem]
    mode_used: Literal["hybrid", "vector", "text"]
