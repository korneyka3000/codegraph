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
    """callers -- caller node property dicts (see query.api.GraphQuery.who_calls).

    M10 T2 (pilot §4.3): when node_id's OWN role is TemporalActivity, a caller
    reached via an INVOKES_ACTIVITY edge (Temporal's `execute_activity_method`,
    the workflow-side invocation mechanism -- see extractors/temporal_ext.py)
    additively carries `mechanism: "invokes_activity"` inside its dict. Same
    precedent as TraceExit.channel's own additive `external`/`external_host`
    props above: `callers` stays a plain, untyped `list[dict]` -- this validates
    with NO code change here at all, `mechanism` is just another key a dict is
    free to carry. A caller reached ONLY via an ordinary CALLS edge never
    carries this key (absent, not null/false). See tests/unit/test_mcp_schemas.py
    for a pinned proof."""

    callers: list[dict]
    truncated: bool


# -- M2 T8: trace_process / find_paths / list_processes / find_entrypoint --


class TraceProcessInput(BaseModel):
    entrypoint_id: str
    direction: Literal["downstream", "upstream"] = "downstream"  # upstream -> error dict, M2
    max_segments: int = 12  # клампится в GraphQuery к [1,20]
    min_confidence: float = 0.3
    include_source: bool = False
    compact: bool = True  # M5 T5 (pilot §7.3): collapse long boring runs (>15
    # steps/segment) in query.traverse.trace_process's post-processing; callers
    # who want the pre-M5 always-full dump pass compact=False (CLI: `trace --full`).


class TraceStep(BaseModel):
    """Один intra-сегмент переход (CALLS/DEPENDS_ON/INVOKES_ACTIVITY; CALLS с
    props["mechanism"]=="temporal_start" -- тот же edge_type, просто с этим
    доп. props-ключом, см. query/traverse.py). direction всегда "out" в M2
    (downstream-only walk).

    M5 T5 (additive): `collapsed` -- None on every REAL step (unchanged from
    before this field existed). trace_process(compact=True, the new default)
    replaces a long boring run's interior with a single SYNTHETIC marker step
    carrying ONLY `collapsed` (the count of hidden interior steps) -- edge_type/
    props/node/direction all fall back to their defaults on a marker (never real
    edge data); consumers should treat `collapsed is not None` as "this is a
    marker, not a real step" (see query/traverse.py's _compact_steps)."""

    edge_type: str = ""
    props: dict = Field(default_factory=dict)
    node: dict = Field(default_factory=dict)
    direction: Literal["out"] = "out"
    collapsed: int | None = None


class TraceExit(BaseModel):
    """Выход сегмента через канал (PRODUCES/CALLS_HTTP): next_entry_ids -- entry-id
    следующих сегментов, восстановленные через NEXT_SEGMENT+via_channel_id
    (см. query/traverse.py модульный докстринг); пустой список -- канал без
    резолвленного потребителя (dead end), не ошибка.

    M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): `channel` is a
    plain, untyped dict (whatever the graph node's own properties are), not a
    nested pydantic model -- every OTHER Channel prop (owner_service, config_ref,
    unresolved, ...) this field has always carried verbatim still does.

    M10 T4 (linking/http_routes.py's own module docstring, "SHARED-CHANNEL PROPS"
    section): `external`/`external_host` moved OFF the channel node's own props
    (they now ride the CALLS_HTTP edge instead, per-claim -- see that module's
    docstring for why) -- `channel` therefore no longer carries them at all.
    `TraceExit` grows two ADDITIVE fields mirroring that move instead:
    `external: bool = False` / `external_host: str | None = None`, populated by
    query/traverse.py's `_resolve_exits` off the walked edge(s). See
    tests/unit/test_mcp_schemas.py for a pinned proof."""

    channel: dict
    next_entry_ids: list[str]
    external: bool = False
    external_host: str | None = None


class TraceSegment(BaseModel):
    service: str
    entry: dict
    steps: list[TraceStep]
    exits: list[TraceExit]
    truncated: bool  # рёбра ЭТОГО сегмента реально срезаны depth-капом 15 (за капом
    # есть непройденные рёбра -- полная 15-хоповая цепочка НЕ truncated) или branch-капом 8


class TraceProcessOutput(BaseModel):
    segments: list[TraceSegment]
    # min по всем пройденным рёбрам (шаги + переходы); 1.0 если рёбер нет. M9 T1:
    # exit-хопы с exit.external=True (см. TraceExit; M10 T4 -- было channel.
    # external=True) ИСКЛЮЧЕНЫ из этого минимума -- см. query/traverse.py::
    # trace_process докстринг за полной формулой до/после.
    confidence: float
    truncated: bool  # true если truncated у любого сегмента ИЛИ max_segments срезал список
    # M9 T1 review Important: machine-readable спутник exclusion'а выше -- число
    # exit-входов ВСЕЙ трассы с exit.external=True (0 = полностью внутренний
    # трейс). Без него программный/MCP-потребитель, читающий confidence=1.0 с
    # верха, заключил бы «fully traced» для трассы, упирающейся в границу
    # воркспейса (человек видит external-леги в рендере, машина -- нет; прецедент
    # -- поле `truncated`, существующее ровно для этого). Счётчик, не bool --
    # строго богаче при том же falsy-прочтении. АДДИТИВНО: default 0, pre-M9
    # result-dict без ключа валидируется как прежде (запинено в
    # tests/unit/test_mcp_schemas.py).
    external_exit_count: int = 0


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
    qualified_name: str | None = None  # владеющего символа -- денормализовано на
    # Chunk-узел при load (pipeline/load._chunk_props, M3 T7 review fix); None, если
    # символ чанка отсутствовал в staged nodes (защитный edge case) или граф загружен
    # pre-T7 load'ом, ещё не материализовавшим это поле
    enclosing_symbol: str | None = None  # M10 T3 (pilot §4.1): ТО ЖЕ значение, что
    # qualified_name выше (та же денормализация, тот же None-edge-case) -- явное,
    # однозначно поименованное аддитивное поле, а не замена qualified_name (который
    # остаётся ради обратной совместимости), см. query.retrieval._chunk_item докстринг.
    chunk_kind: str | None = None  # M10 T3: kind узла-владельца ("Module"/"Class"/
    # "Function", см. core/schema.py NODE_KINDS) -- НЕ kind самого чанка (тот всегда
    # "Chunk", см. pipeline/load._chunk_props). Денормализовано на Chunk-узел при load
    # той же техникой, что qualified_name (см. _chunk_props' "kinds" join map); вместе
    # с qualified_name/enclosing_symbol различает method-level чанк (chunk_kind=
    # "Function") от class-level содержимого (chunk_kind="Class") -- пилот §4.1.
    service: str | None = None
    relpath: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    snippet: str  # chunk text, truncated to <=600 chars (see query.retrieval._snippet)
    score: float  # ВСЕГДА fused RRF-score, даже для mode="text"/"vector" (единая,
    # всегда-desc-лучше шкала независимо от режима -- см. query.retrieval.search_code)
    sibling_chunks: int = 0  # M12 T1 (pilot §4.1/§4.2): этот item -- представитель
    # своего symbol_id (post-RRF агрегация по символу, лучший ранг представляет, см.
    # query.retrieval._aggregate_by_symbol) -- sibling_chunks считает ДРУГИЕ чанки
    # ТОГО ЖЕ symbol_id, которые были в (over-fetched) пуле кандидатов; 0, если символ
    # был в пуле единственным чанком (typичный случай -- класс, целиком влезающий в
    # один чанк, chunking/splitter.py rule 2). Default 0 (не Optional/None) -- так же
    # аддитивно, как TraceProcessOutput.external_exit_count: старый result-dict без
    # этого ключа валидируется как прежде (см. tests/unit/test_mcp_schemas.py).


class SearchCodeOutput(BaseModel):
    items: list[SearchCodeItem]
    mode_used: Literal["hybrid", "vector", "text"]
