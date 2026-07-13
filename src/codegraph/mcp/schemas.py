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
    """Один шаг обхода: `node` -[edge_type]-> `neighbor` (или наоборот при
    direction="in" -- направление живёт в самом edge_type/запросе, не переутверждается
    здесь; см. query.api.GraphQuery.expand_neighbors докстринг)."""

    node: str
    edge_type: str
    edge_props: dict
    neighbor: str | None = None


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
