"""GraphStore Protocol: контракт serving-слоя графа. FalkorStore — единственная
реализация в M1 (src/codegraph/stores/falkordb/store.py).

Hop — единственный примитив обхода, который отдаёт neighbors(): многошаговые обходы
(depth>1) собираются в Python поверх повторных вызовов neighbors() выше по стеку
(query/api.py), а не Cypher-паттернами произвольной длины (см. Global Constraints
m1b-serving.md: "Многошаговость -- Python BFS").
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, Protocol

Hop = tuple[str, dict, dict]  # (edge_type, edge_props, node_dict)


class GraphStore(Protocol):
    def ensure_schema(self) -> None: ...

    def upsert_nodes(self, labels: tuple[str, ...], rows: list[dict]) -> int: ...

    def upsert_edges(
        self, edge_type: str, rows: list[dict], known_ids: set[str]
    ) -> tuple[int, int]: ...

    def get_nodes(self, ids: Sequence[str]) -> list[dict]: ...

    def neighbors(
        self,
        node_id: str,
        edge_types: Sequence[str] | None,
        direction: Literal["out", "in", "both"],
        limit: int,
    ) -> list[Hop]: ...

    def stats(self) -> dict: ...

    def graph_exists(self) -> bool: ...  # граф-ключ существует; read-only (без auto-vivify)

    def swap_in(self, build_name: str) -> None: ...  # blue/green: RENAME build_name -> self

    def delete_graph(self) -> None: ...  # удалить СВОЙ граф-ключ, если есть (идемпотентно)

    def raw(self, cypher: str, params: dict | None = None) -> Any:
        """Internal-only: тонкий проброс в g.query. НЕ экспонируется в MCP (см.
        m1b-serving.md Global Constraints: "GraphStore.raw() — internal-only, в MCP
        не экспонируется")."""
        ...
