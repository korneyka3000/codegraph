"""FalkorStore: FalkorDB-реализация graph.GraphStore.

Единственное (вместе с ddl.py/batch.py) место, где строится Cypher для графового
serving-слоя. ensure_schema/upsert_nodes/upsert_edges делегируют в ddl.py/batch.py;
get_nodes/neighbors/stats/raw -- собственные read-запросы этого модуля; swap_in --
blue/green через Redis RENAME.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from falkordb import FalkorDB

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb import batch, ddl
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.graph import Hop

# Cypher-паттерн для каждой стороны обхода; node_id всегда идёт параметром ($id),
# сюда попадают только эти два фиксированных, невыводимых из пользовательского ввода
# фрагмента -- интерполяция строки здесь не является инъекционной поверхностью.
_DIRECTION_PATTERNS = {
    "out": "(n {id: $id})-[e]->(m)",
    "in": "(n {id: $id})<-[e]-(m)",
}


class FalkorStore:
    """GraphStore над одним графом FalkorDB; graph_name -- redis-ключ этого графа
    (см. swap_in() для blue/green переключения build-графа на это имя)."""

    def __init__(self, cfg: FalkorDBConfig, graph_name: str) -> None:
        self.cfg = cfg
        self.graph_name = graph_name
        self._db: FalkorDB | None = None
        self._graph = None

    def _connect(self) -> FalkorDB:
        if self._db is None:
            self._db = connect(self.cfg)
        return self._db

    @property
    def _g(self):
        """Ленивая Graph-обёртка: реальное подключение к FalkorDB происходит при первом
        обращении, не в __init__ (конструирование стора не должно требовать живого
        FalkorDB). Инвалидируется в swap_in -- см. её докстринг: это не оптимизация,
        а условие корректности после RENAME."""
        if self._graph is None:
            self._graph = self._connect().select_graph(self.graph_name)
        return self._graph

    def ensure_schema(self) -> None:
        ddl.ensure_schema(self._connect(), self.graph_name)

    def upsert_nodes(self, labels: tuple[str, ...], rows: list[dict]) -> int:
        return batch.upsert_nodes(self._g, labels, rows)

    def upsert_edges(
        self, edge_type: str, rows: list[dict], known_ids: set[str]
    ) -> tuple[int, int]:
        return batch.upsert_edges(self._g, edge_type, rows, known_ids)

    def get_nodes(self, ids: Sequence[str]) -> list[dict]:
        """`UNWIND $ids AS i MATCH (n {id: i}) RETURN n` -- id, отсутствующие в графе,
        молча пропускаются (промах MATCH просто не даёт строки для данного i)."""
        res = self._g.query(
            "UNWIND $ids AS i MATCH (n {id: i}) RETURN n", {"ids": list(ids)}
        )
        return [row[0].properties for row in res.result_set]

    def neighbors(
        self,
        node_id: str,
        edge_types: Sequence[str] | None,
        direction: Literal["out", "in", "both"],
        limit: int,
    ) -> list[Hop]:
        """out -> `(n {id})-[e]->(m)`, in -> `(n {id})<-[e]-(m)`; both -- оба запроса,
        результаты объединяются и limit применяется к сумме (каждый под-запрос уже
        ограничен тем же limit -- этого достаточно, т.к. после слияния всё равно
        обрезаем до limit; per-side limit не может дать МЕНЬШЕ полных hop'ов, чем
        обрезка суммы). Несуществующий node_id -- MATCH не матчит ничего, обе стороны
        дают [], результат []."""
        if direction == "both":
            merged = self._one_way(node_id, edge_types, "out", limit) + self._one_way(
                node_id, edge_types, "in", limit
            )
            return merged[:limit]
        return self._one_way(node_id, edge_types, direction, limit)

    def _one_way(
        self,
        node_id: str,
        edge_types: Sequence[str] | None,
        direction: Literal["out", "in"],
        limit: int,
    ) -> list[Hop]:
        cypher = f"MATCH {_DIRECTION_PATTERNS[direction]}"
        params: dict[str, Any] = {"id": node_id, "limit": limit}
        if edge_types:
            # $types -- параметр (список строк), НЕ f-string: значения приходят снаружи
            # (в перспективе -- из MCP-инструмента). `WHERE type(e) IN $types` проверен
            # на живом FalkorDB v4.18.11: IN безопасно параметризуется списком (значение
            # сравнивается как строковый литерал, не подставляется в текст запроса) --
            # инъекция через содержимое edge_types невозможна без f-string фолбэка.
            cypher += " WHERE type(e) IN $types"
            params["types"] = list(edge_types)
        cypher += " RETURN e, m LIMIT $limit"
        res = self._g.query(cypher, params)
        return [(e.relation, e.properties, m.properties) for e, m in res.result_set]

    def stats(self) -> dict:
        nodes = self._g.query("MATCH (n) RETURN n.kind, count(n)")
        edges = self._g.query("MATCH ()-[e]->() RETURN type(e), count(e)")
        return {"nodes": dict(nodes.result_set), "edges": dict(edges.result_set)}

    def swap_in(self, build_name: str) -> None:
        """Blue/green: атомарный Redis `RENAME build_name self.graph_name` --
        перезаписывает существующий self.graph_name целиком, если он был (стандартная
        Redis-семантика RENAME; подтверждено живым тестом: старые данные под этим именем
        исчезают, не сливаются).

        ВАЖНО (подтверждено отдельным живым экспериментом при разработке этой задачи --
        см. отчёт m1b-task-3): FalkorDB python-клиент кэширует схему графа
        (label/property-key/relationship-type id -> имя) в Graph.schema и НЕ детектирует
        смену данных под ключом через RENAME -- в отличие от обычных Cypher-мутаций,
        RENAME не проходит через version-bump протокол, на который полагается
        QueryResult для авто-обновления кэша (`SchemaVersionMismatchException`).
        Переиспользование self._g, созданного/использованного ДО этого вызова, после
        RENAME возвращает ГИБРИД: актуальные значения свойств, но label/property-key
        ИМЕНА, декодированные по старой (уже не существующей под этим именем) схеме.
        Поэтому self._graph обязательно сбрасывается в None -- следующее обращение к
        self._g лениво создаст свежий Graph через select_graph().
        """
        self._connect().connection.execute_command("RENAME", build_name, self.graph_name)
        self._graph = None

    def raw(self, cypher: str, params: dict | None = None) -> Any:
        """Internal-only, не для MCP: тонкий проброс в g.query(cypher, params)."""
        return self._g.query(cypher, params)
