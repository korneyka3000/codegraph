"""FalkorStore: FalkorDB-реализация graph.GraphStore.

Единственное (вместе с ddl.py/batch.py) место, где строится Cypher для графового
serving-слоя. ensure_schema/upsert_nodes/upsert_edges делегируют в ddl.py/batch.py;
get_nodes/neighbors/stats/raw -- собственные read-запросы этого модуля; swap_in --
blue/green через Redis RENAME.
"""

from __future__ import annotations

import re
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

# RediSearch query-syntax special characters (M2 T8 fulltext search): a raw query
# string containing these -- e.g. a qualified name like "app.routes.orders" (the
# dot survives; only THESE chars are special) or free text with punctuation --
# must not reach db.idx.fulltext.queryNodes verbatim, or RediSearch parses them as
# query OPERATORS (`-` = NOT, `|` = OR, `@field:` = field-scoping, `*` = wildcard,
# `(`/`)` = grouping, `~` = fuzzy, `"` = phrase, `{}` = tag/range, `$` = param
# marker, `%` = fuzzy-distance, `<`/`>` = numeric range) instead of literal text,
# either raising a syntax error or silently changing what's matched. `-` is
# escaped (not just placed at class edges) so it reads unambiguously here.
_FULLTEXT_SPECIAL_CHARS = re.compile(r'[@{}|()~*"$:%\-<>]')


def _sanitize_fulltext_query(query: str) -> str:
    """Replaces (not strips) each RediSearch special char with a space -- stripping
    outright would glue adjacent tokens together (`"orders-api"` -> `"ordersapi"`,
    a DIFFERENT word from either "orders" or "api"), which is worse than losing the
    character entirely. Runs of whitespace produced by the substitution (or already
    present) collapse to single spaces, and leading/trailing whitespace is trimmed.

    A query that is empty, all-whitespace, or entirely special characters (e.g.
    `"@{}~*"`) sanitizes to `""` -- callers MUST treat that as "no usable query
    text" and skip the RediSearch call entirely (an empty or pure-operator query
    string is a syntax error there, not a legitimate empty-result search)."""
    return " ".join(_FULLTEXT_SPECIAL_CHARS.sub(" ", query).split())


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

    def ensure_schema(self, dim: int | None = None) -> None:
        ddl.ensure_schema(self._connect(), self.graph_name, dim=dim)

    def upsert_nodes(
        self, labels: tuple[str, ...], rows: list[dict], vector_props: tuple[str, ...] = ()
    ) -> int:
        return batch.upsert_nodes(self._g, labels, rows, vector_props=vector_props)

    def upsert_edges(
        self,
        edge_type: str,
        rows: list[dict],
        known_ids: set[str],
        key_props: tuple[str, ...] = (),
    ) -> tuple[int, int]:
        return batch.upsert_edges(self._g, edge_type, rows, known_ids, key_props=key_props)

    def get_nodes(self, ids: Sequence[str]) -> list[dict]:
        """`UNWIND $ids AS i MATCH (n {id: i}) RETURN n` -- id, отсутствующие в графе,
        молча пропускаются (промах MATCH просто не даёт строки для данного i)."""
        res = self._g.query(
            "UNWIND $ids AS i MATCH (n {id: i}) RETURN n", {"ids": list(ids)}
        )
        return [row[0].properties for row in res.result_set]

    def find_by_qualified(self, service: str, qualified: str) -> dict | None:
        """`MATCH (n:Sym {service, qualified_name}) RETURN n ORDER BY n.id LIMIT 1` --
        M3 T2, the qualified-selector-form lookup for query.api.GraphQuery.
        resolve_selector's graph-side resolution (no staging.db needed, unlike the
        pre-M3 CLI `trace`, which read this same shape of answer out of Staging's
        `nodes` table via `_qualified_index`). `:Sym` (not a bare property match, see
        get_nodes_by_kind's own docstring for why THAT method can't use a label) is
        deliberate here: qualified_name/service are only meaningful on code nodes
        (Function/Class/Module), which always carry :Sym (see pipeline/load.py's
        `_labels_for_kind`), and ddl.py indexes exactly `(:Sym).qualified_name` /
        `(:Sym).service` -- scoping the MATCH to the label lets FalkorDB use those
        indexes instead of a full node scan. ORDER BY id LIMIT 1: deterministic pick
        on the defensive (should never happen by construction -- ids are derived from
        (service, descriptors), so two DIFFERENT ids sharing (service, qualified_name)
        would mean two source spans genuinely collided) case of more than one match."""
        res = self._g.query(
            "MATCH (n:Sym {service: $service, qualified_name: $qualified}) "
            "RETURN n ORDER BY n.id LIMIT 1",
            {"service": service, "qualified": qualified},
        )
        return res.result_set[0][0].properties if res.result_set else None

    def get_nodes_by_kind(self, kind: str) -> list[dict]:
        """`MATCH (n {kind: $kind}) RETURN n` -- property match on `n.kind` (every
        loaded node carries it, see pipeline/load._NODE_CORE_FIELDS), NOT a label
        match: Cypher labels cannot be parameterized (`MATCH (n:$kind)` is not
        valid syntax), and interpolating `kind` as a label would need its own
        allowlist-before-f-string guard (see batch.py's `_validate_labels`). A
        property match sidesteps that whole class of concern for free -- `kind` is
        just another bind parameter here, same as `id` in get_nodes above. M2 T8's
        only caller is query.api.GraphQuery.list_processes (kind="BusinessProcess")
        -- BusinessProcess nodes aren't reachable via get_nodes (their ids aren't
        known ahead of time by any caller) or via neighbors (no fixed node to walk
        from), so this is the one place a plain "give me every node of this kind"
        query is needed."""
        res = self._g.query("MATCH (n {kind: $kind}) RETURN n", {"kind": kind})
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
        дают [], результат [].

        Каждый Hop несёт СВОЁ direction ("out"/"in", см. Hop в stores/graph.py) --
        _one_way проставляет его по стороне запроса, которая его породила, поэтому
        в both-режиме после слияния out- и in-хопы остаются различимы (не единое
        значение на весь результат)."""
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
        # direction -- параметр ЭТОГО вызова (не выведен из Cypher-результата): каждая
        # строка результата пришла из ОДНОГО фиксированного _DIRECTION_PATTERNS[direction]
        # паттерна выше, поэтому все строки этого под-запроса имеют одно и то же
        # истинное направление -- то самое direction, с которым вызван _one_way.
        return [(e.relation, e.properties, m.properties, direction) for e, m in res.result_set]

    def stats(self) -> dict:
        nodes = self._g.query("MATCH (n) RETURN n.kind, count(n)")
        edges = self._g.query("MATCH ()-[e]->() RETURN type(e), count(e)")
        return {"nodes": dict(nodes.result_set), "edges": dict(edges.result_set)}

    def search_fulltext(
        self, query: str, k: int, kinds: Sequence[str] | None = None
    ) -> list[dict]:
        """`CALL db.idx.fulltext.queryNodes('Sym', $q) YIELD node, score` over the
        Sym(name, qualified_name, docstring) fulltext index (ddl.ensure_schema) --
        `query` is sanitized first (RediSearch operator chars -> space, see
        _sanitize_fulltext_query); a query that sanitizes to "" returns [] WITHOUT
        ever calling FalkorDB (an empty/pure-operator RediSearch query string is a
        syntax error there, not a legitimate empty-result search -- see
        query.api.GraphQuery.find_entrypoint, the only caller).

        `kinds`, if given, narrows results to `node.kind IN $kinds` (a property
        filter alongside the fulltext YIELD, same parameterization pattern as
        neighbors()'s `type(e) IN $types` -- no injection surface). Results are
        ordered by RediSearch relevance score, descending, capped at `k`; each
        result is the node's properties with an added "score" key (float)."""
        sanitized = _sanitize_fulltext_query(query)
        if not sanitized:
            return []
        cypher = "CALL db.idx.fulltext.queryNodes('Sym', $q) YIELD node, score"
        params: dict[str, Any] = {"q": sanitized, "k": k}
        if kinds:
            cypher += " WHERE node.kind IN $kinds"
            params["kinds"] = list(kinds)
        cypher += " RETURN node, score ORDER BY score DESC LIMIT $k"
        res = self._g.query(cypher, params)
        return [{**node.properties, "score": score} for node, score in res.result_set]

    def graph_exists(self) -> bool:
        """True, если граф-ключ self.graph_name существует (membership в GRAPH.LIST).

        Единственный read-only способ спросить о существовании: любой GRAPH.QUERY
        (включая безобидный MATCH из stats()) auto-vivify'ит пустой граф-ключ как
        побочный эффект (наблюдалось живьём на v4.18.11 в T6) -- поэтому cli.stats
        проверяет существование ИМЕННО этим методом ДО первого запроса, и redis-вызов
        остаётся внутри stores/falkordb/ (граница импортов)."""
        return self.graph_name in self._connect().list_graphs()

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

    def delete_graph(self) -> None:
        """Удаляет граф-ключ self.graph_name, если он существует; идемпотентно.

        Несуществующий граф: GRAPH.DELETE отвечает `ResponseError("Invalid graph
        operation on empty key")` (замерено живьём на v4.18.11, тот же маркер, что
        наблюдался в swap_in-тестах T3) -- глотаем подстрочно, по образцу
        ddl._swallow_ddl_errors; любая другая ошибка пробрасывается. Кэшированная
        Graph-обёртка сбрасывается в None в обоих исходах (тот же довод, что в
        swap_in: схемный кэш клиента не переживает смену данных под ключом --
        следующий пользователь этого store-объекта должен получить свежий handle).
        """
        try:
            self._g.delete()
        except Exception as e:
            if "empty key" not in str(e).lower():
                raise
        finally:
            self._graph = None

    def raw(self, cypher: str, params: dict | None = None) -> Any:
        """Internal-only, не для MCP: тонкий проброс в g.query(cypher, params)."""
        return self._g.query(cypher, params)
