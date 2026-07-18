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

import redis.exceptions
from falkordb import FalkorDB

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb import batch, ddl
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.graph import Hop

# M3 T7: db.idx.vector.queryNodes over a Chunk.embedding that has NO vector index
# (degraded graph -- no embedder has ever run this workspace, see ddl.ensure_schema's
# `dim`-gated CREATE VECTOR INDEX) raises this exact substring -- confirmed live
# against FalkorDB v4.18.11 (redis.exceptions.ResponseError). Same discipline as
# ddl.py's `_IGNORABLE_DDL_MARKERS`: a substring match on a real, empirically-captured
# error message, not a blind catch-and-swallow of every ResponseError (a malformed
# query or a genuinely different server-side failure must still propagate).
_NO_VECTOR_INDEX_MARKER = "undefined attribute"

# search_vector_chunks' service filter: `queryNodes(..., $k, ...)` picks its k nearest
# neighbors BEFORE any `WHERE node.service = $service` runs (k is an argument to the
# KNN procedure itself, not a Cypher LIMIT applied after filtering -- unlike
# search_fulltext/search_text_chunks, whose fulltext procedure has no k argument at
# all and where LIMIT naturally runs after WHERE) -- so filtering can leave FEWER than
# k rows even when >=k service-matching chunks exist in the graph. Over-fetching this
# multiple of k from the procedure call, THEN filtering, THEN trimming to k in Python
# (not a Cypher LIMIT after the WHERE -- trimming client-side keeps this method's own
# ORDER BY/slice logic in one place) gives real headroom without a second round trip.
_VECTOR_SERVICE_FILTER_OVERFETCH = 4

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


def _fulltext_or_query(sanitized: str) -> str | None:
    """M4 T3: builds the second-pass (AND -> OR) RediSearch query text from an
    already-sanitized query (see _sanitize_fulltext_query) -- `None` when there is
    nothing to widen: a single-token query's implicit-AND and OR forms are
    identical (AND/OR over exactly one term is just that term), so callers must
    skip the second pass entirely rather than re-run an identical query against
    the same index. `"".split()` is also `[]` (0 tokens), so an already-empty
    sanitized query (defensively -- real callers short-circuit on that before
    ever reaching here) likewise yields `None`, not a bogus `""` OR-query.

    Multi-token: the same tokens re-joined with `" | "` -- RediSearch's OR
    operator (see _FULLTEXT_SPECIAL_CHARS' own docstring for why a raw `|` in
    USER input is stripped; here it is deliberately reintroduced as an operator,
    not user-supplied text)."""
    tokens = sanitized.split()
    return " | ".join(tokens) if len(tokens) > 1 else None


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
        result is the node's properties with an added "score" key (float).

        M4 T3: when this first (implicit-AND) pass returns zero rows AND the
        sanitized query has more than one token, a second pass re-runs the same
        query OR-joined -- see _fulltext_result_set's docstring for the full
        mixed-language rationale and the RRF-dampens-OR-noise argument (shared
        with search_text_chunks, which gets the identical fallback over Chunk)."""
        sanitized = _sanitize_fulltext_query(query)
        if not sanitized:
            return []
        cypher = "CALL db.idx.fulltext.queryNodes('Sym', $q) YIELD node, score"
        params: dict[str, Any] = {"q": sanitized, "k": k}
        if kinds:
            cypher += " WHERE node.kind IN $kinds"
            params["kinds"] = list(kinds)
        cypher += " RETURN node, score ORDER BY score DESC LIMIT $k"
        result_set = self._fulltext_result_set(cypher, params, sanitized)
        return [{**node.properties, "score": score} for node, score in result_set]

    def _fulltext_result_set(
        self, cypher: str, params: dict[str, Any], sanitized: str
    ) -> list:
        """M4 T3: shared two-pass (AND -> OR) execution for search_fulltext (Sym)
        and search_text_chunks (Chunk) -- both build their own index/WHERE/RETURN
        `cypher` (with `params["q"]` holding the first-pass, implicit-AND query
        text) and hand the finished query here just for this fallback, so the
        AND->OR logic itself lives in exactly one place.

        Why: RediSearch's fulltext queryNodes runs an implicit AND over query
        tokens (see _sanitize_fulltext_query) -- ALL of them must match the SAME
        document. A mixed-language natural-language query (e.g. a Russian question
        naming an English identifier, the M3 final review's own finding) needs
        just ONE token with zero matches in an English-identifier corpus to zero
        out the whole AND query, even though the English identifier token alone
        would have found exactly the right node. Retrying with OR only when the
        first pass is EMPTY keeps this a pure widening with no other behavior
        change: a query that already found something under AND is returned as-is
        (identical results, a single round trip, the fallback isn't even
        attempted), and a single-token query has no second pass to run at all
        (_fulltext_or_query returns None -- its AND and OR forms coincide).

        Noise from a widened OR match (a result sharing only ONE of several query
        tokens, not all of them) isn't filtered out here -- it doesn't need to be:
        every caller of search_fulltext/search_text_chunks feeds this leg's
        ranking into RRF (query/retrieval.rrf, k=60), which is rank-based, not
        score-based -- a weak OR-only match ranks low within THIS leg's own
        ordering and so contributes only a small 1/(k+rank+1) term to the fused
        score, the same self-dampening RRF already gives any weak candidate in
        either leg. No schema/mode_used change: OR is an internal detail of
        whichever text leg triggered it, invisible to callers of search_code/
        find_entrypoint beyond "more candidates, ranked appropriately"."""
        res = self._g.query(cypher, params)
        if res.result_set:
            return res.result_set
        or_query = _fulltext_or_query(sanitized)
        if or_query is None:
            return res.result_set
        res = self._g.query(cypher, {**params, "q": or_query})
        return res.result_set

    def search_vector_chunks(
        self, vec: list[float], k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """`CALL db.idx.vector.queryNodes('Chunk', 'embedding', $k, vecf32($vec)) YIELD
        node, score` -- score is cosine DISTANCE (empirically confirmed live: querying
        with a vector identical to a stored one yields ~0, a near-orthogonal one yields
        ~1 -- i.e. LOWER is more similar, the opposite convention from search_fulltext's
        RediSearch relevance score), so results are ordered `ORDER BY score ASC` (nearest
        first), NOT DESC like search_fulltext.

        `service`, if given, over-fetches `k * _VECTOR_SERVICE_FILTER_OVERFETCH`
        candidates from the vector procedure itself (see that constant's own docstring
        for why a plain post-hoc `WHERE` can't just reuse the same `k`), applies
        `WHERE node.service = $service`, and trims the result back down to `k` in
        Python (not a second `LIMIT $k` in the Cypher -- keeping the final cap as one
        plain Python slice here, right where the docstring explaining it lives).

        No vector index on this graph (degraded -- ensure_schema only creates one when
        `dim` is given, i.e. some embedder has actually run) -> `[]`, never an
        exception: confirmed live that FalkorDB raises `redis.exceptions.ResponseError`
        ("...undefined attribute...") querying an absent index, which this method
        catches by that specific substring (same discipline as ddl.py's
        `_swallow_ddl_errors`) and turns into an honest empty result -- indistinguishable
        from "the index exists but nothing matched", which is exactly the right
        degraded-graph behavior for a caller that just wants "no vector matches", not a
        crash."""
        fetch_k = k * _VECTOR_SERVICE_FILTER_OVERFETCH if service else k
        cypher = (
            "CALL db.idx.vector.queryNodes('Chunk', 'embedding', $k, vecf32($vec)) "
            "YIELD node, score"
        )
        params: dict[str, Any] = {"k": fetch_k, "vec": vec}
        if service:
            cypher += " WHERE node.service = $service"
            params["service"] = service
        cypher += " RETURN node, score ORDER BY score ASC"
        try:
            res = self._g.query(cypher, params)
        except redis.exceptions.ResponseError as e:
            if _NO_VECTOR_INDEX_MARKER not in str(e).lower():
                raise
            return []
        return [(node.properties, score) for node, score in res.result_set][:k]

    def search_vector_chunks_exact(
        self, vec: list[float], k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """M5 T2 (pilot Bug A fix): deterministic full-scan twin of
        search_vector_chunks -- `MATCH (c:Chunk) WHERE c.embedding IS NOT NULL [AND
        c.service = $service] RETURN c, vec.cosineDistance(c.embedding, vecf32($vec))
        AS dist ORDER BY dist ASC, c.id ASC LIMIT $k`, no ANN index (`db.idx.vector.
        queryNodes`, HNSW) involved at all. Motivation: that index rebuilds unseeded
        on every graph load (live-confirmed by the M4 pilot, docs/superpowers/reports/
        2026-07-18-m4-pilot.md §4.1) -- hit@k measured against it is NOT reproducible
        across identical eval runs. This method trades ANN's speed for an O(n) scan
        any CI/comparison run can afford at eval scale; production/MCP search is
        untouched and stays on search_vector_chunks (ANN) -- see `codegraph eval
        retrieval --exact` (cli.py) for the only caller that opts into this.

        score semantics MATCH search_vector_chunks EXACTLY, not merely "the same sign
        convention" -- live-verified against this same FalkorDB build (v4.18.11):
        querying `db.idx.vector.queryNodes` and `vec.cosineDistance` with the SAME
        stored vectors and the SAME query vector returns byte-identical numbers (0.0
        for an identical vector, 1.0 orthogonal, 2.0 opposite -- both apparently
        compute the standard `1 - cosine_similarity` formula). So `dist` is returned
        AS-IS here, no `1 - dist` inversion -- inverting it would silently desync
        exact's score from ANN's for any caller that ever compares the two
        numerically, and RRF/eval code must stay agnostic to which twin produced a
        given (id, score) pair (query.retrieval routes through either transparently).

        `ORDER BY dist ASC, c.id ASC`: the id tiebreak is REQUIRED for determinism,
        not cosmetic -- two chunks at the exact same distance (plausible on a small/
        synthetic eval fixture, and the whole point of "exact" is a byte-identical
        result across repeated identical calls) would otherwise order however
        FalkorDB's own internal tie resolution happens to fall, which is not a
        documented/guaranteed stable order.

        Unlike search_vector_chunks, no try/except ResponseError dance is needed:
        this is a plain MATCH+WHERE+ORDER BY+LIMIT over Chunk nodes, not a `CALL
        db.idx.vector.queryNodes` procedure that requires a vector INDEX to exist --
        a degraded graph (no embedder has ever run, so no Chunk carries a non-null
        embedding, or the graph has no Chunk nodes at all) simply matches zero rows
        and returns `[]` through completely ordinary Cypher semantics (live-verified:
        MATCH on an entirely absent label, on a graph key that doesn't even exist
        yet, raises nothing and returns an empty result_set) -- there is no separate
        "index missing" failure mode to catch here.

        `service`, if given, is a plain `WHERE c.service = $service` alongside the
        `IS NOT NULL` check -- unlike search_vector_chunks' k*
        _VECTOR_SERVICE_FILTER_OVERFETCH dance, no over-fetch is needed: `LIMIT $k`
        runs in Cypher AFTER both WHERE clauses evaluate over every matching row, not
        as an argument to a KNN procedure that already truncated to k BEFORE the
        service filter could run (same reasoning as search_text_chunks' own
        docstring, which over-fetches for the identical reason search_vector_chunks
        does and NOT for the identical reason this method doesn't need to)."""
        cypher = "MATCH (c:Chunk) WHERE c.embedding IS NOT NULL"
        params: dict[str, Any] = {"vec": vec, "k": k}
        if service:
            cypher += " AND c.service = $service"
            params["service"] = service
        cypher += (
            " RETURN c, vec.cosineDistance(c.embedding, vecf32($vec)) AS dist "
            "ORDER BY dist ASC, c.id ASC LIMIT $k"
        )
        res = self._g.query(cypher, params)
        return [(node.properties, dist) for node, dist in res.result_set]

    def search_text_chunks(
        self, query: str, k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """Mirrors search_fulltext's sanitize-then-short-circuit contract (see
        _sanitize_fulltext_query) over the Chunk(text, context_header) fulltext index
        (ddl.ensure_schema) instead of Sym -- `service`, if given, filters `WHERE
        node.service = $service` same as search_fulltext's `kinds`; the fulltext
        procedure itself takes no `k` argument, so (unlike search_vector_chunks) a
        plain `ORDER BY score DESC LIMIT $k` AFTER the WHERE is already correct with no
        over-fetch needed. Returns `[(chunk_props, score)]` tuples -- see
        search_vector_chunks's own docstring for why, shared with it.

        M4 T3: also mirrors search_fulltext's AND->OR fallback (see
        _fulltext_result_set's docstring for the full mixed-language rationale) --
        zero rows on a multi-token first pass retries OR-joined before returning."""
        sanitized = _sanitize_fulltext_query(query)
        if not sanitized:
            return []
        cypher = "CALL db.idx.fulltext.queryNodes('Chunk', $q) YIELD node, score"
        params: dict[str, Any] = {"q": sanitized, "k": k}
        if service:
            cypher += " WHERE node.service = $service"
            params["service"] = service
        cypher += " RETURN node, score ORDER BY score DESC LIMIT $k"
        result_set = self._fulltext_result_set(cypher, params, sanitized)
        return [(node.properties, score) for node, score in result_set]

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
