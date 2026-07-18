"""GraphQuery: read-only слой поверх GraphStore для MCP v0 (graph_stats/get_source/
expand_neighbors/who_calls). Единственный потребитель Cypher-скрытого GraphStore.raw()
здесь -- НИКАКОЙ (raw() не экспонируется даже внутри этого модуля: все 4 метода строятся
на get_nodes()/neighbors()/stats(), см. stores/graph.py GraphStore.raw() docstring).

Fresh-store-per-call (обязательное свойство, не оптимизация): __init__ принимает
`store_factory: Callable[[], GraphStore]`, а не готовый store-инстанс. Каждый публичный
метод вызывает store_factory() заново и использует полученный handle только в рамках
одного вызова, никогда не кэширует его на self. Причина -- живая находка Task 3
(.superpowers/sdd/m1b-task-3-report.md, раздел "RENAME semantics observed"):
FalkorDB python-клиент кэширует schema id->name на объекте `falkordb.Graph`
(labels/properties/relationships), а `RENAME` (используемый blue/green-свопом в
pipeline/load.py при КАЖДОМ `codegraph index`) НЕ бампает FalkorDB schema-version
counter -- обычный auto-refresh (`SchemaVersionMismatchException`) по этому пути не
срабатывает. MCP-сервер живёт долго и обрабатывает запросы в ОТДЕЛЬНОМ процессе от
`codegraph index`; если бы GraphQuery держал один FalkorStore (и через него один
`falkordb.Graph`) на весь свой жизненный цикл, то index, запущенный в другом процессе
между двумя MCP-вызовами, привёл бы к тому, что уже открытый Graph-хендл после RENAME
декодировал бы новые properties через СТАРЫЕ id->name таблицы -- тот же тихий
гибрид-баг, что Task 3 воспроизвёл и закрыл инвалидацией `self._graph = None` внутри
`FalkorStore.swap_in`. Тот фикс защищает только САМ FalkorStore-объект, который сделал
своп; он не защищает НИКАКОЙ другой долгоживущий FalkorStore-объект, смотрящий на то же
имя графа. Пересоздание FalkorStore (а значит -- свежий `select_graph()`, пустой
schema-кэш) на каждый tool-call дёшево при нашем масштабе (M1) и структурно иммунно к
этому классу багов, а не полагается на то, что клиент когда-нибудь заметит RENAME.

Ограничения ответов (контроллерская поправка 5): expand_neighbors/who_calls никогда не
кидают исключения наружу и никогда не возвращают неограниченный список -- limit
(expand_neighbors, явный параметр) / внутренний _DEFAULT_CALLER_LIMIT (who_calls, нет
пользовательского параметра в контракте брифа) всегда капают суммарные
hops/callers, а `truncated` отражает факт обрезки. Все 4 метода -- error-dict вместо
исключений (поправка 3): узел/файл не найден, путь убегает из корня сервиса, store
недоступен -- всё это `{"error": "..."}"`, никогда `raise`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Literal

from codegraph.core.selectors import RouteSelector, parse_selector
from codegraph.core.spans import LineIndex
from codegraph.embedding.base import Embedder
from codegraph.query import retrieval, traverse
from codegraph.stores.falkordb.connection import StoreError, StoreUnavailable
from codegraph.stores.graph import GraphStore

_VALID_DIRECTIONS = frozenset({"out", "in", "both"})  # expand_neighbors direction validation
_DEPTH_MIN, _DEPTH_MAX = 1, 3  # expand_neighbors depth clamp
_MAX_DEPTH_MIN, _MAX_DEPTH_MAX = 1, 5  # who_calls max_depth clamp
_DEFAULT_CALLER_LIMIT = 50  # who_calls: внутренний cap суммарных callers (нет параметра limit
# в контракте брифа who_calls(node_id, transitive, max_depth) -- в отличие от
# expand_neighbors -- но "Ограничения ответов: ... truncated ... обязателен" распространяется
# на оба инструмента, поэтому cap внутренний, module-level (тесты monkeypatch'ат его для
# детерминированной проверки truncated без построения графа на 50+ узлов).

# -- M2 T8 --
_VALID_TRACE_DIRECTIONS = frozenset({"downstream", "upstream"})
_MAX_SEGMENTS_MIN, _MAX_SEGMENTS_MAX = 1, 20  # trace_process max_segments clamp
_MAX_HOPS_MIN, _MAX_HOPS_MAX = 1, 12  # find_paths max_hops clamp
_FIND_ENTRYPOINT_K_MIN, _FIND_ENTRYPOINT_K_MAX = 1, 20  # find_entrypoint k clamp
_BUSINESS_PROCESS_KIND = "BusinessProcess"

# -- M3 T7: search_code --
_VALID_SEARCH_MODES = frozenset({"hybrid", "vector", "text"})
_SEARCH_CODE_K_MIN, _SEARCH_CODE_K_MAX = 1, 20


class GraphQuery:
    """service_paths: {service_name: абсолютный путь корня сервиса на диске} -- источник
    для get_source (сервис -> корень -> relpath). Строится вызывающей стороной
    (mcp/server.py: build_server из cfg.services, уже резолвленных config.loader в
    абсолютные пути).

    embedder_factory (M3 T7, опционален -- None сохраняет ровно M2-поведение:
    find_entrypoint остаётся чистым fulltext, search_code недоступен на уровне MCP
    (build_server всегда даёт свой default), а _get_embedder() ниже тривиально
    возвращает None без единого вызова factory): в ОТЛИЧИЕ от store_factory,
    embedder НЕ пересоздаётся на каждый вызов -- см. _get_embedder's докстринг про
    то, почему fresh-per-call здесь НЕ применяется (эмбеддер -- тяжёлая модель, а не
    тонкий сетевой хендл с проблемой протухающего schema-кэша, см. модульный
    докстринг про store_factory)."""

    def __init__(
        self,
        store_factory: Callable[[], GraphStore],
        service_paths: dict[str, Path],
        embedder_factory: Callable[[], Embedder | None] | None = None,
    ) -> None:
        self.store_factory = store_factory
        self.service_paths = service_paths
        self.embedder_factory = embedder_factory
        self._embedder: Embedder | None = None

    def _get_embedder(self) -> Embedder | None:
        """Ленивое, но НЕ fresh-per-call создание embedder'а -- см. класс-докстринг.
        Кэшируется на self ПОСЛЕ ПЕРВОГО УСПЕШНОГО создания (embedder_factory()
        вернул не-None): построение реального embedder'а -- тяжёлая операция
        (загрузка модели sentence-transformers, HTTP-хендл к OpenAI/Voyage), которую
        не нужно повторять на каждый MCP tool-call, В ОТЛИЧИЕ ОТ store_factory,
        которую нужно вызывать заново на каждый вызов (см. модульный докстринг про
        RENAME/schema-кэш FalkorDB-клиента -- у embedder'а нет аналогичной "живая
        модель под чужими данными" проблемы, кэш безопасен весь срок жизни
        GraphQuery). Неуспех (factory() вернула None -- напр. `_default_embedder_
        factory` в mcp/server.py поймала CodegraphError) НЕ кэшируется -- следующий
        вызов пробует factory() заново, а не запоминает "недоступен" навсегда; это
        дёшево (тот же catch-and-return-None, что уже произошёл один раз) и на
        случай, если причина недоступности была временной (напр. сетевой сбой при
        первой загрузке HF-модели)."""
        if self._embedder is not None:
            return self._embedder
        if self.embedder_factory is None:
            return None
        embedder = self.embedder_factory()
        if embedder is not None:
            self._embedder = embedder
        return embedder

    def graph_stats(self) -> dict:
        try:
            store = self.store_factory()
            return store.stats()
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def get_source(self, node_id: str, context_lines: int = 0) -> dict:
        try:
            store = self.store_factory()
            nodes = store.get_nodes([node_id])
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

        if not nodes:
            return {"error": f"node not found: {node_id}"}
        node = nodes[0]

        relpath = node.get("relpath")
        if relpath is None:
            return {
                "error": f"node has no source location (no relpath -- Service node?): {node_id}"
            }

        service = node.get("service")
        root = self.service_paths.get(service)
        if root is None:
            return {"error": f"unknown service {service!r} for node {node_id}"}

        # Путь строится ТОЛЬКО как service_paths[service]/relpath -- никаких абсолютных
        # relpath (pathlib "/" отбрасывает левую часть целиком, если правая абсолютна --
        # проверка is_absolute() обязана идти ДО join, иначе join сам себя обесценивает)
        # и никакого ".."-побега за пределы корня сервиса (amendment 2).
        rel = Path(relpath)
        if rel.is_absolute():
            return {"error": f"invalid relpath (must be relative to service root): {relpath}"}
        root_resolved = Path(root).resolve()
        candidate = (root_resolved / rel).resolve()
        if not candidate.is_relative_to(root_resolved):
            return {"error": f"invalid relpath (escapes service root): {relpath}"}
        if not candidate.is_file():
            return {"error": f"source file not found: {candidate}"}

        start_byte, end_byte = node.get("start_byte"), node.get("end_byte")
        start_line, end_line = node.get("start_line"), node.get("end_line")
        if start_byte is None or end_byte is None or start_line is None or end_line is None:
            return {"error": f"node has no byte/line span: {node_id}"}

        data = candidate.read_bytes()
        # stale -- ВСЕГДА по исходному (не расширенному context_lines) байтовому срезу:
        # это единственный срез, для которого content_hash вообще был посчитан при индексации.
        computed_hash = sha256(data[start_byte:end_byte]).hexdigest()
        stale = computed_hash != node.get("content_hash")

        # context_lines расширяет срез ПО СТРОКАМ файла (не байтам) вокруг исходного
        # start_line/end_line узла, клампится к границам файла.
        ctx = max(0, context_lines)
        li = LineIndex(data)
        last_line0 = max(li.line_count - 1, 0)
        out_start0 = max(0, (start_line - 1) - ctx)
        out_end0 = min(last_line0, (end_line - 1) + ctx)
        seg_start, _ = li.line_span(out_start0)
        _, seg_end = li.line_span(out_end0)

        return {
            "source": data[seg_start:seg_end].decode("utf-8", errors="replace"),
            "file": str(candidate),
            "start_line": out_start0 + 1,
            "end_line": out_end0 + 1,
            "stale": stale,
        }

    def expand_neighbors(
        self,
        node_id: str,
        edge_types: Sequence[str] | None = None,
        direction: Literal["out", "in", "both"] = "both",
        depth: int = 1,
        limit: int = 50,
    ) -> dict:
        """edge_types=None и edge_types=[] эквивалентны -- оба означают "без фильтра по
        типу ребра" (store._one_way делает `if edge_types:` перед добавлением `WHERE
        type(e) IN $types`, пустой список falsy -- та же ветка, что и None).

        Невалидный direction -> `{"error": ...}` ДО обращения к store_factory (M2):
        direction в сигнатуре типизирован Literal["out","in","both"], но это лишь
        подсказка типа для статических чекеров/схемы -- вызывающая сторона (в т.ч.
        MCP-клиент, CLI trace) может передать произвольную строку в рантайме, и эта
        проверка -- защитный рубеж независимо от вызывающего слоя."""
        if direction not in _VALID_DIRECTIONS:
            return {"error": f"invalid direction: {direction!r}"}
        depth = max(_DEPTH_MIN, min(_DEPTH_MAX, depth))
        try:
            store = self.store_factory()
            visited = {node_id}
            frontier = [node_id]
            nodes_by_id: dict[str, dict] = {}
            hops: list[dict] = []
            truncated = False
            for _ in range(depth):
                next_frontier: list[str] = []
                for nid in frontier:
                    if len(hops) >= limit:
                        truncated = True
                        break
                    remaining = limit - len(hops)
                    # remaining+1: запрашиваем на один hop больше, чем нужно, чтобы
                    # отличить "у узла ровно remaining соседей" от "соседей больше,
                    # чем влезает" -- store.neighbors() сам уже режет по LIMIT,
                    # без этого зонда переполнение неотличимо от точного совпадения.
                    step = store.neighbors(nid, edge_types, direction, remaining + 1)
                    if len(step) > remaining:
                        step = step[:remaining]
                        truncated = True
                    for edge_type, edge_props, node_dict, hop_direction in step:
                        neighbor_id = node_dict.get("id")
                        hops.append(
                            {
                                "node": nid,
                                "edge_type": edge_type,
                                "edge_props": edge_props,
                                "neighbor": neighbor_id,
                                "direction": hop_direction,
                            }
                        )
                        if neighbor_id is not None:
                            nodes_by_id[neighbor_id] = node_dict
                            if neighbor_id not in visited:
                                visited.add(neighbor_id)
                                next_frontier.append(neighbor_id)
                if truncated:
                    break
                frontier = next_frontier
                if not frontier:
                    break
            return {"nodes": list(nodes_by_id.values()), "hops": hops, "truncated": truncated}
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def who_calls(self, node_id: str, transitive: bool = False, max_depth: int = 3) -> dict:
        max_depth = max(_MAX_DEPTH_MIN, min(_MAX_DEPTH_MAX, max_depth))
        depth = max_depth if transitive else 1
        try:
            store = self.store_factory()
            visited = {node_id}
            frontier = [node_id]
            callers_by_id: dict[str, dict] = {}
            truncated = False
            for _ in range(depth):
                next_frontier: list[str] = []
                for nid in frontier:
                    if len(callers_by_id) >= _DEFAULT_CALLER_LIMIT:
                        truncated = True
                        break
                    remaining = _DEFAULT_CALLER_LIMIT - len(callers_by_id)
                    step = store.neighbors(nid, ["CALLS"], "in", remaining + 1)
                    if len(step) > remaining:
                        step = step[:remaining]
                        truncated = True
                    for _edge_type, _edge_props, node_dict, _direction in step:
                        caller_id = node_dict.get("id")
                        if caller_id is None:
                            continue
                        callers_by_id[caller_id] = node_dict
                        if caller_id not in visited:
                            visited.add(caller_id)
                            next_frontier.append(caller_id)
                if truncated:
                    break
                frontier = next_frontier
                if not frontier:
                    break
            return {"callers": list(callers_by_id.values()), "truncated": truncated}
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def trace_process(
        self,
        entrypoint_id: str,
        direction: Literal["downstream", "upstream"] = "downstream",
        max_segments: int = 12,
        min_confidence: float = 0.3,
        include_source: bool = False,
    ) -> dict:
        """M2: downstream only -- direction="upstream" is a deliberate deferral
        (query.traverse.trace_process only implements the out-edge/downstream walk;
        an upstream trace would need its own transition table, not just direction
        flip -- see the M2 plan's trace_process interface note), not a bug: it
        returns a structured error like any other invalid/unsupported input here,
        never an exception. Both invalid-direction and upstream-not-supported are
        checked BEFORE store_factory() (same amendment-1-adjacent principle as
        expand_neighbors' direction check -- no store needed to reject either)."""
        if direction not in _VALID_TRACE_DIRECTIONS:
            return {"error": f"invalid direction: {direction!r}"}
        if direction == "upstream":
            return {"error": "upstream tracing not supported in M2"}
        max_segments = max(_MAX_SEGMENTS_MIN, min(_MAX_SEGMENTS_MAX, max_segments))
        try:
            store = self.store_factory()
            result = traverse.trace_process(store, entrypoint_id, max_segments, min_confidence)
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}
        if "error" in result or not include_source:
            return result
        self._attach_sources(result)
        return result

    def _attach_sources(self, result: dict) -> None:
        """include_source=True: best-effort get_source() on every node the trace
        surfaced (entry/step/exit-channel), mutating each node dict in place
        (fresh dicts from this same trace call -- safe to mutate before returning
        to the caller). A node get_source() can't resolve (e.g. a Channel has no
        source location) is left as-is, not an error -- this is an enrichment, not
        a requirement."""
        for segment in result.get("segments", []):
            self._attach_source_to_node(segment["entry"])
            for step in segment["steps"]:
                self._attach_source_to_node(step["node"])
            for exit_ in segment["exits"]:
                self._attach_source_to_node(exit_["channel"])

    def _attach_source_to_node(self, node: dict) -> None:
        node_id = node.get("id")
        if node_id is None:
            return
        src = self.get_source(node_id)
        if "error" not in src:
            node["source"] = src["source"]

    def find_paths(
        self,
        from_id: str,
        to_id: str,
        max_hops: int = 8,
        edge_types: Sequence[str] | None = None,
    ) -> dict:
        max_hops = max(_MAX_HOPS_MIN, min(_MAX_HOPS_MAX, max_hops))
        try:
            store = self.store_factory()
            return traverse.find_paths(store, from_id, to_id, max_hops, edge_types)
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def list_processes(self) -> dict:
        """BusinessProcess-узлы графа, отсортированные по id (детерминизм --
        store.get_nodes_by_kind не гарантирует порядок строк). Полные property-
        dict'ы как есть (id/name/entrypoint_id/source + любые прочие props) -- та
        же конвенция "не подрезать", что у expand_neighbors'а "nodes"."""
        try:
            store = self.store_factory()
            nodes = store.get_nodes_by_kind(_BUSINESS_PROCESS_KIND)
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}
        return {"processes": sorted(nodes, key=lambda n: n.get("id") or "")}

    def find_entrypoint(
        self,
        query: str,
        kinds: Sequence[str] | None = None,
        k: int = 5,
    ) -> dict:
        """v2 (M3 T7): тонкая обёртка над retrieval.find_entrypoint (store_factory +
        embedder через self._get_embedder() + StoreError-граница) -- вся RRF-
        фьюжн/деградационная логика живёт там (см. её докстринг). store.search_fulltext
        делает и sanitize, и сам fulltext-запрос -- пустой результат (в т.ч. запрос,
        целиком состоящий из RediSearch-спецсимволов, или граф без единого совпадения)
        НЕ ошибка, обычный {"results": [], "mode_used": ...}."""
        k = max(_FIND_ENTRYPOINT_K_MIN, min(_FIND_ENTRYPOINT_K_MAX, k))
        try:
            store = self.store_factory()
            embedder = self._get_embedder()
            return retrieval.find_entrypoint(store, embedder, query, k=k, kinds=kinds)
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def search_code(
        self,
        query: str,
        k: int = 8,
        service: str | None = None,
        mode: Literal["hybrid", "vector", "text"] = "hybrid",
        exact: bool = False,
    ) -> dict:
        """M3 T7, 9-й MCP-инструмент: тонкая обёртка над retrieval.search_code, тот
        же store_factory/embedder/StoreError-паттерн, что find_entrypoint выше.
        mode валидируется здесь, ДО store_factory() -- тот же принцип "дешёвая,
        заведомо проваленная проверка не должна платить за подключение к store", что
        у expand_neighbors/trace_process direction (см. модульный докстринг,
        "Ограничения ответов"); retrieval.search_code ПОВТОРНО валидирует mode само
        (defense in depth для прямых вызывающих retrieval.py в обход GraphQuery).

        `exact` (M5 T2, pilot Bug A): passed straight through to retrieval.search_code
        -- see its own docstring for what it does (routes the vector leg through the
        deterministic full-scan store method instead of ANN). Deliberately NOT part
        of the search_code MCP tool's own signature (mcp/server.py's hand-written
        wrapper omits it) -- this parameter exists for `codegraph eval retrieval
        --exact` (cli.py) only; every MCP/agent-facing caller keeps getting ANN."""
        if mode not in _VALID_SEARCH_MODES:
            return {"error": f"invalid search mode: {mode!r}"}
        k = max(_SEARCH_CODE_K_MIN, min(_SEARCH_CODE_K_MAX, k))
        try:
            store = self.store_factory()
            embedder = None if mode == "text" else self._get_embedder()
            return retrieval.search_code(
                store, embedder, query, k=k, service=service, mode=mode, exact=exact
            )
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}

    def resolve_selector(self, selector: str) -> dict:
        """M3 T2: resolves a "<service>:<METHOD> <path>" / "<service>:<dotted.name>"
        selector (core.selectors.parse_selector -- same grammar cli.py's `trace`
        command exposes, see that module's docstring) straight against the LOADED
        graph, no staging.db involved at all -- the M2 final review carry-item this
        closes: `codegraph trace` used to hard-require a prior `codegraph index` run's
        staging.db on disk purely to resolve the selector string, even though the
        walk itself was always graph-only.

        Route form: every Channel(http_route) is fetched (`get_nodes_by_kind`,
        Python-side filter on channel_kind/owner_service/http_method/path_template --
        no dedicated store method for this, unlike find_by_qualified, since
        get_nodes_by_kind + neighbors already say everything needed and Channel ids
        are deterministic on this exact triple so at most one real match is expected;
        see core/schema.py make_channel_node/core/ids.chan_http) then its HANDLES
        out-neighbor is the entrypoint -- sorted by id for a deterministic pick on the
        defensive duplicate-match case, same convention as linking/processes.py's
        `_route_index`/`_handles_index`.
        Qualified form: `store.find_by_qualified(service, qualified)`.

        Returns {"node_id": ...} on success, {"error": ...} otherwise -- a malformed
        selector (parse_selector returns None) and a well-formed-but-unresolved one
        are reported with the SAME "entrypoint not found for selector: ..." message
        (matching the pre-M3 CLI's own undifferentiated wording for both cases). A
        malformed selector is rejected BEFORE store_factory() is ever called -- same
        principle as expand_neighbors'/trace_process's own direction validation (see
        this module's docstring, "Ограничения ответов"): a cheap, pure precondition
        already known to fail shouldn't pay for a store connection first."""
        parsed = parse_selector(selector)
        if parsed is None:
            return {"error": f"entrypoint not found for selector: {selector}"}
        try:
            store = self.store_factory()
            if isinstance(parsed, RouteSelector):
                node_id = self._resolve_route_selector(store, parsed)
            else:
                node = store.find_by_qualified(parsed.service, parsed.qualified)
                node_id = node.get("id") if node else None
        except (StoreError, StoreUnavailable) as e:
            return {"error": f"falkordb unreachable: {e}"}
        if node_id is None:
            return {"error": f"entrypoint not found for selector: {selector}"}
        return {"node_id": node_id}

    @staticmethod
    def _resolve_route_selector(store: GraphStore, sel: RouteSelector) -> str | None:
        candidates = sorted(
            (
                n for n in store.get_nodes_by_kind("Channel")
                if n.get("channel_kind") == "http_route"
                and n.get("owner_service") == sel.service
                and n.get("http_method") == sel.method
                and n.get("path_template") == sel.path
            ),
            key=lambda n: n.get("id") or "",
        )
        if not candidates:
            return None
        hops = store.neighbors(candidates[0]["id"], ["HANDLES"], "out", 10)
        handlers = sorted(hops, key=lambda h: h[2].get("id") or "")
        return handlers[0][2].get("id") if handlers else None
