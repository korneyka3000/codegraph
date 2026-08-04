"""retrieval.py: гибридный (text + vector) поиск по Chunk-узлам графа (M3 T7) --
`rrf` (generic Reciprocal Rank Fusion) + `search_code` (MCP-инструмент №9) +
`find_entrypoint` (v2 -- заменяет M2 T8's чисто-fulltext реализацию из query/api.py,
см. GraphQuery.find_entrypoint, теперь тонкая обёртка вокруг функции этого модуля).

Оба потребителя (`search_code`/`find_entrypoint`) делят одну и ту же деградационную
логику: vector-ветка нужна embedder (тяжёлая модель -- см. query/api.py
GraphQuery._get_embedder про кэш-политику, ОТЛИЧНУЮ от fresh-store-per-call) И
совпадение Meta.embed_model графа с embedder.model_id (иначе граф либо вообще не
эмбеден, либо эмбеден ДРУГОЙ моделью -- векторный индекс для ТЕКУЩЕГО embedder'а либо
не существует, либо существует, но хранит НЕ те вектора, которые embedder.embed_query
сейчас производил бы; сравнивать нужно именно на этом уровне, а не полагаться только
на low-level "нет индекса -> пусто" в FalkorStore.search_vector_chunks, чтобы дать
ПОЛЬЗОВАТЕЛЮ actionable "reindex needed", а не молчаливый пустой результат). Оба
условия объединены в `_vector_unusable_reason` -- единственное место этой проверки.

Реакция на "вектор недоступен" РАЗНАЯ у двух потребителей и разная по mode:
  - `search_code(mode="vector")` -- error dict (пользователь явно попросил вектор).
  - `search_code(mode="hybrid")` -- молчаливая деградация в text-only
    (`mode_used: "text"`), НЕ ошибка.
  - `find_entrypoint` (нет параметра mode вовсе, всегда "хочет" гибрид) -- ВСЕГДА
    молчаливая деградация в fulltext-only, никогда ошибка -- обратная совместимость
    с M2 (find_entrypoint у агента не должен был начать падать только потому, что
    воркспейс ещё не проиндексирован эмбеддером).

`mode_used` живёт ТОЛЬКО на верхнем уровне возвращаемого dict (не дублируется в
каждый item search_code's "items"): в отличие от per-item полей (score и т.п.),
это факт ОБ ЭТОМ ЗАПРОСЕ целиком, один на весь ответ -- top-level поле остаётся
осмысленным даже когда "items"/"results" пуст (0 совпадений), чего per-item поле
дать не может."""

from __future__ import annotations

from collections.abc import Sequence

from codegraph.embedding.base import Embedder
from codegraph.stores.graph import GraphStore

_SEARCH_MODES = frozenset({"hybrid", "vector", "text"})
_SNIPPET_MAX_CHARS = 600
_META_NODE_ID = "meta"  # singleton Meta-узел, см. pipeline/load.py

# find_entrypoint: kinds-фильтр применяется ПОСЛЕ RRF-фьюжна (см. find_entrypoint's
# докстринг) -- Sym-fulltext ranking и chunk-vector-ranking запрашиваются с запасом
# (k * этот множитель) вместо голого k, чтобы после фьюжна+фильтра по kind осталось
# достаточно кандидатов для честных k результатов; тот же приём (over-fetch, обрезать
# в Python), что store.search_vector_chunks использует для СВОЕГО service-фильтра.
_ENTRYPOINT_POOL_FACTOR = 4

# M12 T1 (pilot §4.1: sibling chunks of ONE class crowd the method-chunk target out
# of top-k -- DocumentStorageClient's top-1..8 were ALL sibling chunks of the SAME
# class). search_code's top-k now means k DISTINCT SYMBOLS (see
# _aggregate_by_symbol below), not k raw chunks -- a literal k-sized fetch per leg
# often can't even PRODUCE k distinct symbols after aggregation: an oversized class
# contributing more than k sibling chunks (all sharing one symbol_id, differing only
# by ##cN in chunk_id -- chunking/splitter.py rule 3) can fill a k-sized fetch
# ENTIRELY with itself, and a method chunk ranked just past that cutoff never enters
# the candidate pool at all, regardless of how well aggregation collapses what DID
# get fetched. Over-fetch (k * this factor) per leg before fusion -- same
# over-fetch-then-trim-in-Python trick find_entrypoint already uses for its own
# post-fusion kind-filter (_ENTRYPOINT_POOL_FACTOR above); reusing that SAME factor
# (4) here rather than tuning a second constant -- hybrid mode already unions TWO
# separate k*4 per-leg pools (up to 8k distinct candidates pre-aggregation,
# comfortably wider than find_entrypoint's own single-leg 4x pool), and text-only/
# vector-only mode (a single leg, no second list to widen the union) is exactly
# where the extra headroom matters most.
_SEARCH_POOL_FACTOR = 4


def rrf(rankings: Sequence[Sequence[str]], k: int = 60) -> list[tuple[str, float]]:
    """Reciprocal Rank Fusion: score(id) = Σ 1/(k + rank_i + 1) по всем `rankings`,
    где rank_i -- 0-based позиция id в i-м ranking'е (id, отсутствующий в ranking'е,
    не вносит вклад от него вовсе -- не 0-очков-но-присутствует, а просто НЕ
    участвует в сумме для этого ranking'а). Каждый ranking -- список id, отсортированный
    от лучшего к худшему (сам rrf не проверяет и не пересортировывает вход).

    Результат отсортирован по score DESC; равный score -- tie-break по id ASC
    (лексикографически) для полного детерминизма (без tie-break порядок двух id с
    одинаковым score был бы обусловлен лишь порядком обхода dict, который для строк
    в CPython НЕ гарантирован стабильным между процессами/версиями -- см. PYTHONHASHSEED).

    Пустой `rankings` (или список из одних пустых ranking'ов) -> `[]`."""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, item_id in enumerate(ranking):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores.items(), key=lambda pair: (-pair[1], pair[0]))


def _meta_embed_model(store: GraphStore) -> str | None:
    """Meta.embed_model с единственного Meta-узла (id="meta", см. pipeline/load.py) --
    None, если узла нет вовсе (граф ни разу не проходил через M3 load_graph) или узел
    есть, но embed_model не проставлен (граф ни разу не эмбеден -- pipeline/load.py
    опускает это поле целиком, а не пишет null, см. _omit_none)."""
    nodes = store.get_nodes([_META_NODE_ID])
    return nodes[0].get("embed_model") if nodes else None


def _vector_unusable_reason(store: GraphStore, embedder: Embedder | None) -> str | None:
    """None -- вектор доступен и его можно использовать прямо сейчас; иначе -- готовая
    (actionable) причина отказа, ОДНА из двух: нет embedder'а вовсе, либо тот embedder
    эмбедит ДРУГОЙ моделью, чем граф (Meta.embed_model). Оба случая (в т.ч. граф вообще
    ни разу не эмбеден -- Meta.embed_model отсутствует) естественно попадают под
    "не совпадает с embedder.model_id" -- ЛЮБОЕ реальное model_id (непустая строка)
    != None."""
    if embedder is None:
        return "no embedder available for vector search"
    meta_model = _meta_embed_model(store)
    if meta_model != embedder.model_id:
        return (
            f"embed model mismatch: reindex needed (graph was indexed with "
            f"embed_model={meta_model!r}, current embedder is {embedder.model_id!r})"
        )
    return None


def _snippet(text: str) -> str:
    return text if len(text) <= _SNIPPET_MAX_CHARS else text[:_SNIPPET_MAX_CHARS]


def _chunk_item(props: dict, score: float) -> dict:
    return {
        "chunk_id": props.get("id"),
        "symbol_id": props.get("symbol_id"),
        # Денормализовано на Chunk-узел при load (pipeline/load.py _chunk_props:
        # symbol_id -> qualified_name join живёт ТАМ, не здесь -- search_code горячий
        # путь, per-call get_nodes-backfill был бы лишним round trip'ом на каждый
        # запрос; см. _chunk_props' докстринг про выбор). None -- символ чанка не был
        # в staged nodes (защитный edge case) либо граф загружен pre-T7 load'ом,
        # ещё не материализовавшим это поле.
        "qualified_name": props.get("qualified_name"),
        # M10 T3 (pilot §4.1: "right class in top-1, wrong sub-chunk" -- big classes
        # split into N chunks, method-level pieces already carry the METHOD's own
        # qualified_name via chunking/splitter.py rule 3, but nothing on the item
        # said so explicitly). enclosing_symbol is the SAME value as qualified_name
        # above -- both ARE "the owning node's qualified_name", already resolved and
        # denormalized onto this Chunk node at load time (no second store lookup, no
        # N+1 -- see pipeline/load._chunk_props' own "kinds" docstring section). Kept
        # as its own additive key (not a rename of qualified_name, which stays for
        # back-compat) so the response is self-describing without the agent having to
        # infer "this is the chunk's owning symbol" from an ambiguously-named field.
        "enclosing_symbol": props.get("qualified_name"),
        # M10 T3: the owning node's OWN kind ("Module"/"Class"/"Function" -- see
        # core/schema.py NODE_KINDS; deliberately the SAME capitalization already
        # used everywhere else a node's kind is surfaced, e.g. find_entrypoint's
        # results -- not a second, lowercase convention for the identical concept),
        # denormalized onto the Chunk node at load time exactly like qualified_name
        # above (pipeline/load._chunk_props' "kinds" join map). Paired with
        # qualified_name/enclosing_symbol this is what actually lets an agent tell a
        # method-level chunk (chunk_kind="Function", qualified_name is
        # "ClassName.method_name") apart from class-level content (chunk_kind=
        # "Class": either a whole small class, or a header/gap piece of an oversized
        # one -- both share the bare class qualified_name, see splitter.py rule 3) --
        # qualified_name's dotted-vs-bare shape alone left that ambiguous. None for
        # the same defensive/pre-T3-graph reasons as qualified_name.
        "chunk_kind": props.get("chunk_kind"),
        "service": props.get("service"),
        "relpath": props.get("relpath"),
        "start_line": props.get("start_line"),
        "end_line": props.get("end_line"),
        "snippet": _snippet(props.get("text", "")),
        "score": score,
    }


def _aggregate_by_symbol(
    fused: list[tuple[str, float]], props_by_id: dict[str, dict],
) -> list[tuple[str, float, int]]:
    """M12 T1 (pilot §4.1): groups the (already post-RRF) `fused` candidate pool by
    symbol_id -- each DISTINCT symbol's representative is its best-ranked chunk
    (`fused` is already best-first, so "first occurrence of a symbol_id while
    walking `fused`" IS "that symbol's best-ranked chunk" -- no separate max/sort
    step needed). Returns one `(representative_chunk_id, representative_score,
    sibling_chunks)` tuple per distinct symbol, in the SAME best-first order as
    `fused` (i.e. ordered by each group's own representative's rank -- exactly what
    walking `fused` and skipping every chunk whose symbol was already seen gives
    you). `sibling_chunks` -- count of OTHER chunk ids anywhere in the WHOLE `fused`
    pool that share this symbol_id (0 when the symbol had only one chunk in the
    pool); deliberately counted over the FULL pool, not just the chunks ranked ahead
    of the representative -- it answers "how many other chunks of this symbol did
    the search see at all" (an agent-facing fact about the match), not "how many
    were beaten" (an internal ranking detail).

    A chunk whose symbol_id can't be resolved (missing from `props_by_id` --
    defensive edge case, see `_search_code_result`'s own None-guard below -- or a
    pre-M10-T3 graph that never denormalized `symbol_id` onto the Chunk node at
    all) falls back to grouping by its OWN chunk_id: it becomes its own singleton
    group rather than being merged with every OTHER symbol_id-less chunk into one
    false mega-group. This is what makes "all-unique symbols" (in practice: every
    graph before this field existed, and still the common case after -- a class
    small enough to fit in one chunk, chunking/splitter.py rule 2) a byte-identical
    no-op: with no shared symbol_id anywhere in the pool, every chunk is already its
    own group, in the exact order `fused` already had them."""

    def group_key(chunk_id: str) -> str:
        props = props_by_id.get(chunk_id)
        symbol_id = props.get("symbol_id") if props else None
        return symbol_id if symbol_id else chunk_id

    sibling_counts: dict[str, int] = {}
    for chunk_id, _score in fused:
        key = group_key(chunk_id)
        sibling_counts[key] = sibling_counts.get(key, 0) + 1

    groups: list[tuple[str, float, int]] = []
    seen: set[str] = set()
    for chunk_id, score in fused:
        key = group_key(chunk_id)
        if key in seen:
            continue
        seen.add(key)
        groups.append((chunk_id, score, sibling_counts[key] - 1))
    return groups


def _search_code_result(
    fused: list[tuple[str, float]], props_by_id: dict[str, dict], k: int, mode_used: str,
) -> dict:
    """k distinct SYMBOLS (M12 T1), not k raw chunks -- see _aggregate_by_symbol.
    `fused` is expected to already be the OVER-FETCHED (pool-sized, not bare-k)
    candidate pool -- see _SEARCH_POOL_FACTOR -- so aggregation has real headroom to
    find k distinct symbols instead of just deduping whatever k raw chunks happened
    to be fetched."""
    items = []
    for chunk_id, score, sibling_chunks in _aggregate_by_symbol(fused, props_by_id)[:k]:
        props = props_by_id.get(chunk_id)
        if props is None:
            continue  # defensive: каждый fused id обязан прийти из props_by_id ниже
        item = _chunk_item(props, score)
        item["sibling_chunks"] = sibling_chunks
        items.append(item)
    return {"items": items, "mode_used": mode_used}


def _text_only_search_code(
    store: GraphStore, query: str, k: int, service: str | None,
) -> dict:
    pool = k * _SEARCH_POOL_FACTOR  # M12 T1 -- see _SEARCH_POOL_FACTOR's own docstring
    text_hits = store.search_text_chunks(query, pool, service)
    ranking = [props["id"] for props, _score in text_hits]
    props_by_id = {props["id"]: props for props, _score in text_hits}
    fused = rrf([ranking])
    return _search_code_result(fused, props_by_id, k, "text")


def search_code(
    store: GraphStore,
    embedder: Embedder | None,
    query: str,
    k: int = 8,
    service: str | None = None,
    mode: str = "hybrid",
    exact: bool = False,
) -> dict:
    """text -- всегда доступна (fulltext по Chunk(text, context_header), sanitize --
    см. store.search_text_chunks). vector/hybrid нуждаются в usable embedder (см.
    _vector_unusable_reason); mode="vector" без него -- error dict, mode="hybrid" --
    молчаливая деградация в text-only (mode_used="text"). "score" в каждом item --
    ВСЕГДА fused RRF-score (даже для чистых mode="text"/"vector" -- однократная RRF
    над ОДНИМ ranking'ом не меняет порядок, лишь даёт единый, всегда-desc-лучше
    масштаб очков независимо от режима; сырые оценки text (RediSearch relevance,
    больше -- лучше) и vector (cosine-дистанция, МЕНЬШЕ -- лучше, см.
    store.search_vector_chunks) иначе несовместимы по знаку/масштабу между режимами).

    M12 T1 (pilot §4.1): `k` -- k DISTINCT SYMBOLS, not k raw chunks. Post-RRF-fusion,
    candidates are aggregated by symbol_id (see _aggregate_by_symbol) -- each symbol
    is represented by its single best-ranked chunk, so several sibling chunks of ONE
    oversized class (chunking/splitter.py rule 3) collapse to ONE position instead of
    crowding out other, genuinely different, symbols. Each item additively carries
    `sibling_chunks: int` -- how many OTHER chunks of that SAME symbol were in the
    (over-fetched) candidate pool, 0 when the symbol was unique there. This one
    aggregation point sits inside `_search_code_result`, downstream of ALL three
    modes' own fusion (text/vector/hybrid all funnel through it) -- so the k-distinct-
    symbols contract holds identically regardless of `mode`. Getting k distinct
    symbols back needs more raw candidates than a literal k-sized fetch would ever
    provide (an oversized class alone can fill a k-sized fetch), so each per-leg
    store call now asks for `k * _SEARCH_POOL_FACTOR` chunks, not k -- see that
    constant's own docstring for the exact number and why it's shared with
    find_entrypoint's pool. Graphs where every symbol maps to exactly one chunk (no
    class was ever big enough to split, chunking/splitter.py rule 2 -- e.g. every
    fixture in this repo's own test suite) see byte-identical output to before this
    aggregation existed, just with `sibling_chunks=0` added to every item.

    `exact` (M5 T2, pilot Bug A -- no-op unless mode is "vector"/"hybrid" AND a
    vector search actually runs): routes the vector leg through
    `store.search_vector_chunks_exact` (deterministic full Cypher scan, no ANN index)
    instead of `store.search_vector_chunks` (ANN, unseeded HNSW rebuild per graph
    load -- ranking not reproducible across identical runs, see that method's own
    docstring). RRF fusion itself is unaffected either way -- `rrf()` only ever sees
    a plain ranking of ids, never the raw store scores (see `vector_ranking` below);
    only WHICH store method produced that ranking changes. Not exposed via the
    search_code MCP tool (mcp/server.py's own hand-written wrapper omits it on
    purpose) -- production/agent-facing search stays ANN-only; `--exact` is a
    `codegraph eval retrieval` CLI-only knob for deterministic CI hit@k (cli.py).

    Результат: `{"items": [...], "mode_used": "text"|"vector"|"hybrid"}` при успехе,
    `{"error": "..."}` при невалидном mode или недоступном векторе для mode="vector"."""
    if mode not in _SEARCH_MODES:
        return {"error": f"invalid search mode: {mode!r} (expected one of {sorted(_SEARCH_MODES)})"}

    if mode == "text":
        return _text_only_search_code(store, query, k, service)

    reason = _vector_unusable_reason(store, embedder)
    if reason is not None:
        if mode == "vector":
            return {"error": reason}
        return _text_only_search_code(store, query, k, service)  # hybrid: silent degrade

    # reason is None только когда embedder не None (см. _vector_unusable_reason) --
    # можно безопасно звать embedder.embed_query ниже. exact selects WHICH store
    # method runs the vector leg -- see this function's own docstring. pool (not a
    # bare k) -- M12 T1, see _SEARCH_POOL_FACTOR's own docstring.
    vector_search = store.search_vector_chunks_exact if exact else store.search_vector_chunks
    pool = k * _SEARCH_POOL_FACTOR
    vector_hits = vector_search(embedder.embed_query(query), pool, service)
    vector_ranking = [props["id"] for props, _score in vector_hits]
    props_by_id = {props["id"]: props for props, _score in vector_hits}

    if mode == "vector":
        fused = rrf([vector_ranking])
        return _search_code_result(fused, props_by_id, k, "vector")

    # hybrid: тоже собираем text ranking (тем же pool) и фьюзим оба
    text_hits = store.search_text_chunks(query, pool, service)
    text_ranking = [props["id"] for props, _score in text_hits]
    for props, _score in text_hits:
        props_by_id.setdefault(props["id"], props)
    fused = rrf([text_ranking, vector_ranking])
    return _search_code_result(fused, props_by_id, k, "hybrid")


def find_entrypoint(
    store: GraphStore,
    embedder: Embedder | None,
    query: str,
    k: int = 5,
    kinds: Sequence[str] | None = None,
) -> dict:
    """v2 (M3 T7) -- RRF(Sym-fulltext ranking, chunk-vector ranking агрегированный до
    symbol_id) вместо M2's чистого fulltext. kinds-фильтр применяется ПОСЛЕ фьюжна
    (на объединённом кандидат-пуле, resolved node props), а НЕ отдельно на каждом
    ranking'е ДО фьюжна -- т.к. chunk-vector ranking в принципе не может быть
    Cypher-отфильтрован по kind (kind -- свойство СИМВОЛА, а не чанка). Sym-fulltext
    ranking ВСЁ РАВНО получает kinds сразу в сам store.search_fulltext-вызов (не
    только пост-фильтром) -- избыточно-но-безопасно: это (a) дешевле (меньше
    кандидатов гоняется через фьюжн), (b) сохраняет M2-тесты (find_entrypoint's
    fake-store юниты, см. tests/unit/test_query_api.py) проходящими БЕЗ изменений --
    они пином проверяют, что kinds передаётся в САМ fulltext-вызов; итоговый набор
    результатов идентичен что с (a+post-filter), что с (только post-filter), т.к.
    post-filter идемпотентен над уже-отфильтрованными кандидатами.

    Деградация (нет embedder / Meta-мисматч / нет векторного индекса -- все три
    случая сводятся к _vector_unusable_reason) -- ВСЕГДА молчаливая (mode_used=
    "text"), никогда error dict: обратная совместимость с M2 (find_entrypoint не
    имел параметра mode и никогда не отказывал только из-за отсутствия эмбеддинга).

    Результат: `{"results": [...node props + "score"...], "mode_used": "text"|"hybrid"}`
    -- те же поля, что M2's `{"results": [...]}`, плюс "mode_used" (обратная
    совместимость по полям: старые потребители, игнорирующие незнакомые ключи,
    не ломаются)."""
    reason = _vector_unusable_reason(store, embedder)
    if reason is not None:
        sym_hits = store.search_fulltext(query, k, kinds=kinds)
        return {"results": sym_hits, "mode_used": "text"}

    # reason is None только когда embedder не None (см. _vector_unusable_reason).
    pool = k * _ENTRYPOINT_POOL_FACTOR
    sym_hits = store.search_fulltext(query, pool, kinds=kinds)
    text_ranking = [h["id"] for h in sym_hits]
    props_by_id = {h["id"]: {kk: vv for kk, vv in h.items() if kk != "score"} for h in sym_hits}

    chunk_hits = store.search_vector_chunks(embedder.embed_query(query), pool, service=None)
    vector_ranking: list[str] = []
    seen_symbols: set[str] = set()
    for props, _score in chunk_hits:  # best-first (ascending cosine distance)
        sym_id = props.get("symbol_id")
        if not sym_id or sym_id in seen_symbols:
            continue  # max-по-symbol: держим только ПЕРВОЕ (= лучшее) вхождение символа
        seen_symbols.add(sym_id)
        vector_ranking.append(sym_id)

    fused = rrf([text_ranking, vector_ranking])

    missing_ids = [item_id for item_id, _score in fused if item_id not in props_by_id]
    if missing_ids:
        for node in store.get_nodes(missing_ids):
            props_by_id[node["id"]] = node

    results = []
    for item_id, score in fused:
        props = props_by_id.get(item_id)
        if props is None:
            continue  # vector ranking указал на id, который больше не резолвится (stale)
        if kinds and props.get("kind") not in kinds:
            continue
        results.append({**props, "score": score})
        if len(results) >= k:
            break

    return {"results": results, "mode_used": "hybrid"}
