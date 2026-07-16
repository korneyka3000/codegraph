"""S9 load: staging (SQLite) -> FalkorDB, blue/green.

Композиция store_factory (закреплена живым тестом Task 3, tests/integration/
test_falkordb_store.py): build_store = store_factory(f"{graph_name}__build") --
это store, В КОТОРЫЙ мы пишем узлы/рёбра; final_store = store_factory(graph_name) --
отдельный store с ЦЕЛЕВЫМ именем, и именно НА НЁМ вызывается final_store.swap_in(
build_name), потому что FalkorStore.swap_in(build_name) переименовывает build_name
в self.graph_name (см. store.py: `RENAME build_name self.graph_name`) -- self
здесь обязан УЖЕ быть final-именем, иначе получим RENAME в неверную сторону.

Labels staging не хранит как готовый набор для serving-графа -- реконструируем по
(kind, roles) (Staging.iter_nodes() отдаёт оба, roles восстановлены из labels-json,
см. staging.py): {Module,Class,Function} (кодовые) -> ("Sym", kind, *roles) --
roles добавляют multi-label поверх kind (M2, см. core/schema.py ROLE_KINDS);
Service -> ("Service",); Channel -> ("Channel",); BusinessProcess ->
("BusinessProcess",) -- эти три игнорируют roles (см. _labels_for_kind). Ребро ->
группировка по type (единственный дискриминатор, который есть у EdgeRec и который
batch.py принимает как edge_type).

known_ids собирается ПОКА обходим все узлы (один проход iter_nodes(), до единой
записи ребра) -- это обязательное условие корректности endpoint-policy рёбер:
`batch.upsert_edges` дропает ребро, если src/dst нет в known_ids, а known_ids
должен отражать ПОЛНЫЙ набор узлов графа, не только уже записанную лейбл-группу
(иначе ребро между двумя ещё не сгруппированными узлами дропалось бы ложно).

Свойства узлов/рёбер: None-значения ВЫРЕЗАНЫ из props целиком, не переданы как
null. Живой пробой (см. отчёт m1b-task-5) подтверждено: FalkorDB `SET n += {k:
null}` для НИКОГДА не существовавшего свойства -- no-op (ключ не появляется);
для УЖЕ существующего -- СТИРАЕТ его (открытая семантика Cypher `+=`). Обе ветки
безопасны сами по себе, но проще и однозначнее просто не посылать null. Списки
строк (decorators) и bool (is_async) отправляются как есть -- живой пробой
подтверждено, что FalkorDB хранит и возвращает python list/bool без искажений
через UNWIND/SET += (json-string fallback из плана не понадобился).

Crash-recovery: build-граф сбрасывается (delete_graph) ПЕРВЫМ действием каждого
прогона. Успешный прогон и так потребляет build-ключ через RENAME, но прогон,
упавший ПОСЛЕ частичной записи и ДО swap_in, оставляет ключ жить -- без сброса
этот мусор протёк бы в финальный граф при следующем успешном прогоне (живьём
воспроизведено в первичном ревью T5; регрессия -- test_load_graph_resets_stale_
build_graph_from_crashed_run). Заодно сброс снимает и след-риск уровня свойств:
None-omission (выше) не стирает устаревшее значение на переиспользуемом узле,
но переиспользуемых узлов теперь не бывает -- build всегда стартует пустым.

M3 T6: Chunk nodes (label ("Chunk",)) + a singleton Meta node (label ("Meta",),
id "meta") -- a SEPARATE code path from the NodeRec-based loop above, since
`ChunkRow` (staging's `chunks` table, via `staging.iter_chunks()`) isn't a `NodeRec`
at all (no `kind`, no `roles` -- see `stores/staging.py`'s own `ChunkRow` docstring
for why it lives outside the `NodeRec` universe). Two consequences that follow
directly from that separation, both deliberate (M3 scope, per the T6 brief):

  - Chunk ids are NEVER added to `known_ids` -- `staging.iter_chunks()` is never
    consulted while building `known_ids` above, only `staging.iter_nodes()` is. M3
    creates no edges to/from Chunk nodes at all (retrieval hits join back to the graph
    via each chunk's own `symbol_id` PROPERTY, not a graph edge) -- so this omission is
    exactly the desired behavior, not an oversight: a (hypothetical) edge row naming a
    chunk_id as src/dst would be correctly dropped by `batch.upsert_edges`' own
    known_ids prefilter, same as any other unknown endpoint.
  - Chunk nodes are written via TWO separate `upsert_nodes` calls (`_chunk_node_
    batches`, below) -- one for rows that HAVE a real embedding (`vector_props=
    ("embedding",)`, the `vecf32(r.embedding)` path) and one for rows that don't
    (embedder was skipped this run, or a chunk simply hasn't been embedded yet). NOT
    because `vecf32(NULL)` errors -- live-verified against FalkorDB v4.18.11, it
    doesn't: `SET n.<p> = vecf32(NULL)` raises nothing and simply never sets the
    property (`<p>` is absent from the node afterwards, same as if that SET clause
    had never run -- see `stores/falkordb/store.py`'s `search_vector_chunks` for the
    same "missing index/data degrades to an empty/no-op result, not an exception"
    FalkorDB behavior elsewhere in this codebase). The real reason for the split is
    row SHAPE, not error avoidance: a row either carries a genuinely usable embedding
    (a real `list[float]` at `row["embedding"]`) or it has no `"embedding"` key at
    all -- keeping "key absent" and "key present but NULL" from ever meaning the same
    thing here -- and keeps `batch.upsert_nodes` itself completely unaware of "some
    rows have this prop, some don't". The other half of the reason: a row without a
    usable embedding also needs `embed_model` stripped from its regular props (see
    `_chunk_node_batches` below -- a chunk that carries no embedding must not claim
    one was computed for it either), which is naturally done in the same per-row
    branch that decides which of the two calls a row belongs to.

Meta.embed_model/dim are read from `staging.get_meta("embed_model"/"embed_dim")` --
written by `pipeline.chunk_embed.run` the last time an embedder was actually used (see
`_embed_meta`'s own docstring for why staging's own meta table, not a parameter
threaded in from the caller, is the right source of truth here) -- and are OMITTED
(not sent as null) when absent, same `_omit_none` convention as every other node's
props. Meta.schema_version is always present (`core.schema.SCHEMA_VERSION`, a plain
constant, never absent) -- the Meta node itself is ALWAYS written, even for a graph
with zero chunks/no embedder ever run (simpler contract than a sometimes-present node:
every loaded graph has exactly one Meta node, full stop).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable

from codegraph.core.errors import InvariantError
from codegraph.core.schema import SCHEMA_VERSION, EdgeRec, NodeRec
from codegraph.embedding.codec import unpack_vector
from codegraph.stores.graph import GraphStore
from codegraph.stores.staging import ChunkRow, Staging

logger = logging.getLogger(__name__)

_CODE_KINDS = frozenset({"Module", "Class", "Function"})

_NODE_CORE_FIELDS = (
    "id", "kind", "service", "name", "qualified_name",
    "relpath", "start_line", "end_line", "start_byte", "end_byte", "content_hash",
)
_EDGE_CORE_FIELDS = ("resolution", "confidence", "extractor", "evidence_file", "evidence_line")


def _labels_for_kind(kind: str, roles: tuple[str, ...] = ()) -> tuple[str, ...]:
    """Кодовые kinds (Module/Class/Function) -> ("Sym", kind, *roles) -- roles
    добавляют доп. label'ы поверх kind (multi-label, см. core/schema.py ROLE_KINDS).
    Service/Channel/BusinessProcess -- фиксированный однословный label, roles
    игнорируются (роли осмысленны только для кодовых узлов)."""
    if kind in _CODE_KINDS:
        return ("Sym", kind, *roles)
    if kind == "Service":
        return ("Service",)
    if kind == "Channel":
        return ("Channel",)
    if kind == "BusinessProcess":
        return ("BusinessProcess",)
    raise InvariantError(f"unknown node kind for graph load: {kind!r}")


def _omit_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _node_props(n: NodeRec) -> dict:
    core = {field: getattr(n, field) for field in _NODE_CORE_FIELDS}
    props = _omit_none({**core, **n.props})
    # M2 T8: roles live as graph LABELS via _labels_for_kind (multi-label, e.g.
    # :Sym:Function:RouteHandler) -- but store.get_nodes()/neighbors() only ever
    # return n.properties (see stores/falkordb/store.py: `RETURN n`/`RETURN e, m`
    # decode to .properties, never labels(n)), so query/traverse.py (which walks
    # role-gated transitions -- MessageConsumer/RouteHandler/TemporalWorkflow) has
    # no way to see a node's roles from a plain node dict without this explicit
    # mirror. Omitted entirely (not even []) when a node carries no roles, same
    # spirit as _omit_none: don't store a not-applicable field on kinds that never
    # have roles (Channel/BusinessProcess/Service) or role-less code nodes.
    if n.roles:
        props["roles"] = list(n.roles)
    return props


def _edge_props(e: EdgeRec) -> dict:
    core = {field: getattr(e, field) for field in _EDGE_CORE_FIELDS}
    return _omit_none({**core, **e.props})


# M3 T1: key_props per edge type -- passed through to store.upsert_edges (batch.py)
# so its MERGE pattern includes these props in the relationship key, not just (src,dst).
# NEXT_SEGMENT is the only type that currently needs one: linking/segments.py can derive
# TWO NEXT_SEGMENT edges between the SAME (src,dst) pair when a producer reaches the same
# downstream node via two DIFFERENT channels (see core/schema.py's SCHEMA_VERSION
# "2 -> 3" history comment for the staging-side half of this same fix) -- without
# via_channel_id in the graph MERGE key too, the second edge would silently overwrite the
# first in FalkorDB even after staging correctly kept both rows. Every other edge type is
# untouched (empty tuple, i.e. today's (src,dst)-only MERGE key, unchanged).
_KEY_PROPS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "NEXT_SEGMENT": ("via_channel_id",),
}


def _key_props_for(edge_type: str) -> tuple[str, ...]:
    return _KEY_PROPS_BY_TYPE.get(edge_type, ())


def _edge_row(e: EdgeRec) -> dict:
    """Row dict for store.upsert_edges: src/dst always, plus -- for a type listed in
    _KEY_PROPS_BY_TYPE -- that type's key_props promoted to the row's TOP level (read
    from e.props, defaulting absence to '' the same way staging.upsert_edges normalizes
    the via_channel PK column, so a row's key_props value is always present, never
    sometimes-missing). Top level, not just inside props, because batch.upsert_edges'
    MERGE pattern reads key_props as `r.<k>` -- a plain dict key lookup, not a nested
    r.props.<k> (see its own docstring). The value ALSO stays inside props (_edge_props
    already puts it there via **e.props) completely unchanged -- `SET e += r.props` is
    what actually persists it as a real, queryable graph edge property (e.g.
    query/traverse.py's `_resolve_exits` reads via_channel_id off the edge's own
    properties); the top-level copy exists ONLY to feed the MERGE key and is otherwise
    redundant with what's already in props."""
    row: dict = {"src": e.src, "dst": e.dst, "props": _edge_props(e)}
    for key in _key_props_for(e.type):
        row[key] = e.props.get(key, "")
    return row


# -- M3 T6: Chunk nodes + Meta node (see module docstring for why this is a separate
# code path from the NodeRec-based nodes loop above) --

_CHUNK_LABELS = ("Chunk",)
_META_LABELS = ("Meta",)

_CHUNK_PROP_FIELDS = (
    "symbol_id", "service", "relpath", "ord", "start_line", "end_line",
    "content_hash", "text", "context_header", "embed_model",
)


def _chunk_props(row: ChunkRow, qualified_names: dict[str, str] | None = None) -> dict:
    """`ChunkRow` -> Chunk node props: the T6 brief's own field list ({id, symbol_id,
    service, relpath, ord, start_line, end_line, content_hash, text, context_header,
    embed_model}) PLUS an explicit `kind: "Chunk"` -- every OTHER node kind in this
    graph carries `kind` as a plain property, not just as a graph label, because
    `FalkorStore.get_nodes_by_kind`/`stats()` (`MATCH (n) RETURN n.kind, count(n)`,
    grouped in cli.py's `stats` command) read it as a property (Cypher labels can't be
    parameterized, see `get_nodes_by_kind`'s own docstring) -- a Chunk/Meta node
    without `kind` would group into a `None` bucket there, breaking `stats()`'s
    `sorted()` call the moment a graph has both a kind-bearing node and a Chunk/Meta
    one (i.e. every real workspace, since Meta is always written). Deliberately
    excludes `embedding` (travels via a SEPARATE top-level row field for the
    vector_props/vecf32 path, never inside props -- see `_chunk_node_batches` and
    `batch.upsert_nodes`' own docstring) and `embedded_hash` (a staging-only
    bookkeeping column, never graph-visible). None-valued fields are omitted, same
    `_omit_none` convention as `_node_props` -- concretely `context_header`/
    `embed_model` for a chunk that hasn't been through `fill_headers_all`/an embed
    pass (respectively) yet.

    `qualified_names` (M3 T7 review fix -- search_code items owe the brief's own
    `qualified_name?` field): a {node_id: qualified_name} join map, built by
    `load_graph` during the SAME `staging.iter_nodes()` pass that already collects
    `known_ids` (zero extra staging I/O -- NOT read via `_CHUNK_PROP_FIELDS`/getattr,
    since qualified_name isn't a ChunkRow column at all: it's the owning SYMBOL's
    property, joined in via `row.symbol_id`). Denormalizing onto the Chunk node AT
    LOAD TIME -- rather than back-filling per-search via `get_nodes` over the result
    set (the `retrieval.find_entrypoint` pattern) -- is deliberate: qualified_name is
    index-time-static (same lifecycle as the already-denormalized `service`/`relpath`
    on this exact node), `search_code` is the agent's primary hot path (a once-per-
    index join beats a per-call graph round trip forever), and blue/green full-rebuild
    semantics mean the very next `codegraph index`/`codegraph load` run materializes
    the field -- no migration concern. A `symbol_id` with no staged node (defensive:
    `chunk_embed` only derives symbol_ids from already-staged defs, but staging.db
    outlives any single run) simply gets no `qualified_name` property (`_omit_none`),
    which `query.retrieval._chunk_item` reports as `qualified_name: None`."""
    qualified = (qualified_names or {}).get(row.symbol_id)
    core = {
        "id": row.chunk_id, "kind": "Chunk", "qualified_name": qualified,
        **{f: getattr(row, f) for f in _CHUNK_PROP_FIELDS},
    }
    return _omit_none(core)


def _chunk_node_batches(
    staging: Staging,
    dim: int | None = None,
    qualified_names: dict[str, str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Every staged chunk (`staging.iter_chunks()`, ALL services -- never `staging.
    iter_nodes()`, a completely separate table/id-space, see module docstring), split
    into two row lists: chunks WITH a real, USABLE embedding (get the `vector_props=
    ("embedding",)` treatment) and chunks WITHOUT one (embedder skipped this run, not
    yet embedded for some other reason, or -- see below -- a dimension mismatch) --
    see module docstring for why this split into two upsert_nodes calls, rather than
    one, is the design choice here: NOT `vecf32(NULL)` error avoidance (it doesn't
    error, see module docstring), but keeping row shape unambiguous (key absent vs.
    key present-but-NULL) and giving "no usable embedding" rows one place to also
    drop `embed_model` from their props.

    `dim` is the SAME dimension `ensure_schema` just sized the vector index to (from
    `_embed_meta`), and it gates embedded rows in BOTH directions:

    - `dim is None` (this run had NO working embedder -- degradation path/`--no-embed`
      -- so `ensure_schema` created NO vector index and Meta carries NO embed_model):
      EVERY row goes to `without_vector`, even one whose staging row still holds an
      embedding blob. Such stale blobs genuinely exist on this path (reviewer-
      reproduced, M3 T6 coordinator fix): a PRIOR run embedded the workspace, then a
      LATER run against the SAME staging.db degrades to embedder=None --
      `chunk_embed.run` correctly clears the workspace embed_model/dim meta, but
      `upsert_chunks`' ON-CONFLICT contract deliberately preserves each unchanged
      chunk's embedding column (that preservation is what lets a THIRD run, embedder
      restored, reuse them all -- embedded==0). The pre-fix guard (`dim is not None
      and len != dim`) waved those stale blobs straight into the vecf32 batch,
      producing Chunk nodes carrying embedding+embed_model while Meta advertises no
      model and no vector index exists -- an inconsistent graph. Stale-skipped rows
      also get `embed_model` dropped from their props (a Chunk advertising a model
      whose embedding it doesn't carry is the same inconsistency at property
      granularity); ONE summary warning is logged, not one per chunk.
    - `dim` given: a row whose DECODED vector length differs is routed to
      `without_vector` (per-row warning naming the actual cause, plus the same
      embed_model drop), rather than being silently dropped later by
      `batch.upsert_nodes`' own per-row bisection-and-skip safety net -- which would
      also catch a real FalkorDB-side vecf32 dimension error, but only after the
      fact, with no context tying it back to "this chunk's embedding doesn't match
      the index". Not reachable via the single real production path today (every
      chunk committed by ONE `chunk_embed.run` call shares the SAME embedder, hence
      the SAME dimension) -- but `staging.db` persists across separate `codegraph
      index`/`codegraph load` invocations, and an embedding.model switch interrupted
      mid-run could leave mixed-dimension rows behind."""
    with_vector: list[dict] = []
    without_vector: list[dict] = []
    stale_skipped = 0
    for row in staging.iter_chunks():
        entry = {"id": row.chunk_id, "props": _chunk_props(row, qualified_names)}
        if row.embedding is None:
            without_vector.append(entry)
            continue
        if dim is None:
            entry["props"].pop("embed_model", None)
            stale_skipped += 1
            without_vector.append(entry)
            continue
        vector = unpack_vector(row.embedding)
        if len(vector) != dim:
            logger.warning(
                "chunk %s has a %d-dim embedding but the vector index is %d-dim "
                "(stale/mismatched staging.db?) -- loading without a vector",
                row.chunk_id, len(vector), dim,
            )
            entry["props"].pop("embed_model", None)
            without_vector.append(entry)
        else:
            entry["embedding"] = vector
            with_vector.append(entry)
    if stale_skipped:
        logger.warning(
            "%d chunk(s) carry embeddings from a prior run, but this run has no "
            "embedder (no vector index, Meta has no embed_model) -- loading them "
            "without vectors; a later run with the embedder restored will reuse the "
            "staged embeddings as-is",
            stale_skipped,
        )
    return with_vector, without_vector


def _embed_meta(staging: Staging) -> tuple[str | None, int | None]:
    """embed_model/dim last written by `pipeline.chunk_embed.run` into staging's own
    meta table (`set_meta("embed_model", ...)`/`set_meta("embed_dim", ...)`) -- read
    back here rather than threaded in as a `load_graph` parameter, because staging's
    meta table persists exactly as long as the chunks/nodes/edges tables themselves do
    (same SQLite file): `codegraph load` (a separate CLI command, reusing an on-disk
    staging.db from a PRIOR `codegraph index` run, calling `load_graph` directly with
    no `chunk_embed.run` call of its own in between) needs to see the SAME embed_model/
    dim that run last computed, not "whatever this particular load_graph caller
    happens to know about" -- staging IS the single source of truth here, same as
    every other input load_graph reads (see module's own opening paragraph).

    Empty-string sentinel (`chunk_embed.run`'s own "no embedder this run" path writes
    "" rather than leaving a stale value in place, precisely so a `--no-embed` run
    doesn't leave a PRIOR run's now-inapplicable embed_model/dim behind -- every
    `codegraph index` run re-chunks EVERY configured service from scratch, so a chunk
    with no embedding this run truly has none, not "the old one, still valid") reads
    back as None here, identically to a genuinely-absent key."""
    model = staging.get_meta("embed_model") or None
    dim_raw = staging.get_meta("embed_dim") or None
    return model, int(dim_raw) if dim_raw is not None else None


def load_graph(
    staging: Staging,
    store_factory: Callable[[str], GraphStore],
    graph_name: str,
) -> dict:
    """staging -> `<graph_name>__build` (предварительно сброшенный) -> ensure_schema ->
    upsert (nodes then chunks/meta then edges, grouped) -> swap_in в graph_name.
    Возврат -- счётчики для report.build_report."""
    embed_model, dim = _embed_meta(staging)
    build_name = f"{graph_name}__build"
    build_store = store_factory(build_name)
    # crash-recovery: снести возможный мусор от прогона, упавшего до swap_in
    # (см. модульный докстринг) -- ДО ensure_schema, чтобы схема легла на пустой граф
    build_store.delete_graph()
    build_store.ensure_schema(dim=dim)

    # -- 1. nodes: сгруппировать по labels-набору, попутно собрать known_ids +
    # qualified_names (M3 T7: symbol_id -> qualified_name join map для денормализации
    # qualified_name на Chunk-узлы, см. _chunk_props -- ТОТ ЖЕ единственный проход
    # iter_nodes(), ни одного дополнительного обращения к staging) --
    nodes_by_labels: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    known_ids: set[str] = set()
    qualified_names: dict[str, str] = {}
    for n in staging.iter_nodes():
        labels = _labels_for_kind(n.kind, n.roles)
        nodes_by_labels[labels].append({"id": n.id, "props": _node_props(n)})
        known_ids.add(n.id)
        qualified_names[n.id] = n.qualified_name

    nodes_written = 0
    nodes_written_by_label: dict[str, int] = {}
    for labels, rows in nodes_by_labels.items():
        written = build_store.upsert_nodes(labels, rows)
        nodes_written += written
        nodes_written_by_label[":".join(labels)] = written

    # -- 1b. M3 T6: Chunk nodes (staging.iter_chunks(), a SEPARATE table/id-space from
    # staging.iter_nodes() above -- chunk ids are never added to known_ids/edges, see
    # module docstring) + a singleton Meta node (ALWAYS written, even with zero chunks/
    # no embedder ever run -- see module docstring for both) --
    chunk_rows_with_vector, chunk_rows_without_vector = _chunk_node_batches(
        staging, dim=dim, qualified_names=qualified_names
    )
    # batch.upsert_nodes already no-ops (returns 0, no Cypher built) on an empty rows
    # list regardless of vector_props -- no need for an `if rows:` guard around either
    # call here, only around the resulting nodes_written/-by_label bookkeeping below.
    chunks_written = build_store.upsert_nodes(
        _CHUNK_LABELS, chunk_rows_with_vector, vector_props=("embedding",)
    ) + build_store.upsert_nodes(_CHUNK_LABELS, chunk_rows_without_vector)
    if chunks_written:
        nodes_written += chunks_written
        # Derived from the SAME labels tuple just passed to upsert_nodes (":".join,
        # identical to how the generic NodeRec loop above keys nodes_written_by_label)
        # rather than a separately-hardcoded string -- if _CHUNK_LABELS ever grows a
        # second label, this key follows automatically instead of silently going stale.
        nodes_written_by_label[":".join(_CHUNK_LABELS)] = chunks_written

    # "kind": "Meta" for the identical reason Chunk gets one -- see _chunk_props'
    # own docstring (FalkorStore.stats()/get_nodes_by_kind read `kind` as a plain
    # property, never the graph label).
    meta_props = _omit_none({
        "kind": "Meta", "embed_model": embed_model, "dim": dim,
        "schema_version": SCHEMA_VERSION,
    })
    meta_written = build_store.upsert_nodes(_META_LABELS, [{"id": "meta", "props": meta_props}])
    if meta_written:
        nodes_written += meta_written
        nodes_written_by_label[":".join(_META_LABELS)] = meta_written

    # -- 2. edges: сгруппировать по type; known_ids уже ПОЛНЫЙ (весь проход nodes
    # выше завершён до этой точки) --
    edges_by_type: dict[str, list[dict]] = defaultdict(list)
    for e in staging.iter_edges():
        edges_by_type[e.type].append(_edge_row(e))

    edges_written = 0
    edges_written_by_type: dict[str, int] = {}
    edges_dropped_by_type: dict[str, int] = {}
    for edge_type, rows in edges_by_type.items():
        written, dropped = build_store.upsert_edges(
            edge_type, rows, known_ids, key_props=_key_props_for(edge_type)
        )
        edges_written += written
        edges_written_by_type[edge_type] = written
        edges_dropped_by_type[edge_type] = dropped

    # -- 3. blue/green: final_store -- ОТДЕЛЬНЫЙ store с целевым именем; swap_in
    # вызывается НА НЁМ (см. модульный докстринг) --
    final_store = store_factory(graph_name)
    final_store.swap_in(build_name)

    return {
        "nodes_written": nodes_written,
        "nodes_written_by_label": nodes_written_by_label,
        "edges_written": edges_written,
        "edges_written_by_type": edges_written_by_type,
        "edges_dropped_missing_endpoint": sum(edges_dropped_by_type.values()),
        "edges_dropped_by_type": edges_dropped_by_type,
    }
