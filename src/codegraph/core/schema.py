"""IR узлов/рёбер и константы схемы. Единый словарь для staging, load и eval."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from codegraph.core import ids

# Schema history (staging.db on-disk layout -- see stores/staging.py's _DDL and
# _check_schema_version):
#   1 -> 2 (M2 final whole-milestone review, fix-now batch): edges.origin_service
#     replaces edges.src_service as begin_service()'s deletion key. src_service was
#     derived from an edge's OWN src prefix (_id_service(e.src)), which is None for
#     any chan:/proc:-prefixed src regardless of which service's analyze emitted the
#     edge -- HANDLES (src=chan:, fastapi_ext's own convention) and kafka CONTAINS
#     (chan:topic -> chan:event) edges therefore always had src_service=NULL, so
#     begin_service(service)'s old "WHERE src_service=?" could never delete them: they
#     silently survived every re-index, and a renamed route/topic/event left a stale
#     HANDLES/CONTAINS edge (plus its now-orphaned Channel node) poisoning S7's route
#     table on the SECOND `codegraph index` run. origin_service is an explicit "which
#     service's analyze wrote this batch" fact supplied by the CALLER of upsert_edges,
#     independent of the edge's own endpoints (see Staging.upsert_edges/begin_service
#     docstrings) -- plus a companion Staging.gc_orphan_channels() sweep for the
#     orphaned Channel node itself, run at the end of link_workspace.
#   2 -> 3 (M3 T1, mandatory M2-final-review carry-item): edges gains
#     `via_channel TEXT NOT NULL DEFAULT ''`, folded into the PRIMARY KEY --
#     PRIMARY KEY(src, dst, type, via_channel) instead of (src, dst, type).
#     linking/segments.py can legitimately derive TWO NEXT_SEGMENT edges between the
#     SAME (src, dst) pair when a producer reaches the same downstream node via two
#     DIFFERENT channels (e.g. both a Kafka event AND a direct HTTP call fan out to the
#     same handler) -- under the old 3-column PK those two edges collided on the same
#     row and the second INSERT OR REPLACE silently clobbered the first, losing one
#     via_channel_id's worth of segment topology with no error at all.
#     Staging.upsert_edges derives via_channel from `props.get("via_channel_id", "")`
#     (empty string for the overwhelming majority of edges that carry no
#     via_channel_id at all -- their PK behavior is unchanged from v2). SQLite cannot
#     ALTER a PRIMARY KEY in place, so this is a straight `CREATE TABLE IF NOT EXISTS`
#     schema swap, not a live migration: there is no data-preserving upgrade path from
#     a v2 (or earlier) staging.db. Opening one now is a LOUD failure --
#     Staging._check_schema_version_before_ddl raises InvariantError, telling the
#     caller to delete the file and re-run indexing from scratch (staging.db is a
#     disposable derived cache, never a source of truth -- see that method's own
#     docstring for why the version check must run BEFORE ensure_schema's DDL, not
#     after, to guarantee this is the error an old file actually surfaces).
#   3 -> 4 (M3 T6, cache-hardening carry from the T3 review): chunks gains
#     `embedded_hash TEXT` (see stores/staging.py's ChunkRow/chunks_missing_embedding
#     docstrings for what it's for). Unlike T3's OWN chunks-table addition (which
#     needed no bump -- `chunks` was a brand new table then, no pre-T3 v3 staging.db
#     could already have one to collide with), THIS is a real column addition to a
#     table that has existed, and been populated, since v3 -- a pre-T6 staging.db
#     genuinely lacks `embedded_hash`, and hitting any of the new
#     embedded_hash-referencing code (chunks_missing_embedding/set_embeddings) against
#     one would otherwise raise a raw sqlite3.OperationalError ("no such column"), the
#     exact bug class the 1 -> 2 version-check-ordering fix above exists to turn into
#     a loud, actionable InvariantError instead. Bumping SCHEMA_VERSION is what routes
#     it there: `_check_schema_version_before_ddl` compares the OLD file's stored
#     schema_version ("3") against this constant (now 4) and raises before
#     `ensure_schema`'s DDL -- and therefore before any embedded_hash-touching code --
#     ever runs. No data-preserving upgrade path here either (same "staging is a
#     disposable derived cache" reasoning as 2 -> 3) -- delete and re-run indexing.
#   4 -> 5 (M4 T1, persistent cross-run embedding cache): chunks gains `input_hash
#     TEXT` (NULL until chunking/augment.py's `fill_headers_all` writes it via the new
#     `Staging.set_input_hashes` -- see that function and ChunkRow's own docstring in
#     stores/staging.py) -- the EXACT embedder input's hash
#     (`sha256(augment_text(header, text))`; augment.py is the single source of truth
#     for that format, staging itself never builds the hash) -- plus a brand new table,
#     `embedding_cache(input_hash, embed_model, dim, vec, PRIMARY KEY(input_hash,
#     embed_model))`: a GLOBAL, cross-`codegraph index`-run cache. Unlike `chunks`
#     itself, `begin_service` never wipes it (see that method's own docstring's "M3 T3"
#     comment, updated for this change) -- it has no `service` column and no DELETE
#     statement anywhere ever targets it -- so a chunk whose exact embedder input
#     (header + text) is unchanged since ANY prior run reuses its vector from this
#     table at ZERO provider cost, even after its OWN `chunks` row was deleted and
#     recreated from scratch by begin_service (a full re-analyze of that service). No
#     GC is implemented for this table: staging.db is itself a disposable, one-shot
#     derived cache per workspace (same "delete and recreate, never migrate" reasoning
#     as every entry above) -- an occasionally stale/orphaned embedding_cache row costs
#     a few bytes forever, never correctness.
#
#     `embedded_hash` (the column T6 added, 3 -> 4 above) CHANGES SEMANTICS here: it
#     used to store the chunk's `content_hash` AT EMBED TIME; it now stores the
#     chunk's `input_hash` AT EMBED TIME instead (still written by `set_embeddings`,
#     still compared inside `chunks_missing_embedding` -- see that method's own
#     docstring, rewritten for this change). This is not just a rename: content_hash
#     is blind to the augmentation HEADER (graph position/imports/doc/produces-
#     consumes-calls...), so a v4 workspace where some OTHER symbol's rename/edge
#     change altered THIS chunk's header text, with this chunk's OWN source untouched,
#     silently kept serving a stale embedding forever (embedded_hash == content_hash
#     never budged -- content_hash cannot see a header-only change by construction).
#     Comparing against input_hash instead closes that hole for free, since input_hash
#     already folds the header in -- a header-only change and a text-only change are
#     now detected through the exact same single comparison.
#
#     `input_hash` is populated INDEPENDENTLY of `embedding`/`embed_model`/
#     `embedded_hash` (at header-fill time, before any embed call happens even runs) --
#     so it stays OUTSIDE the `chunks` table's existing embedding/embed_model/
#     embedded_hash CHECK constraint (see `_DDL` in stores/staging.py): a chunk can
#     legitimately have a non-NULL `input_hash` and a still-NULL `embedding` (freshly
#     chunked, headers filled, not embedded yet), which that CHECK's NULL-together
#     invariant says nothing about.
#   5 -> 6 (M5 T4, closes the M4-T7 "shared edge" residual gap for real): edges'
#     PRIMARY KEY widens ONE MORE column, to (src, dst, type, via_channel,
#     origin_service) -- and `origin_service` becomes `TEXT NOT NULL DEFAULT ''`
#     (previously nullable; NULL, meaning "no owner" -- S7/linking-derived batches,
#     see Staging.upsert_edges' own docstring -- is now represented as the empty
#     string instead, since a PRIMARY KEY column can never be NULL in SQLite). This
#     is the SAME kind of PK-widening migration as 2 -> 3 above (via_channel joining
#     the PK) -- SQLite cannot ALTER a PRIMARY KEY in place, so there is, again, no
#     data-preserving upgrade path: `CREATE TABLE IF NOT EXISTS` against an
#     already-existing v5 `edges` table is a pure no-op (it does NOT retroactively
#     widen that table's PK, no matter how the DDL string changed), so an unguarded
#     old file would silently keep the OLD, narrower PK forever -- reopening one now
#     is a LOUD failure (`Staging._check_schema_version_before_ddl` raises
#     InvariantError) exactly like every other entry in this history.
#
#     WHY: M4 T7 (see the pre-this-bump `Staging.upsert_edges` docstring, still
#     legible in git history) discovered that two DIFFERENT services can
#     legitimately assert the IDENTICAL (src, dst, type, via_channel) edge -- e.g.
#     kafka_ext.py's producer branch (THIS service sends to topic X, event Y) and a
#     DIFFERENT service's consumer branch (its own dispatch_dict registers a
#     handler for topic X, event Y) each independently derive the SAME `CONTAINS
#     chan:kafka_topic:X -> chan:event_type:Y` edge from their own service's idiom
#     config. M4 T7 fixed the resulting non-determinism (which "winner" a plain
#     `INSERT OR REPLACE` picked depended on `cfg.services` iteration order, stable
#     under a full reindex but NOT under `--incremental`) by making the FIRST
#     writer's row win, permanently, via a conflict-aware UPSERT -- but explicitly
#     documented a residual gap it deliberately left open, needing a real schema
#     change to close: if the CURRENT owner of a shared PK stops emitting that edge
#     (e.g. its producer/consumer registration is deleted from source) in a run
#     where the OTHER, still-asserting service is `--incremental`-SKIPPED, the
#     owner's own delete_file_layer/begin_service correctly clears its row -- and
#     nothing re-inserts it, because the sibling that still legitimately asserts it
#     was never reprocessed. The edge then wrongly vanishes until the sibling's next
#     non-skip run (a dump-equivalence divergence against a full reindex of the same
#     tree, which WOULD still have it).
#
#     FIX: origin_service joining the PK means each emitting service now owns its
#     OWN row for a shared edge, unconditionally -- both origins' assertions coexist
#     as two separate rows, and deleting one origin's row (its own begin_service or
#     delete_file_layer, scoped by origin_service exactly as before -- see those
#     methods' own docstrings, unchanged in semantics) never touches a sibling
#     origin's row for the identical (src, dst, type, via_channel) key. This is a
#     STRICTLY STRONGER invariant than M4 T7's "first writer wins" (no origin's
#     assertion is ever silently discarded any more, not even temporarily) -- not a
#     weakening of it. `Staging.upsert_edges` goes back to a plain, honest `INSERT
#     OR REPLACE` per the new (wider) PK -- the M4 T7 conflict-aware `ON CONFLICT ...
#     DO UPDATE ... WHERE` guard, and its own KNOWN RESIDUAL GAP paragraph, are both
#     removed outright (superseded, not layered on top of).
#
#     The graph itself must still end up with exactly ONE edge for a shared PK,
#     deterministically, regardless of how many origins assert it: that is now
#     `pipeline/load.py`'s job, not staging's -- `load_graph` groups staged edges by
#     (src, dst, type, via_channel) and picks a single deterministic winner per group
#     (priority resolution static > dynamic > heuristic, then max confidence, then
#     lexicographically-first origin_service) BEFORE ever batching to FalkorDB -- see
#     that module's own `_dedup_edges` docstring for the exact tie-break rules.
#     `Staging.iter_edges()` itself is deliberately left alone (still yields raw,
#     undeduplicated rows, exactly as before) -- every one of its OTHER consumers
#     (linking/segments.py's `derive`, linking/processes.py, chunking/augment.py,
#     evalx/edges_eval.py, evalx/calls_eval.py) either only ever cares about edge
#     TYPES a shared PK can't arise for in practice (PRODUCES/CONSUMES/HANDLES all
#     have a sym:-prefixed, single-service-owned endpoint; only a chan:-to-chan:
#     CONTAINS edge has no such anchor), or already collapses duplicates innocuously
#     on its own (segments.py's own `derived` dict keys on the pairing OUTCOME, so a
#     duplicate `contains_pairs` entry just re-derives an identical result; the
#     evalx modules build plain `set`s of comparison tuples) -- see this task's own
#     report for the full per-consumer argument.
#
#     `Staging.update_edge_props` (the ONE other place a bare (src, dst, type) key
#     was ever assumed to identify at most one row -- linking/workspace.py's
#     temporal-start marking, the sole real caller) is updated to apply its merge to
#     EVERY matching row, across every origin, independently -- the temporal-start
#     tag belongs to the (src, dst) PAIR semantically (see that call site's own
#     docstring), not to whichever origin happened to write its CALLS row first. Its
#     pre-existing NEXT_SEGMENT guard (via_channel ambiguity, unrelated to this bump)
#     is untouched.
SCHEMA_VERSION = 6
NODE_KINDS = frozenset({
    "Service", "Module", "Class", "Function", "Channel", "BusinessProcess",
})
# Роли — доп. label'ы поверх kind (multi-label, напр. :Sym:Function:RouteHandler),
# НЕ отдельные NODE_KINDS. Валидные значения NodeRec.roles (staging.upsert_nodes
# проверяет roles ⊆ ROLE_KINDS -- InvariantError иначе).
ROLE_KINDS = frozenset({
    "RouteHandler", "MessageConsumer", "MessageProducer",
    "TemporalWorkflow", "TemporalActivity",
})
EDGE_TYPES = frozenset({
    "CONTAINS", "IMPORTS", "CALLS",
    "HANDLES", "DEPENDS_ON", "PRODUCES", "CONSUMES",
    "INVOKES_ACTIVITY", "CALLS_HTTP", "NEXT_SEGMENT", "PART_OF_PROCESS",
})
RESOLUTIONS = frozenset({"static", "dynamic", "heuristic", "trace_validated"})
# Graph-only labels: valid FalkorDB node labels that are NOT valid NodeRec.kind values
# (a plain `MATCH`/`MERGE (n:<label>)` label, never `NodeRec.kind`/NODE_KINDS above) --
# "Sym" is python_core's own structural marker over Module/Class/Function (see
# pipeline/load._labels_for_kind); "Chunk"/"Meta" (M3 T6) are built from ChunkRow/a
# hand-built dict in pipeline/load.py, never a NodeRec at all (see stores/staging.py's
# ChunkRow docstring for why chunks live outside the NodeRec universe entirely).
# stores/falkordb/batch.py's node-label allowlist unions this in alongside NODE_KINDS/
# ROLE_KINDS specifically so there is ONE place (this module, already documented
# elsewhere as the schema's single source of truth) to register every valid graph
# label a future milestone adds -- not a second, easy-to-miss allowlist literal
# sitting three modules away in the storage layer.
GRAPH_ONLY_LABELS = frozenset({"Sym", "Chunk", "Meta"})


@dataclass(frozen=True)
class NodeRec:
    id: str
    kind: str
    service: str
    name: str
    qualified_name: str
    relpath: str | None = None
    start_byte: int | None = None
    end_byte: int | None = None
    start_line: int | None = None  # 1-based
    end_line: int | None = None
    content_hash: str | None = None
    props: dict = field(default_factory=dict)
    # Доп. labels поверх kind (см. ROLE_KINDS) -- только для кодовых узлов (Function
    # и т.п.); Channel/BusinessProcess/Service его игнорируют (см.
    # pipeline/load._labels_for_kind). Валидация ⊆ ROLE_KINDS -- в staging.upsert_nodes,
    # не здесь (NodeRec сам по себе -- голый IR, без побочных проверок при конструировании).
    roles: tuple[str, ...] = ()


@dataclass(frozen=True)
class EdgeRec:
    src: str
    dst: str
    type: str
    resolution: str
    confidence: float
    extractor: str
    evidence_file: str | None = None
    evidence_line: int | None = None  # 1-based
    props: dict = field(default_factory=dict)


def make_service_node(service: str) -> NodeRec:
    return NodeRec(
        id=f"svc:{service}", kind="Service", service=service,
        name=service, qualified_name=service,
    )


def make_channel_node(
    kind: Literal["kafka_topic", "event_type", "http_route"],
    name: str | None = None,
    *,
    owner_service: str | None = None,
    method: str | None = None,
    template: str | None = None,
    **extra_props: object,
) -> NodeRec:
    """Channel-узел (M2 linking): id строится через core.ids-хелперы, service="" --
    каналы кросс-сервисны по природе, не принадлежат одному сервису (см. staging
    upsert_edges инвариант: endpoints, начинающиеся на "chan:", не участвуют в
    cross-service проверке).

    Обязательные параметры зависят от kind:
      - kafka_topic / event_type: `name` обязателен (topic / event-type имя);
        id = ids.chan_kafka(name) / ids.chan_event(name).
      - http_route: `method` И `template` обязательны, `name` не используется;
        id = ids.chan_http(owner_service, method, template) (owner_service=None
        сериализуется в id как "?" -- см. ids.chan_http).

    `owner_service` -- сервис-владелец роута/топика (участвует в id ТОЛЬКО для
    http_route; для kafka_topic/event_type, если задан, попадает только в props
    как есть -- топик/событие сами по себе не привязаны к владельцу в id, в отличие
    от роута). НЕ путать с NodeRec.service (всегда "" для Channel).

    `extra_props` -- произвольные дополнительные свойства узла (напр.
    partition_key), копируются в NodeRec.props без изменений.

    NodeRec.name -- отображаемое имя: `name` для kafka_topic/event_type,
    "<METHOD> <template>" для http_route. qualified_name == id (каналы не имеют
    вложенной структуры, id уже уникально их идентифицирует).
    """
    if kind == "kafka_topic":
        if not name:
            raise ValueError("make_channel_node(kind='kafka_topic') requires name")
        node_id = ids.chan_kafka(name)
        display_name = name
    elif kind == "event_type":
        if not name:
            raise ValueError("make_channel_node(kind='event_type') requires name")
        node_id = ids.chan_event(name)
        display_name = name
    elif kind == "http_route":
        if not method or not template:
            raise ValueError(
                "make_channel_node(kind='http_route') requires method and template"
            )
        node_id = ids.chan_http(owner_service, method, template)
        display_name = f"{method} {template}"
    else:
        raise ValueError(f"unknown channel kind: {kind!r}")

    props: dict[str, object] = dict(extra_props)
    if owner_service is not None:
        props["owner_service"] = owner_service
    props["channel_kind"] = kind
    return NodeRec(
        id=node_id, kind="Channel", service="", name=display_name,
        qualified_name=node_id, props=props,
    )


def make_process_node(
    slug: str, name: str, entrypoint_id: str, source: str,
) -> NodeRec:
    """BusinessProcess-якорь (M2 S7 linking): id = ids.proc_id(slug) (proc:<slug>).
    service="" -- как и Channel, кросс-сервисен по природе. entrypoint_id -- id узла
    точки входа (напр. RouteHandler/consumer/workflow); source -- происхождение
    записи ("config" -- из cfg.processes воркспейса, "derived" -- выведен линковкой).
    """
    node_id = ids.proc_id(slug)
    return NodeRec(
        id=node_id, kind="BusinessProcess", service="", name=name,
        qualified_name=node_id,
        props={"entrypoint_id": entrypoint_id, "source": source},
    )
