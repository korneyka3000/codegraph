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
SCHEMA_VERSION = 3
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
