"""IR узлов/рёбер и константы схемы. Единый словарь для staging, load и eval."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from codegraph.core import ids

SCHEMA_VERSION = 1
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
