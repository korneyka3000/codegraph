import dataclasses

import pytest

from codegraph.core.schema import (
    EDGE_TYPES,
    NODE_KINDS,
    ROLE_KINDS,
    EdgeRec,
    NodeRec,
    make_channel_node,
    make_process_node,
    make_service_node,
)


def test_node_rec_frozen():
    n = NodeRec(id="x", kind="Function", service="s", name="f", qualified_name="m.f")
    with pytest.raises(dataclasses.FrozenInstanceError):
        n.id = "y"


def test_make_service_node():
    n = make_service_node("orders-api")
    assert n.id == "svc:orders-api" and n.kind == "Service"


def test_edge_defaults():
    e = EdgeRec(src="a", dst="b", type="CALLS", resolution="static",
                confidence=1.0, extractor="calls")
    assert e.props == {} and e.evidence_line is None


# -- M2: NODE_KINDS/EDGE_TYPES/ROLE_KINDS extensions --


def test_node_kinds_includes_channel_and_business_process():
    assert {"Channel", "BusinessProcess"} <= NODE_KINDS


def test_edge_types_includes_m2_additions():
    assert {
        "HANDLES", "DEPENDS_ON", "PRODUCES", "CONSUMES",
        "INVOKES_ACTIVITY", "CALLS_HTTP", "NEXT_SEGMENT", "PART_OF_PROCESS",
    } <= EDGE_TYPES


def test_role_kinds_are_exactly_the_six_documented_roles():
    # M7 T4 (OPEN R3): TemporalSignalHandler joins the pre-existing five -- one role
    # shared by @workflow.signal/@workflow.update/@workflow.query-decorated methods
    # (see extractors/temporal_ext.py's module docstring).
    assert ROLE_KINDS == frozenset({
        "RouteHandler", "MessageConsumer", "MessageProducer",
        "TemporalWorkflow", "TemporalActivity", "TemporalSignalHandler",
    })


# -- NodeRec.roles --


def test_node_rec_default_roles_is_empty_tuple():
    n = NodeRec(id="x", kind="Function", service="s", name="f", qualified_name="m.f")
    assert n.roles == ()


def test_node_rec_roles_accepts_tuple_of_role_kinds():
    n = NodeRec(
        id="x", kind="Function", service="s", name="f", qualified_name="m.f",
        roles=("RouteHandler",),
    )
    assert n.roles == ("RouteHandler",)


# -- make_channel_node --


def test_make_channel_node_kafka_topic():
    n = make_channel_node("kafka_topic", "orders.created")
    assert n.id == "chan:kafka_topic:orders.created"
    assert n.kind == "Channel"
    assert n.service == ""
    assert n.name == "orders.created"


def test_make_channel_node_event_type():
    n = make_channel_node("event_type", "OrderPlaced")
    assert n.id == "chan:event_type:OrderPlaced"
    assert n.kind == "Channel"


def test_make_channel_node_temporal_signal():
    # M7 T4 (OPEN R3): temporal_signal reuses the same name-only Channel shape as
    # kafka_topic/event_type (id = ids.chan_temporal_signal(name)) -- see
    # extractors/temporal_ext.py for the handler/sender pair this backs.
    n = make_channel_node("temporal_signal", "complete-survey")
    assert n.id == "chan:temporal_signal:complete-survey"
    assert n.kind == "Channel"
    assert n.service == ""
    assert n.name == "complete-survey"


def test_make_channel_node_temporal_signal_requires_name():
    with pytest.raises(ValueError):
        make_channel_node("temporal_signal")


def test_make_channel_node_http_route_uses_method_and_template():
    n = make_channel_node(
        "http_route", method="POST", template="/orders", owner_service="orders-api",
    )
    assert n.id == "chan:http:orders-api:POST /orders"
    assert n.kind == "Channel"
    assert n.name == "POST /orders"


def test_make_channel_node_http_route_without_owner_uses_question_mark():
    n = make_channel_node("http_route", method="GET", template="/health")
    assert n.id == "chan:http:?:GET /health"


def test_make_channel_node_http_route_requires_method_and_template():
    with pytest.raises(ValueError):
        make_channel_node("http_route", method="GET")
    with pytest.raises(ValueError):
        make_channel_node("http_route", template="/health")


def test_make_channel_node_kafka_requires_name():
    with pytest.raises(ValueError):
        make_channel_node("kafka_topic")


def test_make_channel_node_passes_through_extra_props():
    n = make_channel_node("kafka_topic", "orders.created", partition_key="order_id")
    assert n.props["partition_key"] == "order_id"


# -- make_process_node --


def test_make_process_node():
    n = make_process_node(
        "place-order", "Place Order", entrypoint_id="sym:orders-api:`app`/place().",
        source="config",
    )
    assert n.id == "proc:place-order"
    assert n.kind == "BusinessProcess"
    assert n.service == ""
    assert n.name == "Place Order"
    assert n.props["entrypoint_id"] == "sym:orders-api:`app`/place()."
    assert n.props["source"] == "config"
