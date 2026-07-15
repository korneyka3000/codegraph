import pytest
import yaml
from pydantic import ValidationError

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    ProducerIdiom,
    ServiceConfig,
    ValueSpec,
    WorkspaceConfig,
)

EXAMPLE = """
version: 1
graph_name: kyc
services:
  - name: orders-api
    path: ../orders-api
    python: .venv
    exclude: ["tests/**"]
    http: { base_url_env: ORDERS_API_URL }
    idioms:
      producers:
        - name: outbox
          call: "app.db.outbox.OutboxRepository.add_event"
          channel:
            kind: event_type
            event_type_from: { arg: 0 }
            topic: { const: "orders.events" }
  - name: kyc-worker
    path: ../kyc-worker
    idioms:
      consumers:
        - name: dispatch-map
          kind: dispatch_dict
          registrar_call: "app.consumers.base.register_handlers"
          topic: { const: "orders.events" }
          event_type_from: dict_key
      http_clients:
        - name: default-sdk
          file_glob: "**/clients/*_client.py"
          class_glob: "*Client"
          base_url: { attr: "self._base_url", env: DOCUMENT_MANAGEMENT_URL }
processes:
  - name: "Order KYC onboarding"
    entrypoint: "orders-api:POST /orders"
"""


def test_parse_example_workspace():
    cfg = WorkspaceConfig.model_validate(yaml.safe_load(EXAMPLE))
    assert cfg.graph_name == "kyc"
    assert cfg.storage.falkordb.port == 6379          # default
    assert cfg.embedding.provider == "local"           # default
    assert len(cfg.services) == 2
    outbox = cfg.services[0].idioms.producers[0]
    assert outbox.call == "app.db.outbox.OutboxRepository.add_event"
    assert outbox.channel.kind == "event_type"
    assert outbox.channel.event_type_from.arg == 0
    assert outbox.channel.topic.const == "orders.events"
    dispatch = cfg.services[1].idioms.consumers[0]
    assert dispatch.kind == "dispatch_dict"
    assert dispatch.event_type_from == "dict_key"
    sdk = cfg.services[1].idioms.http_clients[0]
    assert sdk.base_url.env == "DOCUMENT_MANAGEMENT_URL"
    assert cfg.builtin_idioms == [
        "fastapi", "aiokafka", "faststream", "confluent", "temporal", "aiohttp_client",
    ]
    assert cfg.processes[0].entrypoint == "orders-api:POST /orders"


def test_value_spec_exactly_one_source():
    with pytest.raises(ValidationError):
        ValueSpec.model_validate({"const": "x", "arg": 0})
    with pytest.raises(ValidationError):
        ValueSpec.model_validate({})
    assert ValueSpec.model_validate({"kwarg": "event_type"}).kwarg == "event_type"


def test_consumer_dispatch_dict_requires_registrar_call():
    # M3 T1: dict_assign removed (was a validated-but-never-consumed alternate
    # dispatch_dict discovery mechanism -- see ConsumerIdiom's docstring); registrar_call
    # is now the ONLY way to satisfy kind="dispatch_dict".
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({"name": "x", "kind": "dispatch_dict"})
    ok = ConsumerIdiom.model_validate(
        {"name": "x", "kind": "dispatch_dict",
         "registrar_call": "app.consumers.base.register_handlers"}
    )
    assert ok.registrar_call == "app.consumers.base.register_handlers"


def test_consumer_dict_assign_field_removed_rejected_as_unknown():
    # Breaking change (M3 T1, documented on ConsumerIdiom): a codegraph.yaml still
    # specifying dict_assign now fails loudly via pydantic's extra="forbid" (unknown
    # field), not a silent no-op -- an explicit signal to migrate to registrar_call.
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "dispatch_dict",
            "registrar_call": "app.consumers.base.register_handlers",
            "dict_assign": "EVENT_HANDLERS",
        })


def test_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        ProducerIdiom.model_validate(
            {"name": "x", "call": "a.b", "channel": {"kind": "kafka_topic"}, "typo": 1}
        )


def test_channel_spec_kafka_topic_name_from_arg():
    ch = ChannelSpec.model_validate({"kind": "kafka_topic", "name_from": {"arg": 0}})
    assert ch.name_from.arg == 0


def test_service_name_rejects_unsafe_characters():
    for bad in ("my svc", "a:b", "a/b"):
        with pytest.raises(ValidationError):
            ServiceConfig.model_validate({"name": bad, "path": "."})
    svc = ServiceConfig.model_validate({"name": "kyc-worker_2", "path": "."})
    assert svc.name == "kyc-worker_2"
