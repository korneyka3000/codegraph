import pytest
import yaml
from pydantic import ValidationError

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    EmbeddingConfig,
    HttpClientIdiom,
    HttpRouteFromSpec,
    HttpVerbFromSpec,
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


def test_embedding_config_prefixes_default_empty():
    # M5 T6: query_prefix/passage_prefix default to "" -- an existing codegraph.yaml
    # that never mentions either field keeps building the exact same EmbeddingConfig
    # it always has (LocalEmbedder then sees a no-op prefix, see test_embedders.py).
    cfg = EmbeddingConfig()
    assert cfg.query_prefix == ""
    assert cfg.passage_prefix == ""


def test_embedding_config_prefixes_settable_from_yaml():
    raw = yaml.safe_load(
        """
        provider: local
        model: intfloat/multilingual-e5-base
        query_prefix: "query: "
        passage_prefix: "passage: "
        """
    )
    cfg = EmbeddingConfig.model_validate(raw)
    assert cfg.query_prefix == "query: "
    assert cfg.passage_prefix == "passage: "


# -- M6 T2: decorator-SDK HttpClientIdiom (route_from/call/verb_from) --

DECORATOR_SDK_YAML = """
name: decorator-sdk
file_glob: "**/clients/*.py"
class_glob: "*Client"
route_from: { decorator: "path_template", arg: 0 }
call: "driver.fetch_content|driver.fetch"
verb_from: { request_ctor: "Request", enum: "Method" }
"""


def test_decorator_sdk_yaml_shape_parses():
    """Exact YAML shape from the M6 T2 brief -- route_from/call/verb_from round-trip."""
    idiom = HttpClientIdiom.model_validate(yaml.safe_load(DECORATOR_SDK_YAML))
    assert idiom.file_glob == "**/clients/*.py"
    assert idiom.class_glob == "*Client"
    assert idiom.route_from == HttpRouteFromSpec(decorator="path_template", arg=0)
    assert idiom.call == "driver.fetch_content|driver.fetch"
    assert idiom.verb_from == HttpVerbFromSpec(request_ctor="Request", enum="Method")


def test_http_client_idiom_new_fields_absent_by_default():
    """All three new fields absent -> existing verb-mode idiom is byte-identical."""
    idiom = HttpClientIdiom(name="default-sdk")
    assert idiom.route_from is None
    assert idiom.call is None
    assert idiom.verb_from is None


ROUTE_FROM = {"decorator": "path_template", "arg": 0}
VERB_FROM = {"request_ctor": "Request", "enum": "Method"}


@pytest.mark.parametrize(
    "fields",
    [
        {"route_from": ROUTE_FROM},                                # route alone
        {"route_from": ROUTE_FROM, "call": "driver.fetch"},        # no verb_from
        {"route_from": ROUTE_FROM, "verb_from": VERB_FROM},        # no call
        {"call": "driver.fetch"},                                  # call alone
        {"verb_from": VERB_FROM},                                  # verb_from alone
        {"call": "driver.fetch", "verb_from": VERB_FROM},          # both, no route_from
    ],
    ids=["route-only", "route+call", "route+verb", "call-only", "verb-only", "call+verb"],
)
def test_decorator_sdk_partial_field_matrix_is_config_error_fail_closed(fields):
    """Fail-closed DSL matrix (M6 T2 review, Important-2): the three decorator-SDK
    fields are all-or-nothing. route_from without call OR verb_from is a dead-forever
    config (the extractor can never locate the call-site / never resolve a verb ->
    zero claims, silently); call/verb_from without route_from are silently-inert
    fields the verb-mode path never reads. Every partial cell is rejected at
    config-load time; only all-three (decorator-SDK) or none (verb-mode) validate."""
    with pytest.raises(ValidationError):
        HttpClientIdiom.model_validate({"name": "decorator-sdk", **fields})


def test_decorator_sdk_all_three_fields_is_valid():
    idiom = HttpClientIdiom.model_validate({
        "name": "decorator-sdk",
        "route_from": ROUTE_FROM,
        "call": "driver.fetch_content",
        "verb_from": VERB_FROM,
    })
    assert idiom.route_from.decorator == "path_template"
    assert idiom.call == "driver.fetch_content"
    assert idiom.verb_from.request_ctor == "Request"


def test_route_from_arg_defaults_to_zero():
    spec = HttpRouteFromSpec(decorator="path_template")
    assert spec.arg == 0
