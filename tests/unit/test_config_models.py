import pytest
import yaml
from pydantic import ValidationError

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    EmbeddingConfig,
    GenericArgSpec,
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


# -- M6 T3: kind="base_class" ConsumerIdiom (GAPS §4/pilot gap 4: shared-lib
# BaseConsumer[Event] subclasses, CONSUMES=0) --

BASE_CLASS_YAML = """
name: base-consumer-subclass
kind: base_class
base_class: "kyc_base_consumer.base.BaseConsumer"
handler_method: "process_event"
event_type_from: { generic_arg: 0 }
topic: { attr: "self.config.topic" }
"""


def test_base_class_yaml_shape_parses():
    """Exact YAML shape from the M6 T3 brief -- base_class/handler_method/
    event_type_from/topic round-trip."""
    idiom = ConsumerIdiom.model_validate(yaml.safe_load(BASE_CLASS_YAML))
    assert idiom.kind == "base_class"
    assert idiom.base_class == "kyc_base_consumer.base.BaseConsumer"
    assert idiom.handler_method == "process_event"
    assert idiom.event_type_from == GenericArgSpec(generic_arg=0)
    assert idiom.topic.attr == "self.config.topic"


def test_generic_arg_spec_defaults_to_zero():
    assert GenericArgSpec().generic_arg == 0


def test_generic_arg_spec_rejects_negative_index():
    """M6 T3 review Minor-4: a negative generic_arg could only ever mean a config
    typo (there is no negative-indexing semantics for generic params) -- rejected at
    load time via Field(ge=0) instead of silently producing zero claims forever
    (the extractor's own defensive range guard would treat it as a permanent miss)."""
    with pytest.raises(ValidationError):
        GenericArgSpec(generic_arg=-1)


def test_base_class_topic_is_optional():
    idiom = ConsumerIdiom.model_validate({
        "name": "x", "kind": "base_class",
        "base_class": "pkg.Base", "handler_method": "process_event",
        "event_type_from": {"generic_arg": 0},
    })
    assert idiom.topic is None


@pytest.mark.parametrize(
    "fields",
    [
        {"handler_method": "process_event", "event_type_from": {"generic_arg": 0}},
        {"base_class": "pkg.Base", "event_type_from": {"generic_arg": 0}},
        {"base_class": "pkg.Base", "handler_method": "process_event"},
        {"base_class": "pkg.Base"},
        {"handler_method": "process_event"},
        {"event_type_from": {"generic_arg": 0}},
        {},
    ],
    ids=[
        "no-base_class", "no-handler_method", "no-event_type_from",
        "base_class-only", "handler_method-only", "event_type_from-only", "none",
    ],
)
def test_base_class_partial_field_matrix_is_config_error_fail_closed(fields):
    """Fail-closed DSL matrix (same precedent as HttpClientIdiom's route_from
    all-or-nothing, M6 T2): base_class/handler_method/event_type_from are all
    required together -- a partial combination is a dead-forever config (missing
    base_class -> nothing to match against; missing handler_method -> nowhere to
    put the CONSUMES edge; missing event_type_from -> no way to name the channel),
    rejected at config-load time rather than silently producing zero claims."""
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({"name": "x", "kind": "base_class", **fields})


def test_base_class_all_three_fields_is_valid():
    idiom = ConsumerIdiom.model_validate({
        "name": "x", "kind": "base_class",
        "base_class": "pkg.Base", "handler_method": "process_event",
        "event_type_from": {"generic_arg": 0},
    })
    assert idiom.base_class == "pkg.Base"
    assert idiom.handler_method == "process_event"
    assert idiom.event_type_from == GenericArgSpec(generic_arg=0)


@pytest.mark.parametrize(
    "event_type_from",
    ["dict_key", {"arg": 0}, {"kwarg": "event_type"}, {"const": "X"}],
    ids=["dict_key", "arg", "kwarg", "const"],
)
def test_base_class_event_type_from_must_be_generic_arg_shape(event_type_from):
    """event_type_from shapes borrowed from the call-site-based idioms (ValueSpec's
    arg/kwarg/const/env/attr, or dispatch_dict's "dict_key") all presuppose a
    CallFact call-site, which a class definition never has -- only
    {generic_arg: N} is meaningful for kind=base_class, and the DSL rejects the
    others at load time instead of silently matching zero classes forever."""
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "base_class",
            "base_class": "pkg.Base", "handler_method": "process_event",
            "event_type_from": event_type_from,
        })


@pytest.mark.parametrize(
    "topic",
    [{"const": "orders.events"}, {"arg": 0}, {"kwarg": "topic"}, {"env": "TOPIC"}],
    ids=["const", "arg", "kwarg", "env"],
)
def test_base_class_topic_only_supports_attr_shape(topic):
    """kind=base_class has no call-site to resolve const/arg/kwarg/env against
    (unlike call/dispatch_dict's topic, resolved via resolve_value_spec against a
    CallFact) -- only {attr: ...} (a config-reference LABEL, always emitted as an
    unresolved channel) is meaningful here."""
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "base_class",
            "base_class": "pkg.Base", "handler_method": "process_event",
            "event_type_from": {"generic_arg": 0},
            "topic": topic,
        })


# -- M7 T2 (OPEN R2): ValueSpec settings:/enum: sources --


def test_value_spec_settings_source_parses():
    spec = ValueSpec.model_validate({"settings": "app.config.kafka.KafkaSettings.step_topic"})
    assert spec.settings == "app.config.kafka.KafkaSettings.step_topic"


def test_value_spec_enum_source_parses_via_alias():
    """YAML/DSL surface: the natural key is `enum:`, not the Python attribute name
    `enum_` (see ValueSpec's own field docstring for why the trailing underscore) --
    an alias, populated the same way `model_validate` already handles every other
    DSL field."""
    spec = ValueSpec.model_validate({"enum": "app.models.enums.KycTopicName"})
    assert spec.enum_ == "app.models.enums.KycTopicName"


def test_value_spec_enum_source_constructible_by_field_name():
    """`_Strict.model_config` gained `populate_by_name=True` (M7 T2) specifically so
    Python call sites (this test file's own idiom fixtures, kafka_ext.py) can
    construct `ValueSpec(enum_=...)` directly without spelling the YAML alias --
    both spellings populate the SAME field."""
    spec = ValueSpec(enum_="app.models.enums.KycTopicName")
    assert spec.enum_ == "app.models.enums.KycTopicName"


@pytest.mark.parametrize(
    "fields",
    [
        {"settings": "a.B.c", "arg": 0},
        {"enum": "a.B", "arg": 0},
        {"settings": "a.B.c", "enum": "a.B"},
        {"settings": "a.B.c", "const": "x"},
    ],
    ids=["settings+arg", "enum+arg", "settings+enum", "settings+const"],
)
def test_value_spec_exactly_one_source_extended_to_settings_and_enum(fields):
    """Same "exactly one source" contract as the original five fields (M2), now
    covering the two M7 T2 additions -- a ValueSpec naming settings/enum ALONGSIDE
    any other source (including each other) is a config error, not a
    first-source-wins silent resolution."""
    with pytest.raises(ValidationError):
        ValueSpec.model_validate(fields)


def test_value_spec_settings_alone_is_valid():
    assert ValueSpec.model_validate({"settings": "a.B.c"}) is not None


# -- M7 T2: enum source fail-closed outside a producer's topic-identity fields --


def test_channel_spec_event_type_from_enum_source_rejected():
    """R2a's fan-out over-approximation is sanctioned ONLY for a producer's TOPIC
    identity (name_from/topic, both plain kafka_topic-shaped channel identity) --
    the event_type_from field names the produced EVENT's type, a semantically
    different (and, for `event_type` kind, singular per PRODUCES edge) identity;
    fanning that out too has no analogous justification and is rejected at
    config-load time."""
    with pytest.raises(ValidationError):
        ChannelSpec.model_validate({
            "kind": "event_type", "event_type_from": {"enum": "app.models.enums.KycTopicName"},
        })


def test_channel_spec_name_from_enum_source_is_allowed():
    """Sanity/contrast: the SAME enum source on `name_from` (a kafka_topic channel's
    own identity, the field kafka_ext.py's fan-out mechanism actually reads) is NOT
    rejected -- only event_type_from is restricted."""
    ch = ChannelSpec.model_validate({
        "kind": "kafka_topic", "name_from": {"enum": "app.models.enums.KycTopicName"},
    })
    assert ch.name_from.enum_ == "app.models.enums.KycTopicName"


def test_channel_spec_topic_enum_source_is_allowed():
    """Same contrast on the OTHER topic-shaped field (event_type kind's CONTAINS-
    pairing `topic`) -- also unrestricted at the DSL layer (kafka_ext.py documents
    why it doesn't implement fan-out there today, a runtime/extraction decision, not
    a config-validity one)."""
    ch = ChannelSpec.model_validate({
        "kind": "event_type", "event_type_from": {"arg": 0},
        "topic": {"enum": "app.models.enums.KycTopicName"},
    })
    assert ch.topic.enum_ == "app.models.enums.KycTopicName"


def test_consumer_topic_enum_source_rejected():
    """"Consumer topic/event_type_from with enum -> config error" (M7 T2 brief): a
    handler consumes ONE topic, not a fan-out of every enum member -- no analogous
    semantics to the producer side's over-approximation."""
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "call", "call": "pkg.Client.listen",
            "topic": {"enum": "app.models.enums.KycTopicName"},
        })


def test_consumer_event_type_from_enum_source_rejected():
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "dispatch_dict",
            "registrar_call": "app.consumers.base.register_handlers",
            "event_type_from": {"enum": "app.models.enums.KycTopicName"},
        })


def test_base_class_topic_enum_source_still_rejected():
    """base_class's topic validator now accepts attr OR settings (see the test just
    below) but NOT enum -- both the base_class-specific "attr or settings" check and
    the general consumer-wide enum ban independently reject this; either is
    sufficient, this test just pins the observable outcome."""
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({
            "name": "x", "kind": "base_class",
            "base_class": "pkg.Base", "handler_method": "process_event",
            "event_type_from": {"generic_arg": 0},
            "topic": {"enum": "app.models.enums.KycTopicName"},
        })


# -- M7 T2: base_class topic now also accepts {settings: ...} alongside {attr: ...} --


def test_base_class_topic_settings_shape_is_valid():
    """Extends the base_class topic validator (previously attr-only): {settings:
    ...} needs no call-site either (it resolves from the service-wide ClassAttrIndex
    alone, see parsing/consts.py's resolve_settings_source), so it is just as
    meaningful here as {attr: ...} -- both remain valid (pinned separately at the
    extractor level, see test_kafka_extractor.py)."""
    idiom = ConsumerIdiom.model_validate({
        "name": "x", "kind": "base_class",
        "base_class": "pkg.Base", "handler_method": "process_event",
        "event_type_from": {"generic_arg": 0},
        "topic": {"settings": "app.config.kafka.KafkaSettings.step_topic"},
    })
    assert idiom.topic.settings == "app.config.kafka.KafkaSettings.step_topic"
