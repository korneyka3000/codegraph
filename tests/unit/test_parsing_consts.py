from pathlib import Path

from codegraph.config.models import ValueSpec
from codegraph.parsing.class_attrs import ClassAttrIndex, SettingsField
from codegraph.parsing.consts import (
    ConstTable,
    Resolved,
    resolve_arg,
    resolve_settings_source,
    resolve_value_spec,
)
from codegraph.parsing.facts import build_file_facts

CONSTS_SRC = b'''TOPIC = "orders.events"
OTHER_TOPIC = "documents.indexed"
COUNT = 5
DYNAMIC = f"prefix-{COUNT}"


def send(topic, payload=None):
    pass


def call_with_const():
    send(TOPIC)


def call_with_unknown_name():
    send(UNKNOWN_NAME)


def call_with_settings():
    send(settings.DATABASE_URL)


def call_with_env_environ():
    send(os.environ["DOCUMENT_MANAGEMENT_URL"])


def call_with_env_getenv():
    send(os.getenv("SOME_VAR"))


def call_with_fstring():
    send(f"{self._base_url}/documents/{doc_id}")


def call_with_kwarg():
    send(topic=TOPIC, payload="x")


def local_scope():
    LOCAL = "nope"
    return LOCAL
'''


def _facts():
    return build_file_facts("app/x.py", CONSTS_SRC)


def _call_in(facts, func_name, callee="send"):
    fn_idx = next(d.index for d in facts.defs if d.name == func_name)
    return next(c for c in facts.calls if c.enclosing_def == fn_idx and c.callee_name == callee)


def _consts():
    return ConstTable.build(_facts(), CONSTS_SRC)


# -- ConstTable.build: module-level NAME = "literal" only --


def test_const_table_build_module_level_strings():
    consts = _consts()
    assert consts.get("TOPIC") == "orders.events"
    assert consts.get("OTHER_TOPIC") == "documents.indexed"


def test_const_table_excludes_non_string_literal():
    consts = _consts()
    assert consts.get("COUNT") is None


def test_const_table_excludes_fstring_assignment():
    consts = _consts()
    assert consts.get("DYNAMIC") is None


def test_const_table_excludes_function_local_assignment():
    consts = _consts()
    assert consts.get("LOCAL") is None


def test_const_table_unknown_name_is_none():
    consts = _consts()
    assert consts.get("NOPE") is None


# -- resolve_arg --


def test_resolve_arg_string_is_value():
    facts = _facts()
    call = _call_in(facts, "call_with_kwarg")
    payload_arg = next(a for a in call.args if a.keyword == "payload")
    resolved = resolve_arg(payload_arg, _consts())
    assert resolved == Resolved(kind="value", value="x")


def test_resolve_arg_name_in_const_table_is_value():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved == Resolved(kind="value", value="orders.events")


def test_resolve_arg_unknown_name_is_unresolved():
    facts = _facts()
    call = _call_in(facts, "call_with_unknown_name")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved.kind == "unresolved"
    assert resolved.value is None
    assert resolved.config_ref is None


def test_resolve_arg_settings_attribute_is_config_ref():
    facts = _facts()
    call = _call_in(facts, "call_with_settings")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved == Resolved(kind="config_ref", value=None, config_ref="DATABASE_URL")


def test_resolve_arg_os_environ_subscript_is_config_ref():
    facts = _facts()
    call = _call_in(facts, "call_with_env_environ")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved == Resolved(kind="config_ref", value=None, config_ref="DOCUMENT_MANAGEMENT_URL")


def test_resolve_arg_os_getenv_is_config_ref():
    facts = _facts()
    call = _call_in(facts, "call_with_env_getenv")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved == Resolved(kind="config_ref", value=None, config_ref="SOME_VAR")


def test_resolve_arg_fstring_leading_interpolation_is_base_template():
    facts = _facts()
    call = _call_in(facts, "call_with_fstring")
    resolved = resolve_arg(call.args[0], _consts())
    assert resolved == Resolved(kind="template", value="<base>/documents/{doc_id}")


def test_resolve_arg_fstring_non_leading_interpolation_no_base_marker_real_fixture():
    # kyc_worker consumers/orders.py: id=f"kyc-{payload['order_id']}" — interpolation is
    # NOT at the start (literal "kyc-" text precedes it) -> no <base> marker; also
    # exercises the raw-text fallback for a non-identifier/attribute interpolation
    # expression (subscript payload['order_id']).
    path = Path(__file__).parents[2] / "fixtures/services/kyc_worker/app/consumers/orders.py"
    source = path.read_bytes()
    facts = build_file_facts(str(path), source)
    call = next(c for c in facts.calls if c.callee_name == "start_workflow")
    id_arg = next(a for a in call.args if a.keyword == "id")
    resolved = resolve_arg(id_arg, ConstTable.build(facts, source))
    assert resolved.kind == "template"
    assert resolved.value == "kyc-{payload['order_id']}"
    assert "<base>" not in resolved.value


# -- resolve_arg: real fixtures (document-management client SDK) --


def _client_facts():
    path = (
        Path(__file__).parents[2]
        / "fixtures/services/kyc_worker/app/clients/document_management_client.py"
    )
    source = path.read_bytes()
    return build_file_facts(str(path), source)


def test_resolve_arg_fstring_base_marker_get_document_real_fixture():
    facts = _client_facts()
    call = next(c for c in facts.calls if c.callee_name == "get")
    resolved = resolve_arg(call.args[0], ConstTable.build(facts, b""))
    assert resolved == Resolved(kind="template", value="<base>/documents/{doc_id}")


def test_resolve_arg_fstring_base_marker_no_trailing_interpolation():
    facts = _client_facts()
    call = next(c for c in facts.calls if c.callee_name == "post")
    resolved = resolve_arg(call.args[0], ConstTable.build(facts, b""))
    assert resolved == Resolved(kind="template", value="<base>/documents")


# -- resolve_value_spec --


def test_resolve_value_spec_const():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(const="literal-value")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved == Resolved(kind="value", value="literal-value")


def test_resolve_value_spec_arg_index_zero():
    facts = _facts()
    call = _call_in(facts, "call_with_const")  # send(TOPIC) -> args[0]
    spec = ValueSpec(arg=0)
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved == Resolved(kind="value", value="orders.events")


def test_resolve_value_spec_kwarg():
    facts = _facts()
    call = _call_in(facts, "call_with_kwarg")  # send(topic=TOPIC, payload="x")
    spec = ValueSpec(kwarg="topic")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved == Resolved(kind="value", value="orders.events")


def test_resolve_value_spec_env():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(env="FOO_ENV")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved == Resolved(kind="config_ref", value=None, config_ref="FOO_ENV")


def test_resolve_value_spec_attr_is_unresolved_stub():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(attr="_base_url")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved.kind == "unresolved"


def test_resolve_value_spec_missing_arg_index_is_unresolved():
    facts = _facts()
    call = _call_in(facts, "call_with_const")  # only args[0] exists
    spec = ValueSpec(arg=5)
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved.kind == "unresolved"


def test_resolve_value_spec_missing_kwarg_is_unresolved():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(kwarg="nope")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved.kind == "unresolved"


# -- resolve_settings_source / resolve_value_spec settings: source (M7 T2, OPEN R2) --

SETTINGS_FIELD_WITH_DEFAULT = SettingsField(
    class_fqn="app.config.kafka.KafkaSettings", field="step_topic",
    default="kyc.step.changed", env_name="SERVICE_KAFKA_STEP_TOPIC",
)
SETTINGS_FIELD_NO_DEFAULT = SettingsField(
    class_fqn="app.config.kafka.KafkaSettings", field="worker_url",
    default=None, env_name="SERVICE_WORKER_URL",
)
SETTINGS_FIELD_BARE = SettingsField(
    class_fqn="app.config.kafka.KafkaSettings", field="mystery",
    default=None, env_name=None,
)
SETTINGS_INDEX = ClassAttrIndex(
    settings_by_class={
        "app.config.kafka.KafkaSettings": {
            "step_topic": SETTINGS_FIELD_WITH_DEFAULT,
            "worker_url": SETTINGS_FIELD_NO_DEFAULT,
            "mystery": SETTINGS_FIELD_BARE,
        },
    },
    enums_by_class={},
    field_index={},
)


def test_resolve_settings_source_with_default_is_static_value():
    resolved = resolve_settings_source("app.config.kafka.KafkaSettings.step_topic", SETTINGS_INDEX)
    assert resolved == Resolved(kind="value", value="kyc.step.changed")


def test_resolve_settings_source_no_default_env_present_is_config_ref():
    """Mirrors resolve_value_spec's pre-existing `env:` source shape exactly (same
    Resolved kind, same field names) -- a settings field discovered to have no
    literal default but a real env_name is, for every downstream purpose (kafka_ext's
    channel-name/confidence/props machinery), indistinguishable from an idiom that
    named the env var directly."""
    resolved = resolve_settings_source("app.config.kafka.KafkaSettings.worker_url", SETTINGS_INDEX)
    assert resolved == Resolved(kind="config_ref", value=None, config_ref="SERVICE_WORKER_URL")


def test_resolve_settings_source_neither_default_nor_env_is_unresolved():
    """A genuinely bare field (`Field(...)` with no default, no alias, and no
    env_prefix'd model_config in scope -- SettingsField(default=None,
    env_name=None)) has nothing at all to resolve to -- the same honest miss as an
    unknown class/field, not a crash."""
    resolved = resolve_settings_source("app.config.kafka.KafkaSettings.mystery", SETTINGS_INDEX)
    assert resolved.kind == "unresolved"


def test_resolve_settings_source_unknown_field_is_unresolved_not_crash():
    resolved = resolve_settings_source("app.config.kafka.KafkaSettings.nope", SETTINGS_INDEX)
    assert resolved.kind == "unresolved"


def test_resolve_settings_source_unknown_class_is_unresolved_not_crash():
    resolved = resolve_settings_source("app.config.kafka.Nope.step_topic", SETTINGS_INDEX)
    assert resolved.kind == "unresolved"


def test_resolve_settings_source_no_class_attr_index_is_unresolved():
    resolved = resolve_settings_source("app.config.kafka.KafkaSettings.step_topic", None)
    assert resolved.kind == "unresolved"


def test_resolve_value_spec_settings_source_with_default():
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(settings="app.config.kafka.KafkaSettings.step_topic")
    resolved = resolve_value_spec(spec, call, _consts(), SETTINGS_INDEX)
    assert resolved == Resolved(kind="value", value="kyc.step.changed")


def test_resolve_value_spec_settings_source_defaults_class_attr_index_param_to_none():
    """`class_attr_index` is an optional TRAILING parameter (default None, M7 T2
    brief) -- an existing call site that never threads T1's index at all degrades to
    the same honest unresolved outcome as an index that simply doesn't know the
    class, not a TypeError from a suddenly-required argument."""
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(settings="app.config.kafka.KafkaSettings.step_topic")
    resolved = resolve_value_spec(spec, call, _consts())
    assert resolved.kind == "unresolved"


def test_resolve_value_spec_enum_source_is_defensively_unresolved():
    """enum_ is NOT a single-value source at all -- kafka_ext.py special-cases it
    BEFORE ever calling resolve_value_spec (see kafka_ext._emit_enum_fanout_produces),
    fanning out into multiple channels itself from ClassAttrIndex.enum_values
    directly. Reaching this branch at all means some caller didn't special-case it
    first -- defensively unresolved rather than a crash or a fabricated single
    value; ValueSpec's OWN config-load-time validators (config/models.py) are what
    actually keep this from happening for the DSL's sanctioned shapes."""
    facts = _facts()
    call = _call_in(facts, "call_with_const")
    spec = ValueSpec(enum_="app.models.enums.KycTopicName")
    resolved = resolve_value_spec(spec, call, _consts(), SETTINGS_INDEX)
    assert resolved.kind == "unresolved"
