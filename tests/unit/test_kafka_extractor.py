"""M2 T5: extract_kafka (producers/outbox, consumers -- ctor + dispatch_dict).

Real-fixture tests exercise the exact scenarios named in the task brief's self-review
checklist: the custom `outbox` producer idiom (workspace.yaml) at STATIC tier (via a
stubbed ref_symbol_lookup simulating a live SCIP resolution of `add_event`'s callee
span -- mirrors fastapi_ext's DEPENDS_ON stubbing pattern, "юнит: стаб; интеграцию
покроет T9"), the builtin aiokafka producer on document_management's real
events/producer.py (IMPORT_NAME tier, matches test_idiom_match.py's own tier proof),
the builtin aiokafka consumer-ctor on kyc_worker's real consumer_main.py (IMPORT_NAME
tier), and the dispatch_dict consumer on kyc_worker's real consumers/orders.py (STATIC
tier via stub on the registrar call + a second stub resolving the dict value's name
span to the handler's node id -- both spans come straight from T2's real dict_items,
not hand-computed literals).

qualified_of (STATIC tier's ref-by-callee-span -> display_qualified chain) mirrors
fastapi_ext's `_resolve_depends_target`: ref_symbol_lookup at the CALLEE's own span
(CallFact.callee_start_byte/end_byte already IS that span -- no extra span math needed,
unlike Depends() which had to locate an identifier inside separate param text).

Synthetic sources cover branches no real fixture reaches: cross-pattern producer dedup
(two idioms matching the identical call -- first in idiom list order wins, the call is
claimed and the second never runs), unresolved/template/config_ref value-resolution
(heuristic/min(0.6) downgrade + config_ref prop), a module-level producer call (proves
the node_ids[None] -> Module id fallback analyze.py's wiring must provide), and
defensive missing-node-id / missing-dict / non-string-key / unresolved-ref-value
dispatch_dict branches.
"""

from __future__ import annotations

from pathlib import Path

from codegraph.config.builtin_idioms import resolve_builtins
from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    GenericArgSpec,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.extractors.base import FileContext
from codegraph.extractors.kafka_ext import KafkaResult, extract_kafka
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.class_attrs import ClassAttrIndex, SettingsField
from codegraph.parsing.consts import ConstTable
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"


def _fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


def _load(
    relpath: str, service: str, source: bytes, *,
    ref_symbol_lookup=None, class_attr_index: ClassAttrIndex | None = None,
):
    """Builds (ctx, node_ids, consts) exactly as analyze.py's S5 wiring will: node_ids
    is def-index -> resolved node id (from python_core's own per-file output, Module
    node first then exactly one node per facts.defs entry, same order) PLUS a
    None -> Module-node-id entry (CallFact.enclosing_def is None for module-level
    calls -- the same sentinel, so `node_ids.get(call.enclosing_def)` transparently
    falls back to the Module id with no special-casing in the extractor itself).

    `class_attr_index` (M7 T2): same "optional trailing kwarg, default None"
    convention `ref_symbol_lookup` already established -- every pre-existing call
    site (every test in this file before this task) keeps building a ctx with
    class_attr_index=None, unchanged."""
    facts = build_file_facts(relpath, source)
    core_ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    core_res = extract_python_core(core_ctx)
    node_ids = {
        d.index: n.id
        for d, n in zip(facts.defs, core_res.nodes[1:], strict=True)
    }
    node_ids[None] = core_res.nodes[0].id
    ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
        ref_symbol_lookup=ref_symbol_lookup, class_attr_index=class_attr_index,
    )
    consts = ConstTable.build(facts, source)
    return ctx, node_ids, consts


def _outbox_ctx(**kw):
    relpath = "app/services/order.py"
    return _load(relpath, "orders-api", _fixture_bytes(f"orders_api/{relpath}"), **kw)


def _producer_ctx(**kw):
    relpath = "app/events/producer.py"
    source = _fixture_bytes(f"document_management/{relpath}")
    return _load(relpath, "document-management", source, **kw)


def _consumer_main_ctx(**kw):
    relpath = "app/consumer_main.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"), **kw)


def _dispatch_ctx(**kw):
    relpath = "app/consumers/orders.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"), **kw)


def _def(ctx: FileContext, name: str):
    return next(d for d in ctx.facts.defs if d.name == name)


def _idioms(producers=(), consumers=()) -> ServiceIdioms:
    return ServiceIdioms(producers=list(producers), consumers=list(consumers))


OUTBOX_IDIOM = ProducerIdiom(
    name="outbox",
    call="app.db.outbox.OutboxRepository.add_event",
    channel=ChannelSpec(
        kind="event_type", event_type_from=ValueSpec(arg=0), topic=ValueSpec(const="orders.events"),
    ),
)

DOC_PRODUCER_IDIOM = ProducerIdiom(
    name="aiokafka-send",
    call="aiokafka.AIOKafkaProducer.send",
    channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
)

CONSUMER_CTOR_IDIOM = ConsumerIdiom(
    name="aiokafka-consumer-init", kind="call",
    call="aiokafka.AIOKafkaConsumer", topic=ValueSpec(arg=0),
)

DISPATCH_IDIOM = ConsumerIdiom(
    name="dispatch-map", kind="dispatch_dict",
    registrar_call="app.consumers.base.register_handlers",
    topic=ValueSpec(const="orders.events"), event_type_from="dict_key",
)


# -- KafkaResult: contract shape --


def test_kafka_result_field_shape():
    r = KafkaResult(roles={}, channels=[], edges=[], stats={})
    assert r.roles == {}
    assert r.channels == []
    assert r.edges == []
    assert r.stats == {}


def test_no_idioms_at_all_is_a_noop():
    ctx, node_ids, consts = _outbox_ctx()
    result = extract_kafka(ctx, node_ids, ServiceIdioms(), consts)
    assert result == KafkaResult(roles={}, channels=[], edges=[], stats=result.stats)
    assert result.edges == []


# -- outbox producer (custom idiom, workspace.yaml): STATIC tier via scip stub --


def test_outbox_producer_static_tier_produces_event_channel_and_containment():
    ctx0, _, _ = _outbox_ctx()
    add_event = next(c for c in ctx0.facts.calls if c.callee_name == "add_event")
    span = add_event.callee_start_byte
    relpath = "app/services/order.py"

    def ref_lookup(rp, sb):
        if (rp, sb) == (relpath, span):
            return "scip-python python orders-api 0.0 `app.db.outbox`/OutboxRepository#add_event()."
        return None

    ctx, node_ids, consts = _outbox_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_kafka(ctx, node_ids, _idioms(producers=[OUTBOX_IDIOM]), consts)
    place_id = node_ids[_def(ctx, "place").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == place_id
    assert p.dst == "chan:event_type:OrderCreated"
    assert p.resolution == "static" and p.confidence == 1.0
    assert p.extractor == "kafka"
    assert p.evidence_file == relpath
    assert p.props == {}

    contains = [e for e in result.edges if e.type == "CONTAINS"]
    assert len(contains) == 1
    c = contains[0]
    assert c.src == "chan:kafka_topic:orders.events"
    assert c.dst == "chan:event_type:OrderCreated"
    assert c.resolution == "static" and c.confidence == 1.0
    assert c.extractor == "kafka"

    assert result.roles[place_id] == {"MessageProducer"}
    chan_ids = {chan.id for chan in result.channels}
    assert chan_ids == {"chan:event_type:OrderCreated", "chan:kafka_topic:orders.events"}
    event_chan = next(chan for chan in result.channels if chan.id == "chan:event_type:OrderCreated")
    assert event_chan.kind == "Channel" and event_chan.service == ""
    assert event_chan.name == "OrderCreated"
    assert result.stats["producers_resolved"] == 1


def test_outbox_producer_receiver_tier_without_scip_stub():
    """No ref_symbol_lookup wired at all (SCIP unavailable) -- `outbox = OutboxRepository(...)`
    is a same-scope AssignFact, so RECEIVER tier (0.8) still fires; confirms kafka_ext
    doesn't hardcode STATIC-only matching."""
    ctx, node_ids, consts = _outbox_ctx()
    result = extract_kafka(ctx, node_ids, _idioms(producers=[OUTBOX_IDIOM]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].resolution == "heuristic" and produces[0].confidence == 0.8


# -- builtin aiokafka producer (document_management/app/events/producer.py): IMPORT_NAME --


def test_builtin_producer_import_name_tier_documents_indexed_topic():
    ctx, node_ids, consts = _producer_ctx()
    result = extract_kafka(ctx, node_ids, _idioms(producers=[DOC_PRODUCER_IDIOM]), consts)
    emit_id = node_ids[_def(ctx, "emit_document_indexed").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == emit_id
    assert p.dst == "chan:kafka_topic:documents.indexed"
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert result.roles[emit_id] == {"MessageProducer"}
    # kafka_topic kind -- no event pairing, so no CONTAINS edge should appear
    assert not any(e.type == "CONTAINS" for e in result.edges)


# -- builtin aiokafka consumer-ctor (kyc_worker/app/consumer_main.py): IMPORT_NAME --


def test_consumer_ctor_import_name_tier_orders_events_topic():
    ctx, node_ids, consts = _consumer_main_ctx()
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[CONSUMER_CTOR_IDIOM]), consts)
    run_id = node_ids[_def(ctx, "run_consumer").index]

    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    c = consumes[0]
    assert c.src == run_id
    assert c.dst == "chan:kafka_topic:orders.events"
    assert c.resolution == "heuristic" and c.confidence == 0.6
    assert c.props == {"dispatch": "topic"}
    assert c.extractor == "kafka"
    assert result.roles[run_id] == {"MessageConsumer"}
    assert result.stats["consumers_resolved"] == 1


# -- dispatch_dict consumer (kyc_worker/app/consumers/orders.py): STATIC via stub --


def _dispatch_handler_span():
    ctx0, _, _ = _dispatch_ctx()
    reg_call = next(c for c in ctx0.facts.calls if c.callee_name == "register_handlers")
    dict_arg = next(a for a in reg_call.args if a.value_kind == "dict")
    key, value = dict_arg.dict_items[0]
    assert key.string_value == "OrderCreated"
    return reg_call, value.name_start_byte


def test_dispatch_dict_static_tier_handler_consumes_and_topic_containment():
    reg_call, span = _dispatch_handler_span()
    relpath = "app/consumers/orders.py"
    handler_sym = "scip-python python kyc-worker 0.0 `app.consumers.orders`/handle_order_created()."
    handler_id = "sym:kyc-worker:`app.consumers.orders`/handle_order_created()."
    registrar_sym = "scip-python python kyc-worker 0.0 `app.consumers.base`/register_handlers()."

    def ref_lookup(rp, sb):
        # registrar call's OWN callee span -> forces STATIC tier on the registrar_call
        # match itself (brief: "match registrar_call (STATIC ожидаем)"); separately, the
        # dict VALUE's span -> resolves the handler node id. Two independent lookups on
        # the same file, exactly like a real SCIP index would answer both occurrences.
        if (rp, sb) == (relpath, reg_call.callee_start_byte):
            return registrar_sym
        if (rp, sb) == (relpath, span):
            return handler_sym
        return None

    ctx, node_ids, consts = _dispatch_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[DISPATCH_IDIOM]), consts)

    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    c = consumes[0]
    assert c.src == handler_id
    assert c.dst == "chan:event_type:OrderCreated"
    assert c.props == {"dispatch": "event_type"}
    assert c.resolution == "static" and c.confidence == 1.0
    assert c.evidence_line == reg_call.start_line

    contains = [e for e in result.edges if e.type == "CONTAINS"]
    assert len(contains) == 1
    assert contains[0].src == "chan:kafka_topic:orders.events"
    assert contains[0].dst == "chan:event_type:OrderCreated"
    assert contains[0].resolution == "static" and contains[0].confidence == 1.0

    assert result.roles[handler_id] == {"MessageConsumer"}
    assert result.stats["dispatch_handlers_resolved"] == 1


def test_dispatch_dict_handler_unresolved_ref_lookup_skips_with_stat():
    ctx, node_ids, consts = _dispatch_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[DISPATCH_IDIOM]), consts)
    assert not any(e.type == "CONSUMES" for e in result.edges)
    assert not any(e.type == "CONTAINS" for e in result.edges)
    assert result.roles == {}
    assert result.stats["dispatch_handler_unresolved"] == 1


def test_dispatch_dict_no_ref_symbol_lookup_wired_degrades_to_unresolved_no_crash():
    ctx, node_ids, consts = _dispatch_ctx()  # ref_symbol_lookup defaults to None
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[DISPATCH_IDIOM]), consts)
    assert result.edges == []
    assert result.stats["dispatch_handler_unresolved"] == 1


NO_DICT_SRC = b'''def register_handlers(mapping):
    pass


register_handlers(some_variable)
'''


def test_dispatch_dict_registrar_call_without_dict_arg_skips_with_stat():
    relpath = "m.py"
    ctx0, _, _ = _load(relpath, "svc", NO_DICT_SRC)
    reg_call = next(c for c in ctx0.facts.calls if c.callee_name == "register_handlers")

    def ref_lookup(rp, sb):
        # forces a match at all -- "register_handlers" is a same-file LOCAL def here
        # (no import), so none of match_calls' structural tiers apply on their own.
        if (rp, sb) == (relpath, reg_call.callee_start_byte):
            return "scip-python python svc 0.0 `m`/register_handlers()."
        return None

    ctx, node_ids, consts = _load(relpath, "svc", NO_DICT_SRC, ref_symbol_lookup=ref_lookup)
    idiom = ConsumerIdiom(
        name="d", kind="dispatch_dict", registrar_call="m.register_handlers",
        topic=ValueSpec(const="t"), event_type_from="dict_key",
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    assert result.edges == []
    assert result.stats["dispatch_dict_missing"] == 1


NON_STRING_KEY_SRC = b'''SOME_KEY = "computed"


def register_handlers(mapping):
    pass


def handler(event):
    pass


register_handlers({SOME_KEY: handler})
'''


def test_dispatch_dict_non_string_key_entry_skipped():
    relpath = "m.py"

    def ref_lookup(rp, sb):
        return "scip-python python svc 0.0 `m`/handler()."

    ctx, node_ids, consts = _load(relpath, "svc", NON_STRING_KEY_SRC, ref_symbol_lookup=ref_lookup)
    idiom = ConsumerIdiom(
        name="d", kind="dispatch_dict", registrar_call="register_handlers",
        topic=None, event_type_from="dict_key",
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    assert result.edges == []


# -- cross-pattern producer dedup (self-review checklist) --


DEDUP_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send("real-topic")
'''


def test_cross_pattern_producer_dedup_first_idiom_in_list_order_wins():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", DEDUP_SRC)
    first = ProducerIdiom(
        name="first", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    second = ProducerIdiom(
        name="second", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(const="should-not-appear")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[first, second]), consts)

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].dst == "chan:kafka_topic:real-topic"
    chan_names = {c.name for c in result.channels}
    assert "should-not-appear" not in chan_names
    assert len(result.channels) == 1


def test_cross_pattern_producer_dedup_reversed_order_second_now_wins():
    """Same two idioms, swapped list order -- proves priority follows LIST ORDER, not
    some idiom-name/alphabetical tiebreak."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", DEDUP_SRC)
    first = ProducerIdiom(
        name="first", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(const="should-not-appear")),
    )
    second = ProducerIdiom(
        name="second", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[first, second]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].dst == "chan:kafka_topic:should-not-appear"


# -- value-resolution branches: unresolved / template / config_ref --


UNRESOLVED_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send(dynamic_value())
'''


def test_producer_unresolved_channel_no_edge_and_stat_incremented():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", UNRESOLVED_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1


# -- M2 final review: empty resolved channel name must not crash make_channel_node --

EMPTY_STRING_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send("")
'''

EMPTY_FSTRING_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send(f"")
'''


def test_producer_empty_string_channel_name_no_crash_and_stat_incremented():
    """An empty resolved channel name (bare "" literal here, kind="value") used to
    reach make_channel_node(name="") and raise ValueError -- crashing the whole
    `codegraph index` run -- instead of being treated as just another unresolved
    channel like any other resolution failure."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", EMPTY_STRING_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_empty_fstring_channel_name_no_crash_and_stat_incremented():
    """f"" resolves through a DIFFERENT Resolved.kind ("template", not "value" --
    _fstring_template("f\\"\\"") joins zero string_content/interpolation children into
    "") than a bare "" literal -- both empty-value shapes must be guarded
    independently (see kafka_ext._emit_kafka_topic_produces)."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", EMPTY_FSTRING_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    assert result.edges == []
    assert result.stats["producer_unresolved_channel"] == 1


def test_consumer_empty_string_channel_name_no_crash_and_stat_incremented():
    """Same empty-name crash class, consumer side (_emit_call_consumer's own
    consumer_unresolved_topic counter, not the producer one)."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", EMPTY_STRING_SRC.replace(b"send", b"listen"))
    idiom = ConsumerIdiom(
        name="c", kind="call", call="pkg.Client.listen", topic=ValueSpec(arg=0),
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    assert result.edges == []
    assert result.roles == {}
    assert result.stats["consumer_unresolved_topic"] == 1


def test_dispatch_dict_empty_string_key_skipped_no_crash():
    """Same empty-name crash class, dispatch_dict's event-key path (a literal dict key,
    not a Resolved value -- see kafka_ext._emit_dispatch_dict's `not key_arg.string_value`
    guard)."""
    relpath = "m.py"
    src = b'''def register_handlers(mapping):
    pass


def handler(event):
    pass


register_handlers({"": handler})
'''

    def ref_lookup(rp, sb):
        return "scip-python python svc 0.0 `m`/handler()."

    ctx, node_ids, consts = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    idiom = ConsumerIdiom(
        name="d", kind="dispatch_dict", registrar_call="register_handlers",
        topic=None, event_type_from="dict_key",
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}


TEMPLATE_SRC = b'''from pkg import Client


def use():
    client = Client()
    name = "svc"
    client.send(f"{name}-events")
'''


def test_producer_template_value_downgrades_to_heuristic_min_point_six():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", TEMPLATE_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].resolution == "heuristic" and produces[0].confidence == 0.6
    assert produces[0].dst == "chan:kafka_topic:<base>-events"


ENV_SRC = b'''import os
from pkg import Client


def use():
    client = Client()
    client.send(os.environ["TOPIC_NAME"])
'''


def test_producer_config_ref_downgrades_and_carries_prop():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", ENV_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert p.props == {"config_ref": "TOPIC_NAME"}
    assert p.dst == "chan:kafka_topic:${TOPIC_NAME}"


def test_producer_env_spec_directly_is_config_ref():
    """ValueSpec(env=...) -- not ArgFact-text-detected -- also routes through config_ref."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", DEDUP_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(env="KAFKA_TOPIC")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].props == {"config_ref": "KAFKA_TOPIC"}
    assert produces[0].dst == "chan:kafka_topic:${KAFKA_TOPIC}"


# -- defensive: missing node id --


def test_missing_node_id_for_matched_producer_skips_gracefully():
    ctx, _real_node_ids, consts = _outbox_ctx()
    result = extract_kafka(ctx, {}, _idioms(producers=[OUTBOX_IDIOM]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_missing_node_id"] == 1


def test_missing_node_id_for_matched_call_consumer_skips_gracefully():
    ctx, _real_node_ids, consts = _consumer_main_ctx()
    result = extract_kafka(ctx, {}, _idioms(consumers=[CONSUMER_CTOR_IDIOM]), consts)
    assert result.edges == []
    assert result.roles == {}
    assert result.stats["consumer_missing_node_id"] == 1


# -- module-level producer call: node_ids[None] -> Module id fallback --


MODULE_LEVEL_SRC = b'''from pkg import Client

client = Client()
client.send("module-topic")
'''


def test_producer_module_level_call_uses_module_node_id_fallback():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", MODULE_LEVEL_SRC)
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].src == node_ids[None]
    assert result.roles[node_ids[None]] == {"MessageProducer"}


# -- M6 T3: base_class consumer idiom (GAPS §4/pilot gap 4: shared-lib
# BaseConsumer[Event] subclasses, CONSUMES=0 on the real KYC stack) --
#
# Real convention (pilot GAPS §5): `class OCRDataConsumer(BaseConsumer[OCRDataEvent]):`
# with a `process_event` handler method; the base's generic argument IS the event's
# stable static identity (unlike the dynamic `self.config.topic` attribute the same
# class's own `setup()` uses to build the raw aiokafka consumer).

BASE_CLASS_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
)

# The from-import line is load-bearing for every non-STATIC test below (M6 T3 review
# Important-1): the textual IMPORT_NAME fallback requires import-statement evidence
# (mirrors idiom_match._match_import_name_ctor_form) -- bare name equality alone no
# longer matches. Real consumer files always import their base, so this also makes
# the synthetic more faithful to the pilot convention (GAPS §5).
BASE_CLASS_SRC = b'''from kyc_base_consumer.base import BaseConsumer


class OCRDataConsumer(BaseConsumer[OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True

    async def setup(self) -> None:
        pass
'''


def _base_class_ref_lookup(relpath: str):
    """Stubbed SCIP ref-lookup resolving the base name token ("BaseConsumer" in
    `BaseConsumer[OCRDataEvent]`) to the configured FQN's own class symbol -- mirrors
    this file's existing "юнит: стаб; интеграцию покроет T9" stubbing pattern (real
    scip-python integration is out of scope at this unit level, same as every other
    STATIC-tier proof here). A class descriptor ends with "#" (core/ids.py's own
    `structural_descriptor`), not "()." like a method/function."""
    span = BASE_CLASS_SRC.index(b"BaseConsumer[")

    def lookup(rp, sb):
        if (rp, sb) == (relpath, span):
            return "scip-python python some-lib 0.0 `kyc_base_consumer.base`/BaseConsumer#"
        return None

    return lookup


def test_base_class_static_tier_scip_resolved_consumes_event_channel():
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(
        relpath, "kyc-worker", BASE_CLASS_SRC,
        ref_symbol_lookup=_base_class_ref_lookup(relpath),
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    handler_id = node_ids[_def(ctx, "process_event").index]

    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    c = consumes[0]
    assert c.src == handler_id
    assert c.dst == "chan:event_type:OCRDataEvent"
    assert c.resolution == "static" and c.confidence == 1.0
    assert c.props == {"dispatch": "event_type"}
    assert c.extractor == "kafka"
    assert c.evidence_file == relpath
    assert result.roles[handler_id] == {"MessageConsumer"}
    assert result.stats["consumers_resolved"] == 1
    assert result.stats["consumer_base_class_no_generic"] == 0
    chan_ids = {chan.id for chan in result.channels}
    assert chan_ids == {"chan:event_type:OCRDataEvent"}


def test_base_class_textual_fallback_tier_without_scip_stub():
    """No ref_symbol_lookup wired at all (SCIP unavailable) -- the base name's bare
    text ("BaseConsumer") equals the configured base_class's last FQN segment AND the
    file from-imports that name (`from kyc_base_consumer.base import BaseConsumer` in
    BASE_CLASS_SRC), so the IMPORT_NAME-tier textual fallback fires (0.6/heuristic)
    instead of silently producing nothing; pins the scip-resolved vs textual-fallback
    confidence difference against the STATIC-tier test above."""
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", BASE_CLASS_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].resolution == "heuristic" and consumes[0].confidence == 0.6
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"


# -- M6 T3 review Important-1: the textual tier requires import corroboration
# (mirrors idiom_match._match_import_name_ctor_form's exact checks) -- bare name
# equality alone used to match `class C(evil.BaseConsumer[FooEvent])` at 0.6 with
# facts.imports == [] (reviewer-executed false positive).


NO_IMPORT_EVIL_PREFIX_SRC = b'''class C(evil.BaseConsumer[FooEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_module_prefixed_base_without_module_import_is_a_noop():
    """The reviewer's exact false-positive case: `evil.BaseConsumer[...]` with ZERO
    imports in the file -- the prefixed form requires the configured FQN's first
    module segment to actually be imported (ctor-form's _imports_module check), so
    nothing matches and nothing is emitted (not even the no-generic counter: the
    base itself never matched)."""
    relpath = "app/consumers/evil.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", NO_IMPORT_EVIL_PREFIX_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["consumer_base_class_no_generic"] == 0


NO_IMPORT_BARE_SRC = b'''class OCRDataConsumer(BaseConsumer[OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_bare_name_without_import_evidence_is_a_noop():
    """Bare `BaseConsumer[...]` in a file that imports NOTHING: name equality with
    the FQN's last segment is no longer sufficient on its own -- a from-import of
    that name is required for the bare form (ctor-form's `class_seg in imp.names`
    check)."""
    relpath = "app/consumers/noimp.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", NO_IMPORT_BARE_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.roles == {}


MODULE_PREFIX_SRC = b'''import kyc_base_consumer.base


class OCRDataConsumer(kyc_base_consumer.base.BaseConsumer[OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_module_prefixed_base_with_module_import_matches():
    """`import kyc_base_consumer.base` + fully-prefixed base -- the prefixed form's
    module-import corroboration fires (IMPORT_NAME tier, 0.6). Also one of the four
    reviewer-verified-but-unpinned shapes (module-prefixed base)."""
    relpath = "app/consumers/prefixed.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", MODULE_PREFIX_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].resolution == "heuristic" and consumes[0].confidence == 0.6
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"


ALIASED_MODULE_PREFIX_SRC = b'''import kyc_base_consumer.base as shared


class OCRDataConsumer(shared.BaseConsumer[OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_aliased_module_import_matches_prefix_text_not_verified():
    """Chosen semantics, pinned honestly: for a PREFIXED base, the prefix text is NOT
    required to textually match the imported module -- only (a) prefixed form and
    (b) the configured FQN's first module segment imported somewhere in the file
    (ImportFact.target_module records the REAL module name for `import m as alias`,
    see facts.py's aliased_import handling). This is a literal mirror of
    idiom_match._match_import_name_ctor_form's receiver comment ("receiver не обязан
    текстуально совпадать с именем модуля") and is exactly what makes the alias form
    `import kyc_base_consumer.base as shared` + `shared.BaseConsumer[...]` match
    (correctly -- it genuinely IS the configured base) at this weakest tier without
    any alias-resolution machinery."""
    relpath = "app/consumers/aliased.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", ALIASED_MODULE_PREFIX_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].resolution == "heuristic" and consumes[0].confidence == 0.6


WRONG_MODULE_FROM_IMPORT_SRC = b'''from evil import BaseConsumer


class OCRDataConsumer(BaseConsumer[OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_from_import_of_same_name_from_other_module_matches_documented_laxity():
    """Documented laxity, pinned honestly: the bare-name form's from-import check is
    `last_segment in imp.names` for ANY import (the module part is NOT verified
    against the configured FQN's prefix) -- an exact mirror of ctor-form's own
    `any(class_seg in imp.names ...)`, which is precisely this lax for calls too.
    `from evil import BaseConsumer` therefore still matches at the weakest tier;
    tightening beyond the precedent is out of this fix's scope."""
    relpath = "app/consumers/wrongmod.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", WRONG_MODULE_FROM_IMPORT_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].resolution == "heuristic" and consumes[0].confidence == 0.6


DIFFERENT_BASE_SRC = b'''class NotAConsumer(SomeOtherBase[Whatever]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_subclass_of_different_base_is_a_noop():
    relpath = "app/consumers/other.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", DIFFERENT_BASE_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["consumer_base_class_no_generic"] == 0


NO_GENERIC_SRC = b'''from kyc_base_consumer.base import BaseConsumer


class OCRDataConsumer(BaseConsumer):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_no_generic_param_no_claim_and_stat_incremented_not_crash():
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", NO_GENERIC_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["consumer_base_class_no_generic"] == 1


TOPIC_ATTR_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
    topic=ValueSpec(attr="self.config.topic"),
)


def test_base_class_topic_attr_emits_unresolved_channel_and_containment():
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", BASE_CLASS_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[TOPIC_ATTR_IDIOM]), consts)

    assert result.stats["consumers_resolved"] == 1
    contains = [e for e in result.edges if e.type == "CONTAINS"]
    assert len(contains) == 1
    c = contains[0]
    assert c.src == "chan:kafka_topic:${self.config.topic}"
    assert c.dst == "chan:event_type:OCRDataEvent"
    assert c.resolution == "heuristic" and c.confidence == 0.6
    assert c.extractor == "kafka"

    topic_chan = next(
        ch for ch in result.channels if ch.id == "chan:kafka_topic:${self.config.topic}"
    )
    assert topic_chan.props["unresolved"] is True
    assert topic_chan.props["config_ref"] == "self.config.topic"
    assert topic_chan.props["channel_kind"] == "kafka_topic"


MULTI_INHERIT_SRC = b'''from pkg import BaseConsumer, Mixin


class C(Mixin, BaseConsumer[FooEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_multi_inherit_finds_generic_base_not_just_first():
    """`Mixin` (the FIRST base) does not match -- proves the extractor checks every
    base, not just bases[0]."""
    relpath = "app/consumers/multi.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", MULTI_INHERIT_SRC)
    idiom = ConsumerIdiom(
        name="x", kind="base_class", base_class="pkg.BaseConsumer",
        handler_method="process_event", event_type_from=GenericArgSpec(generic_arg=0),
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:FooEvent"


ATTR_GENERIC_SRC = b'''from pkg import BaseConsumer


class AttrBase(BaseConsumer[evtmod.OCRDataEvent]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_generic_arg_attribute_chain_reduces_to_last_identifier():
    relpath = "app/consumers/attr.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", ATTR_GENERIC_SRC)
    idiom = ConsumerIdiom(
        name="x", kind="base_class", base_class="pkg.BaseConsumer",
        handler_method="process_event", event_type_from=GenericArgSpec(generic_arg=0),
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"


MULTI_GENERIC_SRC = b'''from pkg import Base


class C(Base[A, B]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_generic_arg_index_selects_second_param():
    relpath = "app/consumers/multigeneric.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", MULTI_GENERIC_SRC)
    idiom = ConsumerIdiom(
        name="x", kind="base_class", base_class="pkg.Base",
        handler_method="process_event", event_type_from=GenericArgSpec(generic_arg=1),
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[idiom]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:B"


def test_base_class_missing_handler_method_no_crash_stat_incremented():
    """Class structurally/texually matches the configured base (WITH a generic
    param) but lacks the configured handler_method entirely (e.g. renamed) -- must
    degrade gracefully (no claim), not crash on a None def lookup."""
    relpath = "app/consumers/nohandler.py"
    src = b'''from kyc_base_consumer.base import BaseConsumer


class OCRDataConsumer(BaseConsumer[OCRDataEvent]):
    async def some_other_method(self, event) -> bool:
        return True
'''
    ctx, node_ids, consts = _load(relpath, "kyc-worker", src)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.stats["consumer_missing_node_id"] == 1


def test_base_class_missing_node_id_for_handler_skips_gracefully():
    """Handler def exists but node_ids has no entry for it -- defensive parity with
    the call/dispatch_dict consumer paths' own missing-node-id guard."""
    relpath = "app/consumers/ocr.py"
    ctx, _real_node_ids, consts = _load(relpath, "kyc-worker", BASE_CLASS_SRC)
    result = extract_kafka(ctx, {}, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    assert result.edges == []
    assert result.roles == {}
    assert result.stats["consumer_missing_node_id"] == 1


def test_base_class_kind_ignored_by_call_and_dispatch_dict_extraction_paths():
    """Sanity: a base_class-kind idiom mixed into the SAME consumers list as
    call/dispatch_dict idioms doesn't confuse either of those paths (each kind
    filters its own idioms explicitly, see kafka_ext._extract_call_consumers /
    _extract_dispatch_dict_consumers's `idiom.kind != ...` guards)."""
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", BASE_CLASS_SRC)
    result = extract_kafka(
        ctx, node_ids, _idioms(consumers=[CONSUMER_CTOR_IDIOM, BASE_CLASS_IDIOM]), consts,
    )
    # CONSUMER_CTOR_IDIOM (kind="call") matches nothing in this file (no aiokafka
    # ctor call at all) -- only the base_class match should produce an edge.
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].props == {"dispatch": "event_type"}


# -- M6 T3 review Minor-1: reviewer-verified-but-unpinned shapes (the fourth --
# module-prefixed base -- is pinned above alongside the Important-1 semantics).


NESTED_CLASS_SRC = b'''from kyc_base_consumer.base import BaseConsumer


class Outer:
    class InnerConsumer(BaseConsumer[OCRDataEvent]):
        async def process_event(self, event) -> bool:
            return True
'''


def test_base_class_nested_class_inside_class_matches():
    """A consumer class NESTED inside another class: _scan_class_bases' recursive
    walk finds it (keyed by ITS OWN name token, not the outer's), and the handler
    lookup is parent-index-scoped (d.parent == class_def.index), so the inner
    class's own process_event -- not anything on Outer -- gets the role/edge."""
    relpath = "app/consumers/nested.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", NESTED_CLASS_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    handler_id = node_ids[_def(ctx, "process_event").index]
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].src == handler_id
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"
    assert result.roles[handler_id] == {"MessageConsumer"}


THREE_BASES_SRC = b'''from kyc_base_consumer.base import BaseConsumer
from pkg import After, Before


class C(Before, BaseConsumer[OCRDataEvent], After):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_three_bases_middle_position_matches():
    relpath = "app/consumers/middle.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", THREE_BASES_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"


KEYWORD_PLUS_GENERIC_SRC = b'''from abc import ABCMeta

from kyc_base_consumer.base import BaseConsumer


class C(BaseConsumer[OCRDataEvent], metaclass=ABCMeta):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_keyword_argument_alongside_generic_base_matches():
    """`metaclass=ABCMeta` in the same superclasses list is a keyword_argument node
    -- skipped by _scan_class_bases (and by facts' base_exprs), never confused for
    a base; the generic base beside it still matches."""
    relpath = "app/consumers/meta.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", KEYWORD_PLUS_GENERIC_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:OCRDataEvent"


# -- M6 T3 review Minor-2: nested-subscript generic arg -- raw-text channel name --


NESTED_SUBSCRIPT_SRC = b'''from kyc_base_consumer.base import BaseConsumer


class C(BaseConsumer[dict[str, OCRDataEvent]]):
    async def process_event(self, event) -> bool:
        return True
'''


def test_base_class_nested_subscript_generic_arg_uses_raw_text_channel_name():
    """Known limitation, pinned (see _generic_arg_text's docstring): a NESTED
    subscript generic (`BaseConsumer[dict[str, Event]]`) is neither an identifier
    nor an attribute, so the whole raw expression text -- whitespace included,
    exactly as written in source -- becomes the channel name. No real consumer in
    the pilot convention parameterizes its base with anything but a bare event
    class; if one ever does, the honest raw-text identity at least keys producer/
    consumer consistently for byte-identical spellings."""
    relpath = "app/consumers/nestedsub.py"
    ctx, node_ids, consts = _load(relpath, "kyc-worker", NESTED_SUBSCRIPT_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[BASE_CLASS_IDIOM]), consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:event_type:dict[str, OCRDataEvent]"


# -- M6 T4 (GAPS §6/pilot gap 5 -- Kafka producer wrapper + kwarg-sourced topic):
#
# Real convention (camunda-gateway's app/services/producer.py):
#     class KYCEventPublisher:
#         async def publish(self, body, topic_name, customer_uid):
#             producer = await self.producer()                        # local var
#             await producer.send_and_wait(topic=topic_name, ...)      # topic=KWARG
# Call site: `publisher.publish(body, payload.topic_name, payload.customer_uid)` --
# topic_name is itself DYNAMIC (an attribute expression, not a literal).
#
# ValueSpec.kwarg (config/models.py), ArgFact.keyword (parsing/facts.py's
# _build_call_args), and resolve_value_spec's kwarg branch (parsing/consts.py) all
# ALREADY existed before this task -- kafka_ext.py itself needed NO production
# changes: `_emit_producer`/`_emit_kafka_topic_produces`/`_emit_event_type_produces`
# resolve `channel.name_from`/`channel.event_type_from` generically via
# `resolve_value_spec`, regardless of which of the 5 ValueSpec sources is
# configured. The tests below PIN that already-correct, already-generic behavior
# for the specific kwarg/wrapper shapes gap 5 needs -- none of them are expected to
# RED against this module; the one genuinely NEW piece of this task (surfacing
# `producer_unresolved_channel` in the per-service report) lives in
# test_pipeline_analyze.py/test_pipeline_report.py instead, since kr.stats already
# counts this case (see _Sink.stats above) -- only the PIPELINE was silently
# dropping it.


KWARG_TOPIC_SRC = b'''from aiokafka import AIOKafkaProducer


async def use():
    producer = AIOKafkaProducer()
    await producer.send_and_wait(topic="orders.created", value=b"payload")
'''


def test_producer_user_idiom_kwarg_topic_source_produces_channel():
    """Builtin `aiokafka-send-and-wait` (config/builtin_idioms.py, UNTOUCHED by this
    task) uses `name_from={arg: 0}` and still can't see this call at all -- no
    ArgFact has index==0 when topic is passed as `topic=...` with no positional args
    (see the contrast test just below). A CUSTOM idiom with `name_from={kwarg:
    "topic"}` resolves it directly -- find-the-arg-by-keyword-instead-of-position,
    exactly what gap 5 needs. RECEIVER tier (0.8): `producer = AIOKafkaProducer()`
    is a same-scope AssignFact."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", KWARG_TOPIC_SRC)
    idiom = ProducerIdiom(
        name="kwarg-topic", call="aiokafka.AIOKafkaProducer.send_and_wait",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(kwarg="topic")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].dst == "chan:kafka_topic:orders.created"
    assert produces[0].resolution == "heuristic" and produces[0].confidence == 0.8
    assert result.stats["producers_resolved"] == 1


def test_producer_builtin_arg0_idiom_misses_the_same_kwarg_call_by_constraint():
    """Sanity/contrast, using the REAL builtin idiom set unmodified: confirms the
    custom kwarg idiom above does genuinely new work rather than duplicating
    existing builtin coverage -- `aiokafka-send-and-wait`'s own `name_from={arg: 0}`
    really does miss a topic passed ONLY as a kwarg (pilot gap 5's own root cause)."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", KWARG_TOPIC_SRC)
    result = extract_kafka(ctx, node_ids, resolve_builtins(["aiokafka"]), consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_kwarg_missing_from_call_is_unresolved_with_counter_not_crash():
    """The idiom names a kwarg this particular call simply never passes at all
    (topic is entirely absent, not just positional) -- an honest miss, not a crash:
    no edge/channel, `producer_unresolved_channel` bumped."""
    relpath = "m.py"
    src = b'''from aiokafka import AIOKafkaProducer


async def use():
    producer = AIOKafkaProducer()
    await producer.send_and_wait(value=b"payload")
'''
    ctx, node_ids, consts = _load(relpath, "svc", src)
    idiom = ProducerIdiom(
        name="kwarg-topic", call="aiokafka.AIOKafkaProducer.send_and_wait",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(kwarg="topic")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1


# -- wrapper idiom (pilot gap 5's actual fix): match the business-level wrapper
# method itself (`KYCEventPublisher.publish`), sidestepping BOTH the low-level
# `send_and_wait` call's kwarg AND its local-variable-receiver problem (`producer =
# await self.producer()` -- a differently-named local each call, same shape as the
# builtin aiokafka producer's own documented RECEIVER-tier miss, config/
# builtin_idioms.py's module docstring).

WRAPPER_LITERAL_SRC = b'''from app.services.producer import KYCEventPublisher


async def use(publisher: KYCEventPublisher):
    await publisher.publish("body-bytes", "orders.created", "cust-1")
'''

WRAPPER_DYNAMIC_SRC = b'''from app.services.producer import KYCEventPublisher


async def use(publisher: KYCEventPublisher, payload):
    await publisher.publish(payload.body, payload.topic_name, payload.customer_uid)
'''

WRAPPER_IDIOM = ProducerIdiom(
    name="kyc-event-publisher-wrapper",
    call="app.services.producer.KYCEventPublisher.publish",
    channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=1)),
)


def test_producer_wrapper_call_arg_index_literal_topic_produces():
    """`publish(body, topic_name, customer_uid)` -- topic is arg index 1 (0-based;
    `self` is never part of CallFact.args, it's the receiver). A literal there
    resolves exactly like any other arg-indexed producer idiom (the outbox idiom's
    own arg=0 precedent above) -- IMPORT_NAME tier: `publisher` is a bare parameter
    (no same-file `publisher = KYCEventPublisher(...)` AssignFact), so RECEIVER
    doesn't apply, but the file DOES `from app.services.producer import
    KYCEventPublisher` (module `app` imported)."""
    relpath = "app/kyc_engine/activities/kafka_events.py"
    ctx, node_ids, consts = _load(relpath, "camunda-gateway", WRAPPER_LITERAL_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(producers=[WRAPPER_IDIOM]), consts)
    use_id = node_ids[_def(ctx, "use").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == use_id
    assert p.dst == "chan:kafka_topic:orders.created"
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert result.roles[use_id] == {"MessageProducer"}
    assert result.stats["producers_resolved"] == 1


def test_producer_wrapper_call_dynamic_attr_topic_is_unresolved_with_counter():
    """Real pilot shape: `publisher.publish(body, payload.topic_name, ...)` -- the
    topic argument is an attribute EXPRESSION (`payload.topic_name`), not a literal.
    `resolve_arg` has no way to statically know an arbitrary attribute's runtime
    value (unlike a bare name found in ConstTable, or the settings./os.environ[]/
    os.getenv() textual conventions it DOES detect) -- honest unresolved: no
    PRODUCES edge, no Channel node, `producer_unresolved_channel` bumped, no crash.
    This is the exact call this task's brief calls out as needing to be
    read-and-pinned rather than silently doing nothing."""
    relpath = "app/kyc_engine/activities/kafka_events.py"
    ctx, node_ids, consts = _load(relpath, "camunda-gateway", WRAPPER_DYNAMIC_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(producers=[WRAPPER_IDIOM]), consts)

    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1
    assert result.stats["producers_resolved"] == 0


def test_producer_wrapper_const_event_type_produces_event_channel():
    """Documented manual fallback for a fixed-event-type producer wrapper: when the
    call site can't give a resolvable channel identity via the topic argument (the
    dynamic case just above), `event_type: {const: "..."}` fixes the event's TYPE at
    config time -- a human-authored escape hatch for exactly this shape (topic
    dynamic, but the call site itself is monomorphic in event type). Proves the
    combination end to end: wrapper call match + const event_type -> PRODUCES into
    an event_type channel (no CONTAINS: channel.topic is unset here)."""
    relpath = "app/kyc_engine/activities/kafka_events.py"
    ctx, node_ids, consts = _load(relpath, "camunda-gateway", WRAPPER_DYNAMIC_SRC)
    idiom = ProducerIdiom(
        name="kyc-event-publisher-wrapper-fixed-type",
        call="app.services.producer.KYCEventPublisher.publish",
        channel=ChannelSpec(kind="event_type", event_type_from=ValueSpec(const="OrderCreated")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    use_id = node_ids[_def(ctx, "use").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == use_id
    assert p.dst == "chan:event_type:OrderCreated"
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert result.roles[use_id] == {"MessageProducer"}
    assert not any(e.type == "CONTAINS" for e in result.edges)


WRAPPER_EVENT_TYPE_KWARG_SRC = b'''from app.services.producer import KYCEventPublisher


async def use(publisher: KYCEventPublisher):
    await publisher.publish("body-bytes", event_type="OrderCreated")
'''


def test_producer_wrapper_kwarg_event_type_produces_event_channel():
    """M6-T4 review minor (M7 backlog): event_type_from={kwarg: ...}'s resolution
    mechanism is already proven generically at the consts level
    (test_parsing_consts.py's test_resolve_value_spec_kwarg) and at kafka_ext level
    for name_from (test_producer_user_idiom_kwarg_topic_source_produces_channel
    above) -- but was never pinned, at the kafka_ext EXTRACTOR level, for
    event_type_from specifically. Same wrapper-call idiom shape as the const-event-
    type test just above, except the call site passes event_type=... as a KWARG
    instead of fixing it at config time -- proves resolve_value_spec's shared kwarg
    branch (spec.kwarg -> find ArgFact by keyword -> resolve_arg) is actually reached
    via event_type_from too, not just name_from/topic. `_emit_event_type_produces`
    needed no production changes for this -- it already resolves `channel.
    event_type_from` generically via `_resolve_event_type_from`/`resolve_value_spec`,
    exactly like `_emit_kafka_topic_produces` does for `name_from`."""
    relpath = "app/kyc_engine/activities/kafka_events.py"
    ctx, node_ids, consts = _load(relpath, "camunda-gateway", WRAPPER_EVENT_TYPE_KWARG_SRC)
    idiom = ProducerIdiom(
        name="kyc-event-publisher-wrapper-kwarg-type",
        call="app.services.producer.KYCEventPublisher.publish",
        channel=ChannelSpec(kind="event_type", event_type_from=ValueSpec(kwarg="event_type")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    use_id = node_ids[_def(ctx, "use").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == use_id
    assert p.dst == "chan:event_type:OrderCreated"
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert result.roles[use_id] == {"MessageProducer"}
    assert not any(e.type == "CONTAINS" for e in result.edges)
    assert result.stats["producers_resolved"] == 1


# -- M7 T2 (OPEN R2): settings:/enum: ValueSpec sources -- producer-side literals
# from code, and the enum fan-out over-approximation.
#
# ClassAttrIndex fixture shared by every test below (M7 T1's own index -- directly
# constructible per that module's own docstring: "the three fields are this
# module's own assembly detail, freely constructible directly in tests").

KAFKA_SETTINGS_INDEX = ClassAttrIndex(
    settings_by_class={
        "app.config.kafka.KafkaSettings": {
            "step_topic": SettingsField(
                class_fqn="app.config.kafka.KafkaSettings", field="step_topic",
                default="kyc.step.changed", env_name="SERVICE_KAFKA_STEP_TOPIC",
            ),
            "worker_url": SettingsField(
                class_fqn="app.config.kafka.KafkaSettings", field="worker_url",
                default=None, env_name="SERVICE_WORKER_URL",
            ),
        },
    },
    enums_by_class={
        "app.models.enums.KycTopicName": (
            "kyc.camunda.step_changed.basic_survey",
            "kyc.camunda.step_changed.basic_kyc",
            "kyc.camunda.restrictions_changed",
        ),
    },
    field_index={},
)

SETTINGS_PRODUCER_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send("ignored-arg")
'''


def _settings_producer_idiom(settings_ref: str) -> ProducerIdiom:
    return ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(settings=settings_ref)),
    )


def test_producer_settings_source_with_default_is_static_literal_channel():
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", SETTINGS_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    idiom = _settings_producer_idiom("app.config.kafka.KafkaSettings.step_topic")
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.dst == "chan:kafka_topic:kyc.step.changed"
    # RECEIVER tier (0.8): `client = Client()` is a same-scope AssignFact -- settings
    # resolves to a Resolved(kind="value") literal, so _resolution_for keeps the
    # MATCHED CALL's own tier resolution/confidence as-is (a resolved literal never
    # further downgrades, exactly like const:).
    assert p.resolution == "heuristic" and p.confidence == 0.8
    assert p.props == {}
    assert result.stats["producers_resolved"] == 1
    assert result.stats["producer_unresolved_channel"] == 0


def test_producer_settings_source_no_default_is_config_ref_placeholder_channel():
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", SETTINGS_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    idiom = _settings_producer_idiom("app.config.kafka.KafkaSettings.worker_url")
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.dst == "chan:kafka_topic:${SERVICE_WORKER_URL}"
    # config_ref -- SAME downgrade/placeholder/props convention `env:` already gets
    # (resolve_settings_source produces the IDENTICAL Resolved shape spec.env does).
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert p.props == {"config_ref": "SERVICE_WORKER_URL"}
    assert result.stats["producer_unresolved_channel"] == 0


def test_producer_settings_source_unknown_field_is_unresolved_with_existing_counter():
    """"settings-unknown-class/field -> miss counter not crash" (M7 T2 brief) --
    reuses the EXISTING producer_unresolved_channel counter, no new one."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", SETTINGS_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    idiom = _settings_producer_idiom("app.config.kafka.KafkaSettings.nope")
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_settings_source_unknown_class_is_unresolved_with_existing_counter():
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", SETTINGS_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    idiom = _settings_producer_idiom("app.config.kafka.NopeSettings.step_topic")
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    assert result.edges == []
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_settings_source_no_class_attr_index_wired_is_unresolved_not_crash():
    """No ClassAttrIndex wired at all (ctx.class_attr_index stays the default None,
    e.g. a caller that predates M7 T1/T2) -- honest miss, not a crash."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", SETTINGS_PRODUCER_SRC)
    idiom = _settings_producer_idiom("app.config.kafka.KafkaSettings.step_topic")
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    assert result.edges == []
    assert result.stats["producer_unresolved_channel"] == 1


# -- enum: fan-out (OPEN R2a: over-approximation, documented tradeoff) --

ENUM_PRODUCER_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send("ignored")
'''

ENUM_FANOUT_IDIOM = ProducerIdiom(
    name="p", call="pkg.Client.send",
    channel=ChannelSpec(
        kind="kafka_topic", name_from=ValueSpec(enum_="app.models.enums.KycTopicName"),
    ),
)


def test_producer_enum_fanout_three_members_three_produces_and_channels():
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", ENUM_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[ENUM_FANOUT_IDIOM]), consts)
    use_id = node_ids[_def(ctx, "use").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 3
    expected_names = {
        "kyc.camunda.step_changed.basic_survey",
        "kyc.camunda.step_changed.basic_kyc",
        "kyc.camunda.restrictions_changed",
    }
    assert {e.dst for e in produces} == {f"chan:kafka_topic:{n}" for n in expected_names}
    for e in produces:
        assert e.src == use_id
        assert e.resolution == "heuristic" and e.confidence == 0.8
        # callsite_count always present (CALLS-precedent parity, M7 T2 review
        # Important-2 -- extractors/calls.py's own props={"callsite_count": ...}
        # carries 1 for a single site too, never conditionally absent).
        assert e.props == {"mechanism": "enum_fanout", "callsite_count": 1}
        assert e.extractor == "kafka"
    assert {c.name for c in result.channels} == expected_names
    assert len(result.channels) == 3
    assert result.roles[use_id] == {"MessageProducer"}
    assert result.stats["producers_resolved"] == 3
    assert result.stats["producer_unresolved_channel"] == 0


ENUM_TWO_SITES_SRC = b'''from pkg import Client


def use():
    client = Client()
    client.send("first")
    client.send("second")
'''


def test_producer_enum_fanout_two_call_sites_dedup_to_one_edge_per_pair_with_count():
    """M7 T2 review Important-2 (reviewer-verified evidence-clobber): TWO matched
    call-sites in the SAME function x 3-member enum used to emit 6 PRODUCES edges
    -- 3 PK-identical (src, dst, type, via_channel) pairs -- and staging's INSERT
    OR REPLACE silently kept only the LAST site's evidence. Per the CALLS precedent
    (extractors/calls.py: "aggregated per (src, dst) into one CALLS edge with a
    callsite_count and evidence from the first call site encountered"): exactly 3
    edges, each callsite_count=2, evidence_line of the FIRST site in traversal
    order (facts.calls is source order -- `client.send("first")`, line 6), and one
    channel NodeRec per distinct topic (not re-appended per site)."""
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", ENUM_TWO_SITES_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[ENUM_FANOUT_IDIOM]), consts)
    use_id = node_ids[_def(ctx, "use").index]

    first_site_line = next(
        c for c in ctx.facts.calls if c.callee_name == "send"
    ).start_line
    assert first_site_line == 6  # sanity: the FIRST send call's own source line

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 3
    assert {e.dst for e in produces} == {
        "chan:kafka_topic:kyc.camunda.step_changed.basic_survey",
        "chan:kafka_topic:kyc.camunda.step_changed.basic_kyc",
        "chan:kafka_topic:kyc.camunda.restrictions_changed",
    }
    for e in produces:
        assert e.src == use_id
        assert e.props == {"mechanism": "enum_fanout", "callsite_count": 2}
        assert e.evidence_line == first_site_line
    assert len(result.channels) == 3
    assert result.stats["producers_resolved"] == 3


def test_producer_enum_fanout_unknown_enum_is_unresolved_with_existing_counter():
    relpath = "m.py"
    ctx, node_ids, consts = _load(
        relpath, "svc", ENUM_PRODUCER_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    idiom = ProducerIdiom(
        name="p", call="pkg.Client.send",
        channel=ChannelSpec(
            kind="kafka_topic", name_from=ValueSpec(enum_="app.models.enums.Nope"),
        ),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)

    assert result.edges == []
    assert result.channels == []
    assert result.roles == {}
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_enum_fanout_no_class_attr_index_wired_is_unresolved_not_crash():
    relpath = "m.py"
    ctx, node_ids, consts = _load(relpath, "svc", ENUM_PRODUCER_SRC)
    result = extract_kafka(ctx, node_ids, _idioms(producers=[ENUM_FANOUT_IDIOM]), consts)

    assert result.edges == []
    assert result.stats["producer_unresolved_channel"] == 1


def test_producer_enum_fanout_empty_string_member_skipped_no_crash():
    """Same "M2 final review" empty-name crash guard every other make_channel_node
    call site in this module already carries (`_harvest_enum_values`'s own contract
    only requires a string value, not a non-empty one -- an enum member like
    `NONE = ""` legitimately harvests to an empty string): the one empty member is
    skipped silently, the other two still fan out normally, no crash."""
    relpath = "m.py"
    index_with_empty_member = ClassAttrIndex(
        settings_by_class={},
        enums_by_class={
            "app.models.enums.KycTopicName": ("kyc.a", "", "kyc.b"),
        },
        field_index={},
    )
    ctx, node_ids, consts = _load(
        relpath, "svc", ENUM_PRODUCER_SRC, class_attr_index=index_with_empty_member,
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[ENUM_FANOUT_IDIOM]), consts)

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 2
    assert {e.dst for e in produces} == {"chan:kafka_topic:kyc.a", "chan:kafka_topic:kyc.b"}
    assert len(result.channels) == 2
    assert result.stats["producers_resolved"] == 2


# -- outbox-Event ctor pin (M7 T2 brief): existing call:-matching machinery already
# generically handles a class-constructor FQN pattern (ctor-form, idiom_match.
# _is_ctor_pattern) exactly like any method-form wrapper -- kafka_ext.py itself
# needs NO production change for this; a pure regression pin. --

OUTBOX_EVENT_CTOR_SRC = b'''from app.models.outbox import Event


def place_order():
    event = Event(topic="orders.created", body=b"payload")
    return event
'''


def test_outbox_event_ctor_call_site_matches_producer_idiom_pin():
    """`call: "app.models.outbox.Event"` (a bare class FQN, no method segment) is a
    ctor-form pattern (idiom_match._is_ctor_pattern: last segment capitalized) --
    match_calls' IMPORT_NAME tier already resolves it against a real ctor call-site
    (`Event(topic=..., body=...)`) via `from app.models.outbox import Event`
    from-import evidence, the SAME machinery M6 T4's wrapper-method pin already
    proved for a METHOD-form pattern. The topic itself comes from the ctor's own
    `topic=` kwarg (M6-T4's kwarg source, unmodified)."""
    relpath = "app/services/order.py"
    ctx, node_ids, consts = _load(relpath, "orders-api", OUTBOX_EVENT_CTOR_SRC)
    idiom = ProducerIdiom(
        name="outbox-event-ctor", call="app.models.outbox.Event",
        channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(kwarg="topic")),
    )
    result = extract_kafka(ctx, node_ids, _idioms(producers=[idiom]), consts)
    place_id = node_ids[_def(ctx, "place_order").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    p = produces[0]
    assert p.src == place_id
    assert p.dst == "chan:kafka_topic:orders.created"
    assert p.resolution == "heuristic" and p.confidence == 0.6
    assert result.roles[place_id] == {"MessageProducer"}
    assert result.stats["producers_resolved"] == 1


# -- consumer kind=base_class topic: {settings: ...} (M7 T2) -- allowed alongside
# {attr: ...} (pinned above, UNCHANGED): unlike attr (always an unresolved
# config-reference label), settings needs no call-site either but CAN resolve to a
# real literal from the service-wide ClassAttrIndex -- both remain valid paths.

TOPIC_SETTINGS_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
    topic=ValueSpec(settings="app.config.kafka.KafkaSettings.step_topic"),
)


def test_base_class_topic_settings_with_default_emits_literal_channel_and_containment():
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(
        relpath, "kyc-worker", BASE_CLASS_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    result = extract_kafka(ctx, node_ids, _idioms(consumers=[TOPIC_SETTINGS_IDIOM]), consts)

    assert result.stats["consumers_resolved"] == 1
    contains = [e for e in result.edges if e.type == "CONTAINS"]
    assert len(contains) == 1
    c = contains[0]
    assert c.src == "chan:kafka_topic:kyc.step.changed"
    assert c.dst == "chan:event_type:OCRDataEvent"
    # textual IMPORT_NAME base-match tier (no scip stub, mirrors
    # test_base_class_textual_fallback_tier_without_scip_stub) -- resolved.kind is
    # "value" (a real literal), so _resolution_for keeps the tier's own res/conf.
    assert c.resolution == "heuristic" and c.confidence == 0.6
    assert c.extractor == "kafka"

    topic_chan = next(
        ch for ch in result.channels if ch.id == "chan:kafka_topic:kyc.step.changed"
    )
    assert "unresolved" not in topic_chan.props
    assert "config_ref" not in topic_chan.props
    assert topic_chan.props["channel_kind"] == "kafka_topic"


TOPIC_SETTINGS_NO_DEFAULT_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
    topic=ValueSpec(settings="app.config.kafka.KafkaSettings.worker_url"),
)


def test_base_class_topic_settings_no_default_emits_config_ref_placeholder_channel():
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(
        relpath, "kyc-worker", BASE_CLASS_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    result = extract_kafka(
        ctx, node_ids, _idioms(consumers=[TOPIC_SETTINGS_NO_DEFAULT_IDIOM]), consts,
    )
    contains = [e for e in result.edges if e.type == "CONTAINS"]
    assert len(contains) == 1
    assert contains[0].src == "chan:kafka_topic:${SERVICE_WORKER_URL}"

    topic_chan = next(
        ch for ch in result.channels if ch.id == "chan:kafka_topic:${SERVICE_WORKER_URL}"
    )
    # SAME node-props convention (unresolved=True + config_ref) the pre-existing
    # {attr: ...} path already uses in THIS function specifically (a documented
    # divergence from the rest of the module, M6 T3 review Minor-3) -- kept
    # consistent within this one containment-emission function.
    assert topic_chan.props["unresolved"] is True
    assert topic_chan.props["config_ref"] == "SERVICE_WORKER_URL"


TOPIC_SETTINGS_UNKNOWN_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
    topic=ValueSpec(settings="app.config.kafka.KafkaSettings.nope"),
)


def test_base_class_topic_settings_unknown_field_skips_containment_silently_not_crash():
    """Honest miss: the CONSUMES edge (handler -> event channel) is entirely
    unaffected -- only the OPTIONAL topic-containment pairing on top of it is
    skipped, silently, matching this module's existing "no counter" convention for
    this specific containment path (see _emit_base_class_topic_containment's own
    established precedent -- the pre-existing {attr: ...} path never bumped a
    counter either)."""
    relpath = "app/consumers/ocr.py"
    ctx, node_ids, consts = _load(
        relpath, "kyc-worker", BASE_CLASS_SRC, class_attr_index=KAFKA_SETTINGS_INDEX,
    )
    result = extract_kafka(
        ctx, node_ids, _idioms(consumers=[TOPIC_SETTINGS_UNKNOWN_IDIOM]), consts,
    )
    assert result.stats["consumers_resolved"] == 1
    assert not any(e.type == "CONTAINS" for e in result.edges)
