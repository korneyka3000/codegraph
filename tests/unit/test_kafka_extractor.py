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

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.extractors.base import FileContext
from codegraph.extractors.kafka_ext import KafkaResult, extract_kafka
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.consts import ConstTable
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"


def _fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


def _load(relpath: str, service: str, source: bytes, *, ref_symbol_lookup=None):
    """Builds (ctx, node_ids, consts) exactly as analyze.py's S5 wiring will: node_ids
    is def-index -> resolved node id (from python_core's own per-file output, Module
    node first then exactly one node per facts.defs entry, same order) PLUS a
    None -> Module-node-id entry (CallFact.enclosing_def is None for module-level
    calls -- the same sentinel, so `node_ids.get(call.enclosing_def)` transparently
    falls back to the Module id with no special-casing in the extractor itself)."""
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
        ref_symbol_lookup=ref_symbol_lookup,
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
