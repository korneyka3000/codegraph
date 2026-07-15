"""kafka_ext: outbox/dispatch-dict producer+consumer extractor over the idiom DSL (M2 T5).

Three independent idiom shapes, all sourced from the SAME effective ServiceIdioms
(builtin aiokafka/faststream/confluent merged with a service's own custom idioms, see
config.loader.effective_idioms) -- this is the differentiating-niche core: a bare
outbox-repository call or a dispatch-dict registration becomes a first-class PRODUCES/
CONSUMES graph edge, same as a decorator-based route or consumer would.

  - ProducerIdiom (idioms.producers): `idiom_match.match_calls(idiom.call, ...)` finds
    call-sites; `consts.resolve_value_spec` turns the matched ChannelSpec's
    name_from/event_type_from/topic into a concrete channel identity. Two ChannelSpec
    shapes: kafka_topic (one channel, PRODUCES straight into it) and event_type (TWO
    channels -- topic AND event -- PRODUCES goes into the EVENT channel only, with a
    CONTAINS(topic -> event) edge when a topic spec is also present, e.g. the outbox
    idiom's `topic: {const: "orders.events"}`).
  - ConsumerIdiom kind="call" (idioms.consumers): same match_calls machinery, topic
    from idiom.topic straight onto a kafka_topic channel, CONSUMES(enclosing -> chan).
  - ConsumerIdiom kind="dispatch_dict": match_calls on `idiom.registrar_call` locates
    the registration call-site (e.g. `register_handlers({...})`); the FIRST dict-typed
    ArgFact on that call is walked pair by pair -- string keys become event_type
    channels, name/attr values are resolved via `ctx.ref_symbol_lookup` at their own
    T2-supplied byte span (ArgFact.name_start_byte) straight to a handler node id, no
    span math needed (mirrors fastapi_ext's DEPENDS_ON / this module's own qualified_of
    below). kind="decorator" (e.g. builtin faststream's `broker.subscriber`) is NOT
    handled here -- match_decorators only returns raw decorator TEXT, not a CallFact,
    so idiom.topic (a ValueSpec expecting a CallFact to resolve arg/kwarg against) has
    no call-site to resolve against; no current builtin/custom idiom's decorator-kind
    consumer is exercised by any fixture, so this is a documented, deliberate gap
    rather than a fixture-driven implementation (same spirit as fastapi_ext's
    documented degraded-fallback gap for DEPENDS_ON).

Cross-pattern producer dedup is this extractor's OWN responsibility (T3's
match_calls only dedups WITHIN one pattern -- see idiom_match.py's module docstring
and progress.md's carry-forward note for T5): idioms are walked in list order and each
call-site's `callee_start_byte` is claimed by (at most) the FIRST idiom whose pattern
matches it; every later idiom -- producer or the SAME producer list re-run for a
different pattern -- silently skips an already-claimed call. `idioms.consumers`
kind="call" gets its own, independent claim-set (a call can never legitimately be BOTH
a producer and a call-kind consumer, but the same defensive per-call-type dedup is
applied for symmetry/robustness against overlapping custom+builtin idiom patterns).
dispatch_dict registrar-call matches get a third, independent claim-set.

qualified_of (STATIC tier's evidence) mirrors fastapi_ext.py's `_resolve_depends_target`:
ref_symbol_lookup at the CALL's own callee span (CallFact.callee_start_byte/end_byte)
-> SCIP symbol -> `ids.display_qualified` of its descriptors. Real SCIP resolves
aiokafka/confluent/faststream call-sites at this tier in principle; empirically (see
this task's report) none of the fixture files' aiokafka call-sites resolve via SCIP at
all (the packages ship no usable type stubs for pyright), so STATIC never actually
fires there -- RECEIVER/IMPORT_NAME (T3's weaker, structural tiers) do the real work on
these fixtures, and STATIC is proven instead via a stubbed lookup (same "юнит: стаб;
интеграцию покроет T9" pattern as fastapi_ext's DEPENDS_ON).

Value-resolution -> edge resolution/confidence: a `consts.Resolved` of kind "value"
keeps the call match's own tier resolution/confidence as-is; "template" or "config_ref"
downgrades to ("heuristic", min(tier.confidence, 0.6)) -- even a STATIC call match can't
promise an f-string-templated or env-var-driven channel identity is exactly right;
"unresolved" emits NO edge/channel at all (stats-only). "config_ref" additionally
carries `props={"config_ref": <name>}` on the emitted edge. A channel's `name` for
"template"/"config_ref" kinds has no real fixture precedent (every current idiom
resolves to a literal "value") -- "template" uses the resolved template string itself;
"config_ref" uses a "${VAR_NAME}" placeholder, both exercised only by synthetic tests
(see test_kafka_extractor.py) pending a real T6/T9 case.

KafkaResult intentionally has NO `claims` field despite the plan doc's abbreviated
top-line signature `KafkaResult(roles, channels, edges, claims, stats)`: none of this
task's producer/consumer bullets describe kafka ever emitting a claim (unlike temporal's
temporal_start_mark), so an always-empty field would be untested dead weight -- this
mirrors T4's own documented precedent of dropping an unused field from the plan's
abbreviated signature in favor of what the prose actually specifies (progress.md:
"FastapiResult без claims, с node_props -- прозой плана суперсидится top-line
сигнатура"). KafkaResult also has NO `node_props` (nothing here patches per-node props
the way fastapi's http_method/path_template or temporal's workflow_name do --
MessageProducer/MessageConsumer are plain roles, and dispatch is an EDGE prop, not a
node prop).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.core.ids import display_qualified
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.extractors.idiom_match import CallMatch, MatchTier, match_calls
from codegraph.parsing.consts import ConstTable, Resolved, resolve_value_spec
from codegraph.parsing.facts import ArgFact, CallFact
from codegraph.resolvers.scip.symbols import parse_symbol, symbol_to_node_id

from .base import FileContext

_EXTRACTOR = "kafka"


@dataclass(frozen=True)
class KafkaResult:
    roles: dict[str, set[str]]
    channels: list[NodeRec]
    edges: list[EdgeRec]
    stats: dict[str, int]


@dataclass
class _Sink:
    """Mutable accumulator threaded through the extraction helpers below -- bundles the
    four collections every helper needs to append to, so signatures stay short instead
    of repeating `roles, channels, edges, stats` as four separate parameters everywhere."""

    roles: dict[str, set[str]] = field(default_factory=dict)
    channels: list[NodeRec] = field(default_factory=list)
    edges: list[EdgeRec] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=lambda: {
        "producers_resolved": 0,
        "producer_unresolved_channel": 0,
        "producer_missing_node_id": 0,
        "consumers_resolved": 0,
        "consumer_unresolved_topic": 0,
        "consumer_missing_node_id": 0,
        "dispatch_handlers_resolved": 0,
        "dispatch_handler_unresolved": 0,
        "dispatch_dict_missing": 0,
    })

    def add_role(self, node_id: str, role: str) -> None:
        self.roles.setdefault(node_id, set()).add(role)


def _qualified_of(ctx: FileContext):
    """Ref-by-callee-span -> display_qualified chain for match_calls' STATIC tier --
    see module docstring."""

    def fn(call: CallFact) -> str | None:
        if ctx.ref_symbol_lookup is None:
            return None
        sym = ctx.ref_symbol_lookup(ctx.relpath, call.callee_start_byte)
        if sym is None:
            return None
        parsed = parse_symbol(sym)
        if parsed.is_local or parsed.descriptors is None:
            return None
        return display_qualified(parsed.descriptors)

    return fn


def _resolve_ref(ctx: FileContext, start_byte: int | None) -> str | None:
    if start_byte is None or ctx.ref_symbol_lookup is None:
        return None
    sym = ctx.ref_symbol_lookup(ctx.relpath, start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _resolution_for(tier: MatchTier, resolved: Resolved) -> tuple[str, float]:
    if resolved.kind == "value":
        return tier.resolution, tier.confidence
    return "heuristic", min(tier.confidence, 0.6)


def _channel_name(resolved: Resolved) -> str | None:
    if resolved.kind in ("value", "template"):
        return resolved.value
    if resolved.kind == "config_ref":
        return f"${{{resolved.config_ref}}}"
    return None


def _props_for(resolved: Resolved) -> dict:
    return {"config_ref": resolved.config_ref} if resolved.kind == "config_ref" else {}


def _resolve_event_type_from(spec, call: CallFact, consts: ConstTable) -> Resolved:
    """channel.event_type_from is `ValueSpec | Literal["dict_key"]` at the model level
    (shared with ConsumerIdiom's dispatch_dict field) -- "dict_key" has no meaning for
    a ProducerIdiom.channel (there's no dict being iterated at a producer call-site);
    defensively unresolved rather than a resolve_value_spec(str, ...) crash."""
    if not isinstance(spec, ValueSpec):
        return Resolved(kind="unresolved")
    return resolve_value_spec(spec, call, consts)


# -- producers -------------------------------------------------------------------------


def _emit_kafka_topic_produces(
    ctx: FileContext, enclosing_id: str, m: CallMatch, resolved: Resolved, sink: _Sink,
) -> None:
    if resolved.kind == "unresolved":
        sink.stats["producer_unresolved_channel"] += 1
        return
    name = _channel_name(resolved)
    if not name:
        # M2 final review fix: an empty resolved value ("" literal, or an f-string with
        # no static content at all, e.g. f"" -- both resolve to kind="value"/"template"
        # with value="", NOT kind="unresolved") used to reach make_channel_node(name="")
        # below, which raises ValueError ("requires name") -- crashing the whole
        # `codegraph index` run instead of being treated as just another unresolved
        # channel, same as any other resolution failure.
        sink.stats["producer_unresolved_channel"] += 1
        return
    chan = make_channel_node("kafka_topic", name=name)
    resolution, confidence = _resolution_for(m.tier, resolved)
    sink.channels.append(chan)
    sink.edges.append(EdgeRec(
        src=enclosing_id, dst=chan.id, type="PRODUCES",
        resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=m.call.start_line,
        props=_props_for(resolved),
    ))
    sink.add_role(enclosing_id, "MessageProducer")
    sink.stats["producers_resolved"] += 1


def _emit_event_type_produces(
    ctx: FileContext, enclosing_id: str, m: CallMatch, channel: ChannelSpec, consts: ConstTable,
    sink: _Sink,
) -> None:
    event_resolved = _resolve_event_type_from(channel.event_type_from, m.call, consts)
    if event_resolved.kind == "unresolved":
        sink.stats["producer_unresolved_channel"] += 1
        return
    event_name = _channel_name(event_resolved)
    if not event_name:
        # M2 final review fix: same empty-name guard as _emit_kafka_topic_produces --
        # see its comment for why this can't be folded into the "unresolved" branch
        # above (kind="value"/"template" with value="" is a DIFFERENT Resolved shape).
        sink.stats["producer_unresolved_channel"] += 1
        return

    event_chan = make_channel_node("event_type", name=event_name)
    event_res, event_conf = _resolution_for(m.tier, event_resolved)
    sink.channels.append(event_chan)
    sink.edges.append(EdgeRec(
        src=enclosing_id, dst=event_chan.id, type="PRODUCES",
        resolution=event_res, confidence=event_conf, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=m.call.start_line,
        props=_props_for(event_resolved),
    ))
    sink.add_role(enclosing_id, "MessageProducer")
    sink.stats["producers_resolved"] += 1

    if channel.topic is None:
        return
    topic_resolved = resolve_value_spec(channel.topic, m.call, consts)
    if topic_resolved.kind == "unresolved":
        return
    topic_name = _channel_name(topic_resolved)
    if not topic_name:
        # M2 final review fix: same empty-name guard, kept silent (no counter bump) to
        # match this branch's PRE-EXISTING "unresolved topic" convention just above (it
        # never counted a stat either -- the containment edge/topic channel are simply
        # skipped, the already-emitted event PRODUCES edge above is unaffected).
        return
    topic_chan = make_channel_node("kafka_topic", name=topic_name)
    topic_res, topic_conf = _resolution_for(m.tier, topic_resolved)
    sink.channels.append(topic_chan)
    sink.edges.append(EdgeRec(
        src=topic_chan.id, dst=event_chan.id, type="CONTAINS",
        resolution="static" if event_res == "static" and topic_res == "static" else "heuristic",
        confidence=min(event_conf, topic_conf), extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=m.call.start_line,
    ))


def _emit_producer(
    ctx: FileContext, node_ids: dict, idiom: ProducerIdiom, m: CallMatch,
    consts: ConstTable, sink: _Sink,
) -> None:
    enclosing_id = node_ids.get(m.call.enclosing_def)
    if enclosing_id is None:
        sink.stats["producer_missing_node_id"] += 1
        return
    channel = idiom.channel
    if channel.kind == "kafka_topic":
        resolved = (
            resolve_value_spec(channel.name_from, m.call, consts)
            if channel.name_from is not None else Resolved(kind="unresolved")
        )
        _emit_kafka_topic_produces(ctx, enclosing_id, m, resolved, sink)
    elif channel.kind == "event_type":
        _emit_event_type_produces(ctx, enclosing_id, m, channel, consts, sink)
    else:
        sink.stats["producer_unresolved_channel"] += 1  # http_route: not a kafka channel kind


def _extract_producers(
    ctx: FileContext, node_ids: dict, idioms: list[ProducerIdiom], consts: ConstTable, sink: _Sink,
) -> None:
    qualified_of = _qualified_of(ctx)
    claimed_starts: set[int] = set()
    for idiom in idioms:
        for m in match_calls(idiom.call, ctx.facts, qualified_of):
            if m.call.callee_start_byte in claimed_starts:
                continue
            claimed_starts.add(m.call.callee_start_byte)
            _emit_producer(ctx, node_ids, idiom, m, consts, sink)


# -- consumers: kind="call" --------------------------------------------------------------


def _emit_call_consumer(
    ctx: FileContext, node_ids: dict, idiom: ConsumerIdiom, m: CallMatch,
    consts: ConstTable, sink: _Sink,
) -> None:
    enclosing_id = node_ids.get(m.call.enclosing_def)
    if enclosing_id is None:
        sink.stats["consumer_missing_node_id"] += 1
        return
    if idiom.topic is None:
        sink.stats["consumer_unresolved_topic"] += 1
        return
    resolved = resolve_value_spec(idiom.topic, m.call, consts)
    if resolved.kind == "unresolved":
        sink.stats["consumer_unresolved_topic"] += 1
        return
    name = _channel_name(resolved)
    if not name:
        # M2 final review fix: same empty-name guard as the producer side (see
        # _emit_kafka_topic_produces) -- an empty resolved topic value must not crash
        # make_channel_node, and is exactly as "unresolved" as any other failed
        # resolution for the purposes of this counter.
        sink.stats["consumer_unresolved_topic"] += 1
        return

    chan = make_channel_node("kafka_topic", name=name)
    resolution, confidence = _resolution_for(m.tier, resolved)
    sink.channels.append(chan)
    sink.edges.append(EdgeRec(
        src=enclosing_id, dst=chan.id, type="CONSUMES",
        resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=m.call.start_line,
        props={"dispatch": "topic"},
    ))
    sink.add_role(enclosing_id, "MessageConsumer")
    sink.stats["consumers_resolved"] += 1


def _extract_call_consumers(
    ctx: FileContext, node_ids: dict, idioms: list[ConsumerIdiom], consts: ConstTable, sink: _Sink,
) -> None:
    qualified_of = _qualified_of(ctx)
    claimed_starts: set[int] = set()
    for idiom in idioms:
        if idiom.kind != "call" or idiom.call is None:
            continue
        for m in match_calls(idiom.call, ctx.facts, qualified_of):
            if m.call.callee_start_byte in claimed_starts:
                continue
            claimed_starts.add(m.call.callee_start_byte)
            _emit_call_consumer(ctx, node_ids, idiom, m, consts, sink)


# -- consumers: kind="dispatch_dict" -----------------------------------------------------


def _emit_dispatch_dict(
    ctx: FileContext, idiom: ConsumerIdiom, m: CallMatch, consts: ConstTable, sink: _Sink,
) -> None:
    dict_arg = next((a for a in m.call.args if a.value_kind == "dict"), None)
    if dict_arg is None or not dict_arg.dict_items:
        sink.stats["dispatch_dict_missing"] += 1
        return

    resolved_events: list[NodeRec] = []
    for key_arg, value_arg in dict_arg.dict_items:
        # `not key_arg.string_value` (M2 final review fix) rather than `is None`:
        # catches an empty-string key ("": handler) too, which used to reach
        # make_channel_node(name="") below and raise ValueError -- same empty-name
        # crash class as the producer/consumer paths above, just via a dict key
        # literal instead of a Resolved value.
        if key_arg.value_kind != "string" or not key_arg.string_value:
            continue
        handler_id = _resolve_dispatch_handler(ctx, value_arg)
        if handler_id is None:
            sink.stats["dispatch_handler_unresolved"] += 1
            continue

        event_chan = make_channel_node("event_type", name=key_arg.string_value)
        sink.channels.append(event_chan)
        sink.edges.append(EdgeRec(
            src=handler_id, dst=event_chan.id, type="CONSUMES",
            resolution=m.resolution, confidence=m.confidence, extractor=_EXTRACTOR,
            evidence_file=ctx.relpath, evidence_line=m.call.start_line,
            props={"dispatch": "event_type"},
        ))
        sink.add_role(handler_id, "MessageConsumer")
        sink.stats["dispatch_handlers_resolved"] += 1
        resolved_events.append(event_chan)

    if idiom.topic is None or not resolved_events:
        return
    topic_resolved = resolve_value_spec(idiom.topic, m.call, consts)
    if topic_resolved.kind == "unresolved":
        return
    topic_name = _channel_name(topic_resolved)
    if not topic_name:
        # M2 final review fix: same empty-name guard, silent like the "unresolved"
        # branch just above it (this topic-containment pairing is best-effort on top
        # of the already-emitted per-event CONSUMES edges, same convention as
        # _emit_event_type_produces' own topic_chan guard).
        return
    topic_chan = make_channel_node("kafka_topic", name=topic_name)
    sink.channels.append(topic_chan)
    topic_res, topic_conf = _resolution_for(m.tier, topic_resolved)
    for event_chan in resolved_events:
        sink.edges.append(EdgeRec(
            src=topic_chan.id, dst=event_chan.id, type="CONTAINS",
            resolution=topic_res, confidence=topic_conf, extractor=_EXTRACTOR,
            evidence_file=ctx.relpath, evidence_line=m.call.start_line,
        ))


def _resolve_dispatch_handler(ctx: FileContext, value_arg: ArgFact) -> str | None:
    if value_arg.value_kind not in ("name", "attr"):
        return None
    return _resolve_ref(ctx, value_arg.name_start_byte)


def _extract_dispatch_dict_consumers(
    ctx: FileContext, idioms: list[ConsumerIdiom], consts: ConstTable, sink: _Sink,
) -> None:
    qualified_of = _qualified_of(ctx)
    claimed_starts: set[int] = set()
    for idiom in idioms:
        if idiom.kind != "dispatch_dict" or idiom.registrar_call is None:
            continue
        for m in match_calls(idiom.registrar_call, ctx.facts, qualified_of):
            if m.call.callee_start_byte in claimed_starts:
                continue
            claimed_starts.add(m.call.callee_start_byte)
            _emit_dispatch_dict(ctx, idiom, m, consts, sink)


# -- entry point -------------------------------------------------------------------------


def extract_kafka(
    ctx: FileContext, node_ids: dict[int, str], idioms: ServiceIdioms, consts: ConstTable,
) -> KafkaResult:
    sink = _Sink()

    _extract_producers(ctx, node_ids, idioms.producers, consts, sink)
    _extract_call_consumers(ctx, node_ids, idioms.consumers, consts, sink)
    _extract_dispatch_dict_consumers(ctx, idioms.consumers, consts, sink)

    return KafkaResult(roles=sink.roles, channels=sink.channels, edges=sink.edges, stats=sink.stats)
