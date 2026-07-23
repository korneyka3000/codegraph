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

M6 T3 (GAPS §4/pilot gap 4 -- CONSUMES=0 on shared-lib `BaseConsumer[Event]`
subclasses): a FOURTH, independent ConsumerIdiom shape, kind="base_class" -- a class
whose bases contain a subscript `Base[...]` resolving (scip ref-lookup on the base
name token, falling back to a textual FQN-suffix match at idiom_match's IMPORT_NAME
tier confidence when scip has no opinion) to `idiom.base_class` marks its OWN
`handler_method` (not the class, not ctor/setup) MessageConsumer; CONSUMES goes
handler_method -> Channel(event_type from the subscript's generic_arg-th argument,
`idiom.event_type_from` a GenericArgSpec). Unlike the other three shapes, there is no
CallFact call-site at all to match_calls/resolve_value_spec against -- base
resolution and generic-argument extraction instead walk the class's OWN bases via
`_scan_class_bases`, a SEPARATE tree-sitter pass over `ctx.source` (mirrors
ConstTable.build's own independent top-level walk, see parsing/consts.py) keyed by
(name_start_byte, name_end_byte), the same join key FileFacts' DefFact.name_start_byte/
name_end_byte already carries. A class matching the base textually/structurally but
whose base has NO subscript (bare `class C(BaseConsumer):`) is an honest miss, not a
crash: no claim, `consumer_base_class_no_generic` stat bump (surfaced in the
per-service report, pipeline/analyze.py, same as T2's http_* counters). `idiom.topic`,
when given, only ever carries `.attr` (enforced at config load,
config/models.py ConsumerIdiom._kind_requirements) -- there is no call-site to resolve
const/arg/kwarg/env against here either, so it is treated as an ALWAYS-unresolved
config-ref straight through the existing `Resolved(kind="config_ref")` /
`_resolution_for` / `_channel_name` machinery (no new resolution kind needed): an
unresolved kafka_topic Channel(unresolved=True, config_ref=<attr text>) +
CONTAINS(topic -> event), same edge shape _emit_event_type_produces already builds
for its own topic/event pairing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatchcase

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    GenericArgSpec,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.core.ids import display_qualified
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.extractors.idiom_match import CallMatch, MatchTier, match_calls
from codegraph.parsing.consts import ConstTable, Resolved, resolve_value_spec
from codegraph.parsing.facts import ArgFact, CallFact, DefFact
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
        # M6 T3: honest-miss counter -- a class matches base_class's target base
        # (textually or via scip) but has no usable generic argument at all (bare
        # `class C(Base):`, or event_type_from.generic_arg indexes past the end of
        # what the subscript actually carries) -- no claim, not a crash. See
        # pipeline/analyze.py for how this flows into the per-service report (same
        # precedent as T2's http_* failure counters).
        "consumer_base_class_no_generic": 0,
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


# -- consumers: kind="base_class" --------------------------------------------------------
#
# No CallFact call-site exists for this shape at all (the "call site" is a CLASS
# DEFINITION), so none of match_calls/resolve_value_spec applies -- base resolution and
# generic-argument extraction instead walk the class's own bases directly, via a
# SEPARATE tree-sitter pass (`_scan_class_bases`) over ctx.source. FileFacts'
# DefFact.base_exprs (parsing/facts.py, M6 T3) carries only each base's raw TEXT --
# sufficient for the honest-miss/no-bases prefilter below, but NOT the base name
# token's absolute byte position ctx.ref_symbol_lookup (an occurrence table keyed by
# byte offset) needs for the STATIC tier -- same "own independent walk" reasoning as
# ConstTable.build (parsing/consts.py's own module docstring).


@dataclass(frozen=True)
class _ClassBaseToken:
    """One non-keyword base expression of a class_definition, located by
    `_scan_class_bases`. name_start_byte/name_end_byte/name_text already point at the
    LAST identifier segment for an attribute-form base (`pkgmod.BaseConsumer` -> just
    "BaseConsumer") -- mirrors ArgFact's own "attr" value_kind span convention
    (parsing/facts.py's `_build_argfact`). generic_arg_texts is only meaningful when
    is_subscript is True; each entry is likewise already reduced to its last
    identifier segment if it was itself an attribute chain (`Base[evtmod.Event]` ->
    "Event") -- brief: "attribute chain -> last identifier"."""

    is_subscript: bool
    name_start_byte: int
    name_end_byte: int
    name_text: str
    generic_arg_texts: tuple[str, ...] = ()


def _base_token_name(node):
    """`node` is a base expr itself (identifier/attribute) or a subscript's `value`
    field -- returns the tree-sitter node to take name_text/byte-span from (the
    LAST identifier segment for an attribute), or None for an unrecognized shape."""
    if node.type == "identifier":
        return node
    if node.type == "attribute":
        return node.child_by_field_name("attribute")
    return None


def _generic_arg_text(node) -> str:
    if node.type == "attribute":
        last = node.child_by_field_name("attribute")
        if last is not None:
            return last.text.decode("utf-8", errors="replace")
    return node.text.decode("utf-8", errors="replace")


def _scan_class_bases(source: bytes) -> dict[tuple[int, int], list[_ClassBaseToken]]:
    """Every class_definition's non-keyword bases in `source`, keyed by
    (name_start_byte, name_end_byte) of the CLASS's own name token -- the exact join
    key DefFact.name_start_byte/name_end_byte already carries, so a class_def can look
    its own bases up here by identity, with no dependence on traversal-order parity
    between this walk and build_file_facts' own. Grammar facts (tree-sitter-python
    0.25, verified via probe script): a class_definition's bases live under its
    "superclasses" field (an argument_list, entirely ABSENT for a bare `class C:`);
    each base is identifier / attribute / subscript (generic, itself `value`= the
    base expr + repeated `subscript`= fields for each bracket item, e.g. "Base[A, B]")
    / keyword_argument ("metaclass=X" and similar -- not a base, skipped)."""
    from codegraph.parsing.ts import parse

    tree = parse(source)
    result: dict[tuple[int, int], list[_ClassBaseToken]] = {}

    def visit(node) -> None:
        if node.type == "class_definition":
            name_node = node.child_by_field_name("name")
            supers = node.child_by_field_name("superclasses")
            if name_node is not None and supers is not None:
                tokens: list[_ClassBaseToken] = []
                for base in supers.named_children:
                    if base.type == "keyword_argument":
                        continue
                    if base.type == "subscript":
                        value = base.child_by_field_name("value")
                        name_tok = _base_token_name(value) if value is not None else None
                        if name_tok is None:
                            continue  # unrecognized base-expr shape -- no fixture needs it
                        arg_texts = tuple(
                            _generic_arg_text(a)
                            for a in base.children_by_field_name("subscript")
                        )
                        tokens.append(_ClassBaseToken(
                            is_subscript=True,
                            name_start_byte=name_tok.start_byte, name_end_byte=name_tok.end_byte,
                            name_text=name_tok.text.decode("utf-8", errors="replace"),
                            generic_arg_texts=arg_texts,
                        ))
                    else:
                        name_tok = _base_token_name(base)
                        if name_tok is None:
                            continue
                        tokens.append(_ClassBaseToken(
                            is_subscript=False,
                            name_start_byte=name_tok.start_byte, name_end_byte=name_tok.end_byte,
                            name_text=name_tok.text.decode("utf-8", errors="replace"),
                        ))
                result[(name_node.start_byte, name_node.end_byte)] = tokens
        for child in node.children:
            visit(child)

    visit(tree.root_node)
    return result


def _match_base_tier(
    ctx: FileContext, tok: _ClassBaseToken, base_class_fqn: str,
) -> MatchTier | None:
    """STATIC (scip ref-lookup at the base name token's own span -> display_qualified,
    fnmatchcase against base_class_fqn -- same glob forms idiom_match._match_static
    uses) tried first; IMPORT_NAME fallback (bare name-text equality against the
    configured FQN's last segment) when scip has no opinion (lookup unwired, symbol
    not found, or a local/unparseable symbol) -- mirrors match_calls' own
    "first tier that fires wins" priority (idiom_match.py). RECEIVER has no meaning
    here at all (no call-site/receiver, only a class's own base expression)."""
    if ctx.ref_symbol_lookup is not None:
        sym = ctx.ref_symbol_lookup(ctx.relpath, tok.name_start_byte)
        if sym is not None:
            parsed = parse_symbol(sym)
            if not parsed.is_local and parsed.descriptors is not None:
                qualified = display_qualified(parsed.descriptors)
                if fnmatchcase(qualified, base_class_fqn) or fnmatchcase(
                    qualified, "*." + base_class_fqn
                ):
                    return MatchTier.STATIC
    last_segment = base_class_fqn.rsplit(".", 1)[-1]
    if tok.name_text == last_segment:
        return MatchTier.IMPORT_NAME
    return None


def _first_base_match(
    ctx: FileContext, tokens: list[_ClassBaseToken], base_class_fqn: str,
) -> tuple[_ClassBaseToken, MatchTier] | None:
    for tok in tokens:
        tier = _match_base_tier(ctx, tok, base_class_fqn)
        if tier is not None:
            return tok, tier
    return None


def _emit_base_class_topic_containment(
    ctx: FileContext, idiom: ConsumerIdiom, tier: MatchTier, event_chan: NodeRec,
    evidence_line: int, sink: _Sink,
) -> None:
    if idiom.topic is None or idiom.topic.attr is None:
        return  # config/models.py's _kind_requirements guarantees .attr when topic is set
    # No call-site to resolve against at all (unlike the call/dispatch_dict consumer
    # paths) -- idiom.topic.attr is a config-reference LABEL, not something to
    # resolve; reuses the EXISTING "config_ref" Resolved shape (and its established
    # always-downgrades-to-heuristic/<=0.6 convention, `_resolution_for`) directly.
    resolved = Resolved(kind="config_ref", config_ref=idiom.topic.attr)
    resolution, confidence = _resolution_for(tier, resolved)
    topic_chan = make_channel_node(
        "kafka_topic", name=_channel_name(resolved),
        unresolved=True, config_ref=idiom.topic.attr,
    )
    sink.channels.append(topic_chan)
    sink.edges.append(EdgeRec(
        src=topic_chan.id, dst=event_chan.id, type="CONTAINS",
        resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=evidence_line,
    ))


def _emit_base_class_consumer(
    ctx: FileContext, node_ids: dict, class_def: DefFact, idiom: ConsumerIdiom,
    tok: _ClassBaseToken, tier: MatchTier, sink: _Sink,
) -> None:
    if not tok.is_subscript:
        sink.stats["consumer_base_class_no_generic"] += 1
        return

    # config/models.py's _kind_requirements guarantees event_type_from is a
    # GenericArgSpec whenever kind="base_class" validates at all.
    assert isinstance(idiom.event_type_from, GenericArgSpec)
    idx = idiom.event_type_from.generic_arg
    if idx < 0 or idx >= len(tok.generic_arg_texts):
        sink.stats["consumer_base_class_no_generic"] += 1
        return
    event_name = tok.generic_arg_texts[idx]

    handler_def = next(
        (d for d in ctx.facts.defs
         if d.kind == "function" and d.parent == class_def.index
         and d.name == idiom.handler_method),
        None,
    )
    handler_id = node_ids.get(handler_def.index) if handler_def is not None else None
    if handler_id is None:
        # Covers BOTH "handler_method doesn't exist on this class at all" (renamed/
        # removed) and "it exists but node_ids has no entry for it" -- both are, from
        # this extractor's point of view, "nowhere to put the CONSUMES edge",
        # the same defensive counter the call/dispatch_dict consumer paths already
        # use for their own missing-node-id case.
        sink.stats["consumer_missing_node_id"] += 1
        return

    event_chan = make_channel_node("event_type", name=event_name)
    sink.channels.append(event_chan)
    sink.edges.append(EdgeRec(
        src=handler_id, dst=event_chan.id, type="CONSUMES",
        resolution=tier.resolution, confidence=tier.confidence, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=handler_def.start_line,
        props={"dispatch": "event_type"},
    ))
    sink.add_role(handler_id, "MessageConsumer")
    sink.stats["consumers_resolved"] += 1

    _emit_base_class_topic_containment(ctx, idiom, tier, event_chan, handler_def.start_line, sink)


def _extract_base_class_consumers(
    ctx: FileContext, node_ids: dict, idioms: list[ConsumerIdiom], sink: _Sink,
) -> None:
    base_idioms = [i for i in idioms if i.kind == "base_class"]
    if not base_idioms:
        return
    class_defs = [d for d in ctx.facts.defs if d.kind == "class" and d.base_exprs]
    if not class_defs:
        return
    base_tokens = _scan_class_bases(ctx.source)

    for class_def in class_defs:
        tokens = base_tokens.get((class_def.name_start_byte, class_def.name_end_byte), [])
        if not tokens:
            continue
        for idiom in base_idioms:
            # config/models.py's _kind_requirements guarantees base_class is set
            # whenever kind="base_class" validates at all -- narrows the field's
            # `str | None` type for the checker without changing runtime behavior
            # (same precedent as pipeline/analyze.py's own `assert consts is not None`).
            assert idiom.base_class is not None
            match = _first_base_match(ctx, tokens, idiom.base_class)
            if match is None:
                continue
            tok, tier = match
            _emit_base_class_consumer(ctx, node_ids, class_def, idiom, tok, tier, sink)


# -- entry point -------------------------------------------------------------------------


def extract_kafka(
    ctx: FileContext, node_ids: dict[int, str], idioms: ServiceIdioms, consts: ConstTable,
) -> KafkaResult:
    sink = _Sink()

    _extract_producers(ctx, node_ids, idioms.producers, consts, sink)
    _extract_call_consumers(ctx, node_ids, idioms.consumers, consts, sink)
    _extract_dispatch_dict_consumers(ctx, idioms.consumers, consts, sink)
    _extract_base_class_consumers(ctx, node_ids, idioms.consumers, sink)

    return KafkaResult(roles=sink.roles, channels=sink.channels, edges=sink.edges, stats=sink.stats)
