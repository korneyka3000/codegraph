"""Pydantic-модели конфига: workspace, сервисы и Idiom-DSL.

Идиомы — данные: builtin-идиомы (builtin_idioms.py) — экземпляры этих же моделей,
поэтому пользователь может описать любой паттерн в codegraph.yaml без изменения кода.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_BUILTIN_IDIOMS = [
    "fastapi",
    "aiokafka",
    "faststream",
    "confluent",
    "temporal",
    "aiohttp_client",
]


class _Strict(BaseModel):
    # M7 T2: populate_by_name=True (alongside extra="forbid"/frozen=True, unchanged)
    # -- purely additive, verified on pydantic 2.13 to emit no deprecation warning --
    # lets a field defined with an `alias=` (ValueSpec.enum_ below, the first field
    # in this whole DSL to need one) be populated EITHER by its Python attribute name
    # (`ValueSpec(enum_=...)`, every direct-construction call site in this codebase's
    # own extractors/tests) OR by the alias (`ValueSpec.model_validate({"enum": ...})`,
    # the natural YAML/DSL surface -- "enum" reads far better than "enum_" in a
    # codegraph.yaml). Every OTHER existing field in every model below has no alias
    # at all, so this is a no-op for all of them -- both spellings already coincided.
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ValueSpec(_Strict):
    """Откуда берётся строковое значение (имя топика, тип события, base_url...).

    Ровно один источник: литерал, позиционный аргумент, kwarg, env-переменная,
    атрибут объекта, pydantic-Settings поле (M7 T2) или Enum-класс (M7 T2, только
    там, где источник это явно допускает -- см. ChannelSpec/ConsumerIdiom).
    """

    const: str | None = None
    arg: int | None = None
    kwarg: str | None = None
    env: str | None = None
    attr: str | None = None
    # M7 T2 (OPEN R2): "<ClassFQN>.<field>" -- split on the LAST dot (parsing/
    # consts.py's resolve_settings_source) -- ClassAttrIndex.settings_field (M7 T1)
    # lookup. Unlike every other source above, resolution needs NO call-site at all
    # (a pure class-body literal lookup), which is exactly why it's also sanctioned
    # for consumer kind=base_class's topic field (no CallFact there either) --
    # see ConsumerIdiom._kind_requirements below.
    settings: str | None = None
    # M7 T2 (OPEN R2a): enum-class FQN -- ClassAttrIndex.enum_values (M7 T1) lookup.
    # Field name is `enum_`, not `enum` (`enum` alone shadows nothing HERE, but reads
    # oddly as a bare attribute name right next to Python's own `enum` stdlib module
    # this file could otherwise want to import, and "trailing underscore to dodge a
    # near-keyword" is an established, unsurprising convention) -- the pydantic
    # `alias="enum"` keeps the YAML/DSL surface exactly the natural `enum:
    # "app.enums.KycTopicName"` (see _Strict's populate_by_name comment above for how
    # BOTH spellings populate this one field). Unlike every other source, an enum
    # does not name a SINGLE value at all -- kafka_ext.py fans it out into one
    # PRODUCES edge/channel PER member -- so its placement is fail-closed to the ONE
    # field that sanctions (and implements) that over-approximation:
    # ChannelSpec.name_from, a producer kafka_topic channel's own identity.
    # ChannelSpec.event_type_from, ChannelSpec.topic (M7 T2 review Important-1: no
    # fan-out is implemented for the CONTAINS-pairing field, so an enum there was a
    # validated-but-dead cell) and every ConsumerIdiom ValueSpec field all reject it
    # at config-load time (see those models' own validators below) rather than
    # silently doing something nobody asked for.
    enum_: str | None = Field(default=None, alias="enum")

    @model_validator(mode="after")
    def _exactly_one(self) -> ValueSpec:
        set_fields = [
            f for f in ("const", "arg", "kwarg", "env", "attr", "settings", "enum_")
            if getattr(self, f) is not None
        ]
        if len(set_fields) != 1:
            raise ValueError(f"ValueSpec requires exactly one source, got: {set_fields}")
        return self


def _is_enum_source(spec: object) -> bool:
    """Shared guard: `spec` is a ValueSpec carrying an enum_ source. Used by both
    ChannelSpec and ConsumerIdiom's own fail-closed validators below (M7 T2) --
    `event_type_from`'s wider `ValueSpec | Literal["dict_key"] | GenericArgSpec`
    union means every caller needs the SAME isinstance-narrowing first; one shared
    predicate keeps the two checks (and any future one) from silently drifting."""
    return isinstance(spec, ValueSpec) and spec.enum_ is not None


class GenericArgSpec(_Strict):
    """M6 T3 (GAPS §4/pilot gap 4): base_class-kind ConsumerIdiom's event_type
    source -- the Nth subscript argument of the matched base class (`Base[EventA]`
    -> generic_arg: 0 == EventA; `Base[K, V]` -> generic_arg: 1 == V). The ONLY
    EventTypeFrom shape kind="base_class" accepts (enforced by
    ConsumerIdiom._kind_requirements): ValueSpec's arg/kwarg/const/env/attr and
    dispatch_dict's "dict_key" all presuppose a CallFact call-site, which a class
    definition never has. ge=0 (M6 T3 review Minor-4): a negative index has no
    generic-param semantics and could only be a config typo -- rejected at load
    time instead of silently producing zero claims forever (the extractor's own
    defensive range guard would treat it as a permanent honest miss).
    """

    generic_arg: int = Field(default=0, ge=0)


EventTypeFrom = ValueSpec | Literal["dict_key"] | GenericArgSpec


class ChannelSpec(_Strict):
    kind: Literal["kafka_topic", "event_type", "http_route"]
    name_from: ValueSpec | None = None
    topic: ValueSpec | None = None
    event_type_from: EventTypeFrom | None = None

    @model_validator(mode="after")
    def _enum_only_on_topic_identity(self) -> ChannelSpec:
        # M7 T2 (OPEN R2a): enum's fan-out over-approximation is sanctioned -- and
        # implemented (kafka_ext._emit_enum_fanout_produces) -- ONLY for name_from,
        # the kafka_topic kind's own channel identity. event_type_from names the
        # PRODUCED EVENT's type, a different identity fanning out has no sanctioned
        # semantics for (see ValueSpec.enum_'s own docstring) -- rejected at
        # config-load time.
        if _is_enum_source(self.event_type_from):
            raise ValueError(
                "ChannelSpec.event_type_from does not support an enum source -- enum "
                "fan-out is sanctioned only for a producer's topic identity "
                f"(name_from); got {self.event_type_from!r}"
            )
        # M7 T2 review Important-1 (fail-closed): an enum on `topic` is structurally
        # GUARANTEED inert -- the event_type kind's CONTAINS-pairing resolution goes
        # through resolve_value_spec, whose enum_ branch is unconditionally
        # unresolved (kafka_ext implements fan-out only for name_from), and the
        # kafka_topic kind never reads `topic` at all -- so it could only ever be a
        # validated-but-dead config cell (zero edges, zero counters, forever).
        # Unconditional (not event_type-kind-gated), mirroring the
        # event_type_from check's own unconditional shape just above.
        if _is_enum_source(self.topic):
            raise ValueError(
                "ChannelSpec.topic does not support an enum source -- enum fan-out is "
                "implemented only for name_from (the kafka_topic kind's channel "
                f"identity); an enum here would be structurally inert; got {self.topic!r}"
            )
        return self


class ProducerIdiom(_Strict):
    name: str
    call: str  # qualified glob вызываемого: "app.db.outbox.OutboxRepository.add_event"
    channel: ChannelSpec


class ConsumerIdiom(_Strict):
    """M3 T1 breaking change: `dict_assign` (an alternate dispatch_dict discovery
    mechanism -- matching a module-level `EVENT_HANDLERS = {...}` dict ASSIGNMENT
    instead of a registrar CALL site) has been REMOVED. It was accepted and validated
    since M0 (kind="dispatch_dict" could be satisfied by either registrar_call OR
    dict_assign) but never actually consumed -- kafka_ext.py's dispatch_dict handling
    (`_extract_dispatch_dict_consumers`) has only ever matched on `idiom.registrar_call`
    via `match_calls`; a config that set ONLY dict_assign validated successfully but
    silently produced zero CONSUMES edges, a "validated dead path" flagged in M2's
    progress.md backlog. dispatch_dict now requires registrar_call unconditionally --
    a codegraph.yaml still specifying `dict_assign:` fails loading with pydantic's
    extra="forbid" ValidationError (unknown field), a loud signal to migrate to
    registrar_call, instead of the previous silent no-op coverage gap.

    M6 T3 (GAPS §4/pilot gap 4 -- CONSUMES=0 on shared-lib `BaseConsumer[Event]`
    subclasses): kind="base_class" -- a class whose bases contain a subscript
    `Base[...]` resolving to `base_class` (FQN) marks its OWN `handler_method` (NOT
    the class itself, not ctor/setup) role MessageConsumer; the CONSUMES edge goes
    handler_method -> Channel(event_type from the subscript's generic_arg-th
    argument). Requires base_class + handler_method + event_type_from all three
    (fail-closed all-or-nothing, same precedent as HttpClientIdiom's route_from
    matrix, eff706e) -- event_type_from must be a GenericArgSpec (a ValueSpec/
    dict_key source presupposes a call-site, which a class definition never has).
    `topic`, if given, only supports {attr: ...}: there is no call-site to resolve
    const/arg/kwarg/env against either -- it names a config-reference LABEL (e.g.
    "self.config.topic"), always emitted as an unresolved kafka_topic Channel +
    CONTAINS(topic -> event). See extractors/kafka_ext.py for the extraction side.
    """

    name: str
    kind: Literal["call", "decorator", "dispatch_dict", "base_class"]
    call: str | None = None
    decorator: str | None = None
    registrar_call: str | None = None
    topic: ValueSpec | None = None
    event_type_from: EventTypeFrom | None = None
    base_class: str | None = None
    handler_method: str | None = None

    @model_validator(mode="after")
    def _kind_requirements(self) -> ConsumerIdiom:
        # M6 T3: base_class needs ALL THREE of (base_class, handler_method,
        # event_type_from) -- call/decorator/dispatch_dict each name exactly ONE
        # required field, so switching this check from "any" to "all" changes
        # nothing for them (a 1-tuple's any/all coincide) while giving base_class's
        # 3-tuple the fail-closed all-or-nothing shape (HttpClientIdiom precedent).
        required = {
            "call": ("call",),
            "decorator": ("decorator",),
            "dispatch_dict": ("registrar_call",),
            "base_class": ("base_class", "handler_method", "event_type_from"),
        }[self.kind]
        missing = [f for f in required if getattr(self, f) is None]
        if missing:
            raise ValueError(f"consumer kind={self.kind} requires {required}, missing {missing}")
        if self.kind == "base_class":
            if not isinstance(self.event_type_from, GenericArgSpec):
                raise ValueError(
                    "consumer kind=base_class requires event_type_from={generic_arg: N} "
                    "(GenericArgSpec) -- ValueSpec/dict_key presuppose a call-site a class "
                    f"definition never has; got {self.event_type_from!r}"
                )
            # M7 T2: {settings: ...} joins {attr: ...} as the second (and, per
            # ValueSpec.enum_'s own docstring, ONLY the second) meaningful shape
            # here -- both need no call-site (attr is always an unresolved
            # config-reference LABEL; settings resolves from the service-wide
            # ClassAttrIndex alone, see parsing/consts.py's resolve_settings_source)
            # -- const/arg/kwarg/env/enum all still have nothing to resolve against
            # (or, for enum, no sanctioned fan-out semantics on the consumer side --
            # see the dedicated _enum_forbidden_on_consumers validator below, which
            # independently rejects it too).
            if self.topic is not None and self.topic.attr is None and self.topic.settings is None:
                raise ValueError(
                    "consumer kind=base_class topic only supports {attr: ...} or "
                    "{settings: ...} -- there is no call-site to resolve const/arg/kwarg/"
                    f"env/enum against here; got {self.topic!r}"
                )
        return self

    @model_validator(mode="after")
    def _enum_forbidden_on_consumers(self) -> ConsumerIdiom:
        # M7 T2 (OPEN R2a / brief: "consumer topic/event_type_from with enum ->
        # config error"): a consumer handler consumes ONE topic/event -- there is no
        # analogous "fan out CONSUMES to every enum member" semantics the producer
        # side's over-approximation could justify here. Checked for every kind
        # uniformly (not just base_class, whose OWN topic validator above already
        # independently excludes enum via its attr-or-settings allowlist) --
        # kind="call"'s topic and kind="dispatch_dict"'s event_type_from have no
        # other guard against it at all.
        if _is_enum_source(self.topic):
            raise ValueError(
                f"ConsumerIdiom.topic does not support an enum source; got {self.topic!r}"
            )
        if _is_enum_source(self.event_type_from):
            raise ValueError(
                "ConsumerIdiom.event_type_from does not support an enum source; got "
                f"{self.event_type_from!r}"
            )
        return self


class BaseUrlSpec(_Strict):
    attr: str | None = None
    env: str | None = None
    # M7 T3 (OPEN R1): explicit alternative to `env` -- "<ClassFQN>.<field>" (same
    # shape as ValueSpec.settings, M7 T2). http_client_ext.py resolves it via
    # ClassAttrIndex.settings_field (a PER-CLASS lookup -- unlike the self.host
    # auto-anchor's service-wide, ambiguity-prone field_by_name join, naming the
    # class removes any collision risk) and takes priority over auto-anchoring,
    # exactly like `env` already does. No "exactly one of attr/env/settings"
    # validator here (unlike ValueSpec) -- `attr` documents WHERE the base-url text
    # comes from structurally (a fact this extractor does not even read back today),
    # `env`/`settings` separately anchor the TARGET service; fixtures/workspace.yaml
    # already relies on attr+env coexisting (`{attr: "self._base_url", env: ...}`).
    settings: str | None = None


class HttpRouteFromSpec(_Strict):
    """M6 T2: откуда брать path_template — декоратор метода (напр. `@path_template(...)`),
    а не arg0 самого HTTP-вызова. `arg` — позиция строкового аргумента ВНУТРИ декоратора
    (обычно 0 — единственный позиционный арг маршрута)."""

    decorator: str
    arg: int = 0


class HttpVerbFromSpec(_Strict):
    """M6 T2: откуда брать verb — enum-атрибут arg0 конструктора `request_ctor` (напр.
    `Request(Method.GET, ...)` -> `request_ctor="Request"`, `enum="Method"` -> "GET").

    M7 T5 (pilot-rerun.md verb_unresolved=15 -- document-management's real
    `ProxyRequest(Request)` subclass: `request_ctor: "Request"` alone never matched
    the subclass ctor's own name): `request_ctor` is now "|"-separated alternatives
    too ("Request|ProxyRequest") -- same DSL convention as HttpClientIdiom.call (M6
    T2), matched via the SAME `_call_alternatives`/`_matches_call_alt` machinery in
    extractors/http_client_ext.py's `_find_verb` (a bare, receiver-less ctor name
    degrades to plain equality against `CallFact.callee_name`, byte-identical to the
    pre-M7-T5 single-name comparison)."""

    request_ctor: str
    enum: str

    @model_validator(mode="after")
    def _no_empty_alternative(self) -> HttpVerbFromSpec:
        # M7 T5: an empty "|"-alternative (trailing/leading/doubled "|", or a bare "")
        # can only be a config typo -- no ctor is ever named "", so it would validate
        # but could only ever contribute a permanent, silent non-match. Rejected at
        # config-load time instead of degrading to a forever-honest
        # `http_verb_unresolved` miss nobody can explain from the DSL alone.
        if any(alt == "" for alt in self.request_ctor.split("|")):
            raise ValueError(
                f"HttpVerbFromSpec.request_ctor: empty '|'-alternative in {self.request_ctor!r}"
            )
        return self


class HttpClientIdiom(_Strict):
    name: str
    file_glob: str = "**/*_client.py"
    class_glob: str = "*Client"
    base_url: BaseUrlSpec | None = None
    service: str | None = None  # явный pin целевого сервиса, если base_url не резолвится
    # M6 T2: decorator-SDK режим (GAPS §2 pilot gap) — все три поля опциональны и по
    # умолчанию отсутствуют, так что существующий verb-режим (`get`/`post`/... callee +
    # arg0=URL) остаётся байт-в-байт прежним. Когда `route_from` задан, экстрактор
    # переключается на альтернативный режим: маршрут — из декоратора метода, HTTP-вызов —
    # driver-индирекция (`call`), verb — из `Request(Method.X, ...)`-конструктора
    # (`verb_from`) внутри тела метода. См. extractors/http_client_ext.py.
    route_from: HttpRouteFromSpec | None = None
    call: str | None = None  # "recv_tail.callee|recv_tail2.callee2" -- "|"-альтернативы
    verb_from: HttpVerbFromSpec | None = None
    # M7 T3 (OPEN R1): auto-anchor target attribute name -- http_client_ext.py looks
    # for a `self.<host_attr> = <dotted-chain-or-name>` assignment inside the matched
    # client class (any method, AST-walk order) and joins the RHS's last identifier
    # through the service-wide ClassAttrIndex.field_by_name (env-gated) to recover a
    # base_url_env the idiom itself never spelled out explicitly. Bare attribute name
    # only (no "self." prefix -- implicit/fixed); default "host" matches the OPEN R1
    # pilot's own real convention (`self.host = config.services.x_url`) verbatim.
    host_attr: str = "host"

    @model_validator(mode="after")
    def _decorator_sdk_all_or_nothing(self) -> HttpClientIdiom:
        # Fail-closed matrix (M6 T2 + review Important-2, следуя прецеденту
        # ConsumerIdiom._kind_requirements): три decorator-SDK поля — всё-или-ничего.
        # route_from без call — экстрактор не найдёт сам HTTP-вызов; route_from без
        # verb_from — verb никогда не резолвится, ноль claim'ов навсегда (мёртвый
        # конфиг, эмпирически доказано ревью); call/verb_from без route_from —
        # молча-инертные поля, которые verb-режим вовсе не читает. Все частичные
        # комбинации отклоняются на загрузке конфига, а не деградируют в тишину.
        if self.route_from is not None:
            missing = [f for f in ("call", "verb_from") if getattr(self, f) is None]
            if missing:
                raise ValueError(
                    f"HttpClientIdiom: route_from (decorator-SDK mode) requires {missing}"
                )
        else:
            inert = [f for f in ("call", "verb_from") if getattr(self, f) is not None]
            if inert:
                raise ValueError(
                    f"HttpClientIdiom: {inert} are inert without route_from "
                    "(decorator-SDK mode is all-or-nothing)"
                )
        return self


class ServiceIdioms(_Strict):
    producers: list[ProducerIdiom] = Field(default_factory=list)
    consumers: list[ConsumerIdiom] = Field(default_factory=list)
    http_clients: list[HttpClientIdiom] = Field(default_factory=list)


class HttpExposure(_Strict):
    base_url_env: str | None = None


class FalkorDBConfig(_Strict):
    host: str = "localhost"
    port: int = 6379


class StorageConfig(_Strict):
    falkordb: FalkorDBConfig = Field(default_factory=FalkorDBConfig)


class EmbeddingConfig(_Strict):
    provider: Literal["local", "openai", "voyage"] = "local"
    model: str = "jinaai/jina-embeddings-v2-base-code"
    # M5 T6: instruction prefixes some local embedding models expect prepended to
    # their input to distinguish a search QUERY from an indexed PASSAGE (e.g. e5's
    # canonical "query: "/"passage: " -- see intfloat/multilingual-e5-base's model
    # card). "" (the default for both) is a byte-identical no-op -- every existing
    # workspace/model that never mentions either field keeps building the exact same
    # embedder input it always has. Only `embedding.local.LocalEmbedder` currently
    # reads these (threaded through by `embedding.factory.make_embedder`) -- openai/
    # voyage are deliberately left alone: voyage already has its own asymmetric
    # query/document handling via its API's `input_type` param (see voyage.py), and
    # neither openai provider model documents needing a text prefix the way e5 does.
    query_prefix: str = ""
    passage_prefix: str = ""


class ScipConfig(_Strict):
    timeout_min: int = 20
    node_options: str = "--max-old-space-size=8192"


class ProcessDecl(_Strict):
    name: str
    entrypoint: str  # селектор: "<service>:<METHOD> <path>" или qualified symbol


class ServiceConfig(_Strict):
    name: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    path: Path
    python: str | None = None
    exclude: list[str] = Field(default_factory=list)
    http: HttpExposure | None = None
    idioms: ServiceIdioms = Field(default_factory=ServiceIdioms)


class WorkspaceConfig(_Strict):
    version: int = 1
    graph_name: str
    storage: StorageConfig = Field(default_factory=StorageConfig)
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    scip: ScipConfig = Field(default_factory=ScipConfig)
    services: list[ServiceConfig]
    builtin_idioms: list[str] = Field(default_factory=lambda: list(DEFAULT_BUILTIN_IDIOMS))
    processes: list[ProcessDecl] = Field(default_factory=list)
    # M7 T3 (OPEN R1): YAML files (e.g. helm values) carrying env-var -> URL mappings,
    # harvested into an env->service map (linking/env_map.py) that anchors http_call
    # claims whose base_url_env isn't already covered by any ServiceConfig.http.
    # base_url_env (the pre-existing, PRIMARY registry -- this is an ADDITIVE
    # fallback, consulted only when that registry finds no owner). Paths are relative
    # to the workspace yaml's OWN directory -- config/loader.py resolves + validates
    # existence at load time, mirroring ServiceConfig.path's own contract exactly.
    env_sources: list[Path] = Field(default_factory=list)
