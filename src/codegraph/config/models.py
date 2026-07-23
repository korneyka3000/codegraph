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
    model_config = ConfigDict(extra="forbid", frozen=True)


class ValueSpec(_Strict):
    """Откуда берётся строковое значение (имя топика, тип события, base_url...).

    Ровно один источник: литерал, позиционный аргумент, kwarg, env-переменная
    или атрибут объекта.
    """

    const: str | None = None
    arg: int | None = None
    kwarg: str | None = None
    env: str | None = None
    attr: str | None = None

    @model_validator(mode="after")
    def _exactly_one(self) -> ValueSpec:
        set_fields = [
            f for f in ("const", "arg", "kwarg", "env", "attr")
            if getattr(self, f) is not None
        ]
        if len(set_fields) != 1:
            raise ValueError(f"ValueSpec requires exactly one source, got: {set_fields}")
        return self


EventTypeFrom = ValueSpec | Literal["dict_key"]


class ChannelSpec(_Strict):
    kind: Literal["kafka_topic", "event_type", "http_route"]
    name_from: ValueSpec | None = None
    topic: ValueSpec | None = None
    event_type_from: EventTypeFrom | None = None


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
    """

    name: str
    kind: Literal["call", "decorator", "dispatch_dict"]
    call: str | None = None
    decorator: str | None = None
    registrar_call: str | None = None
    topic: ValueSpec | None = None
    event_type_from: EventTypeFrom | None = None

    @model_validator(mode="after")
    def _kind_requirements(self) -> ConsumerIdiom:
        required = {
            "call": ("call",),
            "decorator": ("decorator",),
            "dispatch_dict": ("registrar_call",),
        }[self.kind]
        if not any(getattr(self, f) is not None for f in required):
            raise ValueError(f"consumer kind={self.kind} requires one of {required}")
        return self


class BaseUrlSpec(_Strict):
    attr: str | None = None
    env: str | None = None


class HttpRouteFromSpec(_Strict):
    """M6 T2: откуда брать path_template — декоратор метода (напр. `@path_template(...)`),
    а не arg0 самого HTTP-вызова. `arg` — позиция строкового аргумента ВНУТРИ декоратора
    (обычно 0 — единственный позиционный арг маршрута)."""

    decorator: str
    arg: int = 0


class HttpVerbFromSpec(_Strict):
    """M6 T2: откуда брать verb — enum-атрибут arg0 конструктора `request_ctor` (напр.
    `Request(Method.GET, ...)` -> `request_ctor="Request"`, `enum="Method"` -> "GET")."""

    request_ctor: str
    enum: str


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
