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
    name: str
    kind: Literal["call", "decorator", "dispatch_dict"]
    call: str | None = None
    decorator: str | None = None
    registrar_call: str | None = None
    dict_assign: str | None = None
    topic: ValueSpec | None = None
    event_type_from: EventTypeFrom | None = None

    @model_validator(mode="after")
    def _kind_requirements(self) -> ConsumerIdiom:
        required = {
            "call": ("call",),
            "decorator": ("decorator",),
            "dispatch_dict": ("registrar_call", "dict_assign"),
        }[self.kind]
        if not any(getattr(self, f) is not None for f in required):
            raise ValueError(f"consumer kind={self.kind} requires one of {required}")
        return self


class BaseUrlSpec(_Strict):
    attr: str | None = None
    env: str | None = None


class HttpClientIdiom(_Strict):
    name: str
    file_glob: str = "**/*_client.py"
    class_glob: str = "*Client"
    base_url: BaseUrlSpec | None = None
    service: str | None = None  # явный pin целевого сервиса, если base_url не резолвится


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


class ScipConfig(_Strict):
    timeout_min: int = 20
    node_options: str = "--max-old-space-size=8192"


class ProcessDecl(_Strict):
    name: str
    entrypoint: str  # селектор: "<service>:<METHOD> <path>" или qualified symbol


class ServiceConfig(_Strict):
    name: str
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
