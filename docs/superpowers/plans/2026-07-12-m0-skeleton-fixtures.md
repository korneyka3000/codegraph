# M0: Skeleton, Infrastructure & Fixtures — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Рабочий каркас `codegraph`: uv-пакет, CLI, полный Idiom-DSL конфига с loader'ом и builtin-идиомами, vendored SCIP protobuf, docker-compose с FalkorDB, команда `doctor` с feature-probes, 3 фикстурных микросервиса с golden-разметкой.

**Architecture:** src-layout Python-пакет; конфиг — pydantic-модели, идиомы — данные; SCIP protobuf вендорится сгенерированным `scip_pb2.py`; FalkorDB-подключение и probes изолированы в `stores/falkordb/`; фикстуры — синтаксически валидный Python-код-как-данные (не исполняются).

**Tech Stack:** Python 3.12+, uv, typer, pydantic v2, PyYAML, rich, protobuf, falkordb-py, pytest, ruff, grpcio-tools (dev), Docker (FalkorDB).

**Мастер-план (спека):** `docs/superpowers/specs/2026-07-12-codegraph-design.md` (копируется в Task 1).

## Global Constraints

- Python ≥ 3.12; менеджер — uv; пакет `codegraph`, src-layout.
- Идиомы producer/consumer/client — pydantic-модели, парсящиеся из YAML; builtin-идиомы — экземпляры ТЕХ ЖЕ моделей.
- Весь код общения с FalkorDB — только в `src/codegraph/stores/falkordb/`.
- Фикстурные сервисы НЕ исполняются и НЕ импортируются тестами — только `ast.parse` (это код-как-данные для индексации).
- Тесты, требующие поднятого FalkorDB, помечаются `@pytest.mark.falkordb` и скипаются, если инстанс недоступен.
- Коммит после каждой задачи. Сообщения: `feat(m0): <what>` / `test(m0): <what>` / `chore(m0): <what>`.
- Никаких зависимостей "на будущее" (tree-sitter, fastmcp, sentence-transformers добавятся в M1–M3).

---

### Task 1: Git + uv scaffold

**Files:**
- Create: `.gitignore`, `pyproject.toml`, `src/codegraph/__init__.py`, `tests/unit/test_package.py`, `docs/superpowers/specs/2026-07-12-codegraph-design.md` (копия одобренного мастер-плана из `/Users/korney.burau/.claude/plans/playful-singing-kurzweil.md`)

**Interfaces:**
- Produces: пакет `codegraph` с `__version__ = "0.1.0"`; dev-окружение `uv sync`; git-репозиторий на ветке `main`.

- [ ] **Step 1: git init и .gitignore**

```bash
cd /Users/korney.burau/PyProjects/ast-tree-multi
git init -b main
```

`.gitignore`:
```gitignore
__pycache__/
*.py[cod]
.venv/
.codegraph/
.pytest_cache/
.ruff_cache/
dist/
*.egg-info/
.env
```

- [ ] **Step 2: pyproject.toml**

```toml
[project]
name = "codegraph"
version = "0.1.0"
description = "Code knowledge graph RAG for Python microservices: CLI indexer + MCP server"
requires-python = ">=3.12"
dependencies = [
    "typer>=0.12",
    "pydantic>=2.7",
    "pyyaml>=6.0",
    "rich>=13.7",
    "protobuf>=5.26",
    "falkordb>=1.0.10",
]

[project.scripts]
codegraph = "codegraph.cli:main"

[dependency-groups]
dev = [
    "pytest>=8.2",
    "ruff>=0.5",
    "grpcio-tools>=1.64",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/codegraph"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"
markers = [
    "falkordb: requires a running FalkorDB instance (docker compose up -d)",
]

[tool.ruff]
line-length = 100
target-version = "py312"
exclude = ["fixtures"]  # код-как-данные для индексации, не линтим

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

- [ ] **Step 3: пакет и первый тест**

`src/codegraph/__init__.py`:
```python
__version__ = "0.1.0"
```

`tests/unit/test_package.py`:
```python
import codegraph


def test_version():
    assert codegraph.__version__ == "0.1.0"
```

- [ ] **Step 4: установить окружение и прогнать тест**

Run: `uv sync && uv run pytest tests/unit/test_package.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: скопировать мастер-план как спеку в репо**

```bash
mkdir -p docs/superpowers/specs
cp /Users/korney.burau/.claude/plans/playful-singing-kurzweil.md docs/superpowers/specs/2026-07-12-codegraph-design.md
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(m0): scaffold uv package, pytest, ruff; vendor design spec"
```

---

### Task 2: Config-модели — Idiom-DSL и workspace

**Files:**
- Create: `src/codegraph/config/__init__.py`, `src/codegraph/config/models.py`
- Test: `tests/unit/test_config_models.py`

**Interfaces:**
- Produces (модуль `codegraph.config.models`):
  - `ValueSpec` — источник значения: ровно один из `const: str | arg: int | kwarg: str | env: str | attr: str`.
  - `EventTypeFrom = ValueSpec | Literal["dict_key"]`
  - `ChannelSpec(kind: Literal["kafka_topic","event_type","http_route"], name_from: ValueSpec | None, topic: ValueSpec | None, event_type_from: EventTypeFrom | None)`
  - `ProducerIdiom(name: str, call: str, channel: ChannelSpec)`
  - `ConsumerIdiom(name: str, kind: Literal["call","decorator","dispatch_dict"], call: str | None, decorator: str | None, registrar_call: str | None, dict_assign: str | None, topic: ValueSpec | None, event_type_from: EventTypeFrom | None)`
  - `HttpClientIdiom(name: str, file_glob: str, class_glob: str, base_url: BaseUrlSpec | None, service: str | None)`; `BaseUrlSpec(attr: str | None, env: str | None)`
  - `ServiceIdioms(producers: list[ProducerIdiom], consumers: list[ConsumerIdiom], http_clients: list[HttpClientIdiom])`
  - `HttpExposure(base_url_env: str | None)`
  - `ServiceConfig(name: str, path: Path, python: str | None, exclude: list[str], http: HttpExposure | None, idioms: ServiceIdioms)`
  - `StorageConfig(falkordb: FalkorDBConfig)`; `FalkorDBConfig(host: str = "localhost", port: int = 6379)`
  - `EmbeddingConfig(provider: Literal["local","openai","voyage"] = "local", model: str = "jinaai/jina-embeddings-v2-base-code")`
  - `ScipConfig(timeout_min: int = 20, node_options: str = "--max-old-space-size=8192")`
  - `ProcessDecl(name: str, entrypoint: str)`
  - `WorkspaceConfig(version: int, graph_name: str, storage: StorageConfig, embedding: EmbeddingConfig, scip: ScipConfig, services: list[ServiceConfig], builtin_idioms: list[str], processes: list[ProcessDecl])` — все поля кроме `services` имеют дефолты; `builtin_idioms` default = `["fastapi","aiokafka","faststream","confluent","temporal","aiohttp_client"]`.
  - Все модели: `model_config = ConfigDict(extra="forbid")`.

- [ ] **Step 1: Написать падающие тесты**

`tests/unit/test_config_models.py`:
```python
import pytest
import yaml
from pydantic import ValidationError

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    ProducerIdiom,
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


def test_consumer_dispatch_dict_requires_registrar_or_assign():
    with pytest.raises(ValidationError):
        ConsumerIdiom.model_validate({"name": "x", "kind": "dispatch_dict"})
    ok = ConsumerIdiom.model_validate(
        {"name": "x", "kind": "dispatch_dict", "dict_assign": "EVENT_HANDLERS"}
    )
    assert ok.dict_assign == "EVENT_HANDLERS"


def test_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        ProducerIdiom.model_validate(
            {"name": "x", "call": "a.b", "channel": {"kind": "kafka_topic"}, "typo": 1}
        )


def test_channel_spec_kafka_topic_name_from_arg():
    ch = ChannelSpec.model_validate({"kind": "kafka_topic", "name_from": {"arg": 0}})
    assert ch.name_from.arg == 0
```

- [ ] **Step 2: Запустить — убедиться, что падают**

Run: `uv run pytest tests/unit/test_config_models.py -v`
Expected: FAIL / ERROR (`ModuleNotFoundError: codegraph.config`)

- [ ] **Step 3: Реализовать модели**

`src/codegraph/config/__init__.py` — пустой.

`src/codegraph/config/models.py`:
```python
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
    model_config = ConfigDict(extra="forbid")


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
```

- [ ] **Step 4: Прогнать тесты**

Run: `uv run pytest tests/unit/test_config_models.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/codegraph/config tests/unit/test_config_models.py
git commit -m "feat(m0): config models with idiom DSL (producers/consumers/http clients)"
```

---

### Task 3: Builtin-идиомы как данные

**Files:**
- Create: `src/codegraph/config/builtin_idioms.py`
- Test: `tests/unit/test_builtin_idioms.py`

**Interfaces:**
- Consumes: модели из Task 2.
- Produces: `BUILTIN_IDIOMS: dict[str, ServiceIdioms]` с ключами `fastapi, aiokafka, faststream, confluent, temporal, aiohttp_client`; функция `resolve_builtins(names: list[str]) -> ServiceIdioms` (сливает списки; неизвестное имя → `KeyError` с перечислением известных). Примечание: `fastapi` и `temporal` — структурные экстракторы (декораторы роутов/воркфлоу зашиты в экстракторах M2), их ServiceIdioms здесь пустые; имя в реестре включает/выключает экстрактор.

- [ ] **Step 1: Написать падающий тест**

`tests/unit/test_builtin_idioms.py`:
```python
import pytest

from codegraph.config.builtin_idioms import BUILTIN_IDIOMS, resolve_builtins
from codegraph.config.models import DEFAULT_BUILTIN_IDIOMS, ServiceIdioms


def test_registry_covers_all_defaults():
    assert set(BUILTIN_IDIOMS) == set(DEFAULT_BUILTIN_IDIOMS)


def test_all_builtins_are_valid_service_idioms():
    for name, idioms in BUILTIN_IDIOMS.items():
        assert isinstance(idioms, ServiceIdioms), name


def test_aiokafka_producer_send_topic_from_arg0():
    prods = BUILTIN_IDIOMS["aiokafka"].producers
    send = next(p for p in prods if "send" in p.call)
    assert send.channel.kind == "kafka_topic"
    assert send.channel.name_from.arg == 0


def test_faststream_uses_decorators():
    fs = BUILTIN_IDIOMS["faststream"]
    assert any(c.kind == "decorator" for c in fs.consumers)


def test_resolve_builtins_merges_and_rejects_unknown():
    merged = resolve_builtins(["aiokafka", "faststream"])
    assert len(merged.producers) >= 2
    with pytest.raises(KeyError, match="unknown builtin idiom"):
        resolve_builtins(["kafka-python"])
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_builtin_idioms.py -v`
Expected: FAIL (`ModuleNotFoundError`)

- [ ] **Step 3: Реализовать реестр**

`src/codegraph/config/builtin_idioms.py`:
```python
"""Builtin-идиомы: те же модели, что парсятся из YAML пользователя.

fastapi/temporal — структурные экстракторы (паттерны декораторов зашиты в
extractors M2); их присутствие в списке включает соответствующий экстрактор,
поэтому ServiceIdioms у них пустые.
"""

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    HttpClientIdiom,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)

BUILTIN_IDIOMS: dict[str, ServiceIdioms] = {
    "fastapi": ServiceIdioms(),
    "temporal": ServiceIdioms(),
    "aiokafka": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="aiokafka-send",
                call="aiokafka.AIOKafkaProducer.send",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
            ProducerIdiom(
                name="aiokafka-send-and-wait",
                call="aiokafka.AIOKafkaProducer.send_and_wait",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="aiokafka-consumer-init",
                kind="call",
                call="aiokafka.AIOKafkaConsumer",
                topic=ValueSpec(arg=0),
            ),
            ConsumerIdiom(
                name="aiokafka-subscribe",
                kind="call",
                call="aiokafka.AIOKafkaConsumer.subscribe",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "confluent": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="confluent-produce",
                call="confluent_kafka.Producer.produce",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="confluent-subscribe",
                kind="call",
                call="confluent_kafka.Consumer.subscribe",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "faststream": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="faststream-publisher",
                call="faststream.kafka.KafkaBroker.publisher",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="faststream-subscriber",
                kind="decorator",
                decorator="broker.subscriber",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "aiohttp_client": ServiceIdioms(
        http_clients=[
            HttpClientIdiom(name="aiohttp-client-convention"),
        ],
    ),
}


def resolve_builtins(names: list[str]) -> ServiceIdioms:
    merged = ServiceIdioms()
    for name in names:
        if name not in BUILTIN_IDIOMS:
            known = ", ".join(sorted(BUILTIN_IDIOMS))
            raise KeyError(f"unknown builtin idiom {name!r}; known: {known}")
        src = BUILTIN_IDIOMS[name]
        merged.producers.extend(src.producers)
        merged.consumers.extend(src.consumers)
        merged.http_clients.extend(src.http_clients)
    return merged
```

- [ ] **Step 4: Прогнать тесты**

Run: `uv run pytest tests/unit/test_builtin_idioms.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/codegraph/config/builtin_idioms.py tests/unit/test_builtin_idioms.py
git commit -m "feat(m0): builtin idiom registry as data (aiokafka/confluent/faststream/aiohttp)"
```

---

### Task 4: Config loader + zero-config

**Files:**
- Create: `src/codegraph/config/loader.py`
- Test: `tests/unit/test_config_loader.py`

**Interfaces:**
- Consumes: `WorkspaceConfig`, `ServiceConfig`, `resolve_builtins`.
- Produces (модуль `codegraph.config.loader`):
  - `class ConfigError(Exception)` — человекочитаемые ошибки конфига.
  - `load_workspace(target: Path) -> WorkspaceConfig` — если `target` — YAML-файл, парсит его; если директория — ищет `codegraph.yaml` в ней, не найдя — синтезирует zero-config. Пути сервисов резолвятся относительно файла конфига в абсолютные; несуществующий путь → `ConfigError`. Валидирует `builtin_idioms` через `resolve_builtins` (ошибка → `ConfigError`).
  - `synth_zero_config(repo: Path) -> WorkspaceConfig` — один сервис: `name=repo.name`, `path=repo.resolve()`, `graph_name=repo.name`, дефолтные builtin-идиомы.
  - `effective_idioms(cfg: WorkspaceConfig, svc: ServiceConfig) -> ServiceIdioms` — builtin (по списку workspace) + идиомы сервиса, слитые в один ServiceIdioms.

- [ ] **Step 1: Написать падающие тесты**

`tests/unit/test_config_loader.py`:
```python
from pathlib import Path

import pytest

from codegraph.config.loader import (
    ConfigError,
    effective_idioms,
    load_workspace,
    synth_zero_config,
)

MINIMAL = """
version: 1
graph_name: demo
services:
  - name: svc-a
    path: ./svc-a
"""


def _mk_ws(tmp_path: Path, yaml_text: str) -> Path:
    (tmp_path / "svc-a").mkdir()
    p = tmp_path / "codegraph.yaml"
    p.write_text(yaml_text)
    return p


def test_load_explicit_yaml_resolves_paths(tmp_path):
    p = _mk_ws(tmp_path, MINIMAL)
    cfg = load_workspace(p)
    assert cfg.services[0].path == (tmp_path / "svc-a").resolve()


def test_load_directory_finds_yaml(tmp_path):
    _mk_ws(tmp_path, MINIMAL)
    cfg = load_workspace(tmp_path)
    assert cfg.graph_name == "demo"


def test_zero_config_synthesis(tmp_path):
    repo = tmp_path / "my-repo"
    repo.mkdir()
    cfg = load_workspace(repo)
    assert cfg.graph_name == "my-repo"
    assert len(cfg.services) == 1
    assert cfg.services[0].path == repo.resolve()
    assert synth_zero_config(repo).services[0].name == "my-repo"


def test_missing_service_path_raises(tmp_path):
    p = tmp_path / "codegraph.yaml"
    p.write_text(MINIMAL)  # svc-a не создана
    with pytest.raises(ConfigError, match="svc-a"):
        load_workspace(p)


def test_unknown_builtin_idiom_raises(tmp_path):
    p = _mk_ws(
        tmp_path,
        MINIMAL + "builtin_idioms: [fastapi, nosuch]\n",
    )
    with pytest.raises(ConfigError, match="nosuch"):
        load_workspace(p)


def test_effective_idioms_merges_builtin_and_service(tmp_path):
    p = _mk_ws(
        tmp_path,
        """
version: 1
graph_name: demo
builtin_idioms: [aiokafka]
services:
  - name: svc-a
    path: ./svc-a
    idioms:
      producers:
        - name: outbox
          call: "app.outbox.add_event"
          channel: { kind: event_type, event_type_from: { arg: 0 } }
""",
    )
    cfg = load_workspace(p)
    idioms = effective_idioms(cfg, cfg.services[0])
    names = {pr.name for pr in idioms.producers}
    assert "outbox" in names and "aiokafka-send" in names
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_config_loader.py -v`
Expected: FAIL (`ModuleNotFoundError`)

- [ ] **Step 3: Реализовать loader**

`src/codegraph/config/loader.py`:
```python
"""Загрузка workspace-конфига: явный YAML, поиск в директории, zero-config."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from codegraph.config.builtin_idioms import resolve_builtins
from codegraph.config.models import ServiceConfig, ServiceIdioms, WorkspaceConfig

CONFIG_FILENAME = "codegraph.yaml"


class ConfigError(Exception):
    pass


def synth_zero_config(repo: Path) -> WorkspaceConfig:
    repo = repo.resolve()
    return WorkspaceConfig(
        graph_name=repo.name,
        services=[ServiceConfig(name=repo.name, path=repo)],
    )


def load_workspace(target: Path) -> WorkspaceConfig:
    target = target.resolve()
    if target.is_dir():
        candidate = target / CONFIG_FILENAME
        if not candidate.exists():
            return synth_zero_config(target)
        target = candidate
    if not target.exists():
        raise ConfigError(f"config file not found: {target}")

    try:
        raw = yaml.safe_load(target.read_text()) or {}
        cfg = WorkspaceConfig.model_validate(raw)
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {target}: {e}") from e
    except ValidationError as e:
        raise ConfigError(f"invalid config {target}:\n{e}") from e

    base = target.parent
    resolved_services = []
    for svc in cfg.services:
        path = svc.path if svc.path.is_absolute() else (base / svc.path)
        path = path.resolve()
        if not path.is_dir():
            raise ConfigError(f"service {svc.name!r}: path does not exist: {path}")
        resolved_services.append(svc.model_copy(update={"path": path}))
    cfg = cfg.model_copy(update={"services": resolved_services})

    try:
        resolve_builtins(cfg.builtin_idioms)
    except KeyError as e:
        raise ConfigError(str(e)) from e
    return cfg


def effective_idioms(cfg: WorkspaceConfig, svc: ServiceConfig) -> ServiceIdioms:
    merged = resolve_builtins(cfg.builtin_idioms)
    merged.producers.extend(svc.idioms.producers)
    merged.consumers.extend(svc.idioms.consumers)
    merged.http_clients.extend(svc.idioms.http_clients)
    return merged
```

- [ ] **Step 4: Прогнать тесты**

Run: `uv run pytest tests/unit/test_config_loader.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add src/codegraph/config/loader.py tests/unit/test_config_loader.py
git commit -m "feat(m0): config loader with zero-config synthesis and idiom merging"
```

---

### Task 5: Vendored SCIP protobuf

**Files:**
- Create: `scripts/gen_scip_proto.sh`, `src/codegraph/resolvers/__init__.py`, `src/codegraph/resolvers/scip/__init__.py`, `src/codegraph/resolvers/scip/scip.proto` (vendored), `src/codegraph/resolvers/scip/scip_pb2.py` (generated)
- Test: `tests/unit/test_scip_proto.py`

**Interfaces:**
- Produces: `codegraph.resolvers.scip.scip_pb2` с типами `Index`, `Document`, `Occurrence`, `SymbolInformation`; enum-константы `SymbolRole` (Definition=1, Import=2), `PositionEncoding`.

- [ ] **Step 1: Написать скрипт генерации**

`scripts/gen_scip_proto.sh`:
```bash
#!/usr/bin/env bash
# Regenerate vendored SCIP protobuf bindings.
# scip.proto is vendored from https://github.com/sourcegraph/scip (Apache-2.0).
set -euo pipefail
cd "$(dirname "$0")/.."

SCIP_REF="${SCIP_REF:-v0.5.2}"   # pinned release tag of sourcegraph/scip
DEST=src/codegraph/resolvers/scip

curl -fsSL "https://raw.githubusercontent.com/sourcegraph/scip/${SCIP_REF}/scip.proto" \
  -o "${DEST}/scip.proto"

uv run python -m grpc_tools.protoc \
  -I "${DEST}" \
  --python_out="${DEST}" \
  "${DEST}/scip.proto"

echo "regenerated ${DEST}/scip_pb2.py from sourcegraph/scip@${SCIP_REF}"
```

Примечание исполнителю: перед запуском проверь актуальный последний release-тег `sourcegraph/scip` (`git ls-remote --tags https://github.com/sourcegraph/scip | tail -5`) и зафиксируй его в `SCIP_REF` (обнови значение в скрипте на реально существующий последний тег). Protobuf-пакет в scip.proto — `scip`, сгенерированный файл ляжет как `scip_pb2.py`.

- [ ] **Step 2: Выполнить генерацию**

Run: `chmod +x scripts/gen_scip_proto.sh && ./scripts/gen_scip_proto.sh`
Expected: `regenerated src/codegraph/resolvers/scip/scip_pb2.py ...`; файлы `scip.proto` и `scip_pb2.py` существуют. Создай также пустые `src/codegraph/resolvers/__init__.py` и `src/codegraph/resolvers/scip/__init__.py`.

- [ ] **Step 3: Написать round-trip тест**

`tests/unit/test_scip_proto.py`:
```python
from codegraph.resolvers.scip import scip_pb2


def test_roundtrip_index_with_occurrence():
    idx = scip_pb2.Index()
    doc = idx.documents.add()
    doc.relative_path = "app/main.py"
    doc.position_encoding = scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart
    occ = doc.occurrences.add()
    occ.symbol = "scip-python python demo 0.1 `app.main`/handler()."
    occ.range.extend([3, 4, 3, 11])
    occ.symbol_roles = scip_pb2.SymbolRole.Definition

    data = idx.SerializeToString()
    parsed = scip_pb2.Index()
    parsed.ParseFromString(data)

    d = parsed.documents[0]
    assert d.relative_path == "app/main.py"
    assert list(d.occurrences[0].range) == [3, 4, 3, 11]
    assert d.occurrences[0].symbol_roles & scip_pb2.SymbolRole.Definition


def test_symbol_roles_bitmask_values():
    assert scip_pb2.SymbolRole.Definition == 1
    assert scip_pb2.SymbolRole.Import == 2
```

- [ ] **Step 4: Прогнать тест**

Run: `uv run pytest tests/unit/test_scip_proto.py -v`
Expected: PASS (2 passed). Если имена enum-значений в актуальном scip.proto отличаются — поправь тест по факту содержимого vendored scip.proto (истина — в .proto), но битовые значения Definition=1/Import=2 стабильны.

- [ ] **Step 5: Commit**

```bash
git add scripts/gen_scip_proto.sh src/codegraph/resolvers tests/unit/test_scip_proto.py
git commit -m "chore(m0): vendor SCIP protobuf bindings (sourcegraph/scip, pinned)"
```

---

### Task 6: docker-compose + FalkorDB connection helper

**Files:**
- Create: `docker-compose.yml`, `src/codegraph/stores/__init__.py`, `src/codegraph/stores/falkordb/__init__.py`, `src/codegraph/stores/falkordb/connection.py`
- Test: `tests/integration/test_falkordb_connection.py`

**Interfaces:**
- Consumes: `FalkorDBConfig` из Task 2.
- Produces: `codegraph.stores.falkordb.connection.connect(cfg: FalkorDBConfig) -> falkordb.FalkorDB`; `ping(cfg) -> str` (возвращает версию/`"ok"`, кидает `StoreUnavailable` при недоступности); `class StoreUnavailable(Exception)`. Также conftest-хелпер `falkordb_available()` для skip-логики.

- [ ] **Step 1: docker-compose.yml**

Примечание исполнителю: перед записью зафиксируй актуальный стабильный тег образа: `docker pull falkordb/falkordb:latest && docker image inspect falkordb/falkordb:latest --format '{{index .RepoDigests 0}}'`, затем найди соответствующий версионный тег на Docker Hub (`falkordb/falkordb` → Tags, последний `vX.Y.Z`) и подставь его вместо `<PINNED>`.

```yaml
services:
  falkordb:
    image: falkordb/falkordb:<PINNED>
    container_name: codegraph-falkordb
    ports:
      - "6379:6379"
      - "3000:3000"   # FalkorDB Browser (визуальный обзор графа)
    volumes:
      - falkordb-data:/var/lib/falkordb/data
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "6379", "ping"]
      interval: 5s
      timeout: 3s
      retries: 12

volumes:
  falkordb-data:
```

- [ ] **Step 2: Написать интеграционный тест (падает без модуля)**

`tests/integration/test_falkordb_connection.py`:
```python
import pytest

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreUnavailable, connect, ping

pytestmark = pytest.mark.falkordb


def _available() -> bool:
    try:
        ping(FalkorDBConfig())
        return True
    except StoreUnavailable:
        return False


needs_db = pytest.mark.skipif(not _available(), reason="FalkorDB not running")


@needs_db
def test_ping_ok():
    assert ping(FalkorDBConfig())


@needs_db
def test_roundtrip_tiny_graph():
    db = connect(FalkorDBConfig())
    g = db.select_graph("__codegraph_m0_smoke__")
    try:
        g.query("MERGE (n:Probe {id: 'x'}) SET n += {k: 1} RETURN n")
        res = g.query("MATCH (n:Probe {id: 'x'}) RETURN n.k")
        assert res.result_set[0][0] == 1
    finally:
        g.delete()


def test_unavailable_raises():
    with pytest.raises(StoreUnavailable):
        ping(FalkorDBConfig(host="localhost", port=59999))
```

- [ ] **Step 3: Реализовать connection helper**

`src/codegraph/stores/falkordb/connection.py`:
```python
"""Единственная точка создания подключений к FalkorDB."""

from __future__ import annotations

import redis.exceptions
from falkordb import FalkorDB

from codegraph.config.models import FalkorDBConfig


class StoreUnavailable(Exception):
    pass


def connect(cfg: FalkorDBConfig) -> FalkorDB:
    return FalkorDB(host=cfg.host, port=cfg.port)


def ping(cfg: FalkorDBConfig) -> str:
    try:
        db = connect(cfg)
        db.connection.ping()
    except (redis.exceptions.RedisError, ConnectionError, OSError) as e:
        raise StoreUnavailable(f"FalkorDB not reachable at {cfg.host}:{cfg.port}: {e}") from e
    return "ok"
```

(пустые `__init__.py` для `stores/` и `stores/falkordb/`)

- [ ] **Step 4: Поднять FalkorDB и прогнать**

Run: `docker compose up -d && sleep 3 && uv run pytest tests/integration/test_falkordb_connection.py -v -m falkordb`
Expected: PASS (3 passed) — либо, без Docker в окружении, 2 skipped + `test_unavailable_raises` PASS.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml src/codegraph/stores tests/integration/test_falkordb_connection.py
git commit -m "feat(m0): docker-compose with pinned FalkorDB + connection helper"
```

---

### Task 7: `doctor` — проверки окружения и feature-probes

**Files:**
- Create: `src/codegraph/doctor.py`, `src/codegraph/cli.py`
- Test: `tests/unit/test_doctor.py`, `tests/integration/test_doctor_probes.py`

**Interfaces:**
- Consumes: `load_workspace`, `connection.connect/ping`, `ScipConfig`.
- Produces:
  - `codegraph.doctor.CheckResult(name: str, ok: bool, detail: str)` (dataclass)
  - `run_env_checks(scip: ScipConfig, probe_scip: bool) -> list[CheckResult]` — python≥3.12, `node --version`, `npx --version`; при `probe_scip=True` — `npx --yes @sourcegraph/scip-python@<PINNED_SCIP_PY> --version` (таймаут 180с). `<PINNED_SCIP_PY>` — константа `SCIP_PYTHON_VERSION` в `doctor.py`; исполнитель фиксирует последнюю опубликованную версию (`npm view @sourcegraph/scip-python version`).
  - `run_store_probes(db_factory: Callable[[], "FalkorDB"]) -> list[CheckResult]` — probes на временном графе `__codegraph_probe__` (в `finally` — delete): `ping`, `multi_label` (`MERGE (n:A:B {id:'p'}) RETURN labels(n)`), `set_plus_eq` (`SET n += {k:1}`), `unique_constraint` (redis-команда `GRAPH.CONSTRAINT CREATE __codegraph_probe__ UNIQUE NODE A PROPERTIES 1 id`, затем DROP), `vector_index_cosine` (`CREATE VECTOR INDEX FOR (c:P) ON (c.v) OPTIONS {dimension: 4, similarityFunction: 'cosine'}`), `fulltext` (`CALL db.idx.fulltext.createNodeIndex('P', 't')`). Каждый probe независим: исключение → `ok=False, detail=str(e)`.
  - `codegraph.cli.app` (typer.Typer) и `main()`; команда `doctor [--config PATH] [--probe-scip] [--skip-store]`, выводит rich-таблицу, exit code 1 при любом `ok=False` (кроме skipped store при `--skip-store`).

- [ ] **Step 1: Написать падающие unit-тесты (fake db)**

`tests/unit/test_doctor.py`:
```python
from types import SimpleNamespace

from codegraph.config.models import ScipConfig
from codegraph.doctor import CheckResult, run_env_checks, run_store_probes


class FakeGraph:
    def __init__(self, fail_on: set[str]):
        self.fail_on = fail_on
        self.queries: list[str] = []

    def query(self, q: str, params=None):
        self.queries.append(q)
        for marker in self.fail_on:
            if marker in q:
                raise RuntimeError(f"unsupported: {marker}")
        return SimpleNamespace(result_set=[[1]])

    def delete(self):
        pass


class FakeDB:
    def __init__(self, fail_on: set[str] = frozenset(), constraint_ok: bool = True):
        self._g = FakeGraph(set(fail_on))
        self.constraint_ok = constraint_ok
        self.connection = self

    def select_graph(self, name: str):
        return self._g

    def ping(self):
        return True

    def execute_command(self, *args):
        if not self.constraint_ok:
            raise RuntimeError("constraints unsupported")
        return "OK"


def test_env_checks_include_python_and_node():
    results = run_env_checks(ScipConfig(), probe_scip=False)
    names = [r.name for r in results]
    assert "python" in names and "node" in names and "npx" in names
    py = next(r for r in results if r.name == "python")
    assert py.ok  # мы на 3.12+


def test_store_probes_all_ok_with_fake():
    results = run_store_probes(lambda: FakeDB())
    assert all(r.ok for r in results), [(r.name, r.detail) for r in results]
    names = {r.name for r in results}
    assert {"ping", "multi_label", "set_plus_eq", "unique_constraint",
            "vector_index_cosine", "fulltext"} <= names


def test_store_probe_failure_is_isolated():
    results = run_store_probes(lambda: FakeDB(fail_on={"VECTOR INDEX"}))
    by_name = {r.name: r for r in results}
    assert not by_name["vector_index_cosine"].ok
    assert by_name["fulltext"].ok  # остальные probes не пострадали


def test_constraint_failure_reported():
    results = run_store_probes(lambda: FakeDB(constraint_ok=False))
    by_name = {r.name: r for r in results}
    assert not by_name["unique_constraint"].ok
    assert isinstance(by_name["unique_constraint"], CheckResult)
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_doctor.py -v`
Expected: FAIL (`ModuleNotFoundError: codegraph.doctor`)

- [ ] **Step 3: Реализовать doctor.py**

`src/codegraph/doctor.py`:
```python
"""Диагностика окружения и возможностей FalkorDB (feature-probes)."""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass

from codegraph.config.models import ScipConfig

SCIP_PYTHON_VERSION = "<PINNED_SCIP_PY>"  # исполнитель: npm view @sourcegraph/scip-python version
PROBE_GRAPH = "__codegraph_probe__"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


def _cmd_version(name: str, args: list[str], timeout: int = 30) -> CheckResult:
    exe = shutil.which(args[0])
    if exe is None:
        return CheckResult(name, False, f"{args[0]} not found in PATH")
    try:
        out = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout, check=True
        )
        return CheckResult(name, True, out.stdout.strip().splitlines()[0])
    except (subprocess.SubprocessError, OSError) as e:
        return CheckResult(name, False, str(e))


def run_env_checks(scip: ScipConfig, probe_scip: bool = False) -> list[CheckResult]:
    results = [
        CheckResult(
            "python",
            sys.version_info >= (3, 12),
            f"{sys.version_info.major}.{sys.version_info.minor}",
        ),
        _cmd_version("node", ["node", "--version"]),
        _cmd_version("npx", ["npx", "--version"]),
    ]
    if probe_scip:
        results.append(
            _cmd_version(
                "scip-python",
                ["npx", "--yes", f"@sourcegraph/scip-python@{SCIP_PYTHON_VERSION}",
                 "--version"],
                timeout=180,
            )
        )
    return results


def _probe(name: str, fn: Callable[[], object]) -> CheckResult:
    try:
        fn()
        return CheckResult(name, True)
    except Exception as e:  # probes должны быть изолированы друг от друга
        return CheckResult(name, False, str(e))


def run_store_probes(db_factory: Callable[[], object]) -> list[CheckResult]:
    try:
        db = db_factory()
        db.connection.ping()
    except Exception as e:
        return [CheckResult("ping", False, str(e))]

    results = [CheckResult("ping", True)]
    g = db.select_graph(PROBE_GRAPH)
    try:
        results.append(_probe(
            "multi_label",
            lambda: g.query("MERGE (n:A:B {id: 'p'}) RETURN labels(n)"),
        ))
        results.append(_probe(
            "set_plus_eq",
            lambda: g.query("MERGE (n:A {id: 'q'}) SET n += {k: 1} RETURN n.k"),
        ))

        def _constraint():
            db.connection.execute_command(
                "GRAPH.CONSTRAINT", "CREATE", PROBE_GRAPH,
                "UNIQUE", "NODE", "A", "PROPERTIES", "1", "id",
            )
            db.connection.execute_command(
                "GRAPH.CONSTRAINT", "DROP", PROBE_GRAPH,
                "UNIQUE", "NODE", "A", "PROPERTIES", "1", "id",
            )

        results.append(_probe("unique_constraint", _constraint))
        results.append(_probe(
            "vector_index_cosine",
            lambda: g.query(
                "CREATE VECTOR INDEX FOR (c:P) ON (c.v) "
                "OPTIONS {dimension: 4, similarityFunction: 'cosine'}"
            ),
        ))
        results.append(_probe(
            "fulltext",
            lambda: g.query("CALL db.idx.fulltext.createNodeIndex('P', 't')"),
        ))
    finally:
        try:
            g.delete()
        except Exception:
            pass
    return results
```

- [ ] **Step 4: Реализовать CLI-каркас с командой doctor**

`src/codegraph/cli.py`:
```python
"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from codegraph.config.loader import load_workspace
from codegraph.doctor import run_env_checks, run_store_probes

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


def _render(results, title: str) -> bool:
    table = Table(title=title)
    table.add_column("check")
    table.add_column("status")
    table.add_column("detail")
    all_ok = True
    for r in results:
        all_ok &= r.ok
        table.add_row(r.name, "[green]OK[/]" if r.ok else "[red]FAIL[/]", r.detail)
    console.print(table)
    return all_ok


@app.command()
def doctor(
    config: Path = typer.Option(Path.cwd(), "--config", "-c"),
    probe_scip: bool = typer.Option(False, "--probe-scip"),
    skip_store: bool = typer.Option(False, "--skip-store"),
) -> None:
    """Проверить окружение (python/node/scip-python) и возможности FalkorDB."""
    cfg = load_workspace(config)
    ok = _render(run_env_checks(cfg.scip, probe_scip=probe_scip), "environment")
    if not skip_store:
        from codegraph.stores.falkordb.connection import connect

        ok &= _render(
            run_store_probes(lambda: connect(cfg.storage.falkordb)),
            f"falkordb {cfg.storage.falkordb.host}:{cfg.storage.falkordb.port}",
        )
    raise typer.Exit(0 if ok else 1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Прогнать unit-тесты**

Run: `uv run pytest tests/unit/test_doctor.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: Интеграционный тест probes на реальном FalkorDB**

`tests/integration/test_doctor_probes.py`:
```python
import pytest

from codegraph.config.models import FalkorDBConfig
from codegraph.doctor import run_store_probes
from codegraph.stores.falkordb.connection import StoreUnavailable, connect, ping

pytestmark = pytest.mark.falkordb


def _available() -> bool:
    try:
        ping(FalkorDBConfig())
        return True
    except StoreUnavailable:
        return False


@pytest.mark.skipif(not _available(), reason="FalkorDB not running")
def test_all_required_features_present_on_pinned_image():
    results = run_store_probes(lambda: connect(FalkorDBConfig()))
    failed = [(r.name, r.detail) for r in results if not r.ok]
    assert not failed, f"pinned FalkorDB image lacks features: {failed}"
```

Run: `uv run pytest tests/integration/test_doctor_probes.py -v -m falkordb`
Expected: PASS (или skip без Docker). Если `unique_constraint`/`vector_index_cosine` падают на выбранном образе — это сигнал СМЕНИТЬ пин образа (новее), а не ослаблять тест; синтаксис сверить с документацией FalkorDB для пиненной версии.

- [ ] **Step 7: Smoke CLI**

Run: `uv run codegraph doctor --skip-store` (в директории репо; zero-config сработает на самом репо)
Expected: rich-таблица environment, exit 0 (node/npx присутствуют на машине — иначе честный FAIL и exit 1; это корректное поведение doctor).

- [ ] **Step 8: Commit**

```bash
git add src/codegraph/doctor.py src/codegraph/cli.py tests/unit/test_doctor.py tests/integration/test_doctor_probes.py
git commit -m "feat(m0): doctor command with env checks and falkordb feature probes"
```

---

### Task 8: Фикстурные микросервисы (код-как-данные)

**Files:**
- Create (полные листинги ниже; все `__init__.py` — пустые):
  - `fixtures/services/orders_api/app/{__init__,main,models}.py`, `app/routes/{__init__,orders}.py`, `app/services/{__init__,order}.py`, `app/db/{__init__,session,outbox}.py`
  - `fixtures/services/kyc_worker/app/{__init__,consumer_main}.py`, `app/consumers/{__init__,base,orders}.py`, `app/workflows/{__init__,kyc}.py`, `app/activities/{__init__,documents}.py`, `app/clients/{__init__,document_management_client}.py`
  - `fixtures/services/document_management/app/{__init__,main}.py`, `app/routes/{__init__,documents}.py`, `app/services/{__init__,documents}.py`, `app/events/{__init__,producer}.py`
- Test: `tests/unit/test_fixtures_valid.py`

**Interfaces:**
- Produces: три индексируемых сервиса, покрывающие все экстракторы M2: FastAPI-роуты+Depends, кастомный outbox-producer, dispatch-dict consumer, Temporal workflow/activity, aiohttp client SDK, builtin aiokafka producer. Golden-разметка (Task 9) ссылается на эти символы — при изменении фикстур обновлять golden.

- [ ] **Step 1: Написать тест валидности фикстур**

`tests/unit/test_fixtures_valid.py`:
```python
import ast
from pathlib import Path

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"

EXPECTED_SERVICES = {"orders_api", "kyc_worker", "document_management"}


def test_fixture_services_exist():
    assert {p.name for p in FIXTURES.iterdir() if p.is_dir()} == EXPECTED_SERVICES


def test_all_fixture_files_parse():
    py_files = list(FIXTURES.rglob("*.py"))
    assert len(py_files) >= 20
    for f in py_files:
        ast.parse(f.read_text(), filename=str(f))


def test_key_symbols_present():
    orders = (FIXTURES / "orders_api/app/services/order.py").read_text()
    assert "add_event" in orders and "OrderCreated" in orders
    worker = (FIXTURES / "kyc_worker/app/consumers/orders.py").read_text()
    assert "register_handlers" in worker
    client = (
        FIXTURES / "kyc_worker/app/clients/document_management_client.py"
    ).read_text()
    assert "class DocumentManagementClient" in client
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_fixtures_valid.py -v`
Expected: FAIL (директорий нет)

- [ ] **Step 3: Создать orders_api**

`fixtures/services/orders_api/app/models.py`:
```python
from pydantic import BaseModel


class OrderCreate(BaseModel):
    customer_id: str
    amount: float


class Order(BaseModel):
    id: str
    customer_id: str
    amount: float
    status: str
```

`fixtures/services/orders_api/app/db/session.py`:
```python
class Session:
    async def execute(self, query: str, params: dict | None = None) -> None:
        pass


async def get_db():
    session = Session()
    yield session
```

`fixtures/services/orders_api/app/db/outbox.py`:
```python
from app.db.session import Session


class OutboxRepository:
    """Транзакционный outbox: события уходят в Kafka отдельным relay-подом."""

    def __init__(self, db: Session):
        self._db = db

    async def add_event(self, event_type: str, payload: dict) -> None:
        await self._db.execute(
            "INSERT INTO outbox (event_type, payload) VALUES (:t, :p)",
            {"t": event_type, "p": payload},
        )
```

`fixtures/services/orders_api/app/services/order.py`:
```python
import uuid

from app.db.outbox import OutboxRepository
from app.db.session import Session
from app.models import Order, OrderCreate


class OrderService:
    def __init__(self, db: Session):
        self._db = db

    async def place(self, req: OrderCreate) -> Order:
        order = Order(
            id=str(uuid.uuid4()),
            customer_id=req.customer_id,
            amount=req.amount,
            status="pending_kyc",
        )
        await self._persist(order)
        outbox = OutboxRepository(self._db)
        await outbox.add_event(
            "OrderCreated",
            {"order_id": order.id, "customer_id": order.customer_id},
        )
        return order

    async def _persist(self, order: Order) -> None:
        await self._db.execute("INSERT INTO orders ...", order.model_dump())
```

`fixtures/services/orders_api/app/routes/orders.py`:
```python
from fastapi import APIRouter, Depends

from app.db.session import Session, get_db
from app.models import Order, OrderCreate
from app.services.order import OrderService

router = APIRouter(prefix="/orders")


@router.post("")
async def create_order(req: OrderCreate, db: Session = Depends(get_db)) -> Order:
    service = OrderService(db)
    return await service.place(req)


@router.get("/{order_id}")
async def get_order(order_id: str, db: Session = Depends(get_db)) -> Order:
    service = OrderService(db)
    return await service.get(order_id)
```

Примечание: `OrderService.get` отсутствует намеренно? НЕТ — добавь в `OrderService`:
```python
    async def get(self, order_id: str) -> Order:
        await self._db.execute("SELECT ...", {"id": order_id})
        return Order(id=order_id, customer_id="", amount=0.0, status="unknown")
```

`fixtures/services/orders_api/app/main.py`:
```python
from fastapi import FastAPI

from app.routes.orders import router as orders_router

app = FastAPI(title="orders-api")
app.include_router(orders_router)
```

- [ ] **Step 4: Создать kyc_worker**

`fixtures/services/kyc_worker/app/consumers/base.py`:
```python
from collections.abc import Awaitable, Callable

Handler = Callable[[dict], Awaitable[None]]

EVENT_HANDLERS: dict[str, Handler] = {}


def register_handlers(mapping: dict[str, Handler]) -> None:
    EVENT_HANDLERS.update(mapping)
```

`fixtures/services/kyc_worker/app/consumers/orders.py`:
```python
from app.consumers.base import register_handlers
from app.workflows.kyc import KycWorkflow
from temporalio.client import Client


async def _temporal() -> Client:
    return await Client.connect("temporal:7233")


async def handle_order_created(payload: dict) -> None:
    client = await _temporal()
    await client.start_workflow(
        KycWorkflow.run,
        payload,
        id=f"kyc-{payload['order_id']}",
        task_queue="kyc",
    )


register_handlers({"OrderCreated": handle_order_created})
```

`fixtures/services/kyc_worker/app/consumer_main.py`:
```python
import json

from aiokafka import AIOKafkaConsumer

from app.consumers import orders  # noqa: F401  (регистрация хэндлеров)
from app.consumers.base import EVENT_HANDLERS


async def run_consumer() -> None:
    consumer = AIOKafkaConsumer("orders.events", bootstrap_servers="kafka:9092")
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            handler = EVENT_HANDLERS.get(event["event_type"])
            if handler is not None:
                await handler(event["payload"])
    finally:
        await consumer.stop()
```

`fixtures/services/kyc_worker/app/workflows/kyc.py`:
```python
from datetime import timedelta

from temporalio import workflow

from app.activities.documents import verify_documents


@workflow.defn
class KycWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> str:
        status = await workflow.execute_activity(
            verify_documents,
            payload["order_id"],
            start_to_close_timeout=timedelta(minutes=5),
        )
        return status
```

`fixtures/services/kyc_worker/app/activities/documents.py`:
```python
import os

from temporalio import activity

from app.clients.document_management_client import DocumentManagementClient


@activity.defn
async def verify_documents(order_id: str) -> str:
    client = DocumentManagementClient(
        base_url=os.environ["DOCUMENT_MANAGEMENT_URL"],
    )
    doc = await client.get_document(order_id)
    return doc.get("status", "unknown")
```

`fixtures/services/kyc_worker/app/clients/document_management_client.py`:
```python
import aiohttp


class DocumentManagementClient:
    """Рукописный SDK сервиса document-management."""

    def __init__(self, base_url: str):
        self._base_url = base_url

    async def get_document(self, doc_id: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._base_url}/documents/{doc_id}") as resp:
                return await resp.json()

    async def create_document(self, payload: dict) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._base_url}/documents", json=payload) as resp:
                return await resp.json()
```

- [ ] **Step 5: Создать document_management**

`fixtures/services/document_management/app/services/documents.py`:
```python
class DocumentService:
    async def fetch(self, doc_id: str) -> dict:
        return {"id": doc_id, "status": "verified"}

    async def store(self, payload: dict) -> dict:
        return {"id": "new-doc", **payload}
```

`fixtures/services/document_management/app/events/producer.py`:
```python
import json

from aiokafka import AIOKafkaProducer

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
        await _producer.start()
    return _producer


async def emit_document_indexed(doc_id: str) -> None:
    producer = await get_producer()
    await producer.send("documents.indexed", json.dumps({"doc_id": doc_id}).encode())
```

`fixtures/services/document_management/app/routes/documents.py`:
```python
from fastapi import APIRouter

from app.events.producer import emit_document_indexed
from app.services.documents import DocumentService

router = APIRouter(prefix="/documents")


@router.get("/{doc_id}")
async def get_document(doc_id: str) -> dict:
    service = DocumentService()
    return await service.fetch(doc_id)


@router.post("")
async def create_document(payload: dict) -> dict:
    service = DocumentService()
    doc = await service.store(payload)
    await emit_document_indexed(doc["id"])
    return doc
```

`fixtures/services/document_management/app/main.py`:
```python
from fastapi import FastAPI

from app.routes.documents import router as documents_router

app = FastAPI(title="document-management")
app.include_router(documents_router)
```

- [ ] **Step 6: Прогнать тест**

Run: `uv run pytest tests/unit/test_fixtures_valid.py -v`
Expected: PASS (3 passed)

- [ ] **Step 7: Commit**

```bash
git add fixtures/services tests/unit/test_fixtures_valid.py
git commit -m "feat(m0): three fixture microservices covering all M2 extractors"
```

---

### Task 9: workspace.yaml фикстур + golden-разметка

**Files:**
- Create: `fixtures/workspace.yaml`, `fixtures/golden/edges.yaml`, `fixtures/golden/traces.yaml`
- Test: `tests/unit/test_golden_wellformed.py`

**Interfaces:**
- Consumes: `load_workspace` (Task 4), фикстуры (Task 8).
- Produces: валидный workspace-конфиг фикстур; golden-файлы в формате, который M1/M2 eval будет читать: edge-записи `{src: {service, symbol}, type, dst: {service, symbol} | {channel}}`; trace-записи `{entrypoint, segments: [{service, entry: {symbol}, via_channel}]}`.

- [ ] **Step 1: Написать падающий тест**

`tests/unit/test_golden_wellformed.py`:
```python
from pathlib import Path

import yaml

from codegraph.config.loader import load_workspace

FIXTURES = Path(__file__).parents[2] / "fixtures"

EDGE_TYPES = {
    "CONTAINS", "IMPORTS", "CALLS", "HANDLES", "DEPENDS_ON",
    "PRODUCES", "CONSUMES", "INVOKES_ACTIVITY", "CALLS_HTTP",
}


def test_fixture_workspace_loads():
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    assert {s.name for s in cfg.services} == {
        "orders-api", "kyc-worker", "document-management",
    }
    orders = next(s for s in cfg.services if s.name == "orders-api")
    assert orders.idioms.producers[0].name == "outbox"
    assert cfg.processes[0].entrypoint == "orders-api:POST /orders"


def test_golden_edges_wellformed():
    data = yaml.safe_load((FIXTURES / "golden" / "edges.yaml").read_text())
    assert data["version"] == 1
    assert len(data["edges"]) >= 12
    for e in data["edges"]:
        assert e["type"] in EDGE_TYPES, e
        assert "service" in e["src"] and "symbol" in e["src"]
        assert ("channel" in e["dst"]) != ("symbol" in e["dst"])  # ровно одно


def test_golden_traces_reference_channels_from_edges():
    edges = yaml.safe_load((FIXTURES / "golden" / "edges.yaml").read_text())
    traces = yaml.safe_load((FIXTURES / "golden" / "traces.yaml").read_text())
    known_channels = {
        e["dst"]["channel"] for e in edges["edges"] if "channel" in e["dst"]
    }
    trace = traces["traces"][0]
    assert trace["entrypoint"] == "orders-api:POST /orders"
    assert len(trace["segments"]) == 3
    for seg in trace["segments"][1:]:
        assert seg["via_channel"] in known_channels
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_golden_wellformed.py -v`
Expected: FAIL (файлов нет)

- [ ] **Step 3: fixtures/workspace.yaml**

```yaml
version: 1
graph_name: fixtures
services:
  - name: orders-api
    path: ./services/orders_api
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
    path: ./services/kyc_worker
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
  - name: document-management
    path: ./services/document_management
    http: { base_url_env: DOCUMENT_MANAGEMENT_URL }
processes:
  - name: "Order KYC onboarding"
    entrypoint: "orders-api:POST /orders"
```

- [ ] **Step 4: fixtures/golden/edges.yaml**

```yaml
# Эталонные рёбра, размеченные вручную по коду fixtures/services.
# При изменении фикстур — обновлять синхронно. Формат dst: symbol XOR channel.
version: 1
edges:
  # --- orders-api: внутрисервисные CALLS и DI ---
  - src: {service: orders-api, symbol: app.routes.orders.create_order}
    type: CALLS
    dst: {service: orders-api, symbol: app.services.order.OrderService.place}
  - src: {service: orders-api, symbol: app.routes.orders.create_order}
    type: DEPENDS_ON
    dst: {service: orders-api, symbol: app.db.session.get_db}
  - src: {service: orders-api, symbol: app.routes.orders.get_order}
    type: DEPENDS_ON
    dst: {service: orders-api, symbol: app.db.session.get_db}
  - src: {service: orders-api, symbol: app.services.order.OrderService.place}
    type: CALLS
    dst: {service: orders-api, symbol: app.services.order.OrderService._persist}
  - src: {service: orders-api, symbol: app.services.order.OrderService.place}
    type: CALLS
    dst: {service: orders-api, symbol: app.db.outbox.OutboxRepository.add_event}
  # --- outbox producer → канал типа события ---
  - src: {service: orders-api, symbol: app.services.order.OrderService.place}
    type: PRODUCES
    dst: {channel: "chan:event_type:OrderCreated"}
  # --- kyc-worker: consumer, temporal, http-клиент ---
  - src: {service: kyc-worker, symbol: app.consumers.orders.handle_order_created}
    type: CONSUMES
    dst: {channel: "chan:event_type:OrderCreated"}
  - src: {service: kyc-worker, symbol: app.consumer_main.run_consumer}
    type: CONSUMES
    dst: {channel: "chan:kafka_topic:orders.events"}
  - src: {service: kyc-worker, symbol: app.consumers.orders.handle_order_created}
    type: CALLS
    dst: {service: kyc-worker, symbol: app.workflows.kyc.KycWorkflow.run}
  - src: {service: kyc-worker, symbol: app.workflows.kyc.KycWorkflow.run}
    type: INVOKES_ACTIVITY
    dst: {service: kyc-worker, symbol: app.activities.documents.verify_documents}
  - src: {service: kyc-worker, symbol: app.activities.documents.verify_documents}
    type: CALLS
    dst: {service: kyc-worker,
          symbol: app.clients.document_management_client.DocumentManagementClient.get_document}
  - src: {service: kyc-worker,
          symbol: app.clients.document_management_client.DocumentManagementClient.get_document}
    type: CALLS_HTTP
    dst: {channel: "chan:http:document-management:GET /documents/{doc_id}"}
  # --- document-management: HANDLES и builtin aiokafka producer ---
  - src: {service: document-management, symbol: app.routes.documents.get_document}
    type: HANDLES
    dst: {channel: "chan:http:document-management:GET /documents/{doc_id}"}
  - src: {service: document-management, symbol: app.routes.documents.create_document}
    type: CALLS
    dst: {service: document-management, symbol: app.events.producer.emit_document_indexed}
  - src: {service: document-management, symbol: app.events.producer.emit_document_indexed}
    type: PRODUCES
    dst: {channel: "chan:kafka_topic:documents.indexed"}
channels:
  - id: "chan:kafka_topic:orders.events"
    contains: ["chan:event_type:OrderCreated"]
```

Примечание к семантике HANDLES: в графе ребро направлено Channel→RouteHandler; в golden-файле src/dst записаны в порядке «код — канал» для единообразия читаемости, eval-загрузчик (M2) нормализует направление по типу ребра.

- [ ] **Step 5: fixtures/golden/traces.yaml**

```yaml
version: 1
traces:
  - entrypoint: "orders-api:POST /orders"
    segments:
      - service: orders-api
        entry: {symbol: app.routes.orders.create_order}
        via_channel: null
      - service: kyc-worker
        entry: {symbol: app.consumers.orders.handle_order_created}
        via_channel: "chan:event_type:OrderCreated"
      - service: document-management
        entry: {symbol: app.routes.documents.get_document}
        via_channel: "chan:http:document-management:GET /documents/{doc_id}"
```

- [ ] **Step 6: Прогнать тесты**

Run: `uv run pytest tests/unit/test_golden_wellformed.py -v`
Expected: PASS (3 passed)

- [ ] **Step 7: Commit**

```bash
git add fixtures/workspace.yaml fixtures/golden tests/unit/test_golden_wellformed.py
git commit -m "feat(m0): fixture workspace config and hand-labeled golden edges/traces"
```

---

### Task 10: CLI — init, index --dry-run, заглушки остального

**Files:**
- Modify: `src/codegraph/cli.py`
- Create: `codegraph.example.yaml`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: `load_workspace`, `effective_idioms`.
- Produces: команды `init` (пишет `codegraph.yaml` из шаблона `codegraph.example.yaml`, отказывается перезаписывать), `index --dry-run` (печатает стадии S1–S10 и таблицу сервисов с числом идиом; без `--dry-run` — выходит с сообщением "not implemented until M1", exit 2), заглушки `load/stats/trace/serve/eval` (exit 2 с честным сообщением "planned for M<n>").

- [ ] **Step 1: Написать падающие тесты**

`tests/unit/test_cli.py`:
```python
from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app

runner = CliRunner()
FIXTURES_WS = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"


def test_index_dry_run_lists_stages_and_services():
    result = runner.invoke(app, ["index", str(FIXTURES_WS), "--dry-run"])
    assert result.exit_code == 0, result.output
    for stage in ("S1", "S5", "S10"):
        assert stage in result.output
    assert "orders-api" in result.output
    assert "kyc-worker" in result.output


def test_index_without_dry_run_not_implemented():
    result = runner.invoke(app, ["index", str(FIXTURES_WS)])
    assert result.exit_code == 2
    assert "M1" in result.output


def test_init_writes_template_and_refuses_overwrite(tmp_path):
    result = runner.invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0
    cfg = tmp_path / "codegraph.yaml"
    assert cfg.exists() and "services:" in cfg.read_text()
    again = runner.invoke(app, ["init", str(tmp_path)])
    assert again.exit_code == 1
    assert "exists" in again.output


def test_stub_commands_exit_2():
    for cmd, milestone in [("stats", "M1"), ("trace", "M2"), ("serve", "M1"), ("eval", "M2")]:
        result = runner.invoke(app, [cmd])
        assert result.exit_code == 2, cmd
        assert milestone in result.output
```

- [ ] **Step 2: Запустить — падает**

Run: `uv run pytest tests/unit/test_cli.py -v`
Expected: FAIL (нет команд index/init/...)

- [ ] **Step 3: Дописать CLI**

Добавить в `src/codegraph/cli.py` (после команды `doctor`, перед `main`):

```python
STAGES = [
    ("S1", "discover", "конфиг / zero-config, валидация путей"),
    ("S2", "scan", "обход .py, sha256"),
    ("S3", "resolve", "scip-python per service"),
    ("S4", "read-scip", "protobuf → defs/refs"),
    ("S5", "parse+extract", "tree-sitter, идиомы → claims"),
    ("S6", "join", "SCIP refs × call-sites → CALLS"),
    ("S7", "link", "каналы, роуты, NEXT_SEGMENT, процессы"),
    ("S8", "chunk+embed", "AST-чанки + эмбеддинги"),
    ("S9", "load", "UNWIND-батчи → FalkorDB (blue/green)"),
    ("S10", "report", "качество графа"),
]

TEMPLATE = Path(__file__).parent.parent.parent / "codegraph.example.yaml"


@app.command()
def index(
    target: Path = typer.Argument(Path.cwd()),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Построить граф workspace (M0: только --dry-run)."""
    cfg = load_workspace(target)
    if not dry_run:
        console.print("[yellow]index is not implemented until M1; use --dry-run[/]")
        raise typer.Exit(2)
    from codegraph.config.loader import effective_idioms

    stage_table = Table(title=f"pipeline plan · graph={cfg.graph_name}")
    stage_table.add_column("stage")
    stage_table.add_column("name")
    stage_table.add_column("what")
    for sid, name, what in STAGES:
        stage_table.add_row(sid, name, what)
    console.print(stage_table)

    svc_table = Table(title="services")
    for col in ("service", "path", "producers", "consumers", "http_clients"):
        svc_table.add_column(col)
    for svc in cfg.services:
        idioms = effective_idioms(cfg, svc)
        svc_table.add_row(
            svc.name, str(svc.path),
            str(len(idioms.producers)), str(len(idioms.consumers)),
            str(len(idioms.http_clients)),
        )
    console.print(svc_table)


@app.command()
def init(target: Path = typer.Argument(Path.cwd())) -> None:
    """Создать codegraph.yaml из прокомментированного шаблона."""
    dest = target / "codegraph.yaml"
    if dest.exists():
        console.print(f"[red]{dest} already exists[/]")
        raise typer.Exit(1)
    dest.write_text(TEMPLATE.read_text())
    console.print(f"created {dest}")


def _stub(milestone: str) -> None:
    console.print(f"[yellow]planned for {milestone}[/]")
    raise typer.Exit(2)


@app.command()
def stats() -> None:
    """Статистика графа (M1)."""
    _stub("M1")


@app.command()
def load() -> None:
    """Загрузка в FalkorDB из staging (M1)."""
    _stub("M1")


@app.command()
def trace() -> None:
    """Трассировка бизнес-процесса (M2)."""
    _stub("M2")


@app.command()
def serve() -> None:
    """MCP-сервер (M1: v0)."""
    _stub("M1")


@app.command()
def eval() -> None:
    """Оценка качества графа/retrieval (M2)."""
    _stub("M2")
```

И `codegraph.example.yaml` в корне репо — скопировать содержимое примера из мастер-плана (`docs/superpowers/specs/2026-07-12-codegraph-design.md`, раздел «Конфиг»), добавив комментарий-шапку:
```yaml
# codegraph workspace config.
# Реестр сервисов (локальные чекауты) + идиомы producer/consumer/http-client.
# Идиомы — данные: опишите свой outbox/диспатчер здесь, код менять не нужно.
# Документация полей: docs/superpowers/specs/2026-07-12-codegraph-design.md
...
```
(вместо `...` — YAML из раздела «Конфиг (реестр сервисов + идиомы-как-данные)» спеки, с путями-примерами `../orders-api` и т.д.)

Внимание на `TEMPLATE`: путь `Path(__file__).parent.parent.parent` работает в editable-установке uv (src-layout: `src/codegraph/cli.py` → корень). Для M0 этого достаточно; упаковку шаблона в package-data решим при первой дистрибуции.

- [ ] **Step 4: Прогнать тесты**

Run: `uv run pytest tests/unit/test_cli.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Полный прогон и ruff**

Run: `uv run ruff check . && uv run pytest`
Expected: ruff — 0 ошибок; pytest — все unit PASS, integration (falkordb) PASS или skipped.

- [ ] **Step 6: Финальная верификация M0 (чек мастер-плана)**

```bash
docker compose up -d
uv run codegraph doctor            # все чеки зелёные (env + probes)
uv run codegraph index fixtures/workspace.yaml --dry-run   # план стадий + 3 сервиса
uv run pytest                      # зелёный
```
Expected: exit 0 везде; в dry-run таблице у orders-api producers ≥ 3 (outbox + 2 builtin aiokafka), у kyc-worker consumers ≥ 3.

- [ ] **Step 7: Commit**

```bash
git add src/codegraph/cli.py codegraph.example.yaml tests/unit/test_cli.py
git commit -m "feat(m0): cli init/index --dry-run, honest stubs for M1/M2 commands"
```
