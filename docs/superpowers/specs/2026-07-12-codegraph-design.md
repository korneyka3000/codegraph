# `codegraph` — Code Knowledge Graph RAG для Python-микросервисов: мастер-план

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) или superpowers:executing-plans. Это мастер-план (архитектура + вехи). Перед началом КАЖДОГО milestone — сгенерировать его детальный TDD-план через superpowers:writing-plans (bite-sized задачи с кодом), используя этот файл как спеку.

**Goal:** Построить инструмент (CLI-индексатор + MCP-сервер), который из локальных чекаутов Python-микросервисов строит детерминированный граф знаний кода и позволяет LLM-агенту трассировать кросс-сервисные бизнес-процессы через async-границы (route → … → outbox → Kafka-канал → consumer → Temporal activity → HTTP → другой сервис).

**Architecture:** Конвейер стадий: tree-sitter (структура) + scip-python (резолв символов, SCIP protobuf) → span-join → узлы/рёбра с пометкой `resolution/confidence` → linking через контрактные `Channel`-узлы → AST-чанкинг с контекстной аугментацией → эмбеддинги. Промежуточное состояние — в staging-кэше (SQLite-файл пайплайна, НЕ хранилище графа); serving — FalkorDB (граф + HNSW vector + fulltext в одном контейнере) за интерфейсами `GraphStore`/`VectorStore`. Агент получает типизированные MCP-инструменты, никакого text-to-Cypher.

**Tech Stack:** Python 3.12+, uv, typer, fastmcp, pydantic, tree-sitter, vendored SCIP protobuf, FalkorDB (docker compose), sentence-transformers (default) / Voyage / OpenAI (extras), pytest (+testcontainers). Внешняя зависимость: Node.js для `npx @sourcegraph/scip-python@<pinned>`.

## Global Constraints

- Python ≥ 3.12; менеджер — uv; рабочее имя пакета `codegraph` (переименование — решение пользователя, тривиально).
- Zero-config режим обязателен: `codegraph index <repo>` без YAML = один сервис, builtin-идиомы.
- Кросс-сервисные рёбра между кодовыми узлами запрещены — только через `Channel`-узлы (валидация при вставке в staging). Исключение — derived `NEXT_SEGMENT` с обязательным `via_channel_id`.
- На каждом ребре: `resolution ∈ {static, dynamic, heuristic, trace_validated}` + `confidence: float`.
- `CALLS` — только истинный синхронный вызов (не containment/usage) — главный урок спеки.
- Идиомы producer/consumer/client — данные (pydantic-модели из YAML), builtin-идиомы — экземпляры тех же моделей: пользователь доконфигурирует паттерн без изменения кода.
- Весь Cypher — только внутри `stores/falkordb/` (3 файла); многошаговые обходы — итеративный BFS в Python.
- MCP-агенту не выдаётся raw-query доступ; `GraphStore.raw()` — internal-only.

---

## Контекст

Пользователь принёс спеку-исследование (`compass_artifact_wf-65dc1b6b-…_text_markdown.md` в корне репо) о построении Graph RAG для Python-микросервисов (KYC-домен) и попросил: ревью спеки, поиск улучшений, проектирование реализации. Директория `ast-tree-multi` пуста (только спека) — greenfield.

Цель проекта: детерминированный multi-hop по бизнес-процессам через границы сервисов и брокеров — ниша, где grep+LSP-агент и vector-RAG объективно проваливаются (спека честно это аргументирует). Инструмент должен работать и на одиночном Python-репо, но целевой сценарий — микросервисная архитектура.

**Выводы спеки, принятые за основу:** symbol resolution — главный риск; scip-python как основной резолвер; строгое разделение семантики рёбер; Temporal-воркфлоу как first-class якоря процессов; типизированные инструменты вместо text-to-Cypher; contextual retrieval для чанков; KùzuDB исключена (заархивирована окт-2025).

**Поправки к спеке по итогам ревью (реализуются этим планом):**
1. SCIP не даёт call graph напрямую — CALLS = join SCIP reference-occurrences × tree-sitter call-sites по span (спека этот шаг пропустила).
2. Добавлен HTTP-ститчинг сервис→сервис (`CALLS_HTTP` через рукописные aiohttp-клиенты `*Client`) — крупнейшая содержательная дыра спеки.
3. Стабильные ID узлов = SCIP-дескрипторы, не file+span (иначе ломается инкрементальность и привязка чанков).
4. Мульти-репо модель: реестр сервисов в конфиге; кросс-сервисные связи только через контрактные узлы.
5. Обход — в приложении (BFS), хранилище — тупой store: миграция движка = замена адаптера.
6. Расширен набор MCP-инструментов (+`find_paths`, `who_calls`, `list_processes`, `search_code`, `graph_stats`).
7. Хранилище v1 — сразу FalkorDB (минуя Postgres-этап спеки): пользователь отверг SQLite/Postgres как основное хранилище; FalkorDB — целевая точка самой спеки.
8. Outbox pattern пользователя (бизнес-код пишет в outbox-таблицу, отдельный relay-под публикует): PRODUCES снимается с точки записи события в бизнес-коде через конфиг-идиом; relay статически невидим и не нужен.
9. Каналы обобщены: `kafka_topic` и `event_type` — оба вида `Channel` + ребро `CONTAINS` topic→event_type (у пользователя диспатч и по топику, и по типу события, по-разному в сервисах).

**Решения пользователя (Q&A):** стек = Kafka (aiokafka, через outbox) + HTTP сервис→сервис (aiohttp-клиенты-SDK вида `document_management_client.py` → `class DocumentManagementClient`) + Temporal; RabbitMQ нет. Форма = CLI + MCP. Хранилище делегировано мне → FalkorDB за абстракцией. Сервисы = отдельные git-репо, локальные чекауты. Нюансы диспатча консьюмеров — на конфиг/человека (не автомагия).

---

## Структура пакета

```
ast-tree-multi/
├── pyproject.toml                  # uv; extras: local-emb, openai, voyage, dev
├── docker-compose.yml              # falkordb/falkordb:<pinned>, volume, 6379 (+3000 browser)
├── codegraph.example.yaml          # прокомментированный пример конфига
├── scripts/gen_scip_proto.sh       # regen scip_pb2.py из vendored scip.proto (dev-only)
├── src/codegraph/
│   ├── cli.py                      # typer: init | index | load | doctor | stats | trace | serve | eval
│   ├── config/
│   │   ├── models.py               # pydantic: WorkspaceConfig, ServiceConfig, Idiom-модели, ProcessDecl
│   │   ├── loader.py               # поиск codegraph.yaml, zero-config синтез, merge builtin-идиом
│   │   └── builtin_idioms.py       # aiokafka/faststream/confluent/fastapi/temporal/aiohttp_client как данные
│   ├── core/
│   │   ├── schema.py               # label'ы/типы рёбер, IR: NodeRec/EdgeRec/Claims; инварианты (запрет кросс-сервис code→code)
│   │   ├── ids.py                  # стабильные ID из SCIP-символов, local-символы
│   │   ├── spans.py                # SCIP (line, utf8/16-col) ↔ байтовые оффсеты tree-sitter
│   │   └── errors.py               # per-service degradable ошибки
│   ├── resolvers/
│   │   ├── base.py                 # SymbolResolver Protocol + DefRec/RefRec
│   │   ├── fallback.py             # эвристика: import-граф + имена; resolution=heuristic, conf=0.6
│   │   └── scip/                   # scip_pb2.py (vendored) | runner.py (npx, venv-env, timeout, кэш) | symbols.py | reader.py
│   ├── parsing/
│   │   ├── ts.py                   # tree-sitter сессия + скомпилированные Query (defs/calls/decorators/imports)
│   │   ├── facts.py                # FileFacts: def-иерархия, call-sites, декораторы, строковые аргументы
│   │   └── consts.py               # частичный резолв констант: литералы, class-attrs, f-string→шаблон, env→config_ref
│   ├── extractors/                 # base.py (Protocol+registry) | python_core | calls | fastapi | kafka | temporal | http_client
│   ├── linking/                    # channels (унификация) | http_routes (матчинг) | segments (NEXT_SEGMENT) | processes
│   ├── chunking/                   # base | splitter (greedy sibling-merge по AST) | augment (контекст-header)
│   ├── embedding/                  # base | local (jina-embeddings-v2-base-code) | openai_emb | voyage
│   ├── stores/
│   │   ├── graph.py / vector.py    # Protocols
│   │   ├── staging.py              # SQLite staging-кэш пайплайна (файлы/символы/рёбра/чанки; фундамент M4)
│   │   └── falkordb/               # store.py | ddl.py (индексы+feature-probes) | batch.py (UNWIND, бисекция ошибок)
│   ├── pipeline/                   # runner.py (оркестрация, кэш, изоляция ошибок) | stages.py (S1..S10) | report.py
│   ├── query/                      # api.py (типизированный слой) | traverse.py (BFS trace) | retrieval.py (RRF-гибрид)
│   └── mcp/                        # server.py (FastMCP, 9 инструментов) | schemas.py (pydantic контракты)
├── fixtures/
│   ├── workspace.yaml              # 3 фикстурных сервиса + кастомные идиомы + процесс
│   ├── golden/edges.yaml, traces.yaml
│   └── services/orders_api | kyc_worker | document_management
└── tests/unit | integration | eval
```

## Модель данных

**Узлы.** Кодовые: `:Sym` + kind-label (`Module|Class|Function`) + роли-label'ы (`RouteHandler|MessageConsumer|MessageProducer|TemporalWorkflow|TemporalActivity`). Свойства `:Sym`: `id`, `kind`, `name`, `qualified_name`, `service`, `file`, `start/end_line`, `start/end_byte`, `content_hash`, `signature`, `docstring`, `is_async`; ролевые: `http_method`+`path_template` (учёт `APIRouter(prefix=)`), `workflow_name`, `activity_name`, `idiom`. Некодовые: `Channel {id, kind: kafka_topic|event_type|http_route, name, owner_service?, http_method?, path_template?, unresolved?, config_ref?}`, `Service`, `BusinessProcess {entrypoint_id, source: config|temporal}`, `Chunk {id=<sym_id>#c<N>, symbol_id, ordinal, text, context_header, embedding, content_hash, embed_model}`, `Meta {schema_version, embed_model, embed_dim, config_hash}`. Мульти-label — под feature-probe; fallback: один label + `roles: []`.

**Рёбра** (все несут `resolution`, `confidence`, `extractor`, `evidence_file:line`):

| Ребро | Направление | Особенности |
|---|---|---|
| CONTAINS | Service→Module→Class→Function; **Channel(topic)→Channel(event_type)** | покрывает двойной диспатч |
| IMPORTS | Module→Module | сверка с SCIP Import-роли |
| CALLS | Function→Function | `callsite_count`; `start_workflow` → `mechanism: temporal_start, resolution: dynamic` |
| HANDLES | Channel(http_route)→RouteHandler | |
| DEPENDS_ON | RouteHandler/Function→Function | FastAPI DI: `Depends`/`Annotated[..., Depends]` |
| PRODUCES / CONSUMES | Function→Channel | consumer: `dispatch: topic|event_type` |
| INVOKES_ACTIVITY | TemporalWorkflow→TemporalActivity | только внутрисервисный резолв; кросс-сервисные имена — в doctor-report |
| CALLS_HTTP | Function→Channel(http_route) | `http_method`, `path_template` |
| NEXT_SEGMENT | exit-узел→entry-узел следующего сегмента | **только derived** (linking/segments.py), обязательный `via_channel_id`, conf = произведение пары |
| PART_OF_PROCESS | entry-узел сегмента→BusinessProcess | `order: int`, derived |

**Индексы FalkorDB** (`ddl.py`, до загрузки): range-индексы + UNIQUE-констрейнты на `Sym.id`, `Channel.id`, `Chunk.id`; range на `qualified_name`, `service`, `Channel.kind/.name`, `Chunk.symbol_id`. Vector: `CREATE VECTOR INDEX FOR (ch:Chunk) ON (ch.embedding) OPTIONS {dimension: $dim, similarityFunction: 'cosine'}` (probe: нет cosine → euclidean + L2-нормализация). Fulltext: `db.idx.fulltext.createNodeIndex('Chunk','text','context_header')` и `('Sym','name','qualified_name','docstring')`.

## Пайплайн индексации (S1–S10)

Артефакты в `.codegraph/` (staging.db, `scip/<svc>.scip`, report.json). Staging — служебный кэш: FalkorDB всегда пересоздаваем из него (`codegraph load --from-staging`) — снимает риск in-memory природы Redis.

- **S1 discover** — конфиг или zero-config синтез; валидация путей/venv.
- **S2 scan** — обход `.py` (pathspec: .gitignore + excludes), sha256 → таблица files.
- **S3 resolve** — per-service: `npx --yes @sourcegraph/scip-python@<pinned> index . --project-name <svc> --output <cache>/svc.scip`, `cwd=service.path`, env `VIRTUAL_ENV=<venv>, PATH=<venv>/bin:$PATH, NODE_OPTIONS=--max-old-space-size=8192` (эквивалент активации venv — так pyright видит зависимости). Кэш по merkle-hash дерева. Таймаут (def. 20 мин) + kill process group. Ошибка → сервис `degraded` + fallback-резолвер, остальные не страдают.
- **S4 read SCIP** — из `Index.documents[]`: `relative_path`, `position_encoding`, `occurrences[]` (`range` 3-или-4 int32, 0-based, конец эксклюзивен → нормализация в 4), `symbol_roles` (бит 1=Definition, 2=Import), `symbols[]` (`SymbolInformation.symbol/kind/enclosing_symbol/documentation`). **Конверсия колонок**: SCIP считает в code units указанной кодировки (UTF-16 у pyright!), tree-sitter — в байтах; `spans.py` строит per-line конвертер (критично: кириллица в докстрингах). Symbol-строка: `local N` либо `scip-python python <pkg> <ver> <descriptors>`; дескрипторы (`` `mod.sub`/Class#method(). ``) — канонический хвост. Результат: пер-файловые таблицы `scip_defs` / `scip_refs` в staging — они же фундамент инкрементальности M4.
- **S5 parse+extract** — один tree-sitter-парс на файл, общий `FileFacts` для всех экстракторов. `python_core` → Module/Class/Function + CONTAINS + IMPORTS + docstrings. `calls` → call-sites (query по `(call function: [identifier|attribute])`), позиция name-токена callee + охватывающий def. Доменные экстракторы матчат декораторы/вызовы против `Idiom`-списка (builtin+конфиг), строки — через `consts.py` (литерал; module-константа; `settings.X`/`os.environ` → `config_ref`; f-string → `{param}`-шаблон). Эмитят **claims** (ChannelClaim/RouteClaim/HttpCallClaim), не рёбра.
- **S6 join** — CALLS: lookup `refs_by_pos[(path, line, col)]` для callee-токена, промах → bisect по включению интервалов; caller через `scip_defs`. Дубли → `callsite_count`. Внешние пакеты отбрасываются (счётчик в report). Claims привязываются к символам так же.
- **S7 link** — унификация `Channel` по нормализованному имени между сервисами; `CONTAINS` topic→event_type; таблица роутов всех сервисов + матчинг HttpCallClaim (метод + посегментно: статика строго, `{param}` wildcard; кандидаты сужаются через `http.base_url_env`-маппинг); промах → `Channel(unresolved=true, conf=0.5)` в doctor-report. Деривация NEXT_SEGMENT из пар (PRODUCES→C, C←CONSUMES) и (CALLS_HTTP→C, C−HANDLES→H). BusinessProcess: из конфига + каждый TemporalWorkflow; PART_OF_PROCESS с order.
- **S8 chunk+embed** — после link (аугментация использует граф-позицию): header = `file · service · родительский класс/сигнатура · импорты · docstring · kind + типы рёбер` (напр. «RouteHandler POST /orders; produces event_type OrderCreated»); эмбеддится `header + "\n\n" + text`. Кэш по `content_hash` + модель.
- **S9 load** — UNWIND-батчи (1000, группировка по label-набору и (edge_type, src_label, dst_label)): `UNWIND $rows AS r MERGE (n:Sym:Function:RouteHandler {id: r.id}) SET n += r.props`; рёбра через двойной MATCH+MERGE. Ошибка батча → бисекция до кривой строки. **Blue/green**: пишем в `<graph>__build`, атомарный Redis `RENAME` (граф = redis-ключ).
- **S10 report** — counts, % heuristic, % orphan, нерезолвленные каналы/URL, degraded-сервисы → rich-stdout + report.json (это и есть `doctor`-метрики качества графа).

**Стабильные ID** (`core/ids.py`): кодовый узел = `sym:<service>:<scip-descriptors-без-pkg/version>`; SCIP-`local N` → `sym:<svc>:<relpath>:<enclosing>:<name>@local`; каналы `chan:kafka_topic:<name>` / `chan:event_type:<name>` / `chan:http:<owner|?>:<METHOD> <template>`; `content_hash` — только детекция изменений, в ID не входит.

## Интерфейсы (Protocols)

```python
class SymbolResolver(Protocol):   # scip-python | fallback | (будущее: basedpyright, stack-graphs)
    name: str
    def resolve(self, service: ServiceCtx, files: Sequence[FileRec], cache_dir: Path) -> ResolveResult: ...
    # ResolveResult: iter_defs()/iter_refs() -> DefRec/RefRec(path, symbol, span_bytes, roles); degraded: bool

class Extractor(Protocol):
    name: str
    def extract(self, ctx: FileContext) -> ExtractionResult: ...
    # FileContext: service, path, source: bytes, tree, facts: FileFacts, consts: ConstTable,
    #              idioms: list[Idiom], resolve_span: Callable[[Span], str | None]
    # ExtractionResult: nodes: [NodeRec], edges: [EdgeRec], claims: [ChannelClaim|RouteClaim|HttpCallClaim]

class Chunker(Protocol):
    def chunk(self, ctx: FileContext, symbols: Sequence[NodeRec]) -> list[ChunkRec]: ...

class Embedder(Protocol):
    model_id: str; dim: int
    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]: ...

class GraphStore(Protocol):
    def ensure_schema(self, meta: GraphMeta) -> None: ...
    def upsert_nodes(self, labels: tuple[str, ...], rows: Sequence[dict]) -> int: ...
    def upsert_edges(self, edge_type: str, src_label: str, dst_label: str, rows: Sequence[dict]) -> int: ...
    def get_nodes(self, ids: Sequence[str]) -> list[dict]: ...
    def neighbors(self, node_id: str, edge_types: Sequence[str] | None,
                  direction: Literal["out","in","both"], limit: int) -> list[Hop]: ...  # единственный примитив BFS
    def stats(self) -> GraphStats: ...
    def swap_in(self, build_name: str) -> None: ...   # blue/green
    def raw(self, cypher: str, params: dict) -> Any: ...  # internal-only

class VectorStore(Protocol):
    def ensure_indexes(self, dim: int) -> None: ...
    def upsert_chunks(self, rows: Sequence[dict]) -> int: ...
    def search_vector(self, vec, k, flt) -> list[ScoredChunk]: ...
    def search_text(self, query, k, flt) -> list[ScoredChunk]: ...
# FalkorGraphStore реализует оба протокола над одним графом.
```

## Конфиг (реестр сервисов + идиомы-как-данные)

```yaml
version: 1
graph_name: kyc
storage: { falkordb: { host: localhost, port: 6379 } }
embedding: { provider: local, model: jinaai/jina-embeddings-v2-base-code }
scip: { timeout_min: 20, node_options: "--max-old-space-size=8192" }

services:
  - name: orders-api
    path: ../orders-api          # локальный чекаут
    python: .venv                # venv для scip-python (autodetect)
    exclude: ["tests/**", "alembic/**"]
    http: { base_url_env: ORDERS_API_URL }   # так другие сервисы находят этот
    idioms:
      producers:
        - name: outbox                                   # кастомный outbox-идиом — чисто данные
          call: "app.db.outbox.OutboxRepository.add_event"
          channel:
            kind: event_type
            event_type_from: { arg: 0 }                  # или {kwarg: event_type}
            topic: { const: "orders.events" }            # → CONTAINS topic→event_type
  - name: kyc-worker
    path: ../kyc-worker
    idioms:
      consumers:
        - name: dispatch-map                             # диспатч по dict[event_type → handler]
          kind: dispatch_dict
          registrar_call: "app.consumers.base.register_handlers"
          topic: { const: "orders.events" }
          event_type_from: dict_key
      http_clients:
        - name: default-sdk                              # уточнение builtin-конвенции *Client
          file_glob: "**/clients/*_client.py"
          class_glob: "*Client"
          base_url: { attr: "self._base_url", env: DOCUMENT_MANAGEMENT_URL }
  - name: document-management
    path: ../document-management
    http: { base_url_env: DOCUMENT_MANAGEMENT_URL }      # builtin-идиом достаточно

builtin_idioms: [fastapi, aiokafka, faststream, confluent, temporal, aiohttp_client]
processes:
  - name: "Order KYC onboarding"
    entrypoint: "orders-api:POST /orders"
```

## MCP-инструменты и trace_process

`codegraph serve` (FastMCP, stdio; `--http` опц.). Все входы/выходы — pydantic-схемы, ссылки — стабильные `id`, у всех ответов `limit/truncated`:
`find_entrypoint(query, kinds?, service?, k=5)` · `search_code(query, k=8, service?, mode=hybrid)` (RRF k=60 поверх vector+fulltext) · `get_source(node_id, context_lines=0)` (с диска; несовпадение `content_hash` → `stale: true`) · `expand_neighbors(node_id, edge_types?, direction, depth=1, limit=50)` · `who_calls(node_id, transitive=false, max_depth=3)` · `find_paths(from_id, to_id, max_hops=8)` (двунаправленный BFS) · `trace_process(entrypoint_id, direction=downstream, max_segments=12, min_confidence=0.3)` · `list_processes()` · `graph_stats()`.

Правила `trace_process` (таблица переходов в `traverse.py`): внутри сегмента — out-рёбра CALLS/DEPENDS_ON/INVOKES_ACTIVITY/CALLS(temporal_start), глубина ≤15, ветвление ≤8 с `truncated`; выход — PRODUCES→Channel, CALLS_HTTP→Channel; через канал — CONSUMES (in), topic⇄event_type по CONTAINS в обе стороны, HANDLES→RouteHandler; вход нового сегмента — MessageConsumer/RouteHandler/TemporalWorkflow. Быстрый путь — готовые NEXT_SEGMENT (канал восстанавливается по `via_channel_id`). Выход: `segments[]{service, entry, steps[], exits[{channel, next_segment_ids}]}` + агрегированная confidence; циклы отсекаются по visited-id. CLI-зеркало: `codegraph trace --format mermaid`.

## Milestones (каждый — отдельный TDD-план при исполнении)

**M0 — скелет, инфраструктура, фикстуры.** uv-проект, CLI-каркас, config-модели+loader (весь Idiom-DSL), vendored scip_pb2.py + скрипт генерации, docker-compose, `doctor` (node/npx, scip-python `--version`, ping FalkorDB + feature-probes: multi-label, `SET +=`, constraints, vector cosine, fulltext), 3 фикстурных сервиса (orders_api: FastAPI+Depends+кастомный outbox; kyc_worker: dispatch-dict consumer+Temporal+aiohttp SDK; document_management: FastAPI-цель SDK+builtin aiokafka) + golden YAML.
✅ `docker compose up -d && uv run codegraph doctor` зелёный; `codegraph index fixtures/workspace.yaml --dry-run` печатает план стадий; `pytest tests/unit` зелёный.

**M1 — статическое ядро одного сервиса.** S1–S6 + S9/S10 для Module/Class/Function, CONTAINS/IMPORTS/CALLS; staging; fallback-резолвер; `stats`; MCP v0 (graph_stats, get_source, expand_neighbors, who_calls).
✅ Zero-config индекс `fixtures/services/document_management`; `pytest tests/eval -k calls`: precision ≥ 0.95, recall ≥ 0.85 по golden CALLS; MCP виден из Claude Code.

**M2 — экстракторы, каналы, кросс-сервис, трассировка (ядро ценности).** fastapi/kafka(+outbox)/temporal/http_client, consts.py, весь linking, trace_process/find_paths/list_processes, find_entrypoint (пока fulltext-режим), quality-report.
✅ `codegraph trace "orders-api:POST /orders"` печатает полную цепочку route → create_order → OutboxRepository.add_event → Channel(event_type OrderCreated) → kyc-worker handler → KycWorkflow → verify_documents → DocumentManagementClient.get_document → Channel(http GET /documents/{id}) → handler document-management; `pytest tests/eval`: precision/recall = 1.0 по PRODUCES/CONSUMES/CALLS_HTTP/INVOKES_ACTIVITY/DEPENDS_ON/HANDLES на фикстурах; тест «идиома-как-конфиг» зелёный.

**M3 — retrieval-слой.** Chunker + аугментация + Embedder'ы (local/openai/voyage), vector+fulltext индексы, RRF-гибрид, полные search_code/find_entrypoint, кэш эмбеддингов, `codegraph eval retrieval` (заготовка golden-questions harness).
✅ `search_code("где создаётся заказ и пишется событие в outbox")` → чанк create_order/add_event в top-3 (pytest); повторный index → `re-embedded: 0`; contract-тест всех 9 MCP-инструментов.

**M4 — инкрементальность, hardening, пилот.** Dirty-set по sha256; инвалидация по затронутым символам (изменённый файл → его defs + все refs на них → инцидентные рёбра); re-embed только изменённых чанков; `index --incremental` (fallback full); параллелизм S5/S8; README; **пилот на 1–2 реальных сервисах + честное сравнение с grep+LSP-baseline на золотом наборе вопросов** (методология спеки, Этап 0 — сознательно перенесён сюда: фикстуры дают ground truth раньше и дешевле).
✅ Правка 1 файла фикстуры → `--incremental` логирует counts, время <20% полного; дамп графа после инкремента == дамп после full reindex; реальный сервис ~50k LOC индексируется за минуты; report.json с % heuristic.

**После M4 (опции, вне плана):** LLM-сводки узлов Module/Service/Process (обогащение retrieval, с prompt caching); OTel-трейсы → `trace_validated`; Neo4j/embedded-адаптеры; stack-graphs как инкрементальный резолвер; мульти-язычность.

## Пороги смены решений (из спеки, сохраняются)

- Доля `heuristic` CALLS > ~20% → усиливать резолвер, не расширять фичи.
- grep+LSP бьёт граф на золотом наборе (M4) → остановить расширение графа, вкладываться в инструменты агента.
- Переиндексация коммита > нескольких секунд после M4 → рассмотреть stack-graphs.

## Риски и mitigation

1. **scip-python падает/виснет на больших деревьях** → pinned-версия, таймаут+kill group, `NODE_OPTIONS=--max-old-space-size=8192`, excludes, кэш .scip по merkle-hash, per-service изоляция + fallback-резолвер (conf 0.6), `doctor --probe-scip`.
2. **Cypher-покрытие FalkorDB** (нет APOC, диалект) → весь Cypher в 3 файлах адаптера, запросы тривиальны (UNWIND/MERGE/1-hop), multi-hop в Python BFS, feature-probes с fallback-ветками, интеграционные тесты против pinned-образа; путь отступления — Neo4j-адаптер за тем же Protocol.
3. **URL-резолв aiohttp-клиентов** → трёхуровневый (f-string→шаблон; consts/env→config_ref; матчинг по методу+сегментам с сужением через base_url_env); промах → Channel(unresolved) в doctor, лечится строкой конфига.
4. **Venv'ы сервисов для scip-python** → `python:` в конфиге/autodetect, env-подмена = активация, doctor проверяет интерпретатор и ключевые пакеты по идиомам; нет venv → degraded (first-party резолв работает).
5. **SCIP-позиции (UTF-16) vs байты tree-sitter** → per-line конвертер, property-тесты с кириллицей/эмодзи, fallback join по включению интервалов, несджойненные call-sites — метрика в report.
6. **In-memory природа FalkorDB** → volume + граф всегда восстановим из staging (`load --from-staging`). Смена embed-модели → mismatch с Meta → явная ошибка. Эволюция схемы → schema_version, mismatch → полный реиндекс.

## Верификация (end-to-end)

`tests/eval/` (маркер `falkordb`, testcontainers или `docker compose -p codegraph-test`):
1. `codegraph index fixtures/workspace.yaml --graph test_kyc` → exit 0, counts по report.json.
2. Precision/recall по `golden/edges.yaml` на каждый тип ребра = 1.0 (фикстуры малы — эталон полный).
3. MCP-цепочка in-memory `fastmcp.Client`: list_tools (9, схемы валидны) → `find_entrypoint("create order")` → id роута POST /orders → `trace_process(id)` → посегментное сравнение (entry_id, via_channel_id) с `golden/traces.yaml` — дословно целевой сценарий через outbox/Kafka/Temporal/HTTP.
4. Zero-config: индекс одиночного фикстурного сервиса без YAML → CALLS/роуты/builtin-producer присутствуют.
5. «Идиома-как-конфиг»: индекс с вырезанным outbox-идиомом → PRODUCES и хвост трейса исчезают (assert), с полным конфигом — трейс полный. Прямое доказательство «человек доконфигурирует паттерн без кода».
6. CLI smoke: doctor/stats/trace --format mermaid (typer CliRunner); get_source возвращает код с диска и честный `stale` после правки.
7. Финальная проверка M2+: подключить MCP к Claude Code и вживую спросить «проследи, что происходит после POST /orders» — агент должен собрать кросс-сервисную цепочку из trace_process.

## Порядок исполнения

На каждый milestone: сгенерировать детальный TDD-план (superpowers:writing-plans, bite-sized шаги с кодом) из этого мастер-плана → исполнение через superpowers:subagent-driven-development (рекомендовано) или executing-plans → верификация milestone-чеков → следующий. Git: инициализировать репозиторий в M0, частые коммиты.
