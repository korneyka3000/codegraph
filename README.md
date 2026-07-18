# codegraph

CLI-индексатор + MCP-сервер: граф знаний кода для Python-микросервисных бэкендов.

## Что это

`codegraph` строит **детерминированный** граф знаний кода из локальных чекаутов
Python-микросервисов. Пайплайн: tree-sitter (структура) + `scip-python` (резолв
символов через SCIP protobuf) → джойн по спанам → узлы и рёбра, причём у каждого
ребра явно помечены `resolution` (`static` / `dynamic` / `heuristic`) и
`confidence`. Ребро `CALLS` — это всегда настоящий синхронный вызов, полученный
пересечением SCIP-резолва и tree-sitter call-site, а не эвристика по близости
эмбеддингов или совпадению имён.

Кросс-сервисные связи — единственный легальный мост между разными сервисами —
контрактные узлы `Channel` (Kafka-топик, `event_type`, HTTP-роут); прямое ребро
между кодом двух разных сервисов запрещено на уровне записи в граф. Producer/
consumer/HTTP-client конвенции сервиса — это **конфиг, а не код**: builtin-идиомы
покрывают aiokafka/faststream/confluent/FastAPI/Temporal/аккуратные aiohttp-SDK
из коробки, а свой outbox или нестандартный dispatch описывается несколькими
строками YAML (см. «Идиомы — это конфиг» ниже). Так `PRODUCES`/`CONSUMES`,
`CALLS_HTTP` и derived-рёбра `NEXT_SEGMENT`/`PART_OF_PROCESS` вместе дают
трассировку бизнес-процесса через асинхронные границы: route → outbox →
Kafka-топик → consumer → Temporal-активность → HTTP-клиент → другой сервис —
без единого прямого импорта между репозиториями.

Поверх графа — retrieval-слой (AST-чанкинг с контекстной аугментацией + локальный/
OpenAI/Voyage эмбеддер + гибридный fulltext+vector поиск с RRF-фьюжном) и
MCP-сервер с 9 типизированными read-only инструментами: агент получает
структурированные ответы (`{"error": ...}` вместо исключений, `limit`/`truncated`
у всех ответов), а не text-to-Cypher и не прямой доступ к графовой БД. CLI
дублирует ключевые запросы (`stats`/`trace`/`eval retrieval`) для использования
вне агента.

## Требования

- Python ≥ 3.12, [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose — FalkorDB (граф + HNSW vector + fulltext в одном
  контейнере, `docker-compose.yml`)
- Node.js/npx — для `scip-python` (символьный резолвер); без него сервисы просто
  деградируют в эвристический fallback (см. «Режимы деградации»), пайплайн не падает
- (опционально) `OPENAI_API_KEY` / `VOYAGE_API_KEY`, если не используете локальный
  эмбеддер по умолчанию

## Быстрый старт

```bash
# 1. FalkorDB
docker compose up -d

# 2. Зависимости
uv sync
# локальный эмбеддер по умолчанию (jinaai/jina-embeddings-v2-base-code,
# sentence-transformers) — отдельный extra, потому что тянет torch:
uv sync --extra local-emb
# без этого шага `index`/`serve` всё равно работают -- эмбеддинг-шаг молча
# деградирует в "граф без векторов" (см. "Режимы деградации" ниже)

# 3. Проверить окружение и FalkorDB
uv run codegraph doctor

# 4. Реестр сервисов -- codegraph.yaml из прокомментированного шаблона
uv run codegraph init .
# дальше прописать в нём свои сервисы (path -- локальные чекауты рядом) и,
# при необходимости, идиомы -- см. "Идиомы -- это конфиг" ниже,
# codegraph.example.yaml и fixtures/workspace.yaml как рабочие примеры

# 5. Построить граф (план стадий без записи -- `--dry-run`)
uv run codegraph index codegraph.yaml --dry-run
uv run codegraph index codegraph.yaml

# 6. MCP-сервер (stdio)
uv run codegraph serve codegraph.yaml
```

Регистрация в Claude Code (`claude mcp add`, form проверена живьём в этом
окружении):

```bash
claude mcp add -s user codegraph -- \
  uv run --directory <абсолютный-путь-к-репо> codegraph serve <абсолютный-путь>/fixtures/workspace.yaml
```

### Попробовать без своих сервисов

В репозитории есть готовые фикстуры (`fixtures/workspace.yaml`) — три демо-
сервиса (`orders-api`/`kyc-worker`/`document-management`), тот самый order→KYC
сценарий из примера ниже. Посмотреть план пайплайна без записи можно сразу:

```bash
uv run codegraph index fixtures/workspace.yaml --dry-run
```

## Пример: трассировка бизнес-процесса

```bash
uv run codegraph trace "orders-api:POST /orders" fixtures/workspace.yaml --format mermaid
```

Реальный вывод (после `codegraph index fixtures/workspace.yaml`, FalkorDB поднят):

```mermaid
flowchart TD
    S0["orders-api: app.routes.orders.create_order"]
    S1["kyc-worker: app.consumer_main.run_consumer"]
    S2["kyc-worker: app.consumers.orders.handle_order_created"]
    S3["document-management: app.routes.documents.get_document"]
    S0 -->|OrderCreated| S1
    S0 -->|OrderCreated| S2
    S2 -->|GET /documents/{doc_id}| S3
```

`POST /orders` → `create_order` пишет в outbox (`OutboxRepository.add_event`,
идиом ниже) → канал `event_type:OrderCreated` → оба kyc-worker-сегмента, которым
он интересен (топик-диспетчер `run_consumer` и конкретный обработчик
`handle_order_created`) → `handle_order_created` дергает
`DocumentManagementClient` (HTTP) → роут `GET /documents/{doc_id}` в
`document-management`. Без `--format mermaid` та же трассировка печатается
rich-деревом сегментов (формат по умолчанию, `--format text`).

## Идиомы — это конфиг

Фрагмент `fixtures/workspace.yaml` — кастомный outbox producer, без единой
строчки кода в `codegraph`:

```yaml
services:
  - name: orders-api
    path: ./services/orders_api
    idioms:
      producers:
        - name: outbox
          call: "app.db.outbox.OutboxRepository.add_event"
          channel:
            kind: event_type
            event_type_from: { arg: 0 }
            topic: { const: "orders.events" }
```

Бизнес-код вызывает `OutboxRepository.add_event(event_type, payload)`; отдельный
relay-под публикует в Kafka асинхронно и статически невидим — `codegraph` его
никогда не увидит и не должен. Идиом называет ровно точку вызова и то, откуда
взять идентичность канала (`arg: 0` — первый позиционный аргумент), и этого
достаточно для корректного ребра `PRODUCES`. Уберите этот идиом из конфига — и
`PRODUCES` вместе со всем downstream-хвостом трассировки исчезает (это отдельно
проверяется тестом на фикстурах — «идиома-как-конфиг» доказывает, что паттерн
действительно доконфигурируется без правки кода).

## Инкрементальный индекс

```bash
uv run codegraph index codegraph.yaml --incremental
```

Per-service решение по config-fingerprint + scan-diff: `skipped` (fingerprint и
файлы не менялись), `incremental` (stale-набор файлов — изменённые ∪ добавленные
∪ те, чьи SCIP-рефы сдвинулись после пере-резолва, например сосед
переименованного символа) или `full` (fingerprint не совпал или первый прогон).
Решение и причина — в отчёте (`mode`/`reason` на каждый сервис,
`.codegraph/report.json`).

Честно: `scip-python` сам **не файл-инкрементален** — внутри изменённого сервиса
SCIP всё равно перегоняется по всему сервису целиком (merkle-кэш экономит только
на бит-в-бит идентичном дереве файлов); `--incremental` ускоряет остальное —
безусловный skip неизменившихся сервисов и дорогие парсинг+джойн (S5/S6) только
по stale-набору внутри изменённого. Выигрыш **зависит от масштаба сервиса**: на
фикстурном перф-гейте (`tests/eval/test_incremental_gate.py`, три крошечных
сервиса) правка одного файла даёт `--incremental` время ≈30–40% от полного
холодного прогона той же копии, но на реальном ~60k LOC сервисе (пилот на
Netflix/dispatch,
[отчёт](docs/superpowers/reports/2026-07-18-m4-pilot.md)) правка одного файла
≈ времени полного холодного прогона: scip-перегон всего сервиса доминирует
настолько, что экономия S5/S6 тонет в шуме. Реальную экономию на масштабе даёт
skip-путь — неизменившиеся сервисы пропускаются целиком (~15% от full-cold на
том же пилоте), поэтому основной выигрыш `--incremental` — в мульти-сервисных
workspace, а не в точечных правках внутри одного большого сервиса.

## Режимы деградации

| Сценарий | Поведение |
|---|---|
| `scip-python` недоступен (нет node/npx, сеть, зависший процесс, таймаут) | Сервис помечается `degraded`, пайплайн не падает — эвристический fallback-резолвер строит defs/refs структурно из tree-sitter (top-level def/import, без символьного резолва). Рёбра `CALLS` этого сервиса получают `resolution="heuristic", confidence=0.6`. Отражено в `codegraph index` и `.codegraph/report.json` (`degraded: true`); `codegraph doctor --probe-scip` проверяет `scip-python` отдельно. |
| Эмбеддер недоступен (`uv sync` без `--extra local-emb`, нет `OPENAI_API_KEY`/`VOYAGE_API_KEY`, пакет провайдера не установлен) | `codegraph index` печатает жёлтое предупреждение (`S8: embeddings skipped (...)`) и продолжает — узлы `Chunk` и `context_header` строятся и грузятся как обычно, просто без `embedding`. Граф остаётся полностью пригоден для `CALLS`/трассировки/fulltext-поиска; недоступен только vector-режим retrieval (`search_code`/`find_entrypoint` молча деградируют в `mode_used="text"`). `--no-embed` даёт тот же эффект осознанно и тихо, без предупреждения. |
| FalkorDB недоступен | Из всего пайплайна `index` только последняя стадия (S9, запись графа) реально трогает FalkorDB — S1–S8 (скан/резолв/экстракция/линковка/чанкинг/эмбеддинг) отрабатывают полностью в staging (SQLite) и не теряются. S9 падает с понятным сообщением и `exit 1` (`falkordb unreachable: ...`), staging остаётся на диске — `codegraph load <target>` потом догружает граф из уже посчитанного staging без повторного анализа. Та же граница — у `stats`/`trace`/`serve`/`eval retrieval`. |

## MCP-инструменты

`codegraph serve` (FastMCP, stdio) регистрирует 9 read-only инструментов, все — тонкая
делегация в графовый слой, без text-to-Cypher и без прямого доступа к графовой БД:

| Инструмент | Что делает |
|---|---|
| `graph_stats()` | Счётчики узлов по `kind` и рёбер по `type`. |
| `get_source(node_id, context_lines=0)` | Исходный код узла с диска (+/- контекст); `stale: true`, если файл с тех пор изменился. |
| `expand_neighbors(node_id, edge_types?, direction="both", depth=1, limit=50)` | BFS-соседи узла. |
| `who_calls(node_id, transitive=false, max_depth=3)` | Кто вызывает узел через `CALLS` — прямо или транзитивно. |
| `find_paths(from_id, to_id, max_hops=8, edge_types?)` | Путь между двумя узлами (двунаправленный BFS). |
| `list_processes()` | Все `BusinessProcess`-узлы (из конфига и/или Temporal-воркфлоу). |
| `trace_process(entrypoint_id, direction="downstream", max_segments=12, min_confidence=0.3, include_source=false)` | Трассировка бизнес-процесса сегментами через `Channel`-границы (то, что показывает `codegraph trace`). |
| `find_entrypoint(query, kinds?, k=5)` | Гибридный поиск точки входа (fulltext по `Sym` + vector по `Chunk`, RRF). |
| `search_code(query, k=8, service?, mode="hybrid")` | Поиск по коду (`text` / `vector` / `hybrid`). |

## CLI

`TARGET` везде — путь к конкретному YAML-конфигу, ИЛИ к директории (тогда внутри
неё ищется `codegraph.yaml`, а при отсутствии — zero-config: вся директория как
один сервис); по умолчанию — текущая директория.

| Команда | Что делает |
|---|---|
| `codegraph init [DIR]` | Создать `codegraph.yaml` из прокомментированного шаблона. |
| `codegraph doctor [--probe-scip] [--skip-store] [--config PATH]` | Проверить окружение (python/node/npx[/scip-python]) и возможности FalkorDB (feature-probes). |
| `codegraph index [TARGET] [--dry-run] [--graph NAME] [--no-embed] [--incremental]` | Построить граф: scan → resolve → extract → join → link → chunk+embed → load → report. |
| `codegraph load [TARGET] [--graph NAME]` | Пересобрать граф в FalkorDB из уже посчитанного `staging.db`, без повторного анализа. |
| `codegraph stats [TARGET] [--graph NAME]` | Узлы по `kind` / рёбра по `type`. |
| `codegraph trace SELECTOR [TARGET] [--graph NAME] [--format text\|mermaid]` | Трассировка от точки входа (`"<service>:<METHOD> <path>"` либо `"<service>:<dotted.qualified.name>"`). |
| `codegraph serve [TARGET] [--graph NAME]` | MCP-сервер (stdio). |
| `codegraph eval retrieval [TARGET] [--graph NAME] [--k N] [--questions PATH] [--exact]` | Прогон golden-вопросов (hit@k) через `search_code(mode="hybrid")` — отчёт, не CI-гейт. `--exact` — детерминированный полный скан вместо ANN (см. «Ограничения»). |
| `codegraph --version` | Версия установленного пакета. |

## Ограничения

Честно, без приукрашивания:

- **Только Python.** tree-sitter-запросы, SCIP-резолв и доменные идиомы
  (FastAPI/Kafka/Temporal/HTTP-клиенты) написаны под Python-код; другие языки в
  сервисах не анализируются.
- **Одноимённые классы/функции во взаимоисключающих `if/elif`-ветках
  (feature-flag паттерн): CALLS-рёбра всегда указывают на первую по файлу
  ветку.** Узлы/чанки всех веток присутствуют в графе (id второй+ ветки получает
  суффикс `~2`, `~3`, …), но scip резолвит все ссылки вызывающих в один
  control-flow-insensitive символ → CALLS ложится на первую ветку; корректная
  атрибуция по live-ветке потребовала бы flow-sensitive-анализа (вне scope).
- **Русскоязычный полнотекстовый поиск — практически vector-only.** RediSearch
  (`db.idx.fulltext.queryNodes`) по умолчанию — implicit AND по токенам запроса;
  OR-fallback (второй проход с OR токенов, если AND дал 0 хитов при более чем
  одном токене) даёт смешанному русско-английскому запросу полнотекстовую ногу,
  только если хотя бы один токен (обычно — англ. идентификатор/термин)
  буквально встречается в корпусе. Чисто-кириллический запрос без ни одного
  совпадающего токена остаётся целиком на векторном поиске — `mode="hybrid"` в
  этом случае фактически равен `mode="vector"`.
- **`hit@k` нестабилен между идентичными прогонами `codegraph eval retrieval`.**
  FalkorDB's ANN-индекс для векторного поиска (`db.idx.vector.queryNodes`, HNSW)
  пересобирается несидированным при каждой загрузке графа, так что векторная нога
  ранжирования может отличаться от прогона к прогону на одном и том же графе (см.
  [пилот-отчёт](docs/superpowers/reports/2026-07-18-m4-pilot.md) §4.1). Продакшен-
  поиск (`search_code`/MCP) остаётся ANN как есть; `codegraph eval retrieval
  --exact` даёт полный детерминированный скан (`vec.cosineDistance`, без индекса) —
  используйте его для CI-гейтов и сравнений между прогонами; на больших графах он
  медленнее ANN.
- **`scip-python` не файл-инкрементален** (см. «Инкрементальный индекс» выше) —
  внутри изменённого сервиса SCIP перегоняется целиком; выигрыш `--incremental`
  на правке одного файла масштабо-зависим (фикстуры: ≈30–40% полного прогона;
  реальный ~60k LOC сервис: ≈100%, экономит только skip неизменённых сервисов —
  см. [пилот-отчёт](docs/superpowers/reports/2026-07-18-m4-pilot.md)), а не
  «мгновенно».
- **FalkorDB — in-memory.** Граф живёт в памяти контейнера; персистентность —
  docker volume (`falkordb-data`), но настоящий источник восстановления —
  `staging.db` (создаёт `index`) и `codegraph load`, которая пересобирает граф
  из staging без повторного SCIP-анализа/эмбеддинга.
- **Повторный `index` без изменений не переэмбеддит** (persistent cache по
  `input_hash = sha256(header + text)`, переживает пересоздание сервисного
  слоя) — но на **первом** прогоне платный провайдер (`openai`/`voyage`) платит
  за каждый чанк; кэш экономит только на повторах, не на первом проходе.

## Тесты

```bash
uv run pytest                              # unit + falkordb (нужен docker compose up -d)
uv run pytest -m "scip or emb"             # + реальный scip-python / реальный эмбеддер
```

По умолчанию (`pyproject.toml`, `addopts`) исключены только маркеры `scip` и
`emb` (сеть/внешние пакеты); `falkordb`-тесты идут в дефолтный прогон и требуют
поднятого контейнера.

## Документация

- [`docs/superpowers/specs/2026-07-12-codegraph-design.md`](docs/superpowers/specs/2026-07-12-codegraph-design.md) —
  архитектура, полная модель данных, таблица `resolution`/`confidence`, протоколы.
- [`codegraph.example.yaml`](codegraph.example.yaml) — прокомментированный
  референс всех полей конфига.
- [`fixtures/workspace.yaml`](fixtures/workspace.yaml) — рабочий пример на
  фикстурах (используется в примерах этого README).
