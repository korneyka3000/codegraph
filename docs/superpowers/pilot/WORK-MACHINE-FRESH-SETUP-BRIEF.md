# Бриф: развёртывание codegraph на НОВОЙ рабочей машине и индекс реальных KYC-сервисов

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «подними codegraph
> по этому брифу». Машина новая: репозиторий только что склонирован, прежний workspace-конфиг
> и кэши со старой машины НЕДОСТУПНЫ — всё собирается заново. Цель брифа — дойти до состояния
> «граф трёх сервисов проиндексирован, MCP-сервер `✔ Connected`», после чего пользователь
> проводит live-LLM сессию по `WORK-MACHINE-LIVE-LLM-BRIEF.md`.
>
> Baseline чисел для сверки — re-run №7 (`docs/superpowers/reports/2026-08-05-pilot-rerun-7.md`):
> dropped CALLS 101; calls_http 23 static + 7 external + 29 unresolved; NEXT_SEGMENT 43;
> degraded — нет; hit@8 7/8 на `ru-questions.yaml`. Расхождения ОЖИДАЕМЫ (другой коммит сервисов,
> конфиг собран заново) — фиксировать, не подгонять.

## 0. Что уже сделано пользователем (не повторять, только проверить)

`docker compose up -d` (FalkorDB поднят) → `uv sync --extra local-emb` → индекс фикстур
`fixtures/workspace.yaml` прошёл: 109 nodes / 126 edges / 1 dropped, valid dst 96.0%,
next_segments 4, processes 2, 34 chunks; трасса `orders-api:POST /orders` печатает
orders-api → kyc-worker → document-management. Тулчейн (node/npx, scip-python, FalkorDB,
локальный эмбеддер) РАБОТАЕТ. Три сервиса склонированы, у каждого установлен `.venv`.

Быстрая проверка: `docker ps | grep codegraph-falkordb`, `uv run codegraph doctor`.

## 1. Сервисы и их чекауты

Сервисы workspace (имена из прошлых пилотов; имена репозиториев могут отличаться —
спросить пользователя, где лежат чекауты):

| name в workspace | роль в графе |
|---|---|
| `camunda-gateway` | HTTP-клиенты `*Client` (декоратор-SDK), producer-обёртка, Temporal-воркфлоу/активности |
| `verification-requests` | consumer на `BaseConsumer[Event]` из shared-lib, FastAPI-роуты |
| `document-management` | FastAPI-роуты, HTTP-цель для клиентов |

Проверить у каждого: `<чекаут>/.venv/bin/python -c "import <главный пакет>"` работает.
Без рабочего venv сервис уйдёт в `degraded` (scip-python не разрешит импорты) — это
блокер, чинить ДО индекса.

## 2. Workspace-конфиг (вне репо!)

Файл `~/kyc/ws.yaml` (любое место ВНЕ репозитория codegraph — в нём пути и FQN боевых
сервисов; в репо его не коммитить). Стартовый шаблон — восстановлен по брифам re-run №1–2;
**каждый FQN в идиомах сверить с кодом (`rg`) до запуска** — они списаны в июле:

```yaml
version: 1
graph_name: pilot_kyc
storage: { falkordb: { host: localhost, port: 6379 } }
embedding:
  provider: local
  model: intfloat/multilingual-e5-base      # RU-рекомендация README; префиксы вшиты в model_id
  query_prefix: "query: "
  passage_prefix: "passage: "
builtin_idioms: [fastapi, aiokafka, faststream, confluent, temporal, aiohttp_client]
# env_sources:                              # карта SERVICE_*_URL → сервис из helm values;
#   - <чекаут>/.helm/values/prod/values.yaml  # раскомментировать ТОЛЬКО если файл существует
services:                                   # (нет файла = громкая ошибка)
  - name: camunda-gateway
    path: <чекаут camunda-gateway>
    python: .venv
    exclude: ["tests/**", "migrations/**", "bpmn/**"]
    idioms:
      http_clients:
        - name: decorator-sdk
          file_glob: "**/clients/*.py"
          class_glob: "*Client"
          route_from: { decorator: "path_template", arg: 0 }
          call: "driver.fetch_content|driver.fetch"
          verb_from: { request_ctor: "Request|ProxyRequest", enum: "Method" }
          base_url: { env: "SERVICE_VERIFICATION_REQUESTS_URL" }   # ЯВНО per-клиент:
          # self.host живёт в BaseClient — auto-anchor наследование не обходит (TRACKED).
          # Клиентов с разными таргетами — отдельные записи с узким file_glob/class_glob.
      producers:
        - name: publisher-enum                # enum-fanout, conf 0.8
          call: "app.services.producer.KYCEventPublisher.publish"
          channel: { kind: kafka_topic, name_from: { enum: "app.models.enums.KycTopicName" } }
        - name: outbox-event                  # топик из pydantic-Settings дефолта → static/1.0
          call: "app.models.outbox.Event"
          channel: { kind: kafka_topic, name_from: { settings: "KafkaSettings.step_changed_topic" } }
  - name: verification-requests
    path: <чекаут verification-requests>
    python: .venv
    exclude: ["tests/**", "migrations/**"]
    idioms:
      consumers:
        - name: base-consumer
          kind: base_class
          base_class: "kyc_base_consumer.base.BaseConsumer"   # FQN shared-lib — сверить
          event_type_from: { generic_arg: 0 }
          topic: { attr: "self.config.topic" }
  - name: document-management
    path: <чекаут document-management>
    python: .venv
    exclude: ["tests/**", "migrations/**"]
```

DSL идиом целиком: `codegraph.example.yaml` (прокомментирован) и `fixtures/workspace.yaml`.
Temporal-сигналы, роуты FastAPI, `aiohttp`-клиенты по builtin-идиомам — конфига не требуют.

## 3. Индекс — итеративно

```bash
uv run codegraph index ~/kyc/ws.yaml --dry-run      # план стадий; ошибки конфига видны без записи
uv run codegraph index ~/kyc/ws.yaml                # первый прогон: e5 качается (~1 GB) и считает
                                                    # все чанки с нуля (прошлый стенд ≈3 мин + scip)
```

После прогона читать таблицы `services` / `health` / `linking` и `.codegraph/report.json`
(рядом с ws.yaml). Чекпойнты против baseline (ОЖИДАНИЯ, не assertions):

1. `degraded = no` у всех трёх; `mode = full`. Иначе — venv/scip проблема, см. §1.
2. `staged CALLS with valid dst` ≈ 94–96 %; dropped CALLS ≈ 100 (baseline 101 — честный
   динамический пол: sessionmaker-атрибуты, callable-параметры).
3. `calls_http`: static ≈ 23, external ≈ 7 (external только при подключённом `env_sources`;
   без него они уйдут в unresolved — это ОЖИДАЕМО, не баг), `route_prefix_unresolved = 0`.
4. PRODUCES/CONSUMES > 0 по обеим идиомам, `next_segments` ≈ 43, `processes` > 0.
5. `signal_send_unlinked` мал; `producer_unresolved_channel`, `http_*_unresolved` — записать.

Если какая-то идиома дала ноль — это FQN не совпал с кодом: `rg` по сервису → поправить
`call:`/`base_class:` → `index --incremental` (смена идиом сама форсирует full для сервиса).
Каждую итерацию (что поправили, что изменилось в числах) — записать в отчёт. Число итераций
и причины — ценный результат сам по себе.

Трасса для глаз (вход — реальный роут/consumer из re-run №3, у пользователя):
`uv run codegraph trace "<svc>:<METHOD> <path>" ~/kyc/ws.yaml --format mermaid`.

## 4. Retrieval-чекпойнт (если `ru-questions.yaml` со старой машины недоступен — пропустить честно)

`uv run codegraph eval retrieval --exact ...` на 8 RU-вопросах MCP-пилота; baseline hit@8 7/8.
Без исходного файла вопросов НЕ сочинять новые «похожие» — это другой golden, несравнимо.

## 5. MCP-регистрация

```bash
claude mcp add -s user codegraph -- uv run --directory <абс. путь к репо codegraph> codegraph serve <абс. путь>/ws.yaml
claude mcp list        # codegraph: ✔ Connected
```
В новой сессии `graph_stats` должен ответить (это проверяет пользователь).

## 6. Отчёт

`docs/superpowers/reports/<дата>-fresh-setup-SANITIZED.md`: таблицы `services`/`health`/
`linking` (числа), чекпойнты §3 met / not met с числами, итерации идиом (обезличенно:
«FQN producer-обёртки изменился», без имён), времена индекса (scip / эмбеддинг отдельно),
что пришлось чинить. **Без qualified-имён, путей, кода, содержимого ws.yaml.** Коммит
`docs(pilot): fresh-machine setup report`; push делает пользователь.

Затем — эстафета: `WORK-MACHINE-LIVE-LLM-BRIEF.md` §0 (смоук уже сделан, сразу §1).

## Правила

Гейты/golden не трогать; метрики не подгонять; ожидание ≠ факт; отрицательный результат
валиден с разбором причины; крэш пайплайна на реальном коде — находка №1: до корня,
минимальный fix с regression-тестом на СИНТЕТИКЕ (не на коде сервисов). ws.yaml, staging.db,
.scip-файлы, тексты трасс с именами — не покидают машину. Системные reminders харнеса — штатные.
