# Re-run №3 после M8 — полная трасса бизнес-процесса (ПОЛНАЯ)

> Дата: 2026-07-24. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-3-BRIEF.md`.
> Baseline — `2026-07-24-pilot-rerun-2.md` + open-gaps R4/R5. M8 закрыл оба: транзитивная
> композиция префиксов роутеров (`include_router`-цепочки через файлы) и типизированные
> сигнальные отправители (`handle.signal(Cls.method, …)`). Сервисы те же:
> camunda-gateway / verification-requests / document-management.
> SANITIZED — `2026-07-24-pilot-rerun-3-SANITIZED.md`.

## 0. Обновление и смоук

`git pull` → HEAD `49ad02f` (≥ 278e46d, 8 коммитов M8). doctor зелёный. M8-гейт:
`pytest -m "scip and falkordb" tests/eval/test_m8_gate.py` → **1 passed, 0 failed, 13.3s**.
DSL идиом **не менялся** (M8 целиком extractor/linking-side) → workspace из re-run №2
переносится как есть; правок yaml не потребовалось.

## 1. Чекпойнты (§2 брифа) — факты и вердикт

Прогон: `codegraph index <ws> --no-embed`, 325 файлов, **18.8s**, degraded=0 (все три `full`),
unresolved calls 4.9%, staged CALLS valid dst 91.3% — within-service граф без регрессий.

| Метрика | Baseline №2 | Ожидание M8 | Факт | Вердикт |
|---|---|---|---|---|
| CALLS_HTTP anchored static/1.0 | 0 | ≈23/24 | **23** static/1.0 (все → verification-requests) | ✅ точно |
| calls_http_unresolved | 59 | ≈36 | **36** (35 уникальных ?-каналов, funnel'а нет) | ✅ |
| route_prefix_unresolved | — | 0 на канонической vr-цепочке | **0** | ✅ |
| PRODUCES temporal_signal | 0 | ≈18/18 static/1.0 | **16** static/1.0 + **2** honest-unlinked | ✅ (см. §2) |
| signal_name_unresolved | 18 | 0 | **0** | ✅ полностью |
| signal_send_unlinked | — | 0; ненуль — разглядеть | **2**, оба корректны (§2) | ✅ честно |
| NEXT_SEGMENT | 0 | > 0 | **43** static/1.0 (23 http + 20 signal) | ✅ |
| INVOKES_ACTIVITY | 64 | без регрессий | **64** static/1.0 | ✅ |
| CONSUMES | 16 | +signal | **43** (16 event_type + 27 temporal_signal), static/1.0 | ✅ |
| verb_unresolved | 0 | 0 | **0** | ✅ |
| temporal marks | 7 | 7 | **7** | ✅ |

Каналы: http_route 133 / temporal_signal 25 / event_type 15 / kafka_topic 5.
`channels_gc = 153` — документированный GC-then-recreate http_route-каналов на повторном
прогоне (не потеря данных: 133 живых роут-канала после пересборки). `producer_unresolved_channel = 1`
(builtin aiokafka на `send_and_wait` внутри обёртки — честный динамический топик, покрыт
enum-идиомом с другой стороны). `part_of_process_ambiguous = 27` — сигнальные хендлеры,
принадлежащие нескольким воркфлоу одновременно (один `add_connected_uids` — в SOF и EP);
ожидаемое свойство, не баг.

## 2. `signal_send_unlinked = 2` — разбор (оба корректны)

Из 18 типизированных sender-сайтов **16 → PRODUCES static/1.0**, 2 не связались — и оба это
**правильные честные миссы**, а не потерянные рёбра (M8 «fails safe: резолвнутый символ, не
являющийся signal-хендлером → счётчик, не ребро»):

1. `sdf.py:336` — `handle.signal(parent_signal_info.parent_signal_type, …)`: имя сигнала —
   **рантайм-поле** dataclass'а `ParentSignalInfo`, а не ссылка на `@workflow.signal`-метод.
   Символ резолвится в поле модели (не хендлер) → корректно unlinked.
2. `services/temporal.py:86` — `workflow_handle.signal(step_other_evidence_update, …)`, где
   `step_other_evidence_update = workflow_handle_for_customer.workflow_type.signal_step_other_evidence_update`
   (`temporal.py:75`) — ссылка добыта через `.workflow_type.<attr>` рантайм-хендла (локальная
   переменная) → символ = `local1`, не хендлер → корректно unlinked.

Обе — принципиально динамические конструкции, статически неразрешимые; M8 их не выдумывает.
16 static/1.0 PRODUCES точно соответствуют 16 литерально-типизированным `handle.signal(Cls.method)`.

## 3. ГЛАВНЫЙ вердикт — полная трасса целевого сценария

**Собирается.** Целевой сценарий проекта — «Kafka-событие → consumer → сигнал → signal-хендлер
→ … » с ветвлением в кросс-сервисный HTTP — **впервые материализуется как явные сегмент-переходы
в одном трейсе**. `codegraph trace camunda-gateway:…ConnectedAccountsAddRequestedConsumer.process_event`:

```
S0 (MessageConsumer) camunda: ConnectedAccountsAddRequestedConsumer.process_event
   ├─ channel PATCH /api/v2/requests/{verification_uid}/linked-customers
   │     → verification-requests: add_linked_customers_v2      ← HTTP-хоп (composite path!), кросс-сервис
   ├─ channel add-connected-uids
   │     → EconomicProfileInconsistencyWorkflow.add_connected_uids,
   │       LimitIncreaseSOFWorkflow.add_connected_uids         ← сигнальный хоп (typed), резолв в хендлеры
   └─ channel POST /api/v1/users/legal-entities → unresolved   ← внешний (api-gateway), честно не заякорен
S1 (RouteHandler) verification-requests: add_linked_customers_v2   ← HTTP-хоп ПРИЗЕМЛИЛСЯ в чужом сервисе
S2 (TemporalSignalHandler) camunda: EconomicProfileInconsistencyWorkflow.add_connected_uids
S3 (TemporalSignalHandler) camunda: LimitIncreaseSOFWorkflow.add_connected_uids
```

Оба ключевых **новых** хопа — явные переходы сегментов:
- **HTTP-хоп** `CALLS_HTTP static/1.0 → chan:http:verification-requests:PATCH /api/v2/requests/{…}/linked-customers → HANDLES`.
  Путь **композитный** (`/api/v2` — из `main.py:include_router(prefix=...)`, собран транзитивно через
  файлы; R4 закрыт). Сегмент S1 — реальный route-handler ДРУГОГО сервиса.
- **Сигнальный хоп** `PRODUCES static/1.0 → chan:temporal_signal:add-connected-uids → CONSUMES`.
  arg0 = `WF.add_connected_uids` (typed-ссылка) резолвнут в оба хендлера-воркфлоу (R5 закрыт).

Проверено ещё на трёх входах (partner_profile → complete-survey; source_of_funds_change →
update-source-of-funds-signal; + KYPCreatedConsumer с 4× INVOKES_ACTIVITY из re-run №2) —
паттерн стабилен. NEXT_SEGMENT: 23 http-пары (client→route другого сервиса) + 20 signal-пары
(sender→handler), все static/1.0.

**Дисклеймер честности:** в этом конкретном трейсе за сигнал-хендлером не следует activity→HTTP
(хендлеры `add_connected_uids` кладут данные в очередь воркфлоу — реактивная часть исполняется
в теле воркфлоу, вне синхронной цепочки вызовов). Но оба ранее рвавшихся звена — typed-signal и
composite-path cross-service HTTP — теперь **явные, корректные, static/1.0**. Трасс-уровневый
confidence 0.50 отражает присутствие heuristic/0.5 HTTP-каналов (внешние api-gateway-вызовы,
честно не заякоренные), а не слабость резолвнутых хопов (те — 1.0).

## 4. Вывод

**Целевой сценарий собирается.** M8 закрыл оба блокера re-run №2 на реальном коде: R4 —
`route_prefix_unresolved=0`, 23 из 24 anchored-claim'ов дали кросс-сервисный CALLS_HTTP
static/1.0 с композитными путями (единственный не-матч — честная находка для команды сервисов,
§5); R5 — `signal_name_unresolved` 18→0, 16 typed-sender'ов дали PRODUCES static/1.0, 2
оставшихся — принципиально динамические (корректно unlinked, не потеряны). NEXT_SEGMENT 0→43.
Сквозная трасса «consumer → [signal→handler] + [HTTP→route другого сервиса]» — впервые явные
сегмент-переходы. Within-service граф без регрессий. Новых блокеров нет.

## 5. Мелкое / находки (не блокеры)

- **Находка для команды verification-requests (не codegraph):** камунда-клиент зовёт
  `DELETE /api/v1/requests/{verification_uid}/linked-customers`, а vr на этом пути объявляет
  только PATCH и GET (`app/api/v1/verification_requests.py:388,402`). codegraph честно оставил
  claim unresolved (`chan:http:?`, heuristic) — не сматчил на PATCH/GET по совпадению пути.
- **Backlog codegraph (косметика):** карточка узла-хендлера показывает ЛОКАЛЬНЫЙ `path_template`
  (`/steps/{id}`); композитная идентичность (`/api/v1/steps/{id}`) живёт на Channel-узле —
  compose-back в node-props был бы удобнее для чтения (уже помечено в брифе как беклог).
- **NEXT_SEGMENT по Kafka = 0** и после M8: producer-топики camunda (enum-fanout) не
  потребляются никем в 3-сервисном срезе — свойство среза, не механизма (реальные контрагенты
  вне выборки). Вариант event_type-идентичности producer'а — прежний низкоприоритетный беклог.

## Приложение — воспроизведение

Workspace re-run №2 без изменений (per-клиент base_url + env_sources → реальный helm камунды).
`codegraph index <ws> --no-embed` → `report.json` (`calls_http=59/unresolved=36`,
`next_segments=43`, `route_prefix_unresolved=0`, `signal_send_unlinked=2`, `marks=7`,
`http_verb_unresolved=0`). Трасса — `codegraph trace "<svc>:<dotted>" <ws>` (селекторы роутов
теперь требуют ПОЛНЫХ путей `/api/v1/...`). Граф `pilot_kyc`, staging.db и .scip — вне репозитория.
