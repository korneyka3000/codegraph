# Re-run №2 после M7 — HTTP-якорение, producer-топики, сигналы (ПОЛНАЯ)

> Дата: 2026-07-24. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-2-BRIEF.md`.
> Baseline — `2026-07-23-pilot-rerun.md` + `2026-07-23-pilot-rerun-open-gaps.md` (R1/R2/R3).
> Сервисы те же: camunda-gateway / verification-requests / document-management.
> Санитизированная версия — `2026-07-24-pilot-rerun-2-SANITIZED.md`; новые gap'ы для
> фикс-агента — `2026-07-24-pilot-rerun-2-open-gaps.md`.

## 0. Обновление и смоук

`git pull` → HEAD `ad8aa6d` (≥ 2da3be1, 15 коммитов M7). doctor зелёный (python 3.14 /
node v25.6 / npx 11.8; FalkorDB все feature-probes OK). M7-гейт:
`pytest -m "scip and falkordb" tests/eval/test_m7_gate.py` → **1 passed, 0 failed, 11.7s**
(junit-xml). Механизмы на машине работают.

## 1. Итерации идиом (одна итерация, конфиг — данные)

- **HTTP-якорение (R1)**: как и предупреждал бриф, `self.host` присваивается в
  **унаследованном** `BaseClient.__init__` (`app/clients/bases.py`) → auto-anchor не видит.
  Поэтому — явный per-клиент `base_url`, а идиомы упорядочены «специфичные → catch-all»
  (первая матчнувшая идиома забирает call-site):
  - `vr-client` (`**/clients/verification_requests.py`) → `base_url: {env: SERVICE_VERIFICATION_REQUESTS_URL}`;
  - `legacylizer-client`, `dmout-client` → свои env (оба ведут ВНЕ воркспейса);
  - `decorator-sdk` (catch-all `**/clients/*.py`) — без base_url (честно unanchored);
  - dm: `doc-storage-sdk` с `base_url: {settings: "app.config.services.DocStorageSettings.host"}`
    (env_prefix `doc_storage_` → `DOC_STORAGE_HOST`) — проверка {settings}-якорения.
  - `verb_from: {request_ctor: "Request|ProxyRequest", enum: "Method"}` — во всех идиомах.
- **env_sources**: реальный helm камунды
  `.helm/values/kyc-nl-prod-env/kyc/values.yaml`. Проверено напрямую:
  `build_env_service_map(...)` → `{'SERVICE_VERIFICATION_REQUESTS_URL': 'verification-requests'}`.
  Единственный маппинг в срез — остальные `SERVICE_*_URL` ведут на внешние хосты
  (`rp.prod.env`, `ingress-nginx-legacylizer-controller…`, `kyc-gateway.prod.env`…), что и
  требовалось: карта не выдумывает соответствий.
- **Producer (R2)**: вариант (a) enum-fanout — `call: app.services.producer.KYCEventPublisher.publish`,
  `name_from: {enum: "app.models.enums.KycTopicName"}`. Вариант (b) (outbox/Settings-топик)
  на этом стеке **неприменим**: outbox'а нет вовсе, topic-полей в Settings нет —
  единственная точка публикации это обёртка с `payload.topic_name`.
- **Сигналы (R3)**: ноль конфига, как и обещано.

## 2. Чекпойнты (§2 брифа) — факты и вердикт

Прогон: `codegraph index <ws> --no-embed`, 325 файлов, **18.1s**, degraded=0 (все три `full`),
unresolved calls 4.9%, staged CALLS valid dst 91.3% — **регрессий within-service нет**.

| Метрика | Baseline (re-run №1) | Ожидание M7 | Факт | Вердикт |
|---|---|---|---|---|
| CALLS_HTTP ложные static | 3 | 0 | **0** | ✅ |
| CALLS_HTTP корректные anchored static/1.0 | 0 | > 0 | **0** | ❌ блокирует новый gap R4 (§3.1) |
| http_call claims | 44 | ≈55 | **59** (camunda 50 + dm 9) | ✅ |
| анкеровка claim'ов | 0 | — | **40/59** несут base_url_env (24 → verification-requests через helm; 16 внешние), 19 unanchored | ✅ механизм |
| unanchored | 41 unresolved | ≤ heuristic/0.7 уникальные | 19 → все unresolved (уникального матча нет) | ✅ честно |
| PRODUCES | 0 | > 0 | **4** (enum_fanout, heuristic/0.8) | ✅ |
| temporal_signal каналы | — | ≈ named-хендлеры | **25 каналов**, 34 роли `TemporalSignalHandler` (27 signal + 7 query), **27 CONSUMES static/1.0** | ✅ точно |
| sender↔handler пары | — | пары на именованных | **0 PRODUCES из 18 отправок** (`signal_name_unresolved=18`) | ❌ новый gap R5 (§3.2) |
| NEXT_SEGMENT | 3 (все ложные) | ложных 0; пары > 0 | **0** (ложных 0 ✅, истинных 0) | ⚠️ |
| verb_unresolved | 15 | ↓ | **0** | ✅ полностью |
| INVOKES_ACTIVITY | 64 | без регрессий | **64** static/1.0 | ✅ |
| CONSUMES event_type | 16 | без регрессий | **16** static/1.0 (15 каналов) | ✅ |
| temporal marks | 7 | 7 | **7** | ✅ |

`consumer_base_class_no_generic = 0`, `producer_unresolved_channel = 1` (builtin
aiokafka-идиом на `send_and_wait(topic=topic_name)` внутри самой обёртки — честный
динамический топик; наш enum-идиом покрывает ту же публикацию с другой стороны).

Каналы: http_route 153 / temporal_signal 25 / event_type 15 / kafka_topic 5.

## 3. Что реально закрылось и что вскрылось

**Закрылось (R1 частично, R2, R3-хендлеры, verb-мисс):**
- **Ложные HTTP-рёбра уничтожены.** Три пилотных пары (`/api/v1/steps/{step_uid}`,
  `/api/v1/requests/{verification_uid}`, `/api/v1/customer-info/{customer_uid}` →
  dm `/…/parsed-data`) больше не образуются: строгая форма пути + tier-2 «env известен,
  сервис не в воркспейсе → матчинг вообще не выполняется» (Legacylizer/dmout именно там).
- **env→service из helm работает** (проверено вызовом `build_env_service_map` напрямую).
- **PRODUCES появились**: 4 канала по членам `KycTopicName`, `mechanism=enum_fanout`,
  heuristic/0.8 — честная оверап-проксимация, как и задумано.
- **Сигнальные хендлеры — первоклассные узлы**: 34 роли ровно соответствуют реальным
  27 `@workflow.signal` + 7 `@workflow.query`; 27 CONSUMES в 25 каналов (2 имени
  переиспользованы двумя воркфлоу — `resolution-step-updated`, `add-connected-uids`).
  Имя из `name=SignalType.SCHEDULED_RESTRICTION` (enum-атрибут) тоже разрешилось.
- **verb_unresolved 15 → 0**: `Request|ProxyRequest` закрыл dm-подкласс; +15 claim'ов.

### 3.1. Новый блокер R4 — префикс роутера не собирается (`include_router(prefix=...)`)

24 claim'а к verification-requests **корректно заякорены** (tier 1: helm-карта), сузились
до роутов vr — и **не совпали ни с одним**, потому что роуты в графе зарегистрированы БЕЗ
префикса:

| claim (из клиента camunda) | роут в графе (vr) |
|---|---|
| `GET /api/v1/steps/{step_uid}` | `GET /steps/{step_uid}` |
| `PATCH /api/v1/requests/{verification_uid}` | `PATCH /requests/{verification_uid}` |

Корень: `verification-requests/app/main.py:23` — `app.include_router(api.v1.router, prefix="/api/v1")`,
т.е. префикс задаётся в ДРУГОМ файле на объект-роутер, а `extractors/fastapi_ext.py:84`
(`_route_prefix`) читает префикс только из `APIRouter(prefix=...)` в том же файле
(`fastapi_ext.py:178` — `_template(_route_prefix(assign), path)`); vr объявляет
`APIRouter()` без префикса.

**Доказательство величины эффекта** (прогон реального `_templates_match` по роутам vr с
приписанным `/api/v1`|`/api/v2` против тех же 24 claim'ов):
**23 → уникальный матч (стал бы static/1.0), 0 неоднозначных, 1 — ноль кандидатов**
(`DELETE /api/v1/requests/{verification_uid}/linked-customers`: у vr на этом пути есть
только PATCH и GET — честный мисс и, судя по всему, реальная находка для команды).
С текущими (беспрефиксными) роутами — 0 матчей. То есть R1 доработан верно, а
кросс-сервисный HTTP не заработал из-за **независимого** дефекта, который раньше был
замаскирован funnel-багом.

### 3.2. Новый блокер R5 — типизированные отправители сигналов

`signal_name_unresolved = 18` — это **все** реальные отправки в camunda. Ни одна не
использует строковый литерал; канонический типизированный API temporalio:

```python
await handle.signal(PartnerProfileWorkflow.complete_survey, payload)      # consumers/verification_step_changed/partner_profile.py:94
await handle.signal(LimitIncreaseSOFWorkflow.update_source_of_funds_signal, payload)  # consumers/mixins.py:67
```

`extractors/temporal_ext.py:528` (`_resolve_signal_arg0`) для attr-shaped arg0 идёт в
`consts.resolve_arg`, который знает только модульные литералы → честный мисс. Имя канала
при этом **статически восстановимо**: `Cls.method` → `@workflow.signal(name="…")` этого
метода (хендлерная сторона уже разобрана). Итог: сигнальные каналы есть, но у них 0
producer'ов → хоп «consumer разбудил воркфлоу» по-прежнему невидим.

**Поправка к прошлому отчёту:** «45 отправок» в open-gaps R3 — ошибка подсчёта
(`grep '\.signal('` захватывал и строки декораторов). Реальных sender-сайтов — **18**
(+3 `get_external_workflow_handle`). Хендлеров — 34, как и было.

## 4. Трасс-вердикт (§3 брифа — главный)

`codegraph trace camunda-gateway:app.consumers.partner_profile.KYPCreatedConsumer.process_event`:
сегмент S0 = MessageConsumer, дальше 4 × `INVOKES_ACTIVITY` в `VerificationRequestsActivities.*`
(корректно), затем 6 HTTP-каналов — **все `-> unresolved`** (R4). Сигнальный хоп
`handle.signal(PartnerProfileWorkflow.complete_survey, …)` в трассе **отсутствует** (R5);
видна лишь CALLS-связь на `PartnerProfileWorkflow.run` (из `get_workflow_handle_for`).

`codegraph trace …PartnerProfileWorkflow.complete_survey` → `S0 (TemporalSignalHandler)`,
confidence 1.00: роль видна и является валидной точкой входа сегмента (relay-паттерн
M7-T4 подтверждён на реальном коде со стороны хендлера).

**Вердикт: цепочка «Kafka-событие → consumer → signal → @workflow.signal → activity →
HTTP → чужой роут» пока НЕ отображается целиком.** Работают её первое и среднее звенья
(consumer-роль, activity-леги, роли сигнальных хендлеров); рвётся в двух местах —
сигнальный хоп (R5) и конечный HTTP-хоп (R4). Оба — доказанные, узкие, детерминированно
исправимые дефекты, а не «неопределимость в принципе».

## 5. Вывод

**Существенный прогресс, цель ещё не достигнута.** M7 сделал ровно то, что обещал по
качеству: ложных рёбер **0** (было 3), `verb_unresolved` **0** (было 15), PRODUCES
**4** (было 0), сигнальные хендлеры **34 роли / 25 каналов / 27 CONSUMES static/1.0**
(было 0 — целое измерение), env→service из helm работает, within-service граф без
регрессий. Но обе «полные» цели брифа — корректные anchored-HTTP-рёбра и sender↔handler
пары — дают **0** из-за двух вновь локализованных дефектов: **R4** (FastAPI-префикс из
`include_router` в другом файле, доказано: 23/24 claim'а стали бы static/1.0) и **R5**
(типизированные `handle.signal(Cls.method, …)` — 18/18 реальных отправок). Оба вынесены
с корнем и file:line в `2026-07-24-pilot-rerun-2-open-gaps.md`.

## Приложение — воспроизведение

Workspace: 3 сервиса, per-клиент `base_url`, `env_sources` → реальный helm камунды.
`codegraph index <ws> --no-embed` → `.codegraph/report.json` (`calls_http=59/unresolved=59`,
`next_segments=0`, `marks=7`, `signal_name_unresolved=18`, `producer_unresolved_channel=1`,
`http_verb_unresolved=0`). Симуляция R4:
`_templates_match(prefix+route, claim)` по роутам vr из графа против 24 anchored-claim'ов
staging'а. Граф `pilot_kyc`, staging.db и .scip — вне репозитория (scratchpad).
