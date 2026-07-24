# Остаточные gap'ы после M7 re-run №2 — префикс роутера / типизированные сигналы

> Дата: 2026-07-24. Продолжение `2026-07-24-pilot-rerun-2.md`. M7 закрыл R1 (ложные
> HTTP-рёбра), R2 (producer-топики) и R3-хендлеры (сигнальные роли/каналы) — подтверждено
> на реальном коде. Но обе end-to-end цели (корректный кросс-сервисный HTTP и пары
> sender↔handler) дают 0 из-за ДВУХ вновь локализованных дефектов. Этот документ — вход
> для фикс-агента: корень + доказательство + направление. Правило прежнее: regression-тесты
> на синтетике (`fixtures/realstack/`), не на коде юзера; гейты/golden не трогать.

## R4 (высокий) — FastAPI: префикс из `include_router(..., prefix=...)` не собирается

**Симптом.** 24 http_call claim'а КОРРЕКТНО заякорены на `verification-requests`
(tier 1 через helm-карту env→service — механизм M7 T3 работает), сузились до роутов этого
сервиса и не совпали ни с одним: claim `GET /api/v1/steps/{step_uid}` против роута
`GET /steps/{step_uid}`. Итог: `calls_http_unresolved = 59/59`, корректных anchored
static/1.0 — **0**.

**Корень.** `src/codegraph/extractors/fastapi_ext.py:84` (`_route_prefix`) берёт префикс
ТОЛЬКО из `APIRouter(prefix="…")` в том же файле (используется на `fastapi_ext.py:178`,
`_template(_route_prefix(assign), path)`). Реальная (и самая частая) конвенция — сборка
префикса в другом модуле на объект-роутер:

```python
# verification-requests/app/api/v1/verification_steps.py
router = APIRouter()                       # ← без префикса
@router.get("/steps/{step_uid}") ...
# verification-requests/app/api/v1/__init__.py
router.include_router(verification_steps.router, tags=["steps"])   # ← вложение без префикса
# verification-requests/app/main.py:23
app.include_router(api.v1.router, prefix="/api/v1")                # ← ПРЕФИКС ЗДЕСЬ
```

То есть путь роута = конкатенация префиксов по ЦЕПОЧКЕ `include_router` (транзитивно,
через границы файлов), а сейчас берётся только последний сегмент.

**Доказательство величины.** Реальный `_templates_match` прогнан по роутам vr из графа с
приписанным `/api/v1`|`/api/v2` против тех же 24 anchored-claim'ов:
**23 → УНИКАЛЬНЫЙ матч (стали бы static/1.0), 0 неоднозначных, 1 → ноль кандидатов**
(`DELETE /api/v1/requests/{verification_uid}/linked-customers` — у vr там только PATCH/GET;
честный мисс). С текущими беспрефиксными роутами — 0 матчей. Строгая форма M7 при этом
сама защищает уникальность: claim `/api/v1/requests/{verification_uid}` НЕ схлопывается
на статический `/api/v1/requests/limit_increase_sof`.

**Направление фикса.** Идентичность роут-канала нельзя достроить в S4 (префикс живёт в
другом файле), поэтому — по образцу http_call: экстрактор эмитит (а) роут-факты с
привязкой к СИМВОЛУ роутер-объекта и (б) `router_include`-claim'ы
`(parent_router_symbol, child_router_symbol, prefix)`; композиция путей — в линковке,
транзитивным обходом include-графа до корневого `FastAPI()` (символы роутеров резолвятся
scip-ссылками, как уже делает `INVOKES_ACTIVITY`). Правила честности: цикл или
нерезолвимый родитель → путь остаётся беспрефиксным + счётчик (`route_prefix_unresolved`),
никаких догадок. Дешёвая альтернатива-заглушка (НЕ рекомендуется как единственная):
префикс-эвристика по пути файла — молча даст неверные пути на сервисах с другой раскладкой.
**Тест:** синтетик — сервис A: `router = APIRouter()` + `@router.get("/steps/{id}")` в одном
файле, `app.include_router(router, prefix="/api/v1")` в другом; сервис B с клиентом на
`/api/v1/steps/{id}` и якорем на A → ожидать одно CALLS_HTTP static/1.0; плюс негативный
пин: без composition-цепочки — unresolved, а не матч по хвосту.

## R5 (высокий) — Temporal: типизированные отправители `handle.signal(Cls.method, …)`

**Симптом.** `signal_name_unresolved = 18` — это 18 из 18, т.е. **все** реальные
sender-сайты camunda. PRODUCES в `temporal_signal`-каналы = **0**, поэтому 25 каналов и
27 хендлерных CONSUMES ни с чем не спарены, и хоп «consumer разбудил воркфлоу» в трассе
по-прежнему невидим.

**Корень.** Реальный код использует канонический ТИПИЗИРОВАННЫЙ API temporalio — ссылку
на метод, а не строку:

```python
await handle.signal(PartnerProfileWorkflow.complete_survey, payload)
await handle.signal(LimitIncreaseSOFWorkflow.update_source_of_funds_signal, payload, rpc_timeout=...)
await workflow_handle.signal(step_other_evidence_update, update_input)   # bare name — импортированный метод
```

`src/codegraph/extractors/temporal_ext.py:528` (`_resolve_signal_arg0`): для
`value_kind in ("name","attr")` идёт в `consts.resolve_arg`, который знает только
модульные строковые литералы → `(None, True)` → честный мисс. Строковая ветка
(`:525`) на этом стеке не срабатывает никогда.

**Почему это решаемо статически.** Имя канала уже известно с ДРУГОЙ стороны: тот самый
метод несёт `@workflow.signal(name="complete-survey")`, и хендлерная ветка M7 уже строит
из него `chan:temporal_signal:complete-survey` + CONSUMES. Нужно лишь связать arg0 с этим
символом.

**Направление фикса.** arg0 attr/name-shaped → резолв символа через `ctx.ref_symbol_lookup`
(ровно как `INVOKES_ACTIVITY` резолвит `execute_activity_method`'s arg0) → `sym:`-id метода.
Дальше два варианта:
- (a) **claim + линковка** (предпочтительно, кросс-файловая корректность): эмитить
  `temporal_signal_send`-claim `(src_id, target_method_symbol)`, а в S7 искать CONSUMES-ребро
  из этого символа в `chan:temporal_signal:*` и вешать PRODUCES в ТОТ ЖЕ канал;
- (b) in-file быстрый путь: если метод в том же файле — взять имя прямо из его декоратора.
Резолюция: static/1.0 при разрешённом символе (ссылка — полная истина, как у activity);
существующий heuristic/0.6 оставить для строковой ветки. Нерезолвимый символ → прежний
`signal_name_unresolved`, без догадок.
**Тест:** синтетик — воркфлоу с `@workflow.signal(name="go")` в файле A; в файле B
`await handle.signal(WF.go, payload)` → ожидать PRODUCES в `chan:temporal_signal:go`
и пару sender↔handler; негативный пин: `handle.signal(some_runtime_var)` → счётчик, не ребро.

## Мелкое / принятое

- `producer_unresolved_channel = 1` — builtin aiokafka-идиом видит
  `send_and_wait(topic=payload.topic_name)` внутри самой обёртки; наш enum-идиом
  покрывает ту же публикацию с другой стороны. Честный шум, не баг.
- NEXT_SEGMENT по Kafka = 0 и после R2: producer-топики (`payments.kyc-engine.
  restrictions.changed.v1`, `databus.notifications.gateway.*`) не потребляются никем в
  срезе, а consumer-каналы идентифицируются по event_type. Свойство среза + разная
  идентичность сторон; вариант (c) из прошлого open-gaps (event_type у producer'а)
  остаётся открытым, но приоритет ниже R4/R5.
- Реальная находка для команды сервисов (не codegraph): клиент camunda зовёт
  `DELETE /api/v1/requests/{verification_uid}/linked-customers`, а vr на этом пути
  объявляет только PATCH и GET (`app/api/v1/verification_requests.py:388,402`).

## Приоритеты для фикс-агента

1. **R4 (префикс роутера)** — разблокирует ВЕСЬ кросс-сервисный HTTP-лег: доказанные
   23 корректных static/1.0 ребра из 24 заякоренных claim'ов, здесь и сейчас.
2. **R5 (типизированные сигналы)** — разблокирует сигнальный хоп: 18 отправок ↔ 25 уже
   существующих каналов; без него M7-роли остаются «висящими» узлами.

## Воспроизведение / доказательная база

Граф `pilot_kyc` (тот же workspace). R4: `MATCH (c:Channel {channel_kind:'http_route'})`
— у vr-роутов пути без `/api/v*`; claims — `staging.db: SELECT payload_json FROM claims
WHERE kind='http_call'` (24 с `base_url_env='SERVICE_VERIFICATION_REQUESTS_URL'`);
симуляция — `_templates_match(prefix+route, claim)`. R5: `grep -n '\.signal(' <camunda>/app
| grep -v '@workflow.signal'` → 18; `report.json.services[].signal_name_unresolved` → 18.
env-карта: `build_env_service_map([helm], {…})` → `{'SERVICE_VERIFICATION_REQUESTS_URL':
'verification-requests'}`. Наружу — только структурные факты и числа.
