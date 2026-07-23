# Re-run пилота после M6 — подтверждение закрытия gap'ов на реальных сервисах (ПОЛНАЯ)

> Дата: 2026-07-23. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-BRIEF.md`.
> Baseline — `2026-07-23-pilot-real-services-gaps.md` (GAPS §1/§8): claims = только
> `temporal_start_mark:4`, ноль async-рёбер. Задача re-run: подтвердить, что все 5 gap'ов,
> закрытых вехой M6, работают на РЕАЛЬНЫХ трёх сервисах (camunda-gateway /
> verification-requests / document-management).
> Эта версия — полная (qualified-имена/пути). Санитизированная — `...-pilot-rerun-SANITIZED.md`.

## 0. Обновление и смоук

`git pull` → HEAD `10e168c` (≥ 1ab5939). doctor зелёный (python 3.14 / node v25.6 / npx 11.8;
FalkorDB все feature-probes OK). M6-гейт: `pytest -m "scip and falkordb"
tests/eval/test_m6_gate.py` → **1 passed, 0 failed, 14.2s** (junit-xml, т.к. rtk-хук искажает
консольный вывод). Тулчейн и все пять механизмов на машине работают.

## 1. Итерации идиом

Фактически **одна итерация**: прочитал реальный код, сверил FQN/глобы, написал конфиг —
заработало с первого индекса. Что пришлось описать (данные, не код):

- **HTTP decorator-SDK** (camunda + document-management): `route_from: {decorator: path_template,
  arg: 0}`, `call: "driver.fetch_content|driver.fetch"`, `verb_from: {request_ctor: Request,
  enum: Method}`, `class_glob: "*Client"`, `file_glob: "**/clients/*.py"`.
- **base_class consumer**: FQN базы **различается по сервисам** — ключевая находка итерации:
  - dm/vr: `kyc_base_consumer.base.BaseConsumer` (прямое наследование);
  - camunda: `app.consumers.base.BaseConsumer` — локальная обёртка
    `class BaseConsumer(KYCBaseConsumer[TEvent])` над **алиас-импортом** shared-базы
    (`from kyc_base_consumer import BaseConsumer as KYCBaseConsumer`). Конкретные camunda-консьюмеры
    наследуют локальную обёртку (часто с mixin-первым базовым: `class X(Mixin, BaseConsumer[Event])`),
    поэтому base_class-идиом camunda целится в `app.consumers.base.BaseConsumer`, а generic_arg 0
    корректно берёт `[Event]` даже при mixin-первом порядке баз.
  - handler_method `process_event`, `event_type_from: {generic_arg: 0}`, `topic: {attr: self.config.topic}`.
- **producer wrapper**: `call: app.services.producer.KYCEventPublisher.publish`, `topic: {arg: 1}`
  (`publish(self, body, topic_name, customer_uid)`). Топик — `payload.topic_name` (рантайм,
  `KycTopicName`-enum) → честный unresolved (см. §3, PRODUCES).

## 2. Чекпойнты (§2 брифа) — факты и вердикт

Прогон: `codegraph index <ws> --no-embed`, 325 файлов, **18.1s** (тёплый scip-кэш; degraded=0,
все три `full`), unresolved calls 4.9%, staged CALLS valid dst 91.3% (within-service граф —
без регрессий от baseline).

| Чекпойнт | Ожидание | Факт | Вердикт |
|---|---|---|---|
| INVOKES_ACTIVITY | ≈80 (camunda) | **64**, resolution=static, confidence=1.0 | ✅ (0→64; ~78% из ~82 call-site'ов `execute_activity_method`; остаток — activity по переменной/индирекции, см. §3) |
| temporal_start marks | 4 → 7 | **7** (+3 `start_child_workflow`) | ✅ точно |
| http_call claims → CALLS_HTTP | ≈55 (42 fetch_content + 13 fetch), >0 | **44 claims → 44 CALLS_HTTP** (3 static резолвнуты в чужой роут, 41 heuristic/0.5 unresolved); `http_verb_unresolved=15` (camunda 6 + dm 9) видим в отчёте | ✅ (миссы видимы, не молчание) |
| CONSUMES | по одному на `BaseConsumer[...]`-подкласс | **16**, static/1.0 → **15 event_type-каналов** (AMLRequestUpdatedEvent потреблён дважды: v1+v2) | ✅ |
| PRODUCES | const→канал; динамический топик→`producer_unresolved_channel` | **0 PRODUCES**, `producer_unresolved_channel=2` | ✅ честно (топик динамический) |
| CONTAINS topic→event | при `topic:{attr:...}` | consumer-каналы построены по event_type (generic), topic-attr не даёт литерала — event_type-идентичность канала (стабильнее) | ✅ |
| NEXT_SEGMENT | > 0 | **3**, но **все ложные** — три несвязанных клиент-метода схлопнулись на один dm-роут (S7 без target-якоря; см. §3.1 и `...-open-gaps.md` R1); корректных кросс-хопов = 0 | ⚠️ ребро есть, но резолюция неверна |
| confidence | с venv лучше фикстурного 0.6 | INVOKES_ACTIVITY/CONSUMES = **static/1.0** | ✅ |

`consumer_base_class_no_generic = 0` во всех сервисах (все консьюмеры разрешили generic-событие).

## 3. Дельта vs baseline (GAPS §1) и трасс-вердикт

**Типы рёбер: baseline → re-run**

| Ребро | GAPS baseline | Re-run |
|---|---|---|
| INVOKES_ACTIVITY | 0 | **64** |
| CALLS_HTTP | 0 | **44** |
| CONSUMES | 0 | **16** |
| NEXT_SEGMENT | 0 | **3** |
| temporal_start_mark (claims) | 4 | **7** |
| event_type-каналы | 0 | **15** |
| PRODUCES | 0 | 0 (динамический топик — честно) |

**Трасс-вердикт (§3 брифа — главный).** Смешанный. Within-service Temporal-леги работают:
workflow-менеджеры → `INVOKES_ACTIVITY` → активности (напр. `SOFVerificationRequestManager.setup`
→ `VerificationRequestsActivities.create_request`/`LegacylizerActivities.get_customer_info`) —
64 ребра static/1.0, корректны. НО кросс-сервисный HTTP-хоп, хоть и материализуется как ребро,
**резолвится неверно** (см. §3.1) — механизм CALLS_HTTP работает (claims с точными путями), а
S7-таргетинг без base_url_env мислинкует.

### 3.1. CALLS_HTTP кросс-сервис: 3 «резолвнутых» ребра — ЛОЖНЫЕ (детальный разбор)

Три несвязанных клиент-метода с РАЗНЫМИ корректно-извлечёнными путями привязались к одному dm-роуту:

| SRC клиент-метод | path_template (верный, из claim) | привязалось к (S7) |
|---|---|---|
| `VerificationRequestsClient.get_request_step` | `/api/v1/steps/{step_uid}` | dm `/{initiator_ref}/{vendor}/{vendor_version}/parsed-data` |
| `VerificationRequestsClient.get_verification_request` | `/api/v1/requests/{verification_uid}` | dm `/…/parsed-data` |
| `LegacylizerClient.get_customer_info` | `/api/v1/customer-info/{customer_uid}` | dm `/…/parsed-data` |

Пути не соответствуют; у целевого канала `service=""`. Причина: `base_url:{attr: self.host}` без
`env` → S7 матчит по path_template поперёк всех сервисов без target-якоря и без валидации формы.
**Корректная кросс-сервисная HTTP-резолюция на срезе = 0** (3 ложных, 41 unresolved). Решение —
env→service из `.helm` (детерминированная цепочка `self.host`←`config.services.*`←`SERVICE_*_URL`
env→helm-hostname→имя сервиса); подробно и с направлением фикса — в
`2026-07-23-pilot-rerun-open-gaps.md` (R1). Плюс там же — R2 (producer dynamic topic) и R3
(Temporal signals — 34 хендлера + 45 отправок, не покрыты вообще).

**Честные оговорки (отрицательный результат валиден):**

1. **CALLS_HTTP кросс-сервис резолвится неверно без base_url_env — см. §3.1** (3 ложных ребра,
   41 unresolved, корректных = 0). Решение через `.helm` (env→service) — в `...-open-gaps.md` R1.
2. **INVOKES_ACTIVITY 64, не ~80.** ~78% call-site'ов `workflow.execute_activity_method`.
   Неразрешённые — где arg0 не резолвится в activity-узел (activity по переменной/фабрике, либо
   определён вне просканированного scope). Не регресс — честный потолок статики.
3. **NEXT_SEGMENT — только HTTP (3), Kafka event-пар = 0.** Producer camunda публикует с
   динамическим топиком (`payload.topic_name`) → канала нет; consumer-каналы — по event_type.
   В пределах 3-сервисного среза producer-события (RESTRICTIONS_CHANGED, CREATE_*_NOTIFICATION)
   и consumer-события (OCRDataEvent, AMLRequestUpdatedEvent, KYPCreatedEventBody…) **не
   пересекаются** — реальные контрагенты вне среза. Поэтому Kafka producer↔consumer NEXT_SEGMENT
   здесь физически не образуется; это свойство среза, а не механизма (M6-гейт доказывает механизм
   на `fixtures/realstack/`).
4. **verb_unresolved=15**: document-management использует `ProxyRequest(Request)`-подкласс и
   `driver.fetch` — `verb_from:{request_ctor: "Request"}` не матчит имя `ProxyRequest`; честный
   видимый мисс (9 в dm, 6 в camunda). Уточняемо (доп. request_ctor-альтернативы) — в бэклог.

## 4. Вывод

**Частично.** Три из пяти gap'ов **подтверждены закрытыми и корректными на реальном коде**:
`INVOKES_ACTIVITY` 0→64 (static/1.0), `CONSUMES` 0→16 (static/1.0, event_type-каналы), temporal
start-marks 4→7 — все верны, within-service граф без регрессий (91.3% valid dst, 4.9% unresolved,
degraded=0), миссы стали видимы вместо молчания. Но **три остаточные проблемы требуют доработки**
(вынесены в `2026-07-23-pilot-rerun-open-gaps.md`):
- **R1 HTTP-таргетинг**: ребро CALLS_HTTP создаётся, но кросс-сервисная резолюция без base_url_env
  **ложная** (3 ложных ребра, §3.1) → корректных кросс-хопов 0; решение — env→service из `.helm`.
- **R2 Producer**: `PRODUCES=0` (динамический топик); нужен enum/event_type-якорь.
- **R3 Temporal signals**: не покрыты вообще (34 хендлера + 45 отправок) — крупный невидимый пласт.
Механизмы M6 (activity/consumer) работают; HTTP-лег, producer и сигналы — открыты.

## Приложение — воспроизведение

Workspace-идиомы (пути к чекаутам локальные, вне репо): decorator-sdk (camunda+dm), base_class
consumer (все три, FQN базы различается), producer wrapper (camunda). `codegraph index <ws> --no-embed`
→ сверять типы рёбер / `channel_kind`-split (http_route 135 / event_type 15 / kafka_topic 1) /
claims в staging (`http_call:44, temporal_start_mark:7`). Граф `pilot_kyc`, staging.db и .scip — вне
репозитория (scratchpad), наружу не выносятся.
