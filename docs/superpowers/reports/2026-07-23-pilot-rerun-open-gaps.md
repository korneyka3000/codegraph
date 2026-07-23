# Остаточные gap'ы после M6 re-run — Producer / HTTP-таргетинг / Temporal signals

> Дата: 2026-07-23. Продолжение `2026-07-23-pilot-rerun.md`. M6 закрыл 5 gap'ов из первого
> пилота (INVOKES_ACTIVITY 64, CONSUMES 16, temporal marks 7 — корректны и подтверждены), НО
> re-run на реальном коде вскрыл три **остаточные/новые** проблемы, требующие доработки. Этот
> документ — вход для фикс-агента: корневая причина + доказательство + направление фикса на
> каждую. Правило прежнее: regression-тесты на синтетике (`fixtures/realstack/`), не на коде
> юзера.

## R1 (высокий приоритет) — кросс-сервисный HTTP-таргетинг без base_url_env даёт ЛОЖНЫЕ совпадения

**Симптом.** Из 44 http_call claims: 41 heuristic-unresolved, а **3 «резолвнутых» (static) —
ложные положительные**. Три несвязанных клиентских метода с РАЗНЫМИ, корректно извлечёнными
путями схлопнулись на один и тот же роут другого сервиса:

| SRC (клиент-метод) | извлечённый path_template (верный) | куда привязалось (S7) |
|---|---|---|
| `VerificationRequestsClient.get_request_step` | `/api/v1/steps/{step_uid}` | dm `/{initiator_ref}/{vendor}/{vendor_version}/parsed-data` |
| `VerificationRequestsClient.get_verification_request` | `/api/v1/requests/{verification_uid}` | dm `/…/parsed-data` |
| `LegacylizerClient.get_customer_info` | `/api/v1/customer-info/{customer_uid}` | dm `/…/parsed-data` |

Пути **не соответствуют** матчу; у целевого `Channel` даже `service=""`. То есть `NEXT_SEGMENT=3`
из основного re-run-отчёта — **не 3 корректных кросс-хопа, а 3 артефакта матчинга**. Реальная
корректная кросс-сервисная HTTP-резолюция на этом срезе = **0**.

**Корневая причина.** Идиом задал `base_url: {attr: self.host}` без `env` (хост клиента —
динамический атрибут из Settings). Экстрактор корректно эмитит claim с точным path_template и
`base_url_env=null`, но **S7-линковка без target-якоря матчит claim к роуту по одному лишь
path_template поперёк ВСЕХ сервисов** и мислинкует (или funnel'ит на первый подходящий по форме).
`http_client_ext.py` не виноват (claims верные) — проблема в S7 route-matching при отсутствии
target-сервиса + отсутствие валидации соответствия путей.

**Направление фикса (идея пользователя — извлечь target из `.helm`).** Цепочка client→target
**детерминирована и статически восстановима**:

1. `app/clients/*.py`: `self.host` присваивается из `config.services.<x>_url` (BaseClient ctor).
2. `app/config/services.py`: `SettingsConfigDict(env_prefix="service_")` → поле
   `verification_requests_url` ← env **`SERVICE_VERIFICATION_REQUESTS_URL`**.
3. `.helm/values/<env>/values.yaml`: `SERVICE_VERIFICATION_REQUESTS_URL:
   "http://verification-requests.kyc.svc.cluster.local:8000"` → hostname `verification-requests`
   = **имя сервиса** в workspace.

Итог: `SERVICE_*_URL` env → DNS-хост → service-name. Предложение:
- Дать идиому/воркспейсу источник **env→service map** (парсинг `.helm/values/*/values.yaml`
  по ключам `SERVICE_*_URL`, hostname→service), либо явный `base_url: {env: SERVICE_..._URL}`
  на клиента (тогда S7 пиннит target-сервис из уже существующего `http.base_url_env` реестра).
- **Пиннить CALLS_HTTP target по env→service, затем матчить path ТОЛЬКО среди роутов этого
  сервиса** + требовать совпадения формы пути (иначе — unresolved, не ложный матч).
Это уберёт 3 ложных ребра и даст корректные (напр. `VerificationRequestsClient.*` →
verification-requests-роуты, `DMoutClient.*` → dmout, вне среза → честный external).
**Тест:** синтетик из 2 сервисов + фикстурный env→service map; claim с path сервиса A не должен
матчиться к одинаково-именованному-но-другому роуту сервиса B.

## R2 (средний) — Producer: топик статически известен как enum, но динамичен в точке вызова

**Симптом.** `PRODUCES=0`, `producer_unresolved_channel=2` (честно). Единственная точка
публикации — обёртка `KYCEventPublisher.publish(body, topic_name, customer_uid)`, вызвана как
`publisher.publish(body, payload.topic_name, ...)` — `topic:{arg:1}` = `payload.topic_name`
(рантайм-атрибут) → канал не строится.

**Нюанс.** Топики — **конечный набор строковых констант** enum `KycTopicName`
(`kyc.camunda.step_changed.basic_survey`, `…basic_kyc`, `…partner_profile`,
`…limit_increase_sof`, `…ep_inconsistency`, RESTRICTIONS_CHANGED, CREATE_*_NOTIFICATION). То есть
идентичность канала В ПРИНЦИПЕ статична, просто не на call-site обёртки (там — атрибут payload).

**Направление фикса (варианты, от дешёвого к точному).**
- (a) Регистрировать producer-каналы по **всем членам enum**, указанного в идиоме
  (`topic: {enum: "app.models.enums.KycTopicName"}` — новый источник ValueSpec): создаёт
  topic-каналы для всех возможных топиков без резолва call-site (over-approximation, но
  producer-сторона канала появляется).
- (b) Лёгкий intra-procedural dataflow: если `payload.topic_name` присваивается литералом
  enum в пределах активности/воркфлоу, разрешить его к константе.
- (c) Как и с consumer'ами — строить кросс-сервисные каналы на **event_type** (тип payload/тела
  события), а не на топике; тогда producer↔consumer пары матчатся по классу события. Требует у
  producer-идиома источника event_type (напр. из аннотации типа `payload.body`).
Честно: без (b)/(c) реальные producer↔consumer NEXT_SEGMENT-пары на этом стеке не образуются.

## R3 (новый, не покрыт вообще) — Temporal signals: целое неохваченное измерение

**Факты (camunda-gateway, реальный код).** `temporal_ext.py` обрабатывает только
`@workflow.defn`/`@activity.defn`-роли, `execute_activity[_method]` и `start_workflow` —
**сигналы/квери/апдейты не трогает ни строкой** (grep по экстрактору — ноль совпадений). В коде:
- **34** декоратора `@workflow.signal|query|update` — обработчики сигналов
  (`@workflow.signal(name="complete-survey")`, `"survey-ready"`, `"resolution-step-updated"`,
  `"call-step-finished"`, `"review-step-created-from-bo"`, …).
- **45** сайтов отправки `.signal(` + **3** `get_external_workflow_handle` (кросс-воркфлоу-сигналинг).

**Почему важно.** Сигналы — первоклассная async-граница в orchestration-стеке: внешний код/
consumer/родительский воркфлоу будит ждущий воркфлоу через `handle.signal(name, ...)`, а
`@workflow.signal`-метод — точка входа реакции. Типичная реальная цепочка здесь:
**Kafka-событие → consumer.process_event → (start/signal workflow) → @workflow.signal-хендлер →
activity/HTTP**. Сигнальный хоп сейчас невидим, поэтому трассировка рвётся на «разбудить воркфлоу».

**Направление фикса.**
- Роль/узел: `@workflow.signal|query|update`-методы — как `TemporalSignalHandler` (аналог
  activity-роли), имя канала — из `name=`-аргумента декоратора (стабильная идентичность, как
  event_type у consumer'ов).
- Ребро/канал: сайт `handle.signal("<name>", ...)` → `Channel(kind=temporal_signal, name=<name>)`
  ← `@workflow.signal(name="<name>")`-хендлер (producer/consumer-подобная пара по имени сигнала);
  `get_external_workflow_handle(...).signal(...)` — кросс-воркфлоу-вариант (как start_child).
- Трассировка: сигнальные каналы включить в сегментацию `trace_process`, чтобы «разбудил
  воркфлоу сигналом X» стал явным хопом.
**Тест:** синтетик — воркфлоу с `@workflow.signal(name="go")` + внешний сайт `h.signal("go")` →
ожидать `Channel(temporal_signal:"go")` со связью sender→handler.

## Приоритеты для фикс-агента

1. **R1 (HTTP env→service из .helm)** — убирает ложные рёбра, даёт корректный кросс-сервисный
   HTTP; максимум точности за счётный объём (парсер helm-values + S7 target-pin + path-валидация).
2. **R3 (Temporal signals)** — открывает крупный невидимый пласт orchestration-трассировки
   (34 хендлера + 45 отправок); новый channel-kind + роль, по образцу activity/event_type.
3. **R2 (Producer)** — event_type-идентичность каналов (вариант c) смыкается с уже рабочей
   consumer-стороной и делает producer↔consumer пары резолвимыми.

## Воспроизведение / доказательная база

Граф `pilot_kyc` (тот же workspace re-run). Ложные HTTP-рёбра:
`MATCH (s)-[r:CALLS_HTTP {resolution:'static'}]->(c:Channel) RETURN s.id, c.path_template` — три
разных src, один и тот же `/…/parsed-data`, `c.service=""`. http_call claims (пути верные) —
`staging.db: SELECT payload_json FROM claims WHERE kind='http_call'`. Сигналы —
`grep -rn '@workflow.signal' <camunda>/app` (34) и `grep -c '\.signal('` (45). Env→service —
`.helm/values/*/values.yaml` ключи `SERVICE_*_URL` + `app/config/services.py` env_prefix="service_".
Никакие staging.db/.scip/тексты кода наружу не выносятся; здесь — только структурные факты и числа.
