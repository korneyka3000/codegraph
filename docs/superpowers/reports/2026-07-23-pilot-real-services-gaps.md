# Пилот на реальных KYC-сервисах — карта gap'ов идиом (что не работает и почему)

> Дата: 2026-07-23. Прогон по `docs/superpowers/pilot/WORK-MACHINE-PILOT-BRIEF.md` на
> трёх реальных сервисах (FastAPI + aiokafka + Temporal + рукописный aiohttp-SDK).
> Фокус этого документа по решению пользователя — **не** трассировка/retrieval, а
> **инженерная карта недостатков экстракторов/идиом**: async-леги (HTTP-вызовы,
> Temporal-активности, consumer, producer) на этом стеке дают **ноль** рёбер, и здесь
> зафиксировано, ПОЧЕМУ и ЧТО править. Документ — вход для агента, который будет чинить
> codegraph; каждый gap указывает на конкретный файл:строку и предлагает направление
> фикса + подсказку по тесту (regression — на синтетике, не на коде юзера).
>
> Реальный workspace-конфиг и `staging.db`/граф оставлены вне репозитория (scratchpad);
> здесь — только числа, конвенции и точки правки.

## 0. TL;DR

| Лег | Ребро | Результат | Корневая причина | Файл codegraph |
|---|---|---|---|---|
| HTTP-клиент | `CALLS_HTTP` | **0** (0 http_call claims) | маршрут в декораторе `@path_template`, verb в `Method.X`-enum, вызов через `driver.fetch_content(request)` — экстрактор ждёт callee ∈ {get,post,put,delete,patch} и arg0=URL-шаблон | `extractors/http_client_ext.py:111,132,161` |
| Temporal activity | `INVOKES_ACTIVITY` | **0** (при 43 активностях, 80 вызовах) | код зовёт `workflow.execute_activity_method(...)`; матчер жёстко привязан к `execute_activity` | `extractors/temporal_ext.py:143` |
| Temporal child wf | `start_workflow`-claim | частично: **4/7** | `start_workflow` резолвится, `start_child_workflow`/`execute_child_workflow` — нет | `extractors/temporal_ext.py:167` |
| Kafka consumer | `CONSUMES` | **0** | бизнес-обработчик — `process_event` в подклассе `BaseConsumer[Event]` (shared lib); сырой `AIOKafkaConsumer(self.config.topic,…)` живёт в `setup()`, топик — динамический config-атрибут | `extractors/kafka_ext.py`, DSL `config/models.py:68` (нет kind «subclass») |
| Kafka producer | `PRODUCES` | **0** | публикация — обёртка `KYCEventPublisher.publish(body, topic_name, …)` над `producer.send_and_wait(topic=…)`; топик динамический (`payload.topic_name`), вызов идёт через локальную переменную из `await self.producer()` | `extractors/kafka_ext.py`, `parsing/consts.py` (resolve_arg) |

**Что при этом работает отлично** (важно для калибровки — проблема НЕ в резолвере вообще):
within-service граф чистый — 1553 `CALLS` (100% `static`, 0 heuristic), 91.3% staged CALLS
with valid dst, unresolved 4.9%, degraded=0 на всех трёх сервисах (venv'ы установлены,
scip резолвит), 7 Temporal-воркфлоу как `BusinessProcess`, 98 FastAPI-роутов. Пробел —
исключительно в **кросс-async-границах**, и он структурный: все четыре конвенции этого
стека реализованы через shared-библиотеки и/или динамические значения, которых нынешние
экстракторы и DSL идиом не покрывают.

## 1. Прогон (доказательная база)

Workspace: 3 сервиса, `python: .venv` у каждого, excludes `tests/**`,`migrations/**`.
Команда: `codegraph index <ws>.yaml --no-embed` (чистый замер пайплайна).

```
services: camunda-gateway 178f / verification-requests 93f / document-management 54f  (TOTAL 325 files)
time: 47.3s cold, degraded: []  (все три full, from_cache=false)
health: unresolved calls = 4.9%, staged CALLS with valid dst = 91.3%, dropped(CALLS)=148 (missing_endpoint)
linking: calls_http=0, next_segments=0, channels_gc=0, processes=7, marks=4
```

Типы рёбер в загруженном графе (`MATCH ()-[r]->() RETURN type(r), count`):
`CONTAINS 2388 · CALLS 1553 · IMPORTS 1018 · HANDLES 98 · PART_OF_PROCESS 7 · DEPENDS_ON 2`.
**Отсутствуют полностью:** `PRODUCES`, `CONSUMES`, `CALLS_HTTP`, `NEXT_SEGMENT`, `INVOKES_ACTIVITY`.

Claims в `staging.db` (`SELECT kind,count FROM claims GROUP BY kind`): **`temporal_start_mark: 4` — и всё.**
Ноль `http_call`, ноль kafka producer/consumer claims — async-экстракторы не эмитят claim'ов вовсе.

Каналы в графе: 95 узлов `Channel`, **все — http_route** (FastAPI-роуты сервисов: `POST /config`,
`POST /{document_uid}/submit`, …). Kafka/event-каналов — 0. Ни один `Channel` не связан
cross-service (`calls_http=0`, `next_segments=0`) → трассировка бизнес-процесса через async-
границу здесь физически не собирается; `codegraph trace` по воркфлоу деградирует в плоский
CALLS-обход (тот же кейс, что M4-пилот §7.3 — монолит без Channel-границ).

## 2. Gap #1 — HTTP-клиентский SDK (CALLS_HTTP=0)

**Реальная конвенция** (все 11 клиентов camunda-gateway, `app/clients/*.py`, класс `*Client(BaseClient)`):

```python
class DMoutClient(BaseClient):
    @path_template("/v1/dmout/user_hv/uuid/{customer_uid}")           # ← МАРШРУТ здесь
    async def get_client_hv_sign(self, customer_uid, **kwargs):
        request = Request(Method.GET, self.host, Path(kwargs["path"], customer_uid=customer_uid))  # ← VERB здесь
        return await self.driver.fetch_content(request, parser=...)   # ← сам ВЫЗОВ здесь
```

Счётчики вызовов драйвера: `self.driver.fetch_content` ×42, `self.driver.fetch` ×13. Прямых
`get/post/put/delete/patch(...)`-вызовов — **0**. Verbّы живут в `Method.{GET×20,POST×21,PATCH×12,DELETE×1}`.

**Корневая причина.** `http_client_ext.py`:
- `_VERBS = {get,post,put,delete,patch}` (стр. 111), кандидат — только attribute-call с
  `callee_name ∈ _VERBS` (`_is_candidate_call`, стр. 132). `fetch_content`/`fetch` не проходят.
- Даже если бы прошли — `_resolve_path` (стр. 161) берёт **arg0** и ждёт URL-шаблон
  (`<base>/…` или `"/…"`). Здесь arg0 = `request` (объект `Request`), а путь — в декораторе.

**Направление фикса.** Ввести конфигурируемую конвенцию «SDK с декоратор-маршрутом и
driver-индирекцией»: в `HttpClientIdiom` добавить поля вида
`route_from: {decorator: "path_template", arg: 0}` (откуда брать path_template) и
`call: "self.driver.fetch_content|fetch"` (какой вызов считать HTTP-вызовом), а verb — из
первого аргумента `Request(Method.X, …)` (`verb_from: {call_arg: 0, enum: "Method"}`) либо из
имени метода. Экстрактор тогда: (1) находит методы класса `*Client`, у которых есть декоратор
`@path_template(...)`; (2) внутри метода видит вызов `driver.fetch_content(...)`; (3) path — из
декоратора, verb — из `Method.X` в конструкторе `Request`. Это снимает жёсткую привязку к
verb-имени callee и arg0-URL.
**Тест:** синтетический сервис-клиент с `@route(...)`-декоратором + `driver.call(req)` + `Method.X`
→ ожидать `CALLS_HTTP`-claim с корректным `path_template`/`verb`. Без правки реального кода юзера.

## 3. Gap #2 — Temporal activity invocation (INVOKES_ACTIVITY=0)

**Реальная конвенция** (camunda `app/kyc_engine/workflows/*`): активности вызываются как
`await workflow.execute_activity_method(SomeActivity.some_method, arg, ...)` — **80 вызовов**.
Активности определены штатно: `@activity.defn(name="...")` (43 узла распознаны верно).

**Корневая причина.** `temporal_ext.py:143`:
```python
if call.callee_name != "execute_activity" or call.receiver_text != "workflow":
    continue
```
Матчится только `execute_activity`. Форма `execute_activity_method` (bound-method ref вместо
activity-fn/name) — не покрыта. arg0 в обоих случаях резолвится одинаково
(`arg0.name_start_byte` → `ref_symbol_lookup`, стр. 149-150) — то есть фикс минимален: расширить
множество callee-имён.

**Направление фикса (минимальное, высокоценное — оживляет ~80 рёбер в camunda).** Заменить
строгое сравнение на членство в множестве Temporal-activity-инвокаций:
`{execute_activity, execute_activity_method, execute_local_activity, execute_local_activity_method,
start_activity, start_local_activity}` (receiver `workflow`). Для `start_*`-вариантов — тот же
claim/edge, arg0-резолюция идентична. **Тест:** синтетический workflow с
`workflow.execute_activity_method(Act.m, …)` → ожидать `INVOKES_ACTIVITY` на `Act.m`.

## 4. Gap #3 — Temporal child workflow (start_workflow резолвится 4/7)

`temporal_ext.py:167` матчит только `callee_name == "start_workflow"` (4 вызова — ок,
`temporal_start_mark: 4`). Но код также использует `start_child_workflow` ×3 (и штатно бывает
`execute_child_workflow`). Они не резолвятся. **Фикс:** добавить `start_child_workflow`/
`execute_child_workflow` в матчер claim'а start-workflow (та же arg0-резолюция). **Тест:**
синтетик с `workflow.start_child_workflow(ChildWF.run, …)`.

## 5. Gap #4 — Kafka consumer (CONSUMES=0)

**Реальная конвенция** (все консьюмеры трёх сервисов, `app/consumers/*.py`):
```python
class OCRDataConsumer(BaseConsumer[OCRDataEvent]):     # base из shared lib kyc_base_consumer
    async def process_event(self, event: OCRDataEvent) -> bool:   # ← бизнес-обработчик
        ...
    async def setup(self) -> None:
        self.consumer = AIOKafkaConsumer(self.config.topic, ...)  # ← сырой ctor, топик из config-атрибута
```
Обработчик — **override `process_event`** в подклассе `BaseConsumer[Event]`; цикл чтения и
диспатч живут в базовом классе (в venv, статически невидимо). Топик — `self.config.topic`
(поле Settings, из env), не литерал.

**Корневая причина.** (1) DSL `ConsumerIdiom` (`config/models.py:68`) поддерживает `kind ∈
{call, decorator, dispatch_dict}` — нет вида «подкласс базового класса с методом-обработчиком».
(2) Builtin `aiokafka-consumer-init` (`kind=call, call="aiokafka.AIOKafkaConsumer"`) даже если бы
матчнулся на ctor в `setup()`, привязал бы «consumer» к `setup`, а не к `process_event`, и топик
`self.config.topic` (attr, динамический) не даёт резолвимой идентичности канала. Эмпирически —
**0 consumer claims** (ctor-идиом не сматчился на этом коде).

**Направление фикса.** Новый вид consumer-идиомы, напр.:
```yaml
consumers:
  - name: base-consumer-subclass
    kind: base_class            # НОВОЕ
    base_class: "kyc_base_consumer.base.BaseConsumer"   # scip-резолв подкласса
    handler_method: "process_event"                     # какой метод — сегмент-обработчик
    event_type_from: { generic_arg: 0 }                 # BaseConsumer[OCRDataEvent] → event_type
    topic: { attr: "self.config.topic" }                # для канала — или связать через env/Settings
```
Ключевое: (а) распознавать подкласс по базовому классу (через scip class-hierarchy), (б) сегментом
CONSUMES делать `handler_method`, а не ctor/`setup`, (в) event_type брать из generic-параметра
`BaseConsumer[Event]` (это стабильная статическая идентичность канала, в отличие от динамического
топика). Тогда канал матчится по **event_type** (`OCRDataEvent`, `AMLRequestUpdatedEvent`, …),
что переносимо между producer/consumer без литерального топика.
**Тест:** синтетик с `class C(BaseConsumer[FooEvent])` + `process_event` → ожидать роль consumer на
`process_event` и `Channel(event_type=FooEvent)`.

## 6. Gap #5 — Kafka producer (PRODUCES=0)

**Реальная конвенция** (camunda `app/services/producer.py`):
```python
class KYCEventPublisher:
    async def publish(self, body: str, topic_name: str, customer_uid) -> None:
        producer = await self.producer()                 # локальная из await self._producer
        await producer.send_and_wait(topic=topic_name, value=..., key=...)   # topic — KWARG, динамический
```
Единственный call-site публикации: `publisher.publish(body, payload.topic_name, payload.customer_uid)`
(`app/kyc_engine/activities/kafka_events.py`). Топик — `payload.topic_name` (атрибут payload),
статически не литерал.

**Корневая причина.** Builtin producer-идиом `aiokafka-send-and-wait` берёт топик из
`name_from=arg0`, но здесь `topic=` передан как **kwarg**, а не позиционно; и значение —
динамическое. Плюс вызов идёт на локальной переменной `producer` (из `await self.producer()`),
что мешает резолву receiver'а как `AIOKafkaProducer`. Эмпирически — **0 producer claims**.

**Направление фикса.** (1) В `ChannelSpec`/`ValueSpec` поддержать источник топика из **kwarg**
(`topic: {kwarg: "topic"}`) — сейчас `name_from=arg0` не покрывает keyword-передачу. (2) Для
обёрток-паблишеров разрешить producer-идиом на **пользовательском** методе-обёртке
(`call: "app.services.producer.KYCEventPublisher.publish"`, `topic: {arg: 1}`) — тогда точка
producer'а определяется по бизнес-обёртке, а не по низкоуровневому `send_and_wait`. Но при этом
топик всё равно динамический (`payload.topic_name`) — честно: **резолвимой идентичности канала на
call-site нет**. Реальный мэтч producer↔consumer здесь возможен только через **event_type**
(тип payload/события), а не через топик — что смыкается с фиксом Gap #4 (event_type из generic
consumer'а). Рекомендация: строить кросс-сервисные каналы этого стека на event_type-идентичности
(класс события), а не на литеральном топике.
**Тест:** синтетик producer-обёртка с `send_and_wait(topic=<kwarg>)` → покрыть kwarg-источник топика.

## 7. Приоритеты для фикс-агента

1. **Gap #2 (execute_activity_method)** — минимальный фикс, максимальный эффект: оживляет ~80
   `INVOKES_ACTIVITY` в camunda, делает Temporal workflow→activity трассируемым. Стр.
   `temporal_ext.py:143`. Начать отсюда.
2. **Gap #3 (child workflow)** — тривиальное расширение того же матчера (`temporal_ext.py:167`).
3. **Gap #1 (HTTP decorator-SDK)** — среднее: расширение `HttpClientIdiom` DSL + экстрактора под
   декоратор-маршрут и driver-индирекцию. Даёт CALLS_HTTP из camunda в остальные сервисы.
4. **Gap #4/#5 (consumer subclass + producer wrapper, каналы по event_type)** — крупное: новый
   consumer-kind `base_class` + event_type-идентичность каналов + kwarg-источник топика. Это то,
   что реально включает кросс-сервисную async-трассировку на данном стеке.

## 8. Как воспроизвести

Workspace (пути к чекаутам — локальные; excludes обязательны):
```yaml
version: 1
graph_name: pilot_kyc
services:
  - { name: camunda-gateway,       path: <checkout>, python: .venv, exclude: ["tests/**","migrations/**","bpmn/**"] }
  - { name: verification-requests, path: <checkout>, python: .venv, exclude: ["tests/**","migrations/**"] }
  - { name: document-management,   path: <checkout>, python: .venv, exclude: ["tests/**","migrations/**"] }
builtin_idioms: [fastapi, aiokafka, faststream, confluent, temporal, aiohttp_client]
```
```bash
docker compose up -d
uv run codegraph index <ws>.yaml --no-embed
# затем прямой запрос к графу pilot_kyc: типы рёбер, kind каналов, claims в staging.db
```
Ожидаемо (baseline ДО фиксов): типы рёбер без PRODUCES/CONSUMES/CALLS_HTTP/INVOKES_ACTIVITY;
`claims` = только `temporal_start_mark`. После каждого фикса — сверять появление
соответствующего claim'а/ребра на этом же workspace.
