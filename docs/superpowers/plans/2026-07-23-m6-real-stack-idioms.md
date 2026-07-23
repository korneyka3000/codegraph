# M6 — покрытие идиом реального стека: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox (`- [ ]`). Спека вехи — пилотная карта gap'ов: `docs/superpowers/reports/2026-07-23-pilot-real-services-gaps.md` (далее «GAPS»; каждая задача обязана перечитать свой раздел). Приоритеты — GAPS §7.

**Goal:** Закрыть все 5 gap'ов пилота на реальных KYC-сервисах: async-леги (INVOKES_ACTIVITY, start_child_workflow, CALLS_HTTP через декоратор-SDK, CONSUMES через подкласс, PRODUCES через обёртку с kwarg-топиком) должны давать рёбра на конвенциях этого стека — доказуемо на синтетической «real-stack» фикстуре, без кода пользователя.

**Architecture:** Два тривиальных расширения матчеров (temporal_ext), одно среднее расширение DSL+экстрактора (http_client_ext: маршрут из декоратора, verb из Method-enum, вызов через driver-индирекцию), одно крупное (kafka: consumer-kind `base_class` с event_type из generic-параметра; producer-обёртка + kwarg-источник топика). Кросс-сервисная идентичность каналов этого стека — **event_type** (класс события), не литеральный топик (GAPS §6 вывод) — архитектура M0 это уже умеет (`chan:event_type:*`, CONTAINS topic→event). Всё новое — идиомы-как-данные: builtin-реестр не трогаем, реальные конвенции описываются YAML-ом пользователя; новая синтетическая фикстура `realstack` (4-й воркспейс) зеркалит конвенции стека и получает собственный гейт.

**Tech Stack:** без новых зависимостей. Generic-параметр `BaseConsumer[Event]` — tree-sitter subscript в bases класса; подкласс-детекция — scip-резолв базового класса.

## Global Constraints

- Существующие фикстуры (3 сервиса), golden-наборы и гейты M1–M5 НЕ изменяются ни на байт (новые конвенции — только в новой фикстуре `fixtures/realstack/`). Все гейты зелёные в каждой задаче, трогающей их путь.
- Идиомы — данные: новые поля/kinds в pydantic-DSL (`config/models.py`) с fail-closed валидацией; builtin_idioms.py расширять ТОЛЬКО temporal-матчерами (Gap #2/#3 — структурный экстрактор, там его паттерны и живут); HTTP/kafka-конвенции стека — примером в `codegraph.example.yaml`, не builtin.
- Динамический топик без резолвимой идентичности → честный `Channel(unresolved=true)` + doctor-report; НЕ выдумывать идентичность (GAPS §6: «честно — резолвимой идентичности канала на call-site нет»).
- Каждый gap закрывается синтетическим тестом (unit inline-source + realstack-интеграция); regression на коде пользователя невозможен по определению.
- rtk-хук искажает pytest-вывод: junit-xml всегда; `rtk proxy` для сырых команд. Системные reminders харнеса — штатные.
- SCHEMA_VERSION не меняется (staging-слои не трогаем).

---

## Контекст для исполнителей

Экстракторы: `extractors/temporal_ext.py` (строгие сравнения callee — GAPS §3/§4: строки 143, 167), `extractors/http_client_ext.py` (`_VERBS` стр. 111, `_is_candidate_call` стр. 132, `_resolve_path` стр. 161 — GAPS §2), `extractors/kafka_ext.py` + DSL `config/models.py` (ConsumerIdiom kinds {call, decorator, dispatch_dict} — GAPS §5). Claims-пайплайн: экстракторы эмитят claims → S7 `linking/http_routes.py`/`linking/workspace.py` строят рёбра/каналы. FileContext несёт facts (call-sites с receiver_text/callee_name/args), consts (ConstTable: литералы/attr/env), def/ref-lookups (scip). Идиом-матчинг: `extractors/idiom_match.py` (tiered STATIC/RECEIVER/IMPORT_NAME).

Числа пилота для калибровки ожиданий (GAPS §1): 3 сервиса, 325 файлов, 47.3s cold; camunda: 43 активности / 80 `execute_activity_method` вызовов / 11 `*Client(BaseClient)` с `@path_template` (42× `driver.fetch_content`, 13× `driver.fetch`, verbs `Method.{GET×20,POST×21,PATCH×12,DELETE×1}`) / `start_child_workflow` ×3.

### Task 1: Temporal-матчеры — activity-инвокации и child-workflow (GAPS §3, §4)

**Files:**
- Modify: `src/codegraph/extractors/temporal_ext.py` (два множества callee-имён вместо строгих сравнений)
- Test: `tests/unit/test_temporal_extractor.py`

**Interfaces:**
- Produces: `_ACTIVITY_INVOKE_CALLEES = frozenset({"execute_activity", "execute_activity_method", "execute_local_activity", "execute_local_activity_method", "start_activity", "start_local_activity"})`; `_START_WORKFLOW_CALLEES = frozenset({"start_workflow", "start_child_workflow", "execute_child_workflow"})`. Оба матчера: `callee_name in <set> and receiver_text == "workflow"` (activity-инвокации; для start-workflow — прежний receiver-контракт, ПРОЧИТАТЬ его на стр. ~167: там client/handle-receiver — сохранить как есть, расширить только имена). arg0-резолюция не меняется (GAPS: «резолвится одинаково»).
- INVOKES_ACTIVITY-ребро и temporal_start_mark-claim — прежние формы; только матчинг шире.

- [ ] **Step 1: Падающие юниты** — inline-source: (а) `await workflow.execute_activity_method(Act.m, x)` → INVOKES_ACTIVITY на резолв arg0; (б) `execute_local_activity`/`start_activity` — то же; (в) `workflow.execute_activity_method` с receiver НЕ `workflow` → не матчится; (г) `start_child_workflow(ChildWF.run, …)` и `execute_child_workflow(...)` → temporal_start_mark-claim; (д) прежние формы (`execute_activity`, `start_workflow`) — байт-в-байт прежнее поведение.
- [ ] **Step 2: RED → реализация → GREEN** (junit).
- [ ] **Step 3: Полный сьют + M2-гейт** (temporal-фикстуры не менялись — гейт обязан быть зелёным без сдвигов) **+ ruff; Commit** — `fix(m6): temporal matchers accept *_activity_method / child-workflow variants (pilot gaps 2-3)`

### Task 2: HTTP декоратор-SDK (GAPS §2)

**Files:**
- Modify: `src/codegraph/config/models.py` (HttpClientIdiom: новые опциональные поля), `src/codegraph/extractors/http_client_ext.py` (второй режим кандидата)
- Test: `tests/unit/test_http_client_extractor.py`, `tests/unit/test_config_models.py`

**Interfaces:**
- Produces — расширение `HttpClientIdiom` (все поля опциональны; отсутствуют → прежний verb-режим, ноль изменений поведения):
  ```yaml
  http_clients:
    - name: decorator-sdk
      file_glob: "**/clients/*.py"
      class_glob: "*Client"
      route_from: { decorator: "path_template", arg: 0 }     # маршрут — из декоратора метода
      call: "driver.fetch_content|driver.fetch"               # driver-индирекция: какие вызовы считать HTTP
      verb_from: { request_ctor: "Request", enum: "Method" }  # verb — из Request(Method.X, …) внутри метода
  ```
- Семантика экстрактора (режим decorator-SDK, активен когда `route_from` задан): (1) метод класса, матчащего class_glob/file_glob, с декоратором `route_from.decorator` и строковым arg → path_template (существующий `_resolve_path`-механизм переиспользуется для шаблона из декоратора: `{param}`-плейсхолдеры уже поддержаны); (2) внутри тела метода — call-site, чей receiver+callee матчит один из `call`-альтернатив (`|`-разделитель; receiver-хвост сравнивается по attribute-цепочке, например `self.driver.fetch_content` матчит `driver.fetch_content`); (3) verb: в теле метода найти вызов конструктора `verb_from.request_ctor` и взять attribute-выражение `Method.X` его arg0 → verb `X` (upper). Не нашли verb → claim с `verb=null` → S7-матчинг по одному только пути с conf-штрафом (прочитать как http_routes.link обрабатывает метод — если метод обязателен, эмитить unresolved-Channel + doctor-строку; выбрать по факту чтения и зафиксировать в отчёте задачи).
- Claim-форма (`HttpCallClaim`) не меняется — только источники значений.

- [ ] **Step 1: Падающие юниты DSL** (валидация: route_from без call → ошибка конфига fail-closed; `|`-альтернативы парсятся).
- [ ] **Step 2: Падающие юниты экстрактора** — синтетический клиент из GAPS §2 (класс `DMoutClient(BaseClient)`, `@path_template("/v1/dmout/user_hv/uuid/{customer_uid}")`, `Request(Method.GET, …)`, `await self.driver.fetch_content(request)`) → ровно один http_call-claim: path_template `/v1/dmout/user_hv/uuid/{customer_uid}`, verb GET; метод без декоратора → нет claim'а; прежний verb-режим (существующие тесты) — нетронут.
- [ ] **Step 3: RED → реализация → GREEN; полный сьют + ruff; Commit** — `feat(m6): decorator-route HTTP client idiom (route_from/call/verb_from) (pilot gap 1)`

### Task 3: Kafka consumer-kind `base_class` (GAPS §5)

**Files:**
- Modify: `src/codegraph/config/models.py` (ConsumerIdiom: kind `base_class` + поля), `src/codegraph/extractors/kafka_ext.py` (новая ветка), `src/codegraph/parsing/facts.py` — ТОЛЬКО если facts не несут generic-параметры bases (прочитать: ClassDef bases сейчас парсятся? если нет — добавить `base_exprs: tuple[str, ...]` в def-факты, аддитивно)
- Test: `tests/unit/test_kafka_extractor.py`, `tests/unit/test_config_models.py`, `tests/unit/test_facts.py` (если facts расширяются)

**Interfaces:**
- Produces — новый вид consumer-идиомы:
  ```yaml
  consumers:
    - name: base-consumer-subclass
      kind: base_class
      base_class: "kyc_base_consumer.base.BaseConsumer"   # FQN базового класса
      handler_method: "process_event"
      event_type_from: { generic_arg: 0 }                  # BaseConsumer[OCRDataEvent] → OCRDataEvent
      topic: { attr: "self.config.topic" }                 # опционально; attr → конфиг-ссылка, не литерал
  ```
- Семантика: класс, чьи bases содержат subscript-выражение `Base[...]`, где `Base` резолвится (scip ref-lookup на имени базы; fallback — текстовый суффикс-матч FQN с conf-штрафом идиом-tiers, как в idiom_match) в `base_class` → ролью MessageConsumer помечается **метод `handler_method`** этого класса (не ctor/setup — GAPS §5: «сегментом CONSUMES делать handler_method»); CONSUMES-ребро handler_method → `Channel(event_type=<generic_arg>)`; generic-параметр — текст subscript-аргумента (класс события; если это attribute-цепочка — последний идентификатор). `topic.attr` задан → дополнительно `Channel(kafka_topic, unresolved=true, config_ref="self.config.topic")` + CONTAINS topic→event (unresolved-канал уже поддержан — прочитать make_channel_node/линковку).
- Consumes: scip ref_symbol_lookup из FileContext (резолв имени базы в FQN).

- [ ] **Step 1: facts-проверка** — читает ли `build_file_facts` bases классов; при отсутствии — падающий юнит на `base_exprs` (класс `C(BaseConsumer[FooEvent])` → base_exprs содержит `BaseConsumer[FooEvent]`), реализация аддитивно.
- [ ] **Step 2: Падающие юниты DSL** (kind=base_class требует base_class+handler_method+event_type_from; прочие kinds не затронуты).
- [ ] **Step 3: Падающие юниты экстрактора** — синтетика GAPS §5: `class OCRDataConsumer(BaseConsumer[OCRDataEvent])` с `process_event` → роль consumer на process_event, CONSUMES → `chan:event_type:OCRDataEvent`, dispatch=event_type; подкласс ДРУГОЙ базы → ничего; класс без generic-параметра при `generic_arg`-источнике → claim не эмитится + счётчик в stats (честный miss, не крэш).
- [ ] **Step 4: RED → реализация → GREEN; полный сьют + M2-гейт + ruff; Commit** — `feat(m6): base_class consumer idiom with event_type from generic arg (pilot gap 4)`

### Task 4: Kafka producer-обёртка + kwarg-топик (GAPS §6)

**Files:**
- Modify: `src/codegraph/config/models.py` (`ValueSpec`: источник `kwarg`; уже есть? прочитать — в исходном дизайне упоминался `{kwarg: event_type}`; если есть — только kafka_ext-поддержка), `src/codegraph/extractors/kafka_ext.py` (kwarg-извлечение из CallFact; producer-идиом на пользовательской обёртке уже выражается `call:` — проверить и допокрыть)
- Test: `tests/unit/test_kafka_extractor.py`, `tests/unit/test_consts.py` (если resolve_arg расширяется)

**Interfaces:**
- Produces: `topic: {kwarg: "topic"}` / `event_type_from: {kwarg: "..."}` работают в producer-идиомах (CallFact args — прочитать, несут ли kwargs имена; если нет — аддитивно расширить ArgFact `keyword: str | None` в facts + тест); идиом на обёртке: `call: "app.services.producer.KYCEventPublisher.publish", channel: {kind: kafka_topic, topic: {arg: 1}}` — матчинг по существующим tiers. Динамическое значение топика (attr-выражение `payload.topic_name`) → `Channel(unresolved=true)` + doctor (Global Constraint; уже так для нерезолвимых — сверить и запинить тестом). `event_type: {const: "..."}`-источник на обёртке — уже поддержан ValueSpec'ом (const) — пример в codegraph.example.yaml для стека: обёртка + const-event_type per-производитель как честный ручной вариант, когда тип события фиксирован точкой вызова.
- [ ] **Step 1: Падающие юниты** — `send_and_wait(topic=X)` kwarg-форма с builtin-идиомом (kwarg-источник в builtin? НЕТ — builtin не трогаем [constraint]; юнит на ПОЛЬЗОВАТЕЛЬСКОМ идиоме с kwarg-источником); обёртка `publish(body, topic_name)` с `{arg: 1}` при литеральном топике → PRODUCES+канал; при динамическом (`payload.topic_name`) → unresolved-Channel + счётчик; kwarg отсутствует в вызове → miss-счётчик, не крэш.
- [ ] **Step 2: RED → реализация → GREEN; полный сьют + ruff; Commit** — `feat(m6): kwarg value source + producer wrapper coverage, honest unresolved dynamic topics (pilot gap 5)`

### Task 5: Фикстура `realstack` + гейт вехи

**Files:**
- Create: `fixtures/realstack/` — 2 синтетических мини-сервиса, зеркалящих конвенции стека БЕЗ кода пользователя: `gateway` (декоратор-SDK клиент [Gap 1] + workflow с `execute_activity_method` и `start_child_workflow` [Gaps 2-3] + activity-паблишер через `KYCEventPublisher.publish`-обёртку [Gap 5]) и `worker` (подкласс `BaseConsumer[DocSubmittedEvent]` c `process_event` [Gap 4] + FastAPI-роут — цель SDK). Плюс `fixtures/realstack/workspace.yaml` с идиомами из Task 2-4 примеров и локальной мини-«shared lib» `libs/kyc_base_consumer` (path-зависимость, чтобы scip резолвил базу).
- Create: `fixtures/realstack/golden/edges.yaml` (полный эталон по всем 5 легам), `tests/eval/test_m6_gate.py` (маркеры scip+falkordb)
- Modify: `codegraph.example.yaml` + упаковочная копия (задокументированные примеры новых идиом; drift-guard!)
- Test: гейт

**Interfaces:**
- Гейт: индекс realstack-воркспейса (реальный scip; venv мини-сервисам не нужен — first-party достаточно, деградации быть не должно) → P=R=1.0 по golden на типах INVOKES_ACTIVITY / temporal_start_mark-CALLS / CALLS_HTTP / CONSUMES / PRODUCES; CONTAINS topic→event присутствует; `codegraph trace "gateway:POST /submit"` (или какой роут будет) проходит все 4 async-хопа (route → workflow → activity → publish → event-канал → process_event; SDK-вызов → CALLS_HTTP → worker-роут) — посегментное сравнение с `fixtures/realstack/golden/traces.yaml`.
- Главные фикстурные требования: домены обеих «сервисов» независимы (кросс-рёбра только через Channel), event_type-идентичность связывает producer↔consumer БЕЗ общего литерального топика (доказательство вывода GAPS §6).

- [ ] **Step 1: Фикстура + golden** (писать код фикстуры ПО GAPS-сниппетам; каждое golden-ребро выводимо вручную).
- [ ] **Step 2: Падающий гейт → зелёный** (если какой-то лег не собирается — фиксить экстракторы Task 1-4, НЕ golden).
- [ ] **Step 3: Полные сьюты (default + все scip/emb гейты M1-M6) + ruff; Commit** — `feat(m6): realstack fixture + gate (all five pilot legs traced end-to-end)`

### Task 6: Финал вехи

- [ ] **Step 1:** Полные сьюты junit; сверка каждого GAPS-раздела с закрывающим тестом.
- [ ] **Step 2:** Финальное whole-milestone ревью (fable), фикс-вейв, подтверждение.
- [ ] **Step 3:** Леджер `=== M6 ЗАВЕРШЁН ===`; память; отчёт пользователю + инструкция для рабочей машины: `git pull` → повторный `index` того же workspace → сверка «Ожидаемо ДО фиксов» из GAPS §8 → появление PRODUCES/CONSUMES/CALLS_HTTP/INVOKES_ACTIVITY/NEXT_SEGMENT и живой trace бизнес-процесса.

## Self-review плана

1. **Покрытие GAPS**: §2→T2, §3→T1, §4→T1, §5→T3, §6→T4, §7-приоритеты→порядок T1(минимальный/максимальный эффект)→T2→T3→T4, §8-воспроизведение→T6-инструкция. E2E-доказательство без кода юзера→T5.
2. **Констрейнты**: старые фикстуры/golden нетронуты (новая realstack-фикстура); builtin — только temporal-матчеры; unresolved-честность (T3 topic-attr, T4 dynamic) закреплена тестами.
3. **Типы согласованы**: route_from/verb_from/call (T2) ↔ realstack-yaml (T5); kind=base_class поля (T3) ↔ T5; kwarg-источник (T4) ↔ ValueSpec.
4. **Плейсхолдеров нет**; неизвестности кода (facts bases, ArgFact kwargs, ValueSpec kwarg, http_routes verb-опциональность) оформлены как «прочитать-и-решить» шаги внутри задач с фиксацией решения в отчёте задачи.
