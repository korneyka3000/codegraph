# M7 — Settings-резолвер, HTTP-таргетинг, Temporal signals: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи — карта остаточных gap'ов re-run: `docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md` (далее OPEN; R1/R2/R3 с корневыми причинами и доказательствами) + `2026-07-23-pilot-rerun.md` (числа). Решения пользователя (сессия 2026-07-23): **код-first, helm-optional** — обе цепочки (HTTP-хост, Kafka-топик) сходятся в PydanticSettings-классах (дефолт в коде + env-переопределение из helm); producer в их стеке чаще всего — запись outbox-Event (sqlalchemy-модель с колонкой topic), топик — из Settings-дефолта либо helm-env.

**Goal:** Убить ложные CALLS_HTTP (R1: 3 ложных static-ребра, воронка wildcard-роута), дать producer-каналы литералами через Settings/enum-источники (R2), открыть Temporal signals как первоклассную async-границу (R3: 34 хендлера + 45 отправок невидимы), плюс verb_from-альтернативы (verb_unresolved=15) — всё доказуемо на расширенной realstack-фикстуре.

**Architecture:** Единый новый фундамент — **статический разбор class-body литералов** (Settings-классы pydantic: поле → (дефолт-литерал, env-имя через env_prefix/alias); enum-классы: члены → значения). Поверх него: ValueSpec-источники `settings:`/`enum:` (kafka producer/consumer топики резолвятся в литералы из кода); HTTP-таргетинг — строгая валидация формы пути (claim-`{param}` против static-сегмента роута = НЕ матч) + env→service map из опциональных `env_sources` (helm values, явные файлы в конфиге) + автопривязка клиента через `self.host`-присваивание → Settings-поле → env → сервис; unanchored кросс-сервисный матч теряет право на static/1.0. Signals переиспользуют PRODUCES/CONSUMES-семантику над новым `Channel(kind=temporal_signal, name=<signal name>)` — trace_process получает сигнальный хоп бесплатно (правила обхода уже знают PRODUCES/CONSUMES).

**Tech Stack:** без новых зависимостей (helm values — обычный YAML, парсим существующим yaml-loader'ом).

## Global Constraints

- Существующие фикстуры/golden/гейты M1–M6 не меняются (расширяется ТОЛЬКО fixtures/realstack + его golden/gate в T6). Все гейты зелёные в каждой задаче, трогающей их путь.
- Идиомы — данные: новые источники/поля в pydantic-DSL fail-closed; builtin-реестр не трогаем.
- Ложный матч хуже отсутствия: любое ослабление анкоринга/валидации пути — отклоняется; unanchored матч ≠ static/1.0.
- Каналы: новый kind `temporal_signal` — аддитивно в допустимые kinds; NEXT_SEGMENT-деривация через него — теми же правилами (producer→chan←consumer), via_channel_id обязателен.
- SCHEMA_VERSION не бампится, если не потребуется новая staged-таблица; class-attr данные — in-memory в пределах analyze-прохода сервиса (не staged) — если T1 докажет необходимость staging (кросс-файловость внутри сервиса), решить в T1 и записать (staged таблица = bump).
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

R1-механика (`linking/http_routes.py`, 163 строки): `_templates_match` (стр. 82) — посегментно, «равно ИЛИ хотя бы одна сторона `{param}`» (двунаправленный wildcard — это и есть воронка: роут из одних `{param}` матчит всё своей арности); `_allowed_services` (стр. 93) — narrowing только при base_url_env, None → все сервисы; `_candidates` → `link` (стр. 140). Claims несут `base_url_env` (сейчас null у пилотных клиентов). ValueSpec (config/models.py:28): const/arg/kwarg/env/attr, ровно-один-источник. ConstTable (parsing/consts.py) — module-level литералы per-file. FileFacts: AssignFact'ы (проверить скоупы: class-body/`self.X` в методах — читает T1). Идиом-матчинг: extractors/idiom_match.py (STATIC/RECEIVER/IMPORT_NAME tiers). Пилотные числа для калибровки: 44 http_call claims (3 ложных static + 41 unresolved); producer_unresolved_channel=2; enum KycTopicName ≈ 7 членов; 34 signal/query/update-декоратора + 45 `.signal(`-сайтов + 3 `get_external_workflow_handle`.

### Task 1: Class-attr harvesting — Settings/enum-индекс сервиса

**Files:**
- Create: `src/codegraph/parsing/class_attrs.py`
- Modify: `src/codegraph/parsing/facts.py` — ТОЛЬКО если facts не несут class-body присваиваний с литералами и spans (читать первым; расширять аддитивно по образцу base_exprs M6-T3)
- Modify: `src/codegraph/pipeline/analyze.py` (построение индекса в S5-проходе, до доменных экстракторов)
- Test: `tests/unit/test_class_attrs.py`, `tests/unit/test_parsing_facts.py` (при расширении)

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class ClassAttrIndex:
      """Service-wide: FQN класса (structural qualified_name) -> поля."""
      def settings_field(self, class_fqn_suffix: str, field: str) -> SettingsField | None: ...
      def enum_values(self, class_fqn_suffix: str) -> tuple[str, ...] | None: ...
      def field_by_name(self, field: str) -> SettingsField | None: ...  # уникальный по имени, для auto-join T3; None при коллизии

  @dataclass(frozen=True)
  class SettingsField:
      class_fqn: str; field: str
      default: str | None          # литерал-дефолт, если строковый
      env_name: str | None         # (env_prefix + field).upper() либо alias/validation_alias; None если prefix не найден
  ```
  Семантика: harvest per-file из facts (class-body: annotated/plain присваивания со строковыми литералами; `model_config = SettingsConfigDict(env_prefix="...")` → prefix; enum-детекция — база `Enum`/`StrEnum` текстуально в base_exprs [M6-T3 их уже парсит] И все члены — строковые литералы). Класс-суффикс-матчинг FQN — как base_class в M6 (последние сегменты). Индекс строится в analyze per-service (все файлы прохода, in-memory; кросс-файловость внутри сервиса — да, поэтому построение ДО экстракторного цикла вторым мини-проходом по facts_by_file — дёшево, facts уже в памяти).
- Consumes: `FileFacts` (+base_exprs из M6).

- [ ] **Step 1: Прочитать facts.py** — несут ли class-body присваивания (AssignFact scope). Если нет — падающий тест на аддитивное поле (class-attr присваивания с литералами: name, literal, annotation-текст опционально) → реализация.
- [ ] **Step 2: Падающие юниты class_attrs** — pydantic-Settings класс (`env_prefix="service_"`, поле `verification_requests_url: str = "http://localhost:8000"`) → SettingsField(default="http://localhost:8000", env_name="SERVICE_VERIFICATION_REQUESTS_URL"); поле без дефолта → default=None, env_name есть; alias/validation_alias побеждает prefix; enum-класс (`class KycTopicName(StrEnum): STEP_CHANGED = "kyc.camunda.step_changed"`) → enum_values кортеж значений; не-enum/не-строковые члены → None; field_by_name коллизия двух классов → None (честная неоднозначность).
- [ ] **Step 3: RED → реализация → GREEN; wiring в analyze** (индекс в FileContext или отдельным параметром экстракторам — решить по месту, задокументировать) **+ полный сьют + ruff; Commit** — `feat(m7): class-attr harvesting (pydantic settings fields + enum values) (open-gaps foundation)`

### Task 2: ValueSpec `settings:`/`enum:` + kafka producer/consumer литералы (OPEN R2)

**Files:**
- Modify: `src/codegraph/config/models.py` (ValueSpec: +`settings: str | None` [`"<ClassFQN>.<field>"`], +`enum_: str | None` с alias `enum` [FQN enum-класса]; ровно-один-источник расширить; enum-источник валиден ТОЛЬКО для topic каналов producer'а — fail-closed там, где не поддержан), `src/codegraph/parsing/consts.py` (resolve_value_spec: settings→default-литерал [env-имя → config_ref при отсутствии дефолта], enum → маркер множественности), `src/codegraph/extractors/kafka_ext.py` (producer: enum-источник → fan-out PRODUCES на канал КАЖДОГО значения enum [OPEN R2a: over-approximation, честно задокументированная]; settings-источник → единственный канал по дефолту; consumer topic: {settings:} допускается рядом с {attr:})
- Test: `tests/unit/test_config_models.py`, `tests/unit/test_consts.py`, `tests/unit/test_kafka_extractor.py`

**Interfaces:**
- Produces: `topic: {settings: "app.config.kafka.KafkaSettings.step_topic"}` → канал `chan:kafka_topic:<default>` (resolution static, confidence 1.0 — литерал из кода) либо, без дефолта, `Channel(unresolved, config_ref=<env_name>)`; `topic: {enum: "app.enums.KycTopicName"}` → PRODUCES-рёбра на каналы всех значений (props mechanism="enum_fanout", confidence с штрафом 0.8 — over-approximation); consumer `topic: {settings: ...}` — литеральный topic-канал + CONTAINS topic→event.
- Consumes: ClassAttrIndex (T1) через FileContext/analyze-wiring.
- Producer-точка outbox-Event: ПРОВЕРИТЬ, что существующий `call:`-матчинг берёт FQN конструктора класса (`call: "app.models.outbox.Event"`) — пин-тест (ctor call-site: callee_name=Event, идиом-tiers); kwarg-топик уже есть (M6-T4).

- [ ] **Step 1: Падающие DSL-юниты** (ровно-один-источник с новыми полями; enum вне producer-topic → ошибка).
- [ ] **Step 2: Падающие consts/kafka юниты** — сценарии из Interfaces + outbox-Event ctor пин + settings-поле без дефолта → unresolved с env-именем в config_ref.
- [ ] **Step 3: RED → реализация → GREEN; полный сьют + M2/M6-гейты + ruff; Commit** — `feat(m7): settings/enum topic sources — producer channels from code literals (open-gaps R2)`

### Task 3: HTTP-таргетинг: строгий матчинг + env→service (OPEN R1)

**Files:**
- Modify: `src/codegraph/linking/http_routes.py` (валидация формы: claim-`{param}`-сегмент против static-сегмента роута → НЕ матч [wildcard остаётся только у роут-стороны]; unanchored [нет target-сервиса] матч: требуется ЕДИНСТВЕННЫЙ кандидат И resolution понижается до heuristic/0.7 [не static]; ≥2 кандидатов unanchored → unresolved), `src/codegraph/config/models.py` (WorkspaceConfig: `env_sources: list[Path] = []` — YAML-файлы env-значений [helm values]; ServiceConfig.http уже несёт base_url_env), `src/codegraph/linking/workspace.py` или новый `src/codegraph/linking/env_map.py` (загрузка env_sources → env→value; hostname из URL-значения → первый DNS-лейбл →匹 имя сервиса воркспейса → env→service map), `src/codegraph/extractors/http_client_ext.py` (auto-anchor: для клиент-класса найти присваивание `self.host = <attr-chain>` [AssignFact], хвост цепочки → ClassAttrIndex.field_by_name → env_name → claim.base_url_env автозаполняется; явный `base_url: {env: ...}`/`{settings: ...}` в идиоме — приоритетнее авто)
- Test: `tests/unit/test_http_routes_linking.py`, `tests/unit/test_http_client_extractor.py`, `tests/unit/test_env_map.py`

**Interfaces:**
- Produces: порядок таргетинга claim'а: (1) base_url_env задан (явно или авто) И env→service map знает сервис → матчить ТОЛЬКО его роуты, static/1.0 при уникальном совпадении формы; (2) env задан, но map не знает → unresolved (канал с config_ref=env, доктор-строка); (3) env нет → unanchored-режим (см. выше). Воронка-сценарий OPEN §R1 (три пути → all-params роут) обязан давать 0 матчей формы.
- env_map: `SERVICE_VERIFICATION_REQUESTS_URL: http://verification-requests.kyc.svc.cluster.local:8000` → hostname `verification-requests.kyc...` → лейбл `verification-requests` → сервис (точное имя воркспейса; нет — fuzzy НЕ делаем, unresolved).
- report: счётчик `calls_http_false_form_rejected` не нужен — отклонённые формы просто не кандидаты; but `calls_http_unresolved` уже есть (S7) — остаётся.

- [ ] **Step 1: Падающие юниты матчинга** — воронка (пилотные три пути против `/{a}/{b}/{c}/parsed-data` → 0 кандидатов); корректная пара claim-static==route-static + route-`{param}`-wildcard → матч; claim-`{param}` vs route-static → отказ; unanchored уникальный → heuristic/0.7; unanchored 2 кандидата → unresolved; anchored (env→service) → только роуты сервиса, static/1.0.
- [ ] **Step 2: Падающие юниты env_map + auto-anchor** (helm-yaml фрагмент → map; hostname→service; self.host-присваивание → field_by_name → env; explicit base_url в идиоме приоритетнее).
- [ ] **Step 3: RED → реализация → GREEN; M2/M6-гейты** (fixtures рутов не менялись; M2 CALLS_HTTP static-матчи — anchored через base_url_env фикстур — должны остаться static/1.0: ПРОВЕРИТЬ, фикстуры workspace.yaml НЕСУТ base_url_env → якорь есть → ок) **+ полный сьют + ruff; Commit** — `fix(m7): strict http path-form matching + env→service anchoring (open-gaps R1, kills false edges)`

### Task 4: Temporal signals (OPEN R3)

**Files:**
- Modify: `src/codegraph/extractors/temporal_ext.py` (роли: `@workflow.signal|query|update`-методы → роль `TemporalSignalHandler` [одна роль на все три вида, prop signal_kind]; каналы+claims: signal-хендлер с `name=`-аргументом (или имя метода при отсутствии) → CONSUMES handler → `Channel(temporal_signal:<name>)`; сайты отправки: `<handle>.signal("<name>"| SignalRef, ...)` и `get_external_workflow_handle(...).signal(...)` → PRODUCES sender → тот же канал; имя из arg0-литерала/консты, нерезолвимое → unresolved-канал + счётчик `signal_name_unresolved` в report-wiring по M6-образцу), `src/codegraph/core/schema.py` (kind `temporal_signal` в допустимые Channel-kinds; ROLE_KINDS + `TemporalSignalHandler`), `src/codegraph/core/ids.py` (chan_temporal_signal)
- Test: `tests/unit/test_temporal_extractor.py`, `tests/unit/test_schema.py`

**Interfaces:**
- Produces: PRODUCES/CONSUMES переиспользованы (никаких новых edge-types; trace_process получает сигнальный хоп существующими правилами PRODUCES→chan←CONSUMES; NEXT_SEGMENT-деривация — бесплатно из segments.derive). `@workflow.query` — роль без канала (чтение, не async-граница); `@workflow.update` — как signal (канал), prop signal_kind="update".
- Consumes: consts (arg0-литерал имени), M6 frozenset-паттерн для receiver-агностичного `.signal`-матчинга: receiver ЛЮБОЙ (handle-переменные), но callee ровно `signal`; отсечение шума (не-temporal `.signal(`) — через требование строкового первого аргумента ЛИБО резолва имени + задокументировать FP-риск честно (props mechanism="temporal_signal").

- [ ] **Step 1: Падающие юниты** — `@workflow.signal(name="complete-survey")` метод → роль + CONSUMES → chan:temporal_signal:complete-survey; голый `@workflow.signal` без name → имя метода; `handle.signal("complete-survey", payload)` → PRODUCES тот же канал; `get_external_workflow_handle(wf_id).signal("x")` → PRODUCES; `.signal(variable)` нерезолвимый → unresolved+счётчик; `@workflow.query` → роль, БЕЗ канала; не-temporal `foo.signal(123)` (не-строковый arg0) → не матчится.
- [ ] **Step 2: RED → реализация → GREEN; M2/M6-гейты + полный сьют + ruff; Commit** — `feat(m7): temporal signals as first-class channels (open-gaps R3)`

### Task 5: verb_from альтернативы + polish

**Files:**
- Modify: `src/codegraph/config/models.py` + `src/codegraph/extractors/http_client_ext.py` (`request_ctor: "Request|ProxyRequest"` — `|`-альтернативы как в `call:`), докстринг-фикс «Cross-idiom dedup» (M7-беклог), `tests/unit/test_kafka_extractor.py` (+явный event_type_from={kwarg} producer-тест — M7-беклог)
- Test: соответствующие модули

- [ ] **Step 1: RED → GREEN по трём пунктам; полный сьют + ruff; Commit** — `chore(m7): request_ctor alternatives, docstring accuracy, kwarg event_type pin`

### Task 6: realstack-расширение + гейт + финал вехи

**Files:**
- Modify: `fixtures/realstack/` — добавить: Settings-класс с env_prefix и дефолтами (топик consumer'а + host-поле), фикстурный `env_values.yaml` (helm-values-образный: SERVICE_WORKER_URL с cluster-hostname worker'а) + `env_sources:` в workspace.yaml, outbox-Event producer (sqlalchemy-образный класс Event(topic=...) с topic из settings; producer-идиом с settings-источником), сигнальный лег (gateway workflow: @workflow.signal-хендлер; worker или активность шлёт handle.signal), self.host-присваивание в SDK-клиенте (auto-anchor путь). Воронка-негатив: worker получает all-params роут `/{a}/{b}/{c}/misc` — гейт assert'ит НОЛЬ ложных CALLS_HTTP в него.
- Modify: `fixtures/realstack/golden/edges.yaml` (+PRODUCES [settings-литерал], +CONSUMES/PRODUCES temporal_signal-пары, CALLS_HTTP теперь anchored static/1.0 через env→service), `golden/traces.yaml` (сигнальный хоп в трейсе), `tests/eval/test_m6_gate.py` → расширить ИЛИ новый `test_m7_gate.py` (решение: РАСШИРИТЬ M6-гейт нельзя — он пинит M6-состояние; НОВЫЙ test_m7_gate.py на расширенной фикстуре; M6-гейт останется зелёным, т.к. фикстура аддитивна — ПРОВЕРИТЬ и при конфликте старых golden — скопировать fixtures/realstack → realstack без изменений невозможно... РЕШЕНИЕ: M6-гейт и M7-гейт делят фикстуру; аддитивные файлы не должны ломать M6-golden [новые рёбра новых типов M6-golden не пинит — его eval по типам M6; CALLS_HTTP static-таргет может измениться M6-golden'ом — если ломается, M6-golden обновляется ЧЕСТНО с санкцией контроллера и записью [«верифицированное дополнение», прецедент M1b/M2], не порогом)
- Modify: `codegraph.example.yaml` (+packaged, drift-guard): settings/enum/env_sources примеры
- [ ] **Step 1: Фикстурные дополнения + golden** (вручную выведенные).
- [ ] **Step 2: Падающий M7-гейт → зелёный** (фиксить экстракторы, не golden; воронка-негатив обязателен).
- [ ] **Step 3: Все сьюты (default + все scip/emb гейты M1–M7) + ruff; Commit** — `feat(m7): realstack settings/env/outbox/signal legs + gate (funnel negative pinned)`
- [ ] **Step 4: Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение; леджер `=== M7 ЗАВЕРШЁН ===`; память; отчёт пользователю + re-run бриф №3 на рабочую машину** (чекпойнты: CALLS_HTTP корректные >0 и ложные =0 [3 пилотных пути → unresolved либо верные сервисы]; PRODUCES >0 [settings/enum]; temporal_signal-каналы ≈34 хендлеров; NEXT_SEGMENT сигнальные/HTTP пары; трасса через сигнальный хоп).

## Self-review плана

1. **Покрытие OPEN**: R1→T3 (+T1 фундамент, auto-anchor), R2→T2 (+T1), R3→T4, verb_unresolved→T5, всё доказуемо→T6. Решения пользователя (код-first/helm-optional; outbox-Event producer) — в архитектуре T1/T2/T3.
2. **Ложный-матч-хуже-отсутствия** воплощён: строгая форма, unanchored≠static, воронка-негатив в гейте.
3. **Типы согласованы**: ClassAttrIndex/SettingsField (T1) ← T2 (settings/enum) и T3 (field_by_name auto-anchor); temporal_signal kind (T4) ← T6 golden/trace.
4. Плейсхолдеров нет; неизвестности (AssignFact-скоупы, staged-vs-memory индекс, M6-golden конфликт в T6) оформлены как решения-по-месту с фиксацией в отчётах задач.
