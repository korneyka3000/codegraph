# M2: Domain Extractors, Channels, Linking, trace_process — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Веха M2 мастер-плана (docs/superpowers/specs/2026-07-12-codegraph-design.md). База: M1 завершён на fc97951 (гейт P=R=1.0). Тесты в брифах — контракт; реализация — build-to-interface по паттернам M1.

**Goal:** Кросс-сервисный граф бизнес-процессов: экстракторы fastapi/kafka(+outbox)/temporal/http-client поверх Idiom-DSL из M0, Channel-узлы (kafka_topic/event_type/http_route), linking (унификация каналов, матчинг HTTP-клиентов на роуты, деривация NEXT_SEGMENT, BusinessProcess-якоря), `trace_process`/`find_paths`/`list_processes`/`find_entrypoint` в MCP + CLI `trace`. Гейт M2: precision/recall = 1.0 по HANDLES/DEPENDS_ON/PRODUCES/CONSUMES/INVOKES_ACTIVITY/CALLS_HTTP на фикстурах; трейс `orders-api:POST /orders` совпадает с golden/traces.yaml посегментно; тест «идиома-как-конфиг» зелёный.

**Architecture:** Экстракторы (S5) эмитят роли на узлах + claims в staging (новая таблица); S6 join как в M1; **S7 link_workspace(cfg, staging)** — НОВАЯ стадия ПОСЛЕ всех analyze_service: унифицирует Channel-узлы, матчит HttpCallClaims на таблицу роутов, дериватит NEXT_SEGMENT/PART_OF_PROCESS, помечает mechanism-рёбра. Трассировка — итеративный BFS в Python (query/traverse.py) с таблицей правил переходов.

**Tech Stack:** без новых зависимостей (fulltext — встроенный FalkorDB, DDL уже паттернизирован).

## Global Constraints

- Коммиты `feat(m2)/test(m2)/fix(m2)/chore(m2)`; после каждой задачи; uv.lock вместе с pyproject.
- **Watch-items финального ревью M1 (обязательны):**
  1. `Staging.upsert_edges`-инвариант: NEXT_SEGMENT — единственное разрешённое кросс-сервисное sym→sym ребро, ТОЛЬКО при наличии props.via_channel_id (иначе InvariantError). Рёбра с channel-концами (`chan:`) и process-концами (`proc:`) — кросс-сервисность не проверяется.
  2. `begin_service` НЕ должен стирать выход S7: NULL-src-удаление заменить на удаление ТОЛЬКО рёбер с extractor из S5/S6-набора; S7-рёбра/каналы живут в отдельном «workspace»-скоупе — `clear_workspace_layer()` вызывается перед link_workspace. Контракт: S7 всегда идёт после ВСЕХ analyze_service (полный прогон); инкрементальность — M4.
  3. `FalkorStore.neighbors` hop payload получает поле `direction: "out"|"in"` (в both-режиме различимо); GraphQuery/схемы/тулы прокидывают.
  4. GraphQuery: невалидный direction → error dict (не KeyError).
  5. NODE_KINDS/EDGE_TYPES/роли расширяются fail-closed: списки в schema.py — единственный источник; batch/loader валидируют по ним.
- Роли — доп. label'ы поверх kind (`:Sym:Function:RouteHandler`) — multi-label доказан doctor-probe M0.
- Channel id: `chan:kafka_topic:<name>` / `chan:event_type:<name>` / `chan:http:<owner|?>:<METHOD> <template>`; BusinessProcess: `proc:<slug>`.
- NEXT_SEGMENT/PART_OF_PROCESS — ТОЛЬКО derived в S7 (`derived: true`, `via_channel_id` обязателен у NEXT_SEGMENT); confidence = произведение конфиденсов пары.
- На всех новых рёбрах resolution/confidence; env-косвенность → `config_ref` prop.
- Идиомы — данные из M0-моделей; матчинг — ярусный (см. T3), builtin-идиомы могут потребовать корректировки паттернов под реальные scip-имена — данные, не код.
- Весь Cypher — stores/falkordb/ (fulltext DDL/запрос — туда же). Многошаговость — Python BFS.
- rtk-хук: pytest через `--junit-xml` или редирект. Гейты НЕ ослаблять.
- tree-sitter: без Query API.

---

### Task 1: Schema/staging/store расширения + watch-item фиксы

**Files:**
- Modify: `src/codegraph/core/schema.py`, `src/codegraph/core/ids.py`, `src/codegraph/stores/staging.py`, `src/codegraph/pipeline/load.py`, `src/codegraph/stores/falkordb/batch.py`, `src/codegraph/stores/falkordb/store.py`, `src/codegraph/query/api.py`, `src/codegraph/mcp/schemas.py`, `src/codegraph/mcp/server.py`
- Test: дополнения в test_core_schema/test_staging/test_falkordb_batch/test_query_api/test_pipeline_load

**Interfaces:**
- schema: `NODE_KINDS += {"Channel", "BusinessProcess"}`; `ROLE_KINDS = frozenset({"RouteHandler","MessageConsumer","MessageProducer","TemporalWorkflow","TemporalActivity"})`; `EDGE_TYPES += {"HANDLES","DEPENDS_ON","PRODUCES","CONSUMES","INVOKES_ACTIVITY","CALLS_HTTP","NEXT_SEGMENT","PART_OF_PROCESS"}`; `NodeRec.roles: tuple[str, ...] = ()` (валидны только из ROLE_KINDS — validate в staging.upsert_nodes); `make_channel_node(kind, name, **props) -> NodeRec` (id по ids-хелперам, service=""), `make_process_node(slug, name, entrypoint_id, source) -> NodeRec`.
- ids: `chan_kafka(name)`, `chan_event(name)`, `chan_http(owner: str | None, method, template)` (owner None → "?"), `proc_id(slug)`, `slugify(name)`.
- staging: labels-колонка = json [kind, *roles]; `upsert_nodes` валидирует roles ⊆ ROLE_KINDS; инвариант upsert_edges по Global Constraint 1 (`chan:`/`proc:`/`svc:` концы — без кросс-проверки; sym→sym кросс-сервис разрешён ТОЛЬКО type==NEXT_SEGMENT c props["via_channel_id"]); `begin_service` больше НЕ трогает NULL-src глобально; НОВОЕ: `claims` таблица (`service, relpath, kind, payload_json, PRIMARY KEY(service, relpath, kind, payload_json)`), `add_claims(service, kind, rows: list[dict])`, `claims_for(kind, service=None) -> list[dict]` (payload + service/relpath), `clear_workspace_layer()` — удаляет узлы kind∈{Channel,BusinessProcess} и рёбра extractor=="linking"; `update_edge_props(src, dst, type, props_merge: dict)`.
- load: `_labels_for_kind` → кодовые ("Sym", kind, *roles); Channel → ("Channel",); BusinessProcess → ("BusinessProcess",). batch label-allowlist расширить (NODE_KINDS∪ROLE_KINDS∪{"Sym"}...), edge-allowlist = EDGE_TYPES.
- store.neighbors: Hop → `(edge_type, edge_props, node_dict, direction)` где direction ∈ {"out","in"} (4-кортеж; ВСЕ вызыватели обновить: query/api.py, тесты). GraphQuery: невалидный direction-аргумент → `{"error": "invalid direction ..."}` до обращения к store.
- Существующие тесты, зависящие от 3-кортежа Hop/старого labels — обновить (это санкционировано).

- [ ] Падающие тесты по каждому пункту (инвариант NEXT_SEGMENT ±via_channel_id; chan-концы свободны; roles валидация; claims round-trip; clear_workspace_layer выборочность; update_edge_props merge; hop direction в live-тесте store; GraphQuery direction error) → RED → реализация → GREEN; full suite (ожидай правок существующих hop/labels тестов) + falkordb-маркер + ruff.
- [ ] Commit: `feat(m2): schema/staging/store extensions (roles, channels, claims, NEXT_SEGMENT invariant, hop direction)`

---

### Task 2: facts-расширения + consts.py

**Files:**
- Modify: `src/codegraph/parsing/facts.py`
- Create: `src/codegraph/parsing/consts.py`
- Test: дополнения test_parsing_facts + новый tests/unit/test_parsing_consts.py

**Interfaces:**
- facts: `ArgFact(index: int | None, keyword: str | None, value_kind: Literal["string","fstring","name","attr","dict","other"], text: str, string_value: str | None, name_start_byte: int | None, name_end_byte: int | None, dict_items: list[tuple[ArgFact, ArgFact]] | None)` — string_value для string (без кавычек); для name/attr — спаны идентификатора (attr: последний сегмент) для scip-lookup; dict — пары ключ/значение (каждый ArgFact-ом, index=None).
- `CallFact.args: list[ArgFact]` (для существующих тестов — поле добавляется, старые ассерты живут); `CallFact.receiver_text: str | None` (текст выражения перед последней точкой у attribute-вызова, напр. "producer", "self._db"); `AssignFact(target: str, callee_name: str | None, call_args: list[ArgFact] | None, start_line)` — только простые `name = Callee(...)` / `name = await Callee(...)`; `FileFacts.assigns: list[AssignFact]`; `DefFact.params: list[ParamFact(name, annotation_text: str | None, default_text: str | None, default_start_byte: int | None, default_end_byte: int | None)]`.
- consts: `ConstTable.build(facts, source) -> ConstTable` (module-level `NAME = "literal"`); `resolve_arg(arg: ArgFact, consts: ConstTable) -> Resolved` где `Resolved(kind: Literal["value","template","config_ref","unresolved"], value: str | None, config_ref: str | None)`: string → value; name в consts → value; fstring → template (интерполяции `{expr}` → `{param}`-имена: идентификатор → его имя, `self._x`/атрибуты → имя последнего сегмента; `{self._base_url}` в НАЧАЛЕ шаблона → маркер base_url: `Resolved(kind="template", value="<base>/documents/{doc_id}")` — префикс `<base>`); `os.environ["X"]`/`os.getenv("X")`/`settings.X` в тексте → config_ref("X"); иначе unresolved.
- `resolve_value_spec(spec: ValueSpec, call: CallFact, consts) -> Resolved` — const → value; arg/kwarg → найти ArgFact → resolve_arg; env → config_ref(spec.env); attr — для http-идиом (T6), здесь заглушка unresolved.

- [ ] Verbatim-тесты (написать при исполнении по контракту, минимум): args позиционные/kwargs/string_value; f-string `f"{self._base_url}/documents/{doc_id}"` → template `<base>/documents/{doc_id}`; dict-литерал в args (ключи-строки, значения-имена со спанами); AssignFact для `_producer = AIOKafkaProducer(...)` и `producer = await get_producer()`; ParamFact для `db: Session = Depends(get_db)` (annotation_text "Session", default_text "Depends(get_db)", спаны); consts: `TOPIC = "orders.events"` резолв name-арга; settings.X → config_ref. Smoke на всех 29 фикстурных файлах.
- [ ] RED → реализация → GREEN; full + ruff. Commit: `feat(m2): call args/assign/param facts + partial constant resolution`

---

### Task 3: Ярусный idiom-матчер

**Files:**
- Create: `src/codegraph/extractors/idiom_match.py`
- Test: `tests/unit/test_idiom_match.py`

**Interfaces:**
- `MatchTier`: `STATIC` (resolution "static", conf 1.0) | `RECEIVER` ("heuristic", 0.8) | `IMPORT_NAME` ("heuristic", 0.6).
- `CallMatch(call: CallFact, tier, resolution: str, confidence: float)`.
- `match_calls(pattern: str, facts: FileFacts, qualified_of: Callable[[CallFact], str | None]) -> list[CallMatch]` — ярусы:
  1. STATIC: `qualified_of(call)` (scip-lookup по callee-спану → display_qualified) и `fnmatchcase(qualified, pattern)` ИЛИ `fnmatchcase(qualified, "*." + pattern)`.
  2. RECEIVER: attribute-вызов, `receiver_text` — имя, найденное в facts.assigns с `callee_name == последний-сегмент-класса-из-паттерна` (паттерн "aiokafka.AIOKafkaProducer.send": класс "AIOKafkaProducer", метод "send"), callee_name вызова == метод.
  3. IMPORT_NAME: файл импортирует модуль-префикс паттерна (первый сегмент, напр. "aiokafka") ИЛИ from-import имени класса; callee_name == последний сегмент паттерна; для ctor-паттернов (последний сегмент — CamelCase класс, напр. "aiokafka.AIOKafkaConsumer") callee_name == класс.
  Первый сработавший ярус побеждает; матчи дедуплицируются по call-спану.
- `match_decorators(pattern: str, defs: list[DefFact]) -> list[tuple[DefFact, str]]` — decorator-текст: `fnmatchcase(dec, pattern + "(*")` или префикс `pattern + "("`/равенство; вернуть (def, полный текст декоратора).
- Паттерны builtin-идиом (config/builtin_idioms.py) СКОРРЕКТИРОВАТЬ по реальности при исполнении T5 (данные): например aiokafka-send может стать "*.AIOKafkaProducer.send" — задокументировать выбранные паттерны.

- [ ] Тесты: STATIC на первопартийном qualified (фикстурный outbox: qualified_of-стаб возвращает "app.db.outbox.OutboxRepository.add_event", pattern точный → tier STATIC); RECEIVER: `producer.send` c AssignFact producer=AIOKafkaProducer → 0.8; IMPORT_NAME: `AIOKafkaConsumer("t")` при from-import → 0.6; `producer = await get_producer()` + send → IMPORT_NAME (не RECEIVER); отрицательные (нет импорта/имя не то → пусто); дедуп ярусов.
- [ ] RED → реализация → GREEN; full + ruff. Commit: `feat(m2): tiered idiom matcher (static/receiver/import-name evidence)`

---

### Task 4: extractors/fastapi.py

**Files:**
- Create: `src/codegraph/extractors/fastapi_ext.py` (не fastapi — коллизия имён)
- Modify: `src/codegraph/pipeline/analyze.py` (вызов доменных экстракторов в S5 при активной идиоме "fastapi")
- Test: `tests/unit/test_fastapi_extractor.py`

**Interfaces:**
- `extract_fastapi(ctx: FileContext, node_ids: dict[int, str]) -> FastapiResult(roles: dict[node_id, set[str]], channels: list[NodeRec], edges: list[EdgeRec], claims: list[dict])` — node_ids: def-index → resolved node id (из python_core прогона; analyze прокидывает).
- Роут-детект: DefFact.decorators с текстом `<recv>.<method>("path"...)` где method ∈ {get,post,put,delete,patch,head,options}; `<recv>` — имя переменной с AssignFact callee_name ∈ {APIRouter, FastAPI} (иначе skip). Префикс: из AssignFact APIRouter kwargs `prefix="..."` (ArgFact keyword). template = prefix + path (пустой path → prefix). Канал: `make_channel_node("http_route", ...)` id = `ids.chan_http(owner=ctx.service, method, template)`, props {http_method, path_template, owner_service}. Рёбра: HANDLES (src=chan id, dst=handler id, extractor "fastapi", static, 1.0, evidence). Роль RouteHandler на handler-узле; props узла += {http_method, path_template} (через claims "node_props"? — НЕТ: roles/props обновляются повторным upsert_nodes в analyze — extractor возвращает roles-map и props-patch, analyze мёржит в NodeRec перед staging.upsert_nodes; порядок S5: python_core.extract → доменные экстракторы → merge → upsert).
- DEPENDS_ON: для каждого ParamFact с default_text, содержащим `Depends(`: найти идентификатор внутри скобок (span в default-диапазоне; ts-мини-парс или regex по default_text + вычисление спана имени от default_start_byte) → scip-lookup ref на этом спане (`ctx.resolve_span`) → target id; edge (handler → target, "DEPENDS_ON", static, 1.0, props {"via": "depends"}); `Annotated[X, Depends(y)]` — annotation_text содержит Depends → тот же поиск в annotation-диапазоне, via "annotated". Нерезолв → stats counter, ребра нет.
- Claims: route-таблица S7 восстановима из staged Channel(http_route)+HANDLES — отдельный route-claim НЕ нужен.

- [ ] Тесты на фикстурных orders_api/document_management файлах (реальные facts; resolve_span — стаб по словарю спанов из scip? юнит: стаб; интеграцию покроет T9): RouteHandler роли у create_order/get_order/get_document/create_document; каналы `chan:http:orders-api:POST /orders`, `GET /orders/{order_id}`, `GET /documents/{doc_id}`, `POST /documents`; HANDLES направление chan→handler; DEPENDS_ON create_order→get_db (via depends); префикс APIRouter учтён.
- [ ] RED → реализация (+analyze wiring за флагом активных builtin-идиом) → GREEN; full + ruff. Commit: `feat(m2): fastapi extractor (routes, HANDLES, DEPENDS_ON, http channels)`

---

### Task 5: extractors/kafka_ext.py + temporal_ext.py

**Files:**
- Create: `src/codegraph/extractors/kafka_ext.py`, `src/codegraph/extractors/temporal_ext.py`
- Modify: `src/codegraph/config/builtin_idioms.py` (паттерны по реальности, ДАННЫЕ), `src/codegraph/pipeline/analyze.py` (wiring)
- Test: `tests/unit/test_kafka_extractor.py`, `tests/unit/test_temporal_extractor.py`

**Interfaces:**
- kafka: `extract_kafka(ctx, node_ids, idioms: ServiceIdioms, consts) -> KafkaResult(roles, channels, edges, claims, stats)`:
  - producers: match_calls по каждому ProducerIdiom → resolve_value_spec(channel.name_from|event_type_from/topic) → Channel(kafka_topic|event_type) + PRODUCES (src=enclosing-функция id через node_ids/module, dst=chan; resolution/conf = ярус матча ∧ резолв значения: value → как ярус; template/config_ref → "heuristic", min(conf, 0.6), props config_ref при env); event_type+topic оба → CONTAINS chan(topic)→chan(event_type) (extractor "kafka", static/heuristic по резолву).
  - consumers kind="call": match → топик из ValueSpec (arg 0 у AIOKafkaConsumer) → CONSUMES (enclosing → chan) + роль MessageConsumer, props dispatch="topic".
  - consumers kind="dispatch_dict": match registrar_call (STATIC first-party) → dict-ArgFact: ключи-строки = event_type; значения-имена → scip-lookup спана → handler id → CONSUMES handler→chan:event_type:<key> (dispatch="event_type") + роль MessageConsumer на handler; topic из idiom.topic → CONTAINS topic→event_type для каждого ключа.
  - роли MessageProducer на функциях-источниках PRODUCES.
- temporal: `extract_temporal(ctx, node_ids) -> TemporalResult(roles, edges, claims)`:
  - `@workflow.defn` на классе → роль TemporalWorkflow (+props workflow_name=имя класса); `@activity.defn` → TemporalActivity.
  - `workflow.execute_activity(fn_ref, ...)`: call с callee "execute_activity" и receiver "workflow"; arg0 name/attr → scip-lookup → activity id → INVOKES_ACTIVITY (src = enclosing def id — метод run; ПОДНЯТЬ до workflow-КЛАССА: src = id класса-родителя, если enclosing внутри @workflow.defn-класса; иначе enclosing) — golden: KycWorkflow.run → verify_documents… golden src = app.workflows.kyc.KycWorkflow.run (метод!) — сверься с golden: src = МЕТОД run. Значит просто enclosing id. props {"by": "ref"}, static, 1.0.
  - `*.start_workflow(...)`: claim {"kind": "temporal_start_mark", src_id, dst_span→scip → dst_id} — S7 пометит существующее CALLS-ребро props mechanism="temporal_start", resolution="dynamic" (update_edge_props).
- builtin_idioms коррекция: задокументировать финальные паттерны (напр. producers "*.AIOKafkaProducer.send"/"*.AIOKafkaProducer.send_and_wait", consumer-ctor "aiokafka.AIOKafkaConsumer").

- [ ] Тесты на фикстурах (стабы resolve_span/consts где нужно): outbox producer → PRODUCES place→chan:event_type:OrderCreated (STATIC путь: qualified_of от стаба) + CONTAINS orders.events→OrderCreated; builtin producer doc-mgmt: emit_document_indexed→chan:kafka_topic:documents.indexed (IMPORT_NAME ярус, heuristic 0.6); consumer-ctor kyc: run_consumer→chan:kafka_topic:orders.events; dispatch_dict: handle_order_created→chan:event_type:OrderCreated + containment; temporal: роли, INVOKES_ACTIVITY run→verify_documents, temporal_start-claim для start_workflow.
- [ ] RED → реализация → GREEN; full + ruff. Commit: `feat(m2): kafka/outbox and temporal extractors over idiom DSL`

---

### Task 6: extractors/http_client_ext.py

**Files:**
- Create: `src/codegraph/extractors/http_client_ext.py`
- Modify: `src/codegraph/pipeline/analyze.py` (wiring)
- Test: `tests/unit/test_http_client_extractor.py`

**Interfaces:**
- `extract_http_client(ctx, node_ids, idioms) -> HttpClientResult(claims, stats)`:
  - Область: файл матчит HttpClientIdiom.file_glob (fnmatch по relpath) И класс матчит class_glob → методы класса сканируются.
  - Паттерн вызова: attribute-call с callee ∈ {get, post, put, delete, patch} и receiver, содержащим "session" (aiohttp `session.get(...)`/`s.post(...)` — receiver_text любой; критерий: внутри метода клиент-класса И арг0 — string/fstring с URL-шаблоном). arg0 через consts.resolve_arg: template с ведущим `<base>` (f"{self._base_url}/...") → path = хвост после `<base>`; чистая строка с "/" в начале → path как есть; иначе unresolved (stats).
  - Claim `http_call`: {"src_id": id метода (enclosing def через node_ids), "verb": UPPER(callee), "path_template": path, "base_url_env": из idiom.base_url.env | None, "resolution_hint": "static"|"heuristic" по резолву arg0, "evidence": relpath+line}.
  - Рёбра здесь НЕ создаются — CALLS_HTTP делает S7 (нужна таблица роутов всех сервисов).
- Роль MessageProducer НЕ ставится; роли для клиентов не вводим (мастер-план не требует).

- [ ] Тесты на kyc_worker/document_management_client.py (реальные facts): два claim'а — get_document (GET /documents/{doc_id}) и create_document (POST /documents), base_url_env=DOCUMENT_MANAGEMENT_URL (из workspace-идиомы), resolution_hint static (f-string с `<base>`-префиксом детерминирован); файл вне glob → пусто.
- [ ] RED → реализация → GREEN; full + ruff. Commit: `feat(m2): aiohttp client-SDK extractor (http_call claims)`

---

### Task 7: linking (S7) + пайплайн/отчёт wiring

**Files:**
- Create: `src/codegraph/linking/__init__.py`, `src/codegraph/linking/http_routes.py`, `src/codegraph/linking/segments.py`, `src/codegraph/linking/processes.py`, `src/codegraph/linking/workspace.py`
- Modify: `src/codegraph/cli.py` (index: link_workspace между analyze-циклом и load), `src/codegraph/pipeline/report.py` (+каналы/next_segment/unresolved http в сводке)
- Test: `tests/unit/test_linking_*.py`, обновление e2e

**Interfaces:**
- `workspace.link_workspace(cfg: WorkspaceConfig, staging: Staging) -> dict` (link-report): вызывает по порядку: `staging.clear_workspace_layer()` → temporal_start-марки (claims kind temporal_start_mark → update_edge_props) → `http_routes.link(cfg, staging)` → `segments.derive(staging)` → `processes.materialize(cfg, staging)`; возвращает счётчики {calls_http, calls_http_unresolved, next_segments, processes, marks}.
- http_routes: таблица роутов из staged Channel(http_route)+HANDLES (все сервисы); для http_call-claims: кандидаты = сервисы, чей `http.base_url_env == claim.base_url_env` (если None — все); матч verb + посегментно (статические сегменты строго, `{param}` — wildcard с обеих сторон? НЕТ: шаблон клиента `{doc_id}` матчит `{param}` роута — оба плейсхолдеры эквивалентны; клиентский статический сегмент против роутового `{param}` — матч). Успех → CALLS_HTTP (src=claim.src_id, dst=существующий chan http_route, resolution=claim.hint, conf 1.0/0.6, extractor "linking"). Провал → Channel(http_route, owner "?", unresolved=true) + CALLS_HTTP к нему (conf 0.5, heuristic) + счётчик.
- segments: пары (X -PRODUCES→ C, Y -CONSUMES→ C) и (X -CALLS_HTTP→ C, C -HANDLES→ Y): NEXT_SEGMENT X→Y props {via_channel_id: C, derived: true}, conf = произведение, extractor "linking", resolution = слабейший из пары (static+static→static, иначе heuristic). Каналы через CONTAINS topic→event: producer в event, consumer топика → тоже пара (Y слушает topic ⊇ event) — деривация учитывает контейнмент в ОДНУ сторону (event-producer → topic-consumer И topic-producer→event-consumer? Мастер-план: consumer всего топика ловит все его event_type → пара (producer→event, consumer→topic, event∈topic) → NEXT_SEGMENT; (producer→topic, consumer→event) — производитель в топик без типа не гарантирует событие → НЕ дериватить, оставить каналному переходу trace'а).
- processes: из cfg.processes (entrypoint-селектор `"<service>:<METHOD> <path>"` → найти Channel(http_route owner=service, method, template) → HANDLES → handler id; ИЛИ qualified-селектор `service:qualified.name`) + по одному на каждый TemporalWorkflow-узел (source "temporal", entrypoint = workflow node). BusinessProcess NodeRec + PART_OF_PROCESS (entry-узлы сегментов процесса, order): трасса = лёгкий BFS по NEXT_SEGMENT от entry (segments-view), каждому entry ребро с order 0..N.
- cli.index: после цикла analyze — `link_report = link_workspace(cfg, staging)` (внутри _store_guard не нужен — staging-only), report передать в build_report (расширение сигнатуры/словаря).

- [ ] Тесты: юниты сегмент-деривации на синтетическом staging (обе пары, контейнмент-правило, confidence-произведение, via_channel_id, инвариант пропускает NEXT_SEGMENT); http-матчинг (сегменты/verb/плейсхолдеры, кандидат-сужение по env, unresolved-канал); processes (config-селектор и temporal-якорь, order). E2e-обновление: полный index fixtures/workspace.yaml → в staging есть NEXT_SEGMENT цепочка orders→kyc→doc-mgmt.
- [ ] RED → реализация → GREEN; full + falkordb + ruff. Commit: `feat(m2): S7 linking (http route match, NEXT_SEGMENT derivation, processes) + pipeline wiring`

---

### Task 8: trace_process + find_paths + list_processes + find_entrypoint + CLI trace

**Files:**
- Create: `src/codegraph/query/traverse.py`
- Modify: `src/codegraph/query/api.py`, `src/codegraph/mcp/schemas.py`, `src/codegraph/mcp/server.py`, `src/codegraph/cli.py` (команда trace), `src/codegraph/stores/falkordb/ddl.py` + `store.py` (fulltext индекс Sym(name, qualified_name, docstring) + `search_fulltext(query, k, kinds?) -> list[dict]`)
- Test: `tests/unit/test_traverse.py` (fake store), `tests/unit/test_cli_trace.py`, интеграция в T9

**Interfaces:**
- traverse: таблица правил как в мастер-плане §MCP/trace: intra-сегмент out-рёбра {CALLS, DEPENDS_ON, INVOKES_ACTIVITY} + CALLS(mechanism=temporal_start) (props-детект); глубина сегмента ≤15, ветвление ≤8 (`truncated` маркер на сегменте); выходы {PRODUCES, CALLS_HTTP} → каналы; кросс-канал: chan(event) ←CONSUMES← consumers; chan(topic) → вниз по CONTAINS к event-каналам И ←CONSUMES← прямые topic-консьюмеры; chan(http) →HANDLES→ handler; вход нового сегмента = узел с ролью из {MessageConsumer, RouteHandler, TemporalWorkflow} или любой consumer/handler-целевой узел. Быстрый путь: NEXT_SEGMENT (+восстановление канала по via_channel_id). Цикл-отсечка по visited entry-ids; max_segments clamp 1..20 default 12; min_confidence фильтр рёбер.
- `trace_process(entrypoint_id, direction="downstream", max_segments=12, min_confidence=0.3, include_source=False) -> {segments: [{service, entry: NodeOut, steps: [{edge_type, props, node: NodeOut, direction}], exits: [{channel: NodeOut, next_entry_ids: [str]}], truncated: bool}], confidence: float, truncated: bool}` (downstream only в M2; upstream — задел, error dict).
- api: `find_paths(from_id, to_id, max_hops=8, edge_types=None)` — двунаправленный BFS по neighbors (обе стороны), путь как list[{node, edge_type, direction}]; `list_processes()` — BusinessProcess-узлы + entrypoint; `find_entrypoint(query, kinds=None, k=5)` — store.search_fulltext (SANITIZE query: экранировать RediSearch-спецсимволы `@{}|()~*"$:%-` → пробелы; пустой результат — не ошибка).
- MCP: +4 инструмента (схемы pydantic; trace_process включает confidence); CLI `codegraph trace <selector> [--graph] [--format text|mermaid]` — селектор как в processes (route-селектор или qualified) → entrypoint id → trace → text-дерево (rich) или mermaid flowchart (узлы-сегменты, рёбра-каналы).
- ddl: fulltext-индекс создаётся в ensure_schema (idempotent swallow как прежде).

- [ ] Тесты: traverse на fake store — фикстуро-подобный мини-граф (route→calls→produce→event-chan→consumer→invokes→activity→calls_http→http-chan→handles→handler2): 3 сегмента, каналы в exits, temporal_start шаг внутри сегмента, min_confidence отсечка, max_segments truncation, цикл не виснет; find_paths находит путь через NEXT_SEGMENT-ребро; CLI trace: text-вывод содержит цепочку, mermaid — валидные стрелки (смоук); fulltext — live-часть в T9.
- [ ] RED → реализация → GREEN; full + falkordb + ruff. Commit: `feat(m2): trace_process/find_paths/list_processes/find_entrypoint + cli trace`

---

### Task 9: Eval M2 + гейт вехи

**Files:**
- Modify: `src/codegraph/evalx/calls_eval.py` → generalize (или новый `edges_eval.py`), `tests/eval/`
- Create: `tests/eval/test_m2_gate.py`
- Test: юниты на новые eval-функции

**Interfaces:**
- `load_golden_edges(path, types: set[str]) -> set[tuple]` — для типов с dst.channel: кортеж (src_service, src_qualified, dst_channel_id); HANDLES в golden записан «код—канал» → нормализация направления (в найденном src=chan, dst=handler → сравнивать нормализованно: (handler_service, handler_qualified, chan_id)); mechanism-рёбра исключаются только для типа CALLS.
- `found_edges(staging, types) -> tuple[set, int]` — тот же inner-join к nodes для sym-концов; chan-концы берутся как id напрямую (без join); dangling sym-концы → счётчик.
- Гейт-тест (pytestmark scip + falkordb НЕ нужен — staging-only): полный прогон: analyze все 3 сервиса + link_workspace → (а) P/R == 1.0 для каждого из {HANDLES, DEPENDS_ON, PRODUCES, CONSUMES, INVOKES_ACTIVITY, CALLS_HTTP} с fp/fn в сообщениях; (б) channels containment: chan:kafka_topic:orders.events CONTAINS chan:event_type:OrderCreated присутствует; (в) trace_process от entrypoint POST /orders (через staging→фейкстор? трейс работает по GraphStore — для гейта поднять граф в FalkorDB (load_graph) и трейсить по нему; УПРОЩЕНИЕ: сделать InMemoryGraphStore-адаптер над staging для тестов? НЕТ — гейт делаем честно через FalkorDB: маркер + falkordb тоже) — посегментное сравнение (entry qualified + via_channel_id) с fixtures/golden/traces.yaml; (г) «идиома-как-конфиг»: копия workspace-конфига БЕЗ outbox-идиомы → PRODUCES place→OrderCreated отсутствует, NEXT_SEGMENT orders→kyc отсутствует; с полным — есть. Гейт-маркеры: scip + falkordb.
- CLI-проверка мастер-плана: `codegraph trace "orders-api:POST /orders"` печатает цепочку (в e2e-тест или руками в отчёте).

- [ ] RED (нет функций) → GREEN; полный прогон всех маркеров; ruff. Commit: `feat(m2): typed-edge eval + trace gate (M2)`
- [ ] ЕСЛИ гейт падает: НЕ ослаблять; DONE_WITH_CONCERNS с полными fp/fn/дифф трейса — решение за контроллером.

---

## Верификация M2 (после T9)

1. `uv run python -m pytest --junit-xml=...` (default) + `-m falkordb` + `-m scip` (гейты M1 и M2) — все зелёные; ruff чист.
2. `uv run codegraph index fixtures/workspace.yaml --graph m2check && uv run codegraph trace "orders-api:POST /orders" --graph m2check` — цепочка route → … → Channel(OrderCreated) → kyc handler → workflow → activity → Channel(GET /documents/{doc_id}) → doc-mgmt handler; `--format mermaid` валиден. Cleanup графа.
3. MCP: contract-тест расширен (8 инструментов); живое подключение из Claude Code — ручной пункт пользователя.
4. Финальное whole-milestone ревью (fable) + фикс-волна.

