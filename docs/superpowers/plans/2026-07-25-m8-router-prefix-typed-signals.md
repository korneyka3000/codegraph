# M8 — транзитивный префикс роутеров и типизированные сигналы: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи: `docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md` (R4/R5 с корнями, file:line и доказательствами величины) + числа re-run №2 в `2026-07-24-pilot-rerun-2.md`.

**Goal:** Разблокировать оба end-to-end хопа, оставшиеся после M7: кросс-сервисный HTTP (R4: FastAPI-префикс из `include_router(..., prefix=…)` в другом файле — доказано 23/24 anchored-claim'а станут static/1.0) и сигнальный хоп (R5: типизированные `handle.signal(Cls.method, …)` — 18/18 реальных отправок; имя восстановимо из уже разобранной хендлерной стороны). Доказательство — расширение realstack + гейт.

**Architecture:** Оба фикса — один и тот же архитектурный ход «S5 эмитит факты-claims, S7 связывает кросс-файлово» (прецедент http_call/CALLS_HTTP из M2). R4: fastapi_ext перестаёт строить роут-каналы прямо в S5 — вместо этого эмитит per-file claims `route_decl` (символ роутер-объекта, verb, локальный шаблон, handler-узел, роли) и `router_include` (символ родителя, символ ребёнка, префикс); новый линковочный шаг (S7, до http_routes.link) композитно собирает полные шаблоны транзитивным обходом include-цепочек до корневого `FastAPI()`-объекта и создаёт Channel+HANDLES (+DEPENDS_ON переносится как есть). Цикл/нерезолвимый родитель → беспрефиксный путь + счётчик `route_prefix_unresolved` (никаких догадок). R5: temporal_ext для attr/name-shaped arg0 резолвит СИМВОЛ метода через ref_symbol_lookup (как INVOKES_ACTIVITY) и эмитит claim `temporal_signal_send` (src-узел, символ метода); новый линковочный шаг находит CONSUMES-ребро этого символа в `chan:temporal_signal:*` и вешает PRODUCES sender→тот же канал (static/1.0 — символьная ссылка есть полная истина); строковая ветка не меняется (heuristic/0.6, прямая S5-эмиссия).

**Tech Stack:** без новых зависимостей.

## Global Constraints

- Кортежи golden (type, src, dst) существующих фикстур НЕ меняются: composition обязана выдать байт-те-же шаблоны для in-file `APIRouter(prefix=…)`-случаев (все текущие фикстуры). Все гейты M1–M7 зелёные в каждой задаче; сдвиг golden возможен ТОЛЬКО по протоколу верифицированного дополнения.
- evalx/edges_eval сравнивает (type, src, dst[, resolution/confidence — ПРОЧИТАТЬ EdgeTuple в T1 и зафиксировать]) — перенос эмиссии каналов S5→S7 не должен сдвинуть ни один существующий кортеж; смена extractor-тега допустима только если он вне сравнения (проверить и записать).
- Инкрементальная когерентность: все новые claims — per-file (delete_file_layer уже чистит по relpath); S7-созданные каналы/рёбра — extractor="linking" (clear_workspace_layer пересобирает); Channel-GC (gc_orphan_channels) продолжает работать. M4 supreme-гейт (дамп-эквивалентность) обязан выжить обе задачи.
- Честность: нерезолвимое — счётчик + беспрефиксный/без-ребра исход, не догадка; static/1.0 только при символьном резолве.
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

R4-механика: `extractors/fastapi_ext.py:84` `_route_prefix` (только same-file `APIRouter(prefix=…)`), `:178` `_template(...)`; роут-каналы и HANDLES сейчас — S5 domain_channels/domain_edges (origin_service=svc). Include-цепочка реального кода: `APIRouter()` в файле A → `router.include_router(child.router, tags=[…])` в `__init__.py` → `app.include_router(api.v1.router, prefix="/api/v1")` в main.py. Символы роутер-объектов резолвятся scip (module-level assignments → defs). R5-механика: `temporal_ext.py:528` `_resolve_signal_arg0` — attr/name → consts.resolve_arg → мисс; строковая ветка `:525` работает. Хендлерная сторона уже строит `chan:temporal_signal:<name>` + CONSUMES от символа метода. Числа для калибровки: 24 anchored-claim'а → 23 уникальных матча с префиксом; 18 типизированных отправок + 3 `get_external_workflow_handle`; 25 каналов / 27 CONSUMES уже есть.

Порядок линковки в `linking/workspace.py` (прочитать): новые шаги — роут-композиция ДО http_routes.link (ему нужны полные шаблоны), сигнальная линковка ПОСЛЕ всех analyze (CONSUMES-рёбра всех сервисов в staging) и ДО segments.derive (NEXT_SEGMENT из новых PRODUCES).

### Task 1: R4 — route_decl/router_include claims + композиция префиксов

**Files:**
- Modify: `src/codegraph/extractors/fastapi_ext.py` (эмиссия claims вместо прямых каналов; roles/DEPENDS_ON — как сейчас), `src/codegraph/pipeline/analyze.py` (wiring claims), новый `src/codegraph/linking/router_prefix.py` (композиция: граф include'ов по символам; DFS от каждого route_decl вверх до FastAPI-корня; конкатенация префиксов в порядке цепочки; цикл-guard; счётчик), `src/codegraph/linking/workspace.py` (шаг до http_routes.link), `src/codegraph/pipeline/report.py` (route_prefix_unresolved по M6-прецеденту)
- Test: `tests/unit/test_fastapi_extractor.py`, `tests/unit/test_router_prefix.py`, `tests/unit/test_pipeline_analyze.py`

**Interfaces:**
- claims: `route_decl` payload `{router_symbol, verb, path, handler_node_id, prefix_local}` (prefix_local — same-file `APIRouter(prefix=…)`, вливается в композицию первым звеном); `router_include` payload `{parent_symbol, child_symbol, prefix}` (`prefix` может быть None). Символы — через существующие def/ref-lookups (степень достоверности как у INVOKES_ACTIVITY; нерезолвимый символ → route_decl остаётся с локальным шаблоном + счётчик).
- Композиция: полный шаблон = concat(префиксы корень→лист) + prefix_local + path; корень = объект без родителя в include-графе (FastAPI() или роутер, который никто не включает — для беспрефиксных сервисов это тождество). Канал/HANDLES создаются в router_prefix.py по нынешним формам (make_channel_node http_route + HANDLES channel→handler; extractor="linking", origin=None).
- Существующие same-file фикстуры: их route_decl.prefix_local воспроизводит нынешний шаблон байт-в-байт (гейты M2/M6/M7 без сдвигов — проверка внутри задачи).

- [ ] **Step 1: Прочитать edges_eval.EdgeTuple** — входит ли extractor/resolution в сравнение; зафиксировать в отчёте. Прочитать fastapi_ext полностью.
- [ ] **Step 2: Падающие юниты фактов** — route_decl с router_symbol; router_include с prefix и без; include незнакомого символа → claim с None-символом (счётчик на композиции).
- [ ] **Step 3: Падающие юниты композиции** — цепочка A(router)→B(include без префикса)→C(include prefix=/api/v1) → шаблон /api/v1/steps/{id}; same-file prefix_local — прежний результат; цикл → беспрефиксный + счётчик; нерезолвимый родитель → то же; два роута одного роутера → оба с префиксом.
- [ ] **Step 4: RED → реализация → GREEN; wiring workspace.py.**
- [ ] **Step 5: Гейты M2/M6/M7 (кортежи каналов/HANDLES обязаны быть байт-теми-же) + M4 supreme + полный сьют + ruff; Commit** — `feat(m8): transitive router-prefix composition via route_decl/router_include claims (rerun-2 R4)`

### Task 2: R5 — типизированные сигнальные отправители

**Files:**
- Modify: `src/codegraph/extractors/temporal_ext.py` (attr/name arg0 → ref_symbol_lookup → claim `temporal_signal_send` {src_node_id, method_symbol}; строковая ветка не тронута; нерезолвимый — прежний счётчик), новый шаг в `src/codegraph/linking/workspace.py` (или мини-модуль `linking/signal_send.py`): по claim'ам найти CONSUMES-ребро `sym:<method>` → `chan:temporal_signal:*` и создать PRODUCES src→канал (static/1.0, mechanism="temporal_signal", extractor="linking"); символ без CONSUMES (метод не-signal или чужой скоуп) → счётчик `signal_send_unlinked`
- Test: `tests/unit/test_temporal_extractor.py`, `tests/unit/test_linking_signal_send.py`, report-wiring тесты

**Interfaces:**
- `handle.signal(PartnerProfileWorkflow.complete_survey, …)` → arg0 attr → ref на `complete_survey`-метод → символ → claim; S7: CONSUMES from sym → канал `chan:temporal_signal:complete-survey` (имя декоратора, УЖЕ разобранное хендлерной веткой) → PRODUCES тот же канал. Bare-name импортированный метод — тот же путь (ref_symbol_lookup по name-токену). `get_external_workflow_handle(...).signal(Cls.m, …)` — тот же матчер.
- Резолюции: символ разрешён и CONSUMES найден → static/1.0; разрешён, но CONSUMES нет → signal_send_unlinked (счётчик, без ребра); не разрешён → signal_name_unresolved (прежнее).

- [ ] **Step 1: Падающие юниты экстрактора** (attr → claim; bare-name → claim; строковый литерал — прежняя прямая эмиссия PRODUCES heuristic/0.6 не тронута; переменная → прежний счётчик).
- [ ] **Step 2: Падающие юниты линковки** (claim + существующий CONSUMES → PRODUCES static/1.0 в тот же канал; claim без CONSUMES → unlinked-счётчик; кросс-файловость — символ из другого файла сервиса).
- [ ] **Step 3: RED → реализация → GREEN; порядок в workspace.py (до segments.derive — NEXT_SEGMENT из пар); M2/M6/M7 + M4 + полный сьют + ruff; Commit** — `feat(m8): typed signal senders via symbol-resolved claims (rerun-2 R5)`

### Task 3: realstack-леги + гейт

**Files:**
- Modify: `fixtures/realstack/` — gateway: вынести include-цепочку в отдельные файлы (роутер без префикса в routes-файле + `app.include_router(router, prefix="/api/v1")` в main), обновить клиентские пути на префиксные; typed-signal лег: `handle.signal(SubmissionWorkflow.doc_approved, …)` из consumer'а (вместо/вместе со строковым — оба варианта в golden с корректными резолюциями static/1.0 и heuristic/0.6)
- Modify: `fixtures/realstack/golden/edges.yaml` (+ обновление HTTP-кортежей на префиксные шаблоны — это M6/M7-записи: протокол верифицированного дополнения с санкцией контроллера — тут санкция ЕСТЬ: смена шаблонов есть суть R4-фикса; каждое изменение задокументировать в header), `golden/traces.yaml` (сигнальный хоп теперь с PRODUCES-парой), `tests/eval/test_m8_gate.py` (маркеры scip+falkordb: P=R=1.0 по типам + route_prefix_unresolved=0 + signal_send пары + funnel-negative остаётся + трейс через typed-signal хоп)
- [ ] **Step 1: Фикстурные правки + golden (вручную выведенные, header-протокол).**
- [ ] **Step 2: Падающий M8-гейт → зелёный** (фиксить механику, не golden; M6/M7-гейты на shared-golden — прогнать и подтвердить/санкционировать сдвиги честно).
- [ ] **Step 3: Все сьюты + все гейты M1–M8 + ruff; Commit** — `feat(m8): realstack router-chain + typed-signal legs + gate`

### Task 4: Финал вехи

- [ ] **Step 1:** Полные сьюты junit; сверка R4/R5-разделов open-gaps с закрывающими тестами.
- [ ] **Step 2:** Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение.
- [ ] **Step 3:** Леджер `=== M8 ЗАВЕРШ�etern ===`; память; push; re-run бриф №3 (ожидания: CALLS_HTTP anchored static/1.0 ≈23; sender↔handler пары 18−нерезолвимые; NEXT_SEGMENT сигнальные+HTTP; полная трасса «Kafka → consumer → signal → workflow → activity → HTTP → чужой роут» — ГЛАВНЫЙ вердикт, ради которого весь проект); отчёт пользователю.

## Self-review плана

1. **Покрытие open-gaps №2**: R4→T1 (направление фикса реализовано дословно: claims+линковка, транзитивность, честные счётчики), R5→T2 (вариант (a) claim+линковка как предпочтительный; строковая ветка нетронута), доказательство→T3, «мелкое/принятое» — NEXT_SEGMENT-kafka=0 остаётся свойством среза (backlog-вариант (c) прежний), DELETE-находка — для команды пользователя, не для нас.
2. **Главный риск** — сдвиг существующих golden при переносе эмиссии: закрыт констрейнтом байт-тех-же шаблонов для same-file случаев + явным протоколом для реально-меняющихся (T3 realstack HTTP-кортежи).
3. **Типы согласованы**: claims-формы T1/T2 ↔ линковочные шаги ↔ T3 golden; счётчики по установленному прецеденту.
4. Плейсхолдеров нет; проверки-по-месту (EdgeTuple-поля, порядок workspace.py) — явные первые шаги задач.
