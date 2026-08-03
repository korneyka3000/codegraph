# M10 — агентский опыт и долги: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи: MCP-пилот `docs/superpowers/reports/2026-08-03-mcp-pilot.md` §4-§5 (три входа + диагностика dropped) + TRACKED-долги M9 (леджер `.superpowers/sdd/progress.md`, M10-строки).

**Goal:** Закрыть три входа MCP-пилота (резолв модульных синглтон-вызовов — ~53% dropped CALLS одним паттерном; who_calls для Temporal-активностей; chunk-гранулярность в search_code) и TRACKED-долги M9 (per-edge external props как правильный фикс вместо fail-closed-паллиатива; dissolution-фикс с инверсией двух no-write-пинов).

**Architecture:** Синглтон-резолв — S6-уровневый доджойн: callee-форма `<module_attr>.<method>` где модуль содержит module-level `name = ClassName(...)`-присваивание → dst перенацеливается на `ClassName.method`-узел (тип из AssignFact того же модуля — тот же приём, что M7 class-attr harvesting; резолв ClassName через существующие def/ref-lookups; честный fallback: нет узла метода → прежний drop). who_calls — расширение обхода для узлов с ролью TemporalActivity: INVOKES_ACTIVITY-источники включаются в ответ (помечены mechanism'ом). search_code — ответ обогащается `enclosing_symbol`/`chunk_kind` (class/method) из уже существующих staged-полей чанка; метрика eval НЕ ослабляется (строгий hit@k остаётся; отчёт пилота прав: правильный класс ≠ попадание в метод). Per-edge external — external/external_host переезжают на CALLS_HTTP-ребро (props уже есть у EdgeRec), traverse читает exit-hop edge-props; shared-канальный fail-closed-мерж упрощается (node-props уходят), config_ref last-wins умирает вместе с ним. Dissolution — read-compare запись композитного пути (инверсия двух T2-пинов «avoid no-op writes» — санкционировано финалом M9).

**Tech Stack:** без новых зависимостей.

## Global Constraints

- Гейты M1–M9 зелёные в каждой задаче; golden-сдвиги только по протоколу (T1 добавит CALLS-рёбра на пилот-подобной синтетике — существующие фикстуры без синглтон-паттерна → нулевые сдвиги; проверять).
- Синглтон-резолв: только module-level присваивания с резолвимым ClassName и существующим методом-узлом; всё прочее — прежний честный drop (счётчик остаётся тем же механизмом «staged CALLS valid dst %»). resolution static при scip-резолве ClassName, heuristic при текстовом.
- who_calls: INVOKES_ACTIVITY-источники — ТОЛЬКО для целей с ролью TemporalActivity, помечены в ответе (аддитивное поле mechanism); прежние вызовы без изменений байтово.
- search_code: только аддитивные поля ответа; MCP-схема аддитивна; eval-метрика не трогается.
- Per-edge external: trace-семантика (confidence-исключение, external_exit_count, рендер) сохраняется бит-в-бит — меняется ИСТОЧНИК чтения (edge-props вместо node-props); канальные node-props external можно оставить на переходный период или снять — решить в задаче и задокументировать; M9-гейт пины обновить честно по протоколу.
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

Пилот §5: 149 dropped CALLS; 79 (53%) — `app.db.registry.session` (`registry = _DBRegistry(config.database.dsn)` module-level; 79 сайтов `registry.session()` в vr+dm); ещё ~30 — методы на модель-инстансах (та же форма). S6: `extractors/calls.py::build_calls` — dst из ref-символа callee; module.attr-символ не имеет узла → load дропает. AssignFact (M2, parsing/facts.py) несёт module-level присваивания; RHS-call `ClassName(...)` — ctor-форма. who_calls: `query/api.py`/`query/traverse.py` — обход по CALLS in-edges. search_code ответ: `query/retrieval.py` — поля из chunk-props; staged chunk несёт symbol_id (= id узла-владельца чанка). Per-edge: M9-финал Important-1 паллиатив в `linking/http_routes.py` (_reconcile_shared_channel_external_props); правильное направление задокументировано там же. Dissolution: `linking/router_prefix.py` TRACKED-заметка (:286-область) + два пина no-write в test_router_prefix.py.

### Task 1: Резолв модульных синглтон-вызовов (пилот §5 — 53% dropped)

**Files:**
- Modify: `src/codegraph/extractors/calls.py` (доджойн: callee-ref символ формы `module`/attr [не класс/функция] → lookup module-level AssignFact-типов), `src/codegraph/pipeline/analyze.py` (прокладка module-level типов: per-file карта `name → ClassName-символ` из facts, service-wide индекс по образцу ClassAttrIndex — РЕШИТЬ по месту: claims-reuse [инкремент-когерентность!] как class_attrs [M7-T1 прецедент] — ДА, claims kind="module_singletons")
- Test: `tests/unit/test_calls_join.py`, `tests/unit/test_pipeline_analyze.py`

**Interfaces:**
- Harvest (в class_attrs-подобном пре-проходе или расширением его): module-level `name = ClassName(...)` (RHS — ctor-форма: последний сегмент капитализирован — существующая эвристика kafka ctor) → claim `{name, class_symbol|class_name_text}`; ClassName резолвится через def/ref-lookup (scip) → символ класса.
- Доджойн в build_calls: callee не разрешился в узел ИЛИ разрешился в module.attr-символ без узла → проверить синглтон-карту сервиса: `<name>.<method>` где name в карте → кандидат-узел `sym:<svc>:<ClassDescriptors>#<method>().` — существует в staged defs → dst=узел метода, resolution по тиру резолва ClassName (static при scip / heuristic 0.6 при текстовом), props mechanism="singleton_dispatch"; не существует → прежний путь (drop на load).
- Инкремент-когерентность: claims per-file (delete_file_layer), индекс собирается как class_attrs (M7-T1 механика, включая эскалацию? НЕТ — синглтон-типы влияют на CALLS других файлов так же, как class_attrs на консьюмеров! ПРОВЕРИТЬ: правка файла с `registry = ...` меняет джойн вызовов в ДРУГИХ файлах → нужен тот же escalation-механизм шага 8a: включить kind="module_singletons" в digest эскалации [прочитать как 8a считает digest — если по всем class_attrs-claims, добавить новый kind в него; задокументировать]).

- [ ] **Step 1: Прочитать** M7-T1 harvest/escalation механику (analyze.py 8a; class_attrs.py), пилотный разбор §5. Зафиксировать design-решения в отчёте.
- [ ] **Step 2: Падающие юниты harvest** (module-level ctor-присваивание → claim; функция-RHS → нет; annotated-присваивание — прочитать AssignFact-возможности).
- [ ] **Step 3: Падающие юниты доджойна** — синтетика пилотного паттерна: `registry = _DBRegistry(...)` + `async with registry.session() as s` в другом файле → CALLS на `_DBRegistry.session` узел (resolution по тиру); метод не существует у класса → drop как раньше; НЕ-module-level (локальная переменная) → не трогаем (прежний путь); эскалация: правка синглтон-файла под incremental → full re-extract.
- [ ] **Step 4: RED → реализация → GREEN; M1-гейт (P/R неизменны — фикстуры без паттерна), M4-гейт (эскалация!), полный сьют + ruff; Commit** — `feat(m10): module-level singleton method-call resolution (pilot: 53% of dropped CALLS)`

### Task 2: who_calls × INVOKES_ACTIVITY (пилот §4.3)

**Files:**
- Modify: `src/codegraph/query/api.py` (who_calls: цель с ролью TemporalActivity → дополнительно in-рёбра INVOKES_ACTIVITY; элементы ответа несут аддитивное `mechanism: "invokes_activity"`), `src/codegraph/mcp/schemas.py` (аддитивно), tool-description who_calls (упомянуть активности)
- Test: `tests/unit/test_query_api.py`, MCP-контракт

- [ ] **Step 1: Падающие юниты** — активность с INVOKES_ACTIVITY-источниками → они в ответе с mechanism; обычная функция → байтово прежний ответ; transitive-режим — прочитать семантику и решить (включать INVOKES_ACTIVITY-хопы в транзитивный обход для активностей? ДА, симметрично; задокументировать).
- [ ] **Step 2: RED → GREEN; полный сьют + ruff; Commit** — `feat(m10): who_calls surfaces INVOKES_ACTIVITY sources for temporal activities (pilot 4.3)`

### Task 3: search_code chunk-гранулярность (пилот §4.1-4.2)

**Files:**
- Modify: `src/codegraph/query/retrieval.py` (ответ: +`enclosing_symbol` [qualified_name узла-владельца чанка — staged symbol_id → узел], +`chunk_kind` ["class"|"function"|"module" — kind узла-владельца]), `src/codegraph/mcp/server.py` (search_code description: подсказка service-фильтра для «где обрабатывается» [сервер] vs «кто вызывает» [клиент] — пилот §4.2), `src/codegraph/mcp/schemas.py` (аддитивно)
- Test: `tests/unit/test_retrieval.py`, MCP-контракт

- [ ] **Step 1: Падающие юниты** — ответ несёт enclosing_symbol/chunk_kind (по staged узлу symbol_id); eval-метрика НЕ трогается (никаких изменений в evalx — подтвердить нулевым диффом).
- [ ] **Step 2: RED → GREEN; полный сьют + M3-гейт (retrieval-путь!) + ruff; Commit** — `feat(m10): search_code returns enclosing symbol + chunk kind (pilot 4.1)`

### Task 4: Per-edge external + dissolution (TRACKED-долги M9)

**Files:**
- Modify: `src/codegraph/linking/http_routes.py` (external/external_host/config_ref → props CALLS_HTTP-РЕБРА; канальные external-node-props снимаются [канал остаётся ?-owner unresolved-узлом]; `_reconcile_shared_channel_external_props` УДАЛЯЕТСЯ [паллиатив больше не нужен — рёбра per-claim, коллизий нет]; счётчики прежние), `src/codegraph/query/traverse.py` (exit-hop: external-детекция из props РЕБРА [neighbors отдаёт edge-props? прочитать Hop-контракт store.neighbors — если рёбра-props не прокидываются, добавить аддитивно в FalkorStore.neighbors+Hop], confidence-исключение и external_exit_count — семантика бит-в-бит), `src/codegraph/cli.py` (рендер из edge-props), `linking/router_prefix.py` + `tests/unit/test_router_prefix.py` (dissolution: read-compare запись — patch пишется/снимается по фактическому сравнению с staged-значением; ДВА no-write-пина ИНВЕРТИРУЮТСЯ [санкция M9-финала]; supreme-инвариант для edit-shape «все mounts удалены» восстанавливается — пин по probe1-сценарию)
- Test: `tests/unit/test_linking_http_routes.py` (коллизионные пины a/b/c/d ПЕРЕПИСЫВАЮТСЯ под новую семантику: per-edge — каждый claim несёт СВОИ props, «коллизия» исчезает как класс; честный комментарий), `tests/unit/test_traverse.py`, `tests/eval/test_m9_gate.py` (external-пины на ребро — протокол-обновление), M4-гейт

- [ ] **Step 1: Прочитать** Hop/neighbors-контракт; зафиксировать план прокладки edge-props.
- [ ] **Step 2: Падающие юниты per-edge** (external на ребре; два claim'а разных env на один канал → КАЖДОЕ ребро со своими props — ни клоббера, ни мержа; traverse исключает по edge-props; счётчики).
- [ ] **Step 3: RED → реализация → GREEN.**
- [ ] **Step 4: Dissolution read-compare + инверсия пинов + probe1-пин → GREEN.**
- [ ] **Step 5: M9-гейт (протокол-обновление пинов) + M4 supreme + полный сьют + ruff; Commit** — `fix(m10): per-edge external props (proper M9 fix); dissolution read-compare (supreme invariant restored)`

### Task 5: realstack-леги + гейт + финал

**Files:**
- Modify: `fixtures/realstack/` (синглтон-лег: module-level `store = DocStore(...)` + вызов `store.persist()` из другого файла → CALLS-ребро; activity-who_calls уже покрыт INVOKES_ACTIVITY-рёбрами гейтов — контракт-тест MCP: who_calls активности非пуст), `fixtures/realstack/golden/edges.yaml` (+ singleton-CALLS), `tests/eval/test_m10_gate.py` (пины: singleton-CALLS resolution/mechanism; search_code-ответ поля [live falkordb]; who_calls-activity через GraphQuery; per-edge external через real round-trip [M9-гейт пины переехали/дополнены])
- [ ] **Step 1: Фикстура+golden (протокол); падающий гейт → зелёный; ВСЕ гейты M1–M10 + полный сьют + ruff; Commit** — `feat(m10): realstack singleton leg + gate (agent-experience pins)`
- [ ] **Step 2: Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение; леджер `=== M10 ЗАВЕРШЁН ===`; память; push; отчёт пользователю + re-run бриф №5** (чекпойнты: dropped CALLS 149 → ~40 [минус ~79 registry + часть моделей]; who_calls активности ×3 непусты; search_code с enclosing_symbol — пере-скоринг 8 RU-вопросов с учётом class-level-попаданий как ИНФОРМАЦИИ [метрика та же]; external на рёбрах — трасса бит-в-бит).

## Self-review плана

1. **Покрытие спеки**: пилот §5→T1 (направление дословно), §4.3→T2, §4.1-4.2→T3 (ответ богаче + подсказка; метрика строгая), TRACKED M9→T4 (оба направления из финала M9 буквально), доказательства→T5. Client-vs-server промах — подсказкой в description (T3), не механикой (честно: неоднозначность запроса).
2. **Эскалация синглтонов** — та же дыра, что class_attrs (T1 включает в 8a-digest; M4-гейт обязателен).
3. **Per-edge упрощает**: паллиатив и коллизионный класс умирают; пины переписываются протокольно с санкцией (зафиксирована финалом M9 как «правильный фикс»).
4. Плейсхолдеров нет; чтения-по-месту размечены.
