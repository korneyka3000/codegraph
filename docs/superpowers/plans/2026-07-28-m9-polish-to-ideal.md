# M9 — доводка до идеала: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи: неидеальности re-run №3 (`docs/superpowers/reports/2026-07-24-pilot-rerun-3.md` §3-дисклеймер, §5) + консолидированный M9-беклог в `.superpowers/sdd/progress.md`. Решение пользователя (2026-07-28): в прод рано, доводим до идеала; MCP-пилот — после полиша, на доведённой версии.

**Goal:** Убрать всё «не идеально» из фидбека №3: внешние HTTP-цели получают честную первоклассную семантику (не «unresolved», и не тянут trace-confidence вниз), карточки хендлеров показывают композитный путь, double-mount роутеров работает вместо discard'а, мелкий беклог-хвост закрыт. Затем — бриф MCP-пилота (первый прогон с эмбеддингами + агентные вопросы, верификация №7 мастер-плана).

**Architecture:** Внешность — свойство ЯКОРЯ, а не провала: tier-2 M7-якорения («env известен, hostname вне воркспейса») перестаёт сливаться с generic-unresolved — канал получает `external=true` + `external_host` (из env-значения), отдельный счётчик, и trace-confidence-модель учится не штрафовать сегмент за честно-внешний выход (внешний хоп — граница знания, не неопределённость). Multi-mount — снятие M8-under-approximation: composed-template-per-mount (один route_decl × N mount-путей → N каналов), ambiguity остаётся только для настоящих конфликтов (два router_decl с разными prefix_local). Compose-back — S7 дописывает финальный шаблон в node-props хендлера (staged nodes update — прочитать, есть ли update-механизм узлов; вероятно, через повторный upsert_nodes патченного NodeRec).

**Tech Stack:** без новых зависимостей.

## Global Constraints

- Существующие golden-кортежи не сдвигаются, КРОМЕ санкционированных: multi-mount добавит НОВЫЕ каналы realstack (аддитивно), external-семантика на фикстурах не проявляется (фикстурные env→вне-workspace случаи есть? прочитать realstack env_values — там только worker; external-лег добавит T4-фикстура). Все гейты M1–M8 зелёные в каждой задаче.
- Честность: external ≠ resolved (кросс-сервисное ребро НЕ создаётся — воркспейс не знает цели); external-канал — терминальный узел трассы с явной пометкой; confidence-модель меняется ТОЛЬКО в сторону «не штрафовать честное знание о границе» — задокументировать формулу до/после.
- Инкрементальная когерентность и M4 supreme-гейт — выживают каждую задачу.
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

Tier-2 сейчас: `linking/http_routes.py` — env известен, `env→service`-резолв не нашёл воркспейс-сервис → generic unresolved-канал `chan:http:?:<VERB> <template>` heuristic/0.5 (re-run №3: 35 уникальных ?-каналов, 36 claims; трасс-confidence 0.50 «отражает присутствие heuristic/0.5 HTTP-каналов… а не слабость резолвнутых хопов»). env-ЗНАЧЕНИЕ (URL с hostname) доступно в env_map на месте решения. Trace-confidence: `query/traverse.py` — прочитать агрегацию (min/product по сегментам/хопам). Multi-mount: `linking/router_prefix.py` `_AMBIGUOUS`-сентинел, докстринг-шейп 3 (M8-финал finding 3). Node-props хендлера: `path_template` пишется fastapi_ext'ом в S5 (локальный); Channel несёт композитный. Беклог-хвост: `client.execute_workflow` в `_START_WORKFLOW_CALLEES` (client-side спеллинг, M6-carry); seam-пин T1×T3 (M5-carry: CALLS в collision-family — репро сценарий в леджере M5-T3-ревью); nested-subscript naming — уже задокументирован (M6), проверить и закрыть пункт.

### Task 1: External-канал — первоклассная семантика tier-2

**Files:**
- Modify: `src/codegraph/linking/http_routes.py` (tier-2: канал `chan:http:ext:<host>:<VERB> <template>`... НЕТ — id-стабильность: прочитать ids.chan_http форму и выбрать: сохранить `?`-owner в id [не ломать], добавив props `external=true, external_host=<hostname>`; резолюция остаётся heuristic, confidence поднять до 0.6? НЕ поднимать без основания — оставить 0.5, но пометить), `src/codegraph/query/traverse.py` (confidence-агрегация: external-каналы исключаются из штрафа [формула до/после в докстринге]; вывод трассы: `→ external <host>` вместо `→ unresolved`), `src/codegraph/pipeline/report.py` (+`calls_http_external` отдельно от unresolved), `src/codegraph/mcp/schemas.py` (если exit-схема несёт unresolved-флаг — аддитивно external)
- Test: `tests/unit/test_linking_http_routes.py`, `tests/unit/test_traverse.py`, report-тесты

**Interfaces:**
- tier-2 (env есть, значение есть, hostname вне воркспейса) → канал с props `{external: true, external_host: "<hostname>"}` + счётчик `calls_http_external`; tier-2-без-значения (env не в env_map вовсе) и unanchored-провалы — прежний generic unresolved (`calls_http_unresolved`). Трасса: external-выход отображается «channel VERB /path → external api-gateway.prod.env», сегментная confidence НЕ умножается на 0.5 этого канала (формула: внешние exit-хопы не входят в произведение; задокументировать и запинить тестом «трасса с единственным external-выходом держит confidence 1.0»).

- [ ] **Step 1: Прочитать** ids.chan_http, tier-решения http_routes, confidence-агрегацию traverse. Зафиксировать формулу до/после в отчёте задачи.
- [ ] **Step 2: Падающие юниты** — tier-2-со-значением → external-props+счётчик; без значения → прежний unresolved; трасса с external-выходом → confidence не штрафуется + рендер «→ external <host>»; MCP-схема аддитивна.
- [ ] **Step 3: RED → реализация → GREEN; гейты M2/M6/M7/M8 (fixtures без external-легов — ноль сдвигов) + полный сьют + ruff; Commit** — `feat(m9): first-class external http targets (props, counter, honest trace confidence)`

### Task 2: Compose-back композитного пути в карточку хендлера

**Files:**
- Modify: `src/codegraph/linking/router_prefix.py` (после композиции — патч node-props хендлера: `path_template` → композитный; механизм: прочитать staged-узел, upsert патченного NodeRec [staging.upsert_nodes INSERT OR REPLACE]; ВНИМАНИЕ инкремент: патч в S7 на каждом прогоне — идемпотентен; но узел принадлежит origin-сервису, а патч идёт из линковки — не ломает ли delete/begin-семантику? узел тот же, только props — прочитать, как temporal_start-марка патчит CALLS [update_edge_props-прецедент]: для УЗЛОВ аналога нет — добавить `staging.update_node_props(id, merge)` по образцу update_edge_props)
- Test: `tests/unit/test_router_prefix.py`, `tests/unit/test_staging.py`

**Interfaces:**
- `staging.update_node_props(node_id: str, merge: dict) -> bool` (shallow-merge в props JSON, по образцу update_edge_props; False если узла нет). router_prefix.link после создания канала патчит handler-узлу `path_template` на композитный (только если отличается). get_source/карточки/retrieval-headers читают props — композитный путь станет виден везде.
- M4-инвариант: дамп-эквивалентность — патч детерминирован, в full и incremental одинаков (S7 всегда полный).

- [ ] **Step 1: Падающие юниты staging.update_node_props → RED → реализация → GREEN.**
- [ ] **Step 2: Падающие юниты router_prefix-патча** (композитный путь в props после link; same-file тривиальная цепочка — патч не меняет [путь совпадает]; идемпотентность двойного link).
- [ ] **Step 3: RED → GREEN; M4 supreme + гейты + полный сьют + ruff; Commit** — `feat(m9): compose-back full path_template into handler node props`

### Task 3: Multi-mount роутеров

**Files:**
- Modify: `src/codegraph/linking/router_prefix.py` (include-граф: child→СПИСОК родительских маунтов; композиция даёт декартово произведение путей по всем цепочкам; один route_decl × N mount-путей → N каналов+HANDLES [id различаются шаблоном — естественно]; `_AMBIGUOUS` остаётся ТОЛЬКО для двух router_decl с разными prefix_local одного символа; докстринг-шейп 3 переписать)
- Test: `tests/unit/test_router_prefix.py`

**Interfaces:**
- `app.include_router(r, prefix="/v1")` + `app.include_router(r, prefix="/legacy")` → роут `/x` даёт КАНАЛЫ `/v1/x` И `/legacy/x`, оба HANDLES на тот же хендлер (FastAPI-семантика: оба пути живые). Byte-identical include-дубликаты (два файла, один и тот же mount) → дедуп по (parent, child, prefix). Циклы/нерезолвимые — прежние discard+counter. Compose-back (T2) при multi-mount: props получают ПЕРВЫЙ по сортировке шаблон + `path_templates`-список всех (задокументировать выбор).

- [ ] **Step 1: Падающие юниты** — double-mount → 2 канала/2 HANDLES; дубликат-include → 1; кросс-произведение по цепочке (mount выше по дереву); прежние ambiguity-кейсы (разные prefix_local) — не тронуты.
- [ ] **Step 2: RED → GREEN; гейты + полный сьют + ruff; Commit** — `feat(m9): multi-mount router support (composed template per mount)`

### Task 4: Беклог-хвост

**Files:**
- Modify: `src/codegraph/extractors/temporal_ext.py` (`execute_workflow` в `_START_WORKFLOW_CALLEES` — client-side спеллинг из M6-carry; тест), `tests/unit/test_calls_join.py` (seam-пин T1×T3 из M5-carry: CALLS в collision-family всегда на первую unsuffixed-ветку — сценарий из леджера M5-T3), закрыть проверкой доков nested-subscript пункт (уже задокументирован в M6 — подтвердить и вычеркнуть)
- [ ] **Step 1: RED → GREEN по трём пунктам; полный сьют + ruff; Commit** — `chore(m9): execute_workflow spelling, T1×T3 seam pin, backlog closeout`

### Task 5: realstack-леги + гейты

**Files:**
- Modify: `fixtures/realstack/` (external-лег: клиент с env-якорем на hostname ВНЕ воркспейса [env_values.yaml + идиома] → external-канал в golden/трассе; double-mount лег: второй mount воркер-роутера или gateway-агрегата → 2 канала в golden), `fixtures/realstack/golden/*` (аддитивно + санкционированные property-обновления), `tests/eval/test_m9_gate.py` (external-props пин + confidence-непenalty пин в трассе; multi-mount пара; compose-back props пин; прежние негативы)
- [ ] **Step 1: Фикстура+golden (протокол); падающий гейт → зелёный; все гейты M1–M9 + полный сьют + ruff; Commit** — `feat(m9): realstack external/multi-mount legs + gate`

### Task 6: Финал вехи + MCP-пилот бриф №4

- [ ] **Step 1:** Полные сьюты; сверка «неидеальностей» №3 с закрытием (external-confidence; карточки; multi-mount; хвост).
- [ ] **Step 2:** Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение.
- [ ] **Step 3:** Леджер `=== M9 ЗАВЕРШЁН ===`; память; push; **бриф №4 — MCP-пилот** (`docs/superpowers/pilot/WORK-MACHINE-MCP-PILOT-BRIEF.md`): полный `codegraph index` С эмбеддингами (e5+префиксы по README-рекомендации; первый прогон платит полную стоимость, повторные — кэш), `claude mcp add` регистрация, агентные сценарии (верификация №7: «проследи, что происходит после <реальный вход>» через trace_process; «где обрабатывается X» через search_code — RU-вопросы; «кто вызывает Y» через who_calls), сбор вердиктов/миссов двумя версиями отчёта; отчёт пользователю.

## Self-review плана

1. **Неидеальности №3 покрыты**: trace-confidence 0.50 от честных внешних → T1; карточка-локальный-путь → T2; NEXT_SEGMENT-kafka=0 — свойство среза (решение прежнее: producer-event_type — отложено, зафиксировано); 2 unlinked-сигнала — принципиально динамические (не чиним статикой — честность); DELETE-находка — команде сервисов.
2. **Беклог M9**: multi-mount → T3; compose-back → T2; execute_workflow/seam-pin/nested-subscript → T4; dispatch.*-остаток 167 — диагностика возможна только на рабочей машине → пункт в MCP-бриф (спросить агент: «SELECT dropped dst-префиксы»); stack-graphs — отложен решением пользователя (в прод рано ≠ перф-критично сейчас).
3. **Типы согласованы**: update_node_props (T2) ← T3 multi-mount path_templates-список; external-props (T1) ← T5 golden/гейт.
4. Плейсхолдеров нет; чтение-по-месту явно размечено.
