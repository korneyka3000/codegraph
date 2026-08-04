# M12 — retrieval-ранжирование, спайк stack-graphs, хвост: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи: решение пользователя 2026-08-05 (все 4 направления по приоритету) + MCP-пилот §4.1 (гранулярность top-k) + M12-беклог леджера + порог #3 мастер-плана (stack-graphs).

**Goal:** (1) Symbol-агрегация при ранжировании search_code — sibling-чанки класса перестают вытеснять метод-чанки из top-k (честное улучшение hit@8 без кручения метрики); (2) M12-мелочь (docstring/пины); (3) исследовательский спайк stack-graphs (цена/выгода замены резолвера — вердикт-документ, timebox); (4) бриф live-LLM-сессии для рабочей машины.

**Architecture:** Ранжирование: RRF уже rank-based — после фьюжна кандидаты агрегируются по symbol_id (лучший ранг представляет символ), финальный top-k — k РАЗЛИЧНЫХ символов (каждый — своим лучшим чанком + `sibling_chunks: N` аддитивно); eval-метрика (hit@k по (service, qualified_name)) НЕ меняется — улучшение произойдёт естественно, потому что Class-сиблинги схлопнутся в одну позицию и освободят слоты. Спайк — чистое исследование под timebox с обязательным вердикт-документом (SymbolResolver Protocol из M0 — готовая интеграционная поверхность; оценивать tree-sitter-stack-graphs зрелость для Python, CLI/биндинги, микро-бенч на фикстурах если запустимо). Live-LLM — бриф-документ (нарративные сценарии + чек-лист галлюцинаций), исполняет пользователь.

**Tech Stack:** ранжирование/мелочь — без новых зависимостей; спайк МОЖЕТ ставить stack-graphs-тулинг в scratchpad (НЕ в проект).

## Global Constraints

- Eval-метрика retrieval НЕ редактируется (evalx zero-diff); улучшение hit@k — только через честное ранжирование. Гейты M1–M10 зелёные; M3-гейт (retrieval) обязателен в T1.
- Агрегация аддитивна к ответу (sibling_chunks, прежние поля на месте); MCP-схема аддитивна; text/vector/hybrid — одна агрегация после фьюжна.
- Спайк не вносит код в src/ — только документ + scratchpad-эксперименты; вердикт обязан содержать: зрелость экосистемы, интеграционную поверхность (SymbolResolver), оценку объёма, микро-бенч или честное «не запустилось потому что», рекомендацию go/no-go/later.
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

Ранжирование: `query/retrieval.py` — RRF (k=60) по рангам text+vector ног → отсортированные хиты; чанки несут symbol_id/qualified_name/chunk_kind/enclosing_symbol (M10-T3). Пилотный кейс §4.1: `DocumentStorageClient` — top-1..8 ВСЕ чанки одного класса (sibling header/gap), целевой метод-чанк вытеснен; после агрегации класс займёт одну позицию. Метрика: evalx/retrieval_eval hit@k по (service, qualified_name) в top-k. M12-мелочь (леджер): vacuous-pass docstring-предложение (module_singletons — pre-M11 staging путь); re-export/star known-miss строка (provenance); `ids.containing_package`/`resolve_relative_import` собственные пины; multi-origin_relpaths тест. Спайк: SymbolResolver Protocol (`resolvers/base.py`), scip-раннер как образец интеграции; порог #3: 57s/правка из-за не-файл-инкрементального scip; stack-graphs = tree-sitter-based инкрементальный резолв (github/stack-graphs, tree-sitter-stack-graphs crate; Python-грамматика tree-sitter-stack-graphs-python?).

### Task 1: Symbol-агрегация ранжирования search_code

**Files:**
- Modify: `src/codegraph/query/retrieval.py` (пост-RRF агрегация по symbol_id: группировка кандидатов, представитель — лучший ранг, top-k по символам; ответ: прежние поля представителя + `sibling_chunks: int` [0 если един]), `src/codegraph/mcp/schemas.py` (аддитивно), `src/codegraph/mcp/server.py` (описание: «top-k различных символов»)
- Test: `tests/unit/test_retrieval.py` (агрегация: 5 чанков 2 символов → 2 позиции, лучший ранг представляет; sibling_chunks счёт; один-чанк-один-символ — прежний вид; k-семантика — k символов), MCP contract
- [ ] **Step 1: Падающие юниты → RED → реализация → GREEN.**
- [ ] **Step 2: M3-гейт** (реальные jina+falkordb: hit@3 6/6 обязан удержаться — фикстурные символы мелкие, сиблингов мало; сдвиг рангов golden-снапшота — по протоколу с санкцией) **+ полный сьют + ruff; Commit** — `feat(m12): symbol-aggregated ranking — k distinct symbols in search_code top-k (pilot 4.1)`

### Task 2: M12-мелочь

**Files:** `src/codegraph/parsing/module_singletons.py` (vacuous-pass предложение: pre-M11 staging + --incremental путь, fail-open-к-M10, самоизлечивается; re-export/star known-miss строка), `tests/unit/test_core_ids.py` (containing_package/resolve_relative_import пины), `tests/unit/test_module_singletons.py` (multi-origin_relpaths тест)
- [ ] **Step 1: RED→GREEN пины + доки; полный сьют + ruff; Commit** — `chore(m12): backlog docs+pins (vacuous-pass path, re-export miss, ids pins, multi-origin)`

### Task 3: Спайк stack-graphs (timebox, вердикт-документ)

**Files:**
- Create: `docs/superpowers/reports/2026-08-05-stack-graphs-spike.md` (коммитится)
- Scratchpad: любые эксперименты (cargo install tree-sitter-stack-graphs / python-грамматика / прогон на fixtures/services)
- [ ] **Step 1: Исследование** — зрелость (репо-активность, python-грамматика статус, известные ограничения кросс-файлового резолва Python [imports/attributes]), CLI/биндинги.
- [ ] **Step 2: Микро-бенч** — попытка прогнать на fixtures/services (index+query definition/references); честное «не запустилось: причина» — валидный исход.
- [ ] **Step 3: Вердикт-документ**: сопоставление со scip-выводом (полнота defs/refs на фикстурах), интеграционная оценка (SymbolResolver-адаптер: что маппится [defs/refs/spans], чего нет [типы?]), объём работ, риски, рекомендация go/no-go/later + условия пересмотра. Commit — `docs(m12): stack-graphs spike verdict`

### Task 4: Бриф live-LLM-сессии

**Files:** `docs/superpowers/pilot/WORK-MACHINE-LIVE-LLM-BRIEF.md` (коммитится)
- Содержание: сценарии нарративного слоя (те же 4 трассы + 3 who_calls + RU-поиск — но через ЖИВОЙ диалог с Claude Code, без прямых MCP-вызовов пользователем); чек-лист наблюдений: (а) выбирает ли агент правильный инструмент (trace vs search vs who_calls), (б) следует ли подсказкам описаний (service-фильтр; активности), (в) галлюцинации (утверждения, отсутствующие в tool-ответах), (г) озвучивает ли external-границы/confidence, (д) латентность диалога; протокол фиксации (полная стенограмма остаётся, SANITIZED-выжимка с вердиктами наружу).
- [ ] **Step 1: Написать бриф; Commit** — `docs(pilot): live-LLM narrative session brief`

### Task 5: Финал вехи

- [ ] **Step 1:** Полные сьюты + все гейты junit.
- [ ] **Step 2:** Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение.
- [ ] **Step 3:** Леджер `=== M12 ЗАВЕРШЁН ===`; память; push; отчёт пользователю (ранжирование: ожидание на бою hit@8 5→7/8 [два гранулярных промаха]; спайк-вердикт; live-LLM бриф готов к прогону).

## Self-review плана

1. **Все 4 направления пользователя**: ранжирование→T1 (приоритет 1), live-LLM→T4, stack-graphs→T3, мелочь→T2.
2. **Метрика не крутится**: T1 меняет РАНЖИРОВАНИЕ (продукт), метрика та же — улучшение честное; M3-гейт стережёт фикстурную регрессию.
3. **Спайк изолирован** (no-src, вердикт-документ обязателен).
4. Плейсхолдеров нет.
