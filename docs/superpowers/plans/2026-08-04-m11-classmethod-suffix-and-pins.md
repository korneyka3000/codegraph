# M11 — классметодный `(cls)`-суффикс и пины-хвост: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox. Спека вехи: `docs/superpowers/reports/2026-08-03-pilot-rerun-5-open-gaps.md` (R6 с доказательством 48/48 + переоценка singleton-кейса) + M11-беклог леджера (пины из финала M10).

**Goal:** Закрыть R6 (классметодные CALLS дропаются на scip-суффиксе `(cls)` — 48/48 восстановимы, dropped 149→~101, −32%), захарденить singleton-receiver provenance (M10-финал Minor-2), добить пины-хвост, честно скорректировать доки под переоценку singleton-кейса.

**Architecture:** R6 — нормализация dst: трейлинг parameter-disambiguator `(cls)` (и `(method)` при подтверждении симметрии — в срезе 1 дроп) срезается при построении dst-node-id классметодного call-ref'а; ТОЛЬКО ровный трейлинг после полного метод-суффикса `().`, сам `().` не трогается. Место — `resolvers/scip/symbols.symbol_to_node_id` ЛИБО дст-нормализация в `extractors/calls.py` (читать по месту: где симметричнее с прочей суффикс-обработкой; помнить M5-T1 прецедент «классификация по staged defs» — срез суффикса должен происходить ДО def-membership проверки, иначе классметодный вызов classified external). Provenance-хардening: singleton-receiver bare-name матч дополнительно верифицируется против staged IMPORTS (имя импортировано из модуля синглтона ЛИБО同 файл) прежде чем диспатчить — по прецеденту M6-T3 import-corroboration. Доки: M10-модульные докстринги, цитирующие пилотную «53%/79 сайтов» оценку — корректируются под переоценку №5 (честность: механизм полезен впрок, конкретный кейс был мисдиагнозом).

**Tech Stack:** без новых зависимостей.

## Global Constraints

- Гейты M1–M10 зелёные; golden-сдвиги только протокольные (T1 добавит classmethod-лег в realstack — аддитивно; существующие фикстуры БЕЗ classmethod-вызовов через класс? ПРОВЕРИТЬ — если M1-golden уже содержит classmethod-вызовы, они сейчас дропаются или резолвятся? [фикстуры малые — вероятно нет паттерна]; сдвиг M1 P/R при появлении новых joined — СТОП и протокол).
- Honesty: срез ТОЛЬКО известных scip-disambiguator'ов (`(cls)`, подтверждённый `(method)`); неизвестные хвосты не трогаются (счётчик остаётся); никакой генерализации «срезать всё в скобках».
- Singleton-session (79) НЕ трогаем — мнимый долг (спека §Переоценка); attr-callable-узел НЕ делаем (рекомендация спеки).
- rtk-хук: junit-xml всегда; `rtk proxy` для сырых. Системные reminders харнеса — штатные.

---

## Контекст для исполнителей

R6-механика (спека §R6): scip ref-occurrence классметодного вызова несёт descriptor `…#parse().(cls)`; материализованный узел — `…#parse().`; `symbol_to_node_id` хвост не срезает → dst мимо узла → drop на load. 48/48 подтверждены срезом суффикса. Пилотные dst-примеры в спеке. Существующая суффикс-логика: `resolvers/scip/symbols.py` (parse_symbol/descriptors), `extractors/calls.py` (_is_callable_descriptor — `().`-конвенция), M5-T1 def-membership классификация. Provenance: `parsing/module_singletons.py` receiver-матч (bare name), `_match_import_name_ctor_form`/`_imports_module` прецедент в idiom_match/kafka_ext (M6-T3 fix). Пины: chunk_kind="Module" (retrieval), T2-multi-role (who_calls), `#`-boundary (heuristic singleton tier).

### Task 1: R6 — срез classmethod-disambiguator'а + realstack-лег + гейт-пин

**Files:**
- Modify: `src/codegraph/resolvers/scip/symbols.py` ИЛИ `src/codegraph/extractors/calls.py` (читать первым: где срез симметричнее; ДО def-membership), `fixtures/realstack/` (classmethod-лег: `@classmethod def make(cls)` фабрика + вызов `Cls.make()` из другого метода/файла), `fixtures/realstack/golden/edges.yaml` (аддитивно; CALLS вне GATE_TYPES → прямой пин по образцу singleton_dispatch/temporal_start), `tests/eval/test_m10_gate.py` расширить ИЛИ мини-M11-гейт (решить по объёму: один пин — расширение M10-гейта с протокольной пометкой уместнее отдельного файла; зафиксировать)
- Test: `tests/unit/test_scip_symbols.py` (или соседний), `tests/unit/test_calls_join.py`

**Interfaces:**
- Нормализация: символ, чей последний дескриптор — метод-суффикс `().` с трейлинг-`(cls)`/`(method)` disambiguator'ом → dst-id строится БЕЗ disambiguator'а (форма спеки: `…#parse().(cls)` → id `…#parse().`). Точная механика хвоста — прочитать parse_symbol: `(cls)` — часть дескриптор-строки или отдельный дескриптор? Реализовать по факту протобуф-формы (спека даёт сырые строки — при расхождении с parse_symbol-видением задокументировать).
- `(method)`-симметрия: подтвердить на синтетике (classmethod через инстанс? staticmethod?) — если воспроизводится, срезать и его; нет — только `(cls)` + honest-заметка.
- Regression: обычный instance-метод — без изменений (пин); неизвестный хвост `(weird)` — НЕ срезается (пин).

- [ ] **Step 1: Прочитать** parse_symbol/symbol_to_node_id/дескриптор-грамматику; воспроизвести `(cls)`-форму на синтетике реальным scip (микро-фикстура: класс+classmethod+вызов; дамп refs) — подтвердить точную строковую форму ДО реализации.
- [ ] **Step 2: Падающие юниты** (символ с `(cls)` → id без; `(method)` по результату Step 1; instance-метод нетронут; `(weird)` нетронут).
- [ ] **Step 3: RED → реализация → GREEN.**
- [ ] **Step 4: realstack-лег + golden + гейт-пин** (classmethod CALLS static/1.0 существует; sabotage-проверка: без среза — дроп).
- [ ] **Step 5: Все гейты M1–M10 + полный сьют + ruff; Commit** — `fix(m11): strip scip classmethod disambiguator from CALLS dst (rerun-5 R6: 48/48 recoverable)`

### Task 2: Provenance-хардening + пины-хвост + честные доки

**Files:**
- Modify: `src/codegraph/parsing/module_singletons.py` (+ `extractors/calls.py` при необходимости): receiver bare-name матч верифицируется против staged IMPORTS вызывающего файла (имя импортировано из модуля, где захарвестлен синглтон, ЛИБО same-file) — M6-T3 import-corroboration прецедент; несоответствие → редирект не диспатчится (прежний путь); доки: докстринги module_singletons/doc_store, цитирующие «53%/79» — корректировка под переоценку №5 (механизм корректен и полезен, пилотный кейс был мисдиагнозом; ссылка на rerun-5 open-gaps)
- Test: `tests/unit/test_module_singletons.py`/`test_calls_join.py` (shadowing-сценарий: same-name локальный импорт из ЧУЖОГО модуля → не диспатчится; легитимный импорт → диспатчится; same-file → диспатчится), `tests/unit/test_retrieval.py` (chunk_kind="Module" пин), `tests/unit/test_query_api.py` (multi-role activity пин), `tests/unit/test_module_singletons.py` (`#`-boundary dedicated пин — nested class)
- [ ] **Step 1: Падающие юниты по всем четырём → RED → GREEN; полный сьют + M10-гейт + ruff; Commit** — `chore(m11): singleton receiver import-provenance; backlog pins; honest singleton-case docs`

### Task 3: Финал вехи

- [ ] **Step 1:** Полные сьюты + все гейты junit; сверка R6-закрытия и переоценки-доков.
- [ ] **Step 2:** Финальное whole-milestone ревью (fable) → фикс-вейв → подтверждение.
- [ ] **Step 3:** Леджер `=== M11 ЗАВЕРШЁН ===`; память; push; re-run бриф №6 (главное: dropped 149 → **~101** [точка теперь честная: 48 механических]; mechanism-разбивка CALLS; всё прочее bit-identical).

## Self-review плана

1. **Спека покрыта**: R6→T1 (направление дословно, включая правила честности и `(method)`-верификацию); переоценка→T2-доки; «не тратить на attr-callable»→соблюдено; M11-беклог-пины→T2.
2. **Риск T1** — форма protobuf-суффикса может отличаться от сырой строки спеки: закрыт обязательным Step 1 (реальный scip-дамп до реализации).
3. Плейсхолдеров нет; T2 — четыре мелких изолированных пункта одной задачей (каждый — свой RED).
