# M5 — закрытие беклога: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox (`- [ ]`). Источники: консолидированный M5-беклог в `.superpowers/sdd/progress.md` (9 пунктов) + пилот-отчёт `docs/superpowers/reports/2026-07-18-m4-pilot.md` (§7.1/§7.2/§7.3, §10).

**Goal:** Закрыть все actionable-пункты M5-беклога: Bug B (~43pp потерянных CALLS), Bug A (недетерминированный hit@k → exact-режим eval), id-коллизии условных классов, per-origin shared-рёбра (T7 residual gap), trace-свёртка, RU-retrieval эксперимент, тест-гигиена и мелкий polish. После M5 — пилот на реальных сервисах пользователя.

**Architecture:** Bug B решается заменой критерия first-party в S6: не `parsed.package != service` (ненадёжен при `--project-name`), а «callee-символ имеет definition в staged scip_defs сервиса» — точный, независимый от имён пакетов критерий (defs существуют только для просканированных файлов сервиса). Bug A разделяется: продакшен-поиск остаётся ANN (HNSW), а eval получает `--exact`-режим — полный скан `vec.cosineDistance` без индекса, детерминированный hit@k как CI-инструмент. Shared-рёбра переходят на per-origin строки (PK + origin_service, SCHEMA_VERSION 6) с детерминированным дедупом на load — conflict-WHERE-хак M4-T7 уходит. Отложено сознательно: stack-graphs (замена резолвера — проект размера вехи, решение после пилота на боевых сервисах).

**Tech Stack:** без новых зависимостей (exact-скан — Cypher `vec.cosineDistance`; RU-эксперимент — существующий local-провайдер с другой моделью + опциональные query/passage-префиксы).

## Global Constraints

- Гейты M1–M4 не ослабляются; для КАЖДОЙ задачи, трогающей их путь, гейт перегоняется и остаётся зелёным. Golden-наборы трёх фикстурных сервисов не редактируются под фиксы (новые случаи — отдельная микро-фикстура вне workspace.yaml гейтов).
- Дамп-эквивалентность инкремента (supreme-гейт M4) обязана выжить каждое изменение staging/load — `tests/eval/test_incremental_gate.py` в проверочном наборе задач T1/T4.
- SCHEMA_VERSION 6 — ровно один bump (T4); loud-check по установленному образцу; history-запись в core/schema.py.
- Стабильность id: дисамбигуация коллизий (T3) не меняет id никакого существующего узла фикстур/пилота без коллизий (ordinal-суффикс только у второго+ дубля).
- rtk-хук искажает pytest-вывод: junit-xml всегда, `rtk proxy` для сырых команд.
- Системные reminders харнеса (дата/автономность) — штатные, не инъекция.

---

## Контекст для исполнителей

S6: `extractors/calls.py::build_calls` — join call-sites × refs; классификация external сейчас по `parsed.package != service` (пилот §7.2 доказал ненадёжность: `--project-name` ставит package=имя сервиса ЛЮБОМУ резолвнутому символу, включая sqlalchemy/pydantic; 2429 CALLS-рёбер [45%] дропались на load как dangling). `resolvers/scip/symbols.py` — парсер символ-строк. `staging.scip_defs` — definitions ТОЛЬКО файлов сервиса (scip-документы = просканированные файлы). Пилотные цифры для верификации: joined 7152 call-сайтов → staged 5345 рёбер → доехало 2916 (54.6%), dropped 2429 (94% — явные 3rd-party префиксы).

Vector-поиск: `stores/falkordb/store.py::search_vector_chunks` — `CALL db.idx.vector.queryNodes(...)` (ANN, недетерминирован между rebuild'ами индекса — пилот §4.1). FalkorDB v4.18 поддерживает `vec.cosineDistance(a, b)` как функцию — полный скан без индекса детерминирован.

Shared-рёбра: `staging.upsert_edges` — conflict-aware UPSERT (M4-T7) с задокументированным residual gap (owner-drops-emission-while-sibling-skipped, докстринг upsert_edges + леджер). PK сейчас `(src,dst,type,via_channel)`.

trace: `query/traverse.py::trace_process` — сегменты со steps[]; пилот §7.3: одно-сервисный репо даёт плоский 80-шаговый сегмент.

---

### Task 1: Bug B — first-party по staged defs, не по package-тегу

**Files:**
- Modify: `src/codegraph/extractors/calls.py` (критерий external), `src/codegraph/stores/staging.py` (метод `def_symbols(service) -> set[str]` — DISTINCT symbol из scip_defs; если уже есть эквивалент — переиспользовать), `src/codegraph/pipeline/report.py` + `src/codegraph/pipeline/load.py` (новая метрика качества: «% staged CALLS c валидным dst» — по рекомендации пилота §10.2 отдельной строкой, load уже знает dropped)
- Create: `fixtures/services/micro_extlib/` НЕ создавать — вместо этого юнит-эмуляция (см. Step 1)
- Test: `tests/unit/test_calls_extractor.py` (или соседний существующий модуль build_calls), `tests/unit/test_pipeline_report.py`

**Interfaces:**
- Produces: `build_calls(..., def_symbols: set[str] | Callable[[], set[str]])` — новый обязательный источник истины: callee-символ классифицируется external ⇔ (не `local N` И символ ∉ def_symbols сервиса). `parsed.package`-критерий УДАЛЯЕТСЯ (не «дополняется» — двойной критерий сохранил бы ложные joined при совпадении имён). local-символы — всегда first-party (как сейчас). Счётчики `calls_joined`/`calls_external` начинают отражать истину; report получает `staged_calls_with_valid_dst_pct` (из load-статистики: written/(written+dropped) по типу CALLS).
- Consumes: `staging.local_def_symbols` (есть), новый `def_symbols(service)` — hoisted один раз на сервис (не per-file SQL).

- [ ] **Step 1: Падающие юниты** — фейковый staging (паттерн существующих тестов build_calls): (а) callee-символ с def в сервисе → joined-ребро (как раньше); (б) callee-символ РЕЗОЛВНУТЫЙ (не local, полный дескриптор `scip-python python SVC ver \`sqlalchemy.orm.session\`/Session#query().`) но БЕЗ def в staged defs → external (счётчик), ребро НЕ эмитится — это ядро Bug B: package в символе может быть ЛЮБЫМ, включая имя самого сервиса; (в) local-символ → first-party всегда; (г) degraded fallback-путь (структурные символы, все с defs) — не меняется.
- [ ] **Step 2: RED** (junit).
- [ ] **Step 3: Реализация** — calls.py: заменить package-сравнение на defs-lookup; докстринг с пилотным обоснованием (§7.2: `--project-name` ставит package=сервис любому резолвнутому символу). staging: `def_symbols`. analyze.py: прокинуть hoisted set (по образцу local_defs_for_file @cache).
- [ ] **Step 4: GREEN юниты; M1-гейт** (`rtk proxy uv run pytest -m scip tests/eval/test_calls_gate.py -q --junit-xml=...`) — фикстурные CALLS все с defs → P/R не сдвигаются; при сдвиге — СТОП, диагностика (не подгонка golden).
- [ ] **Step 5: report-метрика** — юнит: report dict несёт `staged_calls_with_valid_dst_pct`; print-путь с .get-дефолтом (прецедент T1-M4).
- [ ] **Step 6: Полные сьюты + M2/M4-гейты + ruff; Commit** — `fix(m5): first-party CALLS classification by staged defs, not scip package tag (pilot Bug B)`

### Task 2: Bug A — детерминированный `--exact` режим eval retrieval

**Files:**
- Modify: `src/codegraph/stores/falkordb/store.py` (`search_vector_chunks_exact` — полный скан `MATCH (c:Chunk) WHERE c.embedding IS NOT NULL ... ORDER BY vec.cosineDistance(c.embedding, vecf32($q)) ASC LIMIT k` + тот же service-фильтр что у ANN-версии), `src/codegraph/query/retrieval.py` + `src/codegraph/query/api.py` (параметр `exact: bool = False` до search_code/hybrid — vector-нога через exact-метод), `src/codegraph/cli.py` (`eval retrieval --exact`), `src/codegraph/evalx/retrieval_eval.py` (прокинуть)
- Test: `tests/integration/test_falkordb_store.py` (falkordb-маркер), `tests/unit/test_retrieval.py`

**Interfaces:**
- Produces: `search_vector_chunks_exact(query_vec, k, service=None) -> list[ScoredChunk]` — score = cosine similarity (пересчитать из distance: `1 - dist`, согласовать со шкалой ANN-версии — прочитать как ANN возвращает score и выдержать ту же семантику); полный скан детерминирован. `--exact` в CLI документирован: «для CI/сравнений; на больших графах медленнее ANN».
- MCP/продакшен-поиск НЕ трогается (ANN); Bug A остаётся задокументированным свойством ANN (README «Ограничения» уже упоминает — дополнить ссылкой на --exact).

- [ ] **Step 1: Падающий falkordb-тест** — на фикстурном графе: два подряд вызова exact с одним вектором → байт-идентичные списки id и score; exact top-k ⊇ разумности (запрос вектором чанка → сам чанк rank 0).
- [ ] **Step 2: RED → реализация → GREEN** (falkordb-suite).
- [ ] **Step 3: Юнит retrieval** — exact=True прокидывается до store-вызова (фейк-store фиксирует вызов exact-метода); RRF-слияние не меняется.
- [ ] **Step 4: CLI + README-примечание; полные сьюты + ruff; Commit** — `feat(m5): deterministic --exact vector mode for eval retrieval (pilot Bug A)`

### Task 3: id-коллизии условных одноимённых классов (пилот §7.1)

**Files:**
- Modify: `src/codegraph/extractors/python_core.py` (детекция дублей id на эмиссии: within-file seen-set; второй+ дубль получает ordinal-суффикс `~2`, `~3` в id), `src/codegraph/core/ids.py` (если суффикс-хелпер уместнее там)
- Test: `tests/unit/test_python_core_extractor.py`

**Interfaces:**
- Produces: при повторной эмиссии узла с уже занятым в этом файле id (scip-резолв обеих веток на один символ ИЛИ структурный дубль) — второй узел получает `id + "~2"` (стабильно по порядку появления в файле; сдвиг строк НЕ меняет id; добавление ещё одной ветки выше — меняет только суффиксные). CONTAINS-иерархия строится по фактическим (возможно суффиксным) id; методы дублирующихся классов дисамбигуируются тем же механизмом (их id тоже дублируются). Узлы без коллизий — id байт-в-байт прежние (констрейнт стабильности).
- Побочный эффект закрывается: `chunk_embed._symbol_ids_for_file` перестаёт видеть span-конфликт (каждый def → уникальный узел) → файл больше не выпадает из чанкинга.

- [ ] **Step 1: Падающий юнит** — источник с `if/elif` одноимёнными классами (по образцу пилотного `Secret`, оба с методами): все def'ы получают УНИКАЛЬНЫЕ id; вторая ветка — суффикс `~2`; методы обеих веток присутствуют; CONTAINS корректен per-ветка; узлы файла без коллизий — прежние id (снапшот-сравнение с текущим поведением на не-коллизирующем файле).
- [ ] **Step 2: RED → реализация → GREEN.**
- [ ] **Step 3: Интеграционная проверка каскада** — юнит chunk_embed: файл с коллизией больше не скипается (span-match проходит).
- [ ] **Step 4: Полный сьют + M1-гейт + ruff; Commit** — `fix(m5): ordinal-disambiguate colliding def ids from mutually-exclusive branches (pilot Bug 7.1)`

### Task 4: Per-origin shared-рёбра (закрытие T7 residual gap)

**Files:**
- Modify: `src/codegraph/core/schema.py` (SCHEMA_VERSION 6 + history), `src/codegraph/stores/staging.py` (edges PK → `(src,dst,type,via_channel,origin)`; колонка `origin TEXT NOT NULL DEFAULT ''` — переименованный смысл origin_service c NULL→''; upsert_edges: честный INSERT OR REPLACE по новому PK, conflict-WHERE-хак и KNOWN RESIDUAL GAP удаляются; begin_service/delete_file_layer/clear_workspace_layer — без изменений семантики [фильтры по origin уже точны]), `src/codegraph/pipeline/load.py` (iter_edges → дедуп групп `(src,dst,type,via_channel)` перед батчами: приоритет resolution static>dynamic>heuristic, затем max confidence, затем лексикографически первый origin — детерминизм; докстринг), `src/codegraph/stores/staging.py::update_edge_props` (ключ теперь неоднозначен между origins — обновлять ВСЕ строки группы [CALLS temporal_start-марк семантически принадлежит паре, не origin]; NEXT_SEGMENT-guard остаётся)
- Test: `tests/unit/test_staging.py`, `tests/unit/test_pipeline_load.py`, `tests/eval/test_incremental_gate.py` (сценарий residual gap — НОВЫЙ под-кейс)

**Interfaces:**
- Produces: каждый эмиттер владеет СВОЕЙ строкой shared-ребра; удаление владельцем своей эмиссии при skipped-соседе оставляет строку соседа → граф корректен (гейт-кейс: kafka CONTAINS topic→event от producer- И consumer-сервиса; producer удаляет свой идиом → инкремент → ребро живо от consumer'а; consumer тоже удаляет → ребро исчезает). Дедуп на load детерминирован и задокументирован.
- v5-staging открывается с громким recreate (образец v4→v5).

- [ ] **Step 1: Падающие юниты staging** — оба origin'а сосуществуют строками; begin_service одного origin'а не трогает строку другого; v5-shaped-db → InvariantError; mirror-ordering тесты M4 переписываются под новую семантику (обе строки живут — БЕЗ ослабления: новый инвариант сильнее старого «первый выигрывает»).
- [ ] **Step 2: RED → реализация staging+schema → GREEN.**
- [ ] **Step 3: Падающий юнит load-дедупа** — группа из 2 строк (разные origin, разные confidence) → одно ребро с приоритетной resolution/max confidence; детерминизм при перестановке порядка.
- [ ] **Step 4: RED → реализация load → GREEN.**
- [ ] **Step 5: Гейт-кейс residual gap** в test_incremental_gate.py (сценарий выше, через реальный CLI) + ПОЛНЫЙ прогон supreme-гейта (дамп-эквивалентность обязана выжить схемный переход).
- [ ] **Step 6: Все гейты M1–M4 + полный сьют + ruff; Commit** — `feat(m5): per-origin shared edge rows (closes M4-T7 residual gap), deterministic load dedup`

### Task 5: trace-свёртка линейных цепочек (пилот §7.3)

**Files:**
- Modify: `src/codegraph/query/traverse.py` (пост-обработка сегмента: подряд идущие steps без ветвлений/ролей/exit-точек схлопываются в `{"collapsed": N}` при len(steps)>15; параметр `compact: bool = True` в trace_process с прокидкой из MCP/CLI `--full` для отключения), `src/codegraph/cli.py` (рендер «⋯ N внутренних вызовов» в text/mermaid), `src/codegraph/mcp/schemas.py` (опциональное поле collapsed в step-схеме)
- Test: `tests/unit/test_traverse.py`, CLI snapshot-тест

**Interfaces:**
- Produces: сегмент ≤15 шагов — байт-в-байт прежний вывод (фикстурные трейсы не меняются — гейт M2 нетронут); длинный линейный хвост → первые 3 + collapsed-маркер + последние 2 шага цепочки; шаги с ролями (RouteHandler/Consumer/Workflow/Activity), ветвлениями (>1 исходящего в сегменте) и exit-шаги НЕ схлопываются никогда.

- [ ] **Step 1: Падающий юнит** — синтетический сегмент 40 линейных шагов → collapsed-структура; сегмент 10 шагов → нетронут; шаг с ролью посреди цепочки — разрывает свёртку.
- [ ] **Step 2: RED → реализация → GREEN; M2-гейт (трейсы фикстур неизменны); Commit** — `feat(m5): collapse long linear chains in trace segments (single-service ergonomics)`

### Task 6: RU-retrieval эксперимент (порог #2 — первый шаг)

**Files:**
- Modify: `src/codegraph/embedding/base.py` (`embed_query(text) -> list[float]` — дефолт `embed_batch([text])[0]`), `src/codegraph/embedding/local.py` (опциональные `query_prefix`/`passage_prefix` из конфига: passage-префикс в embed_batch [S8-путь], query-префикс в embed_query), `src/codegraph/config/models.py` (2 поля в embedding-конфиге), `src/codegraph/query/retrieval.py` (запросная нога через embed_query)
- Create: `docs/superpowers/reports/2026-07-18-m5-ru-retrieval-experiment.md` (коммитится)
- Test: `tests/unit/test_local_embedder.py` (префиксы применяются/не применяются), `tests/unit/test_retrieval.py` (embed_query используется)

**Interfaces:**
- Produces: обратносовместимо (префиксы пустые по умолчанию → поведение прежнее; embed_query дефолтен). Эксперимент (методология): пилотный клон/staging из scratchpad (жив с M4-T10); re-embed пилота моделью `intfloat/multilingual-e5-base` С префиксами `query: `/`passage: ` (канон e5) через существующий конфиг; `eval retrieval --exact` (T2!) на тех же 8 пилотных вопросах; таблица jina-code-vs-e5 hit@3/hit@8 (jina-числа пересчитать exact-режимом для честного сравнения — ANN-числа пилота несравнимы). Вывод в отчёт; если e5 ≥ +2 вопроса на hit@8 — README-рекомендация «для русскоязычных команд» с конфиг-сниппетом (НЕ смена дефолта).
- Модель тянется через sentence-transformers (та же инфраструктура; ~1GB — терпимо).

- [ ] **Step 1: Падающие юниты префиксов/embed_query → RED → реализация → GREEN.**
- [ ] **Step 2: Эксперимент** (клон/staging пилота; два прогона exact-eval; при мёртвом scratchpad — повторный клон и index по методике M4-T10, времена уже известны).
- [ ] **Step 3: Отчёт + README-сниппет (если порог достигнут); полный сьют + ruff; Commit** — `feat(m5): query/passage prefixes + embed_query; RU-retrieval experiment report (e5 vs jina-code, exact mode)`

### Task 7: Polish-хвост

**Files:**
- Modify: `tests/unit/conftest.py` или локальные conftest cli-тестов (мок make_embedder → сьют −120s при установленном local-emb; проверить implementer-нотой T9: test_cli_m1b/test_cli_index), `src/codegraph/pipeline/chunk_embed.py` (Meta при `--incremental --no-embed`: если в workspace остались живые эмбеддинги [SQL EXISTS] — НЕ затирать embed_model/dim; M4-Minor-3 решение), `src/codegraph/cli.py` (--questions help: wheel-нейтральная формулировка), `tests/integration/test_falkordb_store.py` (OR-fallback × kinds/service-фильтр прямой тест — M4-T3 minor), `src/codegraph/cli.py::doctor` (probe: наличие vector-индекса Chunk в целевом графе → warning с подсказкой re-index; M3-беклог «no-index маркер → doctor probe»)
- Test: соответствующие модули

- [ ] **Step 1: Мок эмбеддера в cli-тестах** — замер до/после (сьют быстрее; ни один тест не потерял смысл — они тестируют CLI-оркестрацию, не модель).
- [ ] **Step 2: Meta-семантика** — юнит: incremental+no-embed при живых векторах сохраняет Meta; full+no-embed затирает (прежнее).
- [ ] **Step 3: OR×kinds тест; help-текст; doctor-probe** (+юнит probe с фейк-store).
- [ ] **Step 4: Полный сьют + ruff; Commit** — `chore(m5): test hygiene (embedder mock), incremental no-embed meta, doctor vector-index probe, misc polish`

### Task 8: Гейт вехи + финал

- [ ] **Step 1:** Полные сьюты default + `-m "scip or emb"` (все гейты M1–M4 + новые кейсы) junit; ruff.
- [ ] **Step 2:** Сверка: все 9 пунктов беклога закрыты или явно перенесены (stack-graphs — единственный сознательный перенос, решение после пилота).
- [ ] **Step 3:** Финальное whole-milestone ревью (fable) по review-package; фикс-вейв; подтверждение.
- [ ] **Step 4:** Леджер `=== M5 ЗАВЕРШЁН ===`, память, отчёт пользователю (числа Bug B до/после на пилотном staging — если клон жив, перепрогнать index и показать восстановленные CALLS; иначе честно юнит-уровнем).

---

## Self-review плана

1. **Покрытие беклога (9 пунктов):** Bug B → T1; Bug A → T2 (eval-детерминизм; ANN-продакшен задокументирован); stack-graphs → сознательный перенос (размер вехи; после боевого пилота); retrieval-качество → T6 (первый измеримый шаг); id-collision → T3; trace-свёртка → T5; unmocked HF → T7; per-origin rows → T4; --no-embed Meta → T7.
2. **Порядок:** T2 до T6 (эксперимент требует exact); T1/T4 оба трогают staging/load — последовательно, SCHEMA_VERSION bump только в T4; T5 независим.
3. **Типы:** def_symbols (T1) — set[str] hoisted; exact-параметр (T2) прокинут cli→evalx→api→store; ordinal-суффикс (T3) — только новые дубли; PK-происхождение (T4) согласовано с load-дедупом.
4. **Плейсхолдеров нет; каждый шаг — тест-сначала; гейты в каждой затрагивающей задаче.**
