# M4 — инкрементальность, hardening, пилот: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Шаги — checkbox (`- [ ]`). Мастер-спека: `docs/superpowers/specs/2026-07-12-codegraph-design.md` (веха M4) + консолидированный M4-беклог в `.superpowers/sdd/progress.md`.

**Goal:** `codegraph index --incremental` (правка файла → пере-анализ только затронутого, дамп == full-реиндекс), persistent-кэш эмбеддингов (повторный `index` → 0 обращений к провайдеру), разгрёб беклога ревью M0–M3, README, пилот на реальном OSS-репо (~50k+ LOC) с grep-baseline-сравнением.

**Architecture:** Инкрементальность двухуровневая: (A) сервис без изменений (tree_hash + конфиг-fingerprint совпали) — скип S3–S6/S8 целиком, staged-слои живы; (B) внутри изменённого сервиса — scip перегоняется полностью (scip-python не файл-инкрементален), но defs/refs-слой S4 переписывается (быстро), а дорогие S5 (parse+extract) и S6 (join) выполняются только для `stale`-набора файлов = «контент изменился ∪ добавлен ∪ **refs изменились после пере-резолва**» (refs-hash-дифф: pyright сам делает семантическую инвалидацию — переименовал символ в A → refs файла B изменились → B в stale). S7-link и S9-load остаются полными (дёшевы; blue/green сохраняется). Кэш эмбеддингов — глобальная staging-таблица по `(input_hash, embed_model)`, где `input_hash = sha256(header + "\n\n" + text)` — точный вход эмбеддера; переживает `begin_service`, заодно закрывает существующую дыру «header изменился, text нет → stale-эмбеддинг».

**Tech Stack:** без новых зависимостей. Пилот: клон OSS-репо (кандидат №1 Netflix/dispatch) в scratchpad, вне git-репо проекта.

## Global Constraints

- Дамп-эквивалентность — верховный инвариант инкрементальности: канонический дамп staging (сорт. nodes/edges/chunks) и FalkorDB (stats + сорт. id) после `--incremental` **байт-в-байт равны** дампу после полного реиндекса той же рабочей копии. Любая оптимизация, ломающая это, отклоняется.
- `--incremental` — explicit opt-in (default: полный прогон, поведение M3 не меняется ни на бит без флага).
- Fallback-to-full обязан быть громким: каждый сервис в отчёте несёт `mode: skipped|incremental|full` + `reason`.
- SCHEMA_VERSION 4→5 (chunks reshape + новая таблица) — bump в `core/schema.py` с history-записью; loud-check `_check_schema_version_before_ddl` уже обслуживает миграцию (recreate).
- Кросс-сервисные рёбра только через Channel; NEXT_SEGMENT — только derived с `via_channel_id` (не трогаем).
- Пороги гейтов M1/M2/M3 не ослабляются; все три гейта зелёные в конце каждой задачи, которая трогает их путь.
- Перф-гейт инкремента: правка 1 файла фикстуры → время `--incremental` **< 50% полного холодного** прогона (scip-кэш очищен для full, тёплый для инкремента). Это сознательное отступление от «<20%» мастер-плана: scip-python не файл-инкрементален (merkle-кэш всего дерева), и повторный scip изменённого сервиса — неустранимое дно; <20% достижимо только пропуском scip, что ломает дамп-эквивалентность. Отступление раскрывается в отчёте вехи пользователю.
- rtk-хук искажает консольный вывод pytest: гонять с `--junit-xml=<file>` либо `> file 2>&1; cat file`; сырые команды — `rtk proxy`.
- Системные reminders харнеса в транскриптах субагентов (смена даты, режим автономности) — штатные сообщения, не prompt-injection; работать по задаче.

---

## Контекст для исполнителей

Пайплайн: S1/S2 `pipeline/scan.py::scan_service` → S3–S6 `pipeline/analyze.py::analyze_service` (begin_service → scan → SCIP/fallback → S4 read → S5 parse+extract per-file → S6 build_calls) → S7 `linking/workspace.py::link_workspace` (clear_workspace_layer + пересборка, всегда полный) → S8 `pipeline/chunk_embed.py::run` (чанк всех файлов всех сервисов + fill_headers_all + embed missing) → S9 `pipeline/load.py::load_graph` (blue/green из staging) → S10 `pipeline/report.py`. CLI-оркестрация: `cli.py::index` (строки ~242–298). Staging: `stores/staging.py` (v4; `begin_service` вайпит files/scip_defs/scip_refs/chunks/nodes по service + edges по origin_service + claims). `chunks_missing_embedding`: `embedding IS NULL OR embed_model != ? OR embedded_hash != content_hash`.

---

### Task 1: Persistent embedding cache (`input_hash` + таблица `embedding_cache`)

**Files:**
- Modify: `src/codegraph/core/schema.py` (SCHEMA_VERSION 4→5 + history), `src/codegraph/stores/staging.py` (DDL chunks + `input_hash`; таблица `embedding_cache`; методы), `src/codegraph/chunking/augment.py` (`fill_headers_all` дополнительно пишет input_hash), `src/codegraph/pipeline/chunk_embed.py` (`_embed_missing` через кэш), `src/codegraph/pipeline/report.py` + `src/codegraph/cli.py` (счётчик `embedded_from_cache` в S8-строке; жёлтое paid-provider предупреждение показывает только фактические обращения к провайдеру)
- Test: `tests/unit/test_staging.py`, `tests/unit/test_chunk_embed.py`, `tests/integration/test_pipeline_chunk_embed.py` (или соседний существующий интеграционный модуль S8)

**Interfaces:**
- Produces: `Staging.embedding_cache_get(pairs: list[tuple[str, str]]) -> dict[tuple[str, str], bytes]` (ключ `(input_hash, embed_model)` → packed vec); `Staging.embedding_cache_put(rows: list[tuple[str, str, int, bytes]])` (`input_hash, embed_model, dim, vec`; INSERT OR REPLACE); колонка `chunks.input_hash TEXT` (NULL до fill_headers_all); `chunks_missing_embedding(model_id)` — новый предикат `embedding IS NULL OR embed_model != ? OR embedded_hash IS NULL OR embedded_hash != input_hash` (embedded_hash теперь хранит **input_hash на момент эмбеддинга** — ловит и смену header, чего v4-семантика по content_hash не видела); `set_embeddings` 4-е поле — input_hash строки.
- Consumes: `augment.augment_text(header, text)` — формат входа эмбеддера, единственный источник истины для input_hash.

- [ ] **Step 1: Падающие юнит-тесты staging** — v5-DDL: `embedding_cache` переживает `begin_service` (put → begin_service → get возвращает строку); `chunks.input_hash` пишется отдельным `set_input_hashes(rows: list[tuple[str, str]])` (chunk_id, input_hash — вызывается из fill_headers_all вместе с set_context_headers); `chunks_missing_embedding` флагует чанк с изменившимся input_hash при том же content_hash (header-change репро!); v4-shaped-db открывается с громким InvariantError (по образцу существующих v3/v2-тестов).
- [ ] **Step 2: RED** (`rtk proxy uv run pytest tests/unit/test_staging.py -q --junit-xml=...`)
- [ ] **Step 3: Реализация staging + schema.py bump** (history-запись: «4→5: chunks.input_hash — точный вход эмбеддера как ключ актуальности; embedding_cache(input_hash, embed_model, dim, vec) — глобальный кросс-ран кэш, НЕ вайпится begin_service; GC не делаем — staging.db одноразовый derived-кэш»). `input_hash = hashlib.sha256(augment_text(header or "", text).encode()).hexdigest()` — считает augment.py, staging хэшей не строит.
- [ ] **Step 4: Падающий юнит chunk_embed** — FakeEmbedder со счётчиком вызовов: первый `run` эмбеддит N; вручную вайпнуть чанки через `begin_service` + повторный полный `run` → `embed_batch` вызван 0 раз, все N из кэша, отчёт `{embedded: N, embedded_from_cache: N, provider_calls... }` — точный состав ключей: `embedded` (всего получило вектор) разделяется на `embedded_from_cache` и `embedded_fresh`; `reused` (уже были в chunks) как раньше.
- [ ] **Step 5: Реализация `_embed_missing`** — партиционирование missing: cache-hit (dim сверяется с embedder.dim; несовпадение = miss) → `set_embeddings` из кэша; miss → `embed_batch` → `set_embeddings` + `embedding_cache_put`. Порядок батчей детерминирован.
- [ ] **Step 6: report/cli** — S8-строка отчёта: `chunks: N (embedded: X fresh + Y cached, reused: Z)`; paid-предупреждение срабатывает от `embedded_fresh > 0` (не от суммарного embedded) — обновить его юнит.
- [ ] **Step 7: Интеграционный тест** (маркер как у соседей S8): два ПОЛНЫХ прогона pipeline по фикстурам с FakeEmbedder — второй прогон `embedded_fresh == 0`. Это делает истинной верификацию мастер-плана «повторный index → re-embedded: 0» (до сих пор — только in-run).
- [ ] **Step 8: GREEN всё + ruff; Commit** — `feat(m4): persistent embedding cache keyed by exact embed input (input_hash, model)`

### Task 2: Беклог-полиш (packaging, contains_pairs, MCP-лог)

**Files:**
- Modify: `pyproject.toml` (package-data), `src/codegraph/cli.py` (`TEMPLATE` через `importlib.resources`), `src/codegraph/evalx/retrieval_eval.py` (если TEMPLATE golden-вопросов читается путём — тоже на resources; проверить по факту), `src/codegraph/query/traverse.py` (contains_pairs: префильтр `chan:`-узлов до BFS-скана — найти по грепу `contains_pairs`), `src/codegraph/mcp/server.py` (лог «loading embedding model <id>…» перед lazy-инициализацией эмбеддера)
- Test: `tests/unit/test_cli_misc.py` (или соседний cli-тест), существующие traverse-тесты

**Interfaces:**
- Produces: `codegraph init` работает из wheel-инсталляции (без файлов репо рядом): `TEMPLATE`-контент читается `importlib.resources.files("codegraph") / "data" / "codegraph.example.yaml"` — файл ПЕРЕЕЗЖАЕТ в `src/codegraph/data/` (симлинк/копия в корне для людей остаётся), `[tool.hatch...]`/`[tool.setuptools...]` — по фактическому билд-бэкенду pyproject.

- [ ] **Step 1: Юнит на init-из-resources** — `python -c "import codegraph, importlib.resources; ..."` в тесте: контент читается без обращения к корню репо; CliRunner `init` в tmp_path выдаёт валидный YAML.
- [ ] **Step 2: Реализация packaging** (перенос example-yaml в package data; `uv build` + smoke-проверка wheel содержимого в тесте не нужна — достаточно resources-чтения).
- [ ] **Step 3: contains_pairs префильтр** — по существующим тестам traverse (поведение не меняется — только скоуп скана: выборка рёбер CONTAINS ограничивается src/dst с префиксом `chan:` там, где строится topic⇄event-карта); прогнать соответствующий юнит-модуль.
- [ ] **Step 4: MCP model-load лог** — logger.info до фабрики эмбеддера в lazy-пути; юнит: caplog фиксирует строку при первом вызове search_code и НЕ фиксирует при втором.
- [ ] **Step 5: GREEN + ruff; Commit** — `chore(m4): wheel-safe data files, contains_pairs prefilter, mcp embedder load log`

### Task 3: Fulltext OR-fallback (двухпроходный AND→OR)

**Files:**
- Modify: `src/codegraph/stores/falkordb/store.py` (`search_fulltext` Sym-нога + `search_text_chunks`: при 0 хитов на много-токенном запросе — второй проход с `|`-join токенов), `fixtures/golden/questions.yaml` (header: примечание про OR-fallback — смешанные RU/EN-запросы теперь получают text-ногу; чисто-кириллические на англ. корпусе — по-прежнему vector-only)
- Test: `tests/integration/test_falkordb_store.py` (falkordb-маркер), юнит на построение OR-запроса

**Interfaces:**
- Produces: та же сигнатура; поведение: `sanitized.split()` > 1 токена И первый проход пуст → повтор с `" | ".join(tokens)`; результат несёт признак в существующем формате (ничего в схемы не добавлять — mode_used у retrieval не меняется; OR — внутренняя деталь text-ноги). Одно-токенные и AND-успешные запросы — ноль изменений.
- RRF-шум от OR гасится фьюжном (rank-based, k=60) — обоснование в докстринге.

- [ ] **Step 1: Падающий falkordb-тест** — на фикстурном графе запрос `"создание OrderCreated заказа"` (смешанный, AND=0 из-за кириллических токенов) возвращает непустой список с чанком, содержащим OrderCreated; чисто-кириллический запрос по-прежнему `[]`; существующие AND-тесты не тронуты.
- [ ] **Step 2: RED → реализация → GREEN** (falkordb-suite целиком + юниты).
- [ ] **Step 3: Гейт M3 перепрогнать** (реальные scip+falkordb+emb; ranks Q1–Q5 не должны сдвинуться — их чисто-кириллические запросы OR не активируют по-прежнему-нулевым токен-хитам; Q6 AND-успешен — фоллбэк не срабатывает). Приложить junit.
- [ ] **Step 4: questions.yaml header-примечание; Commit** — `feat(m4): fulltext OR-fallback for mixed-language queries`

### Task 4: Scan-diff engine (`pipeline/diff.py`)

**Files:**
- Create: `src/codegraph/pipeline/diff.py`
- Modify: `src/codegraph/stores/staging.py` (метод `files_snapshot(service) -> dict[str, str]` relpath→sha256 — если `files_for_service` уже отдаёт пары, переиспользовать её)
- Test: `tests/unit/test_pipeline_diff.py`

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class ServiceDelta:
      added: tuple[str, ...]      # relpath, сорт.
      changed: tuple[str, ...]
      deleted: tuple[str, ...]
      unchanged: tuple[str, ...]
      @property
      def empty(self) -> bool: ...  # not (added or changed or deleted)

  def service_delta(staged: dict[str, str], scanned: list[tuple[str, str, int]]) -> ServiceDelta
  def config_fingerprint(svc: ServiceConfig, idioms: ServiceIdioms, active_idioms: frozenset[str]) -> str
  # sha256 канонического JSON: {"exclude": сорт., "idioms": idioms.model_dump(mode="json"),
  #  "active": сорт. active_idioms, "schema": SCHEMA_VERSION} — смена идиом/excludes/схемы
  #  меняет отпечаток → сервис нельзя скипнуть/инкрементить. path НЕ входит (переезд
  #  чекаута не инвалидирует). Хранение: staging.set_meta(f"svc_fingerprint:{name}", fp).
  ```
- Consumes: `scan_service` rows-формат `(relpath, sha256, size)`.

- [ ] **Step 1: Падающие юниты** — добавленный/изменённый/удалённый/без изменений; детерминизм сортировки; fingerprint меняется от idioms-правки и НЕ меняется от пути.
- [ ] **Step 2: RED → реализация → GREEN + ruff; Commit** — `feat(m4): scan diff engine (per-service file delta + config fingerprint)`

### Task 5: Инкрементальный analyze_service (stale-набор через refs-hash-дифф)

**Files:**
- Modify: `src/codegraph/pipeline/analyze.py` (параметр `incremental: bool = False`; ветка), `src/codegraph/stores/staging.py` (методы `refs_hash_by_file(service) -> dict[str, str]`; `delete_file_layer(service, relpaths: set[str], *, drop_calls_evidence: set[str])` — nodes/claims/chunks по `relpath IN`, edges по `origin_service=? AND evidence_file IN`; `clear_scip_layer(service)` — defs/refs/files целиком)
- Test: `tests/unit/test_staging.py` (новые методы), `tests/integration/test_analyze_incremental.py` (scip-маркер)

**Interfaces:**
- Produces: `analyze_service(..., incremental=False, prior_delta: ServiceDelta | None = None) -> dict` — отчёт получает `mode: "full"|"incremental"|"skipped"` и (для incremental) `stale_files: int`. Контракт ветки incremental (все прочие вызовы — байт-в-байт прежний full-путь):
  1. `prior_delta.empty` и fingerprint совпал → **skip**: НИКАКИХ записей в staging, отчёт `mode="skipped"` с counts из staging (SQL COUNT по service; defs/refs/nodes/edges/chunks).
  2. Иначе: снапшот `old_refs = refs_hash_by_file(svc)`; `clear_scip_layer` → `add_files` (свежий скан) → S3 scip полный (tree_hash изменился → реальный прогон; merkle-кэш обслуживает совпадение сам) → S4 read полный (defs/refs заново). `ref_dirty = {rp: old_refs.get(rp) != new_refs[rp] для существующих}`.
  3. `stale = set(changed) | set(added) | ref_dirty`; `dead = set(deleted)`.
  4. `delete_file_layer(svc, stale | dead, drop_calls_evidence=stale | dead)` — включая CALLS: evidence_file CALLS-ребра = файл call-site'а, то есть ровно тот, чей S6-прогон его пере-эмитит.
  5. S5 parse+extract и S6 build_calls — **только по stale** (facts строятся только для stale; build_calls принимает facts_by_file — передаётся stale-подмножество; def_symbol_lookup остаётся общесервисным — defs полные после S4).
  6. Service-узел пере-эмитится всегда (id стабилен, REPLACE — no-op).
- Корректность (это и есть «инвалидация по затронутым символам» мастер-плана, реализованная через pyright): CALLS/IMPORTS/доменные-claims файла X — функция ТОЛЬКО (контент X, refs X, defs сервиса). Контент не менялся и refs не изменились → staged-выход X валиден. Переименование символа в A меняет refs всех ссылающихся файлов → они в `ref_dirty` → пере-анализ. Хэш refs файла: sha256 отсортированных строк `(symbol, start_byte, end_byte, roles)`.
- Degraded (ScipRunError) в incremental-ветке → немедленный откат на полный путь (`mode="full"`, reason="degraded"): fallback-резолвер не даёт стабильного refs-диффа.

- [ ] **Step 1: Падающие юниты staging-методов** (refs_hash детерминирован и чувствителен к любой строке; delete_file_layer не трогает чужие relpath/сервисы и workspace-слой).
- [ ] **Step 2: RED → реализация staging → GREEN.**
- [ ] **Step 3: Падающий интеграционный тест (scip)** на фикстуре document_management: полный analyze → правка тела одной функции (tmp-копия сервиса!) → incremental analyze → (а) отчёт mode=incremental, stale_files=1; (б) канонический дамп staged nodes/edges/claims сервиса == дамп после ПОЛНОГО analyze той же правленой копии; (в) переименование функции, на которую ссылается другой файл → stale_files ≥ 2 (ref-dirty сосед) и дамп-эквивалентность снова держится; (г) удаление файла → его слой исчез, дамп == full.
- [ ] **Step 4: RED → реализация analyze-ветки → GREEN + ruff; Commit** — `feat(m4): incremental per-file analyze via refs-hash diff (skip / stale-set re-extract)`

### Task 6: Инкрементальный S8 (chunk только stale-файлов)

**Files:**
- Modify: `src/codegraph/pipeline/chunk_embed.py` (`run(..., changed_files: dict[str, set[str]] | None = None)`)
- Test: `tests/unit/test_chunk_embed.py`

**Interfaces:**
- Produces: `changed_files=None` → прежний полный проход (все вызыватели без изменений). Иначе: чанк-петля идёт только по `changed_files[svc.name]` (сервис отсутствует в dict → пропуск петли; chunks unchanged-файлов живы в staging — их begin_service в этой конфигурации не вайпил); `fill_headers_all` — **всегда полный** (headers зависят от графа; изменение сервиса A может поменять header чанка B — пересчёт дёшев, in-memory) + пересчёт input_hash; embed-фаза — прежний workspace-wide `chunks_missing_embedding` (header-изменившиеся чанки re-embed'ятся через кэш T1 — бесплатно при неизменном входе, честно при изменившемся).
- Consumes: T1 input_hash-семантика; T5 stale-наборы (передаст T7).

- [ ] **Step 1: Падающий юнит** — фикстурный staging с 2 «сервисами»: `changed_files={"a": {"x.py"}}` → upsert_chunks вызван только для a/x.py; чанки b-сервиса нетронуты (включая эмбеддинги); headers пересчитаны у всех; embed-фаза подхватила только чанки с изменившимся input_hash.
- [ ] **Step 2: RED → реализация → GREEN + ruff; Commit** — `feat(m4): incremental chunking (changed-files scope, full header refresh)`

### Task 7: CLI `--incremental` + эквивалентность + перф-гейт

**Files:**
- Modify: `src/codegraph/cli.py` (флаг `--incremental`; оркестрация: скан+дифф до analyze; сбор `changed_files`; отчёт), `src/codegraph/pipeline/report.py` (per-service `mode`/`reason`/`stale_files` в таблице)
- Create: `tests/eval/test_incremental_gate.py` (маркеры scip+falkordb)
- Test: `tests/unit/test_cli_index.py` (существующий соседний cli-модуль)

**Interfaces:**
- Produces: `codegraph index <ws> --incremental`: per-service решение — (а) fingerprint mismatch (или отсутствует: первый прогон) → full (begin_service-путь, как сейчас) + запись свежего fingerprint; (б) delta.empty + fingerprint ok → skipped; (в) иначе incremental (T5-ветка). `changed_files` для S8 = stale∪added per service (skipped-сервисы не входят); S7 link_workspace — всегда полный (уже так); S9/S10 без изменений. Отчёт: колонка mode + причина фоллбэка.
- Fallback-триггеры на весь прогон: staging schema-version mismatch ловится существующим loud-check (recreate — является полным прогоном по определению); отсутствие staging.db → full.

- [ ] **Step 1: Юнит cli-оркестрации** (фейки по образцу существующих cli-тестов: analyze_service получает ожидаемые incremental/prior_delta; skipped-сервис не попадает в changed_files; отчёт несёт mode).
- [ ] **Step 2: RED → реализация → GREEN.**
- [ ] **Step 3: Гейт эквивалентности** (`test_incremental_gate.py`, tmp-копия ВСЕХ трёх фикстурных сервисов + workspace.yaml на копии, FakeEmbedder):
  1. полный index → дамп A (staging: сорт. nodes/edges/chunks-строки без BLOB-сравнения эмбеддингов, но С их input_hash/embed_model; FalkorDB: stats + сорт. id узлов/рёбер).
  2. правка тела функции в orders_api → `--incremental` → дамп B; полный реиндекс с нуля (свежий staging.db) той же копии → дамп C. **assert B == C** (главный инвариант).
  3. изоляция: чанки document_management в B байт-идентичны A (включая эмбеддинг-BLOBы — SQL-выборкой).
  4. удаление файла + добавление файла → снова B == C.
  5. skipped-путь: повторный `--incremental` без правок → все сервисы mode=skipped, время < full.
- [ ] **Step 4: Перф-чек** в том же гейте: t_incremental (правка 1 файла, тёплые кэши) < 0.5 × t_full_cold (scip-кэш директория очищена). Замер `time.perf_counter` вокруг CLI-вызовов; записать оба времени в junit-property или print — цифры пойдут в отчёт вехи.
- [ ] **Step 5: GREEN (юниты + новый гейт + M1/M2/M3 гейты) + ruff; Commit** — `feat(m4): codegraph index --incremental (skip/incremental/full per service, dump-equivalence gated)`

### Task 8: Параллелизм S5-парсинга и API-эмбеддинга (с честным замером)

**Files:**
- Modify: `src/codegraph/pipeline/analyze.py` (парс facts_by_file через ThreadPoolExecutor(max_workers=os.cpu_count()); extract-петля остаётся последовательной — staging/lookups не потокобезопасны), `src/codegraph/pipeline/chunk_embed.py` (для НЕ-local провайдеров: до 4 конкурентных embed_batch; порядок set_embeddings детерминирован — результаты собираются по индексу батча), `src/codegraph/embedding/base.py` (флаг `concurrency_safe: bool = False` в протоколе; openai/voyage → True, local/fake → False)
- Test: `tests/unit/test_chunk_embed.py` (детерминизм порядка при конкурентных батчах — FakeEmbedder с искусственной задержкой, `concurrency_safe=True` в тесте)

**Interfaces:**
- Produces: build_file_facts — чистая функция (bytes → FileFacts), парс tree-sitter отпускает GIL в C — распараллеливание корректно; результат собирается в тот же `facts_by_file` dict по relpath (порядок обхода последующих петель не меняется — они идут по сорт. relpaths).
- Замер — часть задачи: до/после на фикстурах (3 сервиса) командой из шага 3; если выигрыш парс-фазы < 15% суммарного времени analyze — параллелизм ОТКАТЫВАЕТСЯ (коммит остаётся только с embed-конкурентностью), факт с цифрами — в отчёт задачи. Мастер-план требует «параллелизм S5/S8» — требование удовлетворяется измеренным решением, не слепым тредпулом.

- [ ] **Step 1: Юнит на детерминизм** конкурентного embed (порядок строк set_embeddings не зависит от задержек батчей).
- [ ] **Step 2: RED → реализация → GREEN.**
- [ ] **Step 3: Замер** — `rtk proxy uv run python -m timeit`-скрипт либо perf_counter-обвязка вокруг analyze_service на 3 фикстурах, 3 повтора, медиана; вывод в отчёт задачи. Решение по откату — по числам.
- [ ] **Step 4: ruff; Commit** — `perf(m4): parallel tree-sitter parse + concurrent API embed batches (measured)`

### Task 9: README + `--version`

**Files:**
- Create: `README.md` (корень)
- Modify: `src/codegraph/cli.py` (`--version` callback: importlib.metadata)
- Test: `tests/unit/test_cli_misc.py` (version smoke)

**Interfaces:**
- README-структура (RU, стиль репо): что это (3 абзаца: детерминированный граф + Channel-контракты + retrieval); Quickstart (docker compose up, uv sync, `codegraph init` → правка yaml → `index` → `serve` + регистрация в Claude Code — команда `claude mcp add -s user codegraph -- uv run --directory <repo> codegraph serve <ws.yaml>`); режимы деградации (no-scip → heuristic 0.6; no-embedder → граф без векторов; no-FalkorDB → index падает только на S9 с понятной ошибкой); `--incremental`; идиомы-как-конфиг (outbox-пример из fixtures/workspace.yaml); ограничения честно (RU-fulltext vector-only; scip не файл-инкрементален; FalkorDB in-memory + volume; языки — только Python).

- [ ] **Step 1: `--version` тест+реализация.**
- [ ] **Step 2: README** (выверить каждую команду копипастой в шелл — команды в README обязаны работать).
- [ ] **Step 3: ruff; Commit** — `docs(m4): README (quickstart, degradation modes, limits) + --version`

### Task 10: Пилот на реальном OSS-репо

**Files:**
- Create: `docs/superpowers/reports/2026-07-17-m4-pilot.md` (коммитится — deliverable вехи), `fixtures/pilot/` НЕ создавать — клон живёт в scratchpad вне репо
- Modify: только если пилот вскроет баги — отдельные `fix(m4-pilot):`-коммиты с тестами

**Interfaces (методология):**
- Репо-кандидаты по порядку: `Netflix/dispatch` (FastAPI+SQLAlchemy, ~100k LOC), fallback `mealie-recipes/mealie` (FastAPI), затем любой FastAPI-проект ≥30k LOC Python. Клон в scratchpad; `uv venv + uv pip install -e .`/requirements — best effort (10 мин), при провале зависимостей — честный degraded-путь (scip без сторонних типов всё равно резолвит first-party) с фиксацией в отчёте.
- Прогон: `codegraph index` zero-config (или минимальный yaml с excludes) → метрики: время по стадиям (перф-счётчики из отчёта), counts, % heuristic CALLS (порог мастер-плана: >20% — сигнал), % unresolved, degraded-статус. `codegraph doctor` до — приложить.
- Retrieval-проба: 6–8 golden-вопросов, написанных ПО КОДУ репо (реальные «где обрабатывается X»), `codegraph eval retrieval` → hit@3 таблица.
- **grep-baseline (методология спеки, Этап 0)**: на каждый вопрос — добросовестная grep-стратегия (rg-команды, которые написал бы человек без графа): зафиксировать (а) сколько команд до правильного файла, (б) найден ли ответ вообще, (в) для 2 «трассировочных» вопросов (кто вызывает / что происходит после) — сравнить с who_calls/trace. Таблица graph-vs-grep в отчёте. Честность: где grep выигрывает (точное имя символа известно) — записать, что выигрывает.
- Инкремент на масштабе: правка 1 файла → `--incremental`: времена full-cold / full-warm / incremental в отчёт; проверка порога «минуты, не десятки минут» (мастер-план: «~50k LOC за минуты»).
- Отчёт-структура: репо+размер; подготовка; метрики индексации; таблица retrieval; таблица grep-vs-graph; инкремент-времена; найденные баги+фиксы; выводы против порогов смены решений мастер-плана.

- [ ] **Step 1: Клон+подготовка+doctor.**
- [ ] **Step 2: Индексация+метрики** (при крэшах — минимальные fix-коммиты с тестами, каждый через обычный ревью-цикл).
- [ ] **Step 3: Golden-вопросы + eval + grep-baseline.**
- [ ] **Step 4: Инкремент-замер.**
- [ ] **Step 5: Отчёт; Commit** — `docs(m4): real-repo pilot report (index metrics, retrieval eval, grep baseline, incremental timings)`

### Task 11: Гейт вехи + финальное ревью

- [ ] **Step 1:** Полные сьюты: default (unit+falkordb) + `-m "scip or emb"` + новый incremental-гейт; junit-подтверждения; ruff.
- [ ] **Step 2:** Верификация чек-листа M4 мастер-плана против фактов (перф-цифры из T7/T10; отступление <20%→<50% — явно в отчёте).
- [ ] **Step 3:** Финальное whole-milestone ревью (fable) по review-package с полным диффом вехи; фикс-вейв при находках; подтверждение.
- [ ] **Step 4:** Леджер `=== M4 ЗАВЕРШЁН ===`, обновление памяти, отчёт пользователю.

---

## Self-review плана

1. **Покрытие мастер-M4**: dirty-set sha256 → T4; инвалидация по затронутым символам → T5 (refs-hash-дифф — та же семантика, механизм честнее); re-embed только изменённых → T1+T6 (input_hash строже спеки: ловит headers); `--incremental` + fallback full → T7; параллелизм S5/S8 → T8; README → T9; пилот+grep-baseline → T10; «время <20%» — сознательное отступление до <50% с обоснованием (Global Constraints) и раскрытием пользователю.
2. **Беклог финревью M3**: persistent cache (T1), packaging (T2), contains_pairs (T2), MCP-лог (T2), OR-семантика (T3), begin_service full-wipe→partial (T5 delete_file_layer), S8 dirty-set+double-parse (T6 — двойной парс уходит для unchanged-файлов по построению), blue/green сохраняется (решение: in-place отложен пост-M4 — load дёшев, подтвердит T10 на масштабе), resolve_selector-скан (T10: проверить на пилоте, чинить только при боли), no-index doctor-probe (НЕ делаем в M4 — вне критического пути, беклог).
3. **Типы согласованы**: ServiceDelta (T4) потребляется T5/T7; changed_files (T6) собирает T7; input_hash (T1) пересчитывает T6-полный-fill_headers_all.
4. **Плейсхолдеров нет**; каждый шаг — тест-сначала, коммит в конце задачи.
