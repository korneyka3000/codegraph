# M3: Retrieval (chunker, embeddings, hybrid search) + early-M3 hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Веха M3 мастер-плана. База: M2 завершён на 8312cb8 (гейт 6/6 P=R=1.0, живой trace). Тесты в брифах — контракт; build-to-interface по паттернам M1/M2.

**Goal:** Семантический retrieval поверх графа: AST-чанкер (чанк ↔ symbol N:1), контекстная аугментация с graph-позицией, pluggable-эмбеддеры (local jina-code / OpenAI / Voyage), Chunk-узлы + HNSW vector index + fulltext в FalkorDB, RRF-гибрид, `search_code` (9-й MCP-инструмент) и полноценный `find_entrypoint`. Плюс обязательный early-M3 список финального ревью M2 (PART_OF_PROCESS rework, NEXT_SEGMENT PK, confidence-таблица и пр.). Гейт M3: search_code hit@3 по golden-вопросам на реальной модели; re-embedded==0 на повторном индексе; contract 9 инструментов.

**Architecture:** S8 = workspace-стадия ПОСЛЕ S7 (аугментация читает graph-позицию из staged рёбер): chunk → augment → embed (кэш по (content_hash, model)) → staging chunks-таблица → load: Chunk-узлы (label Chunk, embedding vecf32) + vector/fulltext DDL + Meta-узел (embed_model/dim). Retrieval: RRF(k=60) над vector-top-N и fulltext-top-N чанков; find_entrypoint = RRF(Sym-fulltext, chunk-vector→symbol-агрегация).

**Tech Stack:** extras: `local-emb` (sentence-transformers), `openai`, `voyage`. Реальная локальная модель — jinaai/jina-embeddings-v2-base-code (спека, раздел H); в юнитах — FakeEmbedder; интеграции с моделью — маркер `emb`.

## Global Constraints

- Коммиты `feat(m3)/fix(m3)/test(m3)/chore(m3)`; uv.lock вместе с pyproject; после каждой задачи.
- **Early-M3 список финального ревью M2 — обязателен** (леджер, задачи T1/T2): NEXT_SEGMENT PK миграция; PART_OF_PROCESS rework; нормативная confidence-таблица; v→v3 InvariantError-путь; evalx mechanism-фильтр; trace-селектор из графа.
- SCHEMA_VERSION → 3 (одна миграция на веху, в T1; существующие staging — громкая инвалидация ПРАВИЛЬНЫМ исключением).
- Весь Cypher — stores/falkordb/ (vector/fulltext DDL и запросы — туда же). Ранжирование/RRF — Python.
- Эмбеддинги в тестах: юниты ТОЛЬКО FakeEmbedder (детерминированный, без сети/torch); реальная модель — маркер `emb` (skipif пакет не установлен); дефолтный гейт БЕЗ emb/scip.
- Chunk id = `<symbol_id>#c<N>`; чанки не пересекаются, границы — AST (функция/класс целиком; крупные — построчный сплит); max_chars default 2000.
- Meta-узел в графе: embed_model/dim; search при мисматче модели конфига → error dict «reindex needed».
- Гейты НЕ ослаблять; golden-вопросы — только с верификацией ожиданий по коду фикстур.
- rtk-хук: junit-xml/redirect.

---

### Task 1: Early-M3 hardening (миграция PK, версия, evalx, конфиг-чистка)

**Files:**
- Modify: `src/codegraph/core/schema.py` (SCHEMA_VERSION=3), `src/codegraph/stores/staging.py`, `src/codegraph/stores/falkordb/batch.py`, `src/codegraph/pipeline/load.py`, `src/codegraph/evalx/edges_eval.py`, `src/codegraph/config/models.py`, `docs/superpowers/specs/2026-07-12-codegraph-design.md` (+нормативная таблица confidence)
- Tests: соответствующие юниты + regression

**Interfaces:**
- staging edges: НОВАЯ колонка `via_channel TEXT NOT NULL DEFAULT ''` в PK → PRIMARY KEY(src, dst, type, via_channel); upsert_edges извлекает `props.get("via_channel_id", "")`. Regression: два NEXT_SEGMENT между одной парой (src,dst) с разными каналами сосуществуют; iter_edges отдаёт оба.
- `Staging.__init__`: version-check ДО DDL если meta-таблица уже существует (порядок: если файл существовал и в нём есть meta → прочитать schema_version → мисматч → InvariantError с «recreate»; иначе DDL+запись). Тест: v2-подобная база (создать вручную старую схему) → InvariantError, НЕ sqlite3.OperationalError.
- batch.upsert_edges: параметр `key_props: tuple[str, ...] = ()` — MERGE-ключ ребра включает эти props: `MERGE (a)-[e:TYPE {k1: r.k1}]->(b)`; load передаёт ("via_channel_id",) для NEXT_SEGMENT (значение из props, '' если нет — тогда ключ не включать? единообразнее: load нормализует отсутствие в ''). Live-тест: два NEXT_SEGMENT-ребра между одной парой узлов в FalkorDB.
- evalx.found_edges: CALLS-рёбра с props.mechanism — исключать (симметрия golden-фильтру); тест: staged temporal_start CALLS не попадает в found({"CALLS"}).
- config: `ConsumerIdiom.dict_assign` УДАЛИТЬ (поле+валидатор-ветка; kind=dispatch_dict требует registrar_call). Breaking для конфигов с dict_assign — задокументировать в докстринге модели. Тесты моделей обновить.
- design-doc: секция «Нормативная таблица resolution/confidence» (ярусы матчей 1.0/0.8/0.6; value/template/config_ref downgrades; temporal_start dynamic 0.9; unresolved-канал 0.5; NEXT_SEGMENT произведение; трейс min; РАСХОЖДЕНИЕ kafka-template≤0.6 vs http-<base>-static — зафиксировать текущее состояние и правило на будущее: «шаблон с детерминированной подстановкой внутри отскоупленного клиент-класса = static; вне скоупа = heuristic»).

- [ ] Падающие тесты по каждому пункту → RED → реализация → GREEN; full + falkordb + ruff; scip разок (гейты живы с v3 — свежий staging в гейтах).
- [ ] Commit: `fix(m3): NEXT_SEGMENT parallel-channel PK, loud v3 migration, evalx mechanism symmetry, config cleanup`

---

### Task 2: PART_OF_PROCESS rework + trace-селектор из графа

**Files:**
- Modify: `src/codegraph/linking/processes.py`, `src/codegraph/query/api.py`, `src/codegraph/cli.py` (trace без staging), `src/codegraph/stores/falkordb/store.py` (+find_by_qualified)
- Tests: real-shape processes-тест, cli trace без staging

**Interfaces:**
- processes: intra-подъём — reverse-adjacency по staged intra-рёбрам (CALLS/DEPENDS_ON/INVOKES_ACTIVITY, включая temporal_start-CALLS): от каждого NEXT_SEGMENT.src подняться до сегмент-entry (узел с ролью {RouteHandler, MessageConsumer, TemporalWorkflow} или без входящих intra-рёбер; циклы — visited); построить entry→entry граф; PART_OF_PROCESS order = BFS-уровень от процесса-entry. REAL-SHAPE тест (фикстуры, degraded-пайплайн): процесс «Order KYC onboarding» имеет PART_OF_PROCESS к create_order(0), run_consumer(1), handle_order_created(1), doc get_document(2) — max order 2, НЕ 0 (регрессия инертности).
- store.find_by_qualified(service: str, qualified: str) -> dict | None (MATCH по qualified_name+service, LIMIT 1 ORDER BY id).
- query/api: `resolve_selector(selector) -> node_id | error` — route-форма через граф (Channel http_route props + HANDLES), qualified-форма через find_by_qualified. CLI trace использует его (staging больше не нужен; сообщение об ошибке при ненайденном селекторе — прежнее UX). linking/processes.py — свой staging-резолв оставляет (S7-контекст), но общая грамматика селектора — в одном модуле (вынести парсер формы в core/selectors.py, оба используют).
- MCP trace_process не меняется (принимает id); CLI-тесты обновить (без staging-фикстуры).

- [ ] RED → GREEN; full + falkordb + ruff.
- [ ] Commit: `fix(m3): real PART_OF_PROCESS chain derivation + graph-based trace selector`

---

### Task 3: AST-чанкер + staging chunks

**Files:**
- Create: `src/codegraph/chunking/__init__.py`, `src/codegraph/chunking/splitter.py`
- Modify: `src/codegraph/stores/staging.py` (chunks-таблица — schema уже v3, таблица добавляется СЕЙЧАС: она в v3-DDL из T1? НЕТ — T1 не знал о chunks. Решение: chunks-таблица входит в v3-DDL здесь, T3; T1 и T3 оба меняют DDL внутри одной вехи/версии — допустимо, staging пересоздаются)
- Tests: `tests/unit/test_chunking.py`, staging chunks round-trip

**Interfaces:**
- `splitter.chunk_file(relpath, source: bytes, facts: FileFacts, symbol_ids: dict[int, str], module_id: str, max_chars=2000) -> list[ChunkRec]`:
  - `ChunkRec(chunk_id, symbol_id, ord, text, start_line, end_line, content_hash)` (frozen dataclass, content_hash = sha256(text)).
  - Правила: каждый top-level def/class — свой чанк (символ = его node id из symbol_ids по DefFact.index); методы НЕ отдельно (класс целиком), НО если класс > max_chars — методы отдельными чанками (symbol_id метода), остаток класса (шапка+докстринг) — чанк класса; функция > max_chars — построчный сплит на куски ≤max_chars (ord 0..N, один symbol_id); module-преамбула (импорты+константы+докстринг до первого def) — чанк модуля (symbol_id = module_id), если непуста.
  - Чанки не пересекаются; конкатенация текстов ⊆ исходника (пробельные хвосты допустимы).
- staging: `chunks(chunk_id TEXT PRIMARY KEY, symbol_id, service, relpath, ord, text, start_line, end_line, content_hash, embedding BLOB NULL, embed_model TEXT NULL)`; `upsert_chunks(service, rows)`; `chunks_for_service(service)`; `chunks_missing_embedding(model) -> list`; `set_embeddings(rows: list[(chunk_id, blob, model)])`; begin_service чистит свои чанки; counts()+`chunks`.
- Тесты на фикстурах: order.py → чанки OrderService (класс ≤2000 — один чанк) + module-преамбула; синтетика: класс >2000 → пометодные; функция >2000 → сплит; спаны/hash корректны.

- [ ] RED → GREEN; full + ruff. Commit: `feat(m3): AST chunker (symbol-aligned, size-bounded) + staging chunks table`

---

### Task 4: Контекстная аугментация

**Files:**
- Create: `src/codegraph/chunking/augment.py`
- Tests: `tests/unit/test_augment.py`

**Interfaces:**
- `augment.build_header(staging, chunk: ChunkRec) -> str` — строки:
  `file: <relpath> · service: <service>`
  `symbol: <qualified_name> (<kind>[, roles]) [· parent: <сигнатура родителя>]`
  `[imports: <до 8 имён модульных импортов>]`
  `[doc: <первая строка docstring>]`
  `[graph: produces <каналы> · consumes <каналы> · calls_http <каналы> · handles <route> · depends_on <имена> · calls <до 5 имён>]` — из staged рёбер узла (символа чанка); только непустые части; каналы — human-имя (kafka/event: name; http: "GET /path").
  `augment_text(header, chunk_text) -> str` = header + "\n\n" + text. Возвращаемое НЕ пишется в staging text (text остаётся чистым кодом) — аугментированная строка идёт только в эмбеддер и в fulltext-поле контекста: staging chunks +колонка `context_header TEXT` (T3 добавит колонку сразу — синхронизируй DDL здесь если T3 уже влит без неё: колонка в T4 → снова DDL-правка в той же v3 — ок, staging пересоздаваем).
- Тест (фикстуры, degraded-пайплайн + link): header для чанка OrderService.place содержит `produces` и `OrderCreated`; для create_order — `RouteHandler` и `POST /orders` (roles/handles); для DocumentManagementClient.get_document — `calls_http` и `GET /documents/{doc_id}`; для module-чанка — file/service без graph-блока.

- [ ] RED → GREEN; full + ruff. Commit: `feat(m3): graph-aware contextual augmentation headers`

---

### Task 5: Embedders (pluggable)

**Files:**
- Create: `src/codegraph/embedding/__init__.py`, `base.py`, `fake.py`, `local.py`, `openai_emb.py`, `voyage.py`, `factory.py`
- Modify: `pyproject.toml` (extras: local-emb → sentence-transformers>=3; openai → openai>=1.50; voyage → voyageai>=0.3; версии уточнить при исполнении), маркер `emb`
- Tests: `tests/unit/test_embedders.py` (fake+factory), `tests/integration/test_local_embedder.py` (marker emb)

**Interfaces:**
- `base.Embedder(Protocol)`: `model_id: str`, `dim: int`, `embed_batch(texts: Sequence[str]) -> list[list[float]]`, `embed_query(text: str) -> list[float]`.
- `fake.FakeEmbedder(dim=8)` — детерминированный (hash-based), для юнитов и degraded-дефолта тестов.
- `local.LocalEmbedder(model: str)` — lazy import sentence_transformers (ImportError → CodegraphError с «uv sync --extra local-emb»); trust_remote_code=True для jina-code (задокументировать); normalize_embeddings=True (cosine).
- `openai_emb`/`voyage` — по env-ключам (OPENAI_API_KEY / VOYAGE_API_KEY), lazy import, model из cfg.
- `factory.make_embedder(cfg: EmbeddingConfig) -> Embedder`: provider local→LocalEmbedder(cfg.model); openai/voyage → соответствующий (без ключа → CodegraphError с подсказкой).
- Marker `emb` в pyproject; интеграционный тест: реальная jina-code (skipif не установлен пакет): dim>0, два разных текста → разные векторы, близкие тексты ближе (cosine sanity).

- [ ] RED → GREEN (fake/factory); emb-интеграция опционально локально; full + ruff. Commit: `feat(m3): pluggable embedders (fake/local/openai/voyage) + extras`

---

### Task 6: S8 chunk+embed стадия + load (Chunk-узлы, vector/fulltext DDL, Meta)

**Files:**
- Create: `src/codegraph/pipeline/chunk_embed.py`
- Modify: `src/codegraph/pipeline/load.py`, `src/codegraph/stores/falkordb/ddl.py` (+vector index Chunk.embedding dim-параметр; +fulltext Chunk(text, context_header)), `src/codegraph/stores/falkordb/batch.py` (vector_props → `SET n.embedding = vecf32(r.embedding)`), `src/codegraph/stores/falkordb/store.py` (ensure_schema(dim)), `src/codegraph/cli.py` (index: S8 между link и load; report), `src/codegraph/pipeline/report.py`
- Tests: юниты (кэш! re-embed 0), falkordb live (Chunk-узлы+vecf32+индексы+Meta)

**Interfaces:**
- `chunk_embed.run(cfg, staging, embedder) -> dict`: для каждого сервиса — файлы из staged files (bytes с диска по ServiceConfig.path), facts → chunk_file → upsert_chunks (context_header через augment); embed: chunks_missing_embedding(embedder.model_id) — augment_text → embed_batch (батчи ≤64) → set_embeddings (float32 little-endian BLOB); report {chunks_total, embedded, reused}. ПОВТОРНЫЙ прогон без изменений: embedded==0 (regression-тест с FakeEmbedder).
- load: Chunk-узлы labels ("Chunk",), props {id, symbol_id, service, relpath, ord, start_line, end_line, content_hash, text, context_header, embed_model} + embedding через vector_props; известные id чанков НЕ идут в edges known_ids (рёбра к чанкам не создаём в M3 — связь по symbol_id свойству). Meta-узел (label Meta, id "meta"): embed_model, dim, schema_version. ensure_schema(dim: int | None) — vector index только при dim.
- cli.index: embedder создаётся ЛЕНИВО: если embedding.provider=local и пакет не установлен → жёлтое предупреждение «S8 skipped (install extra)» и пайплайн продолжается БЕЗ чанков (report отражает) — zero-config UX не ломаем; provider openai/voyage без ключа → аналогично warning+skip. Явный флаг `--no-embed` — пропустить S8.
- report: chunks/embedded/reused строки.

- [ ] RED → GREEN (юниты с FakeEmbedder); falkordb live: загруженный Chunk с embedding, `db.idx.vector.queryNodes` возвращает его (санити), fulltext по context_header находит; Meta узел; full + ruff. Commit: `feat(m3): S8 chunk+embed stage, Chunk nodes with HNSW vector + fulltext, Meta node`

---

### Task 7: Гибридный retrieval + search_code + find_entrypoint v2

**Files:**
- Create: `src/codegraph/query/retrieval.py`
- Modify: `src/codegraph/stores/falkordb/store.py` (+search_vector_chunks(vec, k, service?), +search_text_chunks(query, k, service?)), `src/codegraph/query/api.py` (+search_code; find_entrypoint → гибрид), `src/codegraph/mcp/schemas.py`, `src/codegraph/mcp/server.py` (9-й инструмент), tests contract → 9
- Tests: юниты RRF/фьюжн (fake store), falkordb live retrieval, contract 9

**Interfaces:**
- retrieval: `rrf(rankings: list[list[str]], k=60) -> list[(id, score)]`; `search_code(store, embedder|None, query, k=8, service=None, mode="hybrid"|"vector"|"text")`: vector-ветка требует embedder и Meta-совпадение модели (мисматч → error dict «embed model mismatch: reindex»); text — fulltext по чанкам (sanitize как M2); hybrid — RRF объединение; возврат: [{chunk_id, symbol_id, qualified_name?, service, relpath, start_line, end_line, snippet: text≤600 chars, score}].
- find_entrypoint v2: RRF(Sym-fulltext ranking, chunk-vector ranking агрегированный до symbol_id [max-score по symbol]); kinds-фильтр после фьюжна; embedder отсутствует/Meta-мисматч → деградация до fulltext-only (жёлтое поле "mode": "text-only" в ответе, НЕ ошибка — обратная совместимость M2-поведения).
- MCP search_code (схемы; mode enum); contract-тест: 9 инструментов, search_code text-mode живьём на мини-графе (vector-mode в contract — с FakeEmbedder-подменой? contract использует build_server с реальным factory... сделай build_server принимающим embedder_factory опционально для тестов — санкционировано).
- store: vector-запрос `CALL db.idx.vector.queryNodes('Chunk', 'embedding', $k, vecf32($vec)) YIELD node, score` (+service-фильтр WHERE), text — fulltext queryNodes('Chunk', $q).

- [ ] RED → GREEN; falkordb live (FakeEmbedder-заэмбеженный мини-граф: vector top-1 = ожидаемый чанк; hybrid работает); full + ruff. Commit: `feat(m3): hybrid RRF retrieval, search_code MCP tool, find_entrypoint v2`

---

### Task 8: Golden-вопросы + eval retrieval + ГЕЙТ M3

**Files:**
- Create: `fixtures/golden/questions.yaml`, `src/codegraph/evalx/retrieval_eval.py`, `tests/eval/test_m3_gate.py`
- Modify: `src/codegraph/cli.py` (`eval` команда: `codegraph eval retrieval [target] [--graph]` — прогон вопросов, hit@k таблица; заглушка eval → реальная)
- Tests: юниты retrieval_eval + гейт

**Interfaces:**
- questions.yaml (КАЖДОЕ ожидание верифицировать по коду фикстур при исполнении):
  1. "где создаётся заказ и пишется событие в outbox" → accept symbols: [app.services.order.OrderService.place, app.routes.orders.create_order]
  2. "кто обрабатывает событие OrderCreated" → [app.consumers.orders.handle_order_created]
  3. "какой воркфлоу проверяет документы клиента" → [app.workflows.kyc.KycWorkflow, app.activities.documents.verify_documents]
  4. "где сервис ходит в document management по http" → [app.clients.document_management_client.DocumentManagementClient.get_document, ...create_document]
  5. "где публикуется событие о проиндексированном документе" → [app.events.producer.emit_document_indexed]
  Формат: {question, accept: [{service, symbol}], k: 3}.
- retrieval_eval: `run_questions(store, embedder, questions) -> [{question, hit: bool, rank, top: [...]}]`; hit@k = symbol_id топ-k ∈ accept (по qualified_name+service map).
- ГЕЙТ (маркеры scip+falkordb+emb): полный реальный пайплайн (3 сервиса, scip, link, S8 с РЕАЛЬНОЙ LocalEmbedder jina-code) → load → все 5 вопросов hit@3 == True (assert с выводом промахов и топов); повторный chunk_embed → embedded==0 (кэш-гейт); cleanup. ЕСЛИ модель недоступна (пакет) — тест SKIP с внятной причиной (гейт запускается локально, где extra установлен: `uv sync --extra local-emb` — исполнитель ставит и гоняет).
- cli eval retrieval: тот же прогон против существующего графа+staging; rich-таблица.
- ЕСЛИ вопрос(ы) не проходят hit@3 на реальной модели: НЕ подгонять accept-списки под фактический топ (это curve-fitting!) — допустимо УТОЧНИТЬ ФОРМУЛИРОВКУ вопроса (человеческий перефраз) максимум 1 раз на вопрос с документированием; если и после — СТОП, DONE_WITH_CONCERNS с полными топами. Решение за контроллером.

- [ ] Юниты (fake) → RED → GREEN; установка extra + реальный гейт; full + все маркеры + ruff. Commit: `feat(m3): retrieval golden questions + hit@k eval + M3 gate`

---

## Верификация M3 (после T8)

1. Все сюиты: default + falkordb + scip (M1+M2 гейты) + emb (M3 гейт) + ruff.
2. Живой смоук: `codegraph index fixtures/workspace.yaml` (с установленным local-emb) → `codegraph eval retrieval fixtures/workspace.yaml` — таблица hit@3 5/5. (Скорректировано при исполнении T8: «повторный index → re-embedded 0» невозможен в M3 — begin_service вайпит чанки каждый прогон by design; кэш действует внутри staging-сессии [гейт: 2-й chunk_embed.run → embedded 0/reused 34]; кросс-прогонный persistent-кэш эмбеддингов = M4-беклог.)
3. MCP: contract 9 инструментов; вручную из Claude Code — search_code «где создаётся заказ» (ручной пункт пользователя).
4. Финальное whole-milestone ревью (fable) + фикс-волна.
