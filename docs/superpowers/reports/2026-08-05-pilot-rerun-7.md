# Re-run №7 после M12 — symbol-агрегация ранжирования (ПОЛНАЯ)

> Дата: 2026-08-05. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-7-BRIEF.md`. Baseline — re-run
> №5/№6 + MCP-пилот §3.2/§4.1. M12 добавил symbol-агрегацию: `search_code` возвращает k РАЗЛИЧНЫХ
> символов (sibling-чанки схлопнуты в best-rank представитель + `sibling_chunks`), пул кандидатов
> ×4. Сервисы те же. SANITIZED — рядом; новый gap R7 — `2026-08-05-pilot-rerun-7-open-gaps.md`.

## 0. Смоук

`git pull` → HEAD `48128ed` (≥ e85a497). `uv sync --extra local-emb`. Гейт:
`pytest -m "scip and falkordb" tests/eval/test_m10_gate.py` → **1 passed, 0 failed, 15.4s**. Индекс
(тёплый e5-кэш): **35.7s**, 2338 чанков (0 fresh + cached). Граф bit-identical №6 (M12 не трогал
граф — только ранжирование ответов): dropped CALLS 101, calls_http 59 (23/29/7), NEXT_SEGMENT 43.

## 1. ГЛАВНЫЙ чекпойнт — hit@8 (был 5/8; ожидание 7/8)

**Факт: 5/8 — БЕЗ ИЗМЕНЕНИЙ. Ожидание не достигнуто; разобрано до корня (отрицательный результат
валиден).** M12-агрегация **механически работает** (siblings схлопываются, см. §2), но два
§4.1-промаха **не закрылись** — и оказалось, это НЕ sibling-flooding, а **несовпадение гранулярности
golden-accept с тем, где реально живёт семантический сигнал**.

### Таблица 8 рангов №5 → №7

| # | Вопрос | №5 | №7 | top-1 (№7) |
|---|---|---|---|---|
| 1 | OCR-обработка | HIT r2 | HIT **r3** | OCRDataEvent |
| 2 | Kafka-публикация | HIT r2 | HIT r2 | kafka-workflow |
| 3 | legacylizer-инфо | HIT r4 | HIT r4 | LegacylizerActivities |
| 4 | связанные клиенты v2 | MISS | MISS | services.add_linked_customers (клиент/сервер) |
| 5 | загрузка файла | MISS | MISS | **DocumentStorageClient (класс, r1)** |
| 6 | сигнал завершения опроса | HIT r0 | HIT r0 | PartnerProfileWorkflow.complete_survey |
| 7 | патч шага верификации | HIT r2 | HIT r2 | (history_replayer) |
| 8 | конфигурация URL | MISS | MISS | **ServicesSettings.validate_cross_service_urls (метод, r1)** |

Ранги 5 попаданий стабильны (±1, легитимный сдвиг от пула ×4).

### Разбор двух §4.1-промахов — гранулярность accept, НЕ flooding

Проверено search'ем k=8 и k=20 + инспекцией чанков графа:

- **«загрузка файла» (#5).** `DocumentStorageClient` чанкается ДВОЯКО: 10 class-body чанков
  `##c0..c9` (qualified_name = **класс**) + 10 method-чанков (`upload_file_for_document().` и т.п.,
  qualified_name = метод). M12 схлопнул 10 class-body чанков в ОДИН представитель-**класс**
  (`sibling_chunks=9`) → **класс на rank 1**. Но мой golden-accept — **метод**
  `upload_file_for_document`, и его отдельный метод-чанк **не входит даже в k=20** (его собственный
  vector-score ниже, чем у class-body чанков и sibling-классов-DTO). Т.е. верный ответ (**класс**)
  теперь #1, но accept назвал метод → строгий MISS.
- **«конфигурация URL» (#8).** Зеркально: `ServicesSettings` даёт метод-чанк
  `validate_cross_service_urls` (**rank 1**) + 2 низко-ранжированных class-body чанка. Мой accept —
  **bare-класс** `ServicesSettings`, которого нет в k=20. Т.е. верный ответ (**метод**) #1, но
  accept назвал класс → строгий MISS.

**Ключевое наблюдение:** два промаха указывают в ПРОТИВОПОЛОЖНЫЕ гранулярные стороны (один верен как
класс, другой как метод), поэтому никакая единая политика агрегации их оба не «починит», и curve-fit
невозможен без обмана. Метрику НЕ крутил (accept-списки не менял). Это находка не о ретривале
(верный контейнер #1 в обоих), а о **правиле сопоставления eval'а** — см. R7 в open-gaps.

- **«связанные клиенты v2» (#4)** — MISS без изменений, как и предсказывал бриф (это не
  гранулярность, а клиент/сервер-неоднозначность): top-1 клиентская `services.add_linked_customers`,
  vr-серверный `_v2`-хендлер не top-8; `service`-хинт остаётся рабочим обходом (пилот §4.2).

## 2. sibling_chunks в деле (§2)

Агрегация подтверждена полем на «толстых» классах: `DocumentStorageClient` **sib=9**,
`VerificationRequestsClient` **sib=19**, `VerificationRequestsActivities` **sib=4**,
`ServicesSettings`-class-body коллапсирует тоже. k-зависимость поля (счёт кандидат-пула, не граф-факт)
— как описано в инструменте; jq-скрипты не должны полагаться на него как на инвариант. Механизм
«k различных символов в top-k» работает: дублирующие class-body строки схлопнуты в одну.

## 3. Спот-чек инвариантов (§3) — bit-identical №6

- dropped CALLS = **101** (M12 граф не трогал).
- who_calls×активность: `LegacylizerActivities.get_customer_info` → `invokes_activity` → `setup`.
- Трасса `SourceOfFundsChange.process_event`: confidence **1.00**, external_exit 0, **6 сегментов**.
- calls_http 59 (23 static / 29 unresolved / 7 external), NEXT_SEGMENT 43, unresolved calls 4.9%.

## 4. Вывод

**M12-агрегация работает механически, но hit@8 = 5/8 (ожидание 7/8 не достигнуто) — честно
разобрано.** Дублирующие class-body чанки схлопнуты в один символ-представитель (sib=9/19/4),
`k различных символов` в top-k достигнуто, 5 попаданий стабильны. Но два §4.1-промаха **не закрылись
и не могли** этой правкой: они оказались не sibling-flooding, а **гранулярным рассогласованием
golden-accept** — верный контейнер #1 в обоих (класс `DocumentStorageClient` / метод
`ServicesSettings.validate_cross_service_urls`), но accept назвал противоположный уровень (метод /
класс). Направления промахов зеркальны → единая агрегация их не сводит. Реальный вывод — **не о
ретривале, а о правиле сопоставления eval'а** (R7): строгое `(service, qualified_name)`-равенство не
мостит класс↔метод. §3-инварианты bit-identical №6. Метрику не крутил (anti-curve-fit).

## Приложение — воспроизведение

Workspace+вопросы MCP-пилота без правок. `eval retrieval --exact` → 5/8. Разбор: `search_code`
k=8/k=20 + `MATCH (ch:Chunk) WHERE ch.qualified_name CONTAINS '<Class>'` (двойной чанкинг:
class-body `##cN` + method-чанки). sibling_chunks — поле ответа search_code. Граф `pilot_kyc`,
staging.db и .scip — вне репозитория.
