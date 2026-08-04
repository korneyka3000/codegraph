# Верификация R7-фикса — granularity-aware hit predicate (ПОЛНАЯ)

> Дата: 2026-08-05. Продолжение re-run №7. После отчёта `2026-08-05-pilot-rerun-7.md` +
> open-gaps R7 прилетели два коммита (`f35d86e` fix + `324349b` docs), закрывающие R7:
> granularity-aware hit-предикат в `evalx/retrieval_eval.py` (класс↔метод-мостик). Это верификация
> eval-фикса — граф НЕ менялся, только правило подсчёта hit@k. SANITIZED — рядом.

## 0. Что прилетело

`f35d86e fix(eval): granularity-aware hit predicate — class<->method accept bridging (R7)` +
`324349b docs(eval): correct rule-2 rationale`. Изменены только `src/codegraph/evalx/
retrieval_eval.py` (+53/−7) и `tests/unit/test_retrieval_eval.py` (+183). Golden-вопросы и
`query/retrieval.py` не тронуты (anti-curve-fit — matching-rule-фикс, ровно как я предлагал в R7).

## 1. Предикат (сверено чтением диффа перед прогоном)

Аддитивные OR-правила поверх точного равенства, всё service-gated:
1. `item.qualified_name == symbol` (прежнее поведение; единственное при `chunk_kind`=None/Module).
2. `chunk_kind=="Class"` и `symbol.startswith(qn + ".")` — класс-представитель кредитует accept на
   ЕГО метод.
3. `chunk_kind=="Function"` и `qn.startswith(symbol + ".")` — метод-чанк кредитует accept на ЕГО
   класс.
Обязательная точечная граница `"."` — не даёт `Cls` матчить `ClsOther.method` и `add_linked_customers`
матчить `add_linked_customers_v2`. Чужой класс/сервис не кредитуется. Ровно matching-rule из R7.

## 2. Результат — hit@8 5/8 → **7/8** (ожидание достигнуто)

`eval retrieval --exact`, те же 8 RU-вопросов, тот же граф `pilot_kyc` (не переиндексировал):

| # | вопрос | №7 | R7-verify | как засчитан |
|---|---|---|---|---|
| 5 | загрузка файла | MISS | **HIT r0** | правило 2: класс `DocumentStorageClient` (rank1) ⊇ accept-метод `upload_file_for_document` |
| 8 | конфигурация URL | MISS | **HIT r0** | правило 3: метод `ServicesSettings.validate_cross_service_urls` (rank1) ⊂ accept-класс `ServicesSettings` |
| 4 | связанные клиенты v2 | MISS | **MISS** | честно: клиент-side `services.add_linked_customers` ≠ accept `…_v2` (точечная граница не даёт ложного матча) |
| 1,2,3,6,7 | — | HIT | HIT | без регрессий (ранги 0/2/4/0/2) |

**Итог: 7/8.** Два зеркальных гранулярных промаха §4.1 закрыты корректно; единственный остаток —
#4, **честная клиент/сервер-неоднозначность** (не гранулярность), которую фикс правильно НЕ
маскирует (проверено: точечная граница блокирует `add_linked_customers`→`_v2`). Over-credit'а нет.

## 3. Вывод

**R7 закрыт точно и честно.** hit@8 5/8 → 7/8 без переиндексации и без правки golden — чисто
matching-rule-фикс (enclosing/chunk_kind-aware), ровно по направлению моего open-gaps R7. Оба
гранулярных промаха засчитались по правилам класс↔метод; клиент/сервер-промах #4 корректно остался
MISS (фикс не over-credit'ит — точечная граница работает). Цепочка «M12 symbol-агрегация +
R7-предикат» замкнута: агрегация даёт чистый класс-/метод-представитель, предикат мостит его к
accept любой гранулярности. Новых gap'ов нет; #4 — известная принятая неоднозначность (service-хинт
как обход). Метрику не крутил (golden неизменен).

## Приложение — воспроизведение

Граф `pilot_kyc` (без переиндекса). `eval retrieval --graph pilot_kyc --questions ru-questions.yaml
--exact` → 7/8. Предикат — `evalx/retrieval_eval.py::run_questions.accepts` (правила 1–3 выше).
Проверка «не over-credit»: #4 top-1 `services.add_linked_customers` (Function) vs accept `…_v2` →
rule3 `qn.startswith(symbol+".")` = False (точечная граница) → корректный MISS.
