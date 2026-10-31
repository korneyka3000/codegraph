# Fresh-machine setup — индекс трёх реальных KYC-сервисов (SANITIZED)

> Дата: 2026-11-01. По брифу `pilot/WORK-MACHINE-FRESH-SETUP-BRIEF.md`. Санитизировано: числа,
> вердикты, обезличенные категории — без qualified-имён, путей, кода и содержимого workspace-конфига.
> Baseline — re-run №7 (2026-08-05). Коммиты сервисов — свежие (≈ конец октября), конфиг собран
> заново из шаблона брифа → расхождения ожидаемы, фиксируются, не подгоняются.

## 0. Тулчейн

FalkorDB healthy, `doctor` — все проверки OK (python 3.14, node, npx; ping / multi_label / set_plus_eq /
unique_constraint / vector_index_cosine / fulltext). Три чекаута найдены, у каждого venv рабочий
(главный пакет импортируется из корня сервиса; это source-layout, не installed-пакет — проверка
брифа `-c "import ..."` проходит только с cwd = корень сервиса). У двух сервисов venv на 3.11,
у одного на 3.14 — на degraded не влияет.

## 1. Итог (финальный full-прогон)

### services

| service | files | nodes | edges | joined | unresolved | external | degraded | mode |
|---|---|---|---|---|---|---|---|---|
| оркестратор | 189 | 1585 | 2350 | 1382 | 171 | 1989 | no | full |
| consumer-сервис A | 95 | 614 | 875 | 431 | 126 | 1853 | no | full |
| consumer-сервис B | 54 | 385 | 512 | 250 | 62 | 1175 | no | full |
| **TOTAL** | 338 | 2584 | 3737 | 2063 | 359 | 5017 | | |

load: nodes 5197 / edges 5712 / dropped 107 (все — CALLS, missing endpoint). Чанков 2490.

### health / linking

| метрика | факт | baseline №7 |
|---|---|---|
| degraded | нет | нет |
| unresolved calls | 4.8 % | 4.9 % |
| staged CALLS with valid dst | 94.3 % | — |
| dropped CALLS | **107** | 101 |
| calls_http всего | 50 | 59 |
| — static (anchored) | **23** | 23 |
| — external | **24** | 7 |
| — unresolved | **3** | 29 |
| route_prefix_unresolved | 0 | — |
| NEXT_SEGMENT | **43** | 43 |
| processes | 8 | — |
| PRODUCES / CONSUMES | 22 / 35 | — |
| producer_unresolved_channel | 1 | — |
| signal_send_unlinked | 2 | — |
| part_of_process_ambiguous | 30 | — |
| http route/url/verb unresolved | 0 / 0 / 0 | — |

## 2. Чекпойнты §3

| # | чекпойнт | вердикт | число |
|---|---|---|---|
| 1 | degraded = no, mode = full у всех | **met** | 3/3 |
| 2 | valid dst ≈ 94–96 %, dropped ≈ 100 | **met** | 94.3 %, 107 (+6 к baseline — другой коммит, тот же класс: динамический пол) |
| 3 | static ≈ 23, external ≈ 7, prefix_unresolved = 0 | **static met, external — отклонение (объяснено)** | 23 / 24 / 0 |
| 4 | PRODUCES/CONSUMES > 0 по обеим идиомам, next_segments ≈ 43, processes > 0 | **met (с оговоркой по outbox)** | 22/35, 43, 8 |
| 5 | signal_send_unlinked мал; unresolved — записать | **met** | 2; producer_unresolved_channel 1; http_* 0 |

**external 24 vs 7 — не регрессия.** Сумма static+external+unresolved = 50 (у №7 = 59; другой коммит).
Шаблон брифа давал ОДНУ http-идиому на все клиент-классы с env-якорем внутреннего сервиса; после
разнесения по клиентам (как требует комментарий самого шаблона) claim'ы внешних клиентов получили
честный внешний якорь и ушли из unresolved в external (unresolved 27 → 3). static не изменился.

## 3. Итерации конфига

| итер. | что поправили | эффект |
|---|---|---|
| 0 | шаблон как есть | **ошибка конфига**: consumer `kind=base_class` теперь требует `handler_method` (DSL ужесточился после июля; шаблон брифа не обновлён) → добавлен метод-хендлер базового consumer'а |
| 1 | шаблон + handler_method, env_sources выключен | static **0**, unresolved 50, next_segments 20 — без env→service карты ни один claim не якорится на сервис |
| 2 | включён `env_sources` (helm values оркестратора; путь к values в репо изменился относительно шаблона) | static **23**, unresolved 27, external 0, next_segments **43** |
| 3 | http-идиома разнесена по клиент-классам (по одной записи на файл клиента, свой env-якорь) | external **24**, unresolved **3**, static 23; part_of_process_ambiguous 3 → 30 (см. §4) |
| 4 | outbox-идиома: в шаблоне висела не на том сервисе и на несуществующем FQN (модуль переехал, settings-класс переименован) → перенесена на сервис-владелец и на реальный фабричный метод | PRODUCES 20 → 22; канал — `${ENV}`-плейсхолдер heuristic/0.6, не static; NEXT_SEGMENT не изменился |

Итого **5 прогонов** (1 full холодный + 3 incremental + финальные full ×2 для замеров), 4 правки конфига.
Из шаблонных FQN совпали с кодом: producer-обёртка (enum-fanout), базовый consumer shared-lib, SDK-
декоратор/драйвер/request-конструкторы. Не совпал: outbox.

## 4. Находки (не крэши; крэшей пайплайна не было)

1. **Шаблон брифа устарел по DSL** — `handler_method` обязателен для `base_class`-consumer'а. Громкая
   ошибка валидации, не тихий ноль — поведение правильное; правка — в шаблоне брифа.
2. **env_sources в шаблоне закомментирован** с оговоркой «без него external уйдут в unresolved».
   Факт сильнее: без него в unresolved уходят и **static** (0/50) — внутренний якорь тоже идёт через
   env→service карту, т.к. ни у одного сервиса не задан `http.base_url_env`. Бриф стоит поправить.
3. **Outbox-топик из вложенного Settings-дефолта не резолвится статически** (TRACKED LIMITATION).
   Топики теперь живут как `Field(<вложенная Settings-модель>(topic="…"))`; харвестер `settings:`
   видит только строковые дефолты полей → честный env-плейсхолдер 0.6. Константами топики НЕ
   подставлял (подгонка). Даже со static-топиком NEXT_SEGMENT не появился бы: потребитель этого
   топика в оркестраторе не покрыт consumer-идиомой (у оркестратора она не сконфигурирована).
4. **part_of_process_ambiguous 3 → 30** после разнесения http-идиомы (итер. 3). Новые external-якоря
   увеличили число сегментов-кандидатов в нескольких процессах; processes = 8 не изменился.
   Не разбирал до корня — кандидат на проверку в live-LLM сессии (трассы через внешние exit'ы).

## 5. Времена индекса

| прогон | wall |
|---|---|
| первый full, холодный (скачивание e5 + 2490 fresh эмбеддингов + scip ×3) | **203 s** |
| — из них scip (по mtime .scip-артефактов) | ≈ 1.5 мин |
| — эмбеддинг + скачивание модели (остаток) | ≈ 2 мин |
| incremental после смены идиом | 26 s |
| full `--no-embed`, тёплый scip-кэш | 17.8 s |
| full, эмбеддинги из кэша | 31.3 s |

Постадийного таймера в CLI нет — split scip/эмбеддинг оценен по mtime, не измерен.

## 6. Retrieval-чекпойнт §4

**Пропущен честно**: файл вопросов MCP-пилота на новой машине отсутствует; новые «похожие» вопросы не
сочинялись (другой golden, несравнимо). hit@8 не измерен.

## 7. MCP

`claude mcp add -s user codegraph … serve <ws.yaml>` → `claude mcp list`: **✔ Connected**.
`graph_stats` в новой сессии — проверяет пользователь.

## 8. Дальше

Эстафета: `WORK-MACHINE-LIVE-LLM-BRIEF.md` §1 (смоук §0 закрыт этим отчётом). Рекомендация для
шаблона fresh-setup брифа: `handler_method`, включённый env_sources, per-client http-идиомы.
