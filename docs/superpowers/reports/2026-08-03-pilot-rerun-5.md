# Re-run №5 после M10 — синглтоны, who_calls, search-гранулярность (ПОЛНАЯ)

> Дата: 2026-08-03. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-5-BRIEF.md`. Baseline —
> MCP-пилот `2026-08-03-mcp-pilot.md` (§4–§5). M10 закрыл три его входа + TRACKED-долги M9.
> Сервисы те же. SANITIZED — рядом; новый gap R6 — `2026-08-03-pilot-rerun-5-open-gaps.md`.

## 0. Смоук

`git pull` → HEAD `3dc6da2` (≥ a884464). `uv sync --extra local-emb`. M10-гейт:
`pytest -m "scip and falkordb" tests/eval/test_m10_gate.py` → **1 passed, 0 failed, 17.0s**.
Индекс (тёплый e5-кэш): **36.1s**, 2338 чанков (0 fresh + 2338 cached). Граф-числа bit-identical
M9 (calls_http 59: 23 static / 29 unresolved / 7 external; NEXT_SEGMENT 43; route_prefix 0;
signal_send_unlinked 2; degraded=0).

## 1. ГЛАВНЫЙ чекпойнт — dropped CALLS (был 149, ожидание ~40–70)

**Факт: 149 — БЕЗ ИЗМЕНЕНИЙ. Ожидание не достигнуто; честно разобрано до корня.** Механизм M10
singleton-резолва **работает и корректен** (fail-closed), но на этом корпусе **эмитил 0 рёбер** —
и это не баг, а следствие того, что мой прошлый диагноз оказался мисхарактеризацией.

### 1.1. Singleton-механизм работает, но здесь нечего резолвить

Харвест синглтонов **работает**: 92 claim'а `module_singletons` (static-tier, scip-резолв класса),
включая `registry -> _DBRegistry`, все клиенты (`verification_requests_cli -> VerificationRequestsClient`
и т.д.). Но CALLS-рёбер с `mechanism="singleton_dispatch"` — **ноль**, потому что:

- **Клиентские синглтоны** (`verification_requests_cli.create_request(...)`) scip **уже резолвит
  напрямую** — они и не дропались, восстанавливать нечего (tier срабатывает только при отсутствии dst).
- **`registry.session()`** (79 дропов, «главный кейс» MCP-пилота) — **не вызов метода**. В реальном
  коде (`verification-requests/app/db/registry.py:14`) `session` объявлен как **аннотация класса**
  `session: Callable[..., AsyncSession]`, а фактически `self.session = sessionmaker(...)` присваивается
  в рантайме в `setup()` (`:47`). Т.е. `registry.session()` зовёт callable-**атрибут** (фабрику),
  узла-метода `_DBRegistry#session` **не существует** (в классе только `__init__/setup/close`).
  Резолвер строит кандидат `_DBRegistry.session`, **верифицирует class-shape → узла нет → честно
  отказывается** (никакого ложного ребра). **Моя MCP-пилотная оценка «53% = singleton-методы» была
  ошибочной**: это не метод через синглтон, а динамический callable-атрибут. Механизм их не может (и
  не должен) резолвить.

### 1.2. Точный разбор 149 dropped (метод пилота §5)

| N | dst-форма | природа | восстановимо? |
|---|---|---|---|
| **79** | `_DBRegistry#session.` | аннотация-атрибут + рантайм `sessionmaker` | ❌ динамика (честно) |
| **48** | `Class#method().(cls)` | **классметод, базовый узел СУЩЕСТВУЕТ** | ✅ **48/48** (см. R6) |
| 5 | `#throttle.` / `#_driver_maker.` | callable-атрибуты (декоратор/ctor-param) | ❌ динамика |
| ~17 | `.(method)`, `localN`, разн. models | локальные/динамика | ❌ честно |

`mechanism`-разбивка CALLS: `<none>` 1697, `temporal_start` 8, **`singleton_dispatch` 0**.

**Вывод по §1:** ожидание ~40–70 строилось на моей же оптимистичной singleton-оценке, которая
оказалась мисхарактеризацией. Реальная восстановимая масса — **не синглтоны, а 48 классметодов**
(новый R6, независимый механизм). Singleton-инфраструктура M10 корректна и полезна впрок (92
захарвестленных синглтона, static-tier), но именно этот срез её не задействует.

## 2. who_calls ×3 (пилот §3.3) — ✅ закрыто

| Цель | было (пилот) | M10 |
|---|---|---|
| `LegacylizerActivities.get_customer_info` | **0** («мёртвый код») | **1**, `mechanism="invokes_activity"` → `SOFVerificationRequestManager.setup` |
| — то же, `transitive=true` | — | цепочка ↑ `LimitIncreaseSOFWorkflow.initiate_request / run_sdf_phase` |
| KYCEventPublisher.publish | 1 (grep-точно) | 1 (без регрессий) |
| DocumentStorageClient.upload_file_for_document | 1 (grep-точно) | 1 (без регрессий) |

Активность больше не читается как мёртвый код; INVOKES_ACTIVITY-источники помечены механизмом.

## 3. search_code (пилот §4.1–4.2) — метрика та же, поля новые

`eval retrieval --exact` на тех же 8 RU-вопросах: **hit@8 = 5/8** (не менялось, как и ожидалось).
**Пере-скоринг трёх промахов с новой информацией** (метрику НЕ меняли):

| Промах | новое поле | чтение |
|---|---|---|
| «загрузка файла» | `chunk_kind=Class`, `enclosing_symbol=DocumentStorageClient` | ✅ «правильный класс, class-level чанк» — виден из полей |
| «конфигурация URL» | `chunk_kind=Class`, `enclosing=ServicesSettings` (top-1) | ✅ то же — правильный класс |
| «связанные клиенты v2» | повтор с `service=verification-requests` | ⚠️ ушло на vr-сторону (`add_linked_customers`, модель v2), но точный `_v2`-хендлер не top-1 |

Два §4.1-промаха теперь **самодокументируются** как «верный класс на class-уровне» (агент видит
`chunk_kind`/`enclosing_symbol`). Клиент/сервер-промах: `service`-подсказка переносит выдачу на
верный сервис и фичу (практически попадание), но точный `_v2`-символ по-прежнему не top-1 (сиблинг
v1/v2 + гранулярность — остаётся как честный хвост, метрику не крутим).

## 4. Трассы ×4 (§5) — bit-identical + переезд полей ✅

| Вход | confidence | external_exit_count | сегменты |
|---|---|---|---|
| ConnectedAccounts.process_event | 0.50 | 0 | 4 |
| LegacylizerActivities.get_customer_info | **1.00** | **1** | 1 |
| KYPCreated.process_event | 0.90 | 0 | 7 |
| SourceOfFundsChange.process_event | **1.00** | 0 | 6 |

Confidence/external_exit — **идентичны MCP-пилоту**. **Переезд полей подтверждён**: `external` и
`external_host` теперь ключи **уровня exit-записи** (siblings `channel`, `next_entry_ids`), не внутри
channel. Бонус (M10 first-sight host): Legacylizer-exit несёт реальный
`external_host=ingress-nginx-legacylizer-controller.kube-ingress-nginx.svc.cluster.local` (из helm).

## 5. Эскалация-маркеры (§6) — инкремент таргетирует, no-op не эскалирует (корректно)

Байт-обратимый тест (бэкап → append `\n` в `app/db/registry.py` → `index --incremental` → байт-точный
restore, checksum совпал `6543716a…`): vr → **mode=incremental** (таргетно переанализировал
изменённый файл), camunda/dm → **skipped**. `stale_escalation=None` — и это **корректно**: инертная
правка не меняет singleton-digest (`name→class`-отображение), поэтому full-эскалация не нужна. Сам
маркер `module_singletons_changed` доказан синтетикой M10-гейта; триггерить его на реальном коде —
семантическая правка (смена класса синглтона), которую в чужом репозитории не делаю.

## 6. MCP-описания (§7)

Перезапуск сервера: 9 инструментов, `✔ Connected`. Описание `who_calls` документирует доп. поле
`mechanism="invokes_activity"`; `search_code` — `service`-фильтр и возврат `enclosing_symbol`/
`chunk_kind`. Tool-слой доказан программно (stdio). Live-LLM-сессия — доступна пользователю (сервер
в user-config), нарративный слой опционален.

## 7. Вывод

**Три входа MCP-пилота закрыты; главный числовой ожидание — честно НЕ достигнуто и разобрано.**
- ✅ **who_calls × активности** (пилот §4.3): 0 → invokes_activity-caller (+transitive).
- ✅ **search enclosing/chunk_kind** (пилот §4.1): два промаха самодокументируются как «верный класс»;
  service-хинт улучшает клиент/сервер-кейс.
- ✅ **Трассы**: bit-identical, external-поля переехали на exit-запись, реальный external_host.
- ⚠️ **dropped CALLS 149 (ожидание 40–70) — не достигнуто.** Корень: singleton-механизм корректен, но
  «главный» кейс `registry.session()` — **не метод, а динамический callable-атрибут** (моя прошлая
  оценка 53% ошибочна). Реальная восстановимая масса — **48 классметодов**, дропающихся на суффиксе
  `(cls)` (базовый узел существует 48/48) — **новый R6**, механически исправимо (149→~101).

Отрицательный результат по §1 валиден и полезен: он перенаправляет M11 с (мнимого) singleton-долга на
реальный — классметодный `(cls)`-суффикс.

## Приложение — воспроизведение

Workspace MCP-пилота (e5) без правок. dropped-разбор: staging `edges type='CALLS'` минус `nodes.id`;
классметоды — `dst.endswith('(cls)')`, `dst[:-5] in nodes` → 48/48. singleton: `claims kind=
'module_singletons'` (92), `edges props.mechanism='singleton_dispatch'` (0). who_calls/search/trace —
MCP stdio. Эскалация — байт-обратимый edit+restore registry.py (checksum-verified). Граф `pilot_kyc`,
staging.db и .scip — вне репозитория.
