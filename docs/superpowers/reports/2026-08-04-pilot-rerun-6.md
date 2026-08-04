# Re-run №6 после M11 — classmethod-конструкции (ПОЛНАЯ)

> Дата: 2026-08-04. По `docs/superpowers/pilot/WORK-MACHINE-RERUN-6-BRIEF.md`. Baseline — re-run
> №5 (`2026-08-03-pilot-rerun-5.md` + R6 open-gaps). M11 закрыл R6: `(cls)`-суффикс переосмыслен
> как `cls(...)`-самоконструирование внутри `@classmethod`-фабрик → честный dst это УЗЕЛ КЛАССА.
> Сервисы те же. SANITIZED — рядом.

## 0. Смоук

`git pull` → HEAD `5d4fa9a` (≥ 5d4fa9a). `uv sync --extra local-emb`. Гейт (M11 использует
m10-гейт, отдельного m11 нет): `pytest -m "scip and falkordb" tests/eval/test_m10_gate.py` →
**1 passed, 0 failed, 10.9s**. Индекс (тёплый e5-кэш): **40.1s**, 2338 чанков (0 fresh + cached).

## 1. ГЛАВНЫЙ чекпойнт — dropped CALLS (был 149, ожидание ~101)

**Факт: 149 → 101 (−48). Ожидание достигнуто точно.** Все 48 `(cls)`-дропов R6 восстановлены;
остаток `(cls)`-дропов = **0**.

### 1.1. Разбор восстановленных 48 — construction-рёбра «фабрика → КЛАСС»

M11 переопределил корень R6: `(cls)`-хвост это `cls(...)`-самоконструирование внутри
`@classmethod`-фабрики, поэтому честный dst — **узел класса** (как у прямого `ClassName(...)`), а не
метод. Форма подтверждена на реальном коде:

| src (фабричный классметод) | dst (класс) | res/conf | mechanism | callsite |
|---|---|---|---|---|
| `CreateCaseInput.build_sdf_sof` | `CreateCaseInput#` | static/1.0 | `<none>` | 1 |
| `CreateTaskInput.build_from_sdf_sof_data` | `CreateTaskInput#` | static/1.0 | `<none>` | 1 |
| `GetVerificationRequestActivityInput.with_all_steps` | `GetVerificationRequestActivityInput#` | static/1.0 | `<none>` | 1 |
| `GetVerificationRequestActivityInput.without_steps` | `GetVerificationRequestActivityInput#` | static/1.0 | `<none>` | 1 |

Без `mechanism`-пропа — та же форма, что прямой ctor-вызов (как и обещал бриф). **Важно для
jq/скриптов:** искать эти рёбра надо как `CALLS` с dst на `#` (класс), src — фабричный классметод;
поиск по `…#method().`-dst ничего не найдёт.

### 1.2. Residual 101 — чистый динамический пол

| N | dst-форма | природа | статус |
|---|---|---|---|
| **79** | `_DBRegistry#session.` | аннотация-атрибут + рантайм `sessionmaker` | ❌ динамика (R5-установлено) |
| 5 | `#throttle.` / `#_driver_maker.` | callable-атрибуты | ❌ динамика |
| 3 | `.(sleep_func)` / `.(now_getter)` / … | callable-**параметры** (higher-order) | ❌ динамика |
| **2** | `.(method)` | произвольный callable (по дизайну) | ❌ честно |
| ~12 | `localN`, разн. models | локальные/динамика | ❌ честно |

Каждый остаток — **принципиально динамический** (нет статического узла-цели). Новой восстановимой
массы нет. `(cls)`-дропы = 0. `(method)`-дропов **2**, не «~1» из брифа — оба это произвольные
callable-цели по дизайну (`renew_salesforce_token`-обёртка + один аналог); честная мелочь, не
восстановимо.

### 1.3. Бонус — who_calls(Класс) показывает фабрики рядом с конструкторами

`who_calls(CreateCaseInput#)` → `CreateCaseInput.build_sdf_sof` (фабрика видна как «вызыватель»
класса, наравне с обычными ctor-сайтами). Construction-рёбра теперь полноценно навигируемы.

## 2. Singleton provenance (§2) — невидим, парность держится

`module_singletons` claims = **92** (харвест), рёбер `mechanism="singleton_dispatch"` = **0**.
Парность «92 harvested / 0 dispatched» из №5 сохранена (реального shadowing на корпусе не было →
изменений ноль). relative-import-консюмеры синглтонов легитимны (recall-фикс M11-T2 не даёт
регрессий — 92 без потерь).

## 3. Всё остальное (§3) — bit-identical

- **who_calls × активности**: `LegacylizerActivities.get_customer_info` → 1 caller
  `mechanism="invokes_activity"` (без регрессий).
- **Трасса** (spot-check) `SourceOfFundsChange.process_event`: confidence **1.00**, external_exit 0,
  **6 сегментов** — идентично №5.
- **search_code**: хиты несут `chunk_kind` (Function/Class) + `enclosing_symbol` (без регрессий).
- **CALLS mechanisms**: `<none>` 1697, `temporal_start` 8, `singleton_dispatch` **0** — ожидаемо.
- Граф-числа: calls_http 59 (23 static / 29 unresolved / 7 external), NEXT_SEGMENT 43,
  route_prefix_unresolved 0, signal_send_unlinked 2, unresolved calls 4.9%, degraded=0.

## 4. Carry §4 (пере-скоринг retrieval) — уже сделан в №5

Пере-скоринг трёх промахов с `enclosing_symbol`/`chunk_kind` выполнен в re-run №5 (§3): два
§4.1-промаха самодокументируются как «верный класс, class-level»; клиент/сервер-промах улучшается
`service`-хинтом. Метрика hit@8 = 5/8 без изменений (M11 её не трогает). Здесь не повторяется.

## 5. Вывод

**R6 закрыт точно; residual — чистый динамический пол.** dropped CALLS **149 → 101 (−48)**, ровно
ожидание брифа. 48 восстановлены как construction-рёбра «фабрика-классметод → узел класса»
(static/1.0, форма ctor-вызова, `(cls)` переосмыслен как `cls(...)`-самоконструирование); остаток
`(cls)`-дропов = 0; бонус — who_calls(Класс) показывает фабрики. Оставшиеся 101 — **все
принципиально динамические** (79 sessionmaker-атрибут из R5, callable-атрибуты/параметры/локали,
2 by-design `(method)`); новой восстановимой массы нет — это честный пол статического резолва CALLS
на этом корпусе. Singleton-парность (92/0) и §3-инварианты (трассы, external, who_calls×активности,
search-поля) bit-identical №5. **Новых блокеров нет.**

## Приложение — воспроизведение

Workspace MCP-пилота (e5) без правок. §1: staging `edges type='CALLS'` минус `nodes.id` → 101;
`dst.endswith('(cls)')` → 0; восстановленные — `CALLS` с dst на `#` (класс), src = фабричный
классметод (`build_*`/`with_*`/`from_*`/`without_*`), static/1.0, mech=`<none>`, callsite_count.
singleton: `claims kind='module_singletons'` (92), `props.mechanism='singleton_dispatch'` (0).
who_calls/trace/search — MCP stdio. Граф `pilot_kyc`, staging.db и .scip — вне репозитория.
