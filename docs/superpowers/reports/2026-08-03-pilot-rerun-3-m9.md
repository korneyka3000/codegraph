# Re-run №3 против M9 — регрессия + полировка (ПОЛНАЯ)

> Дата: 2026-08-03. Повторный прогон `docs/superpowers/pilot/WORK-MACHINE-RERUN-3-BRIEF.md`
> против нового билда M9 (external-каналы first-class, compose-back полного path в node-props,
> multi-mount роутеров). Baseline — `2026-07-24-pilot-rerun-3.md` (M8: целевой сценарий
> собрался, NEXT_SEGMENT 43). Задача: подтвердить, что M9 не сломал №3 и что полировка (в т.ч.
> мой §5-беклог из №3) реально приземлилась. Сервисы те же. SANITIZED — рядом.

## 0. Обновление и смоук

`git pull` → HEAD `58df3a4` (10 коммитов M9 поверх M8). doctor зелёный. M9-гейт:
`pytest -m "scip and falkordb" tests/eval/test_m9_gate.py` → **2 passed, 0 failed, 24.4s**.
DSL идиом **не менялся** (M9 extractor/linking/trace-side) → workspace re-run №2/№3 перенесён
как есть (scratchpad был очищен между сессиями — yaml пересоздан идентично).

## 1. Чекпойнты №3 — держатся (регрессии нет)

Прогон: `codegraph index <ws> --no-embed`, 325 файлов, degraded=0 (все три `full`),
unresolved calls 4.9%, staged CALLS valid dst 91.3% — within-service граф без изменений.

| Метрика | №3 (M8) | №3 против M9 | Вердикт |
|---|---|---|---|
| CALLS_HTTP static/1.0 | 23 | **23** (все → verification-requests) | ✅ без регрессий |
| NEXT_SEGMENT | 43 | **43** (23 http + 20 signal), static/1.0 | ✅ |
| route_prefix_unresolved | 0 | **0** | ✅ |
| signal_name_unresolved | 0 | **0** | ✅ |
| PRODUCES temporal_signal | 16 + 2 unlinked | **16** static/1.0 + **2** honest-unlinked (те же 2, оба корректны) | ✅ |
| INVOKES_ACTIVITY / CONSUMES | 64 / 43 | **64 / 43** static/1.0 | ✅ |
| verb_unresolved | 0 | **0** | ✅ |
| **Целевой сценарий** | собирается | **собирается идентично** (S0 consumer → S1 vr RouteHandler + S2/S3 signal-хендлеры) | ✅ |

## 2. Полировка M9 — приземлилась (проверено)

### 2.1. External HTTP-таргеты — first-class (главное улучшение над M8)

`calls_http_unresolved` **36 → 29**; появился `calls_http_external = 7`. Семь клиент-методов,
заякоренных на env, которые Helm-карта резолвит в хосты **ВНЕ воркспейса**, теперь несут
`Channel.external=true` + `config_ref=<env>` + полный path_template — вместо «просто unresolved»:

| клиент-метод | config_ref | path_template |
|---|---|---|
| `LegacylizerClient.get_customer_info` | `SERVICE_LEGACYLIZER_URL` | `/api/v1/customer-info/{customer_uid}` |
| `LegacylizerClient.get_user_entities` | `SERVICE_LEGACYLIZER_URL` | `/api/v1/profiles/{user_uid}/entities` |
| `DMoutClient.get_user_sof` | `SERVICE_DMOUT_URL` | `/v1/dmout/sof/user/uuid/{user_uid}` |
| … (ещё 4: 2 Legacylizer + 2 DMout) | | |

**Влияние на трассу (доказано).** `codegraph trace …LegacylizerActivities.get_customer_info`:
```
trace (confidence=1.00) (1 external exits)
S0 (TemporalActivity) LegacylizerActivities.get_customer_info
   channel GET /api/v1/customer-info/{customer_uid} -> external
```
Хоп помечен **`-> external`** (не `unresolved`), confidence остаётся **1.00**, и есть явный
машиночитаемый сигнал **«1 external exits»** (`query/traverse.py:414`, title в `cli.py:665`).
Контраст с M8: там те же вызовы попадали в `unresolved` и **роняли trace-confidence до 0.5**.
Теперь семантика честная: «знаем точный внешний адресат (env-anchored), просто он вне
индексируемого среза» ≠ «не знаем, куда идёт».

### 2.2. Compose-back полного пути в node-props — закрывает мой §5-беклог из №3

Все **98/98** RouteHandler-узлов теперь несут КОМПОЗИТНЫЙ `path_template` прямо в свойствах
узла (`/api/v1/steps/{step_uid}`, `/api/v2/requests/{verification_uid}/steps`), а не локальный
`/steps/{step_uid}`. В №3 §5 я отметил это как косметический беклог («композитная идентичность
живёт только на Channel-узле») — M9 (`9df8a5b feat(m9): compose-back full path_template into
handler node props`) его закрыл. Карточка хендлера теперь самодостаточна.

### 2.3. +1 start-mark (7 → 8)

`marks` 7→**8**: M9 распознаёт форму `execute_workflow` (интерцептор
`_SentryWorkflowInterceptor.execute_workflow`) вдобавок к `start_workflow`/`start_child_workflow`.
Небольшое честное расширение покрытия старт-хопов, без ложных срабатываний.

### 2.4. Multi-mount — в реальном срезе отсутствует (честно N/A)

M9 умеет один роутер, смонтированный под несколькими префиксами (композитный шаблон на каждый
mount). В этих трёх сервисах такого нет: распределение роутов — `api/v1` (94) + `api/v2` (4),
каждый роутер монтируется однократно. Механизм доказан синтетикой M9-гейта; на реальном срезе
не активируется — это свойство кода сервисов, не пробел.

## 3. `signal_send_unlinked = 2` и `channels_gc = 0` — без изменений

Два unlinked — те же принципиально-динамические конструкции, что и в №3 (рантайм-поле
dataclass'а `ParentSignalInfo.parent_signal_type`; ссылка через `.workflow_type.<attr>`
рантайм-хендла) — корректно не связаны, не потеряны. `channels_gc = 0` (свежий граф, нечего
пересобирать — против 153 на повторном прогоне №3; тот GC-then-recreate по-прежнему штатный).

## 4. Вывод

**Регрессий нет, полировка приземлилась.** Все чекпойнты №3 держатся 1:1 (23 HTTP static/1.0,
NEXT_SEGMENT 43, целевой сценарий собирается идентично). M9 добавил три честных улучшения,
проверенных на реальном коде: (1) **external HTTP-таргеты first-class** — 7 рёбер с `external=true`
+ `config_ref`, unresolved 36→29, в трассе `-> external` с confidence 1.00 и сигналом
«N external exits» (вместо роняющего confidence `unresolved`); (2) **compose-back** полного
пути во все 98 RouteHandler-узлов — закрывает мой §5-беклог из №3; (3) **+1 start-mark**
(`execute_workflow`). Multi-mount к этому срезу неприменим (честно). Новых блокеров нет.

## Приложение — воспроизведение

Workspace re-run №2/№3 без изменений. `codegraph index <ws> --no-embed` → `report.json`
(`calls_http=59`: static 23 / external 7 / unresolved 29; `next_segments=43`; `marks=8`;
`route_prefix_unresolved=0`; `signal_send_unlinked=2`). External: `MATCH (c:Channel {external:true})`.
Compose-back: `MATCH (h:RouteHandler) WHERE h.path_template CONTAINS '/api/'`. Трасса external:
`codegraph trace "<svc>:…LegacylizerActivities.get_customer_info" <ws>` → `(1 external exits)`,
`-> external`, confidence 1.00. Граф `pilot_kyc`, staging.db и .scip — вне репозитория.
