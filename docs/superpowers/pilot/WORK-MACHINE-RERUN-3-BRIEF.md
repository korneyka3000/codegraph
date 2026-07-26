# Бриф: re-run №3 после M8 (рабочая машина) — полная трасса бизнес-процесса

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «прогони re-run по
> этому брифу». Baseline — `docs/superpowers/reports/2026-07-24-pilot-rerun-2.md` (+ open-gaps
> R4/R5 рядом). M8 закрыл оба: транзитивная композиция префиксов роутеров (include_router-цепочки
> через файлы, включая собственный префикс агрегатора) и типизированные сигнальные отправители
> (`handle.signal(Cls.method, …)`). Смоук: `git pull` (HEAD ≥ 278e46d) → `docker compose up -d`
> → `uv sync` → `uv run pytest -m "scip and falkordb" tests/eval/test_m8_gate.py -q
> --junit-xml=/tmp/g.xml` — зелёный = механизмы работают здесь.

## 1. Правки workspace yaml (МАЛО — почти всё уже настроено с re-run №2)

1. **КРИТИЧНО — селекторы процессов на КОМПОЗИТНЫЕ шаблоны**: если в `processes:` есть
   entrypoint'ы вида `"<svc>:GET /steps/{id}"` — теперь роуты живут под полными путями
   (`"<svc>:GET /api/v1/steps/{step_uid}"`). Старый селектор молча даст None. То же для
   любых `codegraph trace "<svc>:VERB /path"`-вызовов.
2. Идиомы HTTP/producer/consumer/signal — БЕЗ изменений (re-run №2 конфиг переносится как есть).

## 2. Чекпойнты (vs baseline re-run №2)

| Метрика | Baseline №2 | Ожидание M8 |
|---|---|---|
| CALLS_HTTP anchored static/1.0 | 0 | **≈23 из 24** anchored-claim'ов (1 честный мисс: `DELETE …/linked-customers` — vr объявляет только PATCH/GET; это и находка для команды сервисов) |
| calls_http_unresolved | 59 | ≈36 (59 − 23) |
| route_prefix_unresolved | — | **0** на канонической цепочке vr; честные ненули: app в `create_app()`-фабрике, фабричные роутеры, double-mount (v1+legacy одним роутером → ambiguous by design — discard, не ложный путь) |
| PRODUCES temporal_signal | 0 | **≈18/18** static/1.0 — все sender-сайты пилота same-service; отдельные репо СТРУКТУРНО не могут выразить cross-service typed-ссылку (TRACKED LIMITATION кусает только monorepo-импорты → signal_send_unlinked, никогда не ложное ребро) |
| signal_name_unresolved | 18 | **0** |
| signal_send_unlinked | — | 0; ненуль = резолвнутый символ, который НЕ signal-хендлер — разглядеть |
| NEXT_SEGMENT | 0 | **> 0**: сигнальные пары (18 PRODUCES × 27 CONSUMES по 25 каналам; typed-пары conf 1.0) + HTTP-пары (23 CALLS_HTTP × HANDLES) |

Эксплуатационные пояснения (не баги): `channels_gc` ≈ числу роутов на повторных прогонах —
документированный GC-then-recreate http_route-каналов, не потеря данных; карточка узла-хендлера
показывает ЛОКАЛЬНЫЙ `path_template` (`/steps/{id}`) — композитная идентичность живёт на
Channel-узле (беклог: compose-back в node-props).

## 3. ГЛАВНЫЙ вердикт — полная трасса целевого сценария

`codegraph trace` от реального входа (Kafka-consumer или роут):
**Kafka-событие → consumer.process_event → handle.signal(Cls.method) → @workflow.signal-хендлер
→ activity → HTTP-клиент → роут другого сервиса** — впервые есть все звенья. Проверить глазами
с пользователем посегментно: сигнальный хоп (PRODUCES→chan:temporal_signal→CONSUMES) и
HTTP-хоп (CALLS_HTTP static/1.0 → HANDLES) должны быть ЯВНЫМИ сегмент-переходами. Это
целевой сценарий всего проекта — вердикт «собирается/не собирается и почему» важнее любых чисел.

## 4. Отчёт — две версии (правило прежнее)

`docs/superpowers/reports/<дата>-pilot-rerun-3.md`: полная (остаётся) + SANITIZED (числа/вердикты
без имён) — коммит+push. Таблица чекпойнтов §2 с фактами; трасс-вердикт §3 (mermaid санитизировать
или описать структуру); новые gap'ы — по образцу open-gaps (корень+file:line+направление), если
вскроются. Санитизированную — пользователю на вычитку.

## Правила

Гейты/golden не трогать; фиксы — минимальные с синтетическими тестами; честность (отрицательный
результат валиден); junit-xml при перехватчиках вывода; системные reminders харнеса — штатные.
