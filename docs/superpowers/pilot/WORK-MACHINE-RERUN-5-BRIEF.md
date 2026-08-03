# Бриф №5: re-run после M10 (рабочая машина) — синглтоны, who_calls, search-гранулярность

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «прогони re-run по
> этому брифу». Baseline — MCP-пилот `docs/superpowers/reports/2026-08-03-mcp-pilot.md` (§4-§5).
> M10 закрыл три его входа + TRACKED-долги M9. Смоук: `git pull` (HEAD ≥ a884464) →
> `docker compose up -d` → `uv sync --extra local-emb` → `uv run pytest -m "scip and falkordb"
> tests/eval/test_m10_gate.py -q --junit-xml=/tmp/g.xml` — зелёный = механизмы работают здесь.

## 1. Индекс (кэш эмбеддингов тёплый — ожидание ~34s)

`codegraph index <ws>` (тот же workspace + e5-конфиг MCP-пилота). Правок yaml НЕ требуется.

## 2. ГЛАВНЫЙ чекпойнт — dropped CALLS (был 149)

Ожидание — честный ДИАПАЗОН, не точка: **~40–70**. Мерить ОБА числа методом пилота §5:
- residual drop: staging `edges WHERE type='CALLS'` минус материализованные `nodes.id`;
- количество и tier `mechanism="singleton_dispatch"`-рёбер (ожидание: static/1.0 на бою —
  `registry`-claim через scip; ~79 registry-сайтов + часть ~30 models/events кандидатов).
Оговорка (находка T5): установленный scip-python может УЖЕ резолвить часть сайтов напрямую
(в простой синтетике неаннотированный паттерн резолвится сам) — потому диапазон; зафиксировать
фактическое разбиение (сколько ушло через singleton_dispatch vs напрямую vs осталось dropped
[локальные/динамика — честно]).

## 3. who_calls ×3 (пилот §3.3)

`LegacylizerActivities.get_customer_info` — теперь НЕПУСТОЙ с `mechanism="invokes_activity"`
(было 0 → агент решал «мёртвый код»); два прежних — по-прежнему grep-точные; бонус:
`transitive=true` на активности → цепочка workflow → starter.

## 4. search_code (пилот §4.1-4.2)

Перегнать 8 RU-вопросов (`eval retrieval --exact`): метрика НЕ менялась — ожидание по-прежнему
5/8. Затем ПЕРЕ-СКОРИНГ трёх промахов С НОВОЙ ИНФОРМАЦИЕЙ (не меняя метрику): хиты теперь несут
`enclosing_symbol` + `chunk_kind` — два §4.1-промаха должны читаться как «правильный класс,
class-level чанк» (видно из полей); клиент/сервер-промах — повторить с `service=verification-requests`
по новой подсказке в описании инструмента → попадание? Зафиксировать таблицей.

## 5. Трассы ×4 — bit-identical + переезд полей

Confidence/`external_exit_count` те же, что в MCP-пилоте. ВНИМАНИЕ: `external`/`external_host`
переехали на УРОВЕНЬ exit-записи (siblings поля `channel`), не внутри channel — обновить
сохранённые jq-пути/скрипты, если были. CALLS_HTTP-счётчики стабильны (23 anchored, 29/7 split).

## 6. Эскалация-маркеры (--incremental)

Правка `app/db/registry.py` → `stale_escalation="module_singletons_changed"` + полный re-extract;
правка Settings-файла → `class_attrs_changed`; оба → `class_attrs+module_singletons_changed`.

## 7. MCP-описания и (опционально) live-LLM

Перезапустить MCP-сервер → описания who_calls (активности) и search_code (service-подсказка)
видны. Опционально — одна live-LLM-сессия: открытый вопрос пилота — рулят ли описания
поведением агента (нарративный слой; tool-слой уже доказан).

## 8. Отчёт — две версии (правило прежнее)

`docs/superpowers/reports/<дата>-pilot-rerun-5.md`: полная + SANITIZED (числа/вердикты/диапазоны
без имён; таблица §2-разбиения dropped; §4-таблица пере-скоринга) — коммит+push. Новые gap'ы —
по образцу open-gaps, если вскроются.

## Правила

Гейты/golden не трогать; фиксы — минимальные с синтетическими тестами; честность (диапазон
лучше ложной точки; отрицательный результат валиден); junit-xml при перехватчиках; системные
reminders харнеса — штатные.
