# Бриф №6: re-run после M11 (рабочая машина) — classmethod-конструкции

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «прогони re-run по
> этому брифу». Baseline — re-run №5 (`2026-08-03-pilot-rerun-5.md`, R6 open-gaps). Смоук:
> `git pull` (HEAD ≥ 5d4fa9a) → `docker compose up -d` → `uv sync --extra local-emb` →
> `uv run pytest -m "scip and falkordb" tests/eval/test_m10_gate.py -q --junit-xml=/tmp/g.xml`.
> Индекс — тёплый кэш (~35s).

## 1. ГЛАВНЫЙ чекпойнт — dropped CALLS (был 149, ожидание ~101, −48)

**КРИТИЧНО для jq/скриптов:** восстановленные 48 — НЕ вызовы методов, а **construction-рёбра
«фабрика-классметод → УЗЕЛ КЛАССА»**: `from_decision → CreateStepDetails#`,
`with_all_steps → GetVerificationRequestActivityInput#`, `build_sdf_sof → CreateCaseInput#` —
static/1.0, callsite_count, БЕЗ mechanism-пропа (та же форма, что прямой `ClassName(...)`).
Поиск по `…#method().`-dst НЕ найдёт ничего — искать CALLS с dst на `#` (класс), где src —
фабричный классметод. Семантика: M11 переопределил корень R6 — `(cls)`-хвост это `cls(...)`-
самоконструирование внутри `@classmethod`-фабрик, честный dst — класс (как у ctor-вызовов).

Мерить: (а) residual drop (метод §5 пилота); (б) остаток `(cls)`-дропов — ожидание **0**;
если >0 — перечислить сайты (гард-отклонённые формы: undecorated/implicit-classmethod/nested —
вход для точечного решения); (в) один `.(method)`-дроп ОСТАЁТСЯ по дизайну (arbitrary callable).
~101 — ожидание, не assertion: каждый из 48 должен пройти гарды (bare `cls(...)` внутри
`@classmethod`-декорированного def); пилотные примеры читаются ровно так.

Бонус-проверка: `who_calls(<Класс>)` теперь показывает фабрики рядом с конструкторами.

## 2. Singleton provenance — ожидание: невидим

Парность «92 harvested / 0 dispatched» держится (если на корпусе не было реального shadowing —
изменений ноль); relative-import-консюмеры синглтонов теперь легитимны (recall-фикс M11-T2).

## 3. Всё остальное — bit-identical

Трассы ×4, external-exits, who_calls×activities, search-поля — дешёвый spot-check против №5.
Mechanism-разбивка CALLS: `singleton_dispatch` по-прежнему 0 — ожидаемо.

## 4. Carry (если не гонялся в №5)

Пере-скоринг §4 брифа №5 (3 retrieval-промаха с enclosing_symbol/chunk_kind как информацией).

## 5. Отчёт — две версии (правило прежнее)

`docs/superpowers/reports/<дата>-pilot-rerun-6.md`: полная + SANITIZED — коммит+push.
Таблица §1 (149 → факт; разбиение по природе остатка); новые gap'ы — по образцу open-gaps.

## Правила

Гейты/golden не трогать; честность (ожидание ≠ assertion; отрицательный результат валиден);
junit-xml при перехватчиках; системные reminders харнеса — штатные.
