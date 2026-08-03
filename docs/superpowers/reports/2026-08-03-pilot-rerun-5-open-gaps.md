# Остаточные gap'ы после M10 re-run №5 — классметодный `(cls)`-суффикс + переоценка singleton

> Дата: 2026-08-03. Продолжение `2026-08-03-pilot-rerun-5.md`. M10 закрыл три входа MCP-пилота
> (who_calls×активности, search enclosing/chunk_kind, external-поля), НО главный числовой
> ожидание (dropped CALLS 149 → 40–70) не достигнуто. Разбор вскрыл: (1) переоценку «singleton»
> кейса и (2) новый механический gap R6. Вход для фикс-агента: корень + доказательство +
> направление. Правило прежнее: regression-тесты на синтетике (`fixtures/realstack/`), не на коде
> юзера; гейты/golden не трогать.

## R6 (высокий) — классметодные вызовы дропаются на scip-суффиксе `(cls)`

**Симптом.** Из 149 dropped CALLS — **48 это классметодные вызовы**, у которых **целевой узел
СУЩЕСТВУЕТ**, но dst-id несёт лишний хвост `(cls)`:

```
DROP dst : sym:camunda-gateway:`app.kyc_engine.activities.helpers`/SDFAnswersParser#parse().(cls)
node     : sym:camunda-gateway:`app.kyc_engine.activities.helpers`/SDFAnswersParser#parse().      ← существует
```

Проверено: `dst.endswith('(cls)')` → 48 рёбер; `dst[:-len("(cls)")] in nodes` → **48/48**. Т.е.
каждый из 48 восстановим простым отбрасыванием суффикса. Примеры целей: `CreateStepDetails#
from_decision().(cls)`, `GetVerificationRequestActivityInput#with_all_steps().(cls)`,
`CreateCaseInput#build_sdf_sof().(cls)` — pydantic-классметоды-фабрики (`@classmethod def from_...`),
массово используемые в activity/workflow-моделях.

**Корень.** scip-python помечает reference-occurrence классметодного вызова descriptor'ом с
`(cls)`-хвостом (disambiguator метода, вызванного через класс/cls), а построитель dst-node-id
(`resolvers/scip/symbols.py` → `symbol_to_node_id`, тот же путь, что для обычных CALLS) этот хвост
**не срезает**, поэтому dst не совпадает с материализованным узлом метода (`…#parse().`) и ребро
дропается на load. Обычные instance-методы такого хвоста не несут → резолвятся, а классметоды —
систематически теряются.

**Доказательство величины.** 48 рёбер, 48/48 с существующим базовым узлом. Правка снизит dropped
149 → **~101** (−32%), и это САМАЯ крупная реально-восстановимая доля (в отличие от 79
`registry.session`, которые невосстановимы — см. ниже).

**Направление фикса.** В `symbol_to_node_id` (или в нормализации dst перед сборкой EdgeRec в
`extractors/calls.py`) срезать трейлинг `(cls)`-дескриптор так же, как уже обрабатываются прочие
scip-суффиксы. Правила честности: срезать ТОЛЬКО ровный трейлинг `(cls)` (и парный `(method)` если
подтвердится симметрия — в срезе был 1 `.(method)`-дроп), не трогать сам `().`-suffix метода.
**Тест:** синтетик — класс с `@classmethod def make(cls)` + вызов `Cls.make()` в другом методе →
ожидать одно CALLS-ребро в `…#make().`, а не дроп; негативный пин: обычный instance-метод по-прежнему
резолвится без изменений.

## Переоценка (не gap, а исправление прошлой гипотезы) — `registry.session()` НЕ singleton-метод

MCP-пилот (§5) назвал 79 `registry.session()`-дропов «module-level singleton method-call» и оценил в
«53% восстановимых через M10». **Это было ошибкой диагноза.** Факт (`verification-requests/app/db/
registry.py`): `session` — **аннотация класса** (`session: Callable[..., AsyncSession]`, `:14`),
реальное значение — рантайм `self.session = sessionmaker(...)` в `setup()` (`:47`). Узла-метода
`_DBRegistry#session` не существует; `registry.session()` зовёт callable-**атрибут** (фабрику).
M10 singleton-резолв отработал **корректно** (fail-closed: кандидат построен, class-shape
верифицирован, узла нет → отказ без ложного ребра), но резолвить тут нечего. Эти 79 —
**принципиально динамические, невосстановимые статикой** (как и 5 `throttle`/`_driver_maker`
callable-атрибутов). Честный статус: не долг механизма, а свойство кода (SQLAlchemy sessionmaker,
присвоенный в setup).

**Опционально (низкий приоритет, для полноты графа).** Если нужна видимость таких «attribute-held
callable» вызовов — можно эмитить УЗЕЛ-атрибут для аннотированных class-level `name: Callable[...]`
полей и вешать на него CALLS (resolution=heuristic, mechanism="attr_callable"). Но: это не метод,
семантика слабая, и 79/84 — SQLAlchemy-инфраструктура (session-фабрика), а не бизнес-хоп. Рекомендация:
**не тратить на это M11**; приоритет — R6.

## Мелкое / принятое (без изменений с прошлых прогонов)

- `producer_unresolved_channel = 1` (динамический топик в обёртке; покрыт enum-идиомом).
- NEXT_SEGMENT по Kafka = 0 (контрагенты вне 3-сервисного среза).
- search: клиент/сервер-неоднозначность `add_linked_customers_v2` — `service`-хинт помогает, точный
  `_v2`-символ не top-1 (сиблинг v1/v2 + чанк-гранулярность); метрику не крутим (anti-curve-fit).

## Приоритеты для фикс-агента (M11)

1. **R6 (классметодный `(cls)`-суффикс)** — механический, детерминированный, 48/48 восстановимы,
   dropped 149→~101. Самая крупная реальная доля.
2. НЕ трогать «singleton session» долг — он мнимый (динамический атрибут, корректно отклонён).
   Опциональный attr-callable-узел — низкий приоритет, слабая семантика.

## Воспроизведение / доказательная база

Граф `pilot_kyc`. R6: `SELECT src,dst FROM edges WHERE type='CALLS'` минус `nodes.id` →
`d.endswith('(cls)')` = 48, `d[:-5] in nodes` = 48. singleton: `claims kind='module_singletons'`
(92 харвест), `edges props.mechanism='singleton_dispatch'` (0 эмитировано). `registry.session`:
`verification-requests/app/db/registry.py:14,47`. Наружу — только структурные факты и числа.
