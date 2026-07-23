# Бриф: re-run пилота после M6 (рабочая машина)

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «прогони
> re-run по этому брифу». Предыстория: первый пилот дал карту gap'ов
> (`docs/superpowers/reports/2026-07-23-pilot-real-services-gaps.md`, далее GAPS) —
> все 5 закрыты вехой M6 (план `docs/superpowers/plans/2026-07-23-m6-real-stack-idioms.md`,
> синтетическое доказательство — `fixtures/realstack/` + `tests/eval/test_m6_gate.py`).
> Задача re-run: подтвердить закрытие на РЕАЛЬНЫХ трёх сервисах против baseline GAPS §1/§8.

## 0. Обновление и смоук

```bash
git pull origin main            # HEAD должен быть >= 1ab5939
docker compose up -d
uv sync
uv run codegraph doctor
uv run pytest -m "scip and falkordb" tests/eval/test_m6_gate.py -q --junit-xml=/tmp/m6gate.xml
```
M6-гейт зелёный = тулчейн и все пять механизмов работают на этой машине. Дальше — реальный workspace.

## 1. Идиомы в workspace yaml (тот же 3-сервисный workspace из GAPS §8)

Добавить к сервисам (FQN'ы СВЕРИТЬ с реальным кодом — ниже формы из GAPS, имена могут отличаться):

```yaml
# camunda-gateway
    idioms:
      http_clients:
        - name: decorator-sdk
          file_glob: "**/clients/*.py"
          class_glob: "*Client"
          route_from: { decorator: "path_template", arg: 0 }
          call: "driver.fetch_content|driver.fetch"      # оба альтерната полными хвостами
          verb_from: { request_ctor: "Request", enum: "Method" }
      producers:
        - name: kyc-publisher-wrapper
          call: "app.services.producer.KYCEventPublisher.publish"   # сверить FQN
          channel:
            kind: event_type
            event_type_from: { const: "<EventClass>" }   # если тип фиксирован точкой вызова;
                                                          # иначе topic: {arg: 1} → честный unresolved
# каждый сервис с консьюмерами
    idioms:
      consumers:
        - name: base-consumer
          kind: base_class
          base_class: "kyc_base_consumer.base.BaseConsumer"   # сверить FQN shared-lib
          handler_method: "process_event"
          event_type_from: { generic_arg: 0 }
          topic: { attr: "self.config.topic" }
```

Примеры с комментариями — `codegraph.example.yaml` (секции decorator-sdk / base_class / wrapper).

## 2. Прогон и чекпойнты (против baseline GAPS §1: claims = только temporal_start_mark:4, ноль async-рёбер)

`uv run codegraph index <ws>.yaml --no-embed` → сверить по отчёту/`.codegraph/report.json`/staging:

| Чекпойнт | Ожидание | Если нет |
|---|---|---|
| INVOKES_ACTIVITY | ≈80 (camunda; GAPS §3) | receiver != `workflow`? иные имена вызовов? |
| temporal_start marks | 4 → 7 (+3 start_child_workflow) | `client.execute_workflow`-форма вне скоупа (задокументировано) |
| http_call claims | ≈55 (42 fetch_content + 13 fetch) → CALLS_HTTP > 0 | смотреть ЖЁЛТЫЕ строки отчёта: url/verb/route_unresolved — миссы теперь видимы, не молчание |
| CONSUMES | по одному на каждый `BaseConsumer[...]`-подкласс → `chan:event_type:*` | alias-импорт базы (`import ... as X`) молча миссит — задокументировано; грепнуть алиасы |
| PRODUCES | обёртка: с const-event_type → канал; с динамическим топиком → `producer_unresolved_channel` в отчёте (ожидаемо) | |
| CONTAINS topic→event | при `topic: {attr: ...}` | |
| NEXT_SEGMENT | > 0 (событийные пары producer↔consumer по event_type) | event_type-имена совпадают между producer-const и consumer-generic? |
| confidence | с venv consumer-рёбра могут быть static/1.0 (лучше фикстурного 0.6) — норма | |

Итерации идиом (уточнение FQN/глобов) — нормальная часть; фиксировать каждую.

## 3. Трассировка (главный вердикт)

`codegraph trace "<svc>:<METHOD> <path>" <ws>.yaml --format mermaid` на 1–2 реальных процессах —
проверить С ПОЛЬЗОВАТЕЛЕМ глазами: route → workflow → activity → publish → event-канал →
process_event; SDK-вызов → CALLS_HTTP → чужой роут. Длинные сегменты схлопываются (`--full` отключает).

## 4. Отчёт — две версии (правило прежнее)

`docs/superpowers/reports/<дата>-pilot-rerun.md`: полная (остаётся здесь) + **SANITIZED**
(наружу: числа/проценты/вердикты/категории, БЕЗ qualified-имён/путей/кода; staging.db и .scip
не выносить). Таблица чекпойнтов §2 c фактами, дельта vs GAPS §1, итерации идиом, вердикт по
трейсу. Санитизированную показать пользователю на вычитку; коммит + push (обе или только
санитизированную — решает пользователь).

## Правила

Гейты/golden не трогать; баги — минимальные fix-коммиты с синтетическими тестами; честность
(отрицательный результат валиден); крэш = находка №1 (диагноз до корня, образец GAPS §7);
системные reminders харнеса — штатные; при перехватчиках pytest-вывода — junit-xml.
