# Бриф: re-run №2 после M7 (рабочая машина) — HTTP-якорение, producer-топики, сигналы

> Задание для Claude Code-сессии на рабочей машине. Пользователь скажет: «прогони re-run по
> этому брифу». Baseline — `docs/superpowers/reports/2026-07-23-pilot-rerun.md` (+ open-gaps
> R1/R2/R3 там же рядом). M7 закрыл все три: строгий HTTP-матчинг + env→service якорение,
> settings/enum-источники топиков, Temporal signals как каналы. Проверяем на реальных трёх
> сервисах. Смоук: `git pull` (HEAD ≥ 2da3be1), `docker compose up -d`, `uv sync`,
> `uv run pytest -m "scip and falkordb" tests/eval/test_m7_gate.py -q --junit-xml=/tmp/g.xml`
> — зелёный = все механизмы работают здесь.

## 1. Дополнения workspace yaml (формы — сверить FQN с реальным кодом)

**HTTP-якорение (R1) — КРИТИЧЕСКАЯ оговорка:** в реальных клиентах `self.host`
присваивается в **BaseClient-конструкторе** (базовый класс) — auto-anchor это НЕ увидит
(TRACKED LIMITATION: наследование не обходится вовсе). Для static/1.0 нужен **явный**
`base_url` на идиоме:

```yaml
      http_clients:
        - name: decorator-sdk
          file_glob: "**/clients/*.py"
          class_glob: "*Client"
          route_from: { decorator: "path_template", arg: 0 }
          call: "driver.fetch_content|driver.fetch"
          verb_from: { request_ctor: "Request|ProxyRequest", enum: "Method" }   # ProxyRequest — dm-verb_unresolved=15 фикс
          base_url: { env: "SERVICE_VERIFICATION_REQUESTS_URL" }   # ← ЯВНО, per-клиент (или {settings: ...})
```
Клиентов с разными таргетами — отдельные идиом-записи (сузьте file_glob/class_glob на
каждого). Плюс env→service карта из helm:

```yaml
env_sources:
  - .helm/values/prod/values.yaml     # путь относительно workspace yaml; нет файла = громкая ошибка;
                                       # нерендеренный helm-шаблон внутри = warn-and-skip
```
Хостнейм `SERVICE_*_URL` матчится по ПЕРВОМУ DNS-лейблу == имя сервиса воркспейса
(`verification-requests.kyc.svc...` → `verification-requests`).

**Producer-топики (R2)** — два варианта, оба честные:

```yaml
      producers:
        # (a) enum-fanout на обёртке: все возможные топики, conf 0.8, mechanism=enum_fanout
        - name: publisher-enum
          call: "app.services.producer.KYCEventPublisher.publish"
          channel: { kind: kafka_topic, name_from: { enum: "app.models.enums.KycTopicName" } }
        # (b) outbox-Event с топиком из Settings-дефолта: static/1.0 при строковом дефолте,
        #     ${ENV}-placeholder при env-only поле
        - name: outbox-event
          call: "app.models.outbox.Event"
          channel: { kind: kafka_topic, name_from: { settings: "KafkaSettings.step_changed_topic" } }
```
Оговорки: fanout будет over-approximate NEXT_SEGMENT против любого consumer'а любого
member-топика (conf-дисконт есть); если `SettingsConfigDict(env_prefix=...)` живёт только
в БАЗОВОМ Settings-классе — env-имена полей подкласса не соберутся (TRACKED LIMITATION;
workaround: повторить model_config в подклассе или per-field alias).

**Сигналы (R3)** — ноль конфига. Ожидание: ≈34 роли TemporalSignalHandler (signal/update —
с каналами, query — только роль), до ≈45 sender-PRODUCES; senders с переменным именем —
в счётчик `signal_name_unresolved` (stdlib `signal.signal` отфильтрован).

## 2. Чекпойнты (vs baseline re-run №1)

| Метрика | Baseline | Ожидание M7 |
|---|---|---|
| CALLS_HTTP ложные static | 3 | **0** (три пилотных пары → unresolved ЛИБО корректный сервис) |
| CALLS_HTTP корректные anchored static/1.0 | 0 | **> 0** (с явными base_url per-клиент) |
| unanchored | 41 unresolved | ≤ heuristic/0.7 уникальные; неоднозначные → unresolved |
| PRODUCES | 0 | **> 0** (settings static и/или enum-fanout 0.8) |
| temporal_signal каналы | — | ≈ named-хендлеры; sender↔handler пары на именованных |
| NEXT_SEGMENT | 3 (все ложные) | ложных 0; сигнальные/HTTP/kafka-пары > 0 |
| verb_unresolved | 15 | ↓ (ProxyRequest-альтернатива) |

Примечание: правка любого Settings-файла под `--incremental` даст
`stale_escalation="class_attrs_changed"` и полный re-extract сервиса — так задумано.

## 3. Трасс-вердикт (главный)

`codegraph trace` на 1–2 реальных процессах, глазами с пользователем: цепочка
**Kafka-событие → consumer.process_event → signal → @workflow.signal-хендлер → activity →
HTTP → чужой роут** должна отображаться в сегментах (сигнальный хоп — новый). Плюс
relay-паттерн (signal-хендлер с локальными вызывающими) — приглядеться к сегментации
(ledger-⚠️ M7-T4: unit-proven, end-to-end впервые здесь).

## 4. Отчёт — две версии (правило прежнее)

`docs/superpowers/reports/<дата>-pilot-rerun-2.md`: полная (остаётся) + SANITIZED (наружу:
числа/вердикты, без имён). Таблица чекпойнтов §2 с фактами; итерации идиом; трасс-вердикт;
новые gap'ы, если вскроются (по образцу open-gaps: корень + file:line + направление).
Санитизированную — пользователю на вычитку; коммит + push.

## Правила

Гейты/golden не трогать; фиксы — минимальные, с синтетическими тестами; честность
(отрицательный результат валиден); junit-xml при перехватчиках вывода; системные reminders
харнеса — штатные.
