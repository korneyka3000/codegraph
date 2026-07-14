"""Builtin-идиомы: те же модели, что парсятся из YAML пользователя.

fastapi/temporal — структурные экстракторы (паттерны декораторов зашиты в
extractors M2); их присутствие в списке включает соответствующий экстрактор,
поэтому ServiceIdioms у них пустые.

M2 T5 верификация паттернов (задача требовала "коррекции по реальности" -- по факту,
после live-прогона `idiom_match.match_calls` против реальных fixtures/services/*, все
call-паттерны ниже, что реально упираются в фикстуры, уже матчатся корректно; правки
строк паттернов НЕ потребовались, см. отчёт задачи для полного протокола проверки):

  - "aiokafka.AIOKafkaProducer.send" -- document_management/app/events/producer.py's
    `producer.send(...)`: aiokafka не резолвится реальным scip-python на этих фикстурах
    (нет пригодных для pyright тайп-стабов) -- STATIC тир не срабатывает НИКОГДА на этих
    файлах; RECEIVER тоже мимо (`_producer = AIOKafkaProducer(...)` -- глобальная
    переменная с ДРУГИМ именем, чем локальный `producer = await get_producer()` в
    call-сайте -- имена target'ов не совпадают, RECEIVER требует точного совпадения);
    матчится по самому слабому, IMPORT_NAME (`from aiokafka import AIOKafkaProducer` +
    имя метода "send") -- heuristic/0.6. Не меняли: паттерн УЖЕ корректен для этого пути;
    "*."-префиксация (рассматривалась как альтернатива для устойчивости к внутренней
    структуре пакета aiokafka) была бы РЕГРЕССИЕЙ -- IMPORT_NAME-тир матчит СЕГМЕНТЫ
    ПАТТЕРНА КАК ЛИТЕРАЛЫ (fnmatchcase используется только у STATIC-тира), так что
    "*.AIOKafkaProducer.send" искал бы импорт с target_module буквально "*" -- никогда
    не найдёт. "aiokafka.AIOKafkaProducer.send_and_wait" -- тот же класс/тир по
    построению (не покрыт ни одной фикстурой -- ни один `.send_and_wait(...)` вызов в
    fixtures/services не встречается).
  - "aiokafka.AIOKafkaConsumer" (ctor, kind=call) -- kyc_worker/app/consumer_main.py's
    `AIOKafkaConsumer("orders.events", ...)`: тот же провал STATIC/RECEIVER (нет
    receiver вовсе -- голый ctor-вызов), матчится IMPORT_NAME ctor-веткой (класс
    напрямую в `from aiokafka import AIOKafkaConsumer`) -- heuristic/0.6. Не меняли.
    "aiokafka.AIOKafkaConsumer.subscribe" -- структурно тот же паттерн, не покрыт
    фикстурами (`.subscribe(...)` нигде не вызывается).
  - confluent_kafka.*/faststream.* -- ни один паттерн вообще не встречается в
    fixtures/services (live grep подтвердил: ни confluent_kafka, ни faststream там не
    импортируются) -- не верифицируемо на этих данных, оставлены как задокументированный
    структурный best-effort (тот же формат, что уже проверенные aiokafka-паттерны).
"""

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    HttpClientIdiom,
    ProducerIdiom,
    ServiceIdioms,
    ValueSpec,
)

BUILTIN_IDIOMS: dict[str, ServiceIdioms] = {
    "fastapi": ServiceIdioms(),
    "temporal": ServiceIdioms(),
    "aiokafka": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="aiokafka-send",
                call="aiokafka.AIOKafkaProducer.send",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
            ProducerIdiom(
                name="aiokafka-send-and-wait",
                call="aiokafka.AIOKafkaProducer.send_and_wait",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="aiokafka-consumer-init",
                kind="call",
                call="aiokafka.AIOKafkaConsumer",
                topic=ValueSpec(arg=0),
            ),
            ConsumerIdiom(
                name="aiokafka-subscribe",
                kind="call",
                call="aiokafka.AIOKafkaConsumer.subscribe",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "confluent": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="confluent-produce",
                call="confluent_kafka.Producer.produce",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="confluent-subscribe",
                kind="call",
                call="confluent_kafka.Consumer.subscribe",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "faststream": ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="faststream-publisher",
                call="faststream.kafka.KafkaBroker.publisher",
                channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=0)),
            ),
        ],
        consumers=[
            ConsumerIdiom(
                name="faststream-subscriber",
                kind="decorator",
                decorator="broker.subscriber",
                topic=ValueSpec(arg=0),
            ),
        ],
    ),
    "aiohttp_client": ServiceIdioms(
        http_clients=[
            HttpClientIdiom(name="aiohttp-client-convention"),
        ],
    ),
}


def resolve_builtins(names: list[str]) -> ServiceIdioms:
    merged = ServiceIdioms()
    for name in names:
        if name not in BUILTIN_IDIOMS:
            known = ", ".join(sorted(BUILTIN_IDIOMS))
            raise KeyError(f"unknown builtin idiom {name!r}; known: {known}")
        src = BUILTIN_IDIOMS[name]
        merged.producers.extend(src.producers)
        merged.consumers.extend(src.consumers)
        merged.http_clients.extend(src.http_clients)
    return merged
