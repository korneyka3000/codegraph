"""Builtin-идиомы: те же модели, что парсятся из YAML пользователя.

fastapi/temporal — структурные экстракторы (паттерны декораторов зашиты в
extractors M2); их присутствие в списке включает соответствующий экстрактор,
поэтому ServiceIdioms у них пустые.
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
