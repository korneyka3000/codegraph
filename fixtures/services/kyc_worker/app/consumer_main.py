import json

from aiokafka import AIOKafkaConsumer

from app.consumers import orders  # noqa: F401  (регистрация хэндлеров)
from app.consumers.base import EVENT_HANDLERS


async def run_consumer() -> None:
    consumer = AIOKafkaConsumer("orders.events", bootstrap_servers="kafka:9092")
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            handler = EVENT_HANDLERS.get(event["event_type"])
            if handler is not None:
                await handler(event["payload"])
    finally:
        await consumer.stop()
