import json

from aiokafka import AIOKafkaProducer

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
        await _producer.start()
    return _producer


async def emit_document_indexed(doc_id: str) -> None:
    producer = await get_producer()
    await producer.send("documents.indexed", json.dumps({"doc_id": doc_id}).encode())
