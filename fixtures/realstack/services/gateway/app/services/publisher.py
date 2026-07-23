"""KYCEventPublisher: business-level wrapper over the raw aiokafka producer call
(GAPS §6/pilot gap 5) -- `publish`'s own `topic_name` argument is dynamic
(whatever the caller passes), so there is no literal topic to resolve statically
here; the producer idiom (workspace.yaml) pins this wrapper's `event_type`
instead, matching producer to consumer WITHOUT a shared literal topic."""


class KYCEventPublisher:
    async def producer(self):
        raise NotImplementedError

    async def publish(self, body: str, topic_name: str, uid: str) -> None:
        producer = await self.producer()
        await producer.send_and_wait(topic=topic_name, value=body, key=uid)
