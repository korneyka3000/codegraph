"""Topic mirror (M7 T2 enum fan-out leg, OPEN R2a): `replicate`'s `topic_name`
argument is runtime data on every call -- the same permanently-dynamic-topic shape
as KYCEventPublisher.publish (GAPS section 6), but here the possible topics DO have a
static identity: the DocTopicName enum (app/topics.py). workspace.yaml's
doc-topic-mirror idiom matches call-sites of THIS method and fans PRODUCES out to
every enum member (heuristic/0.8, mechanism=enum_fanout)."""


class TopicMirror:
    async def producer(self):
        raise NotImplementedError

    async def replicate(self, payload: str, topic_name: str) -> None:
        producer = await self.producer()
        await producer.send_and_wait(topic=topic_name, value=payload)
