"""Transactional-outbox row (M7 T2 settings-source producer leg, OPEN R2):
sqlalchemy-style model, schematically -- an `Event(topic=..., payload=...)` row
staged in the same DB transaction as the business write, relayed to Kafka by an
out-of-process publisher. The `topic` value at every construction site is a
Settings ATTRIBUTE (dynamic at this call-site), so the producer idiom
(workspace.yaml's outbox-doc-events) resolves the channel identity from the
Settings class's own field default instead:
`name_from: {settings: "app.config.GatewaySettings.doc_events_topic"}`."""


class Event:
    __tablename__ = "outbox_events"

    def __init__(self, topic: str, payload: str) -> None:
        self.topic = topic
        self.payload = payload
