"""Outbox repository (M7 T2 settings-source leg): the `Event(...)` ctor call below
is the producer call-site workspace.yaml's outbox-doc-events idiom matches
(`call: "app.models.outbox.Event"`). Note the topic argument is
`self._settings.doc_events_topic` -- a dynamic attribute, exactly the OPEN R2
shape resolve_value_spec alone can never resolve; the idiom's
`name_from: {settings: ...}` sidesteps the call-site entirely and reads the SAME
field's string-literal default from the service-wide ClassAttrIndex."""

from app.config import GatewaySettings
from app.models.outbox import Event


class OutboxRepository:
    def __init__(self, session) -> None:
        self._session = session
        self._settings = GatewaySettings()

    def add_document_event(self, payload: str) -> None:
        event = Event(topic=self._settings.doc_events_topic, payload=payload)
        self._session.add(event)
