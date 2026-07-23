from kyc_base_consumer.base import BaseConsumer

from app.events import DocSubmittedEvent


class ConsumerSettings:
    def __init__(self, topic: str) -> None:
        self.topic = topic


class DocSubmittedConsumer(BaseConsumer[DocSubmittedEvent]):
    """Real convention: `class OCRDataConsumer(BaseConsumer[OCRDataEvent])` -- the
    business handler is the OVERRIDDEN `process_event`; the raw read-loop lives in
    the (out-of-tree, unresolvable here) shared-lib base -- GAPS §5/pilot gap 4."""

    def __init__(self, config: ConsumerSettings) -> None:
        self.config = config

    async def process_event(self, event: DocSubmittedEvent) -> bool:
        return True
