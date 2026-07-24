from kyc_base_consumer.base import BaseConsumer

from app.events import DocSubmittedEvent


class ConsumerSettings:
    def __init__(self, topic: str) -> None:
        self.topic = topic


class DocSubmittedConsumer(BaseConsumer[DocSubmittedEvent]):
    """Real convention: `class OCRDataConsumer(BaseConsumer[OCRDataEvent])` -- the
    business handler is the OVERRIDDEN `process_event`; the raw read-loop lives in
    the (out-of-tree, unresolvable here) shared-lib base -- GAPS §5/pilot gap 4."""

    def __init__(self, config: ConsumerSettings, temporal_client) -> None:
        self.config = config
        self._temporal = temporal_client

    async def process_event(self, event: DocSubmittedEvent) -> bool:
        # M7 T4 (OPEN R3): sender half of the doc-approved signal channel -- a real
        # cross-service hop (this worker consumer signals gateway's
        # DocSubmissionWorkflow.doc_approved handler). temporal_ext matches ANY
        # `<handle>.signal("<literal>", ...)` call (receiver-agnostic by design,
        # heuristic/0.6, mechanism=temporal_signal); `get_workflow_handle` is
        # deliberately NOT a start_workflow-family callee, so no temporal_start
        # claim arises here.
        handle = self._temporal.get_workflow_handle(f"submit-{event.doc_uid}")
        await handle.signal("doc-approved", event.doc_uid)
        return True
