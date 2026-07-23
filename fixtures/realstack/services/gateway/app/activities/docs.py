from temporalio import activity

from app.clients.doc_client import DocClient
from app.services.publisher import KYCEventPublisher


class DocActivities:
    def __init__(self) -> None:
        self._doc_client = DocClient(host="http://worker")
        self._publisher = KYCEventPublisher()

    @activity.defn
    async def fetch_document_content(self, doc_uid: str) -> dict:
        # Gap 1: reaches the decorator-SDK HTTP client (DocClient.fetch_document).
        return await self._doc_client.fetch_document(doc_uid)

    @activity.defn
    async def publish_submitted_event(self, doc_uid: str, topic_name: str) -> None:
        # Gap 5: reaches the producer wrapper (KYCEventPublisher.publish).
        await self._publisher.publish(doc_uid, topic_name, doc_uid)
