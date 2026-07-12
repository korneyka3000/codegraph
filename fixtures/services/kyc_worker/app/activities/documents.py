import os

from temporalio import activity

from app.clients.document_management_client import DocumentManagementClient


@activity.defn
async def verify_documents(order_id: str) -> str:
    client = DocumentManagementClient(
        base_url=os.environ["DOCUMENT_MANAGEMENT_URL"],
    )
    doc = await client.get_document(order_id)
    return doc.get("status", "unknown")
