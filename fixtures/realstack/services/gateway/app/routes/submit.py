from fastapi import APIRouter
from temporalio.client import Client

from app.workflows.submission import DocSubmissionWorkflow

router = APIRouter()


async def _get_client() -> Client:
    return await Client.connect("localhost:7233")


@router.post("/submit")
async def submit_document(doc_uid: str, topic_name: str) -> dict:
    client = await _get_client()
    await client.start_workflow(
        DocSubmissionWorkflow.run,
        doc_uid,
        topic_name,
        id=f"submit-{doc_uid}",
        task_queue="gateway-tasks",
    )
    return {"doc_uid": doc_uid, "status": "submitted"}
