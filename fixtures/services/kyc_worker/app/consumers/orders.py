from app.consumers.base import register_handlers
from app.workflows.kyc import KycWorkflow
from temporalio.client import Client


async def _temporal() -> Client:
    return await Client.connect("temporal:7233")


async def handle_order_created(payload: dict) -> None:
    client = await _temporal()
    await client.start_workflow(
        KycWorkflow.run,
        payload,
        id=f"kyc-{payload['order_id']}",
        task_queue="kyc",
    )


register_handlers({"OrderCreated": handle_order_created})
