from datetime import timedelta

from temporalio import workflow

from app.activities.documents import verify_documents


@workflow.defn
class KycWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> str:
        status = await workflow.execute_activity(
            verify_documents,
            payload["order_id"],
            start_to_close_timeout=timedelta(minutes=5),
        )
        return status
