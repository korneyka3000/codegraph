from datetime import timedelta

from temporalio import workflow

from app.activities.docs import DocActivities
from app.workflows.notify import NotifyWorkflow


@workflow.defn
class DocSubmissionWorkflow:
    @workflow.run
    async def run(self, doc_uid: str, topic_name: str) -> str:
        # Gap 2: workflow.execute_activity_method (bound-method ref, not a bare
        # activity function) -- both activities below are methods of DocActivities.
        await workflow.execute_activity_method(
            DocActivities.fetch_document_content,
            doc_uid,
            start_to_close_timeout=timedelta(minutes=5),
        )
        await workflow.execute_activity_method(
            DocActivities.publish_submitted_event,
            doc_uid,
            topic_name,
            start_to_close_timeout=timedelta(minutes=5),
        )
        # Gap 3: workflow.start_child_workflow (child-workflow variant of start_workflow).
        await workflow.start_child_workflow(
            NotifyWorkflow.run,
            doc_uid,
            id=f"notify-{doc_uid}",
        )
        return doc_uid
