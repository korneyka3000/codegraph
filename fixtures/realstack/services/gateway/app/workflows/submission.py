from datetime import timedelta

from temporalio import workflow

from app.activities.docs import DocActivities
from app.workflows.notify import NotifyWorkflow


@workflow.defn
class DocSubmissionWorkflow:
    def __init__(self) -> None:
        self._approved_docs: list[str] = []

    # M7 T4 (OPEN R3): explicit-name signal handler -- the async half of the signal
    # channel. temporal_ext reads the decorator's own name= string literal as the
    # channel identity (chan:temporal_signal:doc-approved), CONSUMES static/1.0;
    # the sender side lives in worker's DocSubmittedConsumer.process_event
    # (handle.signal("doc-approved", ...) -- a legitimate cross-service hop, the
    # channel is the bridge).
    @workflow.signal(name="doc-approved")
    async def doc_approved(self, doc_uid: str) -> None:
        self._approved_docs.append(doc_uid)

    # M7 T4: @workflow.query is deliberately ROLE-ONLY (TemporalSignalHandler +
    # signal_kind="query" node props, NO channel/edge -- a query is a synchronous
    # read, not an async boundary). The M7 gate proves this negatively: golden
    # lists no channel/edge for it, and PRODUCES/CONSUMES P=R=1.0 would break if
    # one ever appeared.
    @workflow.query
    def approval_state(self) -> list[str]:
        return self._approved_docs

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
