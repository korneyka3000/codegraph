from temporalio import workflow


@workflow.defn
class NotifyWorkflow:
    """Child workflow started via `workflow.start_child_workflow` (GAPS §4/pilot
    gap 3) -- a leaf: nothing further to notify in this synthetic fixture."""

    @workflow.run
    async def run(self, doc_uid: str) -> None:
        return None
