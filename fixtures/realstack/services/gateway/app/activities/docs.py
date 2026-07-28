from temporalio import activity
from temporalio.client import Client

from app.clients.audit_client import AuditClient
from app.clients.doc_client import DocClient
from app.services.publisher import KYCEventPublisher


class DocActivities:
    def __init__(self) -> None:
        self._doc_client = DocClient(host="http://worker")
        self._publisher = KYCEventPublisher()
        # M9 T1 realstack leg: external HTTP target (audit.ext.prod.env, no
        # workspace service) -- see app/clients/audit_client.py's own docstring.
        self._audit_client = AuditClient(host="http://audit-external")

    @activity.defn
    async def fetch_document_content(self, doc_uid: str) -> dict:
        # Gap 1: reaches the decorator-SDK HTTP client (DocClient.fetch_document).
        return await self._doc_client.fetch_document(doc_uid)

    @activity.defn
    async def publish_submitted_event(self, doc_uid: str, topic_name: str) -> None:
        # Gap 5: reaches the producer wrapper (KYCEventPublisher.publish).
        await self._publisher.publish(doc_uid, topic_name, doc_uid)
        # M9 T1 realstack leg: external HTTP target -- reachable from THIS trace's
        # own entrypoint (submit_document -> DocSubmissionWorkflow.run -> HERE via
        # INVOKES_ACTIVITY), so the entry's own trace segment gains a new exit into
        # the external channel (see app/clients/audit_client.py's own docstring).
        await self._audit_client.submit_audit_event(doc_uid)
        # M8 T3 (rerun-2 R5 realstack leg): same-service TYPED signal send -- a typed
        # ref to the M7 T4 signal handler (DocSubmissionWorkflow.doc_approved),
        # resolved via ref_symbol_lookup at extraction time (temporal_ext.py's M8 T2
        # fix) and linked (linking/signal_send.py) onto the SAME
        # chan:temporal_signal:doc-approved channel the pre-existing cross-service
        # STRING-based sender (worker's DocSubmittedConsumer.process_event, below)
        # already produces into -- both legs proven end-to-end on the same real
        # fixture, with honest resolutions (typed -> static/1.0 via linking;
        # string -> heuristic/0.6 direct). Local import breaks the
        # workflows<->activities module cycle (submission.py already imports THIS
        # module at its own top level for its own execute_activity_method refs).
        from app.workflows.submission import DocSubmissionWorkflow

        client = await Client.connect("localhost:7233")
        handle = client.get_workflow_handle(f"submit-{doc_uid}")
        await handle.signal(DocSubmissionWorkflow.doc_approved, doc_uid)
