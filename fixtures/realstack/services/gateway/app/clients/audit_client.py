"""M9 T1 realstack leg (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3):
external HTTP target -- a client anchored, via the SAME env-anchoring path as
DocClient's own WORKER_URL (workspace.yaml's `base_url: {env: ...}` idiom field,
tier 1's PRIMARY registry/env_map lookup), to a REAL, known hostname
(audit.ext.prod.env, env_values.yaml's SERVICE_AUDIT_URL) that names NO workspace
service at all -- neither "gateway" nor "worker".

Proves linking/http_routes.py's tier-2a "external" split end-to-end against real
scip: the synthetic channel gets `external=True` + `external_host="audit.ext.prod.
env"` (honest boundary knowledge, not a modeling gap), `calls_http_external` counts
it separately from `calls_http_unresolved`, and the trace segment reaching this
call (via DocActivities.publish_submitted_event) holds its aggregate confidence
steady instead of being dragged down by this exit's own honest heuristic/0.5 (see
query/traverse.py's module docstring for the exact exclusion mechanism)."""

from app.clients.sdk import BaseClient, Method, Request, path_template


class AuditClient(BaseClient):
    """Same decorator-SDK convention as DocClient (doc_client.py) -- only the
    anchor differs: this class's own idiom (audit-client-decorator-sdk,
    workspace.yaml) points `base_url` at SERVICE_AUDIT_URL directly, which
    resolves to a real-but-outside-the-workspace hostname instead of a modeled
    service."""

    @path_template("/audit/events")
    async def submit_audit_event(self, doc_uid: str, **kwargs) -> dict:
        request = Request(Method.POST, self.host, doc_uid)
        return await self.driver.fetch_content(request)
