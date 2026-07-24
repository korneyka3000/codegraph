"""Second decorator-SDK client (M7 T3 auto-anchor + T5 request_ctor alternatives).

Two deliberate contrasts with DocClient (doc_client.py):

  - AUTO-ANCHOR (OPEN R1): its idiom (workspace.yaml's status-client-proxy-sdk)
    declares NO base_url at all -- the target service is recovered from this
    class's OWN ctor-body `self.host = config.worker_url` assignment:
    http_client_ext joins the RHS tail ("worker_url") through
    ClassAttrIndex.field_by_name to GatewaySettings.worker_url's derived env name
    (SERVICE_WORKER_URL), which linking/env_map.py then resolves to the worker
    service via env_values.yaml's cluster hostname. NOT a BaseClient subclass, on
    purpose: the auto-anchor lookup performs no inheritance walk (http_client_ext's
    own TRACKED LIMITATION -- a base-ctor `self.host = ...` is invisible), so the
    assignment must live in the matched class's own body, as it does here.
  - VERB (M7 T5): the ctor is ProxyRequest, not Request -- matched only because the
    idiom spells `request_ctor: "Request|ProxyRequest"` ("|"-alternatives).

The route shape /api/v1/status/{doc_uid} is ALSO the funnel-negative probe (M7 T3
strict form): it segment-matches worker's real GET /api/v1/status/{doc_uid} route
exactly, and under the OLD bidirectional wildcard rule it would ALSO have matched
worker's all-params GET /{a}/{b}/{c}/misc route (claim-side {doc_uid} absorbing the
static "misc" tail) -- ambiguity, or worse. Strict route-side-only matching keeps
exactly one candidate; the M7 gate pins zero CALLS_HTTP into the misc route."""

from app.clients.sdk import Driver, Method, ProxyRequest, path_template
from app.config import GatewaySettings


class StatusClient:
    def __init__(self, config: GatewaySettings) -> None:
        self.host = config.worker_url
        self.driver = Driver()

    @path_template("/api/v1/status/{doc_uid}")
    async def fetch_status(self, doc_uid: str, **kwargs) -> dict:
        request = ProxyRequest(Method.GET, self.host, doc_uid)
        return await self.driver.fetch_content(request)
