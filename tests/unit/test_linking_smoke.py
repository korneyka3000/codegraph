"""M2 T7 wiring-integration smoke test: analyze ALL THREE real fixture services through
the DEGRADED fallback path (no real scip-python -- `_AlwaysFailRunner`, same technique
as test_pipeline_analyze.py, so this stays a fast unit test) using the REAL
fixtures/workspace.yaml config, then run link_workspace(cfg, staging) and inspect what
derives from genuinely staged (not hand-built synthetic) data.

Scope, deliberately: this is NOT the M2 milestone gate (that's T9's `test_m2_gate.py`,
run against REAL scip-python + FalkorDB, comparing segment-by-segment against
fixtures/golden/traces.yaml). Several resolutions this scenario WOULD need for a single
unbroken orders->kyc->doc-mgmt chain are documented gaps of the degraded fallback
resolver, already pinned in test_pipeline_analyze.py:
  - kyc-worker's dispatch_dict consumer (`handle_order_created`) needs a SCIP ref at the
    dispatch-dict VALUE's span to resolve its CONSUMES edge -- degraded fallback never
    lays one down (see test_analyze_kafka_and_temporal_can_both_be_active_together's own
    comment). So `handle_order_created` never gets a CONSUMES edge here at all.
  - `temporal_start_mark` claims need the same kind of argument-span ref (start_workflow's
    arg0) -- degraded fallback produces ZERO such claims (see
    test_analyze_temporal_active_degraded_fallback_cannot_resolve_invokes_activity).
  - INVOKES_ACTIVITY (workflow -> activity) needs an argument-span ref too -- absent here.

What DOES resolve under degraded fallback (proven below, all from REAL fixture files):
  - fastapi routes/HANDLES (pure decorator-text + AssignFact structure).
  - the outbox producer's event_type PRODUCES + its topic CONTAINS event edge (topic is a
    ValueSpec(const=...), the event_type comes from a literal call arg -- both resolve
    via RECEIVER-tier matching + literal-value resolution, no SCIP needed).
  - kyc-worker's topic-level consumer (`run_consumer`'s bare `AIOKafkaConsumer("orders.
    events", ...)` ctor call) -- IMPORT_NAME tier, again no SCIP needed for a literal arg.
  - document-management's builtin aiokafka producer (`producer.send("documents.indexed"
    , ...)`) -- same IMPORT_NAME/literal-value story.
  - http_client_ext's claims (zero SCIP dependency at all, per its own module docstring)
    match cleanly onto document-management's real routes.

Net result: S7 DOES derive the two genuine cross-service NEXT_SEGMENT transitions the
golden trace needs -- orders-api -> kyc-worker (via the kafka topic-containment pairing)
and kyc-worker -> document-management (via the http CALLS_HTTP/HANDLES pairing) -- proving
the derivation mechanism itself is correct end-to-end against real fixture data. What it
can NOT show here is those two transitions chained through a single path (that requires
kyc-worker's OWN internal consumer->workflow->activity->http-client wiring, which needs
real scip). This is exactly the boundary the task brief draws: "смоук на том, что
деривация от staged-данных работает"; "полная цепочка — гейт T9 с реальным scip".
"""

from __future__ import annotations

from pathlib import Path

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.staging import Staging

WORKSPACE = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"

GET_DOCUMENT_CLIENT = (
    "sym:kyc-worker:`app.clients.document_management_client`/"
    "DocumentManagementClient#get_document()."
)
CREATE_DOCUMENT_CLIENT = (
    "sym:kyc-worker:`app.clients.document_management_client`/"
    "DocumentManagementClient#create_document()."
)
GET_DOCUMENT_HANDLER = "sym:document-management:`app.routes.documents`/get_document()."
CREATE_DOCUMENT_HANDLER = "sym:document-management:`app.routes.documents`/create_document()."
ORDER_SERVICE_PLACE = "sym:orders-api:`app.services.order`/OrderService#place()."
RUN_CONSUMER = "sym:kyc-worker:`app.consumer_main`/run_consumer()."
HANDLE_ORDER_CREATED = "sym:kyc-worker:`app.consumers.orders`/handle_order_created()."


class _AlwaysFailRunner:
    """Same technique as test_pipeline_analyze.py's own _AlwaysFailRunner -- forces the
    degraded fallback path without a real scip-python subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def _analyze_all_degraded(tmp_path) -> tuple[Staging, dict]:
    cfg = load_workspace(WORKSPACE)
    staging = Staging(tmp_path / "s.db")
    active_idioms = frozenset(cfg.builtin_idioms)
    for svc in cfg.services:
        analyze_service(
            svc, staging, tmp_path / "cache", runner=_AlwaysFailRunner(),
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
    return staging, link_workspace(cfg, staging)


def test_degraded_three_service_run_derives_both_cross_service_transitions(tmp_path):
    staging, report = _analyze_all_degraded(tmp_path)

    next_segments = {(e.src, e.dst): e for e in staging.iter_edges() if e.type == "NEXT_SEGMENT"}
    assert report["next_segments"] == len(next_segments) == 3

    # orders-api -> kyc-worker: via kafka topic-containment (OrderCreated event contained
    # by orders.events topic; run_consumer subscribes at the TOPIC level).
    orders_to_kyc = next_segments[(ORDER_SERVICE_PLACE, RUN_CONSUMER)]
    assert orders_to_kyc.props["via_channel_id"] == "chan:event_type:OrderCreated"
    assert orders_to_kyc.resolution == "heuristic"  # PRODUCES is heuristic/0.8 (RECEIVER tier)
    assert abs(orders_to_kyc.confidence - 0.48) < 1e-9  # 0.8 (PRODUCES) * 0.6 (CONSUMES)

    # kyc-worker -> document-management: via http (both http_call claims resolved).
    kyc_to_doc_get = next_segments[(GET_DOCUMENT_CLIENT, GET_DOCUMENT_HANDLER)]
    assert kyc_to_doc_get.resolution == "static" and kyc_to_doc_get.confidence == 1.0
    assert kyc_to_doc_get.props["via_channel_id"] == (
        "chan:http:document-management:GET /documents/{doc_id}"
    )

    kyc_to_doc_create = next_segments[(CREATE_DOCUMENT_CLIENT, CREATE_DOCUMENT_HANDLER)]
    assert kyc_to_doc_create.resolution == "static" and kyc_to_doc_create.confidence == 1.0

    # Documented degraded-mode gap: handle_order_created's dispatch_dict CONSUMES edge
    # needs a value-span SCIP ref the fallback resolver never lays down (see module
    # docstring), so it never appears as either endpoint of a NEXT_SEGMENT edge here.
    assert not any(HANDLE_ORDER_CREATED in pair for pair in next_segments)


def test_degraded_three_service_run_resolves_both_http_claims_cleanly(tmp_path):
    _staging, report = _analyze_all_degraded(tmp_path)
    assert report["calls_http"] == 2
    assert report["calls_http_unresolved"] == 0


def test_degraded_three_service_run_produces_no_temporal_marks(tmp_path):
    """Documented gap (test_pipeline_analyze.py): start_workflow's arg0 needs a SCIP ref
    at an argument span, which the degraded fallback never provides -- zero
    temporal_start_mark claims are ever staged through this path, so link_workspace has
    nothing to mark. Full resolution is T9's real-scip gate."""
    _staging, report = _analyze_all_degraded(tmp_path)
    assert report["marks"] == 0


def test_degraded_three_service_run_materializes_both_process_anchors(tmp_path):
    staging, report = _analyze_all_degraded(tmp_path)
    assert report["processes"] == 2
    procs = {n.props["source"]: n for n in staging.iter_nodes() if n.kind == "BusinessProcess"}
    assert procs.keys() == {"config", "temporal"}
    assert procs["config"].id == "proc:order-kyc-onboarding"
    assert procs["temporal"].props["entrypoint_id"] == (
        "sym:kyc-worker:`app.workflows.kyc`/KycWorkflow#"
    )
