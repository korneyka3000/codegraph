"""M3 T2 real-shape regression: the actual "Order KYC onboarding" fixture chain
(fixtures/workspace.yaml's `processes` decl) through the REAL pipeline (analyze_service
x3, runner=None -> real scip-python; link_workspace) -- the MAIN regression anchor for
"PART_OF_PROCESS derivation is no longer inert" (M2 final review finding this task
starts from: max order was ALWAYS 0 on every real graph).

WHY real scip-python, not the degraded heuristic fallback (verified empirically per
this task's own brief instruction; initial report mis-stated degraded as "tops out at
order 1" -- reviewer-reproduced and corrected to 0, see the degraded test below which
now pins the true number executably):

  - degraded (fallback.resolve_service): the chain breaks at BOTH critical hops, so
    max PART_OF_PROCESS order == 0 across ALL processes --
      (a) `create_order -> OrderService.place`: `service.place(req)` is a METHOD call
          on a locally-typed variable (`service = OrderService(db)`); fallback has no
          type inference and only resolves calls whose callee NAME is a bare top-level
          def (same file or direct from-import) -- so degraded's only CALLS edge out of
          `create_order` targets the OrderService CLASS ctor, `place` gets NO incoming
          intra edge at all, `_entry_of(place)` returns place itself (its own
          disconnected root), and the place->run_consumer NEXT_SEGMENT edge is keyed
          away from `create_order`'s BFS entirely.
      (b) kyc-worker's dispatch_dict consumer idiom
          (`register_handlers({"OrderCreated": handle_order_created})`) resolves its
          handler via `ctx.ref_symbol_lookup` at the DICT VALUE's byte span -- a bare
          name reference, never itself a call site. `fallback.resolve_service` only
          ever builds ref rows from `facts.calls` (see resolvers/fallback.py's own
          docstring), so `handle_order_created` never gets a CONSUMES edge or a
          MessageConsumer role, and everything past it (workflow/activity/http-client/
          doc-mgmt handler) is likewise unreachable -- independent of _entry_of's own
          correctness.
  - real scip-python: populates a ref occurrence for EVERY identifier (not just call
    sites), so the dispatch_dict lookup resolves; `service.place(req)` and
    `client.get_document(...)` method calls also resolve via pyright's real type
    inference. Confirmed live: exactly the shape asserted below, max order == 2.

Markers: the real-scip test carries `scip` (npx/network) per-test -- NOT module-level,
because the degraded companion test deliberately runs in the DEFAULT suite (no scip, no
FalkorDB, ~0.1s): it pins the documented degraded limitation as an executable fact, so
this module's own docstring can't silently drift from reality again (exactly what
happened once already -- see above).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip.runner import ScipRunError, ScipRunner
from codegraph.stores.staging import Staging

FIXTURES = Path(__file__).parents[2] / "fixtures"


def _qualified_id(staging: Staging, service: str, qualified: str) -> str | None:
    for n in staging.iter_nodes():
        if n.service == service and n.qualified_name == qualified:
            return n.id
    return None


@pytest.mark.scip
@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_order_kyc_onboarding_reaches_max_order_two_with_real_scip(tmp_path):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    staging = Staging(tmp_path / "staging.db")
    cache_dir = tmp_path / "scip-cache"
    active_idioms = frozenset(cfg.builtin_idioms)
    try:
        for svc in cfg.services:
            report = analyze_service(
                svc, staging, cache_dir, runner=None,
                active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
            )
            assert report["degraded"] is False, (
                f"{svc.name}: expected real scip-python, got degraded ({report['reason']})"
            )

        link_report = link_workspace(cfg, staging)
        # "Order KYC onboarding" (cfg.processes) + KycWorkflow's own auto temporal
        # anchor -- both always materialize on this fixture (see
        # test_linking_processes.py's config+temporal coexistence test for the
        # synthetic version of this same assertion).
        assert link_report["processes"] == 2
        assert link_report["part_of_process_ambiguous"] == 0

        proc = next(
            n for n in staging.iter_nodes()
            if n.kind == "BusinessProcess" and n.name == "Order KYC onboarding"
        )

        create_order_id = _qualified_id(staging, "orders-api", "app.routes.orders.create_order")
        run_consumer_id = _qualified_id(staging, "kyc-worker", "app.consumer_main.run_consumer")
        handle_order_created_id = _qualified_id(
            staging, "kyc-worker", "app.consumers.orders.handle_order_created"
        )
        get_document_docmgmt_id = _qualified_id(
            staging, "document-management", "app.routes.documents.get_document"
        )
        missing = [
            name for name, nid in (
                ("create_order", create_order_id), ("run_consumer", run_consumer_id),
                ("handle_order_created", handle_order_created_id),
                ("get_document (doc-mgmt)", get_document_docmgmt_id),
            ) if nid is None
        ]
        assert not missing, f"expected qualified-name lookups missing from staged nodes: {missing}"

        part_of = {
            e.src: e.props["order"] for e in staging.iter_edges()
            if e.type == "PART_OF_PROCESS" and e.dst == proc.id
        }
        assert part_of == {
            create_order_id: 0,
            run_consumer_id: 1,
            handle_order_created_id: 1,
            get_document_docmgmt_id: 2,
        }
        assert max(part_of.values()) == 2  # the actual regression pin: pre-fix, always 0
    finally:
        staging.close()


class _AlwaysFailRunner(ScipRunner):
    """Forces the degraded (heuristic fallback) resolve path without any subprocess --
    same technique as tests/unit/test_pipeline_analyze.py's degraded-path tests."""

    def run(self, *args, **kwargs):
        raise ScipRunError("forced degraded for real-shape limitation pin")


def test_degraded_pipeline_stalls_at_order_zero_on_fixtures(tmp_path):
    """Executable pin of the documented degraded-mode limitation (module docstring
    above + linking/processes.py's "Resolver-quality dependency" paragraph): with the
    heuristic fallback resolver, max PART_OF_PROCESS order over the whole fixture
    workspace is 0 -- NOT because _entry_of is broken, but because fallback stages no
    intra edge into `OrderService.place` (method call on a locally-typed variable) and
    no CONSUMES edge for the dispatch_dict handler, so every process's entry-graph
    adjacency is empty at its entrypoint. Runs in the DEFAULT suite (no scip/network:
    _AlwaysFailRunner raises before any subprocess; no FalkorDB: staging-only).

    If this test ever FAILS with max order > 0, the fallback resolver has learned a
    new trick (e.g. method-call resolution) -- that's an improvement, not a bug, but
    this module's docstring, linking/processes.py's docstring, and the M3 T2 report
    must be updated together with the new number.
    """
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    staging = Staging(tmp_path / "staging.db")
    active_idioms = frozenset(cfg.builtin_idioms)
    try:
        for svc in cfg.services:
            report = analyze_service(
                svc, staging, tmp_path / "scip-cache", runner=_AlwaysFailRunner(),
                active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
            )
            assert report["degraded"] is True

        link_report = link_workspace(cfg, staging)
        assert link_report["processes"] == 2  # both anchors still materialize

        orders = [
            e.props["order"] for e in staging.iter_edges() if e.type == "PART_OF_PROCESS"
        ]
        assert orders, "expected at least the order-0 entrypoint memberships"
        assert max(orders) == 0
    finally:
        staging.close()
