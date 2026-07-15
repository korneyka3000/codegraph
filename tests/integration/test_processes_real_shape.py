"""M3 T2 real-shape regression: the actual "Order KYC onboarding" fixture chain
(fixtures/workspace.yaml's `processes` decl) through the REAL pipeline (analyze_service
x3, runner=None -> real scip-python; link_workspace) -- the MAIN regression anchor for
"PART_OF_PROCESS derivation is no longer inert" (M2 final review finding this task
starts from: max order was ALWAYS 0 on every real graph).

WHY real scip-python, not the degraded heuristic fallback (documented per this task's
own brief instruction to verify empirically, not assume): live-probed both paths
(see .superpowers/sdd/m3-task-2-report.md for the full transcript) --

  - degraded (fallback.resolve_service): kyc-worker's dispatch_dict consumer idiom
    (`register_handlers({"OrderCreated": handle_order_created})`) resolves its handler
    via `ctx.ref_symbol_lookup` at the DICT VALUE's byte span -- a bare name
    reference, never itself a call site. `fallback.resolve_service` only ever builds
    ref rows from `facts.calls` (call sites) -- see resolvers/fallback.py's own
    docstring ("Refs -- только к TOP-LEVEL def'ам ... вызов имени"). It has no ref at
    all for a plain name used as a dict value, so `ref_symbol_lookup` returns None
    there, `handle_order_created` never gets a CONSUMES edge or a MessageConsumer
    role, and the entire downstream chain (workflow/activity/http-client/doc-mgmt
    handler) is disconnected from the process's entrypoint BFS -- independent of
    _entry_of's own correctness. Confirmed live: degraded max order tops out at 1.
  - real scip-python: populates a ref occurrence for EVERY identifier (not just call
    sites), so the SAME dispatch_dict lookup resolves; `verify_documents`'s
    `client.get_document(...)` method call also resolves via pyright's real type
    inference (fallback's naive name-based heuristic has no type inference at all, so
    it can't do this either -- it only ever matches a BARE call whose callee NAME is a
    top-level def, same-file or via a direct `from X import name`). Confirmed live:
    exactly the shape asserted below, max order == 2.

Marker `scip` (real scip-python, needs npx/network) -- matches every other
real-pipeline test in this directory (test_pipeline_real.py, test_scip_real.py).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.scip

FIXTURES = Path(__file__).parents[2] / "fixtures"


def _qualified_id(staging: Staging, service: str, qualified: str) -> str | None:
    for n in staging.iter_nodes():
        if n.service == service and n.qualified_name == qualified:
            return n.id
    return None


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
