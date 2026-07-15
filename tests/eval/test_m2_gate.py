"""M2 gate: the REAL pipeline (analyze_service x3, runner=None -> real scip-python;
link_workspace; load_graph into a live FalkorDB) against golden (fixtures/golden/
edges.yaml + traces.yaml) -- the M2 milestone gate. Mirrors M1's tests/eval/
test_calls_gate.py conventions (module docstring, `-m scip` marking, `shutil.which
("npx")` skip, tmp_path staging, print-then-assert diagnostics) and extends them:
  (a) per-type precision/recall (edges_eval, generalized) over {HANDLES, DEPENDS_ON,
      PRODUCES, CONSUMES, INVOKES_ACTIVITY, CALLS_HTTP};
  (b) channel containment (kafka_topic:orders.events CONTAINS event_type:OrderCreated);
  (c) cross-service trace_process vs fixtures/golden/traces.yaml, segment-by-segment,
      via a live FalkorDB graph (needs `falkordb`, unlike M1 -- see conftest.py's
      falkordb_cfg fixture for the runtime availability skip);
  (d) "idiom-as-config": a SECOND pipeline run over a WorkspaceConfig variant with
      orders-api's "outbox" producer idiom removed (in-memory `model_copy`, no file
      touched) must be missing exactly the PRODUCES/NEXT_SEGMENT edges that idiom
      alone discovers, proving idioms are configuration, not hardcoded behavior;
  plus a CLI (`codegraph trace`, CliRunner) check of the master-plan's own
  verification string: the printed segment chain names every hop from the HTTP route
  to the final downstream handler.

All diagnostics (per-type fp/fn, trace segment diff, idiom-config diff, CLI output)
are collected into one `problems` list and asserted ONCE at the end, rather than
failing at the first broken assertion -- given how expensive the shared setup is
(two real scip-python passes + a live FalkorDB load), a single run should surface
every finding at once for a controller decision, not just the first one hit.

Gate is NOT weakened on failure and golden is NOT edited to make it pass -- see
m2-task-9-report.md "Self-review"/"Concerns" if any part of it doesn't pass for real.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.core import ids
from codegraph.evalx.edges_eval import found_edges, load_golden_edges, precision_recall
from codegraph.linking.processes import resolve_selector
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.load import load_graph
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = [pytest.mark.scip, pytest.mark.falkordb]

FIXTURES = Path(__file__).parents[2] / "fixtures"
GOLDEN_EDGES = FIXTURES / "golden" / "edges.yaml"
GOLDEN_TRACES = FIXTURES / "golden" / "traces.yaml"

GATE_TYPES = ("HANDLES", "DEPENDS_ON", "PRODUCES", "CONSUMES", "INVOKES_ACTIVITY", "CALLS_HTTP")

GRAPH_NAME = "__m2_gate__"
ENTRYPOINT_SELECTOR = "orders-api:POST /orders"
ORDERS_TOPIC = ids.chan_kafka("orders.events")
ORDER_CREATED_EVENT = ids.chan_event("OrderCreated")

# CLI trace output check (master-plan verification string): the printed chain must
# name every hop from the HTTP route to the final downstream handler. Kept SHORT --
# rich's Console wraps long unbroken lines under CliRunner's non-tty width (no real
# terminal to size against), which could otherwise split a long dotted qualified name
# (e.g. the ~74-char client method path) across two output lines; whitespace is ALSO
# stripped before matching (see _cli_output_flat) as a second, independent guard
# against exactly that.
CLI_EXPECTED_TOKENS = (
    "create_order",
    "OrderCreated",
    "handle_order_created",
    "KycWorkflow",
    "verify_documents",
    "DocumentManagementClient",
)


def _run_pipeline(cfg: WorkspaceConfig, staging: Staging, cache_dir: Path) -> None:
    """analyze_service (real scip, runner=None) for every configured service, then
    link_workspace -- the same sequence/wiring `codegraph index` (cli.py) uses
    (active_idioms/idioms copied verbatim from there). A degraded service aborts
    immediately (hard assert, not funneled into `problems`): every downstream
    measurement here assumes real SCIP resolution, so a degraded run isn't a partial
    gate result, it's a broken precondition -- see M1's own identical hard-assert in
    test_calls_gate.py."""
    active_idioms = frozenset(cfg.builtin_idioms)
    for svc in cfg.services:
        report = analyze_service(
            svc, staging, cache_dir, runner=None,
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
        assert not report["degraded"], (
            f"real scip expected for all fixture services, got degraded "
            f"{svc.name!r}: {report['reason']}"
        )
    link_workspace(cfg, staging)


def _find_node_id(staging: Staging, service: str, qualified_name: str) -> str:
    for n in staging.iter_nodes():
        if n.service == service and n.qualified_name == qualified_name:
            return n.id
    raise AssertionError(
        f"node not found in staging: service={service!r} qualified_name={qualified_name!r}"
    )


def _no_outbox_config(cfg: WorkspaceConfig) -> WorkspaceConfig:
    """In-memory WorkspaceConfig variant (model_copy at every level -- ServiceConfig/
    ServiceIdioms are frozen pydantic models, see config/models.py) with orders-api's
    OWN "outbox" producer idiom removed; everything else (including orders-api's
    builtin-derived idioms, other services, processes) untouched. No file written, no
    golden touched -- this is the (г)/"idiom-as-config" negative control."""
    orders_idx = next(i for i, s in enumerate(cfg.services) if s.name == "orders-api")
    orders_svc = cfg.services[orders_idx]
    remaining = [p for p in orders_svc.idioms.producers if p.name != "outbox"]
    assert len(remaining) < len(orders_svc.idioms.producers), (
        "fixture workspace.yaml no longer defines an 'outbox' producer idiom for "
        "orders-api -- this negative-control test has nothing left to remove"
    )
    no_outbox_svc = orders_svc.model_copy(
        update={"idioms": orders_svc.idioms.model_copy(update={"producers": remaining})}
    )
    services = list(cfg.services)
    services[orders_idx] = no_outbox_svc
    return cfg.model_copy(update={"services": services})


def _sorted_triples(triples: set[tuple]) -> list[tuple]:
    """Deterministic ordering for diagnostics; via_channel can be None (entrypoint),
    which plain sorted() can't compare against str -- stringify per element."""
    return sorted(triples, key=lambda t: tuple(str(v) for v in t))


def _trace_diff(result: dict) -> list[str]:
    """Order-tolerant, set-EXACT comparison of a trace_process() result against
    fixtures/golden/traces.yaml's single POST /orders trace (controller-adjudicated
    T9 fix wave). With the containment fan-out at OrderService.place's PRODUCES exit,
    segment DISCOVERY order is a BFS artifact (traverse.py sorts next_entry_ids by
    node id, so run_consumer's `consumer_main` module happens to land before
    handle_order_created's `consumers.orders`), not a semantic property -- so
    segments are compared as an exact SET of (service, entry_symbol, via_channel)
    triples, asserting no missing AND no extra. This is NOT a weakening: exact
    membership both ways plus an exact segment-COUNT match; only positional order is
    dropped. Per-segment/overall truncated checks are kept as-is.

    via_channel for a found segment is derived by scanning ALL segments' exits for
    ones whose next_entry_ids contain this segment's entry id (a segment carries its
    outgoing exits, never its own incoming channel -- see query/traverse.py; the old
    positional previous-segment lookup was wrong under branching: the
    handle_order_created segment's real source is segment 0's exit, not segment 1's).
    No incoming exit -> None, matching golden's `via_channel: null` entrypoint
    convention; MORE than one distinct incoming channel is reported as its own
    problem (golden's schema has one via_channel per segment, a multi-channel entry
    wouldn't be representable)."""
    golden_data = yaml.safe_load(GOLDEN_TRACES.read_text())
    golden_trace = next(
        t for t in golden_data["traces"] if t["entrypoint"] == ENTRYPOINT_SELECTOR
    )
    golden_segments = golden_trace["segments"]
    segments = result["segments"]

    problems: list[str] = []
    if len(segments) != len(golden_segments):
        problems.append(
            f"trace: segment count found={len(segments)} golden={len(golden_segments)}\n"
            f"  found services: {[s.get('service') for s in segments]}\n"
            f"  golden services: {[g['service'] for g in golden_segments]}"
        )

    incoming: dict[str, set[str]] = {}  # entry node id -> channel ids leading to it
    for seg in segments:
        for ex in seg.get("exits", []):
            chan_id = (ex.get("channel") or {}).get("id")
            if chan_id is None:
                continue
            for next_id in ex.get("next_entry_ids", []):
                incoming.setdefault(next_id, set()).add(chan_id)

    found_triples: set[tuple] = set()
    for i, seg in enumerate(segments):
        label = f"trace segment {i} (found service={seg.get('service')!r})"
        if seg.get("truncated") is True:
            problems.append(
                f"{label}: truncated=True (steps={seg.get('steps')} exits={seg.get('exits')})"
            )
        entry = seg.get("entry") or {}
        vias = sorted(incoming.get(entry.get("id"), ()))
        if len(vias) > 1:
            problems.append(
                f"{label}: multiple distinct incoming channels {vias} -- not "
                "representable as golden's single via_channel"
            )
        found_triples.add(
            (seg.get("service"), entry.get("qualified_name"), vias[0] if vias else None)
        )

    golden_triples = {
        (g["service"], g["entry"]["symbol"], g["via_channel"]) for g in golden_segments
    }
    missing = golden_triples - found_triples
    extra = found_triples - golden_triples
    if missing or extra:
        problems.append(
            "trace set mismatch (service, entry_symbol, via_channel):\n"
            f"  missing (in golden, not found): {_sorted_triples(missing)}\n"
            f"  extra (found, not in golden): {_sorted_triples(extra)}"
        )

    if result.get("truncated") is True:
        problems.append(f"trace: overall result truncated=True: {result}")
    return problems


def _cli_output_flat(output: str) -> str:
    """Whitespace-stripped CLI output -- guards substring checks against rich's
    Console line-wrapping under CliRunner's non-tty (width-guessed, possibly narrow)
    output: a long unbroken token split across two wrapped lines is still found here,
    since folding never inserts characters, only newlines/indentation between
    already-adjacent ones (see CLI_EXPECTED_TOKENS' own comment)."""
    return "".join(output.split())


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m2_gate(tmp_path, falkordb_cfg):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    # Shared across BOTH pipeline runs below (the full run and the idiom-as-config
    # negative run): ScipRunner's cache key is (service_name, tree_hash), purely
    # content-based (pipeline/scan.py) -- NOT a function of idioms/active_idioms -- so
    # the second run's .scip files are identical to the first's; only the PYTHON-side
    # kafka_ext matching differs (the entire point of that check), so reuse is
    # correct, not stale-cache masking of a real difference.
    cache_dir = tmp_path / "scip-cache"

    # Conventional `<root>/.codegraph/staging.db` layout for the pipeline's own
    # staging file. NOTE (M3 T2): `codegraph trace` no longer reads staging at all --
    # its selector resolves graph-side via GraphQuery.resolve_selector, so the
    # CLI-check block below exercises the loaded FalkorDB graph, not this file;
    # ws_root stays as the zero-config CLI target (no codegraph.yaml needed --
    # service_paths is unused by `codegraph trace`'s default include_source=False)
    # and the conventional home for staging.db either way.
    ws_root = tmp_path / "ws"
    ws_root.mkdir()
    staging_path = ws_root / ".codegraph" / "staging.db"

    problems: list[str] = []
    staging = Staging(staging_path)
    staging2: Staging | None = None
    store = FalkorStore(falkordb_cfg, GRAPH_NAME)
    build_store = FalkorStore(falkordb_cfg, f"{GRAPH_NAME}__build")
    try:
        _run_pipeline(cfg, staging, cache_dir)

        # -- (a) per-type precision/recall ---------------------------------------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M2 gate][{edge_type}] precision={pr['precision']:.4f} "
                f"recall={pr['recall']:.4f} tp={pr['tp']} found={len(found)} "
                f"golden={len(golden)} dangling={dangling}\n"
                f"  fp ({len(pr['fp_list'])}): {pr['fp_list']}\n"
                f"  fn ({len(pr['fn_list'])}): {pr['fn_list']}"
            )
            if pr["precision"] != 1.0 or pr["recall"] != 1.0:
                problems.append(
                    f"{edge_type}: precision={pr['precision']:.4f} "
                    f"recall={pr['recall']:.4f} (want 1.0/1.0); dangling={dangling}\n"
                    f"  fp ({len(pr['fp_list'])}): {pr['fp_list']}\n"
                    f"  fn ({len(pr['fn_list'])}): {pr['fn_list']}"
                )

        # -- (b) channel containment ----------------------------------------------
        contains = {(e.src, e.dst) for e in staging.iter_edges() if e.type == "CONTAINS"}
        if (ORDERS_TOPIC, ORDER_CREATED_EVENT) not in contains:
            problems.append(
                f"containment: expected CONTAINS {ORDERS_TOPIC!r} -> "
                f"{ORDER_CREATED_EVENT!r}; staged CONTAINS pairs: {sorted(contains)}"
            )

        # -- positive control for (d): both edges present in the FULL run ---------
        place_id = _find_node_id(staging, "orders-api", "app.services.order.OrderService.place")
        kyc_entry_id = _find_node_id(
            staging, "kyc-worker", "app.consumers.orders.handle_order_created"
        )
        if not any(
            e.type == "PRODUCES" and e.src == place_id and e.dst == ORDER_CREATED_EVENT
            for e in staging.iter_edges()
        ):
            problems.append(
                f"idiom-as-config control: expected PRODUCES {place_id} -> "
                f"{ORDER_CREATED_EVENT} in the FULL run; not found"
            )
        if not any(
            e.type == "NEXT_SEGMENT" and e.dst == kyc_entry_id for e in staging.iter_edges()
        ):
            problems.append(
                f"idiom-as-config control: expected a NEXT_SEGMENT edge -> "
                f"{kyc_entry_id} (orders -> kyc) in the FULL run; not found"
            )

        # -- resolve entrypoint (staging-only; same mechanism processes.materialize
        # uses, see linking/processes.py -- NOT what `codegraph trace` uses anymore:
        # since M3 T2 the CLI resolves selectors graph-side via
        # GraphQuery.resolve_selector, which the CLI-check block below exercises
        # end-to-end; this staging-side resolve feeds the direct gq.trace_process
        # call and cross-checks the S7 route table independently) -----------------
        entrypoint_id = resolve_selector(staging, ENTRYPOINT_SELECTOR)
        if entrypoint_id is None:
            problems.append(
                f"entrypoint not resolved for selector {ENTRYPOINT_SELECTOR!r} -- "
                "staged Channel(http_route)/HANDLES missing or mismatched"
            )

        # -- (d) idiom-as-config: SECOND run, separate staging, config variant ----
        cfg_no_outbox = _no_outbox_config(cfg)
        staging2 = Staging(tmp_path / "staging2.db")
        _run_pipeline(cfg_no_outbox, staging2, cache_dir)

        place_id2 = _find_node_id(
            staging2, "orders-api", "app.services.order.OrderService.place"
        )
        kyc_entry_id2 = _find_node_id(
            staging2, "kyc-worker", "app.consumers.orders.handle_order_created"
        )
        produces_gone = [
            e for e in staging2.iter_edges()
            if e.type == "PRODUCES" and e.src == place_id2 and e.dst == ORDER_CREATED_EVENT
        ]
        if produces_gone:
            problems.append(
                f"idiom-as-config: PRODUCES {place_id2} -> {ORDER_CREATED_EVENT} still "
                f"present after removing orders-api's outbox idiom: {produces_gone}"
            )
        next_segment_gone = [
            e for e in staging2.iter_edges()
            if e.type == "NEXT_SEGMENT" and e.dst == kyc_entry_id2
        ]
        if next_segment_gone:
            problems.append(
                f"idiom-as-config: NEXT_SEGMENT -> {kyc_entry_id2} still present after "
                f"removing orders-api's outbox idiom: {next_segment_gone}"
            )
        staging2.close()
        staging2 = None

        # -- load into FalkorDB (S9, blue/green) -----------------------------------
        load_stats = load_graph(
            staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME
        )
        # Known, already-documented M1 limitation (m1b-task-8-report.md "Third
        # discrepancy"; see also tests/eval/test_calls_gate.py's
        # EXPECTED_SKIPPED_DANGLING=1): kyc-worker's run_consumer dispatches via a
        # dynamic `handler(event)` call where `handler` is a local variable -- real
        # SCIP resolves that ref to the SAME local symbol as its own def in this
        # file, so build_calls (S6) emits a CALLS edge to it, but python_core only
        # ever builds Nodes for Module/Class/Function defs (never arbitrary locals)
        # -- this dst id has no Node, so it's absent from both found_calls' own
        # node-join (M1's gate) AND load_graph's known_ids membership check here.
        # Not an M2 regression -- allowed as-is; anything ELSE dropped is new and
        # unexplained.
        dropped_by_type = load_stats["edges_dropped_by_type"]
        unexpected_drops = {
            t: n for t, n in dropped_by_type.items() if n and not (t == "CALLS" and n == 1)
        }
        if unexpected_drops:
            problems.append(
                f"load_graph dropped UNEXPECTED edges: {unexpected_drops} "
                f"(full breakdown: {dropped_by_type})"
            )
        staging.close()

        # -- (c) trace_process vs golden, and CLI verification (need entrypoint) --
        if entrypoint_id is not None:
            gq = GraphQuery(
                store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
                service_paths={svc.name: svc.path for svc in cfg.services},
            )
            result = gq.trace_process(entrypoint_id)
            if "error" in result:
                problems.append(f"trace_process error: {result['error']}")
            else:
                print(f"\n[M2 gate][trace] {result}")
                problems.extend(_trace_diff(result))

            runner = CliRunner()
            cli_result = runner.invoke(
                app, ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME]
            )
            print(f"\n[M2 gate][cli trace text]\n{cli_result.output}")
            if cli_result.exit_code != 0:
                problems.append(
                    f"CLI trace exit_code={cli_result.exit_code}: {cli_result.output}"
                )
            else:
                flat = _cli_output_flat(cli_result.output)
                missing = [t for t in CLI_EXPECTED_TOKENS if t not in flat]
                if missing:
                    problems.append(
                        f"CLI trace output missing tokens {missing}:\n{cli_result.output}"
                    )
                get_document_hits = flat.count("get_document")
                if get_document_hits < 2:
                    problems.append(
                        "CLI trace output: expected >=2 'get_document' occurrences "
                        f"(client method + document-management handler), got "
                        f"{get_document_hits}:\n{cli_result.output}"
                    )
                if "routes.documents.get_document" not in flat:
                    problems.append(
                        "CLI trace output missing the document-management handler's "
                        f"own qualified name 'routes.documents.get_document':\n"
                        f"{cli_result.output}"
                    )

            mermaid_result = runner.invoke(
                app,
                ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME,
                 "--format", "mermaid"],
            )
            if mermaid_result.exit_code != 0 or "flowchart TD" not in mermaid_result.output:
                problems.append(
                    f"CLI trace --format mermaid invalid: exit={mermaid_result.exit_code} "
                    f"output:\n{mermaid_result.output}"
                )
    finally:
        staging.close()
        if staging2 is not None:
            staging2.close()
        store.delete_graph()
        build_store.delete_graph()

    assert not problems, "\n\n".join(problems)
