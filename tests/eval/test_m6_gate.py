"""M6 gate: fixtures/realstack proves all FIVE pilot idiom gaps end-to-end (see
docs/superpowers/reports/2026-07-23-pilot-real-services-gaps.md). Mirrors
tests/eval/test_m2_gate.py's harness (module docstring, `-m scip`/`-m falkordb`
marking, `shutil.which("npx")` skip, tmp_path staging, print-then-assert
diagnostics, ONE `problems` list asserted once at the end) over the realstack
workspace instead of fixtures/workspace.yaml:

  (a) per-type precision/recall over {INVOKES_ACTIVITY, CALLS_HTTP, CONSUMES,
      PRODUCES} (fixtures/realstack/golden/edges.yaml), via the SAME
      evalx.edges_eval helpers M2 uses (found_edges/load_golden_edges/
      precision_recall) -- unmodified, reused as-is.
  (b) temporal_start-marked CALLS precision/recall -- gaps 2/3's OWN gate type
      (per this task's brief, listed separately from plain CALLS). edges_eval's
      found_edges/load_golden_edges deliberately EXCLUDE mechanism-tagged CALLS
      records (mirrors M1's calls_eval -- a mechanism tag is CALLS-specific
      metadata neither module was ever asked to compare), so this file builds its
      own tiny mirror-shaped loader/finder for exactly the mechanism=
      "temporal_start" subset instead of teaching the shared M1-M5 eval code a new,
      M6-only shape.
  (c) channel containment: the base_class consumer's OPTIONAL `topic.attr` path
      (gap 4) -- an honest-unresolved kafka_topic Channel CONTAINS the event_type
      channel, same style as M2 gate's own containment check (b).
  (d) an HONESTY pin on the base_class CONSUMES edge's own resolution/confidence:
      heuristic/0.6 (IMPORT_NAME textual tier), NOT static/1.0 -- kyc_base_consumer
      is a path-dependency with no installed venv (fixtures/realstack/libs/
      kyc_base_consumer), so scip genuinely cannot resolve the cross-package base
      class reference, exactly like the real pilot (GAPS §5's finding). Asserted
      explicitly so a future change that accidentally made this resolve at
      STATIC/1.0 (e.g. nesting the lib inside the worker's own tree) would be
      caught as silently making the gate LESS honest, not just "still green".
  (e) cross-service trace_process vs fixtures/realstack/golden/traces.yaml,
      segment-by-segment (same `_trace_diff` shape M2's gate uses -- ported
      verbatim), PLUS a CLI (`codegraph trace`, CliRunner) check that the printed
      segment chain names every hop across all four async legs (route ->
      workflow -> activity -> publish -> event channel -> process_event; and the
      SDK call -> CALLS_HTTP -> worker's route).

`degraded` is asserted `== []` EXPLICITLY (this task's brief) -- a degraded run
would silently weaken every check above (heuristic/0.6 CALLS join instead of
static/1.0, no scip refs at all for the temporal/http_client extractors' own
STATIC-tier attempts), so it is its own named assertion, not folded wordlessly
into `_run_pipeline` the way M2's gate does it.

Gate is NOT weakened on failure and golden is NOT edited to make it pass -- see
.superpowers/sdd/task-5-report.md if any part of it doesn't pass for real.
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

FIXTURES = Path(__file__).parents[2] / "fixtures" / "realstack"
GOLDEN_EDGES = FIXTURES / "golden" / "edges.yaml"
GOLDEN_TRACES = FIXTURES / "golden" / "traces.yaml"

GATE_TYPES = ("INVOKES_ACTIVITY", "CALLS_HTTP", "CONSUMES", "PRODUCES")

GRAPH_NAME = "__m6_gate__"
# M8 T3 (honest pin update -- this gate shares fixtures/realstack with M8's own
# gate): gateway's own route composes to "/api/v1/submit" now (a real cross-file
# include_router chain, R4's realstack proof) -- the pre-M8 "gateway:POST /submit"
# selector no longer resolves any staged route. Worker's own routes (the OTHER
# three golden legs this gate pins) are untouched by the M8 restructuring.
ENTRYPOINT_SELECTOR = "gateway:POST /api/v1/submit"

TOPIC_CHANNEL = ids.chan_kafka("${self.config.topic}")
EVENT_CHANNEL = ids.chan_event("DocSubmittedEvent")

CONSUMER_SYMBOL = "app.consumers.doc_submitted.DocSubmittedConsumer.process_event"

# CLI trace output check (mirrors M2 gate's CLI_EXPECTED_TOKENS): every hop's own
# qualified/display name, across BOTH async legs (Temporal chain + the decorator-SDK
# HTTP call), must appear somewhere in the rendered chain.
CLI_EXPECTED_TOKENS = (
    "submit_document",
    "DocSubmissionWorkflow",
    "fetch_document_content",
    "publish_submitted_event",
    "NotifyWorkflow",
    "DocClient",
    "get_document",
    "DocSubmittedConsumer",
)


def _run_pipeline(cfg: WorkspaceConfig, staging: Staging, cache_dir: Path) -> list[str]:
    """analyze_service (real scip, runner=None) for every configured service, then
    link_workspace -- same sequence/wiring `codegraph index` (cli.py) uses, mirrors
    M2 gate's own `_run_pipeline`. Returns the list of service names that reported
    degraded=True (empty on a healthy run) INSTEAD of hard-asserting inline here, so
    the caller can make `degraded == []` its own explicit, named assertion (this
    task's brief calls this out specifically)."""
    active_idioms = frozenset(cfg.builtin_idioms)
    degraded_services: list[str] = []
    for svc in cfg.services:
        report = analyze_service(
            svc, staging, cache_dir, runner=None,
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
        if report["degraded"]:
            degraded_services.append(svc.name)
    link_workspace(cfg, staging)
    return degraded_services


def _load_golden_temporal_start(path: Path) -> set[tuple[str, str, str, str]]:
    """(src_service, src_qualified, dst_service, dst_qualified) for every golden
    CALLS record carrying `mechanism: temporal_start` -- the mirror image of
    evalx.edges_eval.load_golden_edges' own CALLS handling, which EXCLUDES exactly
    these records (see this module's docstring, point (b))."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "temporal_start":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_temporal_start(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Staged CALLS edges carrying props.mechanism == "temporal_start", normalized
    to the same 4-tuple shape `_load_golden_temporal_start` produces -- mirrors
    evalx.edges_eval.found_edges' own node-id -> (service, qualified_name) join."""
    node_lookup = {
        n.id: (n.service, n.qualified_name) for n in staging.iter_nodes() if n.qualified_name
    }
    out: set[tuple[str, str, str, str]] = set()
    dangling = 0
    for e in staging.iter_edges():
        if e.type != "CALLS" or e.props.get("mechanism") != "temporal_start":
            continue
        src = node_lookup.get(e.src)
        dst = node_lookup.get(e.dst)
        if src is None or dst is None:
            dangling += 1
            continue
        out.add((src[0], src[1], dst[0], dst[1]))
    return out, dangling


def _find_node_id(staging: Staging, service: str, qualified_name: str) -> str:
    for n in staging.iter_nodes():
        if n.service == service and n.qualified_name == qualified_name:
            return n.id
    raise AssertionError(
        f"node not found in staging: service={service!r} qualified_name={qualified_name!r}"
    )


def _sorted_triples(triples: set[tuple]) -> list[tuple]:
    return sorted(triples, key=lambda t: tuple(str(v) for v in t))


def _trace_diff(result: dict) -> list[str]:
    """Order-tolerant, set-EXACT comparison of a trace_process() result against
    fixtures/realstack/golden/traces.yaml's single POST /submit trace -- ported
    verbatim from test_m2_gate.py's own `_trace_diff` (see that module for the full
    rationale: segments compared as an exact SET of (service, entry_symbol,
    via_channel) triples, via_channel derived by scanning every segment's own exits
    for ones whose next_entry_ids name this segment's entry)."""
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
    Console line-wrapping under CliRunner's non-tty (width-guessed) output, same
    rationale as M2 gate's own `_cli_output_flat`."""
    return "".join(output.split())


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m6_gate(tmp_path, falkordb_cfg):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    cache_dir = tmp_path / "scip-cache"
    ws_root = tmp_path / "ws"
    ws_root.mkdir()
    staging_path = ws_root / ".codegraph" / "staging.db"

    problems: list[str] = []
    staging = Staging(staging_path)
    store = FalkorStore(falkordb_cfg, GRAPH_NAME)
    build_store = FalkorStore(falkordb_cfg, f"{GRAPH_NAME}__build")
    try:
        degraded = _run_pipeline(cfg, staging, cache_dir)
        # -- degraded == [] EXPLICITLY (this task's brief: "a degraded run would
        # silently weaken everything") --------------------------------------------
        assert degraded == [], (
            f"realstack must index WITHOUT degrading (first-party-only scip "
            f"resolution suffices for both gateway and worker -- no venv needed) "
            f"-- degraded services: {degraded}"
        )

        # -- (a) per-type precision/recall ---------------------------------------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M6 gate][{edge_type}] precision={pr['precision']:.4f} "
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

        # -- (b) temporal_start-marked CALLS precision/recall (gaps 2/3) ----------
        golden_ts = _load_golden_temporal_start(GOLDEN_EDGES)
        found_ts, dangling_ts = _found_temporal_start(staging)
        pr_ts = precision_recall(found_ts, golden_ts)
        print(
            f"\n[M6 gate][temporal_start CALLS] precision={pr_ts['precision']:.4f} "
            f"recall={pr_ts['recall']:.4f} tp={pr_ts['tp']} found={len(found_ts)} "
            f"golden={len(golden_ts)} dangling={dangling_ts}\n"
            f"  fp ({len(pr_ts['fp_list'])}): {pr_ts['fp_list']}\n"
            f"  fn ({len(pr_ts['fn_list'])}): {pr_ts['fn_list']}"
        )
        if pr_ts["precision"] != 1.0 or pr_ts["recall"] != 1.0:
            problems.append(
                f"temporal_start CALLS: precision={pr_ts['precision']:.4f} "
                f"recall={pr_ts['recall']:.4f} (want 1.0/1.0); dangling={dangling_ts}\n"
                f"  fp ({len(pr_ts['fp_list'])}): {pr_ts['fp_list']}\n"
                f"  fn ({len(pr_ts['fn_list'])}): {pr_ts['fn_list']}"
            )

        # -- (c) channel containment: unresolved topic (config_ref) -> event ------
        contains = {(e.src, e.dst) for e in staging.iter_edges() if e.type == "CONTAINS"}
        if (TOPIC_CHANNEL, EVENT_CHANNEL) not in contains:
            problems.append(
                f"containment: expected CONTAINS {TOPIC_CHANNEL!r} -> "
                f"{EVENT_CHANNEL!r}; staged CONTAINS pairs: {sorted(contains)}"
            )

        # -- (d) honesty pin: base_class CONSUMES is heuristic/0.6, NOT static/1.0 -
        consumer_id = _find_node_id(staging, "worker", CONSUMER_SYMBOL)
        consumes = [
            e for e in staging.iter_edges()
            if e.type == "CONSUMES" and e.src == consumer_id and e.dst == EVENT_CHANNEL
        ]
        if len(consumes) != 1:
            problems.append(
                f"honesty pin: expected exactly ONE CONSUMES {consumer_id!r} -> "
                f"{EVENT_CHANNEL!r}, found {len(consumes)}: {consumes}"
            )
        else:
            edge = consumes[0]
            if (edge.resolution, edge.confidence) != ("heuristic", 0.6):
                problems.append(
                    "honesty pin: base_class CONSUMES expected (resolution="
                    "'heuristic', confidence=0.6) -- kyc_base_consumer has no "
                    "installed venv, so scip cannot resolve the cross-package base "
                    "class reference; the IMPORT_NAME textual tier should fire, NOT "
                    f"a scip STATIC match (1.0) -- got (resolution="
                    f"{edge.resolution!r}, confidence={edge.confidence!r})"
                )

        # -- resolve entrypoint (staging-side; same mechanism processes.materialize
        # uses -- NOT what `codegraph trace` itself uses, see M2 gate's identical
        # comment: this cross-checks the S7 route table independently of the
        # CLI-check block below, which exercises the loaded FalkorDB graph) --------
        entrypoint_id = resolve_selector(staging, ENTRYPOINT_SELECTOR)
        if entrypoint_id is None:
            problems.append(
                f"entrypoint not resolved for selector {ENTRYPOINT_SELECTOR!r} -- "
                "staged Channel(http_route)/HANDLES missing or mismatched"
            )

        # -- load into FalkorDB (S9, blue/green) -----------------------------------
        load_stats = load_graph(
            staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME
        )
        # edges_dropped_by_type carries a ZERO entry for every edge type PRESENT in
        # the graph (see pipeline/load.py: `edges_dropped_by_type[edge_type] =
        # dropped`, unconditionally, once per type-group) -- only nonzero counts are
        # real drops (mirrors M2 gate's own `if n` filter on this exact dict).
        # Unlike M2's fixtures (which carry ONE documented residual CALLS drop --
        # kyc-worker's dynamic dict-dispatch handler), realstack has no such known
        # exception: every edge planned here has both ends staged as real nodes, and
        # the cross-package `kyc_base_consumer` import never becomes an edge at all
        # (python_core.py's own imports_external counter, not a dangling IMPORTS
        # edge -- see extractors/python_core.py). ANY drop here is unexpected.
        unexpected_drops = {
            t: n for t, n in load_stats["edges_dropped_by_type"].items() if n
        }
        if unexpected_drops:
            problems.append(
                f"load_graph dropped UNEXPECTED edges: {unexpected_drops} "
                f"(full breakdown: {load_stats['edges_dropped_by_type']})"
            )
        staging.close()

        # -- (e) trace_process vs golden, and CLI verification (need entrypoint) --
        if entrypoint_id is not None:
            gq = GraphQuery(
                store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
                service_paths={svc.name: svc.path for svc in cfg.services},
            )
            result = gq.trace_process(entrypoint_id)
            if "error" in result:
                problems.append(f"trace_process error: {result['error']}")
            else:
                print(f"\n[M6 gate][trace] {result}")
                problems.extend(_trace_diff(result))

            runner = CliRunner()
            cli_result = runner.invoke(
                app, ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME]
            )
            print(f"\n[M6 gate][cli trace text]\n{cli_result.output}")
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
    finally:
        staging.close()
        store.delete_graph()
        build_store.delete_graph()

    assert not problems, "\n\n".join(problems)
