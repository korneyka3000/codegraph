"""M7 gate: fixtures/realstack (extended in M7 T6) proves every M7 mechanism
(open-gaps R1-R3, docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md)
end-to-end. Mirrors tests/eval/test_m6_gate.py's harness verbatim (module
docstring, `-m scip`/`-m falkordb` marking, `shutil.which("npx")` skip, tmp_path
staging, print-then-assert diagnostics, ONE `problems` list asserted once at the
end) over the SAME workspace and the SAME (extended) golden -- the M6 gate keeps
pinning the M6-era legs on this shared fixture; THIS gate's additional value is the
M7-specific pins below. Helper functions (`_run_pipeline`/`_trace_diff`/loaders)
are PORTED from test_m6_gate.py rather than imported -- the same convention M6
itself established when porting them from test_m2_gate.py (test modules stay
self-contained; a cross-test import would couple two gates that must be
independently runnable).

  (a) per-type precision/recall over {INVOKES_ACTIVITY, CALLS_HTTP, CONSUMES,
      PRODUCES} against the EXTENDED fixtures/realstack/golden/edges.yaml -- which
      now includes the settings-source PRODUCES, the three enum-fan-out PRODUCES,
      the temporal-signal PRODUCES/CONSUMES pair and the second (auto-anchored)
      CALLS_HTTP. P=R=1.0 here already proves absence-of-extras structurally
      (e.g. NO edge into the funnel route, NO channel/edge for @workflow.query) --
      the named negative pins below re-assert the load-bearing ones directly, with
      vacuity guards, so a failure names the exact broken leg.
  (b) temporal_start-marked CALLS precision/recall -- ported M6 check, unchanged
      scope (the M7 fixture adds no new workflow starts; still gated so a
      regression here is THIS gate's failure too, not just M6's).
  (c) M7 T2 settings-source pin: the outbox PRODUCES edge (Event ctor matched
      STATIC via scip; channel name from GatewaySettings.doc_events_topic's
      string-literal default) is (static, 1.0) -- a code-literal settings default
      is exactly as trustworthy as a const, and the call-site match tier carries
      through (kafka_ext._resolution_for on Resolved(kind="value")).
  (d) M7 T2 enum-fan-out pins: one PRODUCES per DocTopicName member from
      replay_document, each (heuristic, 0.8) with props mechanism="enum_fanout"
      AND callsite_count=2 -- TWO textual replicate() call-sites in ONE def must
      dedup into one edge per (src, dst) with the count bumped, not PK-collide
      (kafka_ext._emit_enum_fanout_produces, M7 T2 review Important-2).
  (e) M7 T4 signal pins: sender PRODUCES (worker consumer -> doc-approved channel)
      is (heuristic, 0.6) + mechanism="temporal_signal" (receiver-agnostic
      callee-name match -- the codebase's own documented confidence floor);
      handler CONSUMES (gateway @workflow.signal method) is (static, 1.0) +
      signal_kind="signal" (a decorator match is full ground truth, fastapi
      HANDLES precedent). PLUS the @workflow.query negative: approval_state
      carries the TemporalSignalHandler role (vacuity guard -- the decorator WAS
      seen) yet NO channel edge touches its would-be channel id (role-only, per
      temporal_ext's design).
  (f) M7 T3 anchoring pins: BOTH CALLS_HTTP edges are (static, 1.0) -- DocClient
      via the pre-existing ServiceConfig.http.base_url_env registry (WORKER_URL),
      StatusClient via the FULL new chain: own-body `self.host = config.worker_url`
      auto-anchor -> ClassAttrIndex.field_by_name -> SERVICE_WORKER_URL ->
      env_sources (env_values.yaml) -> worker. An unanchored-but-unique match
      would surface as heuristic/0.7 here, never static -- so this pin IS the
      end-to-end proof of the env->service leg, not just of path matching.
  (g) M7 T3 funnel NEGATIVE (the milestone's own "false match worse than absence"
      constraint, pinned): worker's all-params GET /{a}/{b}/{c}/misc route exists
      as a real staged channel (vacuity guard) and receives ZERO CALLS_HTTP --
      neither into the owned channel nor into an owner-less "?" fallback variant.
      StatusClient's /api/v1/status/{doc_uid} claim is the live probe: under the
      pre-M7 bidirectional wildcard rule its {doc_uid} tail would have matched the
      route's static "misc" tail too (the OPEN R1 pilot funnel, verbatim shape).
  (h) cross-service trace_process vs golden/traces.yaml -- which now carries the
      FOURTH, signal-hop segment (worker process_event -> chan:temporal_signal:
      doc-approved -> gateway DocSubmissionWorkflow.doc_approved) -- plus the CLI
      `codegraph trace` token check including the signal handler's name.

`degraded` is asserted `== []` EXPLICITLY (same rationale as M6: a degraded run
would silently weaken every check above). Gate is NOT weakened on failure and
golden is NOT edited to make it pass -- extractors get fixed instead (this task's
brief, verbatim rule).
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

GRAPH_NAME = "__m7_gate__"
ENTRYPOINT_SELECTOR = "gateway:POST /submit"

# -- M7 pin targets (hand-derived; see golden/edges.yaml's own per-group comments) --
SETTINGS_PRODUCER = ("gateway", "app.services.outbox_repo.OutboxRepository.add_document_event")
SETTINGS_TOPIC_CHANNEL = ids.chan_kafka("kyc.document.events")
ENUM_PRODUCER = ("gateway", "app.routes.ops.replay_document")
ENUM_TOPIC_CHANNELS = tuple(
    ids.chan_kafka(name) for name in ("kyc.doc.review", "kyc.doc.audit", "kyc.doc.archive")
)
ENUM_CALLSITE_COUNT = 2  # two replicate() call-sites in replay_document, deduped
SIGNAL_CHANNEL = ids.chan_temporal_signal("doc-approved")
SIGNAL_SENDER = ("worker", "app.consumers.doc_submitted.DocSubmittedConsumer.process_event")
SIGNAL_HANDLER = ("gateway", "app.workflows.submission.DocSubmissionWorkflow.doc_approved")
QUERY_HANDLER = ("gateway", "app.workflows.submission.DocSubmissionWorkflow.approval_state")
# The channel a @workflow.query handler WOULD get if temporal_ext (wrongly) treated
# query like signal/update -- name falls back to the method's own name there.
QUERY_WOULD_BE_CHANNEL = ids.chan_temporal_signal("approval_state")
HTTP_PINS = {
    ("gateway", "app.clients.doc_client.DocClient.fetch_document"):
        ids.chan_http("worker", "GET", "/documents/{doc_uid}"),
    ("gateway", "app.clients.status_client.StatusClient.fetch_status"):
        ids.chan_http("worker", "GET", "/api/v1/status/{doc_uid}"),
}
FUNNEL_CHANNEL = ids.chan_http("worker", "GET", "/{a}/{b}/{c}/misc")
FUNNEL_CHANNEL_FALLBACK = ids.chan_http(None, "GET", "/{a}/{b}/{c}/misc")

# CLI trace output check (mirrors M6 gate's list + the signal hop's handler).
CLI_EXPECTED_TOKENS = (
    "submit_document",
    "DocSubmissionWorkflow",
    "fetch_document_content",
    "publish_submitted_event",
    "NotifyWorkflow",
    "DocClient",
    "get_document",
    "DocSubmittedConsumer",
    "doc_approved",
)


def _run_pipeline(cfg: WorkspaceConfig, staging: Staging, cache_dir: Path) -> list[str]:
    """Ported from test_m6_gate.py verbatim (which itself mirrors M2's) -- see that
    module for the degraded-services-list rationale."""
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
    """Ported from test_m6_gate.py -- golden CALLS records with mechanism:
    temporal_start (edges_eval deliberately excludes them, see its docstring)."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "temporal_start":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_temporal_start(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Ported from test_m6_gate.py -- staged mechanism="temporal_start" CALLS."""
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


def _edges_between(staging: Staging, edge_type: str, src_id: str, dst_id: str) -> list:
    return [
        e for e in staging.iter_edges()
        if e.type == edge_type and e.src == src_id and e.dst == dst_id
    ]


def _pin_edge(
    problems: list[str], staging: Staging, label: str, edge_type: str,
    src: tuple[str, str], dst_channel: str,
    resolution: str, confidence: float, props_subset: dict | None = None,
) -> None:
    """EXACTLY ONE staged edge (src -> dst_channel) of edge_type, with the given
    resolution/confidence and (optionally) a props SUBSET (extra props are fine --
    e.g. evidence-adjacent keys -- but every pinned key must match exactly)."""
    src_id = _find_node_id(staging, *src)
    edges = _edges_between(staging, edge_type, src_id, dst_channel)
    if len(edges) != 1:
        problems.append(
            f"{label}: expected exactly ONE {edge_type} {src_id!r} -> {dst_channel!r}, "
            f"found {len(edges)}: {edges}"
        )
        return
    edge = edges[0]
    if (edge.resolution, edge.confidence) != (resolution, confidence):
        problems.append(
            f"{label}: expected (resolution={resolution!r}, confidence={confidence!r}), "
            f"got (resolution={edge.resolution!r}, confidence={edge.confidence!r})"
        )
    for key, want in (props_subset or {}).items():
        got = edge.props.get(key)
        if got != want:
            problems.append(
                f"{label}: expected props[{key!r}] == {want!r}, got {got!r} "
                f"(full props: {edge.props})"
            )


def _sorted_triples(triples: set[tuple]) -> list[tuple]:
    return sorted(triples, key=lambda t: tuple(str(v) for v in t))


def _trace_diff(result: dict) -> list[str]:
    """Ported verbatim from test_m6_gate.py (itself ported from test_m2_gate.py) --
    order-tolerant, set-EXACT comparison against golden/traces.yaml's POST /submit
    trace, which since M7 T6 includes the signal-hop segment."""
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
    """Ported from test_m6_gate.py -- guards substring checks against rich's
    line-wrapping under CliRunner."""
    return "".join(output.split())


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m7_gate(tmp_path, falkordb_cfg):
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
        assert degraded == [], (
            f"realstack must index WITHOUT degrading (first-party-only scip "
            f"resolution suffices for both services) -- degraded: {degraded}"
        )

        # -- (a) per-type precision/recall over the EXTENDED golden ---------------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M7 gate][{edge_type}] precision={pr['precision']:.4f} "
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

        # -- (b) temporal_start-marked CALLS precision/recall ---------------------
        golden_ts = _load_golden_temporal_start(GOLDEN_EDGES)
        found_ts, dangling_ts = _found_temporal_start(staging)
        pr_ts = precision_recall(found_ts, golden_ts)
        print(
            f"\n[M7 gate][temporal_start CALLS] precision={pr_ts['precision']:.4f} "
            f"recall={pr_ts['recall']:.4f} tp={pr_ts['tp']} found={len(found_ts)} "
            f"golden={len(golden_ts)} dangling={dangling_ts}"
        )
        if pr_ts["precision"] != 1.0 or pr_ts["recall"] != 1.0:
            problems.append(
                f"temporal_start CALLS: precision={pr_ts['precision']:.4f} "
                f"recall={pr_ts['recall']:.4f} (want 1.0/1.0); dangling={dangling_ts}\n"
                f"  fp ({len(pr_ts['fp_list'])}): {pr_ts['fp_list']}\n"
                f"  fn ({len(pr_ts['fn_list'])}): {pr_ts['fn_list']}"
            )

        # -- (c) M7 T2: settings-source PRODUCES is static/1.0 --------------------
        _pin_edge(
            problems, staging, "settings-source PRODUCES", "PRODUCES",
            SETTINGS_PRODUCER, SETTINGS_TOPIC_CHANNEL,
            resolution="static", confidence=1.0,
        )

        # -- (d) M7 T2: enum fan-out -- heuristic/0.8, mechanism, callsite dedup --
        for chan_id in ENUM_TOPIC_CHANNELS:
            _pin_edge(
                problems, staging, f"enum-fanout PRODUCES -> {chan_id}", "PRODUCES",
                ENUM_PRODUCER, chan_id,
                resolution="heuristic", confidence=0.8,
                props_subset={"mechanism": "enum_fanout", "callsite_count": ENUM_CALLSITE_COUNT},
            )

        # -- (e) M7 T4: signal pair pins + @workflow.query role-only negative -----
        _pin_edge(
            problems, staging, "signal sender PRODUCES", "PRODUCES",
            SIGNAL_SENDER, SIGNAL_CHANNEL,
            resolution="heuristic", confidence=0.6,
            props_subset={"mechanism": "temporal_signal"},
        )
        _pin_edge(
            problems, staging, "signal handler CONSUMES", "CONSUMES",
            SIGNAL_HANDLER, SIGNAL_CHANNEL,
            resolution="static", confidence=1.0,
            props_subset={"signal_kind": "signal"},
        )
        query_id = _find_node_id(staging, *QUERY_HANDLER)
        query_node = next(n for n in staging.iter_nodes() if n.id == query_id)
        if "TemporalSignalHandler" not in query_node.roles:
            problems.append(
                f"@workflow.query vacuity guard: {query_id!r} must carry the "
                f"TemporalSignalHandler role (decorator seen) -- roles: {query_node.roles}"
            )
        # Channel-boundary edges only: python_core's structural CONTAINS
        # (class -> method) legitimately touches the query node and is not what
        # this negative is about.
        query_channel_edges = [
            e for e in staging.iter_edges()
            if e.src == QUERY_WOULD_BE_CHANNEL or e.dst == QUERY_WOULD_BE_CHANNEL
            or (e.type in ("PRODUCES", "CONSUMES") and query_id in (e.src, e.dst))
        ]
        if query_channel_edges:
            problems.append(
                "@workflow.query must be ROLE-ONLY -- no channel edge may touch the "
                f"handler, and its would-be channel {QUERY_WOULD_BE_CHANNEL!r} must not "
                f"exist in any edge; found: {query_channel_edges}"
            )

        # -- (f) M7 T3: BOTH CALLS_HTTP edges anchored static/1.0 -----------------
        for src, chan_id in HTTP_PINS.items():
            _pin_edge(
                problems, staging, f"anchored CALLS_HTTP {src[1]}", "CALLS_HTTP",
                src, chan_id,
                resolution="static", confidence=1.0,
            )

        # -- (g) M7 T3: funnel NEGATIVE (with vacuity guard) ----------------------
        funnel_exists = any(n.id == FUNNEL_CHANNEL for n in staging.iter_nodes())
        if not funnel_exists:
            problems.append(
                f"funnel vacuity guard: channel {FUNNEL_CHANNEL!r} must EXIST as a "
                "staged route (the negative below would otherwise be vacuously true)"
            )
        funnel_hits = [
            e for e in staging.iter_edges()
            if e.type == "CALLS_HTTP" and e.dst in (FUNNEL_CHANNEL, FUNNEL_CHANNEL_FALLBACK)
        ]
        if funnel_hits:
            problems.append(
                f"funnel NEGATIVE violated: {len(funnel_hits)} CALLS_HTTP edge(s) into "
                f"the all-params misc route -- this is the OPEN R1 false-match bug "
                f"resurfacing: {funnel_hits}"
            )

        # -- resolve entrypoint (staging-side, mirrors M6) ------------------------
        entrypoint_id = resolve_selector(staging, ENTRYPOINT_SELECTOR)
        if entrypoint_id is None:
            problems.append(
                f"entrypoint not resolved for selector {ENTRYPOINT_SELECTOR!r}"
            )

        # -- load into FalkorDB (S9, blue/green; zero-drop pin mirrors M6) --------
        load_stats = load_graph(
            staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME
        )
        unexpected_drops = {
            t: n for t, n in load_stats["edges_dropped_by_type"].items() if n
        }
        if unexpected_drops:
            problems.append(
                f"load_graph dropped UNEXPECTED edges: {unexpected_drops} "
                f"(full breakdown: {load_stats['edges_dropped_by_type']})"
            )
        staging.close()

        # -- (h) trace incl. the signal hop, and CLI verification -----------------
        if entrypoint_id is not None:
            gq = GraphQuery(
                store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
                service_paths={svc.name: svc.path for svc in cfg.services},
            )
            result = gq.trace_process(entrypoint_id)
            if "error" in result:
                problems.append(f"trace_process error: {result['error']}")
            else:
                print(f"\n[M7 gate][trace] segments={len(result['segments'])} "
                      f"confidence={result['confidence']}")
                problems.extend(_trace_diff(result))

            runner = CliRunner()
            cli_result = runner.invoke(
                app, ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME]
            )
            print(f"\n[M7 gate][cli trace text]\n{cli_result.output}")
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
