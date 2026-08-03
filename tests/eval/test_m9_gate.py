"""M9 gate: fixtures/realstack (extended in M9 task-5) proves all THREE M9
polish-to-ideal mechanisms (docs/superpowers/plans/2026-07-28-m9-polish-to-ideal.md,
spec = docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3's honest disclaimers)
end-to-end, against REAL scip-python output. Mirrors tests/eval/test_m8_gate.py's
harness verbatim (module docstring, `-m scip`/`-m falkordb` marking,
`shutil.which("npx")` skip, tmp_path staging, print-then-assert diagnostics, ONE
`problems` list asserted once at the end) over the SAME workspace and the SAME
(further extended) golden -- every M6/M7/M8-era leg this gate re-checks is
UNCHANGED from those gates' own pins; this gate's additional value is the M9-specific
pins below. Helper functions are PORTED from test_m8_gate.py rather than imported --
same self-contained-test-module convention M8 itself inherited from M6/M7/M2.

  (T1) external HTTP targets get first-class tier-2a semantics (linking/
      http_routes.py): AuditClient.submit_audit_event (app/clients/audit_client.py)
      is anchored via SERVICE_AUDIT_URL (env_values.yaml) to a REAL, known hostname
      (audit.ext.prod.env) that names NO workspace service -- proven THREE ways:
      (a) staging-level -- the synthetic channel carries `external=True` +
      `external_host="audit.ext.prod.env"` additively (id form UNCHANGED, still
      owner="?", still heuristic/0.5 -- "no unearned confidence"), and
      link_workspace's own `calls_http_external` counter (NOT `calls_http_
      unresolved`) counts exactly this one claim; (b) LIVE FALKORDB ROUND-TRIP --
      M9-T1's own reviewer carry (progress.md M9-T1: "⚠️-carry в T5: external=True
      через реальный FalkorDB round-trip"): after S9 load_graph, `_external_exit_
      hop_diff` inspects the SAME props read back off the trace segment's exit --
      built from `GraphQuery.trace_process`, which walks the LIVE FalkorDB-backed
      store, not staging -- proving the prop survives the blue/green load, not just
      the in-memory staging round-trip unit tests already cover; (c) trace
      aggregate confidence -- the external exit's own honest heuristic/0.5 is
      EXCLUDED from the trace's aggregate-confidence floor (query/traverse.py), so
      the WHOLE trace's confidence stays at its pre-existing baseline (0.6, driven
      by worker's base_class-textual CONSUMES tier, M6 T4 -- see BASELINE_
      CONFIDENCE's own comment for how this number was established) instead of
      being dragged to 0.5 by this NEW exit -- a genuine differential proof: if the
      exclusion regressed, this exact assertion would observe 0.5, not 0.6.
      `external_exit_count == 1` is the companion machine-readable count (M9 T1
      review Important).
  (T2) compose-back: router_prefix.link patches a route handler's OWN node props
      (`path_template`, staged LOCAL-only by fastapi_ext.py in S5) to the
      S7-composed value -- gateway's four routes already exercise this (M8's own
      multi-hop "/api/v1" chain), so THIS gate's own novel proof is the LIVE
      --incremental sub-case below (T2's binding carry), not a fresh staging pin.
  (T3) multi-mount routers: worker's admin_ping (app/routes/admin.py) is mounted
      TWICE from app/main.py -- `app.include_router(admin_router, prefix="/v1")` +
      `app.include_router(admin_router, prefix="/legacy")`, the brief's own literal
      double-mount scenario (same parent, two include-kwarg prefixes). Proven via
      the exhaustive golden HANDLES section (both channels + both HANDLES edges,
      P=R=1.0) PLUS a direct compose-back props pin (`path_template` = the FIRST
      template by lexicographic sort, `path_templates` = both, sorted -- see
      linking/router_prefix.py's own "M9 T3" docstring section for why).

BINDING CARRY (T2 review, progress.md M9-T2: "T5-гейт ОБЯЗАН включить live
--incremental под-кейс на realstack с композитными путями" -- the M4-gate's own
dump-equivalence proof was instrumented by that task's reviewer and found to call
`staging.update_node_props` ZERO times against fixtures/services, whose routes all
compose trivially; realstack is the FIRST fixture whose OWN routes exercise a
non-trivial compose-back patch, 4 calls): `test_m9_gate_incremental_compose_back`
below, a SEPARATE test in this same module, edits a COMPOSED route handler's body
(gateway's app/routes/ops.py::submit_decision, reachable only through the SAME
multi-hop "/api/v1" aggregator chain M8 built) through the REAL CLI
(`codegraph index --incremental`, `typer.testing.CliRunner`, no monkeypatching),
and asserts (a) the handler node's OWN `path_template` prop STILL carries the FULL
composed template afterward -- S5 (fastapi_ext.py) unconditionally re-stages the
node LOCAL-only via `upsert_nodes`' INSERT-OR-REPLACE whenever ITS OWN file goes
stale (wiping any earlier S7 patch's props wholesale, not merging), and S7
(router_prefix.link) ALWAYS runs in FULL immediately after, on every `codegraph
index` invocation, incremental or not (linking/workspace.py's own docstring) -- so
the very next `link_workspace` call re-composes and re-patches before the run
ends; a broken compose-back mechanism would observably regress this prop to the
LOCAL-only fragment instead -- and (b) dump-equivalence against a FRESH FULL
reindex of the identically-edited tree (the M4-gate's own "supreme invariant"),
using PORTED (not imported -- see this module's own self-contained-test-module
convention above) canonical-dump helpers (`_freeze`/`_props`/`_staging_dump`/
`_graph_dump`/`_dump_diff`, byte-identical to tests/eval/test_incremental_gate.py's
own versions, which that module's own docstring already establishes as fully
fixture-agnostic pure functions over any `Staging`/`FalkorStore`).

`degraded` is asserted `== []` EXPLICITLY (same rationale as M6/M7/M8: a degraded
run would silently weaken every check above -- realstack indexes with first-party-
only scip resolution on both services, no venv needed). Gate is NOT weakened on
failure and golden is NOT edited to make it pass -- extractors/linking get fixed
instead (this milestone's brief, verbatim rule, ported from M6/M7/M8)."""

from __future__ import annotations

import json
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

# Unchanged from M8 -- HANDLES already joined the gated type set there.
GATE_TYPES = ("INVOKES_ACTIVITY", "CALLS_HTTP", "CONSUMES", "PRODUCES", "HANDLES")

GRAPH_NAME = "__m9_gate__"
# Unchanged from M8 T3 -- neither the external nor the multi-mount leg touches
# gateway's own composed submit route.
ENTRYPOINT_SELECTOR = "gateway:POST /api/v1/submit"

# -- M7/M8-era pin targets (ported verbatim from test_m8_gate.py -- unaffected by
# either M9 realstack leg) --
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
QUERY_WOULD_BE_CHANNEL = ids.chan_temporal_signal("approval_state")
HTTP_PINS = {
    ("gateway", "app.clients.doc_client.DocClient.fetch_document"):
        ids.chan_http("worker", "GET", "/documents/{doc_uid}"),
    ("gateway", "app.clients.status_client.StatusClient.fetch_status"):
        ids.chan_http("worker", "GET", "/api/v1/status/{doc_uid}"),
}
FUNNEL_CHANNEL = ids.chan_http("worker", "GET", "/{a}/{b}/{c}/misc")
FUNNEL_CHANNEL_FALLBACK = ids.chan_http(None, "GET", "/{a}/{b}/{c}/misc")
TYPED_SIGNAL_SENDER = ("gateway", "app.activities.docs.DocActivities.publish_submitted_event")
GATEWAY_SUBMIT_HANDLER = ("gateway", "app.routes.submit.submit_document")
GATEWAY_SUBMIT_CHANNEL = ids.chan_http("gateway", "POST", "/api/v1/submit")

# -- M9 T1 (task-5) new pin targets: external HTTP target -----------------------
AUDIT_CALLSITE = ("gateway", "app.clients.audit_client.AuditClient.submit_audit_event")
AUDIT_CHANNEL = ids.chan_http(None, "POST", "/audit/events")
AUDIT_HOST = "audit.ext.prod.env"
# Established empirically off THIS fixture's own pre-M9 (M8-era) baseline gate run
# (test_m8_gate.py, unchanged by either M9 leg): worker's base_class-textual
# CONSUMES tier (DocSubmittedConsumer.process_event, M6 gap 4 -- IMPORT_NAME
# heuristic/0.6, no installed kyc_base_consumer venv) is the SOLE sub-1.0 edge
# anywhere on this trace's own walk, so the trace's pre-existing aggregate
# confidence is 0.6, NOT 1.0 -- pinning "stays 0.6" (not "==1.0") is the genuine
# differential proof for T1's own exclusion mechanism: were the NEW external
# exit's own heuristic/0.5 NOT excluded, the aggregate would instead read
# min(0.6, 0.5) == 0.5.
BASELINE_CONFIDENCE = 0.6

# -- M9 T3 (task-5) new pin targets: multi-mount router --------------------------
ADMIN_HANDLER = ("worker", "app.routes.admin.admin_ping")
ADMIN_CHANNEL_V1 = ids.chan_http("worker", "GET", "/v1/ping")
ADMIN_CHANNEL_LEGACY = ids.chan_http("worker", "GET", "/legacy/ping")

# CLI trace output check (extends M8 gate's own list -- unaffected legs' tokens are
# unchanged; "external"/the audit host prove the NEW leg renders, per cli.py's own
# `_trace_tree`/`_trace_mermaid` "external <host>" convention, query/traverse.py's
# module docstring).
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
    "external",
    AUDIT_HOST,
)


def _run_pipeline(
    cfg: WorkspaceConfig, staging: Staging, cache_dir: Path,
) -> tuple[list[str], dict]:
    """Ported from test_m8_gate.py (itself ported from M6/M7/M2) verbatim."""
    active_idioms = frozenset(cfg.builtin_idioms)
    degraded_services: list[str] = []
    for svc in cfg.services:
        report = analyze_service(
            svc, staging, cache_dir, runner=None,
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
        if report["degraded"]:
            degraded_services.append(svc.name)
    link_stats = link_workspace(cfg, staging)
    return degraded_services, link_stats


def _load_golden_temporal_start(path: Path) -> set[tuple[str, str, str, str]]:
    """Ported from test_m8_gate.py -- golden CALLS records with mechanism:
    temporal_start (edges_eval deliberately excludes them, see its docstring)."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "temporal_start":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_temporal_start(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Ported from test_m8_gate.py -- staged mechanism="temporal_start" CALLS."""
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
    resolution/confidence and (optionally) a props SUBSET. Ported from
    test_m8_gate.py verbatim."""
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
    """Ported verbatim from test_m8_gate.py -- order-tolerant, set-EXACT comparison
    against golden/traces.yaml's POST /submit trace. Neither M9 leg changes this
    golden file's own segment SET: the external exit (T1) is a true dead-end (no
    in-workspace consumer, see golden/traces.yaml's own M9 header) and the
    multi-mount route (T3) is unreached by this trace entirely -- see
    test_m9_gate below for the DIRECT external-exit check this set-comparison
    alone can't make (mirrors _typed_signal_hop_diff's own M8-era precedent)."""
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


def _typed_signal_hop_diff(result: dict) -> list[str]:
    """Ported verbatim from test_m8_gate.py -- direct segment-1-exit check for the
    typed signal sender (unaffected by either M9 leg)."""
    problems: list[str] = []
    segments = result["segments"]
    submit_segment = next(
        (
            s for s in segments
            if (s.get("entry") or {}).get("qualified_name") == "app.routes.submit.submit_document"
        ),
        None,
    )
    if submit_segment is None:
        problems.append(
            "typed-signal hop: segment with entry=app.routes.submit.submit_document not found"
        )
        return problems
    exit_channels = {(ex.get("channel") or {}).get("id") for ex in submit_segment.get("exits", [])}
    if SIGNAL_CHANNEL not in exit_channels:
        problems.append(
            f"typed-signal hop: segment 1 (submit_document) must have an exit into "
            f"{SIGNAL_CHANNEL!r} (DocActivities.publish_submitted_event's NEW typed "
            f"sender, reached via INVOKES_ACTIVITY from DocSubmissionWorkflow.run) -- "
            f"exit channels found: {sorted(c for c in exit_channels if c is not None)}"
        )
    return problems


def _external_exit_hop_diff(result: dict) -> list[str]:
    """M9 T1 (task-5) addition, mirrors _typed_signal_hop_diff's own precedent
    exactly: _trace_diff's set-based per-entry comparison alone can't see an exit
    that resolves to ZERO next entries at all (the external channel has no
    in-workspace consumer by construction) -- this directly inspects segment 1's
    (entry=submit_document) own `exits` list for the NEW external channel, proving
    (a) AuditClient.submit_audit_event's CALLS_HTTP was actually WALKED as part of
    THIS trace (reached via publish_submitted_event's own CALLS, itself reached via
    DocSubmissionWorkflow.run's INVOKES_ACTIVITY, itself reached via
    submit_document's own temporal_start CALLS -- all intra-segment edge types), and
    (b) the `external`/`external_host` fields (M10 T4 -- exit-entry-level now,
    edge-sourced, see query/traverse.py's `_resolve_exits`; were `channel`-nested
    pre-M10) survive the FULL round trip this `result` was built from --
    `GraphQuery.trace_process` over the LIVE FalkorDB-backed store (not staging),
    closing the M9-T1 reviewer's own carry (progress.md M9-T1: "external=True через
    реальный FalkorDB round-trip")."""
    problems: list[str] = []
    segments = result["segments"]
    submit_segment = next(
        (
            s for s in segments
            if (s.get("entry") or {}).get("qualified_name") == "app.routes.submit.submit_document"
        ),
        None,
    )
    if submit_segment is None:
        problems.append(
            "external hop: segment with entry=app.routes.submit.submit_document not found"
        )
        return problems
    exits_by_channel_id = {
        (ex.get("channel") or {}).get("id"): ex for ex in submit_segment.get("exits", [])
    }
    exit_ = exits_by_channel_id.get(AUDIT_CHANNEL)
    if exit_ is None:
        problems.append(
            f"external hop: segment 1 (submit_document) must have an exit into "
            f"{AUDIT_CHANNEL!r} (AuditClient.submit_audit_event's NEW external "
            f"target, reached via CALLS from publish_submitted_event) -- exit "
            f"channels found: {sorted(c for c in exits_by_channel_id if c is not None)}"
        )
        return problems
    if exit_.get("external") is not True:
        problems.append(
            f"external hop: exit into {AUDIT_CHANNEL!r} read back off the LIVE "
            f"FalkorDB-backed trace must carry external=True, got {exit_.get('external')!r} "
            f"(full exit: {exit_})"
        )
    if exit_.get("external_host") != AUDIT_HOST:
        problems.append(
            f"external hop: exit into {AUDIT_CHANNEL!r} read back off the LIVE "
            f"FalkorDB-backed trace must carry external_host={AUDIT_HOST!r}, got "
            f"{exit_.get('external_host')!r} (full exit: {exit_})"
        )
    return problems


def _cli_output_flat(output: str) -> str:
    """Ported from test_m8_gate.py -- guards substring checks against rich's
    line-wrapping under CliRunner."""
    return "".join(output.split())


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m9_gate(tmp_path, falkordb_cfg):
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
        degraded, link_stats = _run_pipeline(cfg, staging, cache_dir)
        assert degraded == [], (
            f"realstack must index WITHOUT degrading (first-party-only scip "
            f"resolution suffices for both services) -- degraded: {degraded}"
        )
        print(f"\n[M9 gate][link_stats] {link_stats}")

        # -- per-type precision/recall over the FURTHER EXTENDED golden -----------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M9 gate][{edge_type}] precision={pr['precision']:.4f} "
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

        # -- temporal_start-marked CALLS precision/recall (ported, unaffected) ----
        golden_ts = _load_golden_temporal_start(GOLDEN_EDGES)
        found_ts, dangling_ts = _found_temporal_start(staging)
        pr_ts = precision_recall(found_ts, golden_ts)
        print(
            f"\n[M9 gate][temporal_start CALLS] precision={pr_ts['precision']:.4f} "
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

        # -- M7/M8-era pins (ported verbatim, unaffected by either M9 leg) --------
        _pin_edge(
            problems, staging, "settings-source PRODUCES", "PRODUCES",
            SETTINGS_PRODUCER, SETTINGS_TOPIC_CHANNEL,
            resolution="static", confidence=1.0,
        )
        for chan_id in ENUM_TOPIC_CHANNELS:
            _pin_edge(
                problems, staging, f"enum-fanout PRODUCES -> {chan_id}", "PRODUCES",
                ENUM_PRODUCER, chan_id,
                resolution="heuristic", confidence=0.8,
                props_subset={"mechanism": "enum_fanout", "callsite_count": ENUM_CALLSITE_COUNT},
            )
        _pin_edge(
            problems, staging, "signal sender PRODUCES (string, pre-existing)", "PRODUCES",
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
        _pin_edge(
            problems, staging, "signal sender PRODUCES (typed, M8 T3)", "PRODUCES",
            TYPED_SIGNAL_SENDER, SIGNAL_CHANNEL,
            resolution="static", confidence=1.0,
            props_subset={"mechanism": "temporal_signal"},
        )
        query_id = _find_node_id(staging, *QUERY_HANDLER)
        query_node = next(n for n in staging.iter_nodes() if n.id == query_id)
        if "TemporalSignalHandler" not in query_node.roles:
            problems.append(
                f"@workflow.query vacuity guard: {query_id!r} must carry the "
                f"TemporalSignalHandler role (decorator seen) -- roles: {query_node.roles}"
            )
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
        for src, chan_id in HTTP_PINS.items():
            _pin_edge(
                problems, staging, f"anchored CALLS_HTTP {src[1]}", "CALLS_HTTP",
                src, chan_id,
                resolution="static", confidence=1.0,
            )
        gateway_submit_handler_id = _find_node_id(staging, *GATEWAY_SUBMIT_HANDLER)
        handles_edges = _edges_between(
            staging, "HANDLES", GATEWAY_SUBMIT_CHANNEL, gateway_submit_handler_id
        )
        if len(handles_edges) != 1:
            problems.append(
                "composed HANDLES (gateway multi-hop chain): expected exactly ONE "
                f"HANDLES {GATEWAY_SUBMIT_CHANNEL!r} -> {gateway_submit_handler_id!r}, "
                f"found {len(handles_edges)}: {handles_edges}"
            )
        else:
            handles_edge = handles_edges[0]
            if (handles_edge.resolution, handles_edge.confidence) != ("static", 1.0):
                problems.append(
                    "composed HANDLES (gateway multi-hop chain): expected "
                    "(resolution='static', confidence=1.0), got "
                    f"(resolution={handles_edge.resolution!r}, "
                    f"confidence={handles_edge.confidence!r})"
                )
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

        # -- M9 T1 (task-5): external HTTP target -- staging-level props/counter --
        # NOTE (M10 T4 -- was NODE-level pre-M10, see git history for the old
        # shape): external/external_host/config_ref live on the CALLS_HTTP EDGE
        # itself now (EdgeRec.props, kafka's own `_props_for` convention -- see
        # linking/http_routes.py's own module docstring, "SHARED-CHANNEL PROPS"
        # section, for the full replaced-palliative rationale); the CHANNEL node
        # keeps only `unresolved=True` -- confirmed by reading
        # test_linking_http_routes.py's own
        # test_env_map_hostname_not_a_workspace_service_is_external_tier, which
        # checks `edge.props`, never `ext_chan.props`, for these three keys.
        _pin_edge(
            problems, staging, "external CALLS_HTTP (AuditClient)", "CALLS_HTTP",
            AUDIT_CALLSITE, AUDIT_CHANNEL,
            resolution="heuristic", confidence=0.5,
            props_subset={
                "external": True, "external_host": AUDIT_HOST,
                "config_ref": "SERVICE_AUDIT_URL",
            },
        )
        audit_chan_node = next(
            (n for n in staging.iter_nodes() if n.id == AUDIT_CHANNEL), None
        )
        if audit_chan_node is None:
            problems.append(f"external channel {AUDIT_CHANNEL!r} not found in staging")
        else:
            if audit_chan_node.props.get("unresolved") is not True:
                problems.append(
                    f"external channel {AUDIT_CHANNEL!r}: expected props['unresolved'] "
                    f"== True, got {audit_chan_node.props.get('unresolved')!r} "
                    f"(full props: {audit_chan_node.props})"
                )
            # M10 T4: nothing claim-specific survives on the shared node any more --
            # this is the actual payoff of the fix, worth pinning as a NEGATIVE too.
            leaked = [
                k for k in ("external", "external_host", "config_ref")
                if k in audit_chan_node.props
            ]
            if leaked:
                problems.append(
                    f"external channel {AUDIT_CHANNEL!r}: props {leaked} must NOT "
                    f"live on the shared channel node any more (M10 T4 moved them to "
                    f"the edge) -- full props: {audit_chan_node.props}"
                )
        if link_stats.get("calls_http_external", 0) != 1:
            problems.append(
                f"calls_http_external must be exactly 1 (AuditClient.submit_audit_event, "
                f"the ONLY external-anchored claim in this fixture), got "
                f"{link_stats.get('calls_http_external')!r}"
            )
        if link_stats.get("calls_http_unresolved", 0) != 0:
            problems.append(
                "calls_http_unresolved must stay 0 -- the external leg must be counted "
                f"SEPARATELY (calls_http_external), never folded in here; got "
                f"{link_stats.get('calls_http_unresolved')!r}"
            )

        # -- M9 T3 (task-5): multi-mount -- both HANDLES resolved static/1.0, plus
        # the compose-back props pin (first-sorted template + full path_templates
        # list) -- belt-and-braces alongside the exhaustive golden HANDLES section
        # above (resolution/confidence aren't compared by evalx.edges_eval at all,
        # same rationale as the gateway composed-HANDLES pin above). ---------------
        admin_handler_id = _find_node_id(staging, *ADMIN_HANDLER)
        for label, chan_id in (
            ("multi-mount HANDLES (/v1/ping)", ADMIN_CHANNEL_V1),
            ("multi-mount HANDLES (/legacy/ping)", ADMIN_CHANNEL_LEGACY),
        ):
            edges = _edges_between(staging, "HANDLES", chan_id, admin_handler_id)
            if len(edges) != 1:
                problems.append(
                    f"{label}: expected exactly ONE HANDLES {chan_id!r} -> "
                    f"{admin_handler_id!r}, found {len(edges)}: {edges}"
                )
            elif (edges[0].resolution, edges[0].confidence) != ("static", 1.0):
                problems.append(
                    f"{label}: expected (resolution='static', confidence=1.0), got "
                    f"(resolution={edges[0].resolution!r}, confidence={edges[0].confidence!r})"
                )
        admin_node = next(n for n in staging.iter_nodes() if n.id == admin_handler_id)
        if admin_node.props.get("path_template") != "/legacy/ping":
            problems.append(
                "multi-mount compose-back: admin_ping's path_template must be the "
                "FIRST template by lexicographic sort ('/legacy/ping' < '/v1/ping'), "
                f"got {admin_node.props.get('path_template')!r}"
            )
        if admin_node.props.get("path_templates") != ["/legacy/ping", "/v1/ping"]:
            problems.append(
                "multi-mount compose-back: admin_ping's path_templates must list "
                "BOTH composed templates, sorted, got "
                f"{admin_node.props.get('path_templates')!r}"
            )

        # -- honest-miss counters: a CLEAN fixture must produce CLEAN counters ----
        if link_stats.get("route_prefix_unresolved", 0) != 0:
            problems.append(
                f"route_prefix_unresolved must be 0 on this clean fixture (the "
                f"double-mount route resolves fully, just plurally -- M9 T3 does "
                f"NOT count a multi-template resolution here), got "
                f"{link_stats.get('route_prefix_unresolved')!r}"
            )
        if link_stats.get("signal_send_unlinked", 0) != 0:
            problems.append(
                f"signal_send_unlinked must be 0 on this clean fixture, got "
                f"{link_stats.get('signal_send_unlinked')!r}"
            )

        # -- resolve entrypoint (staging-side, mirrors M8) ------------------------
        entrypoint_id = resolve_selector(staging, ENTRYPOINT_SELECTOR)
        if entrypoint_id is None:
            problems.append(
                f"entrypoint not resolved for selector {ENTRYPOINT_SELECTOR!r}"
            )

        # -- load into FalkorDB (S9, blue/green; zero-drop pin mirrors M8) --------
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

        # -- belt-and-braces: multi-mount compose-back props ALSO survive the LIVE
        # FalkorDB load (node props are loaded verbatim, see pipeline/load.py) ----
        if entrypoint_id is not None:
            gq = GraphQuery(
                store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
                service_paths={svc.name: svc.path for svc in cfg.services},
            )
            live_admin_nodes = FalkorStore(falkordb_cfg, GRAPH_NAME).get_nodes([admin_handler_id])
            if not live_admin_nodes:
                problems.append(
                    f"multi-mount compose-back: admin_ping node {admin_handler_id!r} "
                    "missing from the LIVE FalkorDB graph after load"
                )
            else:
                live_props = live_admin_nodes[0]
                if live_props.get("path_template") != "/legacy/ping":
                    problems.append(
                        "multi-mount compose-back did not survive the FalkorDB load: "
                        f"path_template={live_props.get('path_template')!r}"
                    )
                if live_props.get("path_templates") != ["/legacy/ping", "/v1/ping"]:
                    problems.append(
                        "multi-mount compose-back did not survive the FalkorDB load: "
                        f"path_templates={live_props.get('path_templates')!r}"
                    )

            # -- trace incl. the external exit + BOTH signal hops, and CLI check --
            result = gq.trace_process(entrypoint_id)
            if "error" in result:
                problems.append(f"trace_process error: {result['error']}")
            else:
                print(f"\n[M9 gate][trace] segments={len(result['segments'])} "
                      f"confidence={result['confidence']} "
                      f"external_exit_count={result['external_exit_count']}")
                problems.extend(_trace_diff(result))
                problems.extend(_typed_signal_hop_diff(result))
                problems.extend(_external_exit_hop_diff(result))
                if result["confidence"] != BASELINE_CONFIDENCE:
                    problems.append(
                        f"trace confidence must stay at its pre-existing baseline "
                        f"{BASELINE_CONFIDENCE!r} (the NEW external exit's own "
                        f"heuristic/0.5 must be EXCLUDED from the aggregate, not "
                        f"drag it down to 0.5) -- got {result['confidence']!r}"
                    )
                if result["external_exit_count"] != 1:
                    problems.append(
                        f"external_exit_count must be exactly 1 (ONE external exit, "
                        f"segment 1's AuditClient hop) -- got "
                        f"{result['external_exit_count']!r}"
                    )

            runner = CliRunner()
            cli_result = runner.invoke(
                app, ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME]
            )
            print(f"\n[M9 gate][cli trace text]\n{cli_result.output}")
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


# ============================================================================
# -- BINDING CARRY (T2 review): live --incremental compose-back sub-case.
# Canonical-dump helpers PORTED from tests/eval/test_incremental_gate.py (that
# module's own docstring establishes them as fixture-agnostic pure functions over
# any Staging/FalkorStore -- see this module's own top docstring for why porting,
# not importing, is this codebase's established convention for gate test modules).
# ============================================================================


def _freeze(value):
    """Ported verbatim from test_incremental_gate.py."""
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def _props(raw_json: str) -> tuple:
    """Ported verbatim from test_incremental_gate.py."""
    return _freeze(json.loads(raw_json))


def _staging_dump(staging: Staging) -> dict:
    """Ported verbatim from test_incremental_gate.py -- canonical, sorted dump of
    the ENTIRE workspace staging state via raw SQL (origin_service/via_channel
    included, see that module's own docstring for why both are load-bearing)."""
    db = staging._db  # noqa: SLF001 -- test-only max-rigor introspection, ported as-is.

    nodes = sorted(
        (id_, kind, labels, service, relpath, sb, eb, sl, el, name, qn, ch, _props(props))
        for (id_, kind, labels, service, relpath, sb, eb, sl, el, name, qn, ch, props) in
        db.execute(
            "SELECT id, kind, labels, service, relpath, start_byte, end_byte, start_line, "
            "end_line, name, qualified_name, content_hash, props FROM nodes"
        ).fetchall()
    )
    edges = sorted(
        (src, dst, type_, via, res, conf, ext, ef, el, _props(props), origin)
        for (src, dst, type_, via, res, conf, ext, ef, el, props, origin) in
        db.execute(
            "SELECT src, dst, type, via_channel, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props, origin_service FROM edges"
        ).fetchall()
    )
    chunks = sorted(
        db.execute(
            "SELECT chunk_id, symbol_id, service, relpath, ord, text, start_line, "
            "end_line, content_hash, context_header, embed_model, embedded_hash, "
            "input_hash FROM chunks"
        ).fetchall()
    )
    return {"nodes": nodes, "edges": edges, "chunks": chunks}


def _graph_dump(store: FalkorStore) -> dict:
    """Ported verbatim from test_incremental_gate.py."""
    stats = store.stats()
    node_ids = sorted(row[0] for row in store.raw("MATCH (n) RETURN n.id").result_set)
    edge_triples = sorted(
        tuple(row)
        for row in store.raw("MATCH (a)-[e]->(b) RETURN a.id, type(e), b.id").result_set
    )
    return {"stats": stats, "node_ids": node_ids, "edge_triples": edge_triples}


def _dump_diff(a: dict, b: dict) -> str:
    """Ported verbatim from test_incremental_gate.py."""
    lines = []
    for key in a:
        sa, sb = set(a[key]), set(b[key])
        only_a, only_b = sa - sb, sb - sa
        if only_a or only_b:
            lines.append(f"{key}: only-in-first={len(only_a)} only-in-second={len(only_b)}")
            for row in list(only_a)[:5]:
                lines.append(f"  -A {row}")
            for row in list(only_b)[:5]:
                lines.append(f"  +B {row}")
    return "\n".join(lines) if lines else "(dicts differ only in key set or ordering)"


# -- realstack-specific tmp-copy/workspace helpers (env_sources + libs/, unlike
# test_incremental_gate.py's own fixtures/services-scoped _copy_services, which
# has neither) --


def _copy_realstack_source(dest_root: Path) -> tuple[dict[str, Path], Path]:
    """tmp-copy of realstack's services/ + libs/ + env_values.yaml -- returns
    ({service_name: abs service dir}, abs env_values.yaml path), derived from the
    SOURCE workspace.yaml's own `path`/`env_sources` basenames (not hardcoded)."""
    services_dest = dest_root / "services"
    shutil.copytree(FIXTURES / "services", services_dest)
    shutil.copytree(FIXTURES / "libs", dest_root / "libs")
    env_values_dest = dest_root / "env_values.yaml"
    shutil.copy(FIXTURES / "env_values.yaml", env_values_dest)
    raw = yaml.safe_load((FIXTURES / "workspace.yaml").read_text())
    service_dirs = {
        svc["name"]: services_dest / Path(svc["path"]).name for svc in raw["services"]
    }
    return service_dirs, env_values_dest


def _write_realstack_workspace_yaml(
    dest_dir: Path, service_dirs: dict[str, Path], env_values_path: Path, graph_name: str,
) -> Path:
    """A workspace.yaml pointing at the given (already-copied) service dirs +
    env_values.yaml -- idioms/http/processes/builtin_idioms carried over VERBATIM
    from fixtures/realstack/workspace.yaml, only `path`/`env_sources` (rewritten to
    absolute copied locations) and `graph_name` are touched. Mirrors
    test_incremental_gate.py's own `_write_workspace_yaml`, extended for
    realstack's own `env_sources:` key (fixtures/workspace.yaml, that module's own
    target, has none)."""
    raw = yaml.safe_load((FIXTURES / "workspace.yaml").read_text())
    raw["graph_name"] = graph_name
    for svc in raw["services"]:
        svc["path"] = str(service_dirs[svc["name"]])
    raw["env_sources"] = [str(env_values_path)]
    dest_dir.mkdir(parents=True, exist_ok=True)
    ws_path = dest_dir / "workspace.yaml"
    ws_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return ws_path


def _invoke_index(ws_path: Path, *, incremental: bool = False):
    args = ["index", str(ws_path), "--no-embed"]
    if incremental:
        args.append("--incremental")
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    return result


def _load_report(ws_path: Path) -> dict:
    return json.loads((ws_path.parent / ".codegraph" / "report.json").read_text())


def _assert_not_degraded(report: dict) -> None:
    degraded = [s["service"] for s in report["services"] if s.get("degraded")]
    assert not degraded, f"real scip expected for both services, degraded: {degraded}"


# gateway's app/routes/ops.py::submit_decision -- reached ONLY through the SAME
# multi-hop "/api/v1" aggregator chain M8 built (app/routes/__init__.py's `api`
# router, own prefix "/v1", mounted under app/main.py's include-kwarg prefix
# "/api") -- an INDEPENDENT composed route from the trace entrypoint
# (submit_document), so this sub-case's own edit/assert cannot interact with any
# of test_m9_gate's own trace-shaped checks above.
COMPOSED_DECISION_TEMPLATE = "/api/v1/documents/{doc_uid}/decision"
LOCAL_DECISION_TEMPLATE = "/documents/{doc_uid}/decision"


def _edit_ops_submit_decision_body(gateway_dir: Path) -> None:
    """One function BODY edit (no rename, no signature, no decorator change) --
    submit_decision's own returned dict literal, app/routes/ops.py -- mirrors
    test_incremental_gate.py's own `_edit_orders_api_function_body` shape exactly
    (a plain string-literal replace + a sanity assert that it actually matched)."""
    ops_py = gateway_dir / "app" / "routes" / "ops.py"
    original = ops_py.read_text()
    edited = original.replace('"status": "decided"', '"status": "processed"')
    assert edited != original  # sanity: the replace actually matched something
    ops_py.write_text(edited)


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m9_gate_incremental_compose_back(tmp_path, falkordb_cfg):
    """See this module's own top docstring, "BINDING CARRY" section, for the full
    rationale. Graph-name scoped separately from test_m9_gate's own GRAPH_NAME --
    this test builds and tears down its OWN tmp-copied workspace(s), entirely
    independent of the main gate's direct (uncopied) fixture read."""
    graph_name = "__m9_gate_incremental__"
    full_check_name = f"{graph_name}__full_check"
    graph_names = {graph_name, full_check_name}

    service_dirs, env_values_path = _copy_realstack_source(tmp_path / "src")
    ws_path = _write_realstack_workspace_yaml(
        tmp_path / "main", service_dirs, env_values_path, graph_name,
    )

    try:
        # ==================================================================
        # 1. Full index -> baseline: submit_decision's handler node already
        # carries the COMPOSED template (not the local-only fragment) straight
        # out of a full run (fixture precondition, not the invariant itself).
        # ==================================================================
        _invoke_index(ws_path)
        report_a = _load_report(ws_path)
        _assert_not_degraded(report_a)
        assert all(s["mode"] == "full" for s in report_a["services"]), report_a["services"]

        staging_a = Staging(ws_path.parent / ".codegraph" / "staging.db")
        try:
            handler_a = next(
                n for n in staging_a.iter_nodes()
                if n.service == "gateway"
                and n.qualified_name == "app.routes.ops.submit_decision"
            )
            assert handler_a.props.get("path_template") == COMPOSED_DECISION_TEMPLATE, (
                "fixture precondition: submit_decision must compose to "
                f"{COMPOSED_DECISION_TEMPLATE!r} straight out of a full run, got "
                f"{handler_a.props.get('path_template')!r}"
            )
        finally:
            staging_a.close()

        # ==================================================================
        # 2. Body-only edit (no signature/decorator/idiom change -> a genuine
        # incremental re-analyze, not a fingerprint-driven fallback-to-full) ->
        # --incremental through the REAL CLI.
        # ==================================================================
        _edit_ops_submit_decision_body(service_dirs["gateway"])

        _invoke_index(ws_path, incremental=True)
        report_b = _load_report(ws_path)
        _assert_not_degraded(report_b)
        modes_b = {s["service"]: s["mode"] for s in report_b["services"]}
        assert modes_b["gateway"] == "incremental", modes_b
        assert modes_b["worker"] == "skipped", modes_b

        staging_b = Staging(ws_path.parent / ".codegraph" / "staging.db")
        try:
            handler_b = next(
                n for n in staging_b.iter_nodes()
                if n.service == "gateway"
                and n.qualified_name == "app.routes.ops.submit_decision"
            )
            # THE assertion: S5 (fastapi_ext.py) unconditionally re-staged this
            # node LOCAL-only (app/routes/ops.py went stale) -- a broken
            # compose-back mechanism would observably leave it at
            # LOCAL_DECISION_TEMPLATE here. S7 (router_prefix.link) always runs
            # in FULL immediately after, on every `codegraph index` invocation
            # (linking/workspace.py's own docstring) -- so it must have
            # re-patched the composed value back before this run ended.
            found_template = handler_b.props.get("path_template")
            assert found_template == COMPOSED_DECISION_TEMPLATE, (
                "COMPOSE-BACK REGRESSION: submit_decision's node reverted to the "
                f"LOCAL-only template ({LOCAL_DECISION_TEMPLATE!r}) after "
                f"--incremental re-analyzed its own file, instead of staying at "
                f"the S7-composed {COMPOSED_DECISION_TEMPLATE!r} -- S5 re-emits "
                f"local, S7 must re-patch every full link_workspace call, "
                f"incremental runs included; got {found_template!r}"
            )
            dump_b = _staging_dump(staging_b)
        finally:
            staging_b.close()
        graph_b = _graph_dump(FalkorStore(falkordb_cfg, graph_name))

        # ==================================================================
        # 3. Dump-equivalence vs a FRESH FULL reindex of the SAME edited tree
        # (the M4-gate's own "supreme invariant", ported here -- see this
        # module's own top docstring).
        # ==================================================================
        ws_full_check = _write_realstack_workspace_yaml(
            tmp_path / "full_check", service_dirs, env_values_path, full_check_name,
        )
        _invoke_index(ws_full_check)
        report_c = _load_report(ws_full_check)
        _assert_not_degraded(report_c)
        assert all(s["mode"] == "full" for s in report_c["services"]), report_c["services"]

        staging_c = Staging(ws_full_check.parent / ".codegraph" / "staging.db")
        try:
            dump_c = _staging_dump(staging_c)
        finally:
            staging_c.close()
        graph_c = _graph_dump(FalkorStore(falkordb_cfg, full_check_name))

        assert dump_b == dump_c, (
            "SUPREME INVARIANT violated (M9 realstack compose-back): --incremental "
            f"staging state != full reindex of the same edited tree.\n"
            f"{_dump_diff(dump_b, dump_c)}"
        )
        assert graph_b == graph_c, (
            f"SUPREME INVARIANT violated (FalkorDB): {_dump_diff(graph_b, graph_c)}"
        )
    finally:
        for name in graph_names:
            FalkorStore(falkordb_cfg, name).delete_graph()
            FalkorStore(falkordb_cfg, f"{name}__build").delete_graph()
