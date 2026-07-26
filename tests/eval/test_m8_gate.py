"""M8 gate: fixtures/realstack (extended in M8 T3) proves both M8 mechanisms
(rerun-2 R4/R5, docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md)
end-to-end, against REAL scip-python output -- closing the T2 reviewer's own
open concern (the typed-signal path had unit coverage only, never proven against a
real fixture) and proving R4's transitive router-prefix composition, INCLUDING the
aggregator's-own-prefix fold-in (M8 review Important-1), on real scip too. Mirrors
tests/eval/test_m7_gate.py's harness verbatim (module docstring, `-m scip`/
`-m falkordb` marking, `shutil.which("npx")` skip, tmp_path staging, print-then-assert
diagnostics, ONE `problems` list asserted once at the end) over the SAME workspace and
the SAME (further extended) golden -- every M6/M7-era leg this gate re-checks is
UNCHANGED from those gates' own pins; this gate's additional value is the M8-specific
pins below. Helper functions are PORTED from test_m7_gate.py rather than imported --
same self-contained-test-module convention M7 itself inherited from M6/M2.

  (a) per-type precision/recall over {INVOKES_ACTIVITY, CALLS_HTTP, CONSUMES,
      PRODUCES, HANDLES} against the FURTHER EXTENDED fixtures/realstack/golden/
      edges.yaml -- HANDLES joins the type set for the FIRST time in this milestone's
      gates (M6/M7 never checked it; evalx.edges_eval already supports it natively,
      see that module's own docstring for the channel<->handler direction
      normalization). P=R=1.0 over HANDLES is a DIRECT, exhaustive proof that
      router_prefix.py (S7) composed the byte-exact channel id for EVERY real route in
      the fixture -- both gateway's now-NONTRIVIAL, multi-hop, cross-file chain
      (app/routes/submit.py + app/routes/ops.py's bare leaf routers -> app/routes/
      __init__.py's aggregator, own prefix "/v1" -> app/main.py's include-kwarg prefix
      "/api") and worker's UNCHANGED trivial single-file case (the regression pin).
      This is strictly stronger than route_prefix_unresolved==0 alone, which only
      proves "some confident template was computed", never that it's the CORRECT one.
  (b) temporal_start-marked CALLS precision/recall -- ported M7 check, unchanged scope.
  (c)-(g): ported M7 T2/T3 pins verbatim (settings-source PRODUCES, enum-fanout
      PRODUCES, BOTH CALLS_HTTP anchoring pins, funnel NEGATIVE) -- gateway's own
      restructuring does not touch worker's routes (DocClient/StatusClient both
      target WORKER, unaffected by gateway's own prefix change; verified by reading
      the fixture before writing this gate, not assumed) or any of these legs, so
      every M7-era pin here is a straight regression check, byte-identical.
  (h) M7 T4 signal pins, PLUS the M8 T3 addition: a SECOND, same-service TYPED
      PRODUCES edge (DocActivities.publish_submitted_event -> the SAME
      chan:temporal_signal:doc-approved channel worker's pre-existing string-based
      sender already targets) at (static, 1.0) -- a resolved symbol reference is full
      ground truth, mirrors INVOKES_ACTIVITY/HANDLES. The pre-existing string-based
      sender pin (heuristic, 0.6) is kept UNCHANGED alongside it -- both legs coexist,
      neither replaces the other. route_prefix_unresolved==0 and signal_send_
      unlinked==0 are asserted directly off linking.workspace.link_workspace's own
      returned counters (a CLEAN fixture must produce CLEAN counters -- any nonzero
      value here means the fixture accidentally exercises a failure shape it isn't
      meant to, or the mechanism itself regressed).
  (i) cross-service trace_process vs golden/traces.yaml -- the segment SET is
      byte-identical to M7's own (still 4 segments, same (service, entry, via_channel)
      triples): worker's HTTP hop channel name is untouched (worker doesn't change),
      and the NEW typed-signal producer feeds into the SAME already-golden channel/
      entry the pre-existing string producer already reaches, which the trace's own
      set-based per-entry comparison (incoming channel ids, not producer counts)
      treats as unchanged -- verified empirically, not assumed (see _trace_diff's own
      per-entry "incoming" set construction). This alone would NOT distinguish "only
      the old string producer still works" from "both now work", so this gate ALSO
      directly inspects segment 1's (entry=submit_document) own `exits` list for a
      NEW exit into chan:temporal_signal:doc-approved -- proof the typed sender
      (DocActivities.publish_submitted_event, reached via
      DocSubmissionWorkflow.run's INVOKES_ACTIVITY, itself reached via submit_document's
      temporal_start CALLS -- all intra-segment edge types) was actually walked as
      part of THIS trace, not merely that its PRODUCES edge exists in isolation.

`degraded` is asserted `== []` EXPLICITLY (same rationale as M6/M7: a degraded run
would silently weaken every check above -- realstack indexes with first-party-only
scip resolution on both services, no venv needed). Gate is NOT weakened on failure and
golden is NOT edited to make it pass -- extractors/linking get fixed instead (this
milestone's brief, verbatim rule, ported from M6/M7)."""

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

# M8: HANDLES joins the gated type set for the first time -- see module docstring (a).
GATE_TYPES = ("INVOKES_ACTIVITY", "CALLS_HTTP", "CONSUMES", "PRODUCES", "HANDLES")

GRAPH_NAME = "__m8_gate__"
# M8 T3: gateway's own route composes to "/api/v1/submit" now (a real cross-file
# include_router chain, R4) -- the pre-M8 "gateway:POST /submit" selector no longer
# resolves any staged route (see fixtures/realstack/workspace.yaml's own updated
# `processes:` entry and golden/traces.yaml's updated top-level `entrypoint:`).
ENTRYPOINT_SELECTOR = "gateway:POST /api/v1/submit"

# -- M7-era pin targets (ported verbatim from test_m7_gate.py -- unaffected by M8) --
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

# -- M8 T3 (rerun-2 R5) new pin: same-service TYPED signal sender ------------------
TYPED_SIGNAL_SENDER = ("gateway", "app.activities.docs.DocActivities.publish_submitted_event")

# -- M8 T3 (rerun-2 R4) new pins: gateway's own composed HTTP route identity -------
GATEWAY_SUBMIT_HANDLER = ("gateway", "app.routes.submit.submit_document")
GATEWAY_SUBMIT_CHANNEL = ids.chan_http("gateway", "POST", "/api/v1/submit")

# CLI trace output check (mirrors M7 gate's list -- unaffected by M8: no new symbol
# reachable from the entrypoint's trace needs a fresh token, publish_submitted_event
# and DocSubmissionWorkflow/doc_approved already cover the typed-signal sender/handler
# pair's own enclosing names).
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


def _run_pipeline(
    cfg: WorkspaceConfig, staging: Staging, cache_dir: Path,
) -> tuple[list[str], dict]:
    """Ported from test_m7_gate.py (itself ported from M6/M2), EXTENDED to also
    return link_workspace's own stats dict -- M7's version discarded it, keeping only
    the degraded-services list; M8 needs route_prefix_unresolved/signal_send_unlinked
    off that same dict (see linking/workspace.py's own docstring for both keys)."""
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
    """Ported from test_m7_gate.py -- golden CALLS records with mechanism:
    temporal_start (edges_eval deliberately excludes them, see its docstring)."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "temporal_start":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_temporal_start(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Ported from test_m7_gate.py -- staged mechanism="temporal_start" CALLS."""
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
    resolution/confidence and (optionally) a props SUBSET. Ported from test_m7_gate.py
    verbatim."""
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
    """Ported verbatim from test_m7_gate.py -- order-tolerant, set-EXACT comparison
    against golden/traces.yaml's POST /submit trace. M8 T3 does NOT change this
    golden file: worker's HTTP hop channel name is untouched (worker's routing isn't
    restructured), and the new typed-signal producer feeds into the SAME
    already-golden channel/entry the pre-existing string producer already reaches --
    per-entry "incoming channel ids" is a SET, so a second producer into the identical
    channel doesn't add a new distinct value (see test_m8_gate below for the
    complementary DIRECT exit-existence check this set-comparison alone can't make)."""
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
    """M8 T3 addition: _trace_diff's set-based per-entry comparison alone can't tell
    "only the pre-existing string producer reaches doc_approved" apart from "both the
    string AND the new typed producer do" -- both collapse to the identical golden
    triple (see _trace_diff's own docstring). This directly inspects segment 1's
    (entry=submit_document) OWN `exits` list for a NEW exit into
    chan:temporal_signal:doc-approved -- proof DocActivities.publish_submitted_event's
    typed sender was actually WALKED as an intra-segment step of THIS trace (reached
    via DocSubmissionWorkflow.run's INVOKES_ACTIVITY, itself reached via
    submit_document's own temporal_start CALLS), not merely that its PRODUCES edge
    exists somewhere in isolation."""
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


def _cli_output_flat(output: str) -> str:
    """Ported from test_m7_gate.py -- guards substring checks against rich's
    line-wrapping under CliRunner."""
    return "".join(output.split())


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m8_gate(tmp_path, falkordb_cfg):
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
        print(f"\n[M8 gate][link_stats] {link_stats}")

        # -- (a) per-type precision/recall over the FURTHER EXTENDED golden --------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M8 gate][{edge_type}] precision={pr['precision']:.4f} "
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

        # -- (b) temporal_start-marked CALLS precision/recall (ported, unaffected) --
        golden_ts = _load_golden_temporal_start(GOLDEN_EDGES)
        found_ts, dangling_ts = _found_temporal_start(staging)
        pr_ts = precision_recall(found_ts, golden_ts)
        print(
            f"\n[M8 gate][temporal_start CALLS] precision={pr_ts['precision']:.4f} "
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

        # -- (c) M7 T2: settings-source PRODUCES is static/1.0 (ported) ------------
        _pin_edge(
            problems, staging, "settings-source PRODUCES", "PRODUCES",
            SETTINGS_PRODUCER, SETTINGS_TOPIC_CHANNEL,
            resolution="static", confidence=1.0,
        )

        # -- (d) M7 T2: enum fan-out (ported) ---------------------------------------
        for chan_id in ENUM_TOPIC_CHANNELS:
            _pin_edge(
                problems, staging, f"enum-fanout PRODUCES -> {chan_id}", "PRODUCES",
                ENUM_PRODUCER, chan_id,
                resolution="heuristic", confidence=0.8,
                props_subset={"mechanism": "enum_fanout", "callsite_count": ENUM_CALLSITE_COUNT},
            )

        # -- (e) M7 T4 signal pair pins + @workflow.query role-only negative (ported)
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
        # -- M8 T3 (rerun-2 R5): the NEW same-service TYPED sender, alongside the
        # pre-existing string sender above -- both coexist, neither replaces the
        # other (two independent PRODUCES rows into the same channel).
        _pin_edge(
            problems, staging, "signal sender PRODUCES (typed, M8 T3 new)", "PRODUCES",
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

        # -- (f) M7 T3: BOTH CALLS_HTTP edges anchored static/1.0 (ported, unaffected
        # by gateway's own route restructuring -- both clients target WORKER) -------
        for src, chan_id in HTTP_PINS.items():
            _pin_edge(
                problems, staging, f"anchored CALLS_HTTP {src[1]}", "CALLS_HTTP",
                src, chan_id,
                resolution="static", confidence=1.0,
            )

        # -- M8 T3 (rerun-2 R4): gateway's own composed HANDLES edge, belt-and-braces
        # alongside the exhaustive golden HANDLES section above -- resolution/
        # confidence aren't compared by evalx.edges_eval at all (verified by reading
        # it, see this module's own docstring point (a)), so this is the ONLY place
        # static/1.0 is actually pinned for the newly-composed, non-trivial chain.
        # NOTE: HANDLES' STAGED direction is Channel -> handler (the reverse of
        # CALLS_HTTP/PRODUCES, see edges_eval.py's own docstring) -- _pin_edge's
        # (src, dst_channel) parameter order assumes src resolves to a sym node id
        # and queries edges FROM it, so the channel id is passed as `src` here and
        # the handler's (service, symbol) as the literal dst node id.
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

        # -- (g) M7 T3: funnel NEGATIVE (with vacuity guard, ported) ----------------
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

        # -- M8 T1/T2 own honest-miss counters: a CLEAN fixture must produce CLEAN
        # counters -- any nonzero value means the fixture accidentally exercises a
        # failure shape it isn't meant to, or the mechanism itself regressed. -------
        if link_stats.get("route_prefix_unresolved", 0) != 0:
            problems.append(
                f"route_prefix_unresolved must be 0 on this clean fixture, got "
                f"{link_stats.get('route_prefix_unresolved')!r} (see "
                "linking/router_prefix.py's own honesty-rule failure shapes)"
            )
        if link_stats.get("signal_send_unlinked", 0) != 0:
            problems.append(
                f"signal_send_unlinked must be 0 on this clean fixture, got "
                f"{link_stats.get('signal_send_unlinked')!r} (see "
                "linking/signal_send.py's own honesty rule)"
            )

        # -- resolve entrypoint (staging-side, mirrors M7) ------------------------
        entrypoint_id = resolve_selector(staging, ENTRYPOINT_SELECTOR)
        if entrypoint_id is None:
            problems.append(
                f"entrypoint not resolved for selector {ENTRYPOINT_SELECTOR!r}"
            )

        # -- load into FalkorDB (S9, blue/green; zero-drop pin mirrors M7) --------
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

        # -- (i) trace incl. BOTH signal hops, and CLI verification -----------------
        if entrypoint_id is not None:
            gq = GraphQuery(
                store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
                service_paths={svc.name: svc.path for svc in cfg.services},
            )
            result = gq.trace_process(entrypoint_id)
            if "error" in result:
                problems.append(f"trace_process error: {result['error']}")
            else:
                print(f"\n[M8 gate][trace] segments={len(result['segments'])} "
                      f"confidence={result['confidence']}")
                problems.extend(_trace_diff(result))
                problems.extend(_typed_signal_hop_diff(result))

            runner = CliRunner()
            cli_result = runner.invoke(
                app, ["trace", ENTRYPOINT_SELECTOR, str(ws_root), "--graph", GRAPH_NAME]
            )
            print(f"\n[M8 gate][cli trace text]\n{cli_result.output}")
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
