"""M10 gate: fixtures/realstack (extended in M10 task-5) proves the milestone's
three agent-experience mechanisms (docs/superpowers/plans/2026-08-03-m10-agent-
experience-and-debts.md, spec = docs/superpowers/reports/2026-08-03-mcp-pilot.md
§4-§5) end-to-end, against REAL scip-python output AND a live FalkorDB. Mirrors
tests/eval/test_m9_gate.py's harness (module docstring shape, `-m scip`/
`-m falkordb` marking, `shutil.which("npx")` skip, tmp_path staging, print-then-
assert diagnostics, ONE `problems` list asserted once at the end, ported-not-
imported helpers) over the SAME workspace and the SAME (further extended)
golden.

  (T1) module-level singleton dispatch (pilot §5, parsing/module_singletons.py):
      worker's admin_ping (app/routes/admin.py) calls `store.persist(...)` on the
      module-level DocStore singleton (app/services/doc_store.py's `store =
      DocStore(...)`) -- the pilot's #1 real-world dropped-CALLS shape (53% of
      149 on the real corpus), unit-tested synthetically by Task 1, verified here
      against a REAL scip-python run for the first time. Proven via a dedicated,
      mechanism-filtered found/golden pair (`_found_singleton_dispatch`/
      `_load_golden_singleton_dispatch`) that mirrors this SAME module's own
      `_found_temporal_start`/`_load_golden_temporal_start` pattern exactly --
      CALLS is not, and stays not, a gated type in `GATE_TYPES` below (M6's own
      scoping decision, see golden/edges.yaml's own top comment); adding one
      more mechanism-scoped record does not change that. A direct
      resolution/confidence/props pin (`_pin_calls_edge`, the symbol-dst twin of
      this module's own `_pin_edge`) additionally proves static/1.0 dispatch and
      `mechanism="singleton_dispatch"` -- P/R alone only proves the (src, dst)
      pair matched, not the tier/props riding along with it.
  (T2) who_calls x INVOKES_ACTIVITY (pilot §4.3, query/api.py): a live
      `GraphQuery.who_calls` call, over the REAL indexed realstack graph (not a
      hand-built mini-graph like Task 2's own unit/integration tests), on
      `DocActivities.fetch_document_content` (an existing M6-era TemporalActivity
      -- no new fixture data needed for this leg, only a live re-proof that the
      mechanism survives the FULL real pipeline). Direct mode surfaces
      `DocSubmissionWorkflow.run` with `mechanism="invokes_activity"`; transitive
      mode additionally surfaces `submit_document` one hop further out, over
      `run`'s own ordinary CALLS(mechanism=temporal_start) in-edge -- the exact
      worked example Task 2's own docstring describes, now proven live.
  (T3) search_code chunk-granularity (pilot §4.1, query/retrieval.py): a live,
      TEXT-MODE `GraphQuery.search_code` call (no embedder needed or wired --
      `mode="text"` is always available off `store.search_text_chunks` alone,
      see query/retrieval.py's own module docstring) against the SAME graph,
      after this gate's own S8 `chunk_embed.run(cfg, staging, embedder=None)`
      pass (the main pipeline helper below only runs S1-S7; chunking is a
      genuinely separate stage, see pipeline/chunk_embed.py's own module
      docstring -- `embedder=None` still builds real Chunk nodes/headers, just
      skips embedding vectors, exactly `codegraph index --no-embed`'s own CLI
      convention). The probe text (`"admin-ping-probe"`, admin_ping's own new
      T1 call-site literal) is unique across the whole corpus by construction,
      so the top hit is deterministic without depending on this fixture's
      (small, unsplit) chunk-size thresholds -- proves `enclosing_symbol`/
      `chunk_kind` survive the FULL load_graph round trip onto a REAL chunk.

  (T4) per-edge external (TRACKED M9, closed by M10 Task 4): DELIBERATELY NOT
      re-pinned here. `tests/eval/test_m9_gate.py` already pins this exhaustively
      (staging props, live FalkorDB round-trip via `trace_process`, the
      trace-confidence-exclusion differential proof, CLI rendering) against THIS
      SAME `fixtures/realstack` tree -- since neither this gate's own new legs
      nor any other M10 task touched `linking/http_routes.py`/`query/traverse.py`
      again after Task 4, re-deriving even a subset of that proof here would be
      pure duplication with zero additional coverage (both gates read the
      identical fixture + golden file). `test_m9_gate.py` is unconditionally
      part of "every M1-M10 gate", so it keeps re-proving T4 on every full gate
      run, including this milestone's own final one -- reuse, not silence.

Deliberately NOT ported from test_m9_gate.py: the ~15 individual M6/M7/M8/M9-era
`_pin_edge` calls (SETTINGS_PRODUCER, ENUM_PRODUCER, SIGNAL_CHANNEL/SENDER/
HANDLER, HTTP_PINS, the funnel negative, multi-mount compose-back, ...) -- the
GATE_TYPES precision/recall loop + the temporal_start CALLS block immediately
below ARE ported (generic, parametrized over `edge_type`/mechanism, a handful of
lines each) because they cheaply re-verify AGGREGATE correctness across the
WHOLE golden file including every prior-era addition; the granular per-mechanism
resolution/confidence pins are a separate, much larger layer that
`test_m9_gate.py` (still run on every full gate battery, same fixture, same
golden) already owns byte-for-byte -- copy-pasting them here would drift the
instant either file's pin values needed a future correction. Trace/CLI-rendering
machinery (`_trace_diff`, `_typed_signal_hop_diff`, `_external_exit_hop_diff`,
the `codegraph trace` CliRunner check) is likewise skipped outright: none of
T1-T3 touch tracing, and T1's own call site (admin_ping) is DELIBERATELY
isolated from every trace segment (see app/routes/admin.py's own M10 T1
docstring) -- there is no trace-shaped claim for this gate to make. No separate
`--incremental` sub-test either (unlike M9's own T2-review-mandated binding
carry, see that gate's own docstring) -- no M10 Task 1-4 review created an
equivalent obligation scoped to realstack specifically (progress.md's M10-T1..T4
ledger entries), and Task 1's own unit suite already exhaustively covers the
`module_singletons` incremental-escalation digest (`stale_escalation=
"module_singletons_changed"`) at the mechanism level.

`degraded` is asserted `== []` EXPLICITLY (same rationale as M6-M9: a degraded
run would silently weaken the T1 dispatch tier -- realstack indexes with
first-party-only scip resolution on both services, no venv needed). Gate is NOT
weakened on failure and golden is NOT edited to make it pass -- extractors/
linking get fixed instead (this milestone's brief, verbatim rule, ported from
M6-M9)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.evalx.calls_eval import precision_recall
from codegraph.evalx.edges_eval import found_edges, load_golden_edges
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.load import load_graph
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = [pytest.mark.scip, pytest.mark.falkordb]

FIXTURES = Path(__file__).parents[2] / "fixtures" / "realstack"
GOLDEN_EDGES = FIXTURES / "golden" / "edges.yaml"

GRAPH_NAME = "__m10_gate__"

# Unchanged from M8/M9 -- the generic per-type precision/recall loop; CALLS stays
# out of scope by design (see module docstring + golden/edges.yaml's own top
# comment).
GATE_TYPES = ("INVOKES_ACTIVITY", "CALLS_HTTP", "CONSUMES", "PRODUCES", "HANDLES")

# -- M10 T1 (task-5) pin targets: module-level singleton dispatch ---------------
SINGLETON_CALLER = ("worker", "app.routes.admin.admin_ping")
SINGLETON_METHOD = ("worker", "app.services.doc_store.DocStore.persist")

# -- M10 T2 (task-5) pin targets: who_calls x INVOKES_ACTIVITY, live ------------
# Pre-existing M6-era fixture data (gap 2, golden/edges.yaml) -- no new fixture
# needed for this leg, only a live re-proof over the REAL indexed graph.
ACTIVITY_TARGET = ("gateway", "app.activities.docs.DocActivities.fetch_document_content")
ACTIVITY_CALLER = ("gateway", "app.workflows.submission.DocSubmissionWorkflow.run")
TRANSITIVE_STARTER = ("gateway", "app.routes.submit.submit_document")

# -- M10 T3 (task-5) pin target: search_code chunk-granularity, live -----------
# admin_ping's own new T1 call-site literal (app/routes/admin.py) -- unique
# across the whole corpus by construction, so the top text-mode hit is
# deterministic regardless of this fixture's (small, unsplit) chunk thresholds.
SEARCH_PROBE_TEXT = "admin-ping-probe"
SEARCH_TARGET = SINGLETON_CALLER
SEARCH_TARGET_QUALIFIED = "app.routes.admin.admin_ping"
SEARCH_TARGET_CHUNK_KIND = "Function"


def _run_pipeline(
    cfg: WorkspaceConfig, staging: Staging, cache_dir: Path,
) -> tuple[list[str], dict]:
    """Ported from test_m9_gate.py (itself ported from M6/M7/M8) verbatim."""
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
    """Ported from test_m9_gate.py verbatim -- golden CALLS records with
    mechanism: temporal_start (edges_eval deliberately excludes them, see its
    own docstring)."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "temporal_start":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_temporal_start(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Ported from test_m9_gate.py verbatim -- staged mechanism="temporal_start"
    CALLS."""
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


def _load_golden_singleton_dispatch(path: Path) -> set[tuple[str, str, str, str]]:
    """Mirrors test_m9_gate.py's own `_load_golden_temporal_start` exactly --
    golden CALLS records with mechanism: singleton_dispatch (edges_eval
    deliberately excludes ALL mechanism-tagged CALLS from the generic
    GATE_TYPES loop, see its own docstring)."""
    data = yaml.safe_load(path.read_text()) or {}
    out: set[tuple[str, str, str, str]] = set()
    for e in data.get("edges", []):
        if e.get("type") == "CALLS" and e.get("mechanism") == "singleton_dispatch":
            src, dst = e["src"], e["dst"]
            out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def _found_singleton_dispatch(staging: Staging) -> tuple[set[tuple[str, str, str, str]], int]:
    """Mirrors test_m9_gate.py's own `_found_temporal_start` exactly -- staged
    mechanism="singleton_dispatch" CALLS."""
    node_lookup = {
        n.id: (n.service, n.qualified_name) for n in staging.iter_nodes() if n.qualified_name
    }
    out: set[tuple[str, str, str, str]] = set()
    dangling = 0
    for e in staging.iter_edges():
        if e.type != "CALLS" or e.props.get("mechanism") != "singleton_dispatch":
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


def _pin_calls_edge(
    problems: list[str], staging: Staging, label: str,
    src: tuple[str, str], dst: tuple[str, str],
    resolution: str, confidence: float, props_subset: dict | None = None,
) -> None:
    """EXACTLY ONE staged CALLS edge (src symbol -> dst symbol), with the given
    resolution/confidence and (optionally) a props SUBSET. The symbol-dst twin
    of test_m9_gate.py's own `_pin_edge` (channel-dst) -- both ends resolved via
    `_find_node_id` here, instead of one end being a bare channel-id string."""
    src_id = _find_node_id(staging, *src)
    dst_id = _find_node_id(staging, *dst)
    edges = _edges_between(staging, "CALLS", src_id, dst_id)
    if len(edges) != 1:
        problems.append(
            f"{label}: expected exactly ONE CALLS {src_id!r} -> {dst_id!r}, "
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


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_m10_gate(tmp_path, falkordb_cfg):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    cache_dir = tmp_path / "scip-cache"

    problems: list[str] = []
    staging = Staging(tmp_path / "staging.db")
    store = FalkorStore(falkordb_cfg, GRAPH_NAME)
    build_store = FalkorStore(falkordb_cfg, f"{GRAPH_NAME}__build")
    try:
        degraded, link_stats = _run_pipeline(cfg, staging, cache_dir)
        assert degraded == [], (
            f"realstack must index WITHOUT degrading (first-party-only scip "
            f"resolution suffices for both services) -- degraded: {degraded}"
        )
        print(f"\n[M10 gate][link_stats] {link_stats}")

        # -- per-type precision/recall over golden (ported, unchanged legs) -------
        for edge_type in GATE_TYPES:
            golden = load_golden_edges(GOLDEN_EDGES, {edge_type})
            found, dangling = found_edges(staging, {edge_type})
            pr = precision_recall(found, golden)
            print(
                f"\n[M10 gate][{edge_type}] precision={pr['precision']:.4f} "
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
            f"\n[M10 gate][temporal_start CALLS] precision={pr_ts['precision']:.4f} "
            f"recall={pr_ts['recall']:.4f} tp={pr_ts['tp']} found={len(found_ts)} "
            f"golden={len(golden_ts)} dangling={dangling_ts}"
        )
        if pr_ts["precision"] != 1.0 or pr_ts["recall"] != 1.0:
            problems.append(
                f"temporal_start CALLS: precision={pr_ts['precision']:.4f} "
                f"recall={pr_ts['recall']:.4f} (want 1.0/1.0); dangling={dangling_ts}"
            )

        # -- M10 T1 (task-5): singleton dispatch precision/recall -----------------
        golden_sd = _load_golden_singleton_dispatch(GOLDEN_EDGES)
        found_sd, dangling_sd = _found_singleton_dispatch(staging)
        pr_sd = precision_recall(found_sd, golden_sd)
        print(
            f"\n[M10 gate][singleton_dispatch CALLS] precision={pr_sd['precision']:.4f} "
            f"recall={pr_sd['recall']:.4f} tp={pr_sd['tp']} found={len(found_sd)} "
            f"golden={len(golden_sd)} dangling={dangling_sd}\n"
            f"  fp ({len(pr_sd['fp_list'])}): {pr_sd['fp_list']}\n"
            f"  fn ({len(pr_sd['fn_list'])}): {pr_sd['fn_list']}"
        )
        if pr_sd["precision"] != 1.0 or pr_sd["recall"] != 1.0:
            problems.append(
                f"singleton_dispatch CALLS: precision={pr_sd['precision']:.4f} "
                f"recall={pr_sd['recall']:.4f} (want 1.0/1.0); dangling={dangling_sd}\n"
                f"  fp ({len(pr_sd['fp_list'])}): {pr_sd['fp_list']}\n"
                f"  fn ({len(pr_sd['fn_list'])}): {pr_sd['fn_list']}"
            )
        # P/R alone only proves the (src, dst) pair matched -- resolution/
        # confidence/mechanism need their own direct pin (mirrors every other
        # mechanism-tagged pin in test_m9_gate.py).
        _pin_calls_edge(
            problems, staging, "module-level singleton dispatch CALLS",
            SINGLETON_CALLER, SINGLETON_METHOD,
            resolution="static", confidence=1.0,
            props_subset={"mechanism": "singleton_dispatch"},
        )

        # -- capture node ids needed post-close (staging.iter_nodes() only works
        # before close(); the live GraphQuery checks below use these id STRINGS
        # against FalkorDB instead, mirrors test_m9_gate.py's own
        # admin_handler_id capture) -------------------------------------------
        activity_id = _find_node_id(staging, *ACTIVITY_TARGET)
        activity_caller_id = _find_node_id(staging, *ACTIVITY_CALLER)
        transitive_starter_id = _find_node_id(staging, *TRANSITIVE_STARTER)
        search_target_id = _find_node_id(staging, *SEARCH_TARGET)

        # -- S8 (chunk_embed, text-only -- no embedder wired or needed for T3's
        # own mode="text" search below; mirrors `codegraph index --no-embed`) --
        chunk_stats = run_chunk_embed(cfg, staging, None)
        print(f"\n[M10 gate][chunk_stats] {chunk_stats}")

        # -- S9: load into FalkorDB (blue/green; zero-drop pin mirrors M6-M9) -----
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

        gq = GraphQuery(
            store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
            service_paths={svc.name: svc.path for svc in cfg.services},
        )

        # -- M10 T2 (task-5): who_calls x INVOKES_ACTIVITY, live ------------------
        direct = gq.who_calls(activity_id)
        if "error" in direct:
            problems.append(f"who_calls(direct) error: {direct['error']}")
        else:
            callers_by_id = {c.get("id"): c for c in direct["callers"]}
            if activity_caller_id not in callers_by_id:
                problems.append(
                    f"who_calls(direct) on {ACTIVITY_TARGET!r} (a TemporalActivity) "
                    f"must surface its INVOKES_ACTIVITY source {ACTIVITY_CALLER!r} -- "
                    f"callers found: {sorted(c for c in callers_by_id if c)}"
                )
            elif callers_by_id[activity_caller_id].get("mechanism") != "invokes_activity":
                problems.append(
                    f"who_calls(direct): caller {activity_caller_id!r} must carry "
                    f"mechanism='invokes_activity', got "
                    f"{callers_by_id[activity_caller_id].get('mechanism')!r}"
                )

        transitive = gq.who_calls(activity_id, transitive=True, max_depth=2)
        if "error" in transitive:
            problems.append(f"who_calls(transitive) error: {transitive['error']}")
        else:
            t_callers_by_id = {c.get("id"): c for c in transitive["callers"]}
            missing = {activity_caller_id, transitive_starter_id} - t_callers_by_id.keys()
            if missing:
                problems.append(
                    f"who_calls(transitive, max_depth=2) on {ACTIVITY_TARGET!r} must "
                    f"surface BOTH the direct INVOKES_ACTIVITY source "
                    f"{ACTIVITY_CALLER!r} AND the transitive CALLS "
                    f"(mechanism=temporal_start) starter {TRANSITIVE_STARTER!r} -- "
                    f"missing node ids: {missing}"
                )
            elif t_callers_by_id[transitive_starter_id].get("mechanism") is not None:
                problems.append(
                    f"who_calls(transitive): {transitive_starter_id!r} was reached "
                    f"over an ordinary CALLS hop (not INVOKES_ACTIVITY) -- must carry "
                    f"NO mechanism key, got "
                    f"{t_callers_by_id[transitive_starter_id].get('mechanism')!r}"
                )

        # -- M10 T3 (task-5): search_code chunk-granularity, live, text-mode ------
        search_result = gq.search_code(SEARCH_PROBE_TEXT, mode="text", k=5)
        if "error" in search_result:
            problems.append(f"search_code error: {search_result['error']}")
        else:
            if search_result.get("mode_used") != "text":
                problems.append(
                    f"search_code: expected mode_used='text', got "
                    f"{search_result.get('mode_used')!r}"
                )
            items = search_result.get("items", [])
            if not items:
                problems.append(
                    f"search_code({SEARCH_PROBE_TEXT!r}, mode='text'): no items found "
                    f"(probe text is unique to admin_ping's own body by construction)"
                )
            else:
                top = items[0]
                if top.get("symbol_id") != search_target_id:
                    problems.append(
                        f"search_code top hit: expected symbol_id={search_target_id!r} "
                        f"({SEARCH_TARGET!r}), got {top.get('symbol_id')!r} (item: {top})"
                    )
                if top.get("enclosing_symbol") != SEARCH_TARGET_QUALIFIED:
                    problems.append(
                        f"search_code top hit: expected enclosing_symbol="
                        f"{SEARCH_TARGET_QUALIFIED!r}, got "
                        f"{top.get('enclosing_symbol')!r} (item: {top})"
                    )
                if top.get("chunk_kind") != SEARCH_TARGET_CHUNK_KIND:
                    problems.append(
                        f"search_code top hit: expected chunk_kind="
                        f"{SEARCH_TARGET_CHUNK_KIND!r}, got {top.get('chunk_kind')!r} "
                        f"(item: {top})"
                    )
    finally:
        staging.close()
        store.delete_graph()
        build_store.delete_graph()

    assert not problems, "\n\n".join(problems)
