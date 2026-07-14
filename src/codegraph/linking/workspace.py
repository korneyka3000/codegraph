"""M2 T7: link_workspace -- the S7 orchestrator, run once per `codegraph index`, AFTER
every service's own analyze_service (S1-S6) has completed and BEFORE load_graph (S9).
Fixed pipeline order, each stage consuming the previous stage's output:

  1. `staging.clear_workspace_layer()` -- wipes the PREVIOUS run's own derived layer
     (BusinessProcess nodes + extractor="linking" edges) so link_workspace is safely
     re-runnable without accumulating stale derivations. Must run FIRST: every later
     stage in this function creates or updates extractor="linking" state, and if clear
     ran anywhere else it would either do nothing (nothing derived yet) or destroy this
     run's own freshly-derived edges.
  2. temporal_start_mark claims -> CALLS edges (see `_apply_temporal_start_marks`).
  3. `http_routes.link` -- claims -> CALLS_HTTP.
  4. `segments.derive` -- PRODUCES/CONSUMES/CALLS_HTTP/HANDLES/CONTAINS -> NEXT_SEGMENT.
     Must run AFTER http_routes.link: one of its two pairing rules consumes CALLS_HTTP
     edges that only exist once http_routes.link has written them.
  5. `processes.materialize` -- cfg.processes + TemporalWorkflow roles -> BusinessProcess
     + PART_OF_PROCESS. Must run LAST: its BFS trace walks NEXT_SEGMENT edges that only
     exist once segments.derive has written them.

No separate `linking/channels.py`: the master plan's own "унификация Channel-узлов"
step turns out to need no code of its own. Channel node ids are fully deterministic
(`ids.chan_kafka`/`chan_event`/`chan_http` -- built from (kind, name) or (owner, method,
template) alone, never from anything per-service-instance-specific), and
`Staging.upsert_nodes` is INSERT OR REPLACE keyed on id -- so two services' extractors
(or two separate analyze_service runs) independently emitting "the same" channel just
replace the same row with equivalent content. "Unification" is therefore already a
no-op by construction, not a step that needs to scan for and merge duplicate Channel
nodes; documenting that here (per the task's own instruction) in place of an empty
module. http_routes.link separately reads the resulting unified Channel(http_route)
table (see its own module docstring) -- that IS real linking work, just not "channel
unification" in the sense of deduplicating near-identical nodes.

temporal_start_mark: see `_apply_temporal_start_marks` for the create-vs-update design
and the resolution="dynamic"/confidence=0.9 decision.
"""

from __future__ import annotations

from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import EdgeRec
from codegraph.stores.staging import Staging

from . import http_routes, processes, segments

_EXTRACTOR = "linking"
_MARK_TYPE = "CALLS"
_MARK_RESOLUTION = "dynamic"
_MARK_CONFIDENCE = 0.9
_MARK_PROPS = {"mechanism": "temporal_start"}


def _apply_temporal_start_marks(staging: Staging) -> int:
    """temporal_ext (T5) resolves `*.start_workflow(WorkflowCls.run, ...)` calls into a
    `temporal_start_mark` claim {src_id, dst_id, evidence_line} at EXTRACTION time (both
    ids already resolved -- see temporal_ext.py's own module docstring for why deferring
    that resolution to S7 would be strictly worse). S6's build_calls can never produce
    this edge itself: it joins purely on each call's OWN callee span
    ("start_workflow"), which resolves to the temporalio SDK (or nothing, degraded),
    never to the workflow class passed as arg0 -- so THIS is the only place the edge is
    ever created (live-proven in T5: "S6 это ребро НЕ эмитит").

    Design decision (brief's own "реши и задокументируй"): resolution="dynamic" (a
    dedicated RESOLUTIONS value, unused until now) -- a Temporal start is not a direct
    Python call (not "static"), but it is not a low-confidence structural GUESS either
    (both ends were resolved through a real symbol lookup at extraction time, same
    evidentiary strength as any other STATIC-tier match); "dynamic" names that distinct
    character honestly instead of overloading "heuristic". confidence=0.9: deliberately
    just BELOW the 1.0 reserved for a literal, unambiguous same-process call (the
    Temporal SDK -- not this process -- is what actually performs the invocation, at a
    time/place this graph cannot observe), but well above every "heuristic" tier used
    elsewhere (0.5-0.8) -- both ends of a temporal_start_mark claim are only ever
    produced from a fully resolved symbol lookup (see temporal_ext.py), never a
    partial/fuzzy match, so a heuristic-range confidence would under-state it.

    Create-vs-update: if a CALLS(src_id, dst_id) edge already exists (e.g. a
    hypothetical future static-analysis path that independently proves the same call),
    `update_edge_props` merges the mechanism tag onto it WITHOUT touching its own
    resolution/confidence/extractor -- tagging, not overriding, an already-stronger
    signal. Otherwise a fresh edge is created here, extractor="linking" (so
    clear_workspace_layer's next run can safely retire it if the claim disappears).

    Returns the number of claims processed (both create and update paths).
    """
    claims = staging.claims_for("temporal_start_mark")
    new_edges: list[EdgeRec] = []
    for claim in claims:
        src_id, dst_id = claim["src_id"], claim["dst_id"]
        updated = staging.update_edge_props(src_id, dst_id, _MARK_TYPE, dict(_MARK_PROPS))
        if not updated:
            new_edges.append(EdgeRec(
                src=src_id, dst=dst_id, type=_MARK_TYPE,
                resolution=_MARK_RESOLUTION, confidence=_MARK_CONFIDENCE, extractor=_EXTRACTOR,
                evidence_file=claim.get("_relpath"), evidence_line=claim.get("evidence_line"),
                props=dict(_MARK_PROPS),
            ))
    if new_edges:
        staging.upsert_edges(new_edges)
    return len(claims)


def link_workspace(cfg: WorkspaceConfig, staging: Staging) -> dict:
    """S7 entry point. staging-only (no FalkorDB access) -- callers don't need a
    store-unavailability guard around this call. Returns a JSON-serializable counters
    dict: {calls_http, calls_http_unresolved, next_segments, processes, marks}."""
    staging.clear_workspace_layer()
    marks = _apply_temporal_start_marks(staging)
    http_stats = http_routes.link(cfg, staging)
    segment_stats = segments.derive(staging)
    process_stats = processes.materialize(cfg, staging)
    return {
        "calls_http": http_stats["calls_http"],
        "calls_http_unresolved": http_stats["calls_http_unresolved"],
        "next_segments": segment_stats["next_segments"],
        "processes": process_stats["processes"],
        "marks": marks,
    }
