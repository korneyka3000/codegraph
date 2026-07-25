"""M2 T7: link_workspace -- the S7 orchestrator, run once per `codegraph index`, AFTER
every service's own analyze_service (S1-S6) has completed and BEFORE load_graph (S9).
Fixed pipeline order, each stage consuming the previous stage's output:

  1. `staging.clear_workspace_layer()` -- wipes the PREVIOUS run's own derived layer
     (BusinessProcess nodes + extractor="linking" edges) so link_workspace is safely
     re-runnable without accumulating stale derivations. Must run FIRST: every later
     stage in this function creates or updates extractor="linking" state, and if clear
     ran anywhere else it would either do nothing (nothing derived yet) or destroy this
     run's own freshly-derived edges.
  2. `staging.gc_orphan_channels()` -- M2 final review fix, must run SECOND (right after
     clear_workspace_layer, before ANY derivation stage below -- see its own docstring
     for why this exact position, not "at the end", is load-bearing): sweeps any Channel
     node left with zero referencing edges. By this point every REAL Channel already has
     its S5-native edge (HANDLES/PRODUCES/CONSUMES/CONTAINS -- created in the SAME
     analyze_service batch as the Channel itself, so it survived step 1 untouched,
     extractor != "linking"); the only Channels that can be edge-LESS here are (a) a
     stale one left behind by a route/topic/event rename, whose OLD S5 edge was just
     correctly retired by THIS run's origin_service-scoped begin_service (see
     Staging.begin_service's docstring) but whose NODE has no per-service home to be
     swept by begin_service at all, or (b) a "unresolved fallback" Channel from a PRIOR
     run whose only edge (CALLS_HTTP, extractor="linking") step 1 just wiped. Running
     GC here, BEFORE http_routes.link (step 4) rebuilds its route table by scanning ALL
     staged Channel(http_route) nodes, is what actually closes the M2-final bug: leaving
     a stale Channel visible to that scan for even one more run would let an unrelated,
     unchanged client claim silently re-match it, recreating a fresh CALLS_HTTP edge
     into a route that no longer exists in source and keeping the stale Channel "alive"
     forever (empirically caught by this fix's own double-run regression test).
  3. temporal_start_mark claims -> CALLS edges (see `_apply_temporal_start_marks`).
  4. `linking.signal_send.link` -- M8 T2 (rerun-2 R5): temporal_signal_send claims
     (temporal_ext.py's own typed-sender arg0 resolution, `handle.signal(Cls.method,
     ...)` / a bare-name imported method) -> PRODUCES into the SAME temporal_signal
     Channel a handler's own CONSUMES edge (built directly by temporal_ext in S5,
     unaffected by this step) already targets (see linking/signal_send.py's own
     module docstring for the full algorithm and honesty rule). No POSITIONAL
     dependency on router_prefix/http_routes below -- a disjoint Channel kind,
     placed here purely for prose locality with the OTHER temporal-claim consumer
     (step 3). Must run BEFORE segments.derive (step 7): that stage's own
     exact-channel pairing rule needs this step's freshly-linked PRODUCES edges to
     exist.
  5. `router_prefix.link` -- route_decl/router_include/router_decl claims (M8 T1,
     rerun-2 R4; router_decl -- each router's own declared prefix, folded in at
     every hop -- is the M8 review Important-1 addition) -> Channel(http_route) +
     HANDLES, composed across cross-file `include_router` chains (see
     linking/router_prefix.py's own module docstring for the full algorithm and
     honesty rule). Must run BEFORE http_routes.link (step 6): that stage's own
     `_route_table` scan reads whatever Channel(http_route) nodes are ALREADY
     staged, and this step is what stages them now -- fastapi_ext.py (S5) no longer
     creates them directly.
  6. `http_routes.link` -- claims -> CALLS_HTTP.
  7. `segments.derive` -- PRODUCES/CONSUMES/CALLS_HTTP/HANDLES/CONTAINS -> NEXT_SEGMENT.
     Must run AFTER http_routes.link (step 6) and linking.signal_send.link (step 4):
     its pairing rules consume CALLS_HTTP/PRODUCES edges that only exist once those
     steps have written them.
  8. `processes.materialize` -- cfg.processes + TemporalWorkflow roles -> BusinessProcess
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

M8 T1 (rerun-2 R4): http_route Channels themselves are no longer built by fastapi_ext.py
in S5 at all -- `router_prefix.link` (step 4 above) builds them here in S7 instead, from
route_decl/router_include claims, since a route's full cross-file `include_router`-chain
identity can't be known from a single file. kafka_topic/event_type/temporal_signal
Channels are UNAFFECTED (still built directly by kafka_ext/temporal_ext in S5) -- this
determinism argument continues to hold for them unchanged.

temporal_start_mark: see `_apply_temporal_start_marks` for the create-vs-update design
and the resolution="dynamic"/confidence=0.9 decision.
"""

from __future__ import annotations

from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import EdgeRec
from codegraph.stores.staging import Staging

from . import http_routes, processes, router_prefix, segments, signal_send

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
    dict: {calls_http, calls_http_unresolved, next_segments, processes, marks,
    channels_gc, part_of_process_ambiguous, route_prefix_unresolved,
    signal_send_unlinked}. The part_of_process_ambiguous key (M3 T2) is
    processes.materialize's own `_entry_of`-climb ambiguity count, passed through
    unchanged -- see linking/processes.py's module docstring for what it means.
    route_prefix_unresolved (M8 T1, rerun-2 R4) is router_prefix.link's own
    honest-miss counter -- see linking/router_prefix.py's own module docstring for
    the three failure shapes it counts. signal_send_unlinked (M8 T2, rerun-2 R5) is
    linking.signal_send.link's own honest-miss counter -- a temporal_signal_send
    claim whose method_symbol resolved at extraction time but names no method with a
    live CONSUMES edge into a temporal_signal channel at link time -- see
    linking/signal_send.py's own module docstring."""
    staging.clear_workspace_layer()
    channels_gc = staging.gc_orphan_channels()
    marks = _apply_temporal_start_marks(staging)
    signal_send_stats = signal_send.link(staging)
    router_prefix_stats = router_prefix.link(staging)
    http_stats = http_routes.link(cfg, staging)
    segment_stats = segments.derive(staging)
    process_stats = processes.materialize(cfg, staging)
    return {
        "calls_http": http_stats["calls_http"],
        "calls_http_unresolved": http_stats["calls_http_unresolved"],
        "next_segments": segment_stats["next_segments"],
        "processes": process_stats["processes"],
        "marks": marks,
        "channels_gc": channels_gc,
        "part_of_process_ambiguous": process_stats["part_of_process_ambiguous"],
        "route_prefix_unresolved": router_prefix_stats["route_prefix_unresolved"],
        "signal_send_unlinked": signal_send_stats["signal_send_unlinked"],
    }
