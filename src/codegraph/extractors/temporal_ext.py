"""temporal_ext: Temporal workflow/activity structural extractor (M2 T5).

Like fastapi_ext, this is a STRUCTURAL extractor -- its patterns (`@workflow.defn`,
`@activity.defn`, `workflow.execute_activity(...)`, `*.start_workflow(...)`) are
hardcoded, not data-driven through the idiom DSL (ServiceIdioms), because Temporal's
own decorator/call vocabulary IS the idiom: a service either uses the temporalio SDK
this way or it doesn't. `builtin_idioms.py`'s "temporal" entry is (deliberately) an
empty ServiceIdioms; activation is purely "temporal" ∈ active_idioms (see analyze.py).

  - `@workflow.defn` / `@activity.defn`: `idiom_match.match_decorators` (both are bare,
    non-call decorators -- no CallFact needed, unlike fastapi's `router.get("/x")`)
    grants TemporalWorkflow/TemporalActivity roles; workflow.defn additionally patches
    `workflow_name` onto the class's own node_props (the class's DISPLAY name, not a
    qualified/resolved value -- matches the brief's "имя класса" wording exactly).
  - `workflow.execute_activity(fn_ref, ...)`: matched structurally (callee_name IN
    _ACTIVITY_INVOKE_CALLEES AND receiver_text == "workflow" -- confirmed against the
    real kyc_worker/app/workflows/kyc.py fixture, see this module's test file; NOT a
    glob on receiver, unlike start_workflow below). arg0's own name/attr byte span
    (already exactly ArgFact.name_start_byte/end_byte -- no span math, ArgFact already
    carries "attr" as the LAST segment per T2's contract) resolves via
    ctx.ref_symbol_lookup to the activity's node id. src is the enclosing DEF id as-is
    (the method, e.g. `KycWorkflow.run` -- the brief's own initial "raise to the
    workflow class" idea is explicitly overridden by checking the golden fixture:
    golden src IS the method).
  - `*.start_workflow(fn_ref, ...)`: glob on receiver (ANY receiver -- Temporal client
    handles are typically obtained dynamically, e.g. `client = await Client.connect(...)`,
    so there's no fixed name to check the way execute_activity has "workflow"; see
    test_start_workflow_matches_any_receiver_not_just_client). callee_name IN
    _START_WORKFLOW_CALLEES (M6 T1, below). Produces a "temporal_start_mark" CLAIM,
    not a direct edge -- see design-decision note below.

M6 T1 (pilot report docs/superpowers/reports/2026-07-23-pilot-real-services-gaps.md
§3/§4): a real-stack pilot on camunda-gateway found BOTH matchers above were strict
`==` comparisons against a single literal name, so `INVOKES_ACTIVITY` came out at
**zero** edges despite 43 activities and 80 real call sites -- the code there calls
`workflow.execute_activity_method(SomeActivity.some_method, ...)` (a bound-method
ref), never bare `execute_activity`; and `start_workflow`'s claim only resolved 4/7
real child-workflow starts, missing `start_child_workflow`/`execute_child_workflow`.
Fix is deliberately minimal (GAPS §3: "резолвится одинаково" -- arg0 resolution is
byte-identical regardless of which name matched): each `==` became an `in` against a
frozenset of Temporal's own SDK vocabulary for that operation --
`_ACTIVITY_INVOKE_CALLEES` (receiver check unchanged, still exactly "workflow") and
`_START_WORKFLOW_CALLEES` (receiver check unchanged too -- still no check at all, any
receiver). No other line in either matcher function changed.

Design decision on the temporal_start_mark claim (brief's explicit "реши и
задокументируй"): `dst_id` is resolved NOW, at extraction time, via
ctx.ref_symbol_lookup on arg0's own last-segment span (`KycWorkflow.run` -> the "run"
token's span -- ArgFact.name_start_byte already IS that span for both "attr" and "name"
value_kinds per T2's contract, so no extra span arithmetic is needed here either) --
NOT a deferred (relpath, dst_start_byte) pair for S7 to re-resolve later. Why this is
"чище" (cleaner), as the brief invites: S7 (linking) runs workspace-wide AFTER every
service's own analyze_service call, by which point per-service SCIP ref-lookup closures
are gone; resolving here, while ctx.ref_symbol_lookup is still in scope, means S7's own
job (per the brief: "S7 просто update_edge_props") stays a pure "does a (src_id, dst_id,
CALLS) edge already exist -> tag it {mechanism: temporal_start, ...}" lookup with no
per-service SCIP access of its own to wire up. Concretely, S7 needs BOTH ids to find the
CALLS edge in the first place (`update_edge_props(src, dst, "CALLS", ...)` takes the
full (src, dst, type) key) -- carrying a raw span instead of dst_id would just move the
identical ref-lookup into S7 for no benefit, at the cost of S7 needing per-service
Staging.ref_symbol_at access it doesn't otherwise need. If the ref can't be resolved (no
ref_symbol_lookup wired at all, or the lookup misses -- e.g. degraded/heuristic fallback,
which never lays refs inside call arguments any more than it does for fastapi's
DEPENDS_ON), no claim is emitted: there is nothing for S7 to mark without a dst.

Note on WHY a claim (deferred write) is needed at all, rather than temporal_ext just
emitting a CALLS edge itself: S6's build_calls (extractors/calls.py) joins purely on
each CallFact's OWN callee span -- for `client.start_workflow(KycWorkflow.run, ...)`
that span is "start_workflow" itself, which resolves to the external temporalio SDK
(or nothing, in degraded mode), never to `KycWorkflow.run` -- and `KycWorkflow.run`
passed bare (no call parens) is never itself a CallFact either. So S6 can never produce
the `handle_order_created -> KycWorkflow.run` CALLS edge the golden fixture expects
(see fixtures/golden/edges.yaml's own policy note on this exact case) -- it has to be
synthesized. Whether the eventual write is a fresh CALLS edge or a props-patch onto one
is S7's call (T7, not in scope here); this extractor's job is only to surface the fact
with everything S7 needs to act on it.

TemporalResult's field list is `(roles, node_props, edges, claims, stats)` --
deliberately not the plan doc's abbreviated top-line `TemporalResult(roles, edges,
claims)`: node_props is required (nowhere else to deliver workflow_name -- this module
only ever sees `node_ids: dict[int, str]`, never NodeRec objects, so a props-patch dict
is the only channel analyze.py's `_apply_role_props_patch` can consume), and `stats` is
added for parity with kafka_ext/fastapi_ext (not read downstream yet, same as
fastapi_ext.stats today). `channels` is NOT added: temporal never creates Channel nodes.
This mirrors T4's own documented precedent (progress.md: "FastapiResult без claims, с
node_props -- прозой плана суперсидится top-line сигнатура") of the plan's prose
description winning over its own abbreviated signature line.
"""

from __future__ import annotations

from dataclasses import dataclass

from codegraph.core.schema import EdgeRec
from codegraph.extractors.idiom_match import match_decorators
from codegraph.resolvers.scip.symbols import symbol_to_node_id

from .base import FileContext

_EXTRACTOR = "temporal"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

# M6 T1 (pilot GAPS §3): every Temporal SDK spelling of "invoke an activity from a
# workflow" -- receiver is still required to be exactly "workflow" (unchanged).
_ACTIVITY_INVOKE_CALLEES = frozenset({
    "execute_activity",
    "execute_activity_method",
    "execute_local_activity",
    "execute_local_activity_method",
    "start_activity",
    "start_local_activity",
})

# M6 T1 (pilot GAPS §4): every Temporal SDK spelling of "start a (child) workflow" --
# receiver stays unchecked (ANY receiver, unchanged -- see module docstring).
_START_WORKFLOW_CALLEES = frozenset({
    "start_workflow",
    "start_child_workflow",
    "execute_child_workflow",
})


@dataclass(frozen=True)
class TemporalResult:
    roles: dict[str, set[str]]
    node_props: dict[str, dict]
    edges: list[EdgeRec]
    claims: list[dict]
    stats: dict[str, int]


def _stats() -> dict[str, int]:
    return {
        "defn_missing_node_id": 0,
        "invokes_activity_resolved": 0,
        "invokes_activity_unresolved": 0,
        "invokes_activity_missing_node_id": 0,
        "start_workflow_resolved": 0,
        "start_workflow_unresolved": 0,
        "start_workflow_missing_node_id": 0,
    }


def _resolve_ref(ctx: FileContext, start_byte: int | None) -> str | None:
    if start_byte is None or ctx.ref_symbol_lookup is None:
        return None
    sym = ctx.ref_symbol_lookup(ctx.relpath, start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _extract_defn_roles(
    ctx: FileContext, node_ids: dict[int, str],
    roles: dict[str, set[str]], node_props: dict[str, dict], stats: dict[str, int],
) -> None:
    for d, _text in match_decorators("workflow.defn", ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["defn_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalWorkflow")
        node_props.setdefault(node_id, {})["workflow_name"] = d.name

    for d, _text in match_decorators("activity.defn", ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["defn_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalActivity")


def _extract_invokes_activity(
    ctx: FileContext, node_ids: dict[int, str], edges: list[EdgeRec], stats: dict[str, int],
) -> None:
    for call in ctx.facts.calls:
        if call.callee_name not in _ACTIVITY_INVOKE_CALLEES or call.receiver_text != "workflow":
            continue
        enclosing_id = node_ids.get(call.enclosing_def)
        if enclosing_id is None:
            stats["invokes_activity_missing_node_id"] += 1
            continue
        arg0 = next((a for a in call.args if a.index == 0), None)
        activity_id = _resolve_ref(ctx, arg0.name_start_byte) if arg0 is not None else None
        if activity_id is None:
            stats["invokes_activity_unresolved"] += 1
            continue
        edges.append(EdgeRec(
            src=enclosing_id, dst=activity_id, type="INVOKES_ACTIVITY",
            resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
            evidence_file=ctx.relpath, evidence_line=call.start_line,
            props={"by": "ref"},
        ))
        stats["invokes_activity_resolved"] += 1


def _extract_start_workflow_claims(
    ctx: FileContext, node_ids: dict[int, str], claims: list[dict], stats: dict[str, int],
) -> None:
    for call in ctx.facts.calls:
        if call.callee_name not in _START_WORKFLOW_CALLEES:
            continue
        enclosing_id = node_ids.get(call.enclosing_def)
        if enclosing_id is None:
            stats["start_workflow_missing_node_id"] += 1
            continue
        arg0 = next((a for a in call.args if a.index == 0), None)
        dst_id = _resolve_ref(ctx, arg0.name_start_byte) if arg0 is not None else None
        if dst_id is None:
            stats["start_workflow_unresolved"] += 1
            continue
        claims.append({
            "src_id": enclosing_id,
            "dst_id": dst_id,
            "evidence_line": call.start_line,
        })
        stats["start_workflow_resolved"] += 1


def extract_temporal(ctx: FileContext, node_ids: dict[int, str]) -> TemporalResult:
    roles: dict[str, set[str]] = {}
    node_props: dict[str, dict] = {}
    edges: list[EdgeRec] = []
    claims: list[dict] = []
    stats = _stats()

    _extract_defn_roles(ctx, node_ids, roles, node_props, stats)
    _extract_invokes_activity(ctx, node_ids, edges, stats)
    _extract_start_workflow_claims(ctx, node_ids, claims, stats)

    return TemporalResult(
        roles=roles, node_props=node_props, edges=edges, claims=claims, stats=stats,
    )
