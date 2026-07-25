"""M2 T5: extract_temporal (TemporalWorkflow/TemporalActivity roles, INVOKES_ACTIVITY,
temporal_start_mark claims).

Real-fixture tests exercise the exact scenarios named in the task brief: `@workflow.defn`
on kyc_worker/app/workflows/kyc.py's KycWorkflow class (+node_props workflow_name),
`@activity.defn` on kyc_worker/app/activities/documents.py's verify_documents (a
SEPARATE file -- proves extract_temporal's per-file decorator scan doesn't depend on
workflow/activity living in the same module), `workflow.execute_activity(verify_documents,
...)` inside KycWorkflow.run resolving via a stubbed ref_symbol_lookup at arg0's own
name span (mirrors fastapi_ext's DEPENDS_ON / kafka_ext's qualified_of stubbing
pattern), and `client.start_workflow(KycWorkflow.run, ...)` inside handle_order_created.

Design decision (documented per the brief's explicit "реши и задокументируй"): the
temporal_start_mark claim resolves `dst_id` NOW, at extraction time, via
ctx.ref_symbol_lookup on the arg0 last-segment span (KycWorkflow.run -> "run" token's
own byte span, already exactly ArgFact.name_start_byte/end_byte per T2's contract for
attr-kind args) -- NOT a deferred (relpath, dst_start_byte) pair for S7 to resolve
later. Rationale: S7 (linking) runs workspace-wide, AFTER all per-service analyze_service
calls, and its own stated job here is just `update_edge_props` (mark an edge that
build_calls/S6 could never have created itself, since `start_workflow`'s callee token
resolves externally to temporalio's SDK and `KycWorkflow.run` bare -- no parens -- is
never itself a CallFact) -- giving S7 a ready `dst_id` keeps it a pure "does this
(src,dst,CALLS) edge exist -> tag it" lookup with no per-service SCIP/ref-table access
of its own to wire up. If the ref can't be resolved (no ref_symbol_lookup wired, or the
lookup misses), no claim is emitted at all -- there would be nothing for S7 to mark.

TemporalResult carries node_props (not listed on the plan doc's abbreviated top-line
signature `TemporalResult(roles, edges, claims)`) because `@workflow.defn`'s
workflow_name has nowhere else to go: extract_temporal only ever sees node id STRINGS
(node_ids: dict[int, str]), never the NodeRec objects themselves, so a props-patch
dict is the only channel analyze.py's `_apply_role_props_patch` can consume -- this
mirrors T4's own documented judgement call ("FastapiResult без claims, с node_props --
прозой плана суперсидится top-line сигнатура", per progress.md). `stats` is likewise
added for parity with every other M2 domain extractor (fastapi_ext, kafka_ext); it is
not read anywhere downstream yet, same as fastapi_ext.stats today.

M7 T4 (OPEN R3, docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md): unlike
M2/M6, `channels` is NOW part of TemporalResult -- `@workflow.signal`/`@workflow.update`
handlers and their `.signal(...)` senders both create `Channel(kind="temporal_signal")`
nodes (the M2-era claim "channels is deliberately NOT added -- temporal never creates
Channel nodes", extractors/temporal_ext.py's own old docstring, no longer holds and has
been updated there). `_load` below now also returns a `ConstTable` (built the same way
kafka_ext/http_client_ext's own tests build one) -- the sender-side arg0 resolution
needs it (brief: "Consumes: consts (arg0-литерал имени)"), and `extract_temporal` grew
a third required positional parameter for it, mirroring `extract_kafka`'s own
`(ctx, node_ids, idioms, consts)` shape. See the new tests below the M6 T1 section for
the full signal/update/query scenario matrix, and extractors/temporal_ext.py's own
module docstring for the complete design rationale (name resolution, the three-way
sender arg0 classification, the accepted FP-risk of a receiver-agnostic `.signal(...)`
match, and why no ref_symbol_lookup is needed on either side here unlike
INVOKES_ACTIVITY/start_workflow).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from codegraph.extractors.base import FileContext
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.extractors.temporal_ext import TemporalResult, extract_temporal
from codegraph.parsing.consts import ConstTable
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"


def _fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


def _load(relpath: str, service: str, source: bytes, *, ref_symbol_lookup=None):
    facts = build_file_facts(relpath, source)
    core_ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    core_res = extract_python_core(core_ctx)
    node_ids = {
        d.index: n.id
        for d, n in zip(facts.defs, core_res.nodes[1:], strict=True)
    }
    node_ids[None] = core_res.nodes[0].id
    ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
        ref_symbol_lookup=ref_symbol_lookup,
    )
    # M7 T4: ConstTable.build ignores `facts` (see its own docstring) and only reads
    # `source` -- same construction every consts-consuming extractor's own tests use.
    return ctx, node_ids, ConstTable.build(facts, source)


def _workflow_ctx(**kw):
    relpath = "app/workflows/kyc.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"), **kw)


def _activity_ctx(**kw):
    relpath = "app/activities/documents.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"), **kw)


def _consumer_orders_ctx(**kw):
    relpath = "app/consumers/orders.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"), **kw)


def _def(ctx: FileContext, name: str):
    return next(d for d in ctx.facts.defs if d.name == name)


# -- TemporalResult: contract shape --


def test_temporal_result_field_shape():
    r = TemporalResult(
        roles={}, node_props={}, channels=[], edges=[], claims=[], signal_send_claims=[],
        stats={},
    )
    assert r.roles == {}
    assert r.node_props == {}
    assert r.channels == []
    assert r.edges == []
    assert r.claims == []
    assert r.signal_send_claims == []
    assert r.stats == {}


def test_no_decorators_or_temporal_calls_is_a_noop():
    ctx, node_ids, consts = _load("m.py", "svc", b"def plain():\n    pass\n")
    result = extract_temporal(ctx, node_ids, consts)
    assert result == TemporalResult(
        roles={}, node_props={}, channels=[], edges=[], claims=[], signal_send_claims=[],
        stats=result.stats,
    )


# -- @workflow.defn / @activity.defn roles --


def test_workflow_defn_role_and_workflow_name_prop():
    ctx, node_ids, consts = _workflow_ctx()
    result = extract_temporal(ctx, node_ids, consts)
    class_id = node_ids[_def(ctx, "KycWorkflow").index]

    assert result.roles[class_id] == {"TemporalWorkflow"}
    assert result.node_props[class_id] == {"workflow_name": "KycWorkflow"}


def test_activity_defn_role_on_separate_file():
    """verify_documents lives in a DIFFERENT file than KycWorkflow -- proves the
    decorator scan is purely per-file/per-def, no cross-file assumption."""
    ctx, node_ids, consts = _activity_ctx()
    result = extract_temporal(ctx, node_ids, consts)
    act_id = node_ids[_def(ctx, "verify_documents").index]

    assert result.roles[act_id] == {"TemporalActivity"}
    assert act_id not in result.node_props  # workflow_name is workflow-only


def test_activity_file_has_no_workflow_role():
    ctx, node_ids, consts = _activity_ctx()
    result = extract_temporal(ctx, node_ids, consts)
    assert all("TemporalWorkflow" not in roles for roles in result.roles.values())


def test_defn_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _workflow_ctx()
    result = extract_temporal(ctx, {}, consts)
    assert result.roles == {}
    assert result.node_props == {}
    assert result.stats["defn_missing_node_id"] == 1


# -- INVOKES_ACTIVITY: workflow.execute_activity(verify_documents, ...) --


def test_invokes_activity_run_to_verify_documents_resolved():
    ctx0, _, _ = _workflow_ctx()
    exec_call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity")
    arg0 = next(a for a in exec_call.args if a.index == 0)
    span = arg0.name_start_byte
    relpath = "app/workflows/kyc.py"
    target_sym = "scip-python python kyc-worker 0.0 `app.activities.documents`/verify_documents()."
    target_id = "sym:kyc-worker:`app.activities.documents`/verify_documents()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids, consts = _workflow_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids, consts)
    run_id = node_ids[_def(ctx, "run").index]

    invokes = [e for e in result.edges if e.type == "INVOKES_ACTIVITY"]
    assert len(invokes) == 1
    e = invokes[0]
    assert e.src == run_id  # enclosing METHOD id, per golden (NOT the class)
    assert e.dst == target_id
    assert e.resolution == "static" and e.confidence == 1.0
    assert e.props == {"by": "ref"}
    assert e.extractor == "temporal"
    assert e.evidence_file == relpath
    assert result.stats["invokes_activity_resolved"] == 1


def test_invokes_activity_unresolved_ref_lookup_no_edge_and_stat():
    ctx, node_ids, consts = _workflow_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)
    assert result.stats["invokes_activity_unresolved"] == 1


def test_invokes_activity_missing_ref_symbol_lookup_degrades_no_crash():
    ctx, node_ids, consts = _workflow_ctx()  # ref_symbol_lookup defaults to None
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)
    assert result.stats["invokes_activity_unresolved"] == 1


def test_invokes_activity_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _workflow_ctx(ref_symbol_lookup=lambda rp, sb: "x")
    result = extract_temporal(ctx, {}, consts)
    assert result.edges == []
    assert result.stats["invokes_activity_missing_node_id"] == 1


NON_WORKFLOW_RECEIVER_SRC = b'''def execute_activity(fn, **kw):
    pass


def run():
    other.execute_activity(some_fn)
'''


def test_execute_activity_wrong_receiver_ignored():
    """callee name matches but receiver isn't literally "workflow" -- per brief's
    explicit fixture-checked contract, NOT glob/any-receiver like start_workflow.

    The ref lookup resolves UNCONDITIONALLY (same pattern as
    test_start_workflow_matches_any_receiver_not_just_client): if the receiver guard
    were deleted, the matcher would reach arg0 resolution, succeed, and emit a real
    INVOKES_ACTIVITY edge -- making this no-edge assertion genuinely fail under that
    sabotage (verified). Without a wired lookup the test is vacuous: _resolve_ref
    returns None and no edge appears for a reason unrelated to the receiver check.
    """
    ctx, node_ids, consts = _load(
        "m.py", "svc", NON_WORKFLOW_RECEIVER_SRC,
        ref_symbol_lookup=lambda rp, sb: "scip-python python svc 0.0 `m`/some_fn().",
    )
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)


# -- M6 T1 (pilot GAPS §3): _ACTIVITY_INVOKE_CALLEES widening --
#
# Real camunda-gateway code calls `workflow.execute_activity_method(Act.m, ...)`
# (bound-method ref, not a bare activity fn) at ~80 call sites -- the old strict
# `callee_name != "execute_activity"` check missed every one of them. arg0 resolves
# identically for every member of the widened set (GAPS: "резолвится одинаково"),
# so these tests exercise the SAME arg0-resolution path as
# test_invokes_activity_run_to_verify_documents_resolved above, just through the
# new callee names.

EXECUTE_ACTIVITY_METHOD_SRC = b'''class Act:
    async def m(self, x):
        pass


async def run(x):
    await workflow.execute_activity_method(Act.m, x)
'''


def test_execute_activity_method_resolves_arg0_to_invokes_activity():
    """Primary pilot gap (GAPS §3, temporal_ext.py:143): `execute_activity_method`
    must match and resolve arg0 (`Act.m`, an attribute ref) exactly like
    `execute_activity` resolves a bare name ref."""
    ctx0, _, _ = _load("m.py", "svc", EXECUTE_ACTIVITY_METHOD_SRC)
    call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity_method")
    arg0 = next(a for a in call.args if a.index == 0)
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/Act#m()."
    target_id = "sym:svc:`m`/Act#m()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids, consts = _load(
        "m.py", "svc", EXECUTE_ACTIVITY_METHOD_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    run_id = node_ids[_def(ctx, "run").index]

    invokes = [e for e in result.edges if e.type == "INVOKES_ACTIVITY"]
    assert len(invokes) == 1
    e = invokes[0]
    assert e.src == run_id
    assert e.dst == target_id
    assert e.resolution == "static" and e.confidence == 1.0
    assert e.props == {"by": "ref"}
    assert e.extractor == "temporal"
    assert result.stats["invokes_activity_resolved"] == 1


@pytest.mark.parametrize(
    "callee",
    [
        "execute_local_activity",
        "execute_local_activity_method",
        "start_activity",
        "start_local_activity",
    ],
)
def test_activity_invoke_variants_resolve_same_as_execute_activity(callee):
    """Every remaining member of _ACTIVITY_INVOKE_CALLEES shares the same
    receiver=="workflow" + arg0-resolution contract -- proven per-name rather than
    assumed from the frozenset definition alone."""
    src = (
        b"class Act:\n"
        b"    async def m(self, x):\n"
        b"        pass\n\n\n"
        b"async def run(x):\n"
        b"    await workflow." + callee.encode() + b"(Act.m, x)\n"
    )
    ctx0, _, _ = _load("m.py", "svc", src)
    call = next(c for c in ctx0.facts.calls if c.callee_name == callee)
    arg0 = next(a for a in call.args if a.index == 0)
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/Act#m()."
    target_id = "sym:svc:`m`/Act#m()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids, consts = _load("m.py", "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids, consts)

    invokes = [e for e in result.edges if e.type == "INVOKES_ACTIVITY"]
    assert len(invokes) == 1
    assert invokes[0].dst == target_id
    assert result.stats["invokes_activity_resolved"] == 1


def test_execute_activity_method_wrong_receiver_ignored():
    """Widened callee-name set must still enforce receiver == "workflow" -- mirrors
    test_execute_activity_wrong_receiver_ignored, now for the new name.

    Ref lookup resolves unconditionally for the same reason as there: only the
    receiver guard stands between this call and a real edge, so deleting/breaking
    the guard turns this test RED (sabotage-verified) instead of it passing
    vacuously off _resolve_ref's None.
    """
    src = b'''def execute_activity_method(fn, **kw):
    pass


def run():
    other.execute_activity_method(some_fn)
'''
    ctx, node_ids, consts = _load(
        "m.py", "svc", src,
        ref_symbol_lookup=lambda rp, sb: "scip-python python svc 0.0 `m`/some_fn().",
    )
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)


MIXED_OLD_AND_NEW_ACTIVITY_SRC = b'''class Act:
    async def m(self, x):
        pass


def old_activity_fn(x):
    pass


async def run(x):
    await workflow.execute_activity(old_activity_fn, x)
    await workflow.execute_activity_method(Act.m, x)
'''


def test_old_execute_activity_form_unchanged_alongside_new_variant():
    """Explicit byte-identical pin (brief scenario д): widening the callee-name set
    must not alter the pre-existing `execute_activity` form's matching/resolution in
    any way -- both it and the new `execute_activity_method` form resolve
    independently and correctly from the very same enclosing function."""
    ctx0, _, _ = _load("m.py", "svc", MIXED_OLD_AND_NEW_ACTIVITY_SRC)
    old_call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity")
    new_call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity_method")
    old_span = next(a for a in old_call.args if a.index == 0).name_start_byte
    new_span = next(a for a in new_call.args if a.index == 0).name_start_byte

    old_sym = "scip-python python svc 0.0 `m`/old_activity_fn()."
    old_id = "sym:svc:`m`/old_activity_fn()."
    new_sym = "scip-python python svc 0.0 `m`/Act#m()."
    new_id = "sym:svc:`m`/Act#m()."

    def ref_lookup(rp, sb):
        if sb == old_span:
            return old_sym
        if sb == new_span:
            return new_sym
        return None

    ctx, node_ids, consts = _load(
        "m.py", "svc", MIXED_OLD_AND_NEW_ACTIVITY_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    run_id = node_ids[_def(ctx, "run").index]

    invokes = {e.dst: e for e in result.edges if e.type == "INVOKES_ACTIVITY"}
    assert set(invokes) == {old_id, new_id}
    for e in invokes.values():
        assert e.src == run_id
        assert e.resolution == "static" and e.confidence == 1.0
        assert e.props == {"by": "ref"}
    assert result.stats["invokes_activity_resolved"] == 2


# -- start_workflow claim: handle_order_created -> KycWorkflow.run --


def _start_workflow_span():
    ctx0, _, _ = _consumer_orders_ctx()
    sw = next(c for c in ctx0.facts.calls if c.callee_name == "start_workflow")
    arg0 = next(a for a in sw.args if a.index == 0)
    return sw, arg0.name_start_byte


def test_start_workflow_claim_resolved_dst_id():
    sw, span = _start_workflow_span()
    relpath = "app/consumers/orders.py"
    target_sym = "scip-python python kyc-worker 0.0 `app.workflows.kyc`/KycWorkflow#run()."
    target_id = "sym:kyc-worker:`app.workflows.kyc`/KycWorkflow#run()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids, consts = _consumer_orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids, consts)
    handler_id = node_ids[_def(ctx, "handle_order_created").index]

    assert len(result.claims) == 1
    claim = result.claims[0]
    assert claim == {
        "src_id": handler_id, "dst_id": target_id, "evidence_line": sw.start_line,
    }
    assert result.stats["start_workflow_resolved"] == 1
    # start_workflow never produces a direct edge itself -- only INVOKES_ACTIVITY does.
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)


def test_start_workflow_claim_skipped_when_ref_unresolved():
    ctx, node_ids, consts = _consumer_orders_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_temporal(ctx, node_ids, consts)
    assert result.claims == []
    assert result.stats["start_workflow_unresolved"] == 1


def test_start_workflow_missing_ref_symbol_lookup_degrades_no_crash():
    ctx, node_ids, consts = _consumer_orders_ctx()  # defaults to None
    result = extract_temporal(ctx, node_ids, consts)
    assert result.claims == []
    assert result.stats["start_workflow_unresolved"] == 1


def test_start_workflow_missing_node_id_skips_gracefully():
    _, span = _start_workflow_span()
    relpath = "app/consumers/orders.py"

    def ref_lookup(rp, sb):
        return "scip-python python kyc-worker 0.0 `app.workflows.kyc`/KycWorkflow#run()." \
            if (rp, sb) == (relpath, span) else None

    ctx, _real_node_ids, consts = _consumer_orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, {}, consts)
    assert result.claims == []
    assert result.stats["start_workflow_missing_node_id"] == 1


ANY_RECEIVER_START_WORKFLOW_SRC = b'''from app.workflows.kyc import KycWorkflow


async def run_it(handle):
    await handle.start_workflow(KycWorkflow.run, {})
'''


def test_start_workflow_matches_any_receiver_not_just_client():
    """*.start_workflow(...) -- glob on receiver, unlike execute_activity's fixed
    "workflow" check; the real fixture's receiver happens to be "client", this proves
    it's not hardcoded to that literal name."""
    target_sym = "scip-python python svc 0.0 `app.workflows.kyc`/KycWorkflow#run()."

    def ref_lookup(rp, sb):
        return target_sym

    ctx, node_ids, consts = _load(
        "m.py", "svc", ANY_RECEIVER_START_WORKFLOW_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    assert len(result.claims) == 1
    assert result.claims[0]["src_id"] == node_ids[_def(ctx, "run_it").index]


# -- M6 T1 (pilot GAPS §4): _START_WORKFLOW_CALLEES widening --
#
# camunda-gateway workflows resolve `start_workflow` (4 call sites) but also use
# `start_child_workflow` (x3) and, per Temporal's own SDK vocabulary,
# `execute_child_workflow` -- the old strict `callee_name != "start_workflow"` check
# left those 3/7 unresolved. Same any-receiver + arg0-resolution contract as
# test_start_workflow_matches_any_receiver_not_just_client above, just through the
# new callee names.


@pytest.mark.parametrize("callee", ["start_child_workflow", "execute_child_workflow"])
def test_child_workflow_variants_produce_start_mark_claim(callee):
    """Pilot gap (GAPS §4, temporal_ext.py:167): both child-workflow spellings must
    produce the same temporal_start_mark claim shape as `start_workflow` -- any
    receiver, arg0 resolved to dst_id, never a direct edge."""
    src = (
        b"from app.workflows.child import ChildWF\n\n\n"
        b"async def run_it(handle):\n"
        b"    await handle." + callee.encode() + b"(ChildWF.run, {})\n"
    )
    target_sym = "scip-python python svc 0.0 `app.workflows.child`/ChildWF#run()."
    target_id = "sym:svc:`app.workflows.child`/ChildWF#run()."

    def ref_lookup(rp, sb):
        return target_sym

    ctx, node_ids, consts = _load("m.py", "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids, consts)

    assert len(result.claims) == 1
    claim = result.claims[0]
    assert claim["src_id"] == node_ids[_def(ctx, "run_it").index]
    assert claim["dst_id"] == target_id
    assert result.stats["start_workflow_resolved"] == 1
    # child-workflow start is a claim only, same as start_workflow -- never a direct edge.
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)


# =========================================================================================
# M7 T4 (OPEN R3): temporal signals as first-class channels.
#
# Handlers: @workflow.signal/@workflow.update/@workflow.query-decorated methods -> role
# TemporalSignalHandler (shared by all three) + node_props signal_kind. signal/update
# ALSO get a CONSUMES edge into Channel(temporal_signal, name) -- name from the
# decorator's own name= kwarg (string literal only, mirrors fastapi_ext._route_prefix's
# APIRouter(prefix=...) convention -- NOT const-resolved, unlike the sender side below),
# falling back to the method's own name when name= is absent/non-string/empty. query is
# role-only, no channel/edge at all (read-only, not an async boundary).
#
# Senders: ANY receiver (mirrors _extract_start_workflow_claims' own no-receiver-check
# precedent), callee_name == "signal" exactly -> PRODUCES into the SAME channel-id
# scheme. arg0 resolution is a three-way split (see extractors/temporal_ext.py's
# _resolve_signal_arg0 docstring for the full contract): a string literal (or a bare
# name resolving through the file's ConstTable to a string) MATCHES; a bare
# name/attribute that does NOT resolve (a runtime variable, an unresolvable attribute
# chain) counts as an honest miss (signal_name_unresolved); anything else (a numeric
# literal, an f-string, a dict, no args at all -- doesn't look like a signal-name
# reference in the first place) is silently skipped, no counter -- a noise guard,
# not a miss. No ref_symbol_lookup is used on EITHER side here, unlike
# INVOKES_ACTIVITY/start_workflow above.
# =========================================================================================


SIGNAL_WITH_NAME_SRC = b'''class SurveyWorkflow:
    @workflow.signal(name="complete-survey")
    async def complete_survey(self, payload):
        pass
'''


def test_workflow_signal_with_name_role_and_consumes_channel():
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_WITH_NAME_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    handler_id = node_ids[_def(ctx, "complete_survey").index]

    assert result.roles[handler_id] == {"TemporalSignalHandler"}
    assert result.node_props[handler_id] == {"signal_kind": "signal"}

    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    e = consumes[0]
    assert e.src == handler_id
    assert e.dst == "chan:temporal_signal:complete-survey"
    assert e.resolution == "static" and e.confidence == 1.0
    assert e.extractor == "temporal"
    assert e.evidence_file == "m.py"
    assert e.props == {"signal_kind": "signal"}
    assert [c.id for c in result.channels] == ["chan:temporal_signal:complete-survey"]
    # M7 T4 review: cross-contamination guard -- a handler decorator alone must
    # never ALSO emit the sender-side edge type.
    assert not any(e.type == "PRODUCES" for e in result.edges)


SIGNAL_BARE_SRC = b'''class SurveyWorkflow:
    @workflow.signal
    async def survey_ready(self, payload):
        pass
'''


def test_workflow_signal_bare_decorator_uses_method_name():
    """No name= at all (bare, non-call decorator) -- brief: "(или имя метода при
    отсутствии)" -- channel identity falls back to the method's OWN name."""
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_BARE_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    handler_id = node_ids[_def(ctx, "survey_ready").index]

    assert result.roles[handler_id] == {"TemporalSignalHandler"}
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:temporal_signal:survey_ready"


SIGNAL_CALL_NO_NAME_KWARG_SRC = b'''class SurveyWorkflow:
    @workflow.signal()
    async def survey_ready(self, payload):
        pass
'''


def test_workflow_signal_call_form_without_name_kwarg_uses_method_name():
    """Call-form decorator (`@workflow.signal()`) but no name= kwarg at all -- a
    DIFFERENT code path than the bare (non-call) case above (matched via
    match_decorators' call_prefix branch, then a mini re-parse finds a real
    zero-arg CallFact) -- same method-name fallback."""
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_CALL_NO_NAME_KWARG_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].dst == "chan:temporal_signal:survey_ready"


def test_signal_handler_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _load("m.py", "svc", SIGNAL_WITH_NAME_SRC)
    result = extract_temporal(ctx, {}, consts)
    assert result.roles == {}
    assert result.channels == []
    assert not any(e.type == "CONSUMES" for e in result.edges)
    assert result.stats["signal_handler_missing_node_id"] == 1


# -- @workflow.update: same channel treatment as signal, signal_kind="update" --

UPDATE_WITH_NAME_SRC = b'''class SurveyWorkflow:
    @workflow.update(name="resolution-step-updated")
    async def update_resolution_step(self, payload):
        pass
'''


def test_workflow_update_role_and_consumes_channel_with_update_kind():
    ctx, node_ids, consts = _load("m.py", "svc", UPDATE_WITH_NAME_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    handler_id = node_ids[_def(ctx, "update_resolution_step").index]

    assert result.roles[handler_id] == {"TemporalSignalHandler"}
    assert result.node_props[handler_id] == {"signal_kind": "update"}
    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    assert len(consumes) == 1
    e = consumes[0]
    assert e.dst == "chan:temporal_signal:resolution-step-updated"
    assert e.props == {"signal_kind": "update"}
    # M7 T4 review: same cross-contamination guard as the signal-handler test.
    assert not any(e.type == "PRODUCES" for e in result.edges)


# -- @workflow.query: role ONLY, no channel/edge (read-only, not an async boundary) --

QUERY_SRC = b'''class SurveyWorkflow:
    @workflow.query(name="get-status")
    def get_status(self):
        pass
'''


def test_workflow_query_role_only_no_channel_or_edge():
    ctx, node_ids, consts = _load("m.py", "svc", QUERY_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    handler_id = node_ids[_def(ctx, "get_status").index]

    assert result.roles[handler_id] == {"TemporalSignalHandler"}
    assert result.node_props[handler_id] == {"signal_kind": "query"}
    assert result.channels == []
    assert not any(e.type == "CONSUMES" for e in result.edges)
    assert not any(e.type == "PRODUCES" for e in result.edges)


def test_query_handler_missing_node_id_skips_gracefully():
    """M7 T4 review: _extract_query_roles carries its OWN copy of the
    missing-node-id bump (it shares the counter NAME with the signal/update path
    but not the code line) -- exercised independently here so a typo'd key or a
    dropped increment in the query path's copy can't regress silently."""
    ctx, _real_node_ids, consts = _load("m.py", "svc", QUERY_SRC)
    result = extract_temporal(ctx, {}, consts)
    assert result.roles == {}
    assert result.node_props == {}
    assert result.stats["signal_handler_missing_node_id"] == 1


# -- signal senders: `.signal("<name>", ...)` -> PRODUCES --

SIGNAL_SENDER_STRING_SRC = b'''async def notify(handle, payload):
    await handle.signal("complete-survey", payload)
'''


def test_signal_sender_string_arg0_produces_matching_channel():
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_STRING_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    sender_id = node_ids[_def(ctx, "notify").index]

    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    e = produces[0]
    assert e.src == sender_id
    assert e.dst == "chan:temporal_signal:complete-survey"
    assert e.resolution == "heuristic" and e.confidence == 0.6
    assert e.extractor == "temporal"
    assert e.evidence_file == "m.py"
    assert e.props == {"mechanism": "temporal_signal"}
    assert result.stats["signal_sender_resolved"] == 1
    # M7 T4 review: cross-contamination guard -- a sender call alone must never
    # ALSO emit the handler-side edge type.
    assert not any(e.type == "CONSUMES" for e in result.edges)


def test_signal_sender_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_STRING_SRC)
    result = extract_temporal(ctx, {}, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.channels == []  # M7 T4 review: parity with the handler-side sibling
    assert result.stats["signal_sender_missing_node_id"] == 1
    # M8 T2 (rerun-2 R5): the missing-node-id guard fires BEFORE arg0 is even
    # inspected, so it must short-circuit the new claim path exactly as it already
    # does the direct-edge path above.
    assert result.signal_send_claims == []


EXTERNAL_HANDLE_SIGNAL_SRC = b'''async def notify(wf_id):
    await get_external_workflow_handle(wf_id).signal("x")
'''


def test_signal_sender_via_external_workflow_handle_chained_call_produces():
    """Pins CallFact.receiver_text semantics for a CHAINED-call receiver: `fn` is an
    attribute node whose `object` field is itself a `call` node
    (get_external_workflow_handle(wf_id)) -- receiver_text is the raw source TEXT of
    that whole object field regardless of its node type (facts.py's own contract:
    "= весь текст object-поля"), so it comes back as the literal string
    "get_external_workflow_handle(wf_id)", non-None. Since the sender matcher never
    inspects receiver_text at all (ANY receiver, mirrors _extract_start_workflow_
    claims' own no-receiver-check precedent), this chained form is handled by the
    exact same code path as a plain `handle.signal(...)` -- no special-casing."""
    ctx, node_ids, consts = _load("m.py", "svc", EXTERNAL_HANDLE_SIGNAL_SRC)
    call = next(c for c in ctx.facts.calls if c.callee_name == "signal")
    assert call.receiver_text == "get_external_workflow_handle(wf_id)"

    result = extract_temporal(ctx, node_ids, consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].dst == "chan:temporal_signal:x"
    assert produces[0].src == node_ids[_def(ctx, "notify").index]
    assert not any(e.type == "CONSUMES" for e in result.edges)


HANDLER_AND_SENDER_SRC = b'''class SurveyWorkflow:
    @workflow.signal(name="complete-survey")
    async def complete_survey(self, payload):
        pass


async def notify(handle, payload):
    await handle.signal("complete-survey", payload)
'''


def test_handler_and_sender_in_same_service_share_channel_id():
    """Deterministic chan id (make_channel_node("temporal_signal", name=...)) unifies
    both sides for free -- no explicit dedup/join logic needed (M6-gate-style
    channel unification, per the brief). The channels list itself is pinned too (M7
    T4 review): it deliberately holds TWO fully-equal NodeRec entries here, one
    appended per side -- extractor-level output is NOT deduped; staging's id-keyed
    upsert collapses them later -- and BOTH must carry the exact id every edge's
    dst names, or the unification claim would be vacuous."""
    ctx, node_ids, consts = _load("m.py", "svc", HANDLER_AND_SENDER_SRC)
    result = extract_temporal(ctx, node_ids, consts)

    consumes = [e for e in result.edges if e.type == "CONSUMES"]
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(consumes) == 1 and len(produces) == 1
    assert consumes[0].dst == produces[0].dst == "chan:temporal_signal:complete-survey"
    assert [c.id for c in result.channels] == [
        "chan:temporal_signal:complete-survey", "chan:temporal_signal:complete-survey",
    ]
    assert result.channels[0] == result.channels[1]  # equal NodeRecs, one staged row


# -- sender arg0 resolution: the three-way split --

SIGNAL_SENDER_VARIABLE_SRC = b'''async def notify(handle, signal_name, payload):
    await handle.signal(signal_name, payload)
'''


def test_signal_sender_unresolvable_variable_bumps_counter_no_edge():
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_VARIABLE_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 1


SIGNAL_SENDER_CONST_SRC = b'''SIGNAL_NAME = "complete-survey"


async def notify(handle, payload):
    await handle.signal(SIGNAL_NAME, payload)
'''


def test_signal_sender_name_resolves_via_module_const_produces():
    """arg0-литерал имени (brief's "Consumes: consts") -- a bare identifier IS
    resolvable when it names a module-level `NAME = "literal"` constant, unlike the
    unresolvable-variable case above."""
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_CONST_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    produces = [e for e in result.edges if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].dst == "chan:temporal_signal:complete-survey"
    assert result.stats["signal_name_unresolved"] == 0


NON_TEMPORAL_SIGNAL_CALL_SRC = b'''def run(foo):
    foo.signal(123)
'''


def test_signal_sender_non_string_non_name_arg0_silently_skipped_no_counter():
    """Not a signal-looking call at all (brief's noise-guard bucket): a
    non-string-literal arg0 that's ALSO not name/attr-shaped (a bare int literal)
    is neither matched NOR counted as an honest miss -- distinguishes this from the
    unresolvable-VARIABLE case above, which DOES look like a genuine (if
    unresolvable) signal reference."""
    ctx, node_ids, consts = _load("m.py", "svc", NON_TEMPORAL_SIGNAL_CALL_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0


SIGNAL_SENDER_FSTRING_SRC = b'''async def notify(handle, suffix, payload):
    await handle.signal(f"signal-{suffix}", payload)
'''


def test_signal_sender_fstring_arg0_silently_skipped_no_counter():
    """An f-string arg0 resolves through consts.resolve_arg to its OWN "template"
    kind (not "value") -- deliberately treated as noise here (too uncertain to guess
    a stable channel identity from a template), NOT as an unresolvable-name-like
    miss, even though it plausibly IS a real (if dynamic) signal call in practice.
    Documented scope limitation, mirrors the non-string-non-name-like (123) bucket."""
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_FSTRING_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0


SIGNAL_SENDER_EMPTY_STRING_SRC = b'''async def notify(handle, payload):
    await handle.signal("", payload)
'''


def test_signal_sender_empty_string_arg0_no_crash_and_counts_as_unresolved():
    """Same "M2 final review fix" empty-name guard every other make_channel_node
    call site in this codebase already carries (see kafka_ext.py) -- an empty
    string is a STRING (looks like a genuine, if malformed, signal call), so it
    counts as an honest miss, not a silent skip."""
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_EMPTY_STRING_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 1


SIGNAL_SENDER_NO_ARGS_SRC = b'''def run(sig):
    sig.signal()
'''


def test_signal_sender_no_args_silently_skipped_no_counter():
    ctx, node_ids, consts = _load("m.py", "svc", SIGNAL_SENDER_NO_ARGS_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0


STDLIB_SIGNAL_COLLISION_SRC = b'''import signal


def install_handler(handler):
    signal.signal(signal.SIGTERM, handler)
'''


def test_stdlib_signal_signal_receiver_filtered_no_edge_no_counter():
    """M7 T4 review follow-up: a receiver literally named `signal` is Python's own
    stdlib module in practice, never a Temporal handle (no real handle variable is
    named `signal`), and SIGTERM/SIGINT installation is a common enough idiom that
    letting it bump signal_name_unresolved would add noise on virtually every
    service. The sender matcher's ONE receiver check (`receiver_text == "signal"`
    -- an exact-name EXCLUSION, not a positive filter; every other receiver is
    still matched blindly) drops it before arg0 classification ever runs: no edge,
    no counter. The FP-risk for OTHER non-Temporal `.signal(...)` receivers stays
    accepted+documented as before (temporal_ext.py's module docstring), and an
    ALIASED stdlib import (`import signal as sig; sig.signal(...)`) is a known
    filter limit -- it still lands in the honest-miss bucket like any other
    name-like unresolvable arg0."""
    ctx, node_ids, consts = _load("m.py", "svc", STDLIB_SIGNAL_COLLISION_SRC)
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0
    assert result.stats["signal_sender_resolved"] == 0


# =========================================================================================
# M8 T2 (rerun-2 R5, docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
# typed signal senders -- `handle.signal(Cls.method, payload)` / a bare-name imported
# method arg0. `_resolve_signal_arg0`'s pre-existing consts-only resolution NEVER
# matches either shape (consts.py only knows module-level STRING literals) -- these
# arg0s are attr/name-shaped, so they fall through to a SECOND resolution attempt:
# `_resolve_ref` on arg0's own span, the exact same helper/span convention
# `_extract_invokes_activity` already uses for `execute_activity(fn_ref, ...)`.
# Resolved -> a `temporal_signal_send` CLAIM {src_id, method_symbol, evidence_line} --
# NEVER a direct edge (this file alone can't see whether that symbol is even a signal
# handler, let alone which channel/service it belongs to) -- linking (S7,
# linking/signal_send.py) pairs it with the handler's own CONSUMES edge later. The
# string-literal/module-const-name paths above are completely UNCHANGED (same
# _resolve_signal_arg0 call, same early-return branch); only the case where THAT
# resolution fails now gets a second chance here before conceding
# signal_name_unresolved.
# =========================================================================================

TYPED_SIGNAL_SENDER_ATTR_SRC = b'''class PartnerProfileWorkflow:
    @workflow.signal(name="complete-survey")
    async def complete_survey(self, payload):
        pass


async def notify(handle, payload):
    await handle.signal(PartnerProfileWorkflow.complete_survey, payload)
'''


def test_signal_sender_attr_arg0_resolves_via_ref_symbol_lookup_to_claim():
    """The real R5 shape: `handle.signal(Cls.method, payload)` -- arg0 is
    attr-shaped and names no module-level const, so resolution must fall through to
    ref_symbol_lookup on arg0's own span -- producing a CLAIM, never a direct edge
    (the channel name lives on the handler side, in this SAME file here but that is
    incidental -- the real case is cross-file, see the bare-name/cross-file tests
    below)."""
    ctx0, _, _ = _load("m.py", "svc", TYPED_SIGNAL_SENDER_ATTR_SRC)
    call = next(c for c in ctx0.facts.calls if c.callee_name == "signal")
    arg0 = next(a for a in call.args if a.index == 0)
    assert arg0.value_kind == "attr"
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/PartnerProfileWorkflow#complete_survey()."
    target_id = "sym:svc:`m`/PartnerProfileWorkflow#complete_survey()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids, consts = _load(
        "m.py", "svc", TYPED_SIGNAL_SENDER_ATTR_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    sender_id = node_ids[_def(ctx, "notify").index]

    # NEVER a direct edge -- the channel name lives on the handler side only.
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0
    assert result.stats["signal_sender_symbol_resolved"] == 1
    assert result.stats["signal_sender_resolved"] == 0  # NOT the literal-name counter

    assert len(result.signal_send_claims) == 1
    assert result.signal_send_claims[0] == {
        "src_id": sender_id, "method_symbol": target_id, "evidence_line": call.start_line,
    }


BARE_NAME_IMPORTED_METHOD_SRC = b'''from app.workflows.survey import step_other_evidence_update


async def notify(handle, update_input):
    await handle.signal(step_other_evidence_update, update_input)
'''


def test_signal_sender_bare_name_imported_method_resolves_via_ref_symbol_lookup_to_claim():
    """Bare-name arg0 (an imported function/method reference, no attribute dots at
    all -- brief's own second real-code example) -- the SAME ref_symbol_lookup path
    as the attr-shaped case above, just through ArgFact's "name" value_kind instead
    of "attr" (mirrors INVOKES_ACTIVITY's own name-vs-attr-agnostic arg0
    resolution, see _resolve_ref/_arg0 above)."""
    ctx0, _, _ = _load("m.py", "svc", BARE_NAME_IMPORTED_METHOD_SRC)
    call = next(c for c in ctx0.facts.calls if c.callee_name == "signal")
    arg0 = next(a for a in call.args if a.index == 0)
    assert arg0.value_kind == "name"
    span = arg0.name_start_byte
    target_sym = (
        "scip-python python svc 0.0 `app.workflows.survey`/step_other_evidence_update()."
    )
    target_id = "sym:svc:`app.workflows.survey`/step_other_evidence_update()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids, consts = _load(
        "m.py", "svc", BARE_NAME_IMPORTED_METHOD_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    sender_id = node_ids[_def(ctx, "notify").index]

    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.stats["signal_name_unresolved"] == 0
    assert result.stats["signal_sender_symbol_resolved"] == 1
    assert len(result.signal_send_claims) == 1
    assert result.signal_send_claims[0] == {
        "src_id": sender_id, "method_symbol": target_id, "evidence_line": call.start_line,
    }


UNRESOLVABLE_ATTR_SIGNAL_SRC = b'''async def notify(handle, payload):
    await handle.signal(some_module.SomeClass.some_method, payload)
'''


def test_signal_sender_attr_arg0_unresolvable_symbol_bumps_counter_no_claim_no_edge():
    """arg0 IS attr-shaped (looks exactly like a typed method reference) but
    ref_symbol_lookup genuinely finds nothing for its span (e.g. a real SCIP miss)
    -- falls back to the SAME honest-miss bucket as before this task
    (signal_name_unresolved), never a claim. This is the report's own negative pin
    ("handle.signal(some_runtime_var) -> счётчик, не ребро"), exercised here with a
    WIRED-but-missing lookup, complementing the pre-existing unwired-lookup variant
    (test_signal_sender_unresolvable_variable_bumps_counter_no_edge below, which
    never even attempts ref_symbol_lookup because ctx.ref_symbol_lookup is None)."""
    ctx, node_ids, consts = _load(
        "m.py", "svc", UNRESOLVABLE_ATTR_SIGNAL_SRC, ref_symbol_lookup=lambda rp, sb: None,
    )
    result = extract_temporal(ctx, node_ids, consts)
    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert result.signal_send_claims == []
    assert result.stats["signal_name_unresolved"] == 1
    assert result.stats["signal_sender_symbol_resolved"] == 0


EXTERNAL_HANDLE_TYPED_SIGNAL_SRC = b'''class ChildWorkflow:
    @workflow.signal(name="go")
    async def go(self, payload):
        pass


async def notify(wf_id, payload):
    await get_external_workflow_handle(wf_id).signal(ChildWorkflow.go, payload)
'''


def test_signal_sender_via_external_workflow_handle_with_typed_method_ref_produces_claim():
    """`get_external_workflow_handle(...).signal(Cls.method, ...)` -- same
    receiver-agnostic matcher as the string-literal EXTERNAL_HANDLE test above
    (_extract_signal_senders never inspects receiver_text at all) -- proves the R5
    fix reaches this chained-call SHAPE too, not just a plain `handle.signal(...)`;
    only arg0's own resolution changes, the matcher itself needed no code change."""
    ctx0, _, _ = _load("m.py", "svc", EXTERNAL_HANDLE_TYPED_SIGNAL_SRC)
    call = next(c for c in ctx0.facts.calls if c.callee_name == "signal")
    assert call.receiver_text == "get_external_workflow_handle(wf_id)"
    arg0 = next(a for a in call.args if a.index == 0)
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/ChildWorkflow#go()."
    target_id = "sym:svc:`m`/ChildWorkflow#go()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids, consts = _load(
        "m.py", "svc", EXTERNAL_HANDLE_TYPED_SIGNAL_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids, consts)
    sender_id = node_ids[_def(ctx, "notify").index]

    assert not any(e.type == "PRODUCES" for e in result.edges)
    assert len(result.signal_send_claims) == 1
    assert result.signal_send_claims[0] == {
        "src_id": sender_id, "method_symbol": target_id, "evidence_line": call.start_line,
    }
