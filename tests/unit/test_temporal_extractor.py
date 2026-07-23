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
not read anywhere downstream yet, same as fastapi_ext.stats today. `channels` is
deliberately NOT added -- temporal never creates Channel nodes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from codegraph.extractors.base import FileContext
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.extractors.temporal_ext import TemporalResult, extract_temporal
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
    return ctx, node_ids


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
    r = TemporalResult(roles={}, node_props={}, edges=[], claims=[], stats={})
    assert r.roles == {}
    assert r.node_props == {}
    assert r.edges == []
    assert r.claims == []
    assert r.stats == {}


def test_no_decorators_or_temporal_calls_is_a_noop():
    ctx, node_ids = _load("m.py", "svc", b"def plain():\n    pass\n")
    result = extract_temporal(ctx, node_ids)
    assert result == TemporalResult(
        roles={}, node_props={}, edges=[], claims=[], stats=result.stats,
    )


# -- @workflow.defn / @activity.defn roles --


def test_workflow_defn_role_and_workflow_name_prop():
    ctx, node_ids = _workflow_ctx()
    result = extract_temporal(ctx, node_ids)
    class_id = node_ids[_def(ctx, "KycWorkflow").index]

    assert result.roles[class_id] == {"TemporalWorkflow"}
    assert result.node_props[class_id] == {"workflow_name": "KycWorkflow"}


def test_activity_defn_role_on_separate_file():
    """verify_documents lives in a DIFFERENT file than KycWorkflow -- proves the
    decorator scan is purely per-file/per-def, no cross-file assumption."""
    ctx, node_ids = _activity_ctx()
    result = extract_temporal(ctx, node_ids)
    act_id = node_ids[_def(ctx, "verify_documents").index]

    assert result.roles[act_id] == {"TemporalActivity"}
    assert act_id not in result.node_props  # workflow_name is workflow-only


def test_activity_file_has_no_workflow_role():
    ctx, node_ids = _activity_ctx()
    result = extract_temporal(ctx, node_ids)
    assert all("TemporalWorkflow" not in roles for roles in result.roles.values())


def test_defn_missing_node_id_skips_gracefully():
    ctx, _real_node_ids = _workflow_ctx()
    result = extract_temporal(ctx, {})
    assert result.roles == {}
    assert result.node_props == {}
    assert result.stats["defn_missing_node_id"] == 1


# -- INVOKES_ACTIVITY: workflow.execute_activity(verify_documents, ...) --


def test_invokes_activity_run_to_verify_documents_resolved():
    ctx0, _ = _workflow_ctx()
    exec_call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity")
    arg0 = next(a for a in exec_call.args if a.index == 0)
    span = arg0.name_start_byte
    relpath = "app/workflows/kyc.py"
    target_sym = "scip-python python kyc-worker 0.0 `app.activities.documents`/verify_documents()."
    target_id = "sym:kyc-worker:`app.activities.documents`/verify_documents()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _workflow_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids)
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
    ctx, node_ids = _workflow_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_temporal(ctx, node_ids)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)
    assert result.stats["invokes_activity_unresolved"] == 1


def test_invokes_activity_missing_ref_symbol_lookup_degrades_no_crash():
    ctx, node_ids = _workflow_ctx()  # ref_symbol_lookup defaults to None
    result = extract_temporal(ctx, node_ids)
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)
    assert result.stats["invokes_activity_unresolved"] == 1


def test_invokes_activity_missing_node_id_skips_gracefully():
    ctx, _real_node_ids = _workflow_ctx(ref_symbol_lookup=lambda rp, sb: "x")
    result = extract_temporal(ctx, {})
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
    ctx, node_ids = _load(
        "m.py", "svc", NON_WORKFLOW_RECEIVER_SRC,
        ref_symbol_lookup=lambda rp, sb: "scip-python python svc 0.0 `m`/some_fn().",
    )
    result = extract_temporal(ctx, node_ids)
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
    ctx0, _ = _load("m.py", "svc", EXECUTE_ACTIVITY_METHOD_SRC)
    call = next(c for c in ctx0.facts.calls if c.callee_name == "execute_activity_method")
    arg0 = next(a for a in call.args if a.index == 0)
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/Act#m()."
    target_id = "sym:svc:`m`/Act#m()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids = _load("m.py", "svc", EXECUTE_ACTIVITY_METHOD_SRC, ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids)
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
    ctx0, _ = _load("m.py", "svc", src)
    call = next(c for c in ctx0.facts.calls if c.callee_name == callee)
    arg0 = next(a for a in call.args if a.index == 0)
    span = arg0.name_start_byte
    target_sym = "scip-python python svc 0.0 `m`/Act#m()."
    target_id = "sym:svc:`m`/Act#m()."

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == ("m.py", span) else None

    ctx, node_ids = _load("m.py", "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids)

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
    ctx, node_ids = _load(
        "m.py", "svc", src,
        ref_symbol_lookup=lambda rp, sb: "scip-python python svc 0.0 `m`/some_fn().",
    )
    result = extract_temporal(ctx, node_ids)
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
    ctx0, _ = _load("m.py", "svc", MIXED_OLD_AND_NEW_ACTIVITY_SRC)
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

    ctx, node_ids = _load(
        "m.py", "svc", MIXED_OLD_AND_NEW_ACTIVITY_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids)
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
    ctx0, _ = _consumer_orders_ctx()
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

    ctx, node_ids = _consumer_orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids)
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
    ctx, node_ids = _consumer_orders_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_temporal(ctx, node_ids)
    assert result.claims == []
    assert result.stats["start_workflow_unresolved"] == 1


def test_start_workflow_missing_ref_symbol_lookup_degrades_no_crash():
    ctx, node_ids = _consumer_orders_ctx()  # defaults to None
    result = extract_temporal(ctx, node_ids)
    assert result.claims == []
    assert result.stats["start_workflow_unresolved"] == 1


def test_start_workflow_missing_node_id_skips_gracefully():
    _, span = _start_workflow_span()
    relpath = "app/consumers/orders.py"

    def ref_lookup(rp, sb):
        return "scip-python python kyc-worker 0.0 `app.workflows.kyc`/KycWorkflow#run()." \
            if (rp, sb) == (relpath, span) else None

    ctx, _real_node_ids = _consumer_orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, {})
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

    ctx, node_ids = _load(
        "m.py", "svc", ANY_RECEIVER_START_WORKFLOW_SRC, ref_symbol_lookup=ref_lookup,
    )
    result = extract_temporal(ctx, node_ids)
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

    ctx, node_ids = _load("m.py", "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_temporal(ctx, node_ids)

    assert len(result.claims) == 1
    claim = result.claims[0]
    assert claim["src_id"] == node_ids[_def(ctx, "run_it").index]
    assert claim["dst_id"] == target_id
    assert result.stats["start_workflow_resolved"] == 1
    # child-workflow start is a claim only, same as start_workflow -- never a direct edge.
    assert not any(e.type == "INVOKES_ACTIVITY" for e in result.edges)
