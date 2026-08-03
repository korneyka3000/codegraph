"""Юнит-тест mcp/schemas.py Hop-модели (M2: direction -- новое обязательное поле,
см. query/api.py GraphQuery.expand_neighbors и stores/graph.py Hop). Полный
контракт (реальный сервер + живые ответы) -- tests/integration/test_mcp_contract.py,
marker falkordb; здесь -- только форма pydantic-модели, без сети/FalkorDB."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from codegraph.mcp.schemas import (
    FindEntrypointOutput,
    Hop,
    SearchCodeInput,
    SearchCodeOutput,
    TraceExit,
    TraceProcessInput,
    TraceProcessOutput,
    TraceStep,
    WhoCallsOutput,
)


def test_hop_requires_direction():
    with pytest.raises(ValidationError):
        Hop(node="a", edge_type="CALLS", edge_props={})


def test_hop_accepts_out_and_in_direction():
    assert Hop(node="a", edge_type="CALLS", edge_props={}, direction="out").direction == "out"
    assert Hop(node="a", edge_type="CALLS", edge_props={}, direction="in").direction == "in"


def test_hop_rejects_direction_outside_out_in():
    with pytest.raises(ValidationError):
        Hop(node="a", edge_type="CALLS", edge_props={}, direction="both")


# -- M3 T7: search_code / find_entrypoint v2 schemas --


def test_search_code_input_defaults_to_hybrid_mode():
    assert SearchCodeInput(query="create order").mode == "hybrid"


def test_search_code_input_accepts_all_three_modes():
    for mode in ("hybrid", "vector", "text"):
        assert SearchCodeInput(query="q", mode=mode).mode == mode


def test_search_code_input_rejects_unknown_mode():
    with pytest.raises(ValidationError):
        SearchCodeInput(query="q", mode="sideways")


def test_search_code_output_requires_mode_used():
    with pytest.raises(ValidationError):
        SearchCodeOutput(items=[])


def test_search_code_output_accepts_well_formed_item():
    out = SearchCodeOutput(
        items=[{
            "chunk_id": "c1", "symbol_id": "sym:a:x",
            "qualified_name": "app.mod.x", "service": "svc",
            "relpath": "mod.py", "start_line": 1, "end_line": 2,
            "snippet": "def f(): pass", "score": 0.5,
        }],
        mode_used="hybrid",
    )
    assert out.items[0].chunk_id == "c1"
    assert out.items[0].qualified_name == "app.mod.x"


def test_search_code_item_qualified_name_optional_defaults_none():
    # Pre-T7-loaded graphs / symbol-less chunks legitimately lack the property --
    # the schema must accept its absence, defaulting to None.
    out = SearchCodeOutput(items=[{"snippet": "x", "score": 0.1}], mode_used="text")
    assert out.items[0].qualified_name is None


# -- M10 T3 (pilot §4.1): SearchCodeItem.enclosing_symbol/chunk_kind -- additive,
# same "prove additivity, don't just assert" precedent as TraceExit.channel/
# WhoCallsOutput.callers[].mechanism above.


def test_search_code_output_accepts_item_with_enclosing_symbol_and_chunk_kind():
    out = SearchCodeOutput(
        items=[{
            "chunk_id": "c1", "symbol_id": "sym:a:x",
            "qualified_name": "app.mod.X.method", "enclosing_symbol": "app.mod.X.method",
            "chunk_kind": "Function", "service": "svc",
            "relpath": "mod.py", "start_line": 1, "end_line": 2,
            "snippet": "def method(self): pass", "score": 0.5,
        }],
        mode_used="hybrid",
    )
    assert out.items[0].enclosing_symbol == "app.mod.X.method"
    assert out.items[0].chunk_kind == "Function"


def test_search_code_item_enclosing_symbol_and_chunk_kind_optional_default_none():
    # Pre-T3 result shape (neither key present at all) must keep validating exactly
    # like every other additive field this schema has grown (qualified_name,
    # external_exit_count, mechanism, ...).
    out = SearchCodeOutput(items=[{"snippet": "x", "score": 0.1}], mode_used="text")
    assert out.items[0].enclosing_symbol is None
    assert out.items[0].chunk_kind is None


def test_find_entrypoint_output_requires_mode_used():
    with pytest.raises(ValidationError):
        FindEntrypointOutput(results=[])


def test_find_entrypoint_output_rejects_vector_as_mode_used():
    # find_entrypoint has no pure "vector" mode -- always hybrid or text (M2 back-compat).
    with pytest.raises(ValidationError):
        FindEntrypointOutput(results=[], mode_used="vector")


# -- M5 T5: compact trace segments -- collapsed marker on TraceStep, compact input flag --


def test_trace_step_accepts_a_real_step_unchanged():
    step = TraceStep(edge_type="CALLS", props={}, node={"id": "x"}, direction="out")
    assert step.edge_type == "CALLS"
    assert step.node == {"id": "x"}
    assert step.collapsed is None  # absent on a real step


def test_trace_step_accepts_a_collapsed_marker_with_only_that_field():
    # the exact synthetic marker shape query.traverse._compact_steps produces --
    # additive: a real step's 4 pre-existing fields all still validate exactly as
    # before (previous test), this is the NEW accepted shape, not a replacement.
    step = TraceStep(collapsed=35)
    assert step.collapsed == 35
    assert step.node == {}
    assert step.edge_type == ""
    assert step.direction == "out"


def test_trace_process_input_compact_defaults_true():
    assert TraceProcessInput(entrypoint_id="e1").compact is True


def test_trace_process_input_compact_can_be_set_false():
    assert TraceProcessInput(entrypoint_id="e1", compact=False).compact is False


# -- M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): TraceExit's
# `channel` field is a plain dict (no nested Channel model) -- additive external/
# external_host props (linking/http_routes.py's tier 2a) validate exactly like ANY
# other property already living there (owner_service, config_ref, ...), no schema
# change needed. Pinned here anyway -- same "prove additivity, don't just assert"
# precedent as the M5 T5 collapsed-marker tests above.


def test_trace_exit_channel_accepts_external_props_additively():
    exit_ = TraceExit(
        channel={
            "id": "chan:http:?:GET /x", "external": True,
            "external_host": "api-gateway.prod.svc.cluster.local",
        },
        next_entry_ids=[],
    )
    assert exit_.channel["external"] is True
    assert exit_.channel["external_host"] == "api-gateway.prod.svc.cluster.local"


def test_trace_exit_channel_without_external_props_still_validates_unchanged():
    exit_ = TraceExit(channel={"id": "chan:event_type:X"}, next_entry_ids=["e2"])
    assert "external" not in exit_.channel


# -- M9 T1 review Important: TraceProcessOutput.external_exit_count -- the
# machine-readable top-level boundary signal (same precedent as `truncated`: a
# programmatic/MCP consumer reading confidence=1.0 alone would conclude "fully
# traced" for a trace that actually stops at a workspace boundary). Additive:
# defaults to 0, so a pre-M9 result dict without the key still validates.


def test_trace_process_output_accepts_external_exit_count():
    out = TraceProcessOutput(
        segments=[], confidence=1.0, truncated=False, external_exit_count=3,
    )
    assert out.external_exit_count == 3


def test_trace_process_output_external_exit_count_defaults_to_zero_when_absent():
    """Additivity pin: the exact pre-M9 result shape (no external_exit_count key
    at all) must keep validating -- the field defaults to 0, never required."""
    out = TraceProcessOutput(segments=[], confidence=0.5, truncated=False)
    assert out.external_exit_count == 0


# -- M10 T2 (pilot §4.3): WhoCallsOutput.callers[].mechanism -- same "prove
# additivity, don't just assert" precedent as TraceExit.channel above: `callers`
# stays a plain `list[dict]`, so nothing here NEEDS a schema change to validate --
# pinned anyway so a future refactor that DOES tighten this type has to notice.


def test_who_calls_output_caller_accepts_mechanism_additively():
    out = WhoCallsOutput(
        callers=[{"id": "sym:svc:workflow_run", "mechanism": "invokes_activity"}],
        truncated=False,
    )
    assert out.callers[0]["mechanism"] == "invokes_activity"


def test_who_calls_output_caller_without_mechanism_still_validates_unchanged():
    """Additivity pin: a plain CALLS-sourced caller dict (no mechanism key at
    all -- the pre-T2, and still the ordinary-function, shape) keeps validating,
    and the key stays genuinely absent (not None)."""
    out = WhoCallsOutput(callers=[{"id": "sym:svc:a"}], truncated=False)
    assert "mechanism" not in out.callers[0]
