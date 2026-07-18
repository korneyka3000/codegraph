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
    TraceProcessInput,
    TraceStep,
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
