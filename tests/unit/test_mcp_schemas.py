"""Юнит-тест mcp/schemas.py Hop-модели (M2: direction -- новое обязательное поле,
см. query/api.py GraphQuery.expand_neighbors и stores/graph.py Hop). Полный
контракт (реальный сервер + живые ответы) -- tests/integration/test_mcp_contract.py,
marker falkordb; здесь -- только форма pydantic-модели, без сети/FalkorDB."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from codegraph.mcp.schemas import FindEntrypointOutput, Hop, SearchCodeInput, SearchCodeOutput


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
            "chunk_id": "c1", "symbol_id": "sym:a:x", "service": "svc",
            "relpath": "mod.py", "start_line": 1, "end_line": 2,
            "snippet": "def f(): pass", "score": 0.5,
        }],
        mode_used="hybrid",
    )
    assert out.items[0].chunk_id == "c1"


def test_find_entrypoint_output_requires_mode_used():
    with pytest.raises(ValidationError):
        FindEntrypointOutput(results=[])


def test_find_entrypoint_output_rejects_vector_as_mode_used():
    # find_entrypoint has no pure "vector" mode -- always hybrid or text (M2 back-compat).
    with pytest.raises(ValidationError):
        FindEntrypointOutput(results=[], mode_used="vector")
