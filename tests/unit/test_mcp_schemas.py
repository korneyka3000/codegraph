"""Юнит-тест mcp/schemas.py Hop-модели (M2: direction -- новое обязательное поле,
см. query/api.py GraphQuery.expand_neighbors и stores/graph.py Hop). Полный
контракт (реальный сервер + живые ответы) -- tests/integration/test_mcp_contract.py,
marker falkordb; здесь -- только форма pydantic-модели, без сети/FalkorDB."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from codegraph.mcp.schemas import Hop


def test_hop_requires_direction():
    with pytest.raises(ValidationError):
        Hop(node="a", edge_type="CALLS", edge_props={})


def test_hop_accepts_out_and_in_direction():
    assert Hop(node="a", edge_type="CALLS", edge_props={}, direction="out").direction == "out"
    assert Hop(node="a", edge_type="CALLS", edge_props={}, direction="in").direction == "in"


def test_hop_rejects_direction_outside_out_in():
    with pytest.raises(ValidationError):
        Hop(node="a", edge_type="CALLS", edge_props={}, direction="both")
