"""Общие фикстуры для tests/eval (M2 gate). tests/eval -- не поддерево tests/
integration, поэтому pytest не подхватывает tests/integration/conftest.py's
falkordb_cfg автоматически; тот же fixture, продублированный здесь намеренно (см.
tests/integration/conftest.py) вместо переезда в общий tests/conftest.py -- держит
M1's интеграционный слой нетронутым, минимальный blast radius для M2 T9."""

from __future__ import annotations

import pytest

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreUnavailable, ping


@pytest.fixture
def falkordb_cfg() -> FalkorDBConfig:
    """Конфиг живого FalkorDB; skip, если инстанс недоступен."""
    cfg = FalkorDBConfig()
    try:
        ping(cfg)
    except StoreUnavailable:
        pytest.skip("FalkorDB not running")
    return cfg
