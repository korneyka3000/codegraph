"""Общие фикстуры для интеграционных тестов."""

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
