"""Общие хелперы для интеграционных тестов."""

from __future__ import annotations

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreUnavailable, ping


def falkordb_available() -> bool:
    """True, если FalkorDB отвечает на дефолтный host:port из конфига."""
    try:
        ping(FalkorDBConfig())
        return True
    except StoreUnavailable:
        return False
