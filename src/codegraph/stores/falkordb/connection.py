"""Единственная точка создания подключений к FalkorDB."""

from __future__ import annotations

import redis.exceptions
from falkordb import FalkorDB

from codegraph.config.models import FalkorDBConfig


class StoreUnavailable(Exception):
    pass


# Алиас базового класса всех ошибок redis-py (ConnectionError/TimeoutError/
# ResponseError -- все его наследники) для потребителей вне stores/falkordb/:
# cli ловит недоступность store через (StoreError, StoreUnavailable), не
# импортируя redis напрямую (граница импортов -- redis живёт только здесь).
StoreError = redis.exceptions.RedisError


def connect(cfg: FalkorDBConfig) -> FalkorDB:
    return FalkorDB(host=cfg.host, port=cfg.port)


def ping(cfg: FalkorDBConfig) -> str:
    try:
        db = connect(cfg)
        db.connection.ping()
    except (redis.exceptions.RedisError, ConnectionError, OSError) as e:
        raise StoreUnavailable(f"FalkorDB not reachable at {cfg.host}:{cfg.port}: {e}") from e
    return "ok"
