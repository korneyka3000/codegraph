"""DDL для FalkorDB: range-индексы Sym.* и UNIQUE-констрейнт Sym.id (идемпотентно).

Chunk.id индексы здесь НЕ создаются — появятся в M3 вместе с векторным поиском.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)

# Реальные подстроки ошибок FalkorDB v4.18.11 при повторном создании DDL-объектов —
# захвачены эмпирически на живом контейнере codegraph-falkordb (см. m1b-task-2-report.md):
#   - CREATE INDEX дважды на том же (label, property):
#       redis.exceptions.ResponseError: "Attribute 'id' is already indexed"
#   - GRAPH.CONSTRAINT CREATE дважды:
#       redis.exceptions.ResponseError: 'Constraint already exists'
# Подстрочный (регистронезависимый) матч по этим маркерам даёт идемпотентность;
# всё остальное — например 'missing supporting exact-match index' при нарушении
# порядка индекс-до-констрейнта — настоящая ошибка и пробрасывается.
_IGNORABLE_DDL_MARKERS = ("already indexed", "already exists", "constraint already")


def ensure_schema(db, graph_name: str) -> None:
    """Идемпотентно создаёт индексы и UNIQUE-констрейнт схемы M1 на графе graph_name.

    Порядок важен: индекс на Sym.id создаётся ДО GRAPH.CONSTRAINT CREATE — FalkorDB
    требует уже существующий exact-match индекс на свойстве до наложения UNIQUE
    constraint (иначе 'missing supporting exact-match index'); паттерн доказан
    в doctor._constraint probe.
    """
    g = db.select_graph(graph_name)
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.id)"))
    _swallow_ddl_errors(lambda: db.connection.execute_command(
        "GRAPH.CONSTRAINT", "CREATE", graph_name,
        "UNIQUE", "NODE", "Sym", "PROPERTIES", "1", "id",
    ))
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.qualified_name)"))
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.service)"))


def _swallow_ddl_errors(fn: Callable[[], object]) -> None:
    try:
        fn()
    except Exception as e:
        msg = str(e).lower()
        if not any(marker in msg for marker in _IGNORABLE_DDL_MARKERS):
            raise
        logger.info("ensure_schema: already applied, skipping (%s)", e)
