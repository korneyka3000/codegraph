"""DDL для FalkorDB: range-индексы Sym.* и UNIQUE-констрейнт Sym.id (идемпотентно) +
fulltext-индекс Sym(name, qualified_name, docstring) (M2 T8, для
store.search_fulltext/GraphQuery.find_entrypoint).

M3 T6 adds: vector index on Chunk.embedding (cosine similarity, dimension-parameterized
-- only created when `dim` is given, since the vector index needs a fixed dimension and
a workspace with no working embedder this run has nothing meaningful to size it with,
see `ensure_schema`'s own docstring) + fulltext index on Chunk(text, context_header)
(unconditional -- fulltext search doesn't need a fixed dimension, and a chunk without an
embedding still has real text/context_header worth finding by keyword).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from codegraph.core.errors import InvariantError

logger = logging.getLogger(__name__)

# Реальные подстроки ошибок FalkorDB v4.18.11 при повторном создании DDL-объектов —
# захвачены эмпирически на живом контейнере codegraph-falkordb (см. m1b-task-2-report.md):
#   - CREATE INDEX дважды на том же (label, property):
#       redis.exceptions.ResponseError: "Attribute 'id' is already indexed"
#   - GRAPH.CONSTRAINT CREATE дважды:
#       redis.exceptions.ResponseError: 'Constraint already exists'
#   - db.idx.fulltext.createNodeIndex дважды на том же (label, property) (M2 T8,
#     захвачено эмпирически тем же способом): та же формулировка, что у range-
#     индекса --
#       redis.exceptions.ResponseError: "Attribute 'name' is already indexed"
#     -- уже покрыта маркером "already indexed" ниже, отдельный маркер не нужен.
# Подстрочный (регистронезависимый) матч по этим маркерам даёт идемпотентность;
# всё остальное — например 'missing supporting exact-match index' при нарушении
# порядка индекс-до-констрейнта — настоящая ошибка и пробрасывается.
_IGNORABLE_DDL_MARKERS = ("already indexed", "already exists", "constraint already")


def ensure_schema(db, graph_name: str, dim: int | None = None) -> None:
    """Идемпотентно создаёт индексы и UNIQUE-констрейнт схемы M1 на графе graph_name.

    Порядок важен: индекс на Sym.id создаётся ДО GRAPH.CONSTRAINT CREATE — FalkorDB
    требует уже существующий exact-match индекс на свойстве до наложения UNIQUE
    constraint (иначе 'missing supporting exact-match index'); паттерн доказан
    в doctor._constraint probe.

    `dim` (M3 T6): dimension of the Chunk.embedding vector index -- `CREATE VECTOR
    INDEX` needs a fixed dimension up front (doctor.run_store_probes' own
    `vector_index_cosine` probe proved this exact Cypher shape live), so the index is
    only created when `dim` is not None (`pipeline.load.load_graph` passes the
    embed_model's dimension read back from staging meta, or None when no chunk in this
    graph actually carries a live embedding -- e.g. embedder skipped/`--no-embed` --
    see that module's `_embed_meta`). The Chunk fulltext index (text, context_header)
    is unconditional -- it doesn't need a dimension, and a chunk's text/context_header
    are worth finding by keyword even without an embedding.

    `dim`, when given, MUST be a positive int -- checked here (raising `InvariantError`)
    rather than left to surface as an opaque FalkorDB-side error from the f-string-
    interpolated `OPTIONS {dimension: <dim>, ...}` below (a 0/negative `dim` produces
    syntactically-valid-but-semantically-invalid Cypher that isn't covered by any
    `_IGNORABLE_DDL_MARKERS` entry, so it would otherwise propagate uncaught with no
    diagnostic pointing at the real cause). Not reachable via the one real production
    path today (`pipeline.load._embed_meta` only ever produces a real embedder's own
    `.dim`, and `FakeEmbedder` itself already rejects `dim<=0` at construction) but
    `Embedder` is a structural Protocol with no runtime-enforced positivity guarantee
    for every implementation, and `dim` here ultimately traces back to a plain TEXT
    value in staging's meta table -- a defensive, fail-closed check costs nothing.
    """
    if dim is not None and dim <= 0:
        raise InvariantError(f"ensure_schema: dim must be a positive int, got {dim!r}")
    g = db.select_graph(graph_name)
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.id)"))
    _swallow_ddl_errors(lambda: db.connection.execute_command(
        "GRAPH.CONSTRAINT", "CREATE", graph_name,
        "UNIQUE", "NODE", "Sym", "PROPERTIES", "1", "id",
    ))
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.qualified_name)"))
    _swallow_ddl_errors(lambda: g.query("CREATE INDEX FOR (n:Sym) ON (n.service)"))
    # M2 T8: fulltext index over Sym(name, qualified_name, docstring) --
    # store.search_fulltext's only index (GraphQuery.find_entrypoint). Order
    # relative to the range indexes/constraint above doesn't matter (no
    # supporting-index dependency like the id-constraint has); placed last simply
    # because it's the newest addition.
    _swallow_ddl_errors(lambda: g.query(
        "CALL db.idx.fulltext.createNodeIndex('Sym', 'name', 'qualified_name', 'docstring')"
    ))
    # M3 T6: Chunk vector index (cosine, only when dim is known) + fulltext index
    # (text, context_header) over Chunk -- store.search_vector_chunks/
    # search_text_chunks (T7) read these.
    if dim is not None:
        _swallow_ddl_errors(lambda: g.query(
            f"CREATE VECTOR INDEX FOR (c:Chunk) ON (c.embedding) "
            f"OPTIONS {{dimension: {dim}, similarityFunction: 'cosine'}}"
        ))
    _swallow_ddl_errors(lambda: g.query(
        "CALL db.idx.fulltext.createNodeIndex('Chunk', 'text', 'context_header')"
    ))


def _swallow_ddl_errors(fn: Callable[[], object]) -> None:
    try:
        fn()
    except Exception as e:
        msg = str(e).lower()
        if not any(marker in msg for marker in _IGNORABLE_DDL_MARKERS):
            raise
        logger.info("ensure_schema: already applied, skipping (%s)", e)
