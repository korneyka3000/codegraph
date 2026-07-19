"""Диагностика окружения и возможностей FalkorDB (feature-probes)."""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass

from codegraph.config.models import ScipConfig
from codegraph.constants import SCIP_PYTHON_VERSION

PROBE_GRAPH = "__codegraph_probe__"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


def _cmd_version(name: str, args: list[str], timeout: int = 30) -> CheckResult:
    exe = shutil.which(args[0])
    if exe is None:
        return CheckResult(name, False, f"{args[0]} not found in PATH")
    try:
        out = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout, check=True
        )
        return CheckResult(name, True, (out.stdout.strip().splitlines() or ["<no stdout>"])[0])
    except (subprocess.SubprocessError, OSError) as e:
        return CheckResult(name, False, str(e))


def run_env_checks(scip: ScipConfig, probe_scip: bool = False) -> list[CheckResult]:
    results = [
        CheckResult(
            "python",
            sys.version_info >= (3, 12),
            f"{sys.version_info.major}.{sys.version_info.minor}",
        ),
        _cmd_version("node", ["node", "--version"]),
        _cmd_version("npx", ["npx", "--version"]),
    ]
    if probe_scip:
        results.append(
            _cmd_version(
                "scip-python",
                ["npx", "--yes", f"@sourcegraph/scip-python@{SCIP_PYTHON_VERSION}",
                 "--version"],
                timeout=180,
            )
        )
    return results


def _probe(name: str, fn: Callable[[], object]) -> CheckResult:
    try:
        fn()
        return CheckResult(name, True)
    except Exception as e:  # probes должны быть изолированы друг от друга
        return CheckResult(name, False, str(e))


def run_store_probes(db_factory: Callable[[], object]) -> list[CheckResult]:
    try:
        db = db_factory()
        db.connection.ping()
    except Exception as e:
        return [CheckResult("ping", False, str(e))]

    results = [CheckResult("ping", True)]
    g = db.select_graph(PROBE_GRAPH)
    try:
        results.append(_probe(
            "multi_label",
            lambda: g.query("MERGE (n:A:B {id: 'p'}) RETURN labels(n)"),
        ))
        results.append(_probe(
            "set_plus_eq",
            lambda: g.query("MERGE (n:A {id: 'q'}) SET n += {k: 1} RETURN n.k"),
        ))

        def _constraint():
            # FalkorDB требует существующий exact-match индекс на свойстве
            # ДО создания UNIQUE constraint (иначе:
            # "missing supporting exact-match index"), см.
            # https://docs.falkordb.com/commands/graph.constraint-create.html.
            # Проверено эмпирически на пиненном образе falkordb:v4.18.11.
            g.query("CREATE INDEX FOR (n:A) ON (n.id)")
            db.connection.execute_command(
                "GRAPH.CONSTRAINT", "CREATE", PROBE_GRAPH,
                "UNIQUE", "NODE", "A", "PROPERTIES", "1", "id",
            )
            db.connection.execute_command(
                "GRAPH.CONSTRAINT", "DROP", PROBE_GRAPH,
                "UNIQUE", "NODE", "A", "PROPERTIES", "1", "id",
            )

        results.append(_probe("unique_constraint", _constraint))
        results.append(_probe(
            "vector_index_cosine",
            lambda: g.query(
                "CREATE VECTOR INDEX FOR (c:P) ON (c.v) "
                "OPTIONS {dimension: 4, similarityFunction: 'cosine'}"
            ),
        ))
        results.append(_probe(
            "fulltext",
            lambda: g.query("CALL db.idx.fulltext.createNodeIndex('P', 't')"),
        ))
    finally:
        try:
            g.delete()
        except Exception:
            pass
    return results


def _has_chunk_vector_index(index_rows: list) -> bool:
    """`index_rows` -- FalkorDB's own `CALL db.indexes()` result_set (live-verified
    against v4.18.11): each row is `(label, properties, types, options, language,
    stopwords, entitytype, status, info)`, where `types` is a dict keyed by indexed
    property name, each value a list of index-kind strings ('RANGE', 'FULLTEXT',
    'VECTOR', ...). A Chunk.embedding vector index (`ddl.ensure_schema`'s own
    dim-gated `CREATE VECTOR INDEX FOR (c:Chunk) ON (c.embedding)`) shows up as a row
    with `label == "Chunk"` and `"VECTOR" in types["embedding"]`."""
    return any(
        row[0] == "Chunk" and "VECTOR" in row[2].get("embedding", [])
        for row in index_rows
    )


def check_chunk_vector_index(store) -> CheckResult | None:
    """M3 backlog ("no-index marker -> doctor probe"): flags a graph that has REAL,
    live Chunk embeddings (some `Chunk.embedding IS NOT NULL`) but no vector index
    covering them. `stores/falkordb/store.py`'s own `search_vector_chunks` degrades
    that exact combination to a silent `[]` (see its `_NO_VECTOR_INDEX_MARKER` catch)
    rather than raising -- correct there, an absent index must not crash a search --
    but it also means a user gets no error at all, just empty/degraded results, with
    nothing pointing at the real cause. Typically means `ensure_schema` never ran with
    a real `dim` for this graph's CURRENT data: an index manually dropped after a
    normal `codegraph index`/`codegraph load` run, or Chunk nodes written some other
    way that bypassed `pipeline/load.py`'s own paired `_embed_meta` +
    `ensure_schema(dim=...)` call (see also `pipeline.chunk_embed.run`'s own M5 T7
    `has_live_embeddings`-gated Meta write -- the sibling fix that keeps THAT half of
    the same "does Meta/the index still match what's actually in the graph" question
    honest at write time; this probe catches it after the fact, at doctor time).

    Returns `None` -- nothing to show, nothing actionable -- for every case OTHER than
    that exact combination: the graph doesn't exist yet (`doctor` may run before the
    first `codegraph index`), it exists but no Chunk anywhere carries a live embedding
    (a workspace that has only ever used `--no-embed`, or hasn't reached S8 at all --
    a missing index is EXPECTED here, not a problem), or a vector index already covers
    Chunk.embedding (the healthy case). `store.graph_exists()` is checked FIRST, and
    this function returns immediately when it's False, before any `store.raw(...)`
    call -- same auto-vivify avoidance as `FalkorStore.graph_exists`'s own docstring
    (a bare `GRAPH.QUERY` against a graph name that doesn't exist yet creates an empty
    graph key as a side effect, live-verified in T6).

    Plain-`CheckResult`-return convention, no rich/console dependency -- same as
    `run_env_checks`/`run_store_probes` above -- so this is unit-testable with a fake
    store (`graph_exists`/`raw` only, no real FalkorDB needed). Only the CLI's own
    `doctor()` command decides whether/where to render a non-None result."""
    if not store.graph_exists():
        return None
    has_embedded = store.raw(
        "MATCH (c:Chunk) WHERE c.embedding IS NOT NULL RETURN c LIMIT 1"
    ).result_set
    if not has_embedded:
        return None
    index_rows = store.raw("CALL db.indexes()").result_set
    if _has_chunk_vector_index(index_rows):
        return None
    return CheckResult(
        "chunk_vector_index", False,
        f"graph {store.graph_name!r} has Chunk nodes with live embeddings but no "
        "vector index on Chunk.embedding -- vector search will silently return no "
        "matches; re-run 'codegraph index' (or 'codegraph load' against the existing "
        "staging) to rebuild it",
    )
