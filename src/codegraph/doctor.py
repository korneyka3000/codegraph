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
