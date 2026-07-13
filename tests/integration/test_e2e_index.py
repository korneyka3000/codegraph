"""E2E: CLI `index` -> `stats` over the real pipeline (real scip-python + live
FalkorDB), Step 1 брифа m1b-task-6. Markers: scip (npx/network) + falkordb (docker
compose up -d).

Индексируемая копия фикстуры, не сама fixtures/services/document_management:
zero-config кладёт `.codegraph/` (staging.db, scip-кэш, report.json) РЯДОМ с
индексируемым таргетом (см. cli._workspace_dir) -- если бы мы индексировали
фикстуру напрямую, `.codegraph/` осел бы внутри репозитория и пережил бы упавший
между `index` и cleanup тест (ассерты выше `finally` ничем не защищены). Копия в
tmp_path чище: она удаляется вместе с tmp_path автоматически, никакого отдельного
cleanup для .codegraph не нужно -- остаётся только удалить граф FalkorDB (не
файловый ресурс, чистим явно в finally).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import connect

pytestmark = [pytest.mark.scip, pytest.mark.falkordb]

runner = CliRunner()
FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"
GRAPH = "__e2e_t6__"


def _cleanup_graph() -> None:
    db = connect(FalkorDBConfig())
    for name in (GRAPH, f"{GRAPH}__build"):
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # possibly never created, or already renamed away by swap_in


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_index_then_stats_over_real_pipeline(tmp_path):
    target = tmp_path / "document_management"
    shutil.copytree(FIXTURE, target)
    # scip-python 0.6.6 crashes (`TypeError: Cannot read properties of undefined
    # (reading 'indexOf')` in ScipSymbol.ts normalizeNameOrVersion, while building the
    # __init__.py module-init symbol) when it can't find ANY pyproject.toml walking up
    # from the indexed directory -- empirically confirmed by reproducing it against
    # this exact fixture copied to an ancestor-less tmp dir, and confirming a minimal
    # pyproject.toml here makes it disappear (see m1b-task-6-report.md). The original
    # fixtures/services/document_management "works" only by accident, because it is
    # nested inside this repo's own root pyproject.toml. --project-name (svc.name)
    # still governs the SCIP "package" field regardless of this file's `name`
    # (verified directly against the emitted .scip), so it does not need to match.
    (target / "pyproject.toml").write_text(
        '[project]\nname = "document-management"\nversion = "0.1.0"\n'
    )

    try:
        result = runner.invoke(app, ["index", str(target), "--graph", GRAPH])
        assert result.exit_code == 0, result.output

        report_path = target / ".codegraph" / "report.json"
        assert report_path.exists()
        report = json.loads(report_path.read_text())
        assert report["totals"]["calls_joined"] >= 6

        stats_result = runner.invoke(app, ["stats", str(target), "--graph", GRAPH])
        assert stats_result.exit_code == 0, stats_result.output
        assert "Function" in stats_result.output
        assert "CALLS" in stats_result.output
    finally:
        _cleanup_graph()

    # regression guard: the original fixture tree must stay untouched by this test.
    assert not (FIXTURE / ".codegraph").exists()
