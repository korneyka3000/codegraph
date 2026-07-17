from pathlib import Path

from typer.testing import CliRunner

import codegraph
from codegraph import cli
from codegraph.cli import app

runner = CliRunner()
FIXTURES_WS = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"
REPO_ROOT = Path(__file__).parents[2]


def test_index_dry_run_lists_stages_and_services():
    result = runner.invoke(app, ["index", str(FIXTURES_WS), "--dry-run"])
    assert result.exit_code == 0, result.output
    for stage in ("S1", "S5", "S10"):
        assert stage in result.output
    assert "orders-api" in result.output
    assert "kyc-worker" in result.output


# test_index_without_dry_run_not_implemented (M0-era: asserted exit_code==2 + "M1" in
# output) removed in M1b Task 6 -- `index` without --dry-run now does a REAL full
# pipeline run (scan/resolve/extract/join/load/report), which needs a live FalkorDB
# and either network+npx or the degraded heuristic fallback; that contract is no
# longer expressible as a fast, hermetic assertion in this file. Real-pipeline
# coverage moved to tests/unit/test_cli_m1b.py (monkeypatched analyze_service/
# load_graph/FalkorStore) and tests/integration/test_e2e_index.py (markers scip +
# falkordb, real pipeline on a tmp_path fixture copy). Running the old assertion
# as-is against this repo's live FalkorDB actually executed the full pipeline
# against fixtures/workspace.yaml -- leaving `.codegraph/` inside fixtures/ and a
# stray "fixtures" graph on the shared FalkorDB instance; both were cleaned up by
# hand while diagnosing this (see m1b-task-6-report.md).


def test_init_writes_template_and_refuses_overwrite(tmp_path):
    result = runner.invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0
    cfg = tmp_path / "codegraph.yaml"
    assert cfg.exists() and "services:" in cfg.read_text()
    again = runner.invoke(app, ["init", str(tmp_path)])
    assert again.exit_code == 1
    assert "exists" in again.output


def test_init_defaults_to_cwd(tmp_path, monkeypatch):
    # Регрессия: Path.cwd() должен вычисляться при вызове команды, а не при
    # импорте модуля (typer вычисляет default-выражения один раз при импорте).
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "codegraph.yaml").exists()


# -- M4 T2: TEMPLATE must be wheel-safe (importlib.resources, not a repo-root-escaping
# Path(__file__).parent.parent.parent) -- a wheel install has no repo checkout beside
# it, so the old construction would raise FileNotFoundError there. These tests can't
# build+install a real wheel, but they pin the structural property that actually
# guarantees wheel-safety: TEMPLATE must resolve INSIDE the codegraph package tree.


def test_template_lives_inside_the_package_not_via_repo_root_escape():
    package_root = str(Path(codegraph.__file__).parent)
    assert str(cli.TEMPLATE).startswith(package_root)


def test_root_and_packaged_example_yaml_copies_are_byte_identical():
    """Root-level codegraph.example.yaml (kept for humans browsing the repo) must
    never drift from the packaged copy under src/codegraph/data/ that `codegraph
    init`/TEMPLATE actually reads (the packaged copy is the source of truth) -- this
    only catches the two copies diverging, it doesn't enforce which one to edit."""
    root_copy = REPO_ROOT / "codegraph.example.yaml"
    packaged_copy = REPO_ROOT / "src" / "codegraph" / "data" / "codegraph.example.yaml"
    assert root_copy.read_bytes() == packaged_copy.read_bytes()


# test_stub_commands_exit_2 (M2-era: asserted `codegraph eval` alone exited 2 with
# "planned for M2" -- stats/load left this same list in M1b Task 6, serve left it in
# M1b Task 7, trace left it in M2 Task 8) removed in M3 Task 8 -- `eval` is now a real
# command GROUP (`codegraph eval retrieval ...`, `_stub`'s only remaining caller,
# itself removed), not a flat stub; `codegraph eval` alone now prints group help and
# exits 0 (`no_args_is_help=True`, same convention as the top-level `app`), not 2. Real
# coverage: tests/unit/test_cli_eval.py (monkeypatched, this file's sibling) and
# tests/eval/test_m3_gate.py (real scip+falkordb+emb gate).
