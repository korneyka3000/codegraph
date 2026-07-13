from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app

runner = CliRunner()
FIXTURES_WS = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"


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


def test_stub_commands_exit_2():
    # stats/load left this list in M1b Task 6 -- both are real commands now (see
    # tests/unit/test_cli_m1b.py); trace/eval stay M2, serve stays a stub until Task 7.
    for cmd, milestone in [("trace", "M2"), ("serve", "M1"), ("eval", "M2")]:
        result = runner.invoke(app, [cmd])
        assert result.exit_code == 2, cmd
        assert milestone in result.output
