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


def test_index_without_dry_run_not_implemented():
    result = runner.invoke(app, ["index", str(FIXTURES_WS)])
    assert result.exit_code == 2
    assert "M1" in result.output


def test_init_writes_template_and_refuses_overwrite(tmp_path):
    result = runner.invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0
    cfg = tmp_path / "codegraph.yaml"
    assert cfg.exists() and "services:" in cfg.read_text()
    again = runner.invoke(app, ["init", str(tmp_path)])
    assert again.exit_code == 1
    assert "exists" in again.output


def test_stub_commands_exit_2():
    for cmd, milestone in [("stats", "M1"), ("trace", "M2"), ("serve", "M1"), ("eval", "M2")]:
        result = runner.invoke(app, [cmd])
        assert result.exit_code == 2, cmd
        assert milestone in result.output
