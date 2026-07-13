from typer.testing import CliRunner

from codegraph.cli import app

runner = CliRunner()


def test_index_bad_target_prints_one_liner_not_traceback(tmp_path):
    cfg = tmp_path / "codegraph.yaml"
    cfg.write_text("version: 1\ngraph_name: x\nservices:\n  - name: a\n    path: ./missing\n")
    result = runner.invoke(app, ["index", str(cfg), "--dry-run"])
    assert result.exit_code == 1
    assert "config error" in result.output
    assert "Traceback" not in result.output


def test_doctor_bad_config_prints_one_liner(tmp_path):
    cfg = tmp_path / "codegraph.yaml"
    cfg.write_text("version: 1\ngraph_name: x\nservices:\n  - name: a\n    path: ./missing\n")
    result = runner.invoke(app, ["doctor", "--config", str(cfg), "--skip-store"])
    assert result.exit_code == 1
    assert "config error" in result.output
    assert "Traceback" not in result.output


def test_stages_moved_to_pipeline():
    from codegraph.pipeline.stages import STAGES

    assert STAGES[0][0] == "S1" and STAGES[-1][0] == "S10"
    assert len(STAGES) == 10 and all(len(t) == 3 for t in STAGES)


def test_scip_version_single_source():
    from codegraph import constants, doctor

    assert doctor.SCIP_PYTHON_VERSION == constants.SCIP_PYTHON_VERSION
