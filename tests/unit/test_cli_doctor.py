"""M5 T7 (M3 backlog "no-index marker -> doctor probe"): `codegraph doctor`'s new
`--graph` option + conditional `chunk_vector_index` probe row -- CLI WIRING only
(whether/when the extra row gets appended, whether `--graph` reaches it, whether it's
skipped alongside `--skip-store` and whenever a capability probe already failed).
`check_chunk_vector_index`'s own logic (graph-absent/no-embedded-chunks/index-already-
present/warn) is unit-tested directly in test_doctor.py against a fake store, with no
CLI/typer involvement at all -- duplicating that here would test the same thing twice
through a slower path. Same monkeypatch-the-module-level-name technique as
test_cli_m1b.py/test_cli_chunk_embed.py: `codegraph.cli.run_store_probes`/`FalkorStore`/
`check_chunk_vector_index` are all imported by name into cli.py, so tests replace
exactly those names."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.doctor import CheckResult
from codegraph.stores.falkordb.connection import StoreError

runner = CliRunner()


def _write_workspace(tmp_path: Path, graph_name: str = "wsgraph") -> Path:
    root = tmp_path / "ws"
    root.mkdir()
    (root / "codegraph.yaml").write_text(f"version: 1\ngraph_name: {graph_name}\nservices: []\n")
    return root


_ALL_OK_PROBES = [CheckResult("ping", True), CheckResult("multi_label", True)]


class _FakeStore:
    def __init__(self, cfg, name):
        self.cfg = cfg
        self.name = name


def test_doctor_appends_vector_index_warning_row_and_flips_exit_code(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")
    captured: dict = {}

    def fake_check(store):
        captured["store"] = store
        return CheckResult("chunk_vector_index", False, "needs re-index")

    monkeypatch.setattr("codegraph.cli.run_store_probes", lambda db_factory: list(_ALL_OK_PROBES))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStore)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", fake_check)

    result = runner.invoke(app, ["doctor", "--config", str(root)])
    assert result.exit_code == 1  # ok=False row flips the overall exit code
    assert "chunk_vector_index" in result.output
    assert "needs re-index" in result.output
    assert captured["store"].name == "wsgraph"  # config's own graph_name, no --graph override


def test_doctor_no_extra_row_when_vector_check_returns_none(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")

    monkeypatch.setattr("codegraph.cli.run_store_probes", lambda db_factory: list(_ALL_OK_PROBES))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStore)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", lambda store: None)

    result = runner.invoke(app, ["doctor", "--config", str(root)])
    assert result.exit_code == 0
    assert "chunk_vector_index" not in result.output


def test_doctor_graph_option_overrides_config_graph_name_for_vector_probe(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")
    captured: dict = {}

    def fake_check(store):
        captured["store"] = store
        return None

    monkeypatch.setattr("codegraph.cli.run_store_probes", lambda db_factory: list(_ALL_OK_PROBES))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStore)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", fake_check)

    result = runner.invoke(app, ["doctor", "--config", str(root), "--graph", "override123"])
    assert result.exit_code == 0, result.output
    assert captured["store"].name == "override123"


def test_doctor_skips_vector_probe_when_a_capability_probe_already_failed(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")

    def _boom(store):
        raise AssertionError(
            "check_chunk_vector_index must not run once a capability probe already failed"
        )

    monkeypatch.setattr(
        "codegraph.cli.run_store_probes",
        lambda db_factory: [CheckResult("ping", False, "connection refused")],
    )
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStore)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", _boom)

    result = runner.invoke(app, ["doctor", "--config", str(root)])
    assert result.exit_code == 1
    assert "ping" in result.output.lower()


def test_doctor_vector_probe_store_error_becomes_failure_row_not_traceback(
    tmp_path, monkeypatch
):
    """M5 T7 review fix (Important): FalkorDB dropping between the (green) capability
    probes and the vector-index probe raises a bare StoreError out of
    check_chunk_vector_index's raw()/graph_exists() passthroughs -- pre-fix that
    propagated as a raw traceback out of doctor() (the justifying comment wrongly
    claimed parity with stats(), which actually wraps its store calls in
    _store_guard). Post-fix: the transient failure becomes one more FAILED row in the
    SAME falkordb table (doctor._probe's own per-probe isolation discipline -- NOT
    _store_guard's red-line-and-exit, which would discard the already-computed green
    capability rows), flipping the exit code through the normal `ok` fold."""
    root = _write_workspace(tmp_path, graph_name="wsgraph")

    def _transient(store):
        raise StoreError("Connection reset by peer")

    monkeypatch.setattr("codegraph.cli.run_store_probes", lambda db_factory: list(_ALL_OK_PROBES))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStore)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", _transient)

    result = runner.invoke(app, ["doctor", "--config", str(root)])
    assert result.exit_code == 1
    assert not isinstance(result.exception, StoreError)  # handled, never propagated
    assert "chunk_vector_index" in result.output  # rendered as a doctor failure ROW...
    assert "Connection reset by peer" in result.output  # ...carrying the real cause
    assert "ping" in result.output.lower()  # green capability rows still rendered too
    assert "Traceback" not in result.output


def test_doctor_skip_store_flag_never_touches_store_probes_or_vector_probe(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")

    def _boom(*a, **kw):
        raise AssertionError("must not be called under --skip-store")

    monkeypatch.setattr("codegraph.cli.run_store_probes", _boom)
    monkeypatch.setattr("codegraph.cli.check_chunk_vector_index", _boom)

    result = runner.invoke(app, ["doctor", "--config", str(root), "--skip-store"])
    assert result.exit_code == 0, result.output
