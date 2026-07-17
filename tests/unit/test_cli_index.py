"""M4 T7: `codegraph index --incremental` CLI orchestration -- per-service scan+diff
BEFORE analyze, the config-fingerprint read/compare/write-back, and the `changed_files`
dict threaded into `run_chunk_embed`. Same monkeypatch-the-module-level-name technique
as test_cli_m1b.py/test_cli_chunk_embed.py (analyze_service/run_chunk_embed), extended
with fakes that understand the new incremental/prior_delta/fingerprint_ok kwargs.

Real `scan_service`/`service_delta`/`config_fingerprint` run against real tmp_path
service directories (cheap, pure filesystem+hashing, no need to fake) -- only
analyze_service/run_chunk_embed/link_workspace/load_graph/FalkorStore are faked, same
boundary as every other cli-wiring test file. `Staging` is real too (SQLite-backed):
scenarios that need a specific delta/fingerprint outcome pre-seed staging.db directly
via the SAME production functions (`config_fingerprint`, `scan_service`) the CLI itself
will use, so seeding can never silently drift from what the code under test computes.
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.pipeline.diff import config_fingerprint
from codegraph.pipeline.scan import scan_service
from codegraph.stores.staging import Staging

runner = CliRunner()


def _write_workspace(tmp_path: Path, graph_name: str = "wsgraph") -> Path:
    """One real service (`svc0`) with two real .py files -- `scan_service` needs an
    actual filesystem tree to walk (unlike `analyze_service` itself, which this
    module always fakes)."""
    root = tmp_path / "ws"
    svc_dir = root / "svc0"
    (svc_dir / "app").mkdir(parents=True)
    (svc_dir / "app" / "__init__.py").write_text("")
    (svc_dir / "app" / "main.py").write_text("def f():\n    pass\n")
    (svc_dir / "app" / "other.py").write_text("def g():\n    pass\n")
    root.mkdir(exist_ok=True)
    (root / "codegraph.yaml").write_text(
        f"version: 1\ngraph_name: {graph_name}\nservices:\n"
        "  - name: svc0\n    path: ./svc0\n"
    )
    return root


def _seed_baseline(
    root: Path, *, mutate_main: bool = False, fingerprint: str | None = "__use_real__"
):
    """Pre-populates staging.db with a snapshot matching (or deliberately NOT
    matching) the CURRENT on-disk tree, using the exact production functions
    (`load_workspace`/`effective_idioms`/`config_fingerprint`/`scan_service`) the CLI
    itself will use -- so a test can never accidentally seed a baseline that silently
    drifts from what `_analyze_services` actually computes.

    `mutate_main=True` edits app/main.py's content AFTER the scan-and-seed step, so a
    LATER real scan (inside the CLI invocation) sees a changed sha256 for it --
    produces a non-empty `service_delta` (mode="incremental" territory) while
    fingerprint stays valid.

    `fingerprint`: "__use_real__" (default) seeds the CORRECT config_fingerprint
    (fingerprint_ok=True downstream); any other str seeds a deliberately WRONG one
    (fingerprint_ok=False -- "fingerprint mismatch"); None skips seeding the
    svc_fingerprint meta key entirely (fingerprint_ok=False via "never run before" --
    "first run").
    """
    cfg = load_workspace(root)
    svc = next(s for s in cfg.services if s.name == "svc0")
    active_idioms = frozenset(cfg.builtin_idioms)
    idioms = effective_idioms(cfg, svc)
    scanned, _ = scan_service(svc.path, svc.exclude)

    staging = Staging(root / ".codegraph" / "staging.db")
    staging.add_files(svc.name, scanned)
    if fingerprint is not None:
        fp = (
            config_fingerprint(svc, idioms, active_idioms)
            if fingerprint == "__use_real__" else fingerprint
        )
        staging.set_meta(f"svc_fingerprint:{svc.name}", fp)
    staging.close()

    if mutate_main:
        (svc.path / "app" / "main.py").write_text("def f():\n    return 1\n")


# -- fakes --


def _analyze_spy(reports: dict[str, dict], recorded: list[dict]):
    def fn(
        svc, staging, cache_dir, runner=None, active_idioms=frozenset(), idioms=None,
        incremental=False, prior_delta=None, fingerprint_ok=True,
    ):
        recorded.append({
            "svc": svc.name, "incremental": incremental, "prior_delta": prior_delta,
            "fingerprint_ok": fingerprint_ok,
        })
        return dict(reports[svc.name])
    return fn


def _old_shaped_analyze_fake(recorded: list[dict] | None = None):
    """The pre-M4 fake signature (no incremental/prior_delta/fingerprint_ok params
    at all) -- a TypeError from a caller passing any of those unconditionally would
    mean the CLI broke the "byte-identical without --incremental" contract."""
    def fn(svc, staging, cache_dir, runner=None, active_idioms=frozenset(), idioms=None):
        if recorded is not None:
            recorded.append({"svc": svc.name})
        return dict(_FULL_REPORT)
    return fn


def _chunk_embed_spy(recorded: list):
    def fn(cfg, staging, embedder, changed_files=None):
        recorded.append(changed_files)
        return {
            "chunks_total": 0, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }
    return fn


def _fake_link_workspace(cfg, staging):
    return {
        "calls_http": 0, "calls_http_unresolved": 0,
        "next_segments": 0, "processes": 0, "marks": 0,
    }


def _fake_load_graph(staging, store_factory, graph_name):
    store_factory(f"{graph_name}__build")
    return {
        "nodes_written": 0, "nodes_written_by_label": {},
        "edges_written": 0, "edges_written_by_type": {},
        "edges_dropped_missing_endpoint": 0, "edges_dropped_by_type": {},
    }


class _FakeFalkorStore:
    def __init__(self, cfg, name):
        self.cfg = cfg
        self.name = name


def _patch_common(monkeypatch, analyze_fn, chunk_embed_fn):
    monkeypatch.setattr("codegraph.cli.analyze_service", analyze_fn)
    monkeypatch.setattr("codegraph.cli.link_workspace", _fake_link_workspace)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_fn)
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)


_FULL_REPORT = {
    "service": "svc0", "files": 2, "defs": 0, "refs": 0, "malformed_ranges": 0,
    "nodes": 1, "edges": 0, "imports_external": 0,
    "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0,
    "degraded": False, "reason": None, "from_cache": False, "mode": "full",
}
_SKIPPED_REPORT = {**_FULL_REPORT, "mode": "skipped"}
_INCREMENTAL_REPORT = {
    **_FULL_REPORT, "mode": "incremental", "stale_files": 1,
    "stale_relpaths": ("app/main.py",),
}


# -- --incremental absent: byte-identical to pre-M4 (no new kwargs leak through) --


def test_incremental_flag_off_analyze_service_call_shape_is_byte_identical(tmp_path, monkeypatch):
    """The OLD narrow fake signature (no incremental/prior_delta/fingerprint_ok
    params at all) must still work -- a TypeError here would mean the CLI started
    passing new kwargs unconditionally, breaking every pre-M4 cli test/caller."""
    root = _write_workspace(tmp_path)
    calls: list[dict] = []

    chunk_calls: list = []
    _patch_common(monkeypatch, _old_shaped_analyze_fake(calls), _chunk_embed_spy(chunk_calls))

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert [c["svc"] for c in calls] == ["svc0"]
    # run_chunk_embed called with exactly 3 positional args -> spy's changed_files
    # default (None) is what got recorded, never an (even empty) dict.
    assert chunk_calls == [None]


def test_incremental_flag_off_still_writes_fingerprint_for_a_later_incremental_run(
    tmp_path, monkeypatch
):
    """Global Constraint (M4 plan): a PLAIN `index` run (no --incremental) must ALSO
    write the fresh config fingerprint, so a LATER --incremental run has a real
    baseline instead of unconditionally treating it as "first run"."""
    root = _write_workspace(tmp_path)

    _patch_common(monkeypatch, _old_shaped_analyze_fake(), _chunk_embed_spy([]))
    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output

    cfg = load_workspace(root)
    svc = next(s for s in cfg.services if s.name == "svc0")
    expected_fp = config_fingerprint(
        svc, effective_idioms(cfg, svc), frozenset(cfg.builtin_idioms)
    )
    staging = Staging(root / ".codegraph" / "staging.db")
    assert staging.get_meta("svc_fingerprint:svc0") == expected_fp
    staging.close()


# -- --incremental, first run (no staging.db / no stored fingerprint) --


def test_incremental_first_run_analyzes_full_with_computed_fingerprint_ok_false(
    tmp_path, monkeypatch
):
    root = _write_workspace(tmp_path)  # no seeding at all -- brand new staging.db
    recorded: list[dict] = []
    analyze_fn = _analyze_spy({"svc0": _FULL_REPORT}, recorded)
    chunk_calls: list = []
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy(chunk_calls))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    assert len(recorded) == 1
    assert recorded[0]["incremental"] is False  # bypasses analyze_service's own branch
    assert recorded[0]["fingerprint_ok"] is False  # computed, never the permissive default

    report = json.loads((root / ".codegraph" / "report.json").read_text())
    assert report["services"][0]["mode"] == "full"
    assert report["services"][0]["reason"] == "first run"


def test_incremental_first_run_full_mode_changed_files_is_every_scanned_relpath(
    tmp_path, monkeypatch
):
    root = _write_workspace(tmp_path)
    chunk_calls: list = []
    analyze_fn = _analyze_spy({"svc0": _FULL_REPORT}, [])
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy(chunk_calls))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    assert len(chunk_calls) == 1
    # full mode -> begin_service wiped this service's ENTIRE chunks table -> every
    # currently-scanned file needs re-chunking, not just whatever happened to change.
    assert chunk_calls[0] == {"svc0": {"app/__init__.py", "app/main.py", "app/other.py"}}


# -- --incremental, config fingerprint mismatch (stale prior config) --


def test_incremental_fingerprint_mismatch_forces_full_with_mismatch_reason(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _seed_baseline(root, fingerprint="deliberately-wrong-fingerprint")
    recorded: list[dict] = []
    analyze_fn = _analyze_spy({"svc0": _FULL_REPORT}, recorded)
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy([]))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    assert recorded[0]["incremental"] is False
    assert recorded[0]["fingerprint_ok"] is False

    report = json.loads((root / ".codegraph" / "report.json").read_text())
    assert report["services"][0]["reason"] == "fingerprint mismatch"


# -- --incremental, matching fingerprint + no on-disk changes -> skip --


def test_incremental_skip_service_absent_from_changed_files(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _seed_baseline(root)  # real fingerprint, real matching file snapshot -> delta.empty
    recorded: list[dict] = []
    analyze_fn = _analyze_spy({"svc0": _SKIPPED_REPORT}, recorded)
    chunk_calls: list = []
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy(chunk_calls))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    assert recorded[0]["incremental"] is True
    assert recorded[0]["prior_delta"].empty is True
    assert recorded[0]["fingerprint_ok"] is True

    assert len(chunk_calls) == 1
    assert chunk_calls[0] == {}  # svc0 absent -- NOT {"svc0": set()}
    assert "svc0" not in chunk_calls[0]

    report = json.loads((root / ".codegraph" / "report.json").read_text())
    assert report["services"][0]["mode"] == "skipped"


def test_incremental_skip_does_not_change_stored_fingerprint(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _seed_baseline(root)
    analyze_fn = _analyze_spy({"svc0": _SKIPPED_REPORT}, [])
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy([]))

    staging = Staging(root / ".codegraph" / "staging.db")
    fp_before = staging.get_meta("svc_fingerprint:svc0")
    staging.close()

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    staging = Staging(root / ".codegraph" / "staging.db")
    assert staging.get_meta("svc_fingerprint:svc0") == fp_before
    staging.close()


# -- --incremental, matching fingerprint + a real on-disk change -> incremental --


def test_incremental_changed_service_uses_stale_relpaths_for_changed_files(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _seed_baseline(root, mutate_main=True)  # real fingerprint, app/main.py edited after
    recorded: list[dict] = []
    analyze_fn = _analyze_spy({"svc0": _INCREMENTAL_REPORT}, recorded)
    chunk_calls: list = []
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy(chunk_calls))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    assert recorded[0]["incremental"] is True
    assert recorded[0]["fingerprint_ok"] is True
    assert recorded[0]["prior_delta"].empty is False
    assert recorded[0]["prior_delta"].changed == ("app/main.py",)

    assert chunk_calls[0] == {"svc0": {"app/main.py"}}  # exactly stale_relpaths, not "all files"

    report = json.loads((root / ".codegraph" / "report.json").read_text())
    assert report["services"][0]["mode"] == "incremental"
    assert report["services"][0]["stale_files"] == 1


def test_incremental_non_skip_writes_fresh_fingerprint(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _seed_baseline(root, mutate_main=True)
    analyze_fn = _analyze_spy({"svc0": _INCREMENTAL_REPORT}, [])
    _patch_common(monkeypatch, analyze_fn, _chunk_embed_spy([]))

    result = runner.invoke(app, ["index", str(root), "--incremental"])
    assert result.exit_code == 0, result.output

    cfg = load_workspace(root)
    svc = next(s for s in cfg.services if s.name == "svc0")
    expected_fp = config_fingerprint(
        svc, effective_idioms(cfg, svc), frozenset(cfg.builtin_idioms)
    )
    staging = Staging(root / ".codegraph" / "staging.db")
    assert staging.get_meta("svc_fingerprint:svc0") == expected_fp
    staging.close()
