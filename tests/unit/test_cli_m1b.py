"""CLI `index`/`load`/`stats`/`serve` wired to the real pipeline/MCP server (M1b Tasks
6-7): argument proводка (staging path, graph name, store_factory, build_server args)
через monkeypatched analyze_service/load_graph/FalkorStore/build_server -- без
реального SCIP/FalkorDB/MCP-клиента (это делает tests/integration/test_e2e_index.py и
tests/integration/test_mcp_contract.py, markers scip+falkordb).
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreError
from codegraph.stores.staging import Staging

runner = CliRunner()


def test_store_error_is_redis_error_alias():
    """Граница импортов: cli ловит redis-ошибки ТОЛЬКО через алиас
    connection.StoreError (cli не импортирует redis напрямую) -- алиас обязан
    оставаться идентичен redis.exceptions.RedisError, базовому классу всех
    ошибок redis-py (включая ConnectionError/TimeoutError)."""
    import redis.exceptions

    assert StoreError is redis.exceptions.RedisError


def _write_workspace(tmp_path: Path, n_services: int = 1, graph_name: str = "wsgraph") -> Path:
    """Явный codegraph.yaml с n_services сервисами-каталогами (пустыми -- analyze_service
    монки-патчится, содержимое каталогов пайплайну не нужно)."""
    root = tmp_path / "ws"
    lines = ["version: 1", f"graph_name: {graph_name}", "services:"]
    for i in range(n_services):
        (root / f"svc{i}").mkdir(parents=True)
        lines.append(f"  - name: svc{i}")
        lines.append(f"    path: ./svc{i}")
    root.mkdir(exist_ok=True)
    (root / "codegraph.yaml").write_text("\n".join(lines) + "\n")
    return root


def _fake_analyze_service(recorded: list[dict]):
    def fn(svc, staging, cache_dir, runner=None):
        recorded.append({"svc": svc, "staging": staging, "cache_dir": cache_dir})
        return {
            "service": svc.name, "files": 1, "defs": 0, "refs": 0, "malformed_ranges": 0,
            "nodes": 1, "edges": 0, "imports_external": 0,
            "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0,
            "degraded": False, "reason": None, "from_cache": False,
        }
    return fn


def _fake_load_graph(recorded: list[dict]):
    def fn(staging, store_factory, graph_name):
        store = store_factory(f"{graph_name}__build")
        recorded.append({
            "staging": staging, "graph_name": graph_name,
            "store_cfg": store.cfg, "store_name": store.name,
        })
        return {
            "nodes_written": 0, "nodes_written_by_label": {},
            "edges_written": 0, "edges_written_by_type": {},
            "edges_dropped_missing_endpoint": 0, "edges_dropped_by_type": {},
        }
    return fn


class _FakeFalkorStore:
    """store_factory(name) должен строить FalkorStore(cfg.storage.falkordb, name);
    эта подмена записывает, с какими аргументами её сконструировали, без сети."""

    def __init__(self, cfg, name):
        self.cfg = cfg
        self.name = name


# -- index: полный прогон (без --dry-run) --


def test_index_calls_analyze_service_per_service_and_load_graph_with_config_graph_name(
    tmp_path, monkeypatch
):
    root = _write_workspace(tmp_path, n_services=2, graph_name="wsgraph")
    analyze_calls: list[dict] = []
    load_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service(analyze_calls))
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph(load_calls))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output

    assert [c["svc"].name for c in analyze_calls] == ["svc0", "svc1"]
    for c in analyze_calls:
        assert c["cache_dir"] == root / ".codegraph" / "scip"
        assert isinstance(c["staging"], Staging)

    assert len(load_calls) == 1
    assert load_calls[0]["graph_name"] == "wsgraph"
    assert load_calls[0]["store_name"] == "wsgraph__build"
    assert load_calls[0]["store_cfg"] == FalkorDBConfig()


def test_index_graph_option_overrides_config_graph_name(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    load_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph(load_calls))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root), "--graph", "override123"])
    assert result.exit_code == 0, result.output
    assert load_calls[0]["graph_name"] == "override123"
    assert load_calls[0]["store_name"] == "override123__build"


def test_index_writes_report_json_from_build_report(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=2, graph_name="wsgraph")
    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph([]))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output

    report_path = root / ".codegraph" / "report.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text())
    assert report["totals"]["nodes"] == 2  # 1 per fake-analyzed service x 2 services
    assert len(report["services"]) == 2


def test_index_degraded_service_still_exits_zero_with_warning(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def fake_analyze(svc, staging, cache_dir, runner=None):
        return {
            "service": svc.name, "files": 1, "defs": 0, "refs": 0, "malformed_ranges": 0,
            "nodes": 1, "edges": 0, "imports_external": 0,
            "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0,
            "degraded": True, "reason": "scip-python timeout", "from_cache": False,
        }

    monkeypatch.setattr("codegraph.cli.analyze_service", fake_analyze)
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph([]))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert "degraded" in result.output.lower()
    assert "scip-python timeout" in result.output


def test_index_zero_config_places_codegraph_dir_at_target_root_not_cwd(tmp_path, monkeypatch):
    target = tmp_path / "zerorepo"
    target.mkdir()
    other_cwd = tmp_path / "elsewhere"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)

    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph([]))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(target)])
    assert result.exit_code == 0, result.output
    assert (target / ".codegraph" / "report.json").exists()
    assert not (other_cwd / ".codegraph").exists()


def test_index_accepts_explicit_config_file_path_target(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph([]))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root / "codegraph.yaml")])
    assert result.exit_code == 0, result.output
    assert (root / ".codegraph" / "report.json").exists()


def test_index_dry_run_does_not_touch_codegraph_dir_or_call_pipeline(tmp_path, monkeypatch):
    # regression guard: --dry-run must stay side-effect-free (no staging/report/pipeline calls).
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def _boom(*a, **kw):
        raise AssertionError("must not be called under --dry-run")

    monkeypatch.setattr("codegraph.cli.analyze_service", _boom)
    monkeypatch.setattr("codegraph.cli.load_graph", _boom)

    result = runner.invoke(app, ["index", str(root), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert not (root / ".codegraph").exists()


def test_index_store_unreachable_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def unreachable_load_graph(staging, store_factory, graph_name):
        raise StoreError("Connection refused")

    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", unreachable_load_graph)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


def test_index_store_unreachable_message_with_brackets_renders_literally(tmp_path, monkeypatch):
    # live-verified bug: _store_guard's `console.print(f"[red]falkordb unreachable:[/]
    # {e}")` treated any "["-bearing exception text as rich markup -- escape() must
    # make it survive verbatim instead of crashing or being silently eaten.
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def unreachable_load_graph(staging, store_factory, graph_name):
        raise StoreError("Connection refused [errno=111]")

    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service([]))
    monkeypatch.setattr("codegraph.cli.load_graph", unreachable_load_graph)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 1
    assert "[errno=111]" in result.output
    assert "Traceback" not in result.output


# -- load: только load_graph из существующего staging --


def test_load_missing_staging_db_is_red_error_exit_1(tmp_path):
    root = _write_workspace(tmp_path, n_services=1)
    result = runner.invoke(app, ["load", str(root)])
    assert result.exit_code == 1
    assert "staging" in result.output.lower()
    assert "Traceback" not in result.output


def test_load_wires_existing_staging_into_load_graph(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    staging_path = root / ".codegraph" / "staging.db"
    Staging(staging_path).close()  # pre-created by a prior `index` run

    load_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph(load_calls))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["load", str(root)])
    assert result.exit_code == 0, result.output
    assert len(load_calls) == 1
    assert load_calls[0]["graph_name"] == "wsgraph"
    assert isinstance(load_calls[0]["staging"], Staging)


def test_load_graph_option_overrides_config_graph_name(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    Staging(root / ".codegraph" / "staging.db").close()

    load_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph(load_calls))
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)

    result = runner.invoke(app, ["load", str(root), "--graph", "override999"])
    assert result.exit_code == 0, result.output
    assert load_calls[0]["graph_name"] == "override999"


def test_load_store_unreachable_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    Staging(root / ".codegraph" / "staging.db").close()

    def unreachable_load_graph(staging, store_factory, graph_name):
        raise StoreError("Connection refused")

    monkeypatch.setattr("codegraph.cli.load_graph", unreachable_load_graph)

    result = runner.invoke(app, ["load", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


# -- stats: FalkorStore.stats() -> rich-таблицы --


class _FakeStatsStore:
    def __init__(self, cfg, name):
        self.cfg = cfg
        self.name = name

    def graph_exists(self):
        return True

    def stats(self):
        return {"nodes": {"Function": 3, "Module": 2}, "edges": {"CALLS": 4}}


def test_stats_renders_nodes_and_edges_tables(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeStatsStore)

    result = runner.invoke(app, ["stats", str(root)])
    assert result.exit_code == 0, result.output
    assert "Function" in result.output
    assert "CALLS" in result.output


def test_stats_uses_graph_option_over_config_default(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    captured = {}

    class _Spy:
        def __init__(self, cfg, name):
            captured["cfg"] = cfg
            captured["name"] = name

        def graph_exists(self):
            return True

        def stats(self):
            return {"nodes": {}, "edges": {}}

    monkeypatch.setattr("codegraph.cli.FalkorStore", _Spy)
    result = runner.invoke(app, ["stats", str(root), "--graph", "override777"])
    assert result.exit_code == 0, result.output
    assert captured["name"] == "override777"
    assert captured["cfg"] == FalkorDBConfig()


class _FakeEmptyStatsStore:
    """Граф-ключ СУЩЕСТВУЕТ (после index), но пуст -- отличается от «не индексировали
    вовсе» (_FakeMissingGraphStore ниже): пустой существующий граф -- честные нули,
    exit 0; несуществующий -- ошибка «not found», exit 1."""

    def __init__(self, cfg, name):
        pass

    def graph_exists(self):
        return True

    def stats(self):
        return {"nodes": {}, "edges": {}}


def test_stats_empty_graph_is_honest_zeros_not_an_error(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeEmptyStatsStore)

    result = runner.invoke(app, ["stats", str(root)])
    assert result.exit_code == 0, result.output


class _FakeMissingGraphStore:
    stats_called = False  # class-level: инстанс создаёт сам cli, тест видит класс

    def __init__(self, cfg, name):
        pass

    def graph_exists(self):
        return False

    def stats(self):
        type(self).stats_called = True
        raise AssertionError(
            "stats() must not be called when the graph does not exist -- "
            "GRAPH.QUERY would auto-vivify an empty graph key as a side effect"
        )


def test_stats_never_indexed_graph_is_not_found_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeMissingGraphStore)
    monkeypatch.setattr(_FakeMissingGraphStore, "stats_called", False)

    result = runner.invoke(app, ["stats", str(root)])
    assert result.exit_code == 1
    assert "not found" in result.output
    assert "codegraph index" in result.output
    # выделенное not-found сообщение, НЕ ветка недоступности store
    assert "unreachable" not in result.output
    assert "Traceback" not in result.output
    # существование проверено ДО первого GRAPH.QUERY: stats() не звался вовсе
    # (иначе сам запрос auto-vivify'ил бы пустой граф-ключ как побочный эффект)
    assert _FakeMissingGraphStore.stats_called is False


class _FakeUnreachableStore:
    # test_stats_store_failure_is_red_error_exit_1 (broad `except Exception` around
    # stats(), RuntimeError fake) replaced by this + the narrow-boundary contract:
    # cli catches ONLY (StoreError, StoreUnavailable); any other exception is a real
    # bug and must propagate as a traceback, not be masked by a friendly message.
    def __init__(self, cfg, name):
        pass

    def graph_exists(self):
        raise StoreError("Connection refused")

    def stats(self):
        raise StoreError("Connection refused")


def test_stats_store_unreachable_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeUnreachableStore)

    result = runner.invoke(app, ["stats", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


# -- serve: _load -> build_server(cfg, graph_name).run() (stdio) --


class _FakeServeStore:
    """graph_exists()-only fake -- serve() never touches .stats()/.get_nodes()/etc
    directly (that's all inside GraphQuery, exercised by test_query_api.py and the
    falkordb-marked MCP contract test, not this module's monkeypatched CLI wiring)."""

    def __init__(self, cfg, name):
        self.cfg = cfg
        self.name = name

    def graph_exists(self):
        return True


class _FakeMissingServeStore(_FakeServeStore):
    def graph_exists(self):
        return False


class _FakeUnreachableServeStore(_FakeServeStore):
    def graph_exists(self):
        raise StoreError("Connection refused")


class _FakeServer:
    def __init__(self):
        self.run_called = False

    def run(self):
        self.run_called = True


def _fake_build_server(recorded: list[dict]):
    def fn(cfg, graph_name):
        server = _FakeServer()
        recorded.append({"cfg": cfg, "graph_name": graph_name, "server": server})
        return server

    return fn


def test_serve_loads_config_and_starts_server_with_config_graph_name(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    build_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeServeStore)
    monkeypatch.setattr("codegraph.cli.build_server", _fake_build_server(build_calls))

    result = runner.invoke(app, ["serve", str(root)])
    assert result.exit_code == 0, result.output
    assert len(build_calls) == 1
    assert build_calls[0]["graph_name"] == "wsgraph"
    assert build_calls[0]["cfg"].graph_name == "wsgraph"
    assert build_calls[0]["server"].run_called is True
    assert "not found" not in result.output  # graph_exists() True -> no warning


def test_serve_graph_option_overrides_config_graph_name(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    build_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeServeStore)
    monkeypatch.setattr("codegraph.cli.build_server", _fake_build_server(build_calls))

    result = runner.invoke(app, ["serve", str(root), "--graph", "override555"])
    assert result.exit_code == 0, result.output
    assert build_calls[0]["graph_name"] == "override555"


def test_serve_warns_but_still_starts_when_graph_not_found(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    build_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeMissingServeStore)
    monkeypatch.setattr("codegraph.cli.build_server", _fake_build_server(build_calls))

    result = runner.invoke(app, ["serve", str(root)])
    # amendment 4: graph not existing yet is a YELLOW warning, not a failure -- index
    # may run later against an already-running MCP server (store_factory in
    # build_server re-resolves the graph on every tool call, see query.api.GraphQuery).
    assert result.exit_code == 0, result.output
    assert "not found" in result.output
    assert len(build_calls) == 1  # server still starts
    assert build_calls[0]["server"].run_called is True


def test_serve_store_unreachable_is_red_error_exit_1_and_never_starts_server(
    tmp_path, monkeypatch
):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    build_calls: list[dict] = []
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeUnreachableServeStore)
    monkeypatch.setattr("codegraph.cli.build_server", _fake_build_server(build_calls))

    result = runner.invoke(app, ["serve", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output
    assert build_calls == []  # never got to build_server/run -- hard boundary, no partial start
