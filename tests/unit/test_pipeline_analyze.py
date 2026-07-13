"""S1–S6 analyze_service orchestration: деградированный (fallback) путь через ФЕЙКОВЫЙ
runner, отчёт-словарь, venv-автодетект. Не-деградированный путь юнит-тестируется через
фейковый runner, отдающий валидный (но пустой) SCIP-индекс — без реального scip-python
(это делает интеграционный tests/integration/test_pipeline_real.py, marker scip)."""

from __future__ import annotations

from pathlib import Path

from codegraph.config.models import ServiceConfig
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip import scip_pb2
from codegraph.resolvers.scip.runner import ScipRunError, ScipRunResult
from codegraph.stores.staging import Staging

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"


class _AlwaysFailRunner:
    """Симулирует ScipRunError у scip-python (сеть/npx/venv недоступны) — форсирует
    degraded-путь без реального subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def test_analyze_degraded_path_falls_back_to_heuristic_calls(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert report["degraded"] is True

    nodes = list(st.iter_nodes())
    assert len(nodes) > 0
    assert any(n.id == "svc:document-management" and n.kind == "Service" for n in nodes)

    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(calls) > 0
    assert all(e.resolution == "heuristic" and e.confidence == 0.6 for e in calls)


def test_analyze_degraded_report_dict_fields(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    expected_keys = {
        "service", "files", "defs", "refs", "malformed_ranges", "nodes", "edges",
        "imports_external", "calls_joined", "calls_unresolved", "calls_external",
        "degraded", "reason", "from_cache",
    }
    assert expected_keys <= report.keys()
    assert report["service"] == "document-management"
    assert report["files"] == 8
    assert report["degraded"] is True
    assert report["reason"]  # непустая строка-причина
    assert report["from_cache"] is False
    assert report["nodes"] > 0
    assert report["calls_joined"] > 0


def _empty_scip_bytes() -> bytes:
    return scip_pb2.Index().SerializeToString()


class _FakeSuccessRunner:
    """Пишет валидный, но пустой SCIP-индекс: проверяет НЕ-деградированную ветку
    оркестрации (from_cache passthrough, defs/refs/malformed_ranges из ReaderStats,
    degraded=False/reason=None) без сборки настоящего дерева occurrences."""

    def __init__(self, from_cache: bool):
        self.from_cache = from_cache
        self.calls: list[dict] = []

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        self.calls.append({
            "service_name": service_name, "service_path": service_path,
            "venv": venv, "cache_dir": cache_dir, "tree_hash": tree_hash,
        })
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / f"{service_name}-{tree_hash}.scip"
        out.write_bytes(_empty_scip_bytes())
        return ScipRunResult(scip_path=out, from_cache=self.from_cache)


def test_analyze_static_path_not_degraded(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    runner = _FakeSuccessRunner(from_cache=True)
    report = analyze_service(svc, st, tmp_path / "cache", runner=runner)

    assert report["degraded"] is False
    assert report["reason"] is None
    assert report["from_cache"] is True
    assert report["defs"] == 0 and report["refs"] == 0 and report["malformed_ranges"] == 0
    assert report["files"] == 8


def _minimal_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "main.py").write_text("def f():\n    pass\n")
    return svc_root


class _SpyDegradedRunner:
    """Пишет call-лог, затем всегда кидает ScipRunError — форсирует degraded-путь,
    не требуя валидного SCIP-протобафа; достаточно для проверки venv-автодетекта."""

    def __init__(self):
        self.calls: list[dict] = []

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        self.calls.append({"service_name": service_name, "venv": venv})
        raise ScipRunError("forced for venv-autodetect test")


def test_venv_autodetect_explicit_python(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    (svc_root / "myenv").mkdir()
    svc = ServiceConfig(name="svc", path=svc_root, python="myenv")
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] == svc_root / "myenv"


def test_venv_autodetect_dot_venv_fallback_when_exists(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    (svc_root / ".venv").mkdir()
    svc = ServiceConfig(name="svc", path=svc_root)
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] == svc_root / ".venv"


def test_venv_autodetect_none_when_absent(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] is None


class _FakeDefaultScipRunnerClass:
    """Заглушка на месте класса ScipRunner: конструктор без аргументов (как ScipRunner()),
    .run() сразу кидает ScipRunError — доказывает, что runner=None доходит до конструирования
    default ScipRunner() (см. codegraph.pipeline.analyze.ScipRunner, монки-патченный ниже),
    без реального subprocess/сети."""

    def __init__(self):
        pass

    def run(self, *a, **kw):
        raise ScipRunError("fake default runner")


def test_default_runner_is_constructed_when_none(tmp_path, monkeypatch):
    svc_root = _minimal_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    monkeypatch.setattr("codegraph.pipeline.analyze.ScipRunner", _FakeDefaultScipRunnerClass)
    report = analyze_service(svc, st, tmp_path / "cache", runner=None)
    assert report["degraded"] is True
