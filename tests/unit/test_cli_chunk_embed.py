"""M3 T6: cli.index wiring for S8 (chunk_embed) -- lazy/graceful embedder construction,
`--no-embed`, stage ordering (between link_workspace and load_graph), report plumbing.

Same monkeypatch-the-module-level-name technique as test_cli_m1b.py (analyze_service/
link_workspace/load_graph/FalkorStore), extended to the two new names this task adds:
`codegraph.cli.make_embedder` and `codegraph.cli.run_chunk_embed`.
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.core.errors import CodegraphError

runner = CliRunner()


def _write_workspace(
    tmp_path: Path,
    n_services: int = 1,
    graph_name: str = "wsgraph",
    embedding_provider: str | None = None,
) -> Path:
    root = tmp_path / "ws"
    lines = ["version: 1", f"graph_name: {graph_name}"]
    if embedding_provider is not None:
        lines += ["embedding:", f"  provider: {embedding_provider}"]
    lines.append("services:")
    for i in range(n_services):
        (root / f"svc{i}").mkdir(parents=True)
        lines.append(f"  - name: svc{i}")
        lines.append(f"    path: ./svc{i}")
    root.mkdir(exist_ok=True)
    (root / "codegraph.yaml").write_text("\n".join(lines) + "\n")
    return root


def _fake_analyze_service(
    svc, staging, cache_dir, runner=None, active_idioms=frozenset(), idioms=None
):
    return {
        "service": svc.name, "files": 1, "defs": 0, "refs": 0, "malformed_ranges": 0,
        "nodes": 1, "edges": 0, "imports_external": 0,
        "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0,
        "degraded": False, "reason": None, "from_cache": False,
    }


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


class _FakeEmbedder:
    model_id = "fake-embedder"
    dim = 4

    def embed_batch(self, texts):
        return [[0.0] * 4 for _ in texts]

    def embed_query(self, text):
        return [0.0] * 4


def _patch_pipeline(monkeypatch):
    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service)
    monkeypatch.setattr("codegraph.cli.link_workspace", _fake_link_workspace)
    monkeypatch.setattr("codegraph.cli.load_graph", _fake_load_graph)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)


# ======================================================================================
# -- stage ordering: chunk_embed runs between link_workspace and load_graph --
# ======================================================================================


def test_index_calls_chunk_embed_between_link_and_load(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    call_order: list[str] = []

    def link_spy(cfg, staging):
        call_order.append("link_workspace")
        return _fake_link_workspace(cfg, staging)

    def chunk_embed_spy(cfg, staging, embedder):
        call_order.append("chunk_embed")
        return {
            "chunks_total": 0, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    def load_spy(staging, store_factory, graph_name):
        call_order.append("load_graph")
        return _fake_load_graph(staging, store_factory, graph_name)

    monkeypatch.setattr("codegraph.cli.analyze_service", _fake_analyze_service)
    monkeypatch.setattr("codegraph.cli.link_workspace", link_spy)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.load_graph", load_spy)
    monkeypatch.setattr("codegraph.cli.FalkorStore", _FakeFalkorStore)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert call_order == ["link_workspace", "chunk_embed", "load_graph"]


# ======================================================================================
# -- embedder construction: zero-config default (local provider) wired to make_embedder --
# ======================================================================================


def test_index_passes_make_embedder_result_to_chunk_embed(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    fake = _FakeEmbedder()
    received: list = []

    def chunk_embed_spy(cfg, staging, embedder):
        received.append(embedder)
        return {
            "chunks_total": 0, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: fake)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert received == [fake]


# ======================================================================================
# -- --no-embed: explicit skip, no warning, make_embedder never even called --
# ======================================================================================


def test_index_no_embed_flag_skips_make_embedder_and_passes_none(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    received: list = []

    def chunk_embed_spy(cfg, staging, embedder):
        received.append(embedder)
        return {
            "chunks_total": 0, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    def _boom(cfg):
        raise AssertionError("make_embedder must not be called under --no-embed")

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", _boom)

    result = runner.invoke(app, ["index", str(root), "--no-embed"])
    assert result.exit_code == 0, result.output
    assert received == [None]
    assert "skipped" not in result.output.lower()  # explicit flag, not a degradation


# ======================================================================================
# -- graceful degradation: make_embedder raising CodegraphError -> yellow warning,
# embedder=None, pipeline still completes with exit 0 --
# ======================================================================================


def test_index_embedder_construction_failure_warns_and_continues_with_none(
    tmp_path, monkeypatch
):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")
    received: list = []

    def chunk_embed_spy(cfg, staging, embedder):
        received.append(embedder)
        return {
            "chunks_total": 0, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    def _raise_hint(cfg):
        raise CodegraphError("uv sync --extra local-emb")

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", _raise_hint)

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert received == [None]
    assert "s8" in result.output.lower()
    assert "skipped" in result.output.lower()
    assert "uv sync --extra local-emb" in result.output


# ======================================================================================
# -- report plumbing: chunk_embed's return dict flows into build_report/report.json --
# ======================================================================================


def test_index_report_includes_chunking_stats_from_chunk_embed(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def chunk_embed_spy(cfg, staging, embedder):
        return {
            "chunks_total": 7, "embedded": 5, "embedded_fresh": 3,
            "embedded_from_cache": 2, "reused": 2, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output

    report = json.loads((root / ".codegraph" / "report.json").read_text())
    assert report["chunking"] == {
        "chunks_total": 7, "embedded": 5, "embedded_fresh": 3,
        "embedded_from_cache": 2, "reused": 2, "skipped_no_embedder": 0,
    }
    assert "chunking" in result.output.lower()
    assert "7" in result.output


def test_index_dry_run_does_not_call_chunk_embed(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, n_services=1, graph_name="wsgraph")

    def _boom(*a, **kw):
        raise AssertionError("must not be called under --dry-run")

    monkeypatch.setattr("codegraph.cli.analyze_service", _boom)
    monkeypatch.setattr("codegraph.cli.load_graph", _boom)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", _boom)
    monkeypatch.setattr("codegraph.cli.make_embedder", _boom)

    result = runner.invoke(app, ["index", str(root), "--dry-run"])
    assert result.exit_code == 0, result.output


# ======================================================================================
# -- MINOR-9 (M3 final review; M4 T1 re-gated the trigger from `embedded` to
# `embedded_fresh` -- a repeat run served entirely from the persistent embedding
# cache must NOT warn, even though `embedded` (the combined total) is > 0) --
# ======================================================================================


def test_index_warns_about_paid_provider_when_chunks_were_embedded(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph", embedding_provider="openai")

    def chunk_embed_spy(cfg, staging, embedder):
        return {
            "chunks_total": 5, "embedded": 5, "embedded_fresh": 5,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert "5 chunk(s) embedded via openai API" in result.output
    assert "re-embed" in result.output.lower()


def test_index_no_paid_provider_warning_for_local_provider(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph")  # default provider: local

    def chunk_embed_spy(cfg, staging, embedder):
        return {
            "chunks_total": 5, "embedded": 5, "embedded_fresh": 5,
            "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert "embedded via" not in result.output


def test_index_no_paid_provider_warning_when_nothing_embedded(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path, graph_name="wsgraph", embedding_provider="voyage")

    def chunk_embed_spy(cfg, staging, embedder):
        return {
            "chunks_total": 5, "embedded": 0, "embedded_fresh": 0,
            "embedded_from_cache": 0, "reused": 5, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert "embedded via" not in result.output


def test_index_no_paid_provider_warning_when_everything_served_from_cache(
    tmp_path, monkeypatch
):
    """The M4 T1 regression this whole re-gating exists for: a repeat run against an
    unchanged, paid-provider workspace has `embedded == 5` (chunks got a usable
    vector) but `embedded_fresh == 0` (every one of them came from the persistent
    embedding_cache, zero provider calls) -- must NOT print the paid-provider notice,
    even though the OLD `embedded > 0` gate would have fired here."""
    root = _write_workspace(tmp_path, graph_name="wsgraph", embedding_provider="openai")

    def chunk_embed_spy(cfg, staging, embedder):
        return {
            "chunks_total": 5, "embedded": 5, "embedded_fresh": 0,
            "embedded_from_cache": 5, "reused": 0, "skipped_no_embedder": 0,
        }

    _patch_pipeline(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_chunk_embed", chunk_embed_spy)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: _FakeEmbedder())

    result = runner.invoke(app, ["index", str(root)])
    assert result.exit_code == 0, result.output
    assert "embedded via" not in result.output
