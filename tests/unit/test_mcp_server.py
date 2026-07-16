"""Юниты mcp/server.py `_default_embedder_factory` (M3 T7 review fix: прямой тест
degrade-пути, НЕ зависящий от наличия/отсутствия sentence-transformers в окружении --
make_embedder подменяется monkeypatch'ем в НЕЙМСПЕЙСЕ server-модуля, так что реальный
embedding-стек не трогается вовсе). Полный сетевой контракт search_code -- tests/
integration/test_mcp_contract.py (marker falkordb); здесь -- только фабрика и то, что
degraded build_server вообще поднимается (serve без embedder'а жив, text-only)."""

from __future__ import annotations

import pytest

import codegraph.mcp.server as server_mod
from codegraph.config.models import ServiceConfig, WorkspaceConfig
from codegraph.core.errors import CodegraphError
from codegraph.embedding.fake import FakeEmbedder


def _cfg(tmp_path) -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="t7-embedder-factory",
        services=[ServiceConfig(name="svc", path=tmp_path)],
    )


def test_default_embedder_factory_returns_none_on_codegraph_error(tmp_path, monkeypatch):
    """The degrade contract: make_embedder raising CodegraphError (provider extra not
    installed, API key missing -- every 'can't build an embedder right now' case it
    knows about) -> the factory closure returns None, it does NOT propagate. None is
    exactly what GraphQuery._get_embedder/retrieval treat as 'vector unavailable'
    (hybrid/find_entrypoint degrade to text-only, mode="vector" -> error dict) -- so
    this single None return IS the whole degraded-serve story at this layer."""

    def boom(embedding_cfg):
        raise CodegraphError("sentence-transformers not installed -- ...")

    monkeypatch.setattr(server_mod, "make_embedder", boom)
    factory = server_mod._default_embedder_factory(_cfg(tmp_path))
    assert factory() is None
    assert factory() is None  # every call degrades the same way, no state poisoning


def test_default_embedder_factory_returns_embedder_on_success(tmp_path, monkeypatch):
    embedder = FakeEmbedder(dim=4)
    seen_cfgs = []

    def fake_make_embedder(embedding_cfg):
        seen_cfgs.append(embedding_cfg)
        return embedder

    monkeypatch.setattr(server_mod, "make_embedder", fake_make_embedder)
    cfg = _cfg(tmp_path)
    factory = server_mod._default_embedder_factory(cfg)
    assert factory() is embedder
    assert seen_cfgs == [cfg.embedding]  # the workspace's own EmbeddingConfig, verbatim


def test_default_embedder_factory_propagates_non_codegraph_errors(tmp_path, monkeypatch):
    """Only CodegraphError means 'degrade' (the make_embedder contract) -- any OTHER
    exception is a genuine bug somewhere and must surface as a traceback, not be
    silently converted into 'vector search unavailable' (same narrowing rationale as
    cli._make_embedder_or_warn, see its docstring)."""

    def bug(embedding_cfg):
        raise ValueError("a real bug, not a degradation case")

    monkeypatch.setattr(server_mod, "make_embedder", bug)
    factory = server_mod._default_embedder_factory(_cfg(tmp_path))
    with pytest.raises(ValueError):
        factory()


def test_build_server_constructs_fine_when_make_embedder_raises(tmp_path, monkeypatch):
    """Degraded serve boots: build_server with the DEFAULT embedder_factory (no test
    override) must construct successfully even when make_embedder can only raise --
    the factory is lazy (called per _get_embedder, never at build time), so a
    workspace with no embedding extra installed still gets a working 9-tool server
    (search_code text-mode/hybrid-degraded, find_entrypoint text-only)."""

    def boom(embedding_cfg):
        raise CodegraphError("no provider available")

    monkeypatch.setattr(server_mod, "make_embedder", boom)
    server = server_mod.build_server(_cfg(tmp_path), "t7-embedder-factory")
    assert server is not None  # construction itself never touched make_embedder
