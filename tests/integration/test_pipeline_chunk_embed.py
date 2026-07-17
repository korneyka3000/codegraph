"""M4 T1: integration-level proof of the persistent embedding cache's cross-run
promise -- the master-plan gate itself ("`codegraph index` repeated on an unchanged
workspace makes ZERO embedding-provider calls"). Runs the REAL `analyze_service` +
`link_workspace` against `fixtures/workspace.yaml`'s three services (degraded/
heuristic fallback, same `_AlwaysFailRunner` technique as test_augment.py/
test_chunk_embed.py -- no real scip-python subprocess needed) TWICE, simulating two
consecutive `codegraph index` runs over an unchanged checkout, each followed by a
real `chunk_embed.run` + `load_graph` (S1/S7/S8/S9, the same stage subset cli.index
itself wires together): every `analyze_service` call begins with its own
`staging.begin_service` (wiping and re-deriving that service's whole S1-S6 layer,
chunks included, from the SAME unchanged on-disk files) -- so the second `chunk_embed.
run` sees brand new, never-embedded `chunks` rows, and the ONLY way it can still avoid
re-embedding them is the persistent, cross-run `embedding_cache` table (M4 T1), which
`begin_service` never touches at all.

Unlike tests/unit/test_chunk_embed.py's own repeat-run test (a small synthetic
tmp_path-based workspace), this exercises the same scenario against the REAL,
multi-service fixture corpus end to end through a live FalkorDB load -- matching the
`falkordb` marker convention of this module's S8 integration sibling,
tests/integration/test_chunk_embed_load.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.embedding.fake import FakeEmbedder
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.load import load_graph
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.falkordb

WORKSPACE = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"
GRAPH_NAME = "__m4_t1_cache__"
BUILD_NAME = f"{GRAPH_NAME}__build"


class _AlwaysFailRunner:
    """Same technique as test_augment.py/test_chunk_embed.py -- forces the degraded
    heuristic-fallback path without a real scip-python subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


class _RecordingEmbedder:
    """Wraps a real Embedder, recording each `embed_batch` call's size -- proves
    "zero provider calls" directly, not just by inference from the report dict."""

    def __init__(self, inner):
        self._inner = inner
        self.model_id = inner.model_id
        self.dim = inner.dim
        self.batch_sizes: list[int] = []

    def embed_batch(self, texts):
        self.batch_sizes.append(len(texts))
        return self._inner.embed_batch(texts)

    def embed_query(self, text):
        return self._inner.embed_query(text)


def _run_pipeline(cfg: WorkspaceConfig, staging: Staging, cache_dir: Path) -> None:
    """S1-S7 over every configured service, degraded (no real scip-python) --
    mirrors test_augment.py's `_index_and_chunk_workspace`/test_m3_gate.py's own
    `_run_pipeline` helper. Called TWICE by the test below to simulate two
    consecutive `codegraph index` runs over the SAME unchanged checkout."""
    active_idioms = frozenset(cfg.builtin_idioms)
    for svc in cfg.services:
        analyze_service(
            svc, staging, cache_dir, runner=_AlwaysFailRunner(),
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
    link_workspace(cfg, staging)


def _cleanup(cfg, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # swap_in уже мог унести build-ключ через RENAME


def test_repeat_full_index_over_real_fixtures_makes_zero_provider_calls(
    falkordb_cfg, tmp_path,
):
    """The master-plan gate itself, end to end against the real fixtures: two full
    `codegraph index`-shaped runs (S1/S7/S8/S9) over an UNCHANGED checkout -- the
    second run's `chunk_embed.run` must embed nothing fresh (`embedded_fresh == 0`),
    reuse every chunk from the persistent `embedding_cache` table
    (`embedded_from_cache == chunks_total`), and never call `embed_batch` at all."""
    cfg = load_workspace(WORKSPACE)
    staging = Staging(tmp_path / "s.db")
    cache_dir = tmp_path / "scip-cache"

    try:
        _run_pipeline(cfg, staging, cache_dir)

        embedder = _RecordingEmbedder(FakeEmbedder(dim=8))
        first = run_chunk_embed(cfg, staging, embedder)
        assert first["chunks_total"] > 0
        assert first["embedded_fresh"] == first["chunks_total"]
        assert first["embedded_from_cache"] == 0
        assert first["embedded"] == first["chunks_total"]
        first_provider_calls = len(embedder.batch_sizes)
        assert first_provider_calls > 0

        load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        # -- repeat `codegraph index` run over the SAME unchanged checkout: re-analyze
        # every service (begin_service wipes+recreates chunks from scratch) + re-link
        # + re-chunk+embed + re-load, exactly like a real second invocation --
        _run_pipeline(cfg, staging, cache_dir)
        embedder.batch_sizes.clear()

        second = run_chunk_embed(cfg, staging, embedder)
        assert second["chunks_total"] == first["chunks_total"]
        assert second["embedded_fresh"] == 0  # the master-plan gate itself
        assert second["embedded_from_cache"] == second["chunks_total"]
        assert second["embedded"] == second["chunks_total"]
        assert embedder.batch_sizes == []  # embed_batch never called at all

        load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        # sanity: the graph itself still ends up fully populated with vectors after a
        # cache-only reload -- the persistent cache isn't silently degrading the
        # loaded graph's own completeness.
        store = FalkorStore(falkordb_cfg, GRAPH_NAME)
        res = store.raw("MATCH (c:Chunk) RETURN c.id, c.embedding IS NOT NULL")
        assert len(res.result_set) == second["chunks_total"]
        assert all(has_embedding for _chunk_id, has_embedding in res.result_set)
    finally:
        staging.close()
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)
