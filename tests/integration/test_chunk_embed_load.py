"""Integration test: the FULL S8 -> S9 pipeline (`chunk_embed.run` -> `load_graph`)
against a live FalkorDB, using `FakeEmbedder` end-to-end -- the literal "vecf32 sanity
with FakeEmbedder vectors" scenario from the M3 T6 brief, exercised through the REAL
`chunk_embed.run` entry point (not hand-built `ChunkRec`/embeddings -- see
`test_pipeline_load.py`'s own Chunk/Meta tests for that lower-level, hand-crafted-
vector version, which additionally proves fulltext/no-embedding/Meta-omission
behavior this file doesn't repeat).
"""

from __future__ import annotations

import pytest

from codegraph.chunking.augment import augment_text
from codegraph.config.models import ServiceConfig, WorkspaceConfig
from codegraph.embedding.fake import FakeEmbedder
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.load import load_graph
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.falkordb

GRAPH_NAME = "__t6_pipeline__"
BUILD_NAME = f"{GRAPH_NAME}__build"

SRC = (
    "def create_order():\n"
    "    return 'order created'\n"
    "\n\n"
    "def unrelated_helper():\n"
    "    return 42\n"
)


class _AlwaysFailRunner:
    """Same technique as test_augment.py/test_chunk_embed.py -- forces the degraded
    heuristic-fallback path without a real scip-python subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def _cleanup(cfg, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # swap_in уже мог унести build-ключ через RENAME


def test_full_pipeline_chunk_embed_then_load_vector_search_finds_the_embedded_chunk(
    falkordb_cfg, tmp_path,
):
    """chunk_embed.run(FakeEmbedder) -> load_graph -> query the live vector index with
    THAT SAME embedder's OWN encoding of one chunk's augmented text. A vector's cosine
    similarity to itself is always exactly 1.0 (the maximum possible), so it must rank
    first regardless of what other chunks exist in the graph -- proving vecf32 round-
    trips a REAL FakeEmbedder-produced vector correctly through the entire chunk+embed
    -> load -> query path, not just a hand-crafted one."""
    svc_dir = tmp_path / "svc"
    svc_dir.mkdir()
    (svc_dir / "m.py").write_text(SRC)
    svc = ServiceConfig(name="svc", path=svc_dir)
    cfg = WorkspaceConfig(graph_name=GRAPH_NAME, services=[svc])

    staging = Staging(tmp_path / "s.db")
    analyze_service(svc, staging, tmp_path / "cache", runner=_AlwaysFailRunner())

    embedder = FakeEmbedder(dim=8)
    chunk_report = run_chunk_embed(cfg, staging, embedder)
    assert chunk_report["chunks_total"] == 2
    assert chunk_report["embedded"] == 2

    create_order_row = next(
        r for r in staging.iter_chunks() if "create_order" in r.symbol_id
    )
    other_row = next(
        r for r in staging.iter_chunks() if r.chunk_id != create_order_row.chunk_id
    )

    try:
        load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        store = FalkorStore(falkordb_cfg, GRAPH_NAME)
        # The EXACT same text chunk_embed.run itself fed to embed_batch (header +
        # blank line + code, per augment.augment_text) -- re-embedding it here with
        # embed_query must reproduce the identical vector FakeEmbedder is documented
        # to always produce for identical input text (see fake.py's own determinism
        # docstring), so this query vector is bit-identical to what got stored.
        query_text = augment_text(create_order_row.context_header, create_order_row.text)
        query_vec = embedder.embed_query(query_text)

        res = store.raw(
            "CALL db.idx.vector.queryNodes('Chunk', 'embedding', 2, vecf32($v)) "
            "YIELD node, score RETURN node.id, score ORDER BY score",
            {"v": query_vec},
        )
        result_ids = [row[0] for row in res.result_set]
        assert result_ids[0] == create_order_row.chunk_id
        assert other_row.chunk_id in result_ids  # both chunks made it into the graph
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)


def test_full_pipeline_rerun_chunk_embed_is_idempotent_before_load(
    falkordb_cfg, tmp_path,
):
    """Sanity: the master-plan regression gate (embedded==0, reused==total on an
    unchanged re-run) holds even when the SAME staging subsequently gets loaded into a
    live graph -- chunk_embed's own caching isn't somehow disturbed by a load_graph
    call happening in between two chunk_embed.run calls."""
    svc_dir = tmp_path / "svc"
    svc_dir.mkdir()
    (svc_dir / "m.py").write_text(SRC)
    svc = ServiceConfig(name="svc", path=svc_dir)
    cfg = WorkspaceConfig(graph_name=GRAPH_NAME, services=[svc])

    staging = Staging(tmp_path / "s.db")
    analyze_service(svc, staging, tmp_path / "cache", runner=_AlwaysFailRunner())

    embedder = FakeEmbedder(dim=8)
    first = run_chunk_embed(cfg, staging, embedder)
    assert first["embedded"] == 2

    try:
        load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        second = run_chunk_embed(cfg, staging, embedder)
        assert second["embedded"] == 0
        assert second["reused"] == 2
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)


def test_degraded_rerun_over_unchanged_content_preserves_live_embedding_into_graph(
    falkordb_cfg, tmp_path,
):
    """M5 T7 -- supersedes an earlier version of this test that pinned the OPPOSITE
    outcome (see .superpowers/sdd/task-7-report.md for the full before/after story).
    Coordinator fix's original live repro still applies verbatim up through the
    degraded rerun itself: embed run -> SAME staging.db -> rerun with embedder=None
    (the degradation path -- missing extra/`--no-embed`), content on disk UNCHANGED.
    `upsert_chunks`' ON-CONFLICT contract deliberately preserves each unchanged
    chunk's staged embedding blob across the degraded rerun (that survival is what
    lets a LATER embedder-restored run reuse them for free, at zero provider cost) --
    what changed in M5 T7 is that `Staging.has_live_embeddings()` now correctly
    recognizes that survival and PRESERVES Meta's embed_model/dim instead of blindly
    blanking them. The pre-M5 unconditional clear didn't just make Meta lie about it:
    `_chunk_node_batches`' `dim is None` branch (see its own docstring) trusted that
    lie and stripped the still-valid vector from the load too, discarding real,
    unchanged, genuinely-live search data on every degraded-but-unchanged rerun. Post-
    fix, both the STAGED blob (unchanged, verified below) and Meta (preserved) stay
    honest, so the vector round-trips all the way through `load_graph` into a
    queryable FalkorDB vector index -- as if the degraded rerun had never touched this
    chunk at all, which is exactly true of its embedding."""
    svc_dir = tmp_path / "svc"
    svc_dir.mkdir()
    (svc_dir / "m.py").write_text(SRC)
    svc = ServiceConfig(name="svc", path=svc_dir)
    cfg = WorkspaceConfig(graph_name=GRAPH_NAME, services=[svc])

    staging = Staging(tmp_path / "s.db")
    analyze_service(svc, staging, tmp_path / "cache", runner=_AlwaysFailRunner())

    first = run_chunk_embed(cfg, staging, FakeEmbedder(dim=8))
    assert first["embedded"] == 2  # a real embed run happened first

    degraded = run_chunk_embed(cfg, staging, None)  # then the degraded rerun
    assert degraded["skipped_no_embedder"] == 2
    assert staging.get_meta("embed_model") == "fake-8d"  # preserved -- vectors still live
    assert staging.get_meta("embed_dim") == "8"
    # the unchanged blobs survive in staging (same reuse-cache mechanism as before):
    assert all(row.embedding is not None for row in staging.iter_chunks())

    try:
        load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)

        store = FalkorStore(falkordb_cfg, GRAPH_NAME)
        # EVERY Chunk node still carries its embedding + embed_model...
        res = store.raw(
            "MATCH (c:Chunk) RETURN c.id, c.embedding IS NOT NULL, "
            "c.embed_model IS NOT NULL"
        )
        assert len(res.result_set) == 2
        for _chunk_id, has_embedding, has_model in res.result_set:
            assert has_embedding
            assert has_model
        # ...consistent with Meta (still advertising the live model/dim)...
        meta_props = store.get_nodes(["meta"])[0]
        assert meta_props["embed_model"] == "fake-8d"
        assert meta_props["dim"] == 8
        # ...and a real, queryable vector index actually exists (no error, a real hit).
        res_vec = store.raw(
            "CALL db.idx.vector.queryNodes('Chunk', 'embedding', 1, vecf32($v)) "
            "YIELD node RETURN node.id",
            {"v": [0.0] * 8},
        )
        assert len(res_vec.result_set) == 1
    finally:
        _cleanup(falkordb_cfg, BUILD_NAME, GRAPH_NAME)
