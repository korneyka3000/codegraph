import pytest

from codegraph.doctor import check_chunk_vector_index, run_store_probes
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore

pytestmark = pytest.mark.falkordb


def test_all_required_features_present_on_pinned_image(falkordb_cfg):
    results = run_store_probes(lambda: connect(falkordb_cfg))
    failed = [(r.name, r.detail) for r in results if not r.ok]
    assert not failed, f"pinned FalkorDB image lacks features: {failed}"


VECTOR_PROBE_GRAPH = "__doctor_t7_vector_probe__"


def test_check_chunk_vector_index_silent_on_healthy_graph_then_warns_once_index_dropped(
    falkordb_cfg,
):
    """M3 backlog ("no-index marker -> doctor probe"), live proof against a real
    FalkorDB: a graph with live Chunk embeddings AND a real covering vector index is
    silent (None, no warning row) -- then, simulating an operational mishap (an index
    manually dropped after a normal `codegraph index` run, `DROP VECTOR INDEX` being
    the exact inverse of `ddl.ensure_schema`'s own `CREATE VECTOR INDEX`), the SAME
    graph -- embeddings completely untouched -- starts warning."""
    store = FalkorStore(falkordb_cfg, VECTOR_PROBE_GRAPH)
    try:
        store.ensure_schema(dim=4)
        store.upsert_nodes(
            ("Chunk",),
            [{
                "id": "chunk:x", "props": {"service": "svc"},
                "embedding": [0.1, 0.2, 0.3, 0.4],
            }],
            vector_props=("embedding",),
        )

        # normal graph: live embedding + a real covering vector index -> no warning.
        assert check_chunk_vector_index(store) is None

        # the index gets dropped -- embeddings (and everything else) stay exactly as
        # they were, only the index itself is gone.
        store.raw("DROP VECTOR INDEX FOR (c:Chunk) ON (c.embedding)")

        result = check_chunk_vector_index(store)
        assert result is not None
        assert result.ok is False
        assert result.name == "chunk_vector_index"
        assert "vector index" in result.detail.lower()
        assert VECTOR_PROBE_GRAPH in result.detail
    finally:
        store.delete_graph()
