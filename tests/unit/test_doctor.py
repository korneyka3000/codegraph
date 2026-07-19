from types import SimpleNamespace

from codegraph.config.models import ScipConfig
from codegraph.doctor import (
    CheckResult,
    check_chunk_vector_index,
    run_env_checks,
    run_store_probes,
)


class FakeGraph:
    def __init__(self, fail_on: set[str]):
        self.fail_on = fail_on
        self.queries: list[str] = []

    def query(self, q: str, params=None):
        self.queries.append(q)
        for marker in self.fail_on:
            if marker in q:
                raise RuntimeError(f"unsupported: {marker}")
        return SimpleNamespace(result_set=[[1]])

    def delete(self):
        pass


class FakeDB:
    def __init__(self, fail_on: set[str] = frozenset(), constraint_ok: bool = True):
        self._g = FakeGraph(set(fail_on))
        self.constraint_ok = constraint_ok
        self.connection = self

    def select_graph(self, name: str):
        return self._g

    def ping(self):
        return True

    def execute_command(self, *args):
        if not self.constraint_ok:
            raise RuntimeError("constraints unsupported")
        return "OK"


def test_env_checks_include_python_and_node():
    results = run_env_checks(ScipConfig(), probe_scip=False)
    names = [r.name for r in results]
    assert "python" in names and "node" in names and "npx" in names
    py = next(r for r in results if r.name == "python")
    assert py.ok  # мы на 3.12+


def test_store_probes_all_ok_with_fake():
    results = run_store_probes(lambda: FakeDB())
    assert all(r.ok for r in results), [(r.name, r.detail) for r in results]
    names = {r.name for r in results}
    assert {"ping", "multi_label", "set_plus_eq", "unique_constraint",
            "vector_index_cosine", "fulltext"} <= names


def test_store_probe_failure_is_isolated():
    results = run_store_probes(lambda: FakeDB(fail_on={"VECTOR INDEX"}))
    by_name = {r.name: r for r in results}
    assert not by_name["vector_index_cosine"].ok
    assert by_name["fulltext"].ok  # остальные probes не пострадали


def test_constraint_failure_reported():
    results = run_store_probes(lambda: FakeDB(constraint_ok=False))
    by_name = {r.name: r for r in results}
    assert not by_name["unique_constraint"].ok
    assert isinstance(by_name["unique_constraint"], CheckResult)


# ======================================================================================
# -- M5 T7 (M3 backlog "no-index marker -> doctor probe"): check_chunk_vector_index --
# a graph with LIVE Chunk embeddings but no vector index covering them -> a warning
# row; every other case (graph absent, no embedded chunks, index already present) ->
# None (nothing to show). Fake store: graph_exists()/raw() only -- the two FalkorStore
# methods check_chunk_vector_index actually calls.
# ======================================================================================


class _FakeVectorIndexStore:
    def __init__(self, *, exists=True, has_embedded_chunk=False, vector_index_rows=()):
        self.exists = exists
        self.has_embedded_chunk = has_embedded_chunk
        self.vector_index_rows = list(vector_index_rows)
        self.graph_name = "fake-graph"
        self.raw_calls: list[str] = []

    def graph_exists(self):
        return self.exists

    def raw(self, cypher, params=None):
        self.raw_calls.append(cypher)
        if "db.indexes" in cypher:
            return SimpleNamespace(result_set=self.vector_index_rows)
        return SimpleNamespace(result_set=[["chunk:x"]] if self.has_embedded_chunk else [])


def test_check_chunk_vector_index_none_when_graph_does_not_exist():
    store = _FakeVectorIndexStore(exists=False)
    assert check_chunk_vector_index(store) is None
    # no query at all attempted -- same auto-vivify avoidance as FalkorStore.stats()'s
    # own graph_exists()-first discipline (a GRAPH.QUERY against an absent graph name
    # creates an empty graph key as a side effect).
    assert store.raw_calls == []


def test_check_chunk_vector_index_none_when_no_chunk_has_a_live_embedding():
    store = _FakeVectorIndexStore(exists=True, has_embedded_chunk=False)
    assert check_chunk_vector_index(store) is None


def test_check_chunk_vector_index_none_when_a_vector_index_already_covers_chunk_embedding():
    store = _FakeVectorIndexStore(
        exists=True, has_embedded_chunk=True,
        vector_index_rows=[["Chunk", ["embedding"], {"embedding": ["VECTOR"]}]],
    )
    assert check_chunk_vector_index(store) is None


def test_check_chunk_vector_index_warns_when_embedded_chunks_exist_but_no_index_at_all():
    store = _FakeVectorIndexStore(
        exists=True, has_embedded_chunk=True, vector_index_rows=[],
    )
    result = check_chunk_vector_index(store)
    assert result is not None
    assert isinstance(result, CheckResult)
    assert result.ok is False
    assert result.name == "chunk_vector_index"
    assert "vector index" in result.detail.lower()
    assert "fake-graph" in result.detail


def test_check_chunk_vector_index_warns_when_other_indexes_exist_but_none_cover_chunk_embedding():
    """Distinguishes "no index at all" from "indexes exist, just not the RIGHT one" --
    a graph can have plenty of OTHER indexes (Sym.id, Chunk fulltext, ...) and still
    lack a vector index specifically on Chunk.embedding."""
    store = _FakeVectorIndexStore(
        exists=True, has_embedded_chunk=True,
        vector_index_rows=[
            ["Sym", ["id"], {"id": ["RANGE"]}],
            ["Chunk", ["text", "context_header"], {"text": ["FULLTEXT"]}],
        ],
    )
    result = check_chunk_vector_index(store)
    assert result is not None
    assert result.ok is False
