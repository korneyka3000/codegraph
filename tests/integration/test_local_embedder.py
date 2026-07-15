"""Integration test for `LocalEmbedder` against the REAL jina-embeddings-v2-base-code
model -- no mocking, no fake `sentence_transformers` module. Marker `emb`: excluded
from the default suite (`pyproject.toml`'s `addopts = "... and not emb"`), same
treatment as the `scip` marker, since this needs the `local-emb` extra installed
(`uv sync --extra local-emb`) and downloads a real model (~300MB+) from the HF Hub on
first run.

Availability guard is a `skipif(importlib.util.find_spec(...) is None)`, mirroring
this repo's own `skipif(shutil.which("npx") is None, ...)` pattern for the `scip`
marker (see e.g. `tests/integration/test_scip_real.py`) -- deliberately NOT
`pytest.importorskip` at module scope: that gates the whole module during
COLLECTION (before `-m` marker filtering ever runs), so a plain default `pytest`
run would report this module as one "skipped" item instead of a clean marker
"deselected", even though `local.py` itself is always safely importable (it only
imports `sentence_transformers` lazily, inside `LocalEmbedder.__init__`). A
`skipif` marker, by contrast, is only ever evaluated for items that survive `-m`
selection -- so `-m 'not emb'` (the default) deselects this module's tests without
ever touching `find_spec`, and an explicit `pytest -m emb` without the extra
installed still skips cleanly with a clear reason instead of erroring.
"""

from __future__ import annotations

import importlib.util
import math

import pytest

from codegraph.embedding.local import LocalEmbedder

pytestmark = [
    pytest.mark.emb,
    pytest.mark.skipif(
        importlib.util.find_spec("sentence_transformers") is None,
        reason="sentence-transformers not installed (uv sync --extra local-emb)",
    ),
]


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    return dot / (norm_a * norm_b)


@pytest.fixture(scope="module")
def embedder() -> LocalEmbedder:
    # Module-scoped: loading the model is the expensive part, share it across
    # every test below instead of reloading per test.
    return LocalEmbedder("jinaai/jina-embeddings-v2-base-code")


def test_dim_is_positive_and_matches_model(embedder):
    assert embedder.dim > 0
    assert embedder.dim == len(embedder.embed_query("def f(): pass"))


def test_two_different_texts_give_different_vectors(embedder):
    code = embedder.embed_query("def add(a, b):\n    return a + b\n")
    prose = embedder.embed_query(
        "Shakespeare wrote many famous plays during the sixteenth century."
    )
    assert code != prose
    assert len(code) == embedder.dim
    assert len(prose) == embedder.dim


def test_embed_batch_matches_embed_query_and_shares_dim(embedder):
    texts = [
        "def add(a, b):\n    return a + b\n",
        "class OrderService:\n    def place(self, order):\n        ...\n",
    ]
    batch = embedder.embed_batch(texts)
    assert len(batch) == 2
    for vec, text in zip(batch, texts, strict=True):
        assert len(vec) == embedder.dim
        # Not necessarily bit-identical to a separate embed_query call (batching
        # can take a different codepath), but should be extremely close --
        # cosine ~1.0 against the same text embedded individually.
        assert _cosine(vec, embedder.embed_query(text)) == pytest.approx(1.0, abs=1e-4)


def test_vectors_are_unit_normalized(embedder):
    vec = embedder.embed_query("def subtract(a, b):\n    return a - b\n")
    norm = math.sqrt(sum(v * v for v in vec))
    assert norm == pytest.approx(1.0, abs=1e-4)


def test_cosine_similarity_sanity_close_texts_closer_than_far_ones(embedder):
    # Two near-duplicate functions (same behavior, different names/style) should
    # sit closer together in embedding space than either does to unrelated
    # natural-language prose -- the actual retrieval-quality property that makes
    # this model useful for code search, not just "produces some vector".
    a = embedder.embed_query("def add(a, b):\n    return a + b\n")
    b = embedder.embed_query("def sum_two_numbers(x, y):\n    return x + y\n")
    far = embedder.embed_query("The Mediterranean diet emphasizes fish, olive oil, and vegetables.")

    sim_close = _cosine(a, b)
    sim_far_a = _cosine(a, far)
    sim_far_b = _cosine(b, far)

    assert sim_close > sim_far_a
    assert sim_close > sim_far_b
