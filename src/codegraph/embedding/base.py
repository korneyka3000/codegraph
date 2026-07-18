"""Embedder Protocol: the structural contract every embedding provider satisfies.

Structural (not ABC) typing, same as `stores.graph.GraphStore` -- `fake.FakeEmbedder`,
`local.LocalEmbedder`, `openai_emb.OpenAIEmbedder` and `voyage.VoyageEmbedder` all
satisfy this Protocol without inheriting from it.

Field/method semantics (forward-looking to M3 T6's `chunk+embed` stage, the actual
consumer -- NOT wired up here, see this package's `__init__.py`):

- `model_id`: the exact string T6 will pass to `stores.staging.Staging.set_embeddings`/
  `chunks_missing_embedding` as the `embed_model` cache key -- switching a workspace's
  `EmbeddingConfig.model` (or provider) changes `model_id`, which is what makes
  `chunks_missing_embedding` correctly treat every existing chunk as needing
  re-embedding after a model switch (see that method's own docstring in
  `stores/staging.py`).
- `dim`: the embedding vector length T6 will pass to FalkorDB's
  `CREATE VECTOR INDEX ... OPTIONS {dimension: <dim>, similarityFunction: 'cosine'}`
  (see `doctor.run_store_probes`'s `vector_index_cosine` probe for the Cypher shape).
- `embed_batch`: bulk path, used to embed staged chunk bodies for indexing.
- `embed_query`: single-text path, used to embed a search query at retrieval time.
  For most providers this is just `embed_batch([text])[0]`; Voyage's asymmetric
  `input_type` support is the one exception (see `voyage.py`'s module docstring).
- `concurrency_safe` (M4 T8): True means `pipeline.chunk_embed._embed_missing` may
  fire up to 4 `embed_batch` calls concurrently (`ThreadPoolExecutor`, see that
  module's own docstring) for this embedder instead of its default one-batch-at-a-time
  loop. Set True in `openai_emb.py`/`voyage.py` -- both are remote HTTP API calls,
  where overlapping the network round-trip of several batches is a genuine wall-clock
  win, and whose underlying HTTP clients are safe to drive from multiple threads at
  once. Left False in `local.py` (a local, CPU/GPU-bound `sentence-transformers`
  model -- concurrent Python-thread calls would mostly just serialize on the GIL
  around whatever isn't already vectorized C/CUDA, with no reliable win, and a
  model that ISN'T internally thread-safe would be a correctness hazard, not just a
  missed optimization) and absent on `fake.py` (falls back to this attribute's own
  `False` default below via `getattr`, same effective behavior as an explicit False
  -- `FakeEmbedder` is a test double, never a real concurrency-worthy I/O call).
  Structural/duck-typed callers (every real implementation in this package) don't
  inherit this class, so this in-Protocol default is documentation for type-checking
  purposes only -- `_embed_missing` reads it via `getattr(embedder, "concurrency_safe",
  False)`, not attribute access, so an implementation that never sets it at all (e.g.
  `fake.py`) is exactly as safe as one that sets it to False explicitly.

Every implementation in this package returns unit-normalized (L2 norm 1) vectors, so
cosine similarity reduces to a plain dot product at the retrieval layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class Embedder(Protocol):
    model_id: str
    dim: int
    concurrency_safe: bool = False

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """One unit-normalized vector per input text, same order. `[]` in -> `[]` out."""
        ...

    def embed_query(self, text: str) -> list[float]:
        """Single unit-normalized vector for one query string.

        Default body (M5 T6): `self.embed_batch([text])[0]` -- correct for any
        `Embedder` whose query/passage encoding is symmetric (the common case; every
        provider in this package except `voyage.py` currently overrides this method
        anyway, purely structurally, so this default body is never actually reached
        by fake/local/openai -- see this class's own module docstring). It only has
        teeth for a class that NOMINALLY subclasses `Embedder` (real inheritance, not
        just duck typing) and implements `embed_batch` alone; `voyage.py`'s
        asymmetric `input_type="query"` handling is the one existing case that MUST
        override this default rather than rely on it (it is not just an
        optimization there -- `embed_batch([text])[0]` would silently use the wrong
        `input_type`)."""
        return self.embed_batch([text])[0]
