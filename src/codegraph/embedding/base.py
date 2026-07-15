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

Every implementation in this package returns unit-normalized (L2 norm 1) vectors, so
cosine similarity reduces to a plain dot product at the retrieval layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class Embedder(Protocol):
    model_id: str
    dim: int

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """One unit-normalized vector per input text, same order. `[]` in -> `[]` out."""
        ...

    def embed_query(self, text: str) -> list[float]:
        """Single unit-normalized vector for one query string."""
        ...
