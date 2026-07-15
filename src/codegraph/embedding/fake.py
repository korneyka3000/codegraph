"""FakeEmbedder: deterministic, dependency-free Embedder -- no model download, no
network, no API key. Used by unit tests across the codebase (and available as a
degraded-default stand-in anywhere an `Embedder` is required but a real one isn't)
wherever exercising the real embedding stack would be slow/flaky/require secrets.

Determinism, precisely: `embed_query(text)` derives its vector purely from
`hashlib.sha256` digests of `text` -- deliberately NOT Python's builtin `hash()`
(salted per-process by `PYTHONHASHSEED` for `str` since Python 3.3 -- the same text
would embed differently across processes/runs) and NOT the stdlib `random` module's
global state (only reproducible with explicit, remembered seeding). `hashlib.sha256`
is a pure function of its input bytes: same text -> byte-identical digest, forever, on
any machine -- so two `FakeEmbedder` instances (same or different process, same or
different `dim`) embed the same text identically, and unit tests can assert exact
vector equality rather than "close enough".

Never the zero vector: each component is `uint32 / 0xFFFFFFFF * 2 - 1` for a uint32
drawn from the digest bytes. `0xFFFFFFFF` is odd, so no integer uint32 maps to exactly
0.5 before the `* 2 - 1` -- no component can land on exactly `0.0`. A component-wise
(let alone whole-vector) zero result is structurally impossible, not just unlikely.

Not cryptographically/semantically meaningful: cosine similarity between two
`FakeEmbedder` vectors carries no relationship to the MEANING of the input texts
(unlike a real model) -- it's a stable, collision-resistant fingerprint, not an
embedding in the ML sense. Use this for plumbing/determinism/normalization tests only,
never for retrieval-quality assertions (see `tests/integration/test_local_embedder.py`
for those, against a real model).
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence

_UINT32_MAX = 0xFFFFFFFF


class FakeEmbedder:
    def __init__(self, dim: int = 8, model_id: str | None = None):
        if dim <= 0:
            raise ValueError(f"dim must be positive, got {dim}")
        self.dim = dim
        self.model_id = model_id if model_id is not None else f"fake-{dim}d"

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed_one(text)

    def _embed_one(self, text: str) -> list[float]:
        values: list[float] = []
        counter = 0
        while len(values) < self.dim:
            digest = hashlib.sha256(f"{text}\0{counter}".encode()).digest()
            for offset in range(0, len(digest), 4):
                if len(values) >= self.dim:
                    break
                as_uint32 = int.from_bytes(digest[offset : offset + 4], "big")
                values.append((as_uint32 / _UINT32_MAX) * 2.0 - 1.0)
            counter += 1
        norm = math.sqrt(sum(v * v for v in values))
        return [v / norm for v in values]
