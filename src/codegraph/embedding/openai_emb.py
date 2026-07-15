"""OpenAIEmbedder: OpenAI embeddings-API-backed Embedder (`openai` package, lazy
import inside `__init__` -- never a top-level import in this module, so importing
`codegraph.embedding` never requires the `openai` extra to be installed).

Construction-time guard order (both checked in `__init__`, key BEFORE import):
1. `OPENAI_API_KEY` must be set in the environment -- missing it raises
   `CodegraphError` with an actionable hint immediately, before ever touching the
   `openai` package. Checking this first (rather than the import) means the
   "missing key" error message is what a caller sees even in an environment where
   the `openai` package also happens not to be installed, instead of a confusing
   "package not installed" message that has nothing to do with the actual problem.
2. `import openai` -- `ImportError` (extra not installed) raises `CodegraphError`
   with the `uv sync --extra openai` hint.

Dim resolution -- known-table with a fallback probe, decided and documented here:
`KNOWN_DIMS` covers OpenAI's current embedding models so the common case (`model` in
the table) never needs a network call just to answer `.dim`. For any `model` NOT in
the table (a newer model this table hasn't been updated for yet, a fine-tune, ...),
`dim` is a property that lazily makes exactly ONE real `embed_query` call the first
time it's accessed, caches the resulting length, and never probes again for the
lifetime of this instance. This means `.dim` is sometimes free (known model) and
sometimes costs one real API call the first time (unknown model) -- documented here
rather than silently surprising a caller with a network call on what looks like a
plain attribute read.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

from codegraph.core.errors import CodegraphError

_ENV_VAR = "OPENAI_API_KEY"
_IMPORT_HINT = "openai package not installed -- run `uv sync --extra openai` to install it."
_KEY_HINT = (
    f"{_ENV_VAR} not set -- export {_ENV_VAR}=sk-... "
    "(see https://platform.openai.com/api-keys) before using provider=openai."
)

# Verified against OpenAI's own docs (developers.openai.com/api/docs/guides/embeddings)
# at the time this module was written. `text-embedding-ada-002` is the legacy model,
# included since it's still commonly configured.
KNOWN_DIMS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


class OpenAIEmbedder:
    def __init__(self, model: str = "text-embedding-3-small"):
        api_key = os.environ.get(_ENV_VAR)
        if not api_key:
            raise CodegraphError(_KEY_HINT)
        try:
            from openai import OpenAI
        except ImportError as e:
            raise CodegraphError(_IMPORT_HINT) from e

        self._client = OpenAI(api_key=api_key)
        self.model_id = model
        self._dim = KNOWN_DIMS.get(model)  # None -> probed lazily, see `dim` below

    @property
    def dim(self) -> int:
        if self._dim is None:
            self._dim = len(self.embed_query("dimension probe"))
        return self._dim

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        resp = self._client.embeddings.create(model=self.model_id, input=list(texts))
        return [item.embedding for item in resp.data]

    def embed_query(self, text: str) -> list[float]:
        return self.embed_batch([text])[0]
