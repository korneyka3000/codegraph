"""VoyageEmbedder: Voyage AI embeddings-API-backed Embedder (`voyageai` package, lazy
import inside `__init__` -- same construction-time guard order as `openai_emb.py`:
`VOYAGE_API_KEY` presence checked BEFORE the lazy `import voyageai`, so a missing key
always surfaces the key hint rather than a misleading "package not installed" message
when both are absent.

Asymmetric embeddings via `input_type`: Voyage's `.embed()` API accepts an
`input_type` of `"document"` or `"query"`, which prepends a task-specific prompt
before vectorizing (per Voyage's own docs, embeddings produced with different
`input_type` values remain comparable via cosine -- this is a retrieval-quality
optimization, not a compatibility requirement). `embed_batch` (indexing staged chunk
bodies) always uses `input_type="document"`; `embed_query` (a search query at
retrieval time) always uses `input_type="query"` -- unlike every other Embedder in
this package, `embed_query` here is NOT simply `embed_batch([text])[0]`, it makes its
own `.embed()` call with the query input_type.

Dim resolution: same known-table + first-access-probe-and-cache strategy as
`openai_emb.py` (see that module's docstring for the full rationale). `voyage-code-3`
supports output dimensions of 256/512/1024/2048 via an `output_dimension` request
parameter; this module never sets that parameter, so the model's own default -- 1024,
per Voyage's docs -- applies, which is what `KNOWN_DIMS` pins.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

from codegraph.core.errors import CodegraphError

_ENV_VAR = "VOYAGE_API_KEY"
_IMPORT_HINT = "voyageai package not installed -- run `uv sync --extra voyage` to install it."
_KEY_HINT = (
    f"{_ENV_VAR} not set -- export {_ENV_VAR}=... "
    "(see https://dashboard.voyageai.com) before using provider=voyage."
)

# voyage-code-3's default (unset `output_dimension`) embedding size, per Voyage's own
# docs (docs.voyageai.com/reference/inference) at the time this module was written.
KNOWN_DIMS = {
    "voyage-code-3": 1024,
}


class VoyageEmbedder:
    def __init__(self, model: str = "voyage-code-3"):
        api_key = os.environ.get(_ENV_VAR)
        if not api_key:
            raise CodegraphError(_KEY_HINT)
        try:
            import voyageai
        except ImportError as e:
            raise CodegraphError(_IMPORT_HINT) from e

        self._client = voyageai.Client(api_key=api_key)
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
        result = self._client.embed(list(texts), model=self.model_id, input_type="document")
        return list(result.embeddings)

    def embed_query(self, text: str) -> list[float]:
        result = self._client.embed([text], model=self.model_id, input_type="query")
        return list(result.embeddings[0])
