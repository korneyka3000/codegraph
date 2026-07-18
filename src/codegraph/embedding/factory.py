"""factory.make_embedder: dispatch `EmbeddingConfig.provider` to the concrete
`Embedder` implementation. The only thing this module does -- provider selection --
so callers (M3 T6's `chunk+embed` stage) get a working `Embedder` from a workspace's
`EmbeddingConfig` without knowing which concrete class backs which provider string.

Importing this module is always cheap (no torch, no `openai`/`voyageai`, no network):
`local.py`/`openai_emb.py`/`voyage.py` only import their respective heavy/optional
package lazily inside `__init__`, so this module's own top-level imports of those
three classes never trigger it.

NOT validated here: whether `cfg.model` is actually a model name the chosen provider
recognizes (e.g. `provider: openai` with `model:` left at its `EmbeddingConfig`
default -- the M0 local-provider default `jinaai/jina-embeddings-v2-base-code` --
would construct an `OpenAIEmbedder` that only fails once it makes its first real API
call). `EmbeddingConfig` has no cross-field provider/model validator (M0 scope,
unchanged here); `cfg.model` is passed straight through to whichever provider
`cfg.provider` selects, same as every other config value in this codebase that's
ultimately the workspace author's responsibility to set consistently.
"""

from __future__ import annotations

from codegraph.config.models import EmbeddingConfig
from codegraph.core.errors import CodegraphError
from codegraph.embedding.base import Embedder
from codegraph.embedding.local import LocalEmbedder
from codegraph.embedding.openai_emb import OpenAIEmbedder
from codegraph.embedding.voyage import VoyageEmbedder


def make_embedder(cfg: EmbeddingConfig) -> Embedder:
    if cfg.provider == "local":
        # M5 T6: query_prefix/passage_prefix are LocalEmbedder-only (see
        # EmbeddingConfig's own docstring for why openai/voyage don't get them too).
        return LocalEmbedder(
            cfg.model, query_prefix=cfg.query_prefix, passage_prefix=cfg.passage_prefix
        )
    if cfg.provider == "openai":
        return OpenAIEmbedder(cfg.model)
    if cfg.provider == "voyage":
        return VoyageEmbedder(cfg.model)
    # Unreachable while EmbeddingConfig.provider stays a 3-way Literal (pydantic
    # already rejects anything else at config-load time) -- defensive, not a real path.
    raise CodegraphError(f"unknown embedding provider: {cfg.provider!r}")
