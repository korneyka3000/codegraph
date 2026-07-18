"""LocalEmbedder: sentence-transformers-backed Embedder running the configured model
locally (CPU or GPU, whatever sentence-transformers/torch autodetects) -- no API key,
no per-call network (aside from the one-time HF Hub model download on first
construction, cached locally after that). `sentence-transformers` (and its own `torch`
dependency) is NOT a top-level import anywhere in this module -- it's the `local-emb`
extra (`uv sync --extra local-emb`), imported lazily inside `__init__` so importing
`codegraph.embedding` (or anything that transitively imports this module, e.g.
`factory.py`) never pulls in torch for a workspace that doesn't use provider=local.

SECURITY TRADEOFF -- trust_remote_code=True: jina-embeddings-v2-base-code (the M0
default `EmbeddingConfig.model`), like several other current code-embedding models on
the HF Hub, ships custom Python modeling code alongside its weights rather than using
only stock `transformers` model classes. `sentence-transformers` refuses to load that
custom code unless `trust_remote_code=True` is passed -- which means this constructor
downloads AND EXECUTES arbitrary Python from the Hub repo named by `model` the first
time it's loaded. There is no way to load this specific model family without accepting
that tradeoff. Mitigation is at the config layer, not here: `model` comes from
`EmbeddingConfig.model` in the workspace's own `codegraph.yaml` (a value the workspace
owner pins and commits), never from per-request/per-query user input -- treat changing
it with the same care as adding a new third-party dependency.

Construction is eager, not the encode calls: both the `sentence_transformers` import
AND the model load happen here in `__init__` (one of the two lazy-import points the
brief allows -- the other being "on first call" -- chosen here because `dim` is a
plain, always-available attribute afterwards, backed by
`get_embedding_dimension()`/`get_sentence_embedding_dimension()`, rather than a
property that would need its own first-call/caching dance like `openai_emb`/
`voyage`'s probe-based `dim` does).

VERSION COMPATIBILITY (confirmed live, not just from docs -- see the M3 T5 report):
jina-embeddings-v2-base-code's Hub-hosted remote code imports
`find_pruneable_heads_and_indices` from `transformers.pytorch_utils`, a name removed
in `transformers` 5.x (confirmed against 5.13.1: `ImportError`). `sentence-transformers`
itself declares `transformers<6.0.0,>=4.41.0` as compatible, which is too wide for this
specific model -- `pyproject.toml`'s `local-emb` extra additionally pins
`transformers<5` to stay on the last stable pre-5.0 line (4.57.x at the time this was
written), which loads this model successfully. Revisit this pin if a future version of
this model's remote code (a new commit on the Hub, out of this repo's control) is
updated for `transformers` 5.x.
"""

from __future__ import annotations

from collections.abc import Sequence

from codegraph.core.errors import CodegraphError

_IMPORT_HINT = (
    "sentence-transformers not installed -- run `uv sync --extra local-emb` to "
    "install the local-embedding extra (sentence-transformers + torch)."
)
_LOAD_HINT = (
    "failed to load local embedding model {model!r} ({error}) -- this can happen from "
    "a version-incompatible transformers install (see this module's own \"VERSION "
    "COMPATIBILITY\" docstring section), a network/HF-Hub-access problem on first "
    "download, or a bad model name; try `uv sync --extra local-emb` to (re)install a "
    "known-compatible version, or check network access to huggingface.co."
)


class LocalEmbedder:
    def __init__(self, model: str, query_prefix: str = "", passage_prefix: str = ""):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise CodegraphError(_IMPORT_HINT) from e

        try:
            self._model = SentenceTransformer(model, trust_remote_code=True)
            # sentence-transformers renamed get_sentence_embedding_dimension() to
            # get_embedding_dimension() (the old name still works but emits
            # FutureWarning, confirmed live against sentence-transformers 5.6.0) --
            # prefer the current name, fall back for older installed versions that
            # predate the rename rather than pin to just one.
            if hasattr(self._model, "get_embedding_dimension"):
                self.dim = self._model.get_embedding_dimension()
            else:
                self.dim = self._model.get_sentence_embedding_dimension()
        except Exception as e:
            # Broad on purpose (M3 T6 fix, code review finding + sweep follow-up):
            # BOTH the constructor AND the dimension probe right after it run
            # arbitrary third-party Hub code (trust_remote_code=True, see module
            # docstring), and the constructor may hit the network on first download --
            # ANY exception from either (a known transformers-version ImportError from
            # deep inside that remote code -- see "VERSION COMPATIBILITY" above -- a
            # network/HF-Hub error, a bad model name, or a model whose remote code
            # constructs fine but blows up resolving its own embedding dimension) must
            # degrade the SAME way a missing package does, not crash the whole
            # `codegraph index` run. cli.py's `_make_embedder_or_warn` only ever
            # catches CodegraphError specifically so a genuine bug elsewhere in THIS
            # codebase still surfaces as a traceback -- narrowing THIS except to one
            # specific exception type (or leaving the dim probe OUTSIDE the try, as
            # the first version of this fix did -- the sweep reviewer's catch) would
            # leave that same CLI-level "S8 degrades gracefully" promise silently
            # broken for every other failure mode this remote-code-executing call
            # pair can raise.
            raise CodegraphError(_LOAD_HINT.format(model=model, error=e)) from e

        self.model_id = model
        # M5 T6: instruction prefixes some models (e.g. intfloat/multilingual-e5-*'s
        # canonical "query: "/"passage: ") expect prepended to distinguish a search
        # QUERY from an indexed PASSAGE at encode time -- see EmbeddingConfig's own
        # docstring for where these come from and why they're LocalEmbedder-only.
        # "" (the default for both) makes both prefix expressions below a no-op
        # string concat, so a caller that never sets either gets byte-identical
        # behavior to before this parameter existed.
        self._query_prefix = query_prefix
        self._passage_prefix = passage_prefix
        # M4 T8: local, CPU/GPU-bound compute (not a network call) -- concurrent
        # Python threads would mostly just serialize on the GIL around whatever isn't
        # already vectorized C/CUDA inside sentence-transformers, with no reliable
        # wall-clock win, and this specific model's trust_remote_code=True custom
        # forward pass has no documented thread-safety guarantee either -- stays on
        # chunk_embed's default sequential path (see embedding/base.py's own Protocol
        # docstring for the full rationale, including openai/voyage's opposite case).
        self.concurrency_safe = False

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Bulk PASSAGE path (S8 embeds staged chunk bodies) -- every text gets
        `self._passage_prefix` prepended before encoding (M5 T6)."""
        if not texts:
            return []
        prefixed = [self._passage_prefix + t for t in texts]
        vectors = self._model.encode(prefixed, normalize_embeddings=True)
        return vectors.tolist()

    def embed_query(self, text: str) -> list[float]:
        """Single QUERY path (retrieval time) -- `self._query_prefix` prepended
        (M5 T6). Deliberately NOT `self.embed_batch([text])[0]` (the Protocol's own
        default, see embedding/base.py) once query_prefix/passage_prefix can differ:
        that delegation would run the query text through `embed_batch`'s
        PASSAGE-prefixing instead, mixing the two up -- this method encodes directly
        so the query only ever sees `_query_prefix`."""
        vectors = self._model.encode([self._query_prefix + text], normalize_embeddings=True)
        return vectors.tolist()[0]
