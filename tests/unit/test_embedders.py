"""Unit tests for `codegraph.embedding`: `FakeEmbedder` (determinism/normalization,
no dependencies) + `factory.make_embedder` dispatch and error-hint paths.

No real `sentence-transformers`/`openai`/`voyageai` package is required to run this
file. Two techniques do the work (both documented inline at first use):

- Dispatch-only tests monkeypatch the provider CLASS referenced inside
  `factory.py` with a tiny stub, so they check "did factory pick the right class
  and forward `cfg.model`" without exercising that class's own `__init__` (which
  for the real classes needs a package/network/API key).
- Error-hint tests exercise the REAL provider class's `__init__` guard clauses
  (missing env key; package not importable) end-to-end. The "package not
  importable" case uses `monkeypatch.setitem(sys.modules, "<pkg>", None)` --
  Python's import system raises `ImportError` for any `import <pkg>` while
  `sys.modules["<pkg>"] is None`, regardless of whether the real package happens
  to be installed in the venv running this test. This makes the branch
  deterministic across environments (dev venv with no extras installed, CI, a
  venv that DOES have `openai`/`voyageai`/`sentence-transformers` installed for
  other reasons, ...) instead of depending on what's importable right now.
  `openai_emb`/`voyage` additionally get full behavioral coverage (dim
  known-table/probe-and-cache, embed_batch/embed_query request shape) by
  installing a fully fake stand-in MODULE (not `None`) under the same
  `sys.modules` key, so `from openai import OpenAI` / `import voyageai` resolves
  to a small recording stub instead of `None` or the real package.
"""

import math
import sys
import types
from types import SimpleNamespace

import pytest

from codegraph.config.models import EmbeddingConfig
from codegraph.core.errors import CodegraphError
from codegraph.embedding.factory import make_embedder
from codegraph.embedding.fake import FakeEmbedder

# ---------------------------------------------------------------------------
# FakeEmbedder
# ---------------------------------------------------------------------------


def test_fake_embedder_default_dim_is_8():
    emb = FakeEmbedder()
    assert emb.dim == 8
    assert isinstance(emb.model_id, str) and emb.model_id


def test_fake_embedder_custom_dim():
    emb = FakeEmbedder(dim=16)
    assert emb.dim == 16
    assert len(emb.embed_query("hello world")) == 16


def test_fake_embedder_rejects_nonpositive_dim():
    with pytest.raises(ValueError):
        FakeEmbedder(dim=0)
    with pytest.raises(ValueError):
        FakeEmbedder(dim=-3)


def test_fake_embedder_deterministic_same_text_same_instance():
    emb = FakeEmbedder(dim=8)
    assert emb.embed_query("def add(a, b): return a + b") == emb.embed_query(
        "def add(a, b): return a + b"
    )


def test_fake_embedder_deterministic_across_instances():
    # Same text, two independently constructed embedders (stand-in for "two
    # different processes") -> byte-identical vectors. Proves determinism comes
    # purely from the text (via hashlib), not from any per-instance/per-process
    # state (e.g. `hash()`'s PYTHONHASHSEED salt would fail this).
    text = "class OrderService:\n    def place(self): ...\n"
    a = FakeEmbedder(dim=12).embed_query(text)
    b = FakeEmbedder(dim=12).embed_query(text)
    assert a == b


def test_fake_embedder_different_texts_different_vectors():
    emb = FakeEmbedder(dim=8)
    a = emb.embed_query("def add(a, b): return a + b")
    b = emb.embed_query("The quick brown fox jumps over the lazy dog.")
    assert a != b


def test_fake_embedder_unit_normalized():
    emb = FakeEmbedder(dim=8)
    for text in ["", "x", "a longer piece of code-shaped text() {}", "same" * 50]:
        vec = emb.embed_query(text)
        norm = math.sqrt(sum(v * v for v in vec))
        assert norm == pytest.approx(1.0, abs=1e-9)


def test_fake_embedder_never_zero_vector_or_zero_component():
    emb = FakeEmbedder(dim=8)
    for text in ["", "0", "zero", "null", "\x00", "a" * 1000]:
        vec = emb.embed_query(text)
        assert any(v != 0.0 for v in vec)  # not the zero vector
        assert all(v != 0.0 for v in vec)  # stronger: no component is exactly 0.0


def test_fake_embedder_embed_batch_matches_embed_query_elementwise():
    emb = FakeEmbedder(dim=8)
    texts = ["alpha", "beta", "gamma"]
    batch = emb.embed_batch(texts)
    assert batch == [emb.embed_query(t) for t in texts]


def test_fake_embedder_embed_batch_empty_list():
    emb = FakeEmbedder(dim=8)
    assert emb.embed_batch([]) == []


def test_fake_embedder_model_id_is_a_plain_string_attribute():
    # Protocol requires `model_id: str` readable without any call/argument.
    emb = FakeEmbedder(dim=8, model_id="custom-id")
    assert emb.model_id == "custom-id"


# ---------------------------------------------------------------------------
# factory.make_embedder -- dispatch (provider -> class, cfg.model forwarded)
# ---------------------------------------------------------------------------


class _StubEmbedder:
    """Records the constructor argument; stands in for a real provider class so
    dispatch tests never construct a real Local/OpenAI/VoyageEmbedder (which
    would need a real package/network/API key just to prove factory picked the
    right class)."""

    last_model: str | None = None

    def __init__(self, model: str):
        type(self).last_model = model
        self.model_id = model
        self.dim = 1

    def embed_batch(self, texts):
        return [[0.0] for _ in texts]

    def embed_query(self, text):
        return [0.0]


def test_make_embedder_dispatches_local(monkeypatch):
    monkeypatch.setattr("codegraph.embedding.factory.LocalEmbedder", _StubEmbedder)
    cfg = EmbeddingConfig(provider="local", model="jinaai/jina-embeddings-v2-base-code")
    result = make_embedder(cfg)
    assert isinstance(result, _StubEmbedder)
    assert result.model_id == "jinaai/jina-embeddings-v2-base-code"


def test_make_embedder_dispatches_openai(monkeypatch):
    monkeypatch.setattr("codegraph.embedding.factory.OpenAIEmbedder", _StubEmbedder)
    cfg = EmbeddingConfig(provider="openai", model="text-embedding-3-small")
    result = make_embedder(cfg)
    assert isinstance(result, _StubEmbedder)
    assert result.model_id == "text-embedding-3-small"


def test_make_embedder_dispatches_voyage(monkeypatch):
    monkeypatch.setattr("codegraph.embedding.factory.VoyageEmbedder", _StubEmbedder)
    cfg = EmbeddingConfig(provider="voyage", model="voyage-code-3")
    result = make_embedder(cfg)
    assert isinstance(result, _StubEmbedder)
    assert result.model_id == "voyage-code-3"


# ---------------------------------------------------------------------------
# factory.make_embedder -- error hints, exercised end-to-end (real provider
# classes' own __init__ guard clauses, not stubbed)
# ---------------------------------------------------------------------------


def test_make_embedder_local_missing_package_hint(monkeypatch):
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    cfg = EmbeddingConfig(provider="local", model="jinaai/jina-embeddings-v2-base-code")
    with pytest.raises(CodegraphError, match="uv sync --extra local-emb"):
        make_embedder(cfg)


def test_local_embedder_model_construction_failure_wraps_as_codegraph_error(monkeypatch):
    """Code review finding: the pre-fix code only wrapped the `import
    sentence_transformers` line in try/except -- the SentenceTransformer(...)
    constructor call itself (which executes arbitrary third-party Hub code, and can
    hit the network on first download) was completely unguarded, so any OTHER
    exception it raised (e.g. the documented transformers-version ImportError from
    inside a model's own remote code, or a network/HF-Hub error) propagated
    uncaught -- crashing `codegraph index` outright instead of degrading gracefully
    the same way a missing package does (cli.py's `_make_embedder_or_warn` only ever
    catches CodegraphError). Installs a fake `sentence_transformers` module (present,
    so the import itself succeeds) whose `SentenceTransformer` class raises an
    unrelated exception on construction, standing in for that exact failure mode."""
    from codegraph.embedding.local import LocalEmbedder

    fake_module = types.ModuleType("sentence_transformers")

    class _BoomSentenceTransformer:
        def __init__(self, model, trust_remote_code=False):
            raise RuntimeError("simulated remote-code/network failure")

    fake_module.SentenceTransformer = _BoomSentenceTransformer
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    with pytest.raises(CodegraphError, match="simulated remote-code/network failure"):
        LocalEmbedder("jinaai/jina-embeddings-v2-base-code")


def test_local_embedder_dim_probe_failure_also_wraps_as_codegraph_error(monkeypatch):
    """Sweep-review follow-up to the fix above: the DIMENSION PROBE right after
    construction (get_embedding_dimension()/get_sentence_embedding_dimension()) is the
    same remote-code risk class as the constructor itself -- a model whose Hub-hosted
    code constructs fine but blows up resolving its own embedding dimension must
    degrade identically (CodegraphError -> cli's yellow warning), not crash
    `codegraph index` with a raw traceback. The first version of the fix wrapped only
    the constructor call, leaving the probe outside the try -- this test pins the
    corrected placement."""
    from codegraph.embedding.local import LocalEmbedder

    fake_module = types.ModuleType("sentence_transformers")

    class _DimBoomSentenceTransformer:
        def __init__(self, model, trust_remote_code=False):
            pass  # construction succeeds

        def get_embedding_dimension(self):
            raise RuntimeError("simulated dim-probe failure from remote code")

    fake_module.SentenceTransformer = _DimBoomSentenceTransformer
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    with pytest.raises(CodegraphError, match="simulated dim-probe failure"):
        LocalEmbedder("jinaai/jina-embeddings-v2-base-code")


def test_make_embedder_openai_missing_key_hint(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    cfg = EmbeddingConfig(provider="openai", model="text-embedding-3-small")
    with pytest.raises(CodegraphError, match="OPENAI_API_KEY"):
        make_embedder(cfg)


def test_make_embedder_voyage_missing_key_hint(monkeypatch):
    monkeypatch.delenv("VOYAGE_API_KEY", raising=False)
    cfg = EmbeddingConfig(provider="voyage", model="voyage-code-3")
    with pytest.raises(CodegraphError, match="VOYAGE_API_KEY"):
        make_embedder(cfg)


def test_make_embedder_openai_missing_package_hint(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy")
    monkeypatch.setitem(sys.modules, "openai", None)
    cfg = EmbeddingConfig(provider="openai", model="text-embedding-3-small")
    with pytest.raises(CodegraphError, match="uv sync --extra openai"):
        make_embedder(cfg)


def test_make_embedder_voyage_missing_package_hint(monkeypatch):
    monkeypatch.setenv("VOYAGE_API_KEY", "voyage-test-dummy")
    monkeypatch.setitem(sys.modules, "voyageai", None)
    cfg = EmbeddingConfig(provider="voyage", model="voyage-code-3")
    with pytest.raises(CodegraphError, match="uv sync --extra voyage"):
        make_embedder(cfg)


# ---------------------------------------------------------------------------
# OpenAIEmbedder -- full behavioral coverage against a fake `openai` module
# ---------------------------------------------------------------------------


class _FakeOpenAIClient:
    """Stands in for `openai.OpenAI`: records every `.embeddings.create(...)`
    call and returns a response shaped like the real SDK's
    (`response.data[i].embedding`)."""

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.embeddings = self
        self.calls: list[tuple[str, list[str]]] = []

    def create(self, model, input):
        self.calls.append((model, list(input)))
        vectors = [[float(len(t)), 1.0, 2.0] for t in input]
        return SimpleNamespace(data=[SimpleNamespace(embedding=v) for v in vectors])


def _install_fake_openai(monkeypatch) -> _FakeOpenAIClient:
    captured: dict[str, _FakeOpenAIClient] = {}

    class _Factory(_FakeOpenAIClient):
        def __init__(self, api_key=None):
            super().__init__(api_key)
            captured["client"] = self

    fake_module = types.ModuleType("openai")
    fake_module.OpenAI = _Factory
    monkeypatch.setitem(sys.modules, "openai", fake_module)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy")
    return captured  # populated once OpenAIEmbedder() constructs the client


def test_openai_embedder_known_model_dim_needs_no_probe(monkeypatch):
    from codegraph.embedding.openai_emb import OpenAIEmbedder

    captured = _install_fake_openai(monkeypatch)
    emb = OpenAIEmbedder(model="text-embedding-3-small")
    assert emb.dim == 1536  # from the known-dims table
    assert captured["client"].calls == []  # no probe call was made


def test_openai_embedder_unknown_model_probes_once_and_caches(monkeypatch):
    from codegraph.embedding.openai_emb import OpenAIEmbedder

    captured = _install_fake_openai(monkeypatch)
    emb = OpenAIEmbedder(model="some-future-model")
    assert emb.dim == 3  # len() of the fake client's canned vector
    assert emb.dim == 3  # second access: still 3, no second probe
    assert len(captured["client"].calls) == 1


def test_openai_embedder_embed_batch_and_query(monkeypatch):
    from codegraph.embedding.openai_emb import OpenAIEmbedder

    captured = _install_fake_openai(monkeypatch)
    emb = OpenAIEmbedder(model="text-embedding-3-small")
    batch = emb.embed_batch(["ab", "cde"])
    assert batch == [[2.0, 1.0, 2.0], [3.0, 1.0, 2.0]]
    assert emb.embed_query("ab") == [2.0, 1.0, 2.0]
    models_used = {call[0] for call in captured["client"].calls}
    assert models_used == {"text-embedding-3-small"}


def test_openai_embedder_embed_batch_empty_list(monkeypatch):
    from codegraph.embedding.openai_emb import OpenAIEmbedder

    _install_fake_openai(monkeypatch)
    emb = OpenAIEmbedder(model="text-embedding-3-small")
    assert emb.embed_batch([]) == []


# ---------------------------------------------------------------------------
# VoyageEmbedder -- full behavioral coverage against a fake `voyageai` module
# ---------------------------------------------------------------------------


class _FakeVoyageClient:
    """Stands in for `voyageai.Client`: records every `.embed(...)` call
    (including `input_type`) and returns a response shaped like the real SDK's
    (`response.embeddings`)."""

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.calls: list[tuple[str, list[str], str]] = []

    def embed(self, texts, model, input_type):
        self.calls.append((model, list(texts), input_type))
        vectors = [[float(len(t)), 9.0, 8.0] for t in texts]
        return SimpleNamespace(embeddings=vectors)


def _install_fake_voyageai(monkeypatch) -> dict:
    captured: dict[str, _FakeVoyageClient] = {}

    class _Factory(_FakeVoyageClient):
        def __init__(self, api_key=None):
            super().__init__(api_key)
            captured["client"] = self

    fake_module = types.ModuleType("voyageai")
    fake_module.Client = _Factory
    monkeypatch.setitem(sys.modules, "voyageai", fake_module)
    monkeypatch.setenv("VOYAGE_API_KEY", "voyage-test-dummy")
    return captured


def test_voyage_embedder_known_model_dim_needs_no_probe(monkeypatch):
    from codegraph.embedding.voyage import VoyageEmbedder

    captured = _install_fake_voyageai(monkeypatch)
    emb = VoyageEmbedder(model="voyage-code-3")
    assert emb.dim == 1024  # from the known-dims table
    assert captured["client"].calls == []  # no probe call was made


def test_voyage_embedder_unknown_model_probes_once_and_caches(monkeypatch):
    from codegraph.embedding.voyage import VoyageEmbedder

    captured = _install_fake_voyageai(monkeypatch)
    emb = VoyageEmbedder(model="some-future-model")
    assert emb.dim == 3
    assert emb.dim == 3
    assert len(captured["client"].calls) == 1


def test_voyage_embedder_batch_uses_document_query_uses_query_input_type(monkeypatch):
    from codegraph.embedding.voyage import VoyageEmbedder

    captured = _install_fake_voyageai(monkeypatch)
    emb = VoyageEmbedder(model="voyage-code-3")
    emb.embed_batch(["ab", "cde"])
    emb.embed_query("xy")
    input_types = [call[2] for call in captured["client"].calls]
    assert input_types == ["document", "query"]


def test_voyage_embedder_embed_batch_empty_list(monkeypatch):
    from codegraph.embedding.voyage import VoyageEmbedder

    _install_fake_voyageai(monkeypatch)
    emb = VoyageEmbedder(model="voyage-code-3")
    assert emb.embed_batch([]) == []
