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
from codegraph.embedding.base import Embedder
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
# Embedder Protocol -- embed_query default body (base.py, M5 T6)
# ---------------------------------------------------------------------------


class _MinimalEmbedder(Embedder):
    """Nominally subclasses `Embedder` (real inheritance, not just the structural
    "duck typing" every REAL provider in this package relies on -- see base.py's own
    module docstring) and implements ONLY `embed_batch`. Exists purely to exercise
    the Protocol's own default `embed_query` body in isolation: none of
    fake/local/openai/voyage ever fall through to it (each defines its own
    `embed_query` -- voyage's asymmetrically, via `input_type`), so without a class
    like this one the default body would be unexercised by anything else in this
    suite."""

    model_id = "minimal"
    dim = 3

    def embed_batch(self, texts):
        return [[float(len(t)), 0.0, 0.0] for t in texts]


def test_embedder_protocol_embed_query_default_delegates_to_embed_batch():
    emb = _MinimalEmbedder()
    assert emb.embed_query("abc") == [3.0, 0.0, 0.0]


def test_embedder_protocol_embed_query_default_is_first_and_only_batch_result():
    emb = _MinimalEmbedder()
    assert emb.embed_query("") == [0.0, 0.0, 0.0]


# ---------------------------------------------------------------------------
# factory.make_embedder -- dispatch (provider -> class, cfg.model forwarded)
# ---------------------------------------------------------------------------


class _StubEmbedder:
    """Records the constructor argument; stands in for a real provider class so
    dispatch tests never construct a real Local/OpenAI/VoyageEmbedder (which
    would need a real package/network/API key just to prove factory picked the
    right class). `query_prefix`/`passage_prefix` (M5 T6): accepted-and-ignored
    keyword-only, matching ONLY the shape `factory.make_embedder` actually calls
    `LocalEmbedder` with (`cfg.model` positional + these two kwargs) -- the
    openai/voyage dispatch tests below construct this same stub with just a bare
    positional `model`, which still works since both new params default."""

    last_model: str | None = None

    def __init__(self, model: str, *, query_prefix: str = "", passage_prefix: str = ""):
        type(self).last_model = model
        self.model_id = model
        self.dim = 1
        self.query_prefix = query_prefix
        self.passage_prefix = passage_prefix

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


def test_make_embedder_local_threads_query_and_passage_prefixes(monkeypatch):
    # M5 T6: EmbeddingConfig's two new prefix fields must reach LocalEmbedder's
    # constructor -- openai/voyage deliberately do NOT receive them (see
    # embedding/local.py's own module docstring for why this is local-only).
    monkeypatch.setattr("codegraph.embedding.factory.LocalEmbedder", _StubEmbedder)
    cfg = EmbeddingConfig(
        provider="local",
        model="intfloat/multilingual-e5-base",
        query_prefix="query: ",
        passage_prefix="passage: ",
    )
    result = make_embedder(cfg)
    assert result.query_prefix == "query: "
    assert result.passage_prefix == "passage: "


def test_make_embedder_local_prefixes_default_empty(monkeypatch):
    monkeypatch.setattr("codegraph.embedding.factory.LocalEmbedder", _StubEmbedder)
    cfg = EmbeddingConfig(provider="local", model="jinaai/jina-embeddings-v2-base-code")
    result = make_embedder(cfg)
    assert result.query_prefix == ""
    assert result.passage_prefix == ""


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


def test_local_embedder_sets_concurrency_safe_false(monkeypatch):
    """M4 T8: local embedding is CPU/GPU-bound compute, not a remote API call --
    chunk_embed's concurrent-batch path is opt-in (`concurrency_safe=True`), and
    `LocalEmbedder` never opts in (see embedding/base.py's own Protocol docstring for
    the full rationale, and openai_emb.py/voyage.py's opposite case below)."""
    from codegraph.embedding.local import LocalEmbedder

    fake_module = types.ModuleType("sentence_transformers")

    class _WorkingSentenceTransformer:
        def __init__(self, model, trust_remote_code=False):
            pass

        def get_embedding_dimension(self):
            return 5

    fake_module.SentenceTransformer = _WorkingSentenceTransformer
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    emb = LocalEmbedder("some-model")
    assert emb.concurrency_safe is False


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


class _RecordingSentenceTransformer:
    """Stands in for `sentence_transformers.SentenceTransformer`: records every
    `.encode(texts, ...)` call's texts (in order, across calls) and returns an
    object shaped like a real `numpy` result (`.tolist()`) so `LocalEmbedder.
    embed_batch`'s own `vectors.tolist()` call works unmodified. One vector per
    input text, `[len(text), 0.0, 0.0]` -- deterministic and enough to prove WHAT
    text (prefixed or not) actually reached the model, which is the only thing
    these prefix tests care about."""

    def __init__(self, model, trust_remote_code=False):
        self.calls: list[list[str]] = []

    def get_embedding_dimension(self):
        return 3

    def encode(self, texts, normalize_embeddings=True):
        self.calls.append(list(texts))
        rows = [[float(len(t)), 0.0, 0.0] for t in texts]
        return SimpleNamespace(tolist=lambda: rows)


def _install_fake_sentence_transformers(monkeypatch) -> dict:
    """Same `captured`-dict-populated-by-the-constructor technique as
    `_install_fake_openai`/`_install_fake_voyageai` below -- lets a test reach the
    ONE `_RecordingSentenceTransformer` instance `LocalEmbedder.__init__` built,
    after the fact, to inspect `.calls`."""
    captured: dict[str, _RecordingSentenceTransformer] = {}

    class _Factory(_RecordingSentenceTransformer):
        def __init__(self, model, trust_remote_code=False):
            super().__init__(model, trust_remote_code)
            captured["model"] = self

    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = _Factory
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    return captured


def test_local_embedder_default_prefixes_empty_leaves_text_untouched(monkeypatch):
    """M5 T6 backward-compat anchor: query_prefix/passage_prefix default to "" ->
    embed_batch/embed_query must see the exact, unprefixed input text -- byte-
    identical to pre-M5-T6 behavior for every existing workspace that never sets
    these two new config fields."""
    from codegraph.embedding.local import LocalEmbedder

    captured = _install_fake_sentence_transformers(monkeypatch)
    emb = LocalEmbedder("some-model")
    emb.embed_batch(["def f(): pass", "class C: pass"])
    emb.embed_query("def f(): pass")
    assert captured["model"].calls == [
        ["def f(): pass", "class C: pass"],
        ["def f(): pass"],
    ]


def test_local_embedder_passage_prefix_applied_in_embed_batch_only(monkeypatch):
    from codegraph.embedding.local import LocalEmbedder

    captured = _install_fake_sentence_transformers(monkeypatch)
    emb = LocalEmbedder("some-model", passage_prefix="passage: ")
    emb.embed_batch(["def f(): pass", "class C: pass"])
    assert captured["model"].calls == [["passage: def f(): pass", "passage: class C: pass"]]


def test_local_embedder_query_prefix_applied_in_embed_query_only(monkeypatch):
    from codegraph.embedding.local import LocalEmbedder

    captured = _install_fake_sentence_transformers(monkeypatch)
    emb = LocalEmbedder("some-model", query_prefix="query: ")
    emb.embed_query("where is X defined")
    assert captured["model"].calls == [["query: where is X defined"]]


def test_local_embedder_query_and_passage_prefixes_are_independent(monkeypatch):
    """Canonical e5 usage (M5 T6): query_prefix != passage_prefix. embed_batch
    (indexing passages) must never see query_prefix and embed_query must never see
    passage_prefix -- a naive `embed_query = embed_batch([text])[0]` delegation
    (correct for the OTHER providers, still the Protocol's own default -- see the
    base.py section above) would incorrectly apply passage_prefix to a query, so
    LocalEmbedder can no longer implement embed_query that way once prefixes
    differ."""
    from codegraph.embedding.local import LocalEmbedder

    captured = _install_fake_sentence_transformers(monkeypatch)
    emb = LocalEmbedder("some-model", query_prefix="query: ", passage_prefix="passage: ")
    emb.embed_batch(["alpha", "beta"])
    emb.embed_query("gamma")
    assert captured["model"].calls == [
        ["passage: alpha", "passage: beta"],
        ["query: gamma"],
    ]


def test_local_embedder_embed_batch_empty_list_still_short_circuits(monkeypatch):
    # Pre-existing contract (`[]` in -> `[]` out, no model call at all) must survive
    # the prefix change -- an empty batch has no texts to prefix in the first place.
    from codegraph.embedding.local import LocalEmbedder

    captured = _install_fake_sentence_transformers(monkeypatch)
    emb = LocalEmbedder("some-model", passage_prefix="passage: ")
    assert emb.embed_batch([]) == []
    assert captured["model"].calls == []


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


def test_openai_embedder_sets_concurrency_safe_true(monkeypatch):
    """M4 T8: a remote HTTP API call -- safe (and worth it) to fire concurrently from
    `chunk_embed._embed_missing`, unlike local/fake (see embedding/base.py's own
    Protocol docstring)."""
    from codegraph.embedding.openai_emb import OpenAIEmbedder

    _install_fake_openai(monkeypatch)
    emb = OpenAIEmbedder(model="text-embedding-3-small")
    assert emb.concurrency_safe is True


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


def test_voyage_embedder_sets_concurrency_safe_true(monkeypatch):
    """M4 T8: a remote HTTP API call -- safe (and worth it) to fire concurrently from
    `chunk_embed._embed_missing`, unlike local/fake (see embedding/base.py's own
    Protocol docstring)."""
    from codegraph.embedding.voyage import VoyageEmbedder

    _install_fake_voyageai(monkeypatch)
    emb = VoyageEmbedder(model="voyage-code-3")
    assert emb.concurrency_safe is True
