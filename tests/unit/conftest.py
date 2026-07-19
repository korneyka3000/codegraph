"""Fixtures shared by the whole `tests/unit/` suite.

M5 T7 (carried from the M4-T9 implementer note): `codegraph.cli.make_embedder` is the
zero-config default path for EVERY `codegraph index [--incremental]`/`codegraph eval
retrieval` CLI invocation that doesn't pass `--no-embed` and doesn't itself monkeypatch
`make_embedder` -- a bare `codegraph.yaml` defaults `EmbeddingConfig.provider` to
"local" (see config/models.py), so `_make_embedder_or_warn`/`eval_retrieval` construct a
REAL `LocalEmbedder` (sentence-transformers model load, live-measured ~6-8s EACH --
`SentenceTransformer(...)` re-loads the model from scratch every call, no cross-call
caching) whenever the `local-emb` extra happens to be installed. `tests/unit/
test_cli_m1b.py`/`test_cli_index.py` never touch `codegraph.cli.make_embedder` at all
(they monkeypatch analyze_service/link_workspace/load_graph/FalkorStore, but the
embedder those two files' `index` invocations construct is either never used for
encoding at all -- both files' fake `analyze_service` never actually stages a file, so
`_embed_missing` finds zero missing chunks -- or immediately handed to a `run_chunk_embed`
fake that ignores it outright, see test_cli_index.py's `_chunk_embed_spy`) -- meaning
every one of their non-`--dry-run` `index` invocations paid the full real-construction
cost for a return value neither file's assertions ever inspect. Live-measured before this
fixture existed: 35 tests / 130.5s combined for just these two files (junit `time`
attribute, `.superpowers/sdd/task-7-report.md` has the full before/after numbers).

This fixture patches `codegraph.cli.make_embedder` specifically (not `codegraph.
embedding.factory.make_embedder`, the function's own definition) -- cli.py imports it by
bare name (`from codegraph.embedding.factory import make_embedder`, module docstring:
"analyze_service/.../build_server импортированы по имени... НАМЕРЕННО"), so the name
`codegraph.cli` actually calls at runtime resolves out of `codegraph.cli`'s OWN module
namespace, not the factory module's -- patching the factory's copy would leave `cli.py`'s
already-bound reference untouched (every existing make_embedder-patching test in this
suite, e.g. test_cli_chunk_embed.py/test_cli_eval.py, already targets this exact
attribute for the identical reason).

Autouse, not opt-in: every test in `tests/unit/` gets a working fake embedder for free
UNLESS it re-monkeypatches `codegraph.cli.make_embedder` itself afterward in its own body
(a test that needs different behavior -- a real `CodegraphError` degrade path, a spy
recording calls, a specific dim/model_id -- simply calls `monkeypatch.setattr(
"codegraph.cli.make_embedder", ...)` again; `monkeypatch` doesn't care how many times the
same target was already set THIS test, and restores the TRUE original at teardown either
way, fixture-set fake included -- verified against every existing make_embedder-patching
test in this suite: none of them relies on the REAL factory ever running, they all
replace it themselves, so this fixture changes zero existing assertions). Importing
`codegraph.cli` itself is cheap and side-effect-free at module level (no torch, no
network -- see embedding/factory.py's own module docstring), so paying this fixture's
tiny per-test monkeypatch cost even for unit tests that have nothing to do with the CLI
is not a meaningful tax.

Guard note: this fixture works identically whether `local-emb` (sentence-transformers) is
installed or not -- `FakeEmbedder` is pure Python with no optional-package dependency of
its own, so there is nothing here that could behave differently, or need skipping, in
either environment."""

from __future__ import annotations

import pytest

from codegraph.embedding.fake import FakeEmbedder


@pytest.fixture(autouse=True)
def _fake_cli_make_embedder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: FakeEmbedder())
