"""M3 T6: pipeline.chunk_embed.run (S8) -- unit tests with FakeEmbedder.

Uses a small synthetic tmp_path-based workspace (own service dirs + own .py files),
`analyze_service` run in degraded mode (`_AlwaysFailRunner`, same technique as
test_augment.py's real-fixture harness) so `chunk_embed.run` sees the exact same
staged nodes shape a real `codegraph index` run would produce, without needing a real
scip-python subprocess.

Both mandatory T3/T4 carries get a DIRECT test here (in addition to their own
lower-level tests in test_staging.py/test_augment.py):
  - `test_edited_chunk_only_gets_re_embedded_the_second_time` (T3 carry: hash-aware
    re-embed) -- edits ONE function's body on disk between two `chunk_embed.run` calls
    on the SAME staging session (no intervening `begin_service`/re-analyze) and proves
    only that one chunk's embedding blob actually changed.
  - `test_run_builds_header_index_exactly_once_not_per_service` (T4 carry: one header
    index per workspace) -- spies on `augment._build_index` across a 3-service run.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from codegraph.chunking import augment
from codegraph.config.models import ServiceConfig, WorkspaceConfig
from codegraph.embedding.codec import pack_vector, unpack_vector
from codegraph.embedding.fake import FakeEmbedder
from codegraph.pipeline import chunk_embed
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.staging import Staging


class _AlwaysFailRunner:
    """Same technique as test_augment.py's real-fixture harness -- forces the degraded
    heuristic-fallback path without a real scip-python subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def _write_service(tmp_path: Path, name: str, files: dict[str, str]) -> ServiceConfig:
    svc_dir = tmp_path / name
    svc_dir.mkdir(parents=True, exist_ok=True)
    for relpath, content in files.items():
        p = svc_dir / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    return ServiceConfig(name=name, path=svc_dir)


def _cfg(*services: ServiceConfig) -> WorkspaceConfig:
    return WorkspaceConfig(graph_name="t", services=list(services))


def _analyze_all(cfg: WorkspaceConfig, staging: Staging, tmp_path: Path) -> None:
    for svc in cfg.services:
        analyze_service(svc, staging, tmp_path / "cache", runner=_AlwaysFailRunner())


SRC_TWO_FUNCS = "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n"
SRC_ONE_FUNC = "def mul(a, b):\n    return a * b\n"


def _many_funcs_src(n: int) -> str:
    return "\n\n".join(f"def f{i}():\n    return {i}\n" for i in range(n))


class _RecordingEmbedder:
    """Wraps a real Embedder, recording each `embed_batch` call's size -- for asserting
    the <=64 micro-batching contract without depending on FakeEmbedder internals."""

    def __init__(self, inner):
        self._inner = inner
        self.model_id = inner.model_id
        self.dim = inner.dim
        self.batch_sizes: list[int] = []

    def embed_batch(self, texts):
        self.batch_sizes.append(len(texts))
        return self._inner.embed_batch(texts)

    def embed_query(self, text):
        return self._inner.embed_query(text)


class _SlowBatchEmbedder:
    """Wraps FakeEmbedder with an artificial 50ms sleep per `embed_batch` call --
    stands in for a real openai/voyage network round-trip (M4 T8, review-fixed shape:
    was `_ConcurrentSlowEmbedder` with a hardwired `concurrency_safe=True`; the
    sequential-control phase the review added needs the SAME delay through the
    sequential path, so both knobs moved to the constructor).

      - `concurrency_safe`: True opts into chunk_embed's concurrent-batch path (see
        embedding/base.py's Protocol docstring); False pins the sequential control,
        whose wall-clock floor is mathematical, not statistical -- `time.sleep(0.05)`
        guarantees AT LEAST 0.05s each, so 4 sequential batches cost >= 200ms in
        sleeps alone, no scheduling luck involved.
      - `model_id`: the timing test's two phases share ONE staging DB; a distinct
        model_id makes `chunks_missing_embedding` re-flag every row for the second
        phase (embed_model mismatch) while the first phase's persistent-cache entries
        can't serve it either (cache key includes embed_model) -- a genuine
        4-fresh-batches workload both times.

    Write-order determinism under the concurrent path is NOT a function of the delay
    pattern: with a UNIFORM delay, thread completion order isn't a controlled variable
    at all -- what pins determinism is `ThreadPoolExecutor.map`'s own documented
    guarantee that its iterator yields results in CALL order, not completion order,
    so `_embed_missing`'s write loop is unaffected by scheduling."""

    _DELAY_SECONDS = 0.05

    def __init__(self, concurrency_safe: bool, model_id: str | None = None):
        self._inner = FakeEmbedder(dim=8, model_id=model_id)
        self.model_id = self._inner.model_id
        self.dim = self._inner.dim
        self.concurrency_safe = concurrency_safe
        self.batch_sizes: list[int] = []

    def embed_batch(self, texts):
        time.sleep(self._DELAY_SECONDS)
        self.batch_sizes.append(len(texts))  # list.append -- GIL-atomic, no lock needed
        return self._inner.embed_batch(texts)

    def embed_query(self, text):
        return self._inner.embed_query(text)


class _FailsOnPoisonedBatchEmbedder:
    """FakeEmbedder wrapper that raises on exactly ONE batch, identified by CONTENT
    (a received text ending with one of `poison_texts` -- `augment_text` output always
    ends with the chunk's own raw text), not by call order: under the concurrent path
    call ORDER is scheduler-dependent, but batch COMPOSITION is deterministic
    (`chunks_missing_embedding`'s ORDER BY chunk_id, sliced by 64), so poisoning a
    known batch's texts fails that batch -- and only that batch -- on both paths."""

    def __init__(self, poison_texts: list[str], concurrency_safe: bool):
        self._inner = FakeEmbedder(dim=8)
        self.model_id = self._inner.model_id
        self.dim = self._inner.dim
        self.concurrency_safe = concurrency_safe
        self._poison = tuple(poison_texts)

    def embed_batch(self, texts):
        if any(t.endswith(p) for t in texts for p in self._poison):
            raise RuntimeError("simulated provider failure on the poisoned batch")
        return self._inner.embed_batch(texts)

    def embed_query(self, text):
        return self._inner.embed_query(text)


# ======================================================================================
# -- happy path --
# ======================================================================================


def test_run_chunks_all_services_and_embeds_everything(tmp_path):
    svc_a = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    assert report == {
        "chunks_total": 3, "embedded": 3, "embedded_fresh": 3,
        "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 0,
    }

    rows = list(staging.iter_chunks())
    assert len(rows) == 3
    for row in rows:
        assert row.embedding is not None
        assert row.embed_model == "fake-8d"
        # M4 T1: embedded_hash is compared against (and now equals) input_hash, not
        # content_hash any more -- both are non-None and fresh after a real run
        # (fill_headers_all always populates input_hash before the embed pass).
        assert row.input_hash is not None
        assert row.embedded_hash == row.input_hash
        assert row.context_header is not None
        assert row.context_header.startswith("file:")


def test_run_covers_every_configured_service_not_just_the_first(tmp_path):
    services = [_write_service(tmp_path, f"svc{i}", {"m.py": SRC_ONE_FUNC}) for i in range(3)]
    cfg = _cfg(*services)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    assert report["chunks_total"] == 3
    staged_services = {row.service for row in staging.iter_chunks()}
    assert staged_services == {"svc0", "svc1", "svc2"}


# ======================================================================================
# -- REGRESSION (master-plan gate): re-run with no changes -> embedded==0, reused==total
# ======================================================================================


def test_rerun_without_changes_embeds_zero_and_reuses_all(tmp_path):
    svc = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    first = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert first["embedded"] == first["chunks_total"] == 2
    assert first["embedded_fresh"] == 2
    assert first["embedded_from_cache"] == 0
    assert first["reused"] == 0

    before = {row.chunk_id: row.embedding for row in staging.iter_chunks()}

    second = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert second["chunks_total"] == 2
    assert second["embedded"] == 0
    assert second["reused"] == 2
    # These rows were never "missing" at all (WITHIN-run reuse, chunks table
    # untouched) -- distinct from a genuine embedding_cache HIT (see
    # test_repeat_run_after_service_rewipe_reuses_persistent_cache_with_zero_
    # provider_calls below for that path): neither fresh nor cache-sourced.
    assert second["embedded_fresh"] == 0
    assert second["embedded_from_cache"] == 0

    after = {row.chunk_id: row.embedding for row in staging.iter_chunks()}
    assert after == before  # byte-identical -- nothing was re-embedded


# ======================================================================================
# -- M4 T1: persistent, cross-run embedding cache (Staging.embedding_cache table) --
# ======================================================================================


def test_repeat_run_after_service_rewipe_reuses_persistent_cache_with_zero_provider_calls(
    tmp_path,
):
    """The M4 T1 headline behavior (the master-plan cross-run promise), proven here at
    the chunk_embed.run level (see tests/integration/test_pipeline_chunk_embed.py for
    the same proof against the real fixtures, end to end). Unlike
    test_rerun_without_changes_embeds_zero_and_reuses_all above -- which only proves
    chunks SURVIVING untouched in staging reuse their in-place embedding (the
    `reused` counter, `chunks_missing_embedding` never even sees these rows as
    missing) -- this wipes and regenerates the `chunks` rows themselves (a second
    `analyze_service` pass over the SAME unchanged files: exactly what a real repeat
    `codegraph index` run does, since `analyze_service` always begins with its own
    `begin_service`) and proves the SECOND `run` makes ZERO `embed_batch` calls:
    every chunk is served from the persistent `embedding_cache` table instead, which
    `begin_service` never touches."""
    svc = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    embedder = _RecordingEmbedder(FakeEmbedder(dim=8))
    first = chunk_embed.run(cfg, staging, embedder)
    assert first["chunks_total"] == 2
    assert first["embedded_fresh"] == 2
    assert first["embedded_from_cache"] == 0
    assert first["embedded"] == 2
    assert len(embedder.batch_sizes) == 1  # one genuine provider call happened

    # keyed by symbol_id, not chunk_id: chunk_ids are stable across the re-analyze
    # below (same file, same defs, same span-derived ids) anyway, but symbol_id is
    # the more fundamental invariant this test leans on.
    before = {row.symbol_id: row.embedding for row in staging.iter_chunks()}

    # Simulate a repeat `codegraph index` run: re-analyze the SAME unchanged files --
    # begin_service (inside analyze_service) wipes+recreates this service's `chunks`
    # rows (embedding=NULL) from scratch, with the SAME content -- deterministic ids
    # and content_hash, so the SAME input_hash once fill_headers_all re-renders it.
    _analyze_all(cfg, staging, tmp_path)
    embedder.batch_sizes.clear()

    second = chunk_embed.run(cfg, staging, embedder)
    assert second["chunks_total"] == 2
    assert second["embedded_fresh"] == 0  # the master-plan gate itself
    assert second["embedded_from_cache"] == 2
    assert second["embedded"] == 2
    # these rows WERE missing (fresh chunk rows, embedding=NULL) -- served from cache,
    # not the WITHIN-run "reused" path (which never even calls chunks_missing_embedding
    # positively for them).
    assert second["reused"] == 0
    assert embedder.batch_sizes == []  # embed_batch never called at all

    # The cache-reused vectors are byte-identical to what the first run's real
    # provider call actually produced.
    after = {row.symbol_id: row.embedding for row in staging.iter_chunks()}
    assert after == before


def test_embedding_cache_hit_requires_matching_dim(tmp_path):
    """A cached vector whose DECODED dimension doesn't match the CURRENT embedder's
    `dim` is treated as a cache MISS, not silently reused (see _embed_missing's own
    docstring) -- constructed directly (same model_id string, genuinely different
    dim) rather than relying on a hard-to-arrange natural repro."""
    svc = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8, model_id="shared-model"))
    assert staging.get_meta("embed_dim") == "8"

    _analyze_all(cfg, staging, tmp_path)  # wipe+recreate this service's chunks rows
    # SAME model_id string, but a genuinely DIFFERENT dim -- the cached 8-dim vector
    # under "shared-model" no longer matches this embedder's own dim=4.
    spy = _RecordingEmbedder(FakeEmbedder(dim=4, model_id="shared-model"))
    report = chunk_embed.run(cfg, staging, spy)

    assert report["embedded_from_cache"] == 0
    assert report["embedded_fresh"] == 1  # fell through to a real embed_batch call
    assert len(spy.batch_sizes) == 1
    row = next(staging.iter_chunks())
    assert len(row.embedding) // 4 == 4  # the FRESH (4-dim) vector, not the stale 8-dim one


# ======================================================================================
# -- MINOR-5 (M3 final review): a file removed from disk between S1-S6 scan and S8 --
# ======================================================================================


def test_missing_file_between_scan_and_chunk_embed_is_skipped_not_fatal(tmp_path, caplog):
    """A file staged by analyze_service can be gone by the time chunk_embed.run reads
    it off disk (race: git checkout/rebase mid-index, a concurrent build step, etc.) --
    `.read_bytes()` raising OSError must warn-and-skip that one file, the same way an
    on-disk content change that shifts def spans already does (see
    `_symbol_ids_for_file`'s own docstring), not crash the whole `codegraph index` run
    and lose every other service's already-completed work."""
    svc_a = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    (svc_a.path / "m.py").unlink()  # gone by the time chunk_embed.run reads it

    with caplog.at_level("WARNING"):
        report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    assert report["chunks_total"] == 1  # only svc_b's chunk -- svc_a's file was skipped
    staged_services = {row.service for row in staging.iter_chunks()}
    assert staged_services == {"b"}
    assert any("m.py" in rec.message for rec in caplog.records)


# ======================================================================================
# -- T3 carry: hash-aware re-embed (only the changed chunk, not the whole file/service)
# ======================================================================================


def test_edited_chunk_only_gets_re_embedded_the_second_time(tmp_path):
    svc_dir = tmp_path / "a"
    svc = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)
    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    before = {row.chunk_id: row.embedding for row in staging.iter_chunks()}
    assert len(before) == 2  # add, sub

    # Same-length edit ("a + b" -> "b + a", both 5 bytes) so byte offsets of
    # everything else in the file (sub's own span) are untouched -- isolates this to
    # exactly "add's own text changed", nothing shifts around it. No begin_service/
    # re-analyze call here on purpose: this simulates chunk_embed.run() itself
    # catching a same-session file edit, the exact scenario embedded_hash exists for
    # (see Staging.chunks_missing_embedding's own docstring).
    (svc_dir / "m.py").write_text(
        "def add(a, b):\n    return b + a\n\n\ndef sub(a, b):\n    return a - b\n"
    )

    second = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert second["chunks_total"] == 2
    assert second["embedded"] == 1
    assert second["reused"] == 1
    # The edited chunk's augmented text (header + new body) is genuinely NEW --
    # never embedded under this input_hash before -- so this is a real provider
    # call, not a cache hit.
    assert second["embedded_fresh"] == 1
    assert second["embedded_from_cache"] == 0

    after = {row.chunk_id: row.embedding for row in staging.iter_chunks()}
    changed = [chunk_id for chunk_id in before if before[chunk_id] != after[chunk_id]]
    assert len(changed) == 1
    unchanged = [chunk_id for chunk_id in before if before[chunk_id] == after[chunk_id]]
    assert len(unchanged) == 1


# ======================================================================================
# -- T4 carry: one header index for the whole workspace, not one per service --
# ======================================================================================


def test_run_builds_header_index_exactly_once_not_per_service(tmp_path, monkeypatch):
    services = [_write_service(tmp_path, f"svc{i}", {"m.py": SRC_ONE_FUNC}) for i in range(3)]
    cfg = _cfg(*services)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    build_index_calls: list[int] = []
    original_build_index = augment._build_index

    def spy_build_index(st, chunks=None):
        build_index_calls.append(1)
        return original_build_index(st, chunks=chunks)

    fill_headers_all_calls: list[int] = []
    original_fill_headers_all = augment.fill_headers_all

    def spy_fill_headers_all(st):
        fill_headers_all_calls.append(1)
        return original_fill_headers_all(st)

    fill_headers_calls: list[str] = []
    original_fill_headers = augment.fill_headers

    def spy_fill_headers(st, service):
        fill_headers_calls.append(service)
        return original_fill_headers(st, service)

    monkeypatch.setattr(augment, "_build_index", spy_build_index)
    monkeypatch.setattr(chunk_embed.augment, "fill_headers_all", spy_fill_headers_all)
    monkeypatch.setattr(chunk_embed.augment, "fill_headers", spy_fill_headers)

    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    assert len(build_index_calls) == 1
    assert len(fill_headers_all_calls) == 1
    assert fill_headers_calls == []  # the O(services x graph) per-service path, unused


# ======================================================================================
# -- no-embedder path (cli.index graceful degradation feeds embedder=None here) --
# ======================================================================================


def test_run_with_no_embedder_chunks_without_embedding(tmp_path):
    svc = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    report = chunk_embed.run(cfg, staging, None)

    assert report == {
        "chunks_total": 2, "embedded": 0, "embedded_fresh": 0,
        "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 2,
    }
    rows = list(staging.iter_chunks())
    assert len(rows) == 2
    for row in rows:
        assert row.embedding is None
        assert row.embed_model is None
        assert row.context_header is not None  # headers still get built


def test_run_with_no_embedder_then_rerun_with_embedder_embeds_everything(tmp_path):
    """Sanity: the no-embedder path doesn't poison anything -- a later run WITH an
    embedder against the same (re-chunked) staging still embeds normally."""
    svc = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    chunk_embed.run(cfg, staging, None)
    report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert report["embedded"] == 2
    assert report["embedded_fresh"] == 2  # nothing was ever cached (no-embedder run)
    assert report["embedded_from_cache"] == 0
    assert report["skipped_no_embedder"] == 0


# ======================================================================================
# -- embed batching: <=64 rows per embed_batch call --
# ======================================================================================


def test_embed_batches_capped_at_64(tmp_path):
    svc = _write_service(tmp_path, "a", {"m.py": _many_funcs_src(130)})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    spy = _RecordingEmbedder(FakeEmbedder(dim=8))
    report = chunk_embed.run(cfg, staging, spy)

    assert report["chunks_total"] == 130
    assert report["embedded"] == 130
    assert report["embedded_fresh"] == 130  # first run, nothing cached yet
    assert report["embedded_from_cache"] == 0
    assert spy.batch_sizes == [64, 64, 2]


# ======================================================================================
# -- M4 T8: concurrent embed batches for `concurrency_safe=True` embedders (openai/
# voyage in production; `_SlowBatchEmbedder` here stands in for one) -- wall-clock win
# (against a MEASURED sequential control, review fix -- not just a narrated floor) AND
# deterministic `set_embeddings` write order, plus (review fix) per-batch streaming
# durability when a mid-workload batch fails.
# ======================================================================================


def test_concurrent_embed_batches_give_wall_clock_win_and_deterministic_write_order(
    tmp_path, monkeypatch
):
    """256 tiny functions -> exactly 4 batches of 64 (_EMBED_BATCH_SIZE), each delayed
    50ms by `_SlowBatchEmbedder`. Two timed phases against ONE staging DB:

      - Phase 1 (sequential CONTROL -- review fix: the >=200ms floor used to be only a
        comment): the same workload through the sequential path (concurrency_safe=
        False, distinct model_id -- see `_SlowBatchEmbedder`'s docstring for why that
        keeps phase 2 a genuine 4-fresh-batches workload). 4 blocking `time.sleep
        (0.05)` calls make >=200ms a mathematical floor; asserted at >=190ms.
      - Phase 2 (concurrent): same 4 batches, concurrency_safe=True -- must land close
        to ONE delay's worth of wall time (asserted <130ms, generous margin against a
        loaded CI box), proving the win against the control measured moments earlier
        on this same machine, not against an assumed baseline.

    `Staging.set_embeddings` is spied on (phase 2 only -- cleared in between) to prove
    the concatenation of every chunk_id it's called with, IN CALL ORDER, is already
    sorted -- i.e. writes land in ascending chunk_id order (matching
    `chunks_missing_embedding`'s own `ORDER BY chunk_id`, the same order the batches
    were carved out of `missing` in), never scrambled by scheduling."""
    svc = _write_service(tmp_path, "a", {"m.py": _many_funcs_src(256)})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    written_chunk_ids: list[list[str]] = []
    original_set_embeddings = Staging.set_embeddings

    def spy_set_embeddings(self, rows):
        written_chunk_ids.append([row[0] for row in rows])
        return original_set_embeddings(self, rows)

    monkeypatch.setattr(Staging, "set_embeddings", spy_set_embeddings)

    # -- phase 1: sequential control --
    seq_embedder = _SlowBatchEmbedder(concurrency_safe=False, model_id="slow-seq-8d")
    t0 = time.perf_counter()
    seq_report = chunk_embed.run(cfg, staging, seq_embedder)
    elapsed_seq = time.perf_counter() - t0

    assert seq_report["chunks_total"] == 256
    assert seq_report["embedded_fresh"] == 256
    assert seq_embedder.batch_sizes == [64, 64, 64, 64]
    assert elapsed_seq >= 0.19, (
        f"sequential control under its own mathematical sleep floor? {elapsed_seq:.3f}s"
    )

    written_chunk_ids.clear()  # phase 2's order assertion covers phase 2's writes only

    # -- phase 2: concurrent --
    embedder = _SlowBatchEmbedder(concurrency_safe=True)
    t0 = time.perf_counter()
    report = chunk_embed.run(cfg, staging, embedder)
    elapsed = time.perf_counter() - t0

    assert report["chunks_total"] == 256
    assert report["embedded_fresh"] == 256  # phase 1's cache entries: different model_id
    assert report["embedded_from_cache"] == 0
    assert embedder.batch_sizes == [64, 64, 64, 64]  # 4 full batches, as designed

    # -- wall-clock win, against the control measured above on this same machine --
    assert elapsed < 0.13, (
        f"expected a concurrent wall-clock win, took {elapsed:.3f}s "
        f"(sequential control: {elapsed_seq:.3f}s)"
    )

    # -- deterministic write order --
    flat = [cid for call in written_chunk_ids for cid in call]
    assert len(flat) == 256
    assert flat == sorted(flat)

    # -- no cross-batch mix-up: every stored vector matches what a bare (undelayed,
    # unwrapped) FakeEmbedder produces for that exact chunk's own augmented text --
    # zip-alignment errors under concurrency would show up here as a mismatched vector
    # attached to the wrong chunk_id. Both sides go through the same lossy pack_vector/
    # unpack_vector float32 round-trip (`row.embedding` already has; the freshly
    # computed `expected` doesn't until packed here) -- comparing raw float64 vs a
    # float32-round-tripped value would spuriously differ at ~1e-9, unrelated to
    # whether the vector is actually the RIGHT one.
    reference = FakeEmbedder(dim=8)
    for row in staging.iter_chunks():
        expected = reference.embed_query(augment.augment_text(row.context_header or "", row.text))
        assert unpack_vector(row.embedding) == unpack_vector(pack_vector(expected))


@pytest.mark.parametrize("concurrency_safe", [False, True], ids=["sequential", "concurrent"])
def test_failed_batch_leaves_earlier_batches_durable_and_retry_resumes_past_them(
    tmp_path, concurrency_safe
):
    """Review fix pin (M4 T8): `_embed_batches` must STREAM (lazy generator), so a
    provider failure in batch N leaves batches 0..N-1's vectors already written to
    `chunks` AND the persistent `embedding_cache` when the raise propagates -- exactly
    the pre-T8 per-batch write+commit behavior -- and a retry resumes past them
    (re-embedding only the failed batch). T8's first cut materialized every batch's
    vectors before writing ANY (eager list + eager `zip` argument), silently
    discarding batches 0..N-1's computed work on a batch-N raise; caught in review.
    Parametrized over both paths: the sequential loop and the ThreadPoolExecutor.map
    pipeline must both exhibit the same durability."""
    svc = _write_service(tmp_path, "a", {"m.py": _many_funcs_src(256)})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)
    # Prime the chunks table without embedding anything (embedder=None -- the chunk
    # loop and fill_headers_all still run): chunks only exist after chunk_embed.run's
    # own chunk loop, and the batch carve below needs to see them BEFORE the poisoned
    # run. The poisoned run's own re-chunk pass is a no-op upsert over identical
    # content, so the carve it actually embeds is byte-identical to this preview.
    chunk_embed.run(cfg, staging, None)

    # Replicate `_embed_missing`'s own deterministic batch carve (ORDER BY chunk_id,
    # slices of 64) to learn which rows land in the LAST batch (index 3) -- poisoning
    # by CONTENT keeps the failing batch fixed even though the concurrent path runs
    # its calls in scheduler-dependent order (see _FailsOnPoisonedBatchEmbedder).
    missing = staging.chunks_missing_embedding("fake-8d")
    assert len(missing) == 256  # 4 batches of exactly 64
    expected_durable_ids = [r.chunk_id for r in missing[:192]]
    poison_texts = [r.text for r in missing[192:]]

    embedder = _FailsOnPoisonedBatchEmbedder(poison_texts, concurrency_safe=concurrency_safe)
    with pytest.raises(RuntimeError, match="poisoned batch"):
        chunk_embed.run(cfg, staging, embedder)

    rows = {r.chunk_id: r for r in staging.iter_chunks()}
    durable = sorted(cid for cid, r in rows.items() if r.embedding is not None)
    assert durable == expected_durable_ids  # batches 0-2 written; poisoned batch not

    # ...and the persistent cache holds the same three batches (`embedding_cache_put`
    # runs right after each batch's own `set_embeddings`, inside the streamed loop).
    pairs = [(rows[cid].input_hash, "fake-8d") for cid in expected_durable_ids]
    assert len(staging.embedding_cache_get(pairs)) == 192

    # Retry with a healthy embedder: the 192 durable rows are never re-flagged
    # (embedded_hash == input_hash, same model), so exactly ONE fresh 64-row batch
    # runs -- the failure cost is bounded to the failed batch, not the whole workload.
    retry = _RecordingEmbedder(FakeEmbedder(dim=8))
    report = chunk_embed.run(cfg, staging, retry)
    assert retry.batch_sizes == [64]
    assert report["embedded_fresh"] == 64
    assert report["reused"] == 192


# ======================================================================================
# -- staging meta (embed_model/embed_dim) -- pipeline.load reads these back for the
# Meta node + ensure_schema(dim) (see pipeline/load.py's _embed_meta docstring) --
# ======================================================================================


def test_run_with_embedder_writes_model_and_dim_to_staging_meta(tmp_path):
    svc = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    assert staging.get_meta("embed_model") == "fake-8d"
    assert staging.get_meta("embed_dim") == "8"


def test_run_with_no_embedder_clears_staging_meta(tmp_path):
    svc = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    chunk_embed.run(cfg, staging, None)

    assert not staging.get_meta("embed_model")
    assert not staging.get_meta("embed_dim")


def test_run_with_embedder_then_no_embedder_clears_stale_meta(tmp_path):
    """A LATER run (e.g. `--no-embed`) must not leave a PRIOR run's embed_model/dim
    behind -- every chunk in the workspace was just re-chunked from scratch this run,
    so if this run has no embedder, none of them actually carry that stale model's
    embedding any more (see _embed_meta's own docstring for why this matters at load
    time -- Meta must never advertise a model/dim no Chunk.embedding in the graph
    actually matches)."""
    svc = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    cfg = _cfg(svc)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert staging.get_meta("embed_model") == "fake-8d"

    chunk_embed.run(cfg, staging, None)
    assert not staging.get_meta("embed_model")
    assert not staging.get_meta("embed_dim")


# ======================================================================================
# -- code-review regression: `reused` must never go negative when a service is removed
# from cfg.services without deleting staging.db (a realistic operational scenario --
# staging.db is a persistent, workspace-scoped file across separate `codegraph index`
# invocations, not recreated per run) --
# ======================================================================================


def test_reused_never_negative_when_a_service_is_removed_from_config(tmp_path):
    """Reproduces the exact bug: service B is staged+chunked but never embedded (first
    run, no embedder), THEN a second run's cfg drops B entirely (say, decommissioned)
    while an embedder IS now available. `_embed_missing`'s workspace-wide
    `chunks_missing_embedding` scan still finds and embeds B's stale, never-embedded
    rows even though this run's own file-scan (cfg.services) never touched B -- so
    `embedded` can exceed a cfg.services-SCOPED `chunks_total`. `chunks_total` is
    computed workspace-wide (`staging.counts()["chunks"]`) specifically so this can
    never drive `reused` negative."""
    svc_a = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    svc_b = _write_service(tmp_path, "b", {"m.py": SRC_ONE_FUNC})
    staging = Staging(tmp_path / "s.db")
    cfg_both = _cfg(svc_a, svc_b)
    _analyze_all(cfg_both, staging, tmp_path)

    first = chunk_embed.run(cfg_both, staging, None)  # no embedder yet -- both unembedded
    assert first["chunks_total"] == 2
    assert first["skipped_no_embedder"] == 2

    # service "b" dropped from config -- staging.db NOT deleted, its chunk row for b
    # is still sitting in the table, never re-chunked by this run's own file-scan.
    cfg_a_only = _cfg(svc_a)
    second = chunk_embed.run(cfg_a_only, staging, FakeEmbedder(dim=8))

    assert second["reused"] >= 0  # the actual bug: this used to go negative (-1)
    assert second["embedded"] == 2  # a's (this run) AND b's (stale) rows both embedded
    assert second["chunks_total"] == 2  # workspace-wide total, not "just a's own"
    assert second["embedded"] + second["reused"] == second["chunks_total"]
    # neither row was ever cached before (the first run had no embedder at all, so
    # _embed_missing/embedding_cache_put never ran) -- both are genuine provider calls.
    assert second["embedded_fresh"] == 2
    assert second["embedded_from_cache"] == 0
    assert second["embedded_fresh"] + second["embedded_from_cache"] == second["embedded"]


# ======================================================================================
# -- sweep-review regression: a SPAN-SHIFTING mid-run file edit (file modified on disk
# between analyze_service and chunk_embed.run within one `codegraph index` invocation,
# in a way that MOVES def byte offsets -- unlike the same-length edit in the T3-carry
# test above, which deliberately keeps spans in place) must skip that one file with a
# warning, not KeyError-crash the whole run and discard S1-S7's completed work --
# ======================================================================================


def test_span_shifting_midrun_edit_skips_file_with_warning_instead_of_crashing(
    tmp_path, caplog
):
    import logging

    svc_a = _write_service(tmp_path, "a", {"m.py": SRC_TWO_FUNCS})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    # Length-CHANGING edit to a's file AFTER analyze staged its node spans: a long
    # comment line prepended shifts every def's start/end byte, so the fresh parse's
    # spans can no longer match any staged node.
    (tmp_path / "a" / "m.py").write_text("# a long prepended comment line\n" + SRC_TWO_FUNCS)

    with caplog.at_level(logging.WARNING):
        report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))  # must not raise

    # a's file was skipped (no chunks staged for it -- there were none before either);
    # b's file chunked and embedded normally.
    assert report["chunks_total"] == 1
    assert report["embedded"] == 1
    services_with_chunks = {row.service for row in staging.iter_chunks()}
    assert services_with_chunks == {"b"}
    assert any("spans no longer match" in rec.getMessage() for rec in caplog.records)


# ======================================================================================
# -- M4 T6: changed_files -- incremental S8 chunk-loop scoping. `changed_files=None`
# (every test ABOVE this section, unmodified) is the byte-identical full-run default;
# this section exercises the non-None path -- see chunk_embed.py's own module
# docstring for the full contract (chunk loop scoped, fill_headers_all/embed phase
# always workspace-wide).
# ======================================================================================


def test_changed_files_scopes_chunk_loop_to_named_relpaths_only(tmp_path, monkeypatch):
    """The brief's own Step 1 scenario: two services, `changed_files` names exactly
    one (service, relpath) pair -- `upsert_chunks` must be called ONLY for that pair;
    every other already-staged chunk (a's OWN other file, and the whole other
    service) must be left byte-for-byte untouched, embeddings included -- while
    headers still get recomputed workspace-wide (`fill_headers_all` is never
    changed_files-scoped) and the embed phase still only re-embeds the ONE row whose
    input_hash actually changed."""
    svc_a = _write_service(tmp_path, "a", {"x.py": SRC_ONE_FUNC, "y.py": SRC_TWO_FUNCS})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)

    first = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert first["chunks_total"] == 4  # a/x.py:1 + a/y.py:2 + b/n.py:1

    before_b = {
        r.chunk_id: (r.embedding, r.context_header, r.input_hash)
        for r in staging.chunks_for_service("b")
    }
    before_a_y = {
        r.chunk_id: (r.embedding, r.context_header, r.input_hash)
        for r in staging.chunks_for_service("a") if r.relpath == "y.py"
    }
    assert before_b and before_a_y  # non-vacuous -- both actually have rows to compare

    calls: list[tuple[str, str]] = []
    original_upsert = Staging.upsert_chunks

    def spy_upsert(self, service, relpath, rows):
        calls.append((service, relpath))
        return original_upsert(self, service, relpath, rows)

    monkeypatch.setattr(Staging, "upsert_chunks", spy_upsert)

    # Same-length edit (T3-carry technique -- see test_edited_chunk_only_gets_re_
    # embedded_the_second_time above): keeps a/x.py's staged node span matching the
    # fresh parse without a fresh analyze_service call in between, for the same
    # reason that test needs it (_symbol_ids_for_file span-matches staged nodes
    # against the CURRENT on-disk parse -- see its own docstring).
    (svc_a.path / "x.py").write_text("def mul(a, b):\n    return b * a\n")

    second = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8), changed_files={"a": {"x.py"}})

    assert calls == [("a", "x.py")]  # upsert_chunks called ONLY for a/x.py

    after_b = {
        r.chunk_id: (r.embedding, r.context_header, r.input_hash)
        for r in staging.chunks_for_service("b")
    }
    after_a_y = {
        r.chunk_id: (r.embedding, r.context_header, r.input_hash)
        for r in staging.chunks_for_service("a") if r.relpath == "y.py"
    }
    assert after_b == before_b  # b: byte-untouched, embeddings included
    assert after_a_y == before_a_y  # a/y.py: byte-untouched too -- not in changed_files

    # embed phase: chunks_missing_embedding is still workspace-wide (unscoped by
    # changed_files) -- but only x.py's chunk actually got a NEW input_hash (its own
    # text changed; the other 3 chunks' headers/text never budged), so it's the only
    # genuine re-embed.
    assert second["chunks_total"] == 4
    assert second["embedded_fresh"] == 1
    assert second["embedded_from_cache"] == 0
    assert second["reused"] == 3


def test_changed_files_service_absent_from_dict_skips_its_chunk_loop_entirely(
    tmp_path, monkeypatch
):
    """"service absent -> skip its chunk loop entirely" (brief's own Interfaces
    line): an empty dict names NO service, so BOTH services' chunk loops must be
    skipped, not just left with an empty relpath list coincidentally computed from
    files_for_service. fill_headers_all/the embed phase still run (unconditional,
    workspace-wide) but find nothing new to do against unchanged staged chunks."""
    svc_a = _write_service(tmp_path, "a", {"m.py": SRC_ONE_FUNC})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)
    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))  # baseline: both fully embedded

    calls: list[tuple[str, str]] = []
    original_upsert = Staging.upsert_chunks

    def spy_upsert(self, service, relpath, rows):
        calls.append((service, relpath))
        return original_upsert(self, service, relpath, rows)

    monkeypatch.setattr(Staging, "upsert_chunks", spy_upsert)

    report = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8), changed_files={})

    assert calls == []  # neither service is a key -> zero chunk-loop upserts at all
    assert report["chunks_total"] == 2
    assert report["reused"] == 2  # nothing re-chunked, nothing re-embedded
    assert report["embedded_fresh"] == 0
    assert report["embedded_from_cache"] == 0


def test_changed_files_still_recomputes_headers_workspace_wide(tmp_path, monkeypatch):
    """fill_headers_all itself must still be called (workspace-wide, unconditional)
    even though the chunk loop only touched one service's one file -- the T4 carry's
    own spy technique (test_run_builds_header_index_exactly_once_not_per_service
    above), reused here to prove changed_files doesn't ALSO (wrongly) scope this
    call: a header can depend on a DIFFERENT service's graph position (see module
    docstring), so it must be recomputed for everyone every time, regardless of how
    narrow the chunk loop itself was this run."""
    svc_a = _write_service(tmp_path, "a", {"x.py": SRC_ONE_FUNC})
    svc_b = _write_service(tmp_path, "b", {"n.py": SRC_ONE_FUNC})
    cfg = _cfg(svc_a, svc_b)
    staging = Staging(tmp_path / "s.db")
    _analyze_all(cfg, staging, tmp_path)
    chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))

    fill_headers_all_calls: list[int] = []
    original_fill_headers_all = augment.fill_headers_all

    def spy_fill_headers_all(st):
        fill_headers_all_calls.append(1)
        return original_fill_headers_all(st)

    monkeypatch.setattr(chunk_embed.augment, "fill_headers_all", spy_fill_headers_all)

    (svc_a.path / "x.py").write_text("def mul(a, b):\n    return b * a\n")
    report = chunk_embed.run(
        cfg, staging, FakeEmbedder(dim=8), changed_files={"a": {"x.py"}}
    )

    assert len(fill_headers_all_calls) == 1
    assert report["chunks_total"] == 2  # a/x.py + b/n.py, both still staged
