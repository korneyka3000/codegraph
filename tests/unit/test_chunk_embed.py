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

from pathlib import Path

from codegraph.chunking import augment
from codegraph.config.models import ServiceConfig, WorkspaceConfig
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
        "chunks_total": 3, "embedded": 3, "reused": 0, "skipped_no_embedder": 0,
    }

    rows = list(staging.iter_chunks())
    assert len(rows) == 3
    for row in rows:
        assert row.embedding is not None
        assert row.embed_model == "fake-8d"
        assert row.embedded_hash == row.content_hash
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
    assert first["reused"] == 0

    before = {row.chunk_id: row.embedding for row in staging.iter_chunks()}

    second = chunk_embed.run(cfg, staging, FakeEmbedder(dim=8))
    assert second["chunks_total"] == 2
    assert second["embedded"] == 0
    assert second["reused"] == 2

    after = {row.chunk_id: row.embedding for row in staging.iter_chunks()}
    assert after == before  # byte-identical -- nothing was re-embedded


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
        "chunks_total": 2, "embedded": 0, "reused": 0, "skipped_no_embedder": 2,
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
    assert spy.batch_sizes == [64, 64, 2]


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
