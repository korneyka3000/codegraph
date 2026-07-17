"""M4 T5: incremental analyze_service, real scip-python end to end.

Fake-runner unit tests (tests/unit/test_pipeline_analyze.py) cover the orchestration
contract (mode dispatch, skip precondition, degraded-in-incremental fallback,
content-change-driven staleness). What they CANNOT cover -- and what this file
exists for -- is the ref_dirty mechanism itself: pyright/scip-python re-resolving a
REFERENCING file's occurrences after a symbol elsewhere was renamed, without that
referencing file's own bytes moving at all. That needs a genuine SCIP index; a fake
runner produces no meaningful refs data to diff.

Mutates a tmp_path COPY of the document_management fixture (never the repo fixture
itself) across a sequence of edits, running a real incremental analyze_service after
each one and checking its staged output against a full re-analyze of the SAME
mutated tree at that point -- the brief's own canonical-dump equivalence contract.
First scip run is slow (~2-5s, npx package fetch); later runs on a DIFFERENT
tree_hash still invoke real scip-python but no longer pay the download cost, and the
"full re-analyze" comparison calls reuse the SAME tree_hash as the incremental call
that just ran before them, so they hit ScipRunner's own cache (from_cache=True, no
second real subprocess) -- see resolvers/scip/runner.py.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from codegraph.config.models import ServiceConfig
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.diff import service_delta
from codegraph.pipeline.scan import scan_service
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.scip

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"


def _copy_service(tmp_path: Path) -> Path:
    dst = tmp_path / "svc"
    shutil.copytree(FIXTURE, dst)
    return dst


def _current_delta(staging: Staging, svc: ServiceConfig):
    """Mirrors exactly what analyze_service's own incremental branch computes
    internally for its stale-set math (see pipeline/analyze.py's module docstring)
    -- used here only to hand a REALISTIC prior_delta into analyze_service's public
    `prior_delta` parameter, matching how T7's CLI wiring will call it."""
    old_files = dict(staging.files_for_service(svc.name))
    scanned, _ = scan_service(svc.path, svc.exclude)
    return service_delta(old_files, scanned)


def _dump(staging: Staging, service: str) -> tuple:
    """Canonical sorted dump of staged nodes/edges/claims for ONE service, built
    entirely from Staging's existing public read API (no reaching into the private
    sqlite handle) -- chunks excluded per the brief's own dump contract (S8 is not
    run by analyze_service at all). origin_service/via_channel aren't independently
    surfaced by iter_edges()/EdgeRec; immaterial here since every Staging instance
    in this test is dedicated to exactly ONE service end to end, so every staged
    edge already belongs to `service` by construction."""
    nodes = sorted(
        (
            n.id, n.kind, n.roles, n.relpath, n.start_byte, n.end_byte,
            n.start_line, n.end_line, n.name, n.qualified_name, n.content_hash,
            tuple(sorted(n.props.items())),
        )
        for n in staging.iter_nodes()
        if n.service == service
    )
    edges = sorted(
        (
            e.src, e.dst, e.type, e.resolution, e.confidence, e.extractor,
            e.evidence_file, e.evidence_line, tuple(sorted(e.props.items())),
        )
        for e in staging.iter_edges()
    )
    claims = sorted(
        (kind, tuple(sorted(c.items())))
        for kind in ("temporal_start_mark", "http_call")
        for c in staging.claims_for(kind, service=service)
    )
    return (nodes, edges, claims)


def _full_reanalyze_dump(svc: ServiceConfig, cache_dir: Path, tmp_path: Path, tag: str) -> tuple:
    """A FRESH, dedicated Staging db, full-reanalyzed from the CURRENT on-disk tree
    -- the equivalence baseline. Sharing `cache_dir` with the incremental run being
    compared is deliberate: the tree is byte-identical at this point (nothing is
    mutated between the incremental call and this one), so this hits the
    ScipRunner's own tree_hash cache instead of paying for a second real
    scip-python subprocess."""
    st = Staging(tmp_path / f"full-check-{tag}.db")
    analyze_service(svc, st, cache_dir)
    return _dump(st, svc.name)


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_incremental_matches_full_reanalyze_across_a_mutation_sequence(tmp_path):
    svc_dir = _copy_service(tmp_path)
    svc = ServiceConfig(name="document-management", path=svc_dir)
    cache_dir = tmp_path / "cache"

    st = Staging(tmp_path / "incremental.db")

    # -- baseline: ordinary full analyze, real scip-python. --
    baseline = analyze_service(svc, st, cache_dir)
    assert baseline["mode"] == "full"
    assert baseline["degraded"] is False, baseline["reason"]  # sanity: real scip ran
    assert baseline["files"] == 8

    # -- (a)(b): edit the BODY of one function only -- no rename, no signature
    # change, so no OTHER file's refs can possibly be perturbed. --
    documents_service = svc_dir / "app" / "services" / "documents.py"
    original = documents_service.read_text()
    edited = original.replace(
        'return {"id": doc_id, "status": "verified"}',
        'return {"id": doc_id, "status": "verified", "checked": True}',
    )
    assert edited != original  # sanity: the replace actually matched something
    documents_service.write_text(edited)

    delta1 = _current_delta(st, svc)
    assert delta1.changed == ("app/services/documents.py",)

    report1 = analyze_service(svc, st, cache_dir, incremental=True, prior_delta=delta1)
    assert report1["mode"] == "incremental"
    assert report1["stale_files"] == 1

    assert _dump(st, svc.name) == _full_reanalyze_dump(svc, cache_dir, tmp_path, "1")

    # -- (в): rename a function referenced by ANOTHER file -- touch ONLY the
    # DEFINING file (app/events/producer.py); the referencing file
    # (app/routes/documents.py, which imports and calls it) keeps byte-identical
    # content, so it can only land in `stale` via ref_dirty, never via
    # delta.changed. --
    producer = svc_dir / "app" / "events" / "producer.py"
    producer_src = producer.read_text()
    renamed = producer_src.replace("emit_document_indexed", "emit_document_processed")
    assert renamed != producer_src
    assert "emit_document_indexed" not in renamed
    producer.write_text(renamed)

    routes_before = (svc_dir / "app" / "routes" / "documents.py").read_bytes()

    delta2 = _current_delta(st, svc)
    assert delta2.changed == ("app/events/producer.py",)
    assert "app/routes/documents.py" not in delta2.changed  # byte-identical, still

    report2 = analyze_service(svc, st, cache_dir, incremental=True, prior_delta=delta2)
    assert report2["mode"] == "incremental"
    assert report2["stale_files"] >= 2  # producer.py (changed) + >=1 ref-dirty neighbor
    assert (svc_dir / "app" / "routes" / "documents.py").read_bytes() == routes_before

    assert _dump(st, svc.name) == _full_reanalyze_dump(svc, cache_dir, tmp_path, "2")

    # -- (г): delete a file nothing else imports (main.py is the entrypoint; no
    # other fixture file references it). --
    (svc_dir / "app" / "main.py").unlink()

    delta3 = _current_delta(st, svc)
    assert delta3.deleted == ("app/main.py",)

    report3 = analyze_service(svc, st, cache_dir, incremental=True, prior_delta=delta3)
    assert report3["mode"] == "incremental"
    assert not any(n.relpath == "app/main.py" for n in st.iter_nodes())
    assert not any(e.evidence_file == "app/main.py" for e in st.iter_edges())

    assert _dump(st, svc.name) == _full_reanalyze_dump(svc, cache_dir, tmp_path, "3")


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_incremental_skip_after_full_analyze_with_no_changes(tmp_path):
    """A same-tree re-run (prior_delta.empty, fingerprint_ok default True) does
    ZERO staging writes and reports mode="skipped" with the CURRENT staged counts
    -- proven here against a real, non-trivial staged graph (not a fake-runner
    empty index)."""
    svc_dir = _copy_service(tmp_path)
    svc = ServiceConfig(name="document-management", path=svc_dir)
    cache_dir = tmp_path / "cache"
    st = Staging(tmp_path / "s.db")

    baseline = analyze_service(svc, st, cache_dir)
    assert baseline["degraded"] is False, baseline["reason"]
    counts_before = st.counts_for_service(svc.name)
    dump_before = _dump(st, svc.name)

    delta = _current_delta(st, svc)
    assert delta.empty

    report = analyze_service(svc, st, cache_dir, incremental=True, prior_delta=delta)

    assert report["mode"] == "skipped"
    assert st.counts_for_service(svc.name) == counts_before
    assert _dump(st, svc.name) == dump_before
