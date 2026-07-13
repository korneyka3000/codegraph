"""Реальный scip-python на фикстурном сервисе. Медленно при первом запуске (npx скачивает пакет)."""

import shutil
from pathlib import Path

import pytest

from codegraph.resolvers.scip.runner import ScipRunner

pytestmark = pytest.mark.scip

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_scip_python_on_fixture(tmp_path):
    res = ScipRunner(timeout_s=600).run(
        "document-management", FIXTURE, None, tmp_path, "real"
    )
    assert res.scip_path.stat().st_size > 0
    from codegraph.resolvers.scip import scip_pb2

    idx = scip_pb2.Index()
    idx.ParseFromString(res.scip_path.read_bytes())
    assert len(idx.documents) >= 8  # 8 .py-файлов в фикстуре


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_scip_reader_fills_staging(tmp_path):
    from codegraph.resolvers.scip.reader import read_scip_into_staging
    from codegraph.stores.staging import Staging

    res = ScipRunner(timeout_s=600).run("document-management", FIXTURE, None, tmp_path, "real2")
    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(res.scip_path, "document-management", FIXTURE, st)
    assert stats.defs > 10 and stats.refs > 10 and stats.skipped_documents == 0


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_calls_join_document_management(tmp_path):
    from codegraph.extractors.calls import build_calls
    from codegraph.parsing.facts import build_file_facts
    from codegraph.resolvers.scip.reader import read_scip_into_staging
    from codegraph.stores.staging import Staging

    svc = "document-management"
    res = ScipRunner(timeout_s=600).run(svc, FIXTURE, None, tmp_path, "real3")
    st = Staging(tmp_path / "s.db")
    st.begin_service(svc)
    read_scip_into_staging(res.scip_path, svc, FIXTURE, st)
    facts_by_file = {}
    for f in FIXTURE.rglob("*.py"):
        rel = str(f.relative_to(FIXTURE))
        facts_by_file[rel] = build_file_facts(rel, f.read_bytes())

    def lookup(relpath, start_byte):
        return st.def_symbol_at(svc, relpath, start_byte)

    stats = build_calls(svc, st, facts_by_file, lookup)
    calls = {(e.src, e.dst) for e in st.iter_edges() if e.type == "CALLS"}
    assert stats.calls_joined >= 5
    assert any("create_document" in s and "emit_document_indexed" in d
               for s, d in calls), calls
