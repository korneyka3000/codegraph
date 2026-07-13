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
