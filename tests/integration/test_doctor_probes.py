import pytest

from codegraph.doctor import run_store_probes
from codegraph.stores.falkordb.connection import connect

pytestmark = pytest.mark.falkordb


def test_all_required_features_present_on_pinned_image(falkordb_cfg):
    results = run_store_probes(lambda: connect(falkordb_cfg))
    failed = [(r.name, r.detail) for r in results if not r.ok]
    assert not failed, f"pinned FalkorDB image lacks features: {failed}"
