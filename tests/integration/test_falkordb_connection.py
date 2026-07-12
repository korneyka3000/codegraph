import pytest
from conftest import falkordb_available

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreUnavailable, connect, ping

pytestmark = pytest.mark.falkordb


needs_db = pytest.mark.skipif(not falkordb_available(), reason="FalkorDB not running")


@needs_db
def test_ping_ok():
    assert ping(FalkorDBConfig())


@needs_db
def test_roundtrip_tiny_graph():
    db = connect(FalkorDBConfig())
    g = db.select_graph("__codegraph_m0_smoke__")
    try:
        g.query("MERGE (n:Probe {id: 'x'}) SET n += {k: 1} RETURN n")
        res = g.query("MATCH (n:Probe {id: 'x'}) RETURN n.k")
        assert res.result_set[0][0] == 1
    finally:
        g.delete()


def test_unavailable_raises():
    with pytest.raises(StoreUnavailable):
        ping(FalkorDBConfig(host="localhost", port=59999))
