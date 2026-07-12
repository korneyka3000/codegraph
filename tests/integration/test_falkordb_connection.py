import pytest

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreUnavailable, connect, ping

pytestmark = pytest.mark.falkordb


def test_ping_ok(falkordb_cfg):
    assert ping(falkordb_cfg)


def test_roundtrip_tiny_graph(falkordb_cfg):
    db = connect(falkordb_cfg)
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
