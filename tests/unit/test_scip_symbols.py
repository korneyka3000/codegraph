import pytest

from codegraph.resolvers.scip.symbols import parse_symbol, symbol_to_node_id

SYM = "scip-python python orders-api 0.1 `app.services.order`/OrderService#place()."


def test_parse_global_symbol():
    p = parse_symbol(SYM)
    assert not p.is_local
    assert p.scheme == "scip-python" and p.manager == "python"
    assert p.package == "orders-api" and p.version == "0.1"
    assert p.descriptors == "`app.services.order`/OrderService#place()."


def test_parse_local_symbol():
    p = parse_symbol("local 42")
    assert p.is_local and p.local == "local 42" and p.descriptors is None


def test_symbol_to_node_id_global_and_local():
    assert (
        symbol_to_node_id("orders-api", "app/services/order.py", SYM)
        == "sym:orders-api:`app.services.order`/OrderService#place()."
    )
    assert (
        symbol_to_node_id("orders-api", "app/x.py", "local 7")
        == "sym:orders-api:app/x.py:local7"
    )


def test_malformed_symbol_raises():
    with pytest.raises(ValueError):
        parse_symbol("garbage without enough fields")
