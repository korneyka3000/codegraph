from codegraph.core import ids


def test_relpath_to_module():
    assert ids.relpath_to_module("app/routes/orders.py") == "app.routes.orders"
    assert ids.relpath_to_module("app/__init__.py") == "app"
    assert ids.relpath_to_module("main.py") == "main"


def test_structural_descriptor_and_id():
    d = ids.structural_descriptor(
        "app.services.order", [("class", "OrderService"), ("function", "place")]
    )
    assert d == "`app.services.order`/OrderService#place()."
    expected_id = "sym:orders-api:`app.services.order`/OrderService#place()."
    assert ids.node_id("orders-api", d) == expected_id


def test_module_descriptor_matches_structural_with_empty_nesting():
    assert ids.module_descriptor("app.db") == ids.structural_descriptor("app.db", [])


def test_local_id_normalized():
    assert ids.local_id("svc", "app/x.py", "local 3") == "sym:svc:app/x.py:local3"


def test_display_qualified():
    assert ids.display_qualified("`app.mod`/Cls#meth().") == "app.mod.Cls.meth"
    assert ids.display_qualified("`app.mod`/fn().") == "app.mod.fn"
    assert ids.display_qualified("`app.mod`/") == "app.mod"
