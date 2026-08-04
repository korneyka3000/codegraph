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


# -- M2: channel/process id helpers --


def test_chan_kafka():
    assert ids.chan_kafka("orders.created") == "chan:kafka_topic:orders.created"


def test_chan_event():
    assert ids.chan_event("OrderPlaced") == "chan:event_type:OrderPlaced"


def test_chan_http_with_owner():
    assert ids.chan_http("orders-api", "POST", "/orders") == "chan:http:orders-api:POST /orders"


def test_chan_http_without_owner_uses_question_mark():
    assert ids.chan_http(None, "GET", "/health") == "chan:http:?:GET /health"


def test_proc_id():
    assert ids.proc_id("place-order") == "proc:place-order"


def test_slugify_lowercases_and_replaces_spaces_with_hyphens():
    assert ids.slugify("Place Order") == "place-order"


def test_slugify_strips_non_latin_alnum_hyphen_chars():
    assert ids.slugify("Order_Placed! (v2)") == "orderplaced-v2"


def test_slugify_collapses_repeated_separators_and_trims_edges():
    assert ids.slugify("--KYC   Worker--") == "kyc-worker"


# -- M11 T2 review fix (promoted verbatim from extractors/python_core.py's own
# `_resolve_relative` + inline package derivation, see module docstring):
# `containing_package`/`resolve_relative_import` had no DIRECT pins of their own --
# only covered transitively via python_core's IMPORTS tests and
# module_singletons's receiver-provenance tests (M12 backlog item, noted in the
# M11 T2 report's own "Concerns"). Pure pins of already-correct, unchanged
# behavior -- green on arrival, no production code touched by this task. --


def test_containing_package_init_module_is_its_own_package():
    """A package's `__init__.py` IS its package (mirrors python_core's own
    `test_relative_import_in_init_resolves_to_own_package` fixture path)."""
    assert ids.containing_package("app/services/__init__.py") == "app.services"


def test_containing_package_regular_module_is_its_dotted_parent():
    """A regular module's package is its dotted PARENT (mirrors python_core's own
    `test_relative_import_in_regular_module_unchanged` fixture path)."""
    assert ids.containing_package("app/services/order.py") == "app.services"


def test_containing_package_root_level_module_has_no_package():
    """Edge case no consumer test reaches directly (python_core's and
    module_singletons's own fixtures are always nested, e.g. "app/db/registry.py")
    -- a root-level module (no directory prefix, `test_relpath_to_module`'s own
    canonical "main.py" example above) has no dotted parent to split off: the
    `"." in dotted` ternary's empty-string branch."""
    assert ids.containing_package("main.py") == ""


def test_resolve_relative_import_absolute_target_passes_through_unchanged():
    assert ids.resolve_relative_import("app.db", "external.pkg") == "external.pkg"


def test_resolve_relative_import_bare_dot_resolves_to_package_itself():
    """`from . import order` (python_core's own fixture shape) -- a single leading
    dot with no trailing module text resolves to the package itself, unchanged."""
    assert ids.resolve_relative_import("app.services", ".") == "app.services"


def test_resolve_relative_import_same_package_dotted_target():
    """`.registry`, resolved against the SAME package -- module_singletons's own
    same-package provenance-dispatch case
    (`test_resolve_singleton_call_provenance_relative_import_same_package_dispatches`)."""
    assert ids.resolve_relative_import("app.db", ".registry") == "app.db.registry"


def test_resolve_relative_import_parent_package_dotted_target():
    """`..db.registry` resolved from a SIBLING package ("app.services") -- two
    leading dots means "go up one level from the caller's own package" (to "app"),
    then append the MULTI-SEGMENT rest ("db.registry"). A combination no existing
    pin exercises directly: module_singletons's own "..other" pin resolves a
    single-segment rest, and python_core's own "." pins carry no rest at all."""
    assert ids.resolve_relative_import("app.services", "..db.registry") == "app.db.registry"


def test_resolve_relative_import_root_level_caller_resolves_relative_to_top():
    """Chains `containing_package`'s own root-level edge case (empty package,
    above) into `resolve_relative_import`: a TOP-LEVEL module's `from .sibling
    import x` resolves to the plain top-level sibling name, not a leading-dot
    string -- the empty-`base` branch of the up-level slice."""
    assert ids.resolve_relative_import("", ".sibling") == "sibling"
