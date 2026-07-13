from pathlib import Path

from codegraph.extractors.base import FileContext
from codegraph.extractors.python_core import extract
from codegraph.parsing.facts import build_file_facts

FIXTURE = (Path(__file__).parents[2] / "fixtures" / "services" / "orders_api"
           / "app" / "services" / "order.py")


def _ctx(module_set=frozenset()):
    source = FIXTURE.read_bytes()
    relpath = "app/services/order.py"
    return FileContext(
        service="orders-api", relpath=relpath, source=source,
        facts=build_file_facts(relpath, source),
        def_symbol_lookup=lambda rp, sb: None,
        module_exists=lambda dotted: dotted in module_set,
    )


def test_module_and_defs_nodes():
    res = extract(_ctx())
    by_qn = {n.qualified_name: n for n in res.nodes}
    assert by_qn["app.services.order"].kind == "Module"
    assert by_qn["app.services.order.OrderService"].kind == "Class"
    place = by_qn["app.services.order.OrderService.place"]
    assert place.kind == "Function" and place.props["is_async"] is True
    assert place.id == "sym:orders-api:`app.services.order`/OrderService#place()."
    assert place.content_hash and place.start_line > 1


def test_contains_chain():
    res = extract(_ctx())
    contains = {(e.src, e.dst) for e in res.edges if e.type == "CONTAINS"}
    mod = "sym:orders-api:`app.services.order`/"
    cls = "sym:orders-api:`app.services.order`/OrderService#"
    assert ("svc:orders-api", mod) in contains
    assert (mod, cls) in contains
    assert (cls, "sym:orders-api:`app.services.order`/OrderService#place().") in contains


def test_imports_internal_vs_external():
    res = extract(_ctx(module_set={"app.db.outbox", "app.db.session", "app.models"}))
    imports = {e.dst for e in res.edges if e.type == "IMPORTS"}
    assert "sym:orders-api:`app.db.outbox`/" in imports
    assert "sym:orders-api:`app.models`/" in imports
    assert res.stats["imports_external"] >= 1  # uuid


def test_scip_lookup_takes_precedence():
    ctx = _ctx()
    sym = "scip-python python orders-api 0.1 `app.services.order`/OrderService#"
    ctx2 = FileContext(
        service=ctx.service, relpath=ctx.relpath, source=ctx.source, facts=ctx.facts,
        def_symbol_lookup=lambda rp, sb: sym if ctx.source[sb:sb + 12] == b"OrderService" else None,
        module_exists=lambda d: False,
    )
    res = extract(ctx2)
    cls = next(n for n in res.nodes if n.kind == "Class")
    assert cls.id == "sym:orders-api:`app.services.order`/OrderService#"


def test_contains_edge_endpoints_match_node_ids_with_partial_scip():
    ctx = _ctx()
    cls_sym = "scip-python python orders-api 0.1 `app.services.order`/OrderService#"
    ctx2 = FileContext(
        service=ctx.service, relpath=ctx.relpath, source=ctx.source, facts=ctx.facts,
        # класс — через scip, методы — структурно
        def_symbol_lookup=lambda rp, sb: cls_sym
        if ctx.source[sb:sb + 12] == b"OrderService" else None,
        module_exists=lambda d: False,
    )
    res = extract(ctx2)
    node_ids = {n.id for n in res.nodes}
    for e in res.edges:
        if e.type == "CONTAINS" and e.src.startswith("sym:"):
            assert e.src in node_ids and e.dst in node_ids, e


def test_relative_import_in_init_resolves_to_own_package():
    src = b"from . import order\n"
    relpath = "app/services/__init__.py"
    ctx = FileContext(
        service="orders-api", relpath=relpath, source=src,
        facts=build_file_facts(relpath, src),
        def_symbol_lookup=lambda rp, sb: None,
        module_exists=lambda d: d == "app.services.order",
    )
    res = extract(ctx)
    imports = {e.dst for e in res.edges if e.type == "IMPORTS"}
    assert "sym:orders-api:`app.services.order`/" in imports


def test_relative_import_in_regular_module_unchanged():
    src = b"from . import sibling\n"
    relpath = "app/services/order.py"
    ctx = FileContext(
        service="orders-api", relpath=relpath, source=src,
        facts=build_file_facts(relpath, src),
        def_symbol_lookup=lambda rp, sb: None,
        module_exists=lambda d: d == "app.services.sibling",
    )
    res = extract(ctx)
    imports = {e.dst for e in res.edges if e.type == "IMPORTS"}
    assert "sym:orders-api:`app.services.sibling`/" in imports
