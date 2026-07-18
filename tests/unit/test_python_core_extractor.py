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
    ctx2 = FileContext(
        service=ctx.service, relpath=ctx.relpath, source=ctx.source, facts=ctx.facts,
        # класс — локальный scip-символ (его node id расходится со структурным
        # дескриптором), методы — структурно: рекомпутация endpoint'а провалит тест
        def_symbol_lookup=lambda rp, sb: "local 99"
        if ctx.source[sb:sb + 12] == b"OrderService" else None,
        module_exists=lambda d: False,
    )
    res = extract(ctx2)
    node_ids = {n.id for n in res.nodes}
    for e in res.edges:
        if e.type == "CONTAINS" and e.src.startswith("sym:"):
            assert e.src in node_ids and e.dst in node_ids, e
    local_cls = "sym:orders-api:app/services/order.py:local99"
    assert local_cls in node_ids
    assert any(e.dst == local_cls for e in res.edges if e.type == "CONTAINS")


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


# ======================================================================================
# -- M5 T3 (pilot Bug 7.1): ordinal-disambiguation of within-file id collisions --
#
# Same-named class/function redefined in mutually-exclusive if/elif branches (the
# feature-flag pattern, e.g. dispatch/config.py's `class Secret` in a
# metatron/kms/vault fork -- see docs/superpowers/reports/2026-07-18-m4-pilot.md
# Sec 7.1) can make TWO DIFFERENT DefFacts compute the textually IDENTICAL raw id:
#
#   - SCIP path: pyright/scip-python's symbol table is control-flow-insensitive, so
#     it can resolve BOTH branches' same-named defs to ONE symbol.
#   - structural-fallback path: `nesting_chain` walks the parent chain by NAME
#     (`cur.name`), not by id, so two branches with identical (kind, name) ancestry
#     rebuild an identical descriptor independently of each other.
#
# Left undetected, the second NodeRec silently overwrites the first at
# `Staging.upsert_nodes` (`INSERT OR REPLACE`, PK == id alone) -- one branch's node,
# and transitively its methods, simply vanish from the graph. See extract()'s own
# comment (the `def_ids` loop) for the disambiguation mechanism this pins.
# ======================================================================================

_COLLISION_SRC = b'''\
FLAG = "metatron"

if FLAG == "metatron":
    class Secret:
        def __init__(self, value):
            self.value = value

        def __repr__(self):
            return "<Secret>"

elif FLAG == "kms":
    class Secret:
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return "***"

elif FLAG == "vault":
    class Secret:
        def __init__(self, value):
            self.value = value


class Other:
    def method(self):
        pass
'''

_COLLISION_RELPATH = "svc/flags.py"


def _collision_ctx(def_symbol_lookup):
    return FileContext(
        service="dispatch", relpath=_COLLISION_RELPATH, source=_COLLISION_SRC,
        facts=build_file_facts(_COLLISION_RELPATH, _COLLISION_SRC),
        def_symbol_lookup=def_symbol_lookup,
        module_exists=lambda d: False,
    )


# Expected ids, pinned exactly. A def that does NOT collide (Other/Other.method)
# keeps its id byte-identical to the pre-M5T3 structural formula (id-stability
# constraint). A def that DOES collide gets `ids.disambiguate(raw_id, n)` for its
# n-th (n>=2) occurrence, in FILE-APPEARANCE order -- build_file_facts assigns a
# def's `index` (via `len(defs)`) before recursing into its body, so appearance
# order == facts.defs order == index order, regardless of which if/elif branch a
# def sits in.
_MODULE_ID = "sym:dispatch:`svc.flags`/"
_SECRET_1 = "sym:dispatch:`svc.flags`/Secret#"
_SECRET_2 = "sym:dispatch:`svc.flags`/Secret#~2"
_SECRET_3 = "sym:dispatch:`svc.flags`/Secret#~3"
_INIT_1 = "sym:dispatch:`svc.flags`/Secret#__init__()."
_INIT_2 = "sym:dispatch:`svc.flags`/Secret#__init__().~2"
_INIT_3 = "sym:dispatch:`svc.flags`/Secret#__init__().~3"
_REPR_1 = "sym:dispatch:`svc.flags`/Secret#__repr__()."
_STR_2 = "sym:dispatch:`svc.flags`/Secret#__str__()."
_OTHER = "sym:dispatch:`svc.flags`/Other#"
_OTHER_METHOD = "sym:dispatch:`svc.flags`/Other#method()."

# facts.defs appearance order for _COLLISION_SRC (see module comment above): Secret/
# __init__/__repr__ (branch 1), Secret/__init__/__str__ (branch 2), Secret/__init__
# (branch 3), Other/Other.method. res.nodes[0] is always the Module node (extract()
# appends it first) -- res.nodes[1:] is one entry per facts.defs, in this exact order.
_EXPECTED_DEF_IDS_IN_ORDER = [
    _SECRET_1, _INIT_1, _REPR_1,
    _SECRET_2, _INIT_2, _STR_2,
    _SECRET_3, _INIT_3,
    _OTHER, _OTHER_METHOD,
]


def test_colliding_def_ids_get_ordinal_suffix_structural_path():
    """3-way if/elif/elif collision, purely via the structural-fallback path
    (def_symbol_lookup never resolves anything). All defs -- classes AND methods --
    get unique ids; the 2nd/3rd occurrence of a given raw id gets `~2`/`~3`; the
    non-colliding def in the SAME file (`Other`/`Other.method`) keeps its plain,
    unsuffixed id -- proving the mechanism is surgical, not file-wide."""
    res = extract(_collision_ctx(lambda rp, sb: None))
    all_ids = [n.id for n in res.nodes]
    assert len(all_ids) == len(set(all_ids)), "every node id must be unique"
    assert [n.id for n in res.nodes[1:]] == _EXPECTED_DEF_IDS_IN_ORDER


def test_colliding_def_ids_contains_hierarchy_stays_per_branch():
    """CONTAINS must attach each branch's OWN methods to ITS OWN (possibly
    suffixed) class node -- never cross-wired to a sibling branch's class -- and the
    module must CONTAIN all three (disambiguated) Secret class nodes. This holds
    structurally (parent linkage is by def INDEX, not by id -- see extract()'s own
    `def_ids[d.parent]` lookup), so this test also guards against a future change
    that accidentally keyed CONTAINS off id text instead."""
    res = extract(_collision_ctx(lambda rp, sb: None))
    contains = {(e.src, e.dst) for e in res.edges if e.type == "CONTAINS"}

    assert (_MODULE_ID, _SECRET_1) in contains
    assert (_MODULE_ID, _SECRET_2) in contains
    assert (_MODULE_ID, _SECRET_3) in contains
    assert (_SECRET_1, _INIT_1) in contains
    assert (_SECRET_1, _REPR_1) in contains
    assert (_SECRET_2, _INIT_2) in contains
    assert (_SECRET_2, _STR_2) in contains
    assert (_SECRET_3, _INIT_3) in contains

    # negative: no cross-branch wiring -- a suffixed class must never CONTAIN
    # another branch's (differently-suffixed) method.
    assert (_SECRET_2, _INIT_1) not in contains
    assert (_SECRET_3, _INIT_1) not in contains
    assert (_SECRET_1, _INIT_2) not in contains
    assert (_SECRET_1, _INIT_3) not in contains


def test_colliding_def_ids_get_ordinal_suffix_scip_path():
    """Same collision, but BOTH branches' class/`__init__` name-spans resolve (via
    def_symbol_lookup) to the IDENTICAL scip symbol -- the diagnosed real-world root
    cause (pilot report Sec 7.1: scip-python's control-flow-insensitive symbol table
    resolves both if/elif branches' same-named defs to one symbol, independent of
    codegraph's own structural fallback). Proves the seen-set mechanism covers the
    SCIP-resolved path exactly like the structural path above, since it only ever
    looks at _def_id's finished text, never which branch of _def_id produced it."""
    secret_sym = "scip-python python dispatch 0.1 `svc.flags`/Secret#"
    init_sym = "scip-python python dispatch 0.1 `svc.flags`/Secret#__init__()."

    def lookup(rp, sb):
        if _COLLISION_SRC[sb:sb + 6] == b"Secret":
            return secret_sym
        if _COLLISION_SRC[sb:sb + 8] == b"__init__":
            return init_sym
        return None

    res = extract(_collision_ctx(lookup))
    all_ids = [n.id for n in res.nodes]
    assert len(all_ids) == len(set(all_ids)), "every node id must be unique"
    # The SCIP descriptors above were deliberately chosen to format identically to
    # the structural ones, so the exact same ordinal sequence is expected here too
    # -- same mechanism, different provenance for the raw (pre-suffix) id text.
    assert [n.id for n in res.nodes[1:]] == _EXPECTED_DEF_IDS_IN_ORDER
