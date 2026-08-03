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


def test_symbol_to_node_id_does_not_rewrite_parameter_tails():
    """M11 T1 (rerun-5 R6, docs/superpowers/reports/2026-08-03-pilot-rerun-5-open-
    gaps.md): the classmethod `(cls)`-construction rewrite is DELIBERATELY NOT
    implemented here (see extractors/calls.py's own `_cls_construction_dst` for
    the actual fix + full rationale). Step 1's real-scip-python 0.6.6 dump
    (task-1-report.md) found the tail is scip's OWN `<parameter> ::= '(' <name>
    ')'` grammar rendering a REFERENCE TO A BARE PARAMETER TOKEN -- e.g.
    `Widget#make().(cls)` for the classmethod-factory idiom `return cls(name)`
    INSIDE `make`'s own body -- never a disambiguator on an EXTERNAL
    `Cls.make(...)` call (confirmed tail-free). The rewrite's honesty guards
    (enclosing def is a @classmethod, ref symbol == that def's own symbol +
    "(cls)") need the CallFact/DefFact context only the CALLS-join has;
    `parse_symbol`/`symbol_to_node_id` stay pure/general and UNCHANGED here --
    none of their other 6 call sites (kafka_ext/temporal_ext/fastapi_ext/
    python_core/module_singletons) ever process an arbitrary call-site ref that
    could be parameter-shaped, so a context-free rewrite at this layer would be
    both action-at-a-distance AND unverifiable (exactly the false-match risk
    the task-1 review's construction semantics forbid)."""
    sym = "scip-python python svc 0.1 `w`/Widget#make().(cls)"
    assert (
        symbol_to_node_id("svc", "w.py", sym)
        == "sym:svc:`w`/Widget#make().(cls)"
    )
