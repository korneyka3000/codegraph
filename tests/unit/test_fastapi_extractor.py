"""M2 T4 / M8 T1: extract_fastapi (routes as route_decl/router_include claims,
HANDLES, DEPENDS_ON).

M8 T1 (rerun-2 R4 -- docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
fastapi_ext STOPS emitting Channel(http_route) nodes + HANDLES edges directly --
those require knowing the FULL cross-file `include_router` prefix chain, which a
single-file extractor cannot see (that's linking/router_prefix.py's job, S7). This
extractor instead emits two per-file claim kinds:
  - route_decl: {router_symbol, verb, path, handler_node_id, prefix_local} -- one per
    matched route decorator. `prefix_local` is the SAME-FILE `APIRouter(prefix=...)`
    value fastapi_ext has always computed (byte-identical to the old direct-template
    behavior when there is no cross-file chain -- see router_prefix.py's own tests
    for the composition proof). `router_symbol` identifies the specific router
    object bound to the decorator's receiver, resolved via ctx.def_symbol_lookup on
    the router's OWN same-file `router = APIRouter(...)` assignment target (a
    DEFINITION occurrence) -- None when unresolvable (no lookup wired, or a miss).
  - router_include: {parent_symbol, child_symbol, prefix} -- one per
    `X.include_router(Y, prefix=...)` call, ANY receiver/arg0 shape (not gated on
    "looks like a known APIRouter" the way route matching is -- mirrors temporal_ext's
    own receiver-agnostic `.signal(...)` sender precedent). parent_symbol/child_symbol
    resolve via ctx.ref_symbol_lookup (REFERENCE occurrences -- the call's own
    receiver token / arg0's name token, INVOKES_ACTIVITY-grade confidence); either
    (or both) may be None when unresolvable -- the claim is still emitted (no
    guessing at composition time, see router_prefix.py's own docstring for how a
    None-symbol claim is simply unusable, not a corrupting guess).

Roles (RouteHandler) + node_props (http_method/path_template, computed from the LOCAL
template exactly as before) + DEPENDS_ON are UNCHANGED -- all three are file-local
facts requiring no cross-file knowledge, so they stay exactly as today's S5 behavior.

Route-detection tests run against the REAL orders_api/document_management fixtures.
Decorators are NOT walked as CallFacts by build_file_facts (M1a carried-forward:
decorated_definition unwraps decorator text but never visits it as a call) -- per the
brief's own resolution, the extractor re-parses each decorator's text standalone via
build_file_facts("<decorator>", text + b"\n") to get a real CallFact with .args; that
mini-parse mechanic is exercised implicitly by every route test below (real dec texts
like 'router.post("")').

ref_symbol_lookup (DEPENDS_ON, router_include) and def_symbol_lookup (route_decl's
router_symbol) are stubbed by hand (small closures) rather than routed through
Staging/fallback: M1a's own walker never visits parameter-default expressions or
decorator expressions (progress.md M1a Task 8 carried-forward note), so neither
`Depends(get_db)`'s `get_db` nor a decorator's own receiver identifier ever produces a
CallFact, and the degraded fallback resolver (built purely from facts.calls' own
CALLEE spans) never lays a ref/def down at either kind of span either -- confirmed
empirically (build_file_facts on the real orders.py fixture has zero calls named
"Depends"/"get_db", and resolvers/fallback.py only ever builds def rows from
facts.defs, never from facts.assigns). A real SCIP run *would* resolve both (full
document analysis, not this repo's hand-rolled walker) -- empirically confirmed by
decoding fixtures/.codegraph/scip's own orders-api index directly: `router`'s
assignment IS a Definition-role occurrence, and `orders_router` (main.py's IMPORT-
ALIASED reference to it) resolves to the exact same symbol string. That's proven
end-to-end only where real SCIP is available (the M2/M6/M7 gates); the wiring test in
test_pipeline_analyze.py exercises the degraded path end-to-end and documents this gap
directly rather than asserting a false resolution.
"""

from __future__ import annotations

from pathlib import Path

from codegraph.extractors.base import FileContext
from codegraph.extractors.fastapi_ext import FastapiResult, extract_fastapi
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"


def _fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


def _load(
    relpath: str, service: str, source: bytes, *,
    ref_symbol_lookup=None, def_symbol_lookup=None,
):
    """Builds (ctx, node_ids) exactly as analyze.py's S5 wiring will: node_ids is
    def-index -> resolved node id, derived from python_core's OWN per-file output
    (Module node first, then exactly one node per facts.defs entry, same order).
    `def_symbol_lookup` defaults to "always miss" (mirrors ref_symbol_lookup's own
    None-safe default) -- most tests don't care about router_symbol resolution."""
    facts = build_file_facts(relpath, source)
    core_ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    core_res = extract_python_core(core_ctx)
    node_ids = {
        d.index: n.id
        for d, n in zip(facts.defs, core_res.nodes[1:], strict=True)
    }
    ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=def_symbol_lookup or (lambda rp, sb: None),
        module_exists=lambda d: False,
        ref_symbol_lookup=ref_symbol_lookup,
    )
    return ctx, node_ids


def _orders_ctx(**kw):
    relpath = "app/routes/orders.py"
    return _load(relpath, "orders-api", _fixture_bytes(f"orders_api/{relpath}"), **kw)


def _documents_ctx(**kw):
    relpath = "app/routes/documents.py"
    return _load(
        relpath, "document-management",
        _fixture_bytes(f"document_management/{relpath}"), **kw,
    )


def _def(ctx: FileContext, name: str):
    return next(d for d in ctx.facts.defs if d.name == name)


def _ident_span(param, ident: str, *, annotation: bool = False) -> int:
    """Absolute byte offset of `ident` inside a ParamFact's default/annotation text --
    same computation the extractor itself must do (offset within the text + the
    text's own base byte, NOT a hardcoded literal)."""
    text = param.annotation_text if annotation else param.default_text
    base = param.annotation_start_byte if annotation else param.default_start_byte
    return base + text.index(ident)


# -- FileContext.ref_symbol_lookup: sanctioned M2 T4 extension, default-safe --


def test_file_context_ref_symbol_lookup_defaults_to_none():
    ctx = FileContext(
        service="svc", relpath="m.py", source=b"", facts=build_file_facts("m.py", b""),
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    assert ctx.ref_symbol_lookup is None


# -- FastapiResult: contract shape --


def test_fastapi_result_field_shape():
    r = FastapiResult(
        roles={}, node_props={}, edges=[],
        route_decl_claims=[], router_include_claims=[], router_decl_claims=[], stats={},
    )
    assert r.roles == {}
    assert r.node_props == {}
    assert r.edges == []
    assert r.route_decl_claims == []
    assert r.router_include_claims == []
    assert r.router_decl_claims == []
    assert r.stats == {}


# -- route detection: real fixtures, all 4 routes (self-review checklist) --
# roles/node_props are file-local and UNCHANGED by M8 T1 -- still computed from the
# LOCAL template (prefix_local + path) exactly as before.


def test_create_order_post_orders_route_role_and_props():
    ctx, node_ids = _orders_ctx()
    result = extract_fastapi(ctx, node_ids)
    handler_id = node_ids[_def(ctx, "create_order").index]

    assert result.roles[handler_id] == {"RouteHandler"}
    assert result.node_props[handler_id] == {
        "http_method": "POST", "path_template": "/orders",
    }


def test_get_order_route_template_includes_prefix_and_path_param():
    ctx, node_ids = _orders_ctx()
    result = extract_fastapi(ctx, node_ids)
    handler_id = node_ids[_def(ctx, "get_order").index]

    assert result.roles[handler_id] == {"RouteHandler"}
    assert result.node_props[handler_id] == {
        "http_method": "GET", "path_template": "/orders/{order_id}",
    }


def test_document_management_get_document_and_create_document_routes():
    ctx, node_ids = _documents_ctx()
    result = extract_fastapi(ctx, node_ids)

    get_id = node_ids[_def(ctx, "get_document").index]
    create_id = node_ids[_def(ctx, "create_document").index]

    assert result.roles[get_id] == {"RouteHandler"}
    assert result.node_props[get_id] == {
        "http_method": "GET", "path_template": "/documents/{doc_id}",
    }
    assert result.roles[create_id] == {"RouteHandler"}
    assert result.node_props[create_id] == {
        "http_method": "POST", "path_template": "/documents",
    }


# -- route_decl claims: shape/content (self-review checklist: all four routes) --


def test_route_decl_claims_match_self_review_checklist_all_four_routes():
    orders_ctx, orders_ids = _orders_ctx()
    orders_result = extract_fastapi(orders_ctx, orders_ids)
    docs_ctx, docs_ids = _documents_ctx()
    docs_result = extract_fastapi(docs_ctx, docs_ids)

    triples = {
        (c["verb"], c["prefix_local"], c["path"])
        for c in orders_result.route_decl_claims + docs_result.route_decl_claims
    }
    assert triples == {
        ("POST", "/orders", ""),
        ("GET", "/orders", "/{order_id}"),
        ("GET", "/documents", "/{doc_id}"),
        ("POST", "/documents", ""),
    }


def test_route_decl_claim_shape_and_handler_node_id():
    ctx, node_ids = _orders_ctx()
    result = extract_fastapi(ctx, node_ids)
    create_order = _def(ctx, "create_order")
    create_id = node_ids[create_order.index]

    claim = next(c for c in result.route_decl_claims if c["verb"] == "POST")
    assert claim == {
        "router_symbol": None,  # no def_symbol_lookup wired by _orders_ctx() default
        "verb": "POST",
        "path": "",
        "handler_node_id": create_id,
        "prefix_local": "/orders",
        # M8 review Important-2: the handler def's own start_line -- the exact value
        # the pre-M8 direct-emission HANDLES edge carried as evidence_line, now
        # passed through the claim so linking/router_prefix.py can restore it.
        "evidence_line": create_order.start_line,
    }


def test_stats_routes_counter():
    ctx, node_ids = _documents_ctx()
    result = extract_fastapi(ctx, node_ids)
    assert result.stats["routes"] == 2


# -- route_decl.router_symbol: resolved via ctx.def_symbol_lookup on the router's OWN
# same-file assignment target (a DEFINITION occurrence) --


def test_route_decl_router_symbol_resolves_via_def_symbol_lookup():
    src = b'''from fastapi import APIRouter

router = APIRouter(prefix="/orders")


@router.get("/{id}")
def handler(id: str):
    pass
'''
    relpath = "m.py"
    facts0 = build_file_facts(relpath, src)
    router_assign = next(a for a in facts0.assigns if a.target == "router")
    span = router_assign.target_start_byte
    target_sym = "scip-python python svc 0.0 `m`/router."
    target_id = "sym:svc:`m`/router."

    def def_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _load(relpath, "svc", src, def_symbol_lookup=def_lookup)
    result = extract_fastapi(ctx, node_ids)

    assert len(result.route_decl_claims) == 1
    claim = result.route_decl_claims[0]
    assert claim["router_symbol"] == target_id
    assert claim["verb"] == "GET"
    assert claim["path"] == "/{id}"
    assert claim["prefix_local"] == "/orders"
    handler_id = node_ids[_def(ctx, "handler").index]
    assert claim["handler_node_id"] == handler_id
    # M8 review Important-1: the SAME resolved assignment also emits its own
    # router_decl claim (router_symbol + own declared prefix) -- the hop-parent
    # own-prefix source linking/router_prefix.py folds in at every mount.
    assert result.router_decl_claims == [{
        "router_symbol": target_id, "prefix_local": "/orders",
    }]


def test_route_decl_router_symbol_none_when_def_symbol_lookup_misses():
    ctx, node_ids = _orders_ctx()  # default def_symbol_lookup: always miss
    result = extract_fastapi(ctx, node_ids)
    assert len(result.route_decl_claims) == 2
    assert all(c["router_symbol"] is None for c in result.route_decl_claims)


# -- router_decl claims: EVERY APIRouter()/FastAPI() assignment (M8 review Important-1)
# -- regardless of whether the router has any routes or include_router calls of its
# own in this file: a versioned aggregator (`B = APIRouter(prefix="/v2")`, no routes,
# includes A, included by C) is precisely the shape whose own prefix was invisible to
# every claim form before this, silently composing an INCOMPLETE confident template.


def test_router_decl_emitted_for_routeless_aggregator_apirouter_assignment():
    src = b'''from fastapi import APIRouter

router = APIRouter(prefix="/v2")
'''
    relpath = "app/api/__init__.py"
    facts0 = build_file_facts(relpath, src)
    router_assign = next(a for a in facts0.assigns if a.target == "router")
    span = router_assign.target_start_byte
    target_sym = "scip-python python svc 0.0 `app.api`/router."

    def def_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _load(relpath, "svc", src, def_symbol_lookup=def_lookup)
    result = extract_fastapi(ctx, node_ids)

    assert result.router_decl_claims == [{
        "router_symbol": "sym:svc:`app.api`/router.",
        "prefix_local": "/v2",
    }]
    assert result.route_decl_claims == []  # no routes in this file at all


def test_router_decl_emitted_for_fastapi_assignment_with_empty_prefix():
    """FastAPI() assignments emit too (prefix_local always "" -- FastAPI has no
    prefix concept): the chain ROOT is itself a hop parent whose own prefix
    router_prefix.py must know -- without the app's own claim, every chain ending at
    a FastAPI() root would spuriously discard under the missing-hop-decl rule."""
    src = b'''from fastapi import FastAPI

app = FastAPI(title="svc")
'''
    relpath = "app/main.py"
    facts0 = build_file_facts(relpath, src)
    app_assign = next(a for a in facts0.assigns if a.target == "app")
    span = app_assign.target_start_byte
    target_sym = "scip-python python svc 0.0 `app.main`/app."

    def def_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _load(relpath, "svc", src, def_symbol_lookup=def_lookup)
    result = extract_fastapi(ctx, node_ids)

    assert result.router_decl_claims == [{
        "router_symbol": "sym:svc:`app.main`/app.",
        "prefix_local": "",
    }]


def test_router_decl_skipped_when_router_symbol_unresolvable():
    """A None router_symbol (degraded fallback -- no defs at assignment targets --
    or a genuine SCIP miss) emits NOTHING: an unkeyable claim is unusable at
    composition (mirrors temporal_start_mark's own "no claim without a dst"
    precedent); the affected chains already discard honestly downstream."""
    src = b'''from fastapi import APIRouter

router = APIRouter(prefix="/v2")
'''
    ctx, node_ids = _load("m.py", "svc", src)  # default def lookup: always miss
    result = extract_fastapi(ctx, node_ids)
    assert result.router_decl_claims == []


def test_router_decl_not_emitted_for_non_router_assignments():
    src = b'''client = HttpClient()
'''
    # a lookup that WOULD resolve anything -- proves the callee gate (not a lookup
    # miss) is what rejects the non-router assignment.
    ctx, node_ids = _load(
        "m.py", "svc", src,
        def_symbol_lookup=lambda rp, sb: "scip-python python svc 0.0 `m`/client.",
    )
    result = extract_fastapi(ctx, node_ids)
    assert result.router_decl_claims == []


# -- router_include claims: X.include_router(Y, prefix=...) --


def test_router_include_claim_resolves_parent_and_child_symbols():
    src = b'''from fastapi import APIRouter

parent = APIRouter()
parent.include_router(child)
'''
    relpath = "m.py"
    facts0 = build_file_facts(relpath, src)
    call = next(c for c in facts0.calls if c.callee_name == "include_router")
    parent_span = call.receiver_start_byte
    child_arg = next(a for a in call.args if a.index == 0)
    child_span = child_arg.name_start_byte
    assert parent_span is not None and child_span is not None

    parent_sym = "scip-python python svc 0.0 `m`/parent."
    child_sym = "scip-python python svc 0.0 `other`/child."

    def ref_lookup(rp, sb):
        if (rp, sb) == (relpath, parent_span):
            return parent_sym
        if (rp, sb) == (relpath, child_span):
            return child_sym
        return None

    ctx, node_ids = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)

    assert len(result.router_include_claims) == 1
    claim = result.router_include_claims[0]
    assert claim["parent_symbol"] == "sym:svc:`m`/parent."
    assert claim["child_symbol"] == "sym:svc:`other`/child."
    assert claim["prefix"] is None


def test_router_include_claim_captures_prefix_kwarg():
    src = b'''app.include_router(router, prefix="/api/v1")
'''
    ctx, node_ids = _load("m.py", "svc", src, ref_symbol_lookup=lambda rp, sb: None)
    result = extract_fastapi(ctx, node_ids)
    assert len(result.router_include_claims) == 1
    assert result.router_include_claims[0]["prefix"] == "/api/v1"


def test_router_include_claim_no_prefix_kwarg_is_none():
    src = b'''app.include_router(router)
'''
    ctx, node_ids = _load("m.py", "svc", src, ref_symbol_lookup=lambda rp, sb: None)
    result = extract_fastapi(ctx, node_ids)
    assert result.router_include_claims[0]["prefix"] is None


def test_router_include_claim_child_symbol_attribute_arg_resolves_last_segment():
    """`include_router(api.v1.router)` -- child identity is the ATTRIBUTE's last
    segment (mirrors ArgFact's own "attr" convention, same as INVOKES_ACTIVITY's own
    arg0 resolution), not the whole dotted expression."""
    src = b'''app.include_router(api.v1.router)
'''
    relpath = "m.py"
    facts0 = build_file_facts(relpath, src)
    call = next(c for c in facts0.calls if c.callee_name == "include_router")
    arg0 = next(a for a in call.args if a.index == 0)
    assert arg0.value_kind == "attr"
    child_span = arg0.name_start_byte

    def ref_lookup(rp, sb):
        return "scip-python python svc 0.0 `api.v1`/router." if sb == child_span else None

    ctx, node_ids = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)
    assert result.router_include_claims[0]["child_symbol"] == "sym:svc:`api.v1`/router."


def test_router_include_claim_parent_symbol_attribute_receiver_resolves_last_segment():
    """`self.router.include_router(child)` -- parent identity is the receiver
    ATTRIBUTE chain's last segment ("router"), not "self"."""
    src = b'''class Wiring:
    def setup(self):
        self.router.include_router(child)
'''
    relpath = "m.py"
    facts0 = build_file_facts(relpath, src)
    call = next(c for c in facts0.calls if c.callee_name == "include_router")
    assert call.receiver_text == "self.router"
    parent_span = call.receiver_start_byte

    def ref_lookup(rp, sb):
        return "scip-python python svc 0.0 `m`/Wiring#router." if sb == parent_span else None

    ctx, node_ids = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)
    assert result.router_include_claims[0]["parent_symbol"] == "sym:svc:`m`/Wiring#router."


def test_router_include_claim_unresolvable_symbols_still_emits_claim_with_none():
    """Unfamiliar/unresolvable symbols -> the claim is still emitted, with None
    fields -- no claim is ever silently dropped (router_prefix.py's own composition
    is what decides what a None-symbol claim means, not the extractor)."""
    src = b'''app.include_router(some_dynamic_var)
'''
    ctx, node_ids = _load("m.py", "svc", src, ref_symbol_lookup=lambda rp, sb: None)
    result = extract_fastapi(ctx, node_ids)
    assert len(result.router_include_claims) == 1
    claim = result.router_include_claims[0]
    assert claim["parent_symbol"] is None
    assert claim["child_symbol"] is None


def test_router_include_claim_missing_ref_symbol_lookup_degrades_no_crash():
    """ref_symbol_lookup=None entirely (degraded fallback path) must not raise --
    both symbols just stay None."""
    ctx, node_ids = _load("m.py", "svc", b'''app.include_router(router)\n''')
    result = extract_fastapi(ctx, node_ids)
    claim = result.router_include_claims[0]
    assert claim["parent_symbol"] is None and claim["child_symbol"] is None


def test_non_include_router_calls_produce_no_router_include_claims():
    ctx, node_ids = _orders_ctx()  # no include_router call in this fixture file
    result = extract_fastapi(ctx, node_ids)
    assert result.router_include_claims == []


def test_multiple_include_router_calls_each_produce_a_claim():
    src = b'''app.include_router(a_router)
app.include_router(b_router, prefix="/b")
'''
    ctx, node_ids = _load("m.py", "svc", src)
    result = extract_fastapi(ctx, node_ids)
    assert len(result.router_include_claims) == 2
    assert {c["prefix"] for c in result.router_include_claims} == {None, "/b"}


# -- DEPENDS_ON: create_order/get_order -> get_db, via "depends" (unchanged by M8 T1) --


def test_depends_on_resolves_create_order_to_get_db():
    target_sym = "scip-python python orders-api 0.0 `app.db.session`/get_db()."
    target_id = "sym:orders-api:`app.db.session`/get_db()."
    relpath = "app/routes/orders.py"

    ctx0, _ = _orders_ctx()
    db_param = next(p for p in _def(ctx0, "create_order").params if p.name == "db")
    span = _ident_span(db_param, "get_db")

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)
    handler_id = node_ids[_def(ctx, "create_order").index]

    depends = [e for e in result.edges if e.type == "DEPENDS_ON" and e.src == handler_id]
    assert len(depends) == 1
    assert depends[0].dst == target_id
    assert depends[0].resolution == "static" and depends[0].confidence == 1.0
    assert depends[0].props == {"via": "depends"}
    assert depends[0].extractor == "fastapi"


def test_depends_on_resolves_get_order_to_get_db():
    target_sym = "scip-python python orders-api 0.0 `app.db.session`/get_db()."
    target_id = "sym:orders-api:`app.db.session`/get_db()."
    relpath = "app/routes/orders.py"

    ctx0, _ = _orders_ctx()
    db_param = next(p for p in _def(ctx0, "get_order").params if p.name == "db")
    span = _ident_span(db_param, "get_db")

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)
    handler_id = node_ids[_def(ctx, "get_order").index]

    depends = [e for e in result.edges if e.type == "DEPENDS_ON" and e.src == handler_id]
    assert len(depends) == 1
    assert depends[0].dst == target_id
    assert depends[0].props == {"via": "depends"}


def test_depends_on_both_orders_routes_resolve_in_one_pass():
    """Self-review checklist: create_order->get_db AND get_order->get_db, both via
    depends, in a single extract_fastapi call over the whole file."""
    target_sym = "scip-python python orders-api 0.0 `app.db.session`/get_db()."
    target_id = "sym:orders-api:`app.db.session`/get_db()."
    relpath = "app/routes/orders.py"

    ctx0, _ = _orders_ctx()
    spans = {
        _ident_span(next(p for p in _def(ctx0, name).params if p.name == "db"), "get_db")
        for name in ("create_order", "get_order")
    }

    def ref_lookup(rp, sb):
        return target_sym if rp == relpath and sb in spans else None

    ctx, node_ids = _orders_ctx(ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)

    create_id = node_ids[_def(ctx, "create_order").index]
    get_id = node_ids[_def(ctx, "get_order").index]
    depends_by_src = {e.src: e.dst for e in result.edges if e.type == "DEPENDS_ON"}

    assert depends_by_src[create_id] == target_id
    assert depends_by_src[get_id] == target_id
    assert result.stats["depends_resolved"] == 2
    assert result.stats["depends_unresolved"] == 0


def test_depends_on_unresolved_ref_lookup_counts_stat_and_emits_no_edge():
    ctx, node_ids = _orders_ctx(ref_symbol_lookup=lambda rp, sb: None)
    result = extract_fastapi(ctx, node_ids)

    assert not any(e.type == "DEPENDS_ON" for e in result.edges)
    assert result.stats["depends_unresolved"] == 2
    assert result.stats["depends_resolved"] == 0


def test_depends_on_missing_ref_symbol_lookup_degrades_to_unresolved_no_crash():
    """ref_symbol_lookup=None (caller didn't wire SCIP/fallback refs at all) must not
    raise -- everything just counts unresolved."""
    ctx, node_ids = _orders_ctx()  # ref_symbol_lookup defaults to None
    result = extract_fastapi(ctx, node_ids)

    assert not any(e.type == "DEPENDS_ON" for e in result.edges)
    assert result.stats["depends_unresolved"] == 2


def test_depends_on_annotated_form_via_annotated():
    """Annotated[X, Depends(y)] (annotation_text, no default_text) -- no M2 fixture
    uses this form (grep-confirmed), so synthetic, per the brief's explicit contract."""
    src = b'''from fastapi import APIRouter, Depends

router = APIRouter()


@router.get("/x")
async def handler(db: Annotated[Session, Depends(get_dep)]):
    pass
'''
    relpath = "app/routes/x.py"
    target_sym = "scip-python python svc 0.0 `app.dep`/get_dep()."
    target_id = "sym:svc:`app.dep`/get_dep()."

    facts0 = build_file_facts(relpath, src)
    db_param = next(p for p in facts0.defs[0].params if p.name == "db")
    assert db_param.default_text is None  # annotation-only form, no `=`
    span = _ident_span(db_param, "get_dep", annotation=True)

    def ref_lookup(rp, sb):
        return target_sym if (rp, sb) == (relpath, span) else None

    ctx, node_ids = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)
    handler_id = node_ids[_def(ctx, "handler").index]

    depends = [e for e in result.edges if e.type == "DEPENDS_ON"]
    assert len(depends) == 1
    assert depends[0].src == handler_id
    assert depends[0].dst == target_id
    assert depends[0].props == {"via": "annotated"}


# -- M2 final review: _DEPENDS_RE word-boundary lookbehind --


def test_depends_on_lookalike_mydepends_call_is_not_matched_as_real_depends():
    """Regression: a param default that merely ENDS WITH "Depends(...)" -- e.g. a
    same-file custom `MyDepends` callable entirely unrelated to FastAPI's own
    Depends() -- must not be misread as a real Depends() call. Before the
    `(?<!\\w)` lookbehind, _DEPENDS_RE.search matched the "Depends(" substring
    starting right after "My" (re.search has no left boundary of its own), wrongly
    resolving `factory` as a DEPENDS_ON target."""
    src = b'''from fastapi import APIRouter


def MyDepends(x):
    return x


router = APIRouter()


@router.get("/x")
async def handler(db=MyDepends(factory)):
    pass
'''
    relpath = "m.py"

    # Would resolve if the extractor ever called it -- proves the regex itself rejects
    # the match (no lookup attempted), not merely that some stub happens to return None.
    def ref_lookup(rp, sb):
        return "scip-python python svc 0.0 `m`/factory()."

    ctx, node_ids = _load(relpath, "svc", src, ref_symbol_lookup=ref_lookup)
    result = extract_fastapi(ctx, node_ids)

    assert not any(e.type == "DEPENDS_ON" for e in result.edges)
    # the outer "Depends(" substring pre-check still lets it through to attempt
    # resolution (a fast-path heuristic, not itself boundary-aware) -- _resolve_depends_
    # target's own regex is what correctly rejects it, counted as unresolved same as any
    # other failed resolution.
    assert result.stats["depends_unresolved"] == 1
    assert result.stats["depends_resolved"] == 0
    # legitimate shapes still match after the lookbehind (not an over-broad rejection):
    # bare `Depends(x)` as the whole default (create_order/get_order tests above) and
    # `Annotated[X, Depends(x)]` (comma+space-preceded -- test_depends_on_annotated_
    # form_via_annotated) both already pass, covering start-of-text and mid-text
    # legitimate positions respectively.


# -- prefix / template edge cases (APIRouter prefix, FastAPI no-prefix, both-empty) --
# node_props (LOCAL template) is unchanged; route_decl claims carry the DECOMPOSED
# (prefix_local, path) pair router_prefix.py recombines identically for the trivial
# (no cross-file chain) case -- see linking/router_prefix.py's own tests for the proof.


def test_fastapi_app_direct_route_has_no_prefix():
    src = b'''from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def health():
    pass
'''
    relpath = "app/main.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)

    handler_id = node_ids[_def(ctx, "health").index]
    assert result.node_props[handler_id]["path_template"] == "/health"
    claim = result.route_decl_claims[0]
    assert claim["prefix_local"] == "" and claim["path"] == "/health"


def test_empty_prefix_and_empty_path_template_is_root_slash():
    src = b'''from fastapi import APIRouter

router = APIRouter()


@router.get("")
def root():
    pass
'''
    relpath = "m.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)

    handler_id = node_ids[_def(ctx, "root").index]
    assert result.node_props[handler_id]["path_template"] == "/"
    claim = result.route_decl_claims[0]
    assert claim["prefix_local"] == "" and claim["path"] == ""


def test_route_decorator_extra_kwargs_after_path_still_detected():
    src = b'''from fastapi import APIRouter

router = APIRouter()


@router.get("/x", status_code=200)
def handler():
    pass
'''
    relpath = "m.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)
    assert result.route_decl_claims[0]["path"] == "/x"


# -- negatives: non-route decorators / unbound receivers ignored --


def test_non_route_decorator_call_ignored():
    src = b'''from fastapi import FastAPI

app = FastAPI()


@app.on_event("startup")
async def startup():
    pass
'''
    relpath = "app/main.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)

    assert result.roles == {}
    assert result.route_decl_claims == []
    assert result.edges == []
    assert result.stats["routes"] == 0


def test_bare_non_call_decorator_ignored():
    src = b'''class Foo:
    @staticmethod
    def bar():
        pass
'''
    relpath = "m.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)
    assert result.route_decl_claims == []
    assert result.roles == {}


def test_decorator_receiver_not_bound_to_router_or_fastapi_skipped():
    src = b'''some_other = SomeOtherClass()


@some_other.get("/x")
def handler():
    pass
'''
    relpath = "m.py"
    ctx, node_ids = _load(relpath, "svc", src)
    result = extract_fastapi(ctx, node_ids)
    assert result.route_decl_claims == []
    assert result.roles == {}
    assert result.edges == []


def test_no_decorators_at_all_is_a_noop():
    ctx, node_ids = _load("m.py", "svc", b"def plain():\n    pass\n")
    result = extract_fastapi(ctx, node_ids)
    expected = FastapiResult(
        roles={}, node_props={}, edges=[],
        route_decl_claims=[], router_include_claims=[], router_decl_claims=[],
        stats=result.stats,
    )
    assert result == expected
    assert result.stats["routes"] == 0


def test_missing_node_id_for_matched_route_def_skips_gracefully():
    """Defensive: if analyze.py's node_ids somehow lacks an entry for a matched
    route's DefFact.index, extract_fastapi must not KeyError -- it just skips it."""
    ctx, _real_node_ids = _orders_ctx()
    result = extract_fastapi(ctx, {})
    assert result.route_decl_claims == []
    assert result.edges == []
    assert result.roles == {}
