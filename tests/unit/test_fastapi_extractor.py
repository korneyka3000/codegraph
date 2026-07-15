"""M2 T4: extract_fastapi (routes, HANDLES, DEPENDS_ON, http channels).

Route-detection tests run against the REAL orders_api/document_management fixtures.
Decorators are NOT walked as CallFacts by build_file_facts (M1a carried-forward:
decorated_definition unwraps decorator text but never visits it as a call) -- per the
brief's own resolution, the extractor re-parses each decorator's text standalone via
build_file_facts("<decorator>", text + b"\n") to get a real CallFact with .args; that
mini-parse mechanic is exercised implicitly by every route test below (real dec texts
like 'router.post("")').

ref_symbol_lookup (DEPENDS_ON) is stubbed by hand (a small closure) rather than routed
through Staging/fallback: M1a's own walker never visits parameter-default expressions
(progress.md M1a Task 8 carried-forward note: "вызовы в default-значениях параметров
... не посещаются"), so `Depends(get_db)` never produces a CallFact and the degraded
fallback resolver (built purely from facts.calls) never lays a ref at that span either
-- confirmed empirically (build_file_facts on the real orders.py fixture has zero calls
named "Depends"/"get_db"). A real SCIP run *would* resolve it (full document analysis,
not this repo's hand-rolled walker); that's proven end-to-end only where real SCIP is
available (T9 integration, per brief: "интеграцию покроет T9"). The wiring test in
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


def _load(relpath: str, service: str, source: bytes, *, ref_symbol_lookup=None):
    """Builds (ctx, node_ids) exactly as analyze.py's S5 wiring will: node_ids is
    def-index -> resolved node id, derived from python_core's OWN per-file output
    (Module node first, then exactly one node per facts.defs entry, same order)."""
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
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
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
    r = FastapiResult(roles={}, node_props={}, channels=[], edges=[], stats={})
    assert r.roles == {}
    assert r.node_props == {}
    assert r.channels == []
    assert r.edges == []
    assert r.stats == {}


# -- route detection: real fixtures, all 4 routes (self-review checklist) --


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


# -- channels: ids/names/props (self-review checklist: exact chan: ids) --


def test_channel_ids_match_self_review_checklist_all_four_routes():
    orders_ctx, orders_ids = _orders_ctx()
    orders_result = extract_fastapi(orders_ctx, orders_ids)
    docs_ctx, docs_ids = _documents_ctx()
    docs_result = extract_fastapi(docs_ctx, docs_ids)

    chan_ids = {c.id for c in orders_result.channels} | {c.id for c in docs_result.channels}
    assert chan_ids == {
        "chan:http:orders-api:POST /orders",
        "chan:http:orders-api:GET /orders/{order_id}",
        "chan:http:document-management:GET /documents/{doc_id}",
        "chan:http:document-management:POST /documents",
    }


def test_channel_node_shape():
    ctx, node_ids = _orders_ctx()
    result = extract_fastapi(ctx, node_ids)
    chan = next(c for c in result.channels if c.id == "chan:http:orders-api:POST /orders")

    assert chan.kind == "Channel"
    assert chan.service == ""
    assert chan.name == "POST /orders"
    assert chan.qualified_name == chan.id
    assert chan.props["http_method"] == "POST"
    assert chan.props["path_template"] == "/orders"
    assert chan.props["owner_service"] == "orders-api"
    assert chan.props["channel_kind"] == "http_route"


# -- HANDLES: direction chan -> handler (self-review checklist) --


def test_handles_edge_direction_and_shape_both_orders_routes():
    ctx, node_ids = _orders_ctx()
    result = extract_fastapi(ctx, node_ids)
    handles = {e.src: e for e in result.edges if e.type == "HANDLES"}

    create_id = node_ids[_def(ctx, "create_order").index]
    get_id = node_ids[_def(ctx, "get_order").index]
    assert handles["chan:http:orders-api:POST /orders"].dst == create_id
    assert handles["chan:http:orders-api:GET /orders/{order_id}"].dst == get_id
    for e in handles.values():
        assert e.extractor == "fastapi"
        assert e.resolution == "static"
        assert e.confidence == 1.0
        assert e.evidence_file == ctx.relpath


def test_handles_edge_direction_both_document_routes():
    ctx, node_ids = _documents_ctx()
    result = extract_fastapi(ctx, node_ids)
    handles = {e.src: e.dst for e in result.edges if e.type == "HANDLES"}

    assert handles["chan:http:document-management:GET /documents/{doc_id}"] == \
        node_ids[_def(ctx, "get_document").index]
    assert handles["chan:http:document-management:POST /documents"] == \
        node_ids[_def(ctx, "create_document").index]


def test_stats_routes_counter():
    ctx, node_ids = _documents_ctx()
    result = extract_fastapi(ctx, node_ids)
    assert result.stats["routes"] == 2


# -- DEPENDS_ON: create_order/get_order -> get_db, via "depends" --


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

    assert result.channels[0].id == "chan:http:svc:GET /health"
    handler_id = node_ids[_def(ctx, "health").index]
    assert result.node_props[handler_id]["path_template"] == "/health"


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

    assert result.channels[0].id == "chan:http:svc:GET /"
    handler_id = node_ids[_def(ctx, "root").index]
    assert result.node_props[handler_id]["path_template"] == "/"


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
    assert result.channels[0].id == "chan:http:svc:GET /x"


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
    assert result.channels == []
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
    assert result.channels == []
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
    assert result.channels == []
    assert result.roles == {}
    assert result.edges == []


def test_no_decorators_at_all_is_a_noop():
    ctx, node_ids = _load("m.py", "svc", b"def plain():\n    pass\n")
    result = extract_fastapi(ctx, node_ids)
    expected = FastapiResult(
        roles={}, node_props={}, channels=[], edges=[], stats=result.stats,
    )
    assert result == expected
    assert result.stats["routes"] == 0


def test_missing_node_id_for_matched_route_def_skips_gracefully():
    """Defensive: if analyze.py's node_ids somehow lacks an entry for a matched
    route's DefFact.index, extract_fastapi must not KeyError -- it just skips it."""
    ctx, _real_node_ids = _orders_ctx()
    result = extract_fastapi(ctx, {})
    assert result.channels == []
    assert result.edges == []
    assert result.roles == {}
