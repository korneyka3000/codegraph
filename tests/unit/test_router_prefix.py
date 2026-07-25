"""M8 T1 (rerun-2 R4): linking.router_prefix -- composes FastAPI route path templates
across `include_router` chains that span file (and even service) boundaries, from the
per-file `route_decl`/`router_include` claims fastapi_ext.py now emits instead of
directly building Channel(http_route)/HANDLES itself (see that module's own docstring
for the full "why" -- a single file can never see the whole chain).

Algorithm (`link`): build a `child_symbol -> (parent_symbol, prefix)` graph from every
staged `router_include` claim (symbols already bake in `service` -- see
resolvers/scip/symbols.symbol_to_node_id -- so no cross-service leakage is possible,
and no service scoping is needed here). For each `route_decl` claim, DFS from its own
`router_symbol` up the graph, concatenating each hop's `prefix` in root-to-leaf order,
until a node with no parent (a root -- nobody includes it: a bare `FastAPI()`, or a
router no `include_router` call anywhere ever names as arg0) is reached -- prepend that
accumulated prefix to `prefix_local` + `path` (a route's own same-file
`APIRouter(prefix=...)` and literal path, computed identically to before this task).

HONESTY RULE (no guessing, ever -- mirrors http_routes.py's own "NO static/1.0 without
anchor" binding constraint): three distinct failure shapes ALL collapse to the SAME
outcome -- the composed template is DISCARDED entirely, falling back to
`prefix_local + path` alone (byte-identical to today's pre-M8 `_template` output),
and the route is counted in `route_prefix_unresolved`:
  1. `router_symbol` itself is None (unresolvable at extraction time -- no SCIP, or a
     genuine miss).
  2. A CYCLE in the include graph (A includes B, B includes A).
  3. An UNRESOLVABLE or AMBIGUOUS hop partway up the chain (a router_include claim
     whose own parent_symbol is None, or two DISTINCT parents both naming the same
     child_symbol -- a real config ambiguity, never silently picked).

The TRIVIAL case -- no `router_include` claim anywhere names this router_symbol as a
child at all (a genuine root: same-file `APIRouter(prefix=...)`, no cross-file
`include_router` involvement) -- is NOT a failure: the accumulated ancestor prefix is
simply "", giving `prefix_local + path` again, but WITHOUT bumping the counter (this is
the CRITICAL CONSTRAINT case: every M2/M6/M7 fixture route composes through exactly
this path today, and the golden tuples must not shift by one byte -- see the dedicated
regression test below).

Channel/HANDLES creation mirrors fastapi_ext's OLD direct-emission shape exactly
(make_channel_node("http_route", ...) + HANDLES chan->handler), just relocated here and
now `extractor="linking"` (cleared by clear_workspace_layer, rebuilt fresh every S7 run
-- Channel-GC continues to work, same "GC-then-recreate" pattern already documented for
http_routes.py's own unresolved-fallback channel) instead of "fastapi". `evalx.edges_eval`
does not compare extractor/resolution at all (verified by reading EdgeTuple's own
construction) -- only (type, src_service, src_qualified, dst_channel_id) for HANDLES --
so this relocation cannot itself shift any golden HANDLES/CALLS_HTTP tuple.
"""

from __future__ import annotations

from codegraph.config.models import HttpExposure, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import NodeRec
from codegraph.linking import http_routes, router_prefix
from codegraph.stores.staging import Staging


def _route_decl(
    staging: Staging, service: str, relpath: str, *,
    router_symbol: str | None, verb: str, path: str,
    handler_node_id: str, prefix_local: str = "",
) -> None:
    staging.add_claims(service, relpath, "route_decl", [{
        "router_symbol": router_symbol, "verb": verb, "path": path,
        "handler_node_id": handler_node_id, "prefix_local": prefix_local,
    }])


def _router_include(
    staging: Staging, service: str, relpath: str, *,
    parent_symbol: str | None, child_symbol: str | None, prefix: str | None,
) -> None:
    staging.add_claims(service, relpath, "router_include", [{
        "parent_symbol": parent_symbol, "child_symbol": child_symbol, "prefix": prefix,
    }])


def _fn(id_: str) -> NodeRec:
    return NodeRec(id=id_, kind="Function", service="svc", name="h", qualified_name="q.h")


ROUTER_A = "sym:svc:`app.routes.steps.detail`/router."
ROUTER_B = "sym:svc:`app.routes.steps`/router."
APP = "sym:svc:`app.main`/app."
HANDLER = "sym:svc:`app.routes.steps.detail`/get_step()."


# -- link(): return shape / no-op on empty staging --


def test_link_returns_route_prefix_unresolved_key(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = router_prefix.link(st)
    assert report == {"route_prefix_unresolved": 0}


def test_link_no_claims_is_a_pure_noop(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = router_prefix.link(st)
    assert report == {"route_prefix_unresolved": 0}
    assert not any(n.kind == "Channel" for n in st.iter_nodes())
    assert not list(st.iter_edges())


# -- trivial: genuine root (resolved router_symbol, nobody includes it) --------------
# THE critical-constraint case: every M2/M6/M7 fixture route composes through exactly
# this path today (same-file APIRouter(prefix=...), no cross-file include chain at
# all) -- must reproduce byte-identical templates, and must NOT count as unresolved.


def test_root_router_no_include_chain_reproduces_local_template_exactly(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "orders-api", "app/routes/orders.py",
        router_symbol=ROUTER_A, verb="POST", path="", handler_node_id=HANDLER,
        prefix_local="/orders",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:orders-api:POST /orders"
    assert chan.props["path_template"] == "/orders"
    handles = [e for e in st.iter_edges() if e.type == "HANDLES"]
    assert len(handles) == 1
    assert (handles[0].src, handles[0].dst) == (chan.id, HANDLER)
    assert handles[0].extractor == "linking"
    assert handles[0].resolution == "static" and handles[0].confidence == 1.0


# -- unresolvable router_symbol (None at extraction time) -----------------------------


def test_unresolvable_router_symbol_falls_back_to_local_template_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "orders-api", "app/routes/orders.py",
        router_symbol=None, verb="POST", path="", handler_node_id=HANDLER,
        prefix_local="/orders",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:orders-api:POST /orders"  # SAME value as the root case


# -- 2-level chain: brief's own literal test scenario ---------------------------------
# router = APIRouter() + @router.get("/steps/{id}") in file A;
# app.include_router(router, prefix="/api/v1") in file B.


def test_two_level_chain_composes_include_prefix_with_local_path(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{id}", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v1/steps/{id}"
    assert chan.props["path_template"] == "/api/v1/steps/{id}"


# -- 3-level chain: the REAL pilot shape (rerun-2 open-gaps R4's own repro) -----------
# router = APIRouter() (leaf, file A) -> parent.include_router(leaf.router) (file B,
# NO prefix) -> app.include_router(parent.router, prefix="/api/v1") (file C).


def test_three_level_chain_concatenates_prefixes_root_to_leaf(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{step_uid}",
        handler_node_id=HANDLER, prefix_local="",
    )
    _router_include(
        st, "worker", "app/routes/steps/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix=None,
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/api/v1",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v1/steps/{step_uid}"


def test_three_level_chain_same_file_prefix_local_still_applies(tmp_path):
    """The LEAF router's own prefix_local (its own `APIRouter(prefix=...)`) still
    concatenates AFTER every ancestor's include-time prefix -- prefix composition
    doesn't replace prefix_local, it prepends to it."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/{step_uid}",
        handler_node_id=HANDLER, prefix_local="/steps",
    )
    _router_include(
        st, "worker", "app/routes/steps/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix=None,
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/api/v1",
    )

    router_prefix.link(st)

    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v1/steps/{step_uid}"


# -- two routes sharing one router: BOTH get the composed prefix ---------------------


def test_two_routes_same_router_both_get_composed_prefix(tmp_path):
    st = Staging(tmp_path / "s.db")
    handler2 = "sym:worker:`app.routes.steps.detail`/list_steps()."
    st.upsert_nodes([_fn(HANDLER), _fn(handler2)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/{step_uid}",
        handler_node_id=HANDLER, prefix_local="",
    )
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="",
        handler_node_id=handler2, prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {
        "chan:http:worker:GET /api/v1/{step_uid}",
        "chan:http:worker:GET /api/v1",
    }


# -- cycle guard ------------------------------------------------------------------


def test_cycle_falls_back_to_local_template_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/a.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    # A includes B, B includes A -- a cycle, never structurally valid FastAPI, but the
    # extractor has no way to rule it out ahead of time (claims are per-file, blind to
    # the workspace-wide graph shape) -- composition must not infinitely recurse.
    _router_include(
        st, "worker", "app/routes/a.py",
        parent_symbol=ROUTER_A, child_symbol=ROUTER_B, prefix="/b",
    )
    _router_include(
        st, "worker", "app/routes/b.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix="/a",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /x"  # local template only, no partial prefix


# -- unresolvable parent partway up the chain -----------------------------------------


def test_unresolvable_parent_partway_up_falls_back_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{id}",
        handler_node_id=HANDLER, prefix_local="",
    )
    # someone includes ROUTER_A, but THEIR OWN receiver couldn't be resolved (e.g. a
    # dynamic/complex receiver expression) -- parent_symbol is None.
    _router_include(
        st, "worker", "app/routes/steps/__init__.py",
        parent_symbol=None, child_symbol=ROUTER_A, prefix="/api/v1",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    # the known "/api/v1" prefix is honestly DISCARDED, not partially applied --
    # no guessing, per the module's own honesty rule.
    assert chan.id == "chan:http:worker:GET /steps/{id}"


# -- ambiguous: two distinct parents both claim to include the same child ------------


def test_ambiguous_multiple_parents_for_same_child_falls_back_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    other_parent = "sym:worker:`app.other`/router."
    _router_include(
        st, "worker", "app/routes/steps/__init__.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_include(
        st, "worker", "app/other.py",
        parent_symbol=other_parent, child_symbol=ROUTER_A, prefix="/other",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /x"


def test_unresolvable_child_symbol_include_claim_is_simply_unusable(tmp_path):
    """A router_include claim whose OWN child_symbol is None carries no identity to
    graph anything under -- it's dropped, not treated as "the root has no parent"
    for some unrelated router. The affected route_decl (naming a DIFFERENT,
    resolved router_symbol nobody legitimately includes) still composes as a normal
    root -- unrelated unresolvable claims must not leak into unrelated routes."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/routes/other.py",
        parent_symbol=APP, child_symbol=None, prefix="/unrelated",
    )

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /x"


# -- HANDLES/Channel shape: extractor="linking", owner_service from claim's own "_service" --


def test_channel_owner_service_comes_from_the_claims_own_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "document-management", "app/routes/documents.py",
        router_symbol=None, verb="GET", path="/{doc_id}", handler_node_id=HANDLER,
        prefix_local="/documents",
    )
    router_prefix.link(st)
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.props["owner_service"] == "document-management"
    assert chan.props["channel_kind"] == "http_route"
    assert chan.props["http_method"] == "GET"


# -- end-to-end: composed route feeds http_routes.link into a static/1.0 CALLS_HTTP --
# The brief's own explicit acceptance scenario: "сервис B с клиентом на
# /api/v1/steps/{id} и якорем на A -> ожидать одно CALLS_HTTP static/1.0".


def _cfg() -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="g",
        services=[
            ServiceConfig(name="worker", path="/nonexistent/worker",
                           http=HttpExposure(base_url_env="WORKER_URL")),
            ServiceConfig(name="gateway", path="/nonexistent/gateway"),
        ],
    )


def _client_claim(staging: Staging) -> None:
    staging.add_claims("gateway", "app/clients/worker_client.py", "http_call", [{
        "src_id": "sym:gateway:`app.clients.worker_client`/WorkerClient.fetch_step().",
        "verb": "GET", "path_template": "/api/v1/steps/{step_uid}",
        "base_url_env": "WORKER_URL", "resolution_hint": "static", "evidence_line": 3,
    }])


def test_composed_route_resolves_client_claim_to_static_calls_http(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{step_uid}",
        handler_node_id=HANDLER, prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _client_claim(st)

    router_prefix.link(st)
    http_stats = http_routes.link(_cfg(), st)

    assert http_stats == {"calls_http": 1, "calls_http_unresolved": 0}
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.resolution == "static" and calls_http.confidence == 1.0
    assert calls_http.dst == "chan:http:worker:GET /api/v1/steps/{step_uid}"


def test_negative_pin_without_composition_client_claim_is_unresolved_not_tail_matched(
    tmp_path,
):
    """Without the router_include chain (e.g. an unresolvable router_symbol), the
    route stays at its LOCAL template ("/steps/{step_uid}", no "/api/v1") -- a
    client claim for the FULL "/api/v1/steps/{step_uid}" path must fall back to
    unresolved, never silently tail-match the differently-shaped local route (the
    exact funnel-bug shape M7 T3 already guards against for OTHER cases)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=None, verb="GET", path="/steps/{step_uid}",
        handler_node_id=HANDLER, prefix_local="",
    )
    _client_claim(st)

    router_prefix.link(st)
    http_stats = http_routes.link(_cfg(), st)

    assert http_stats == {"calls_http": 1, "calls_http_unresolved": 1}
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.resolution == "heuristic" and calls_http.confidence == 0.5
