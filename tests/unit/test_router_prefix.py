"""M8 T1 (rerun-2 R4): linking.router_prefix -- composes FastAPI route path templates
across `include_router` chains that span file (and even service) boundaries, from the
per-file `route_decl`/`router_include`/`router_decl` claims fastapi_ext.py now emits
instead of directly building Channel(http_route)/HANDLES itself (see that module's own
docstring for the full "why" -- a single file can never see the whole chain).

Algorithm (`link`): build a `child_symbol -> LIST of (parent_symbol, prefix)` mounts
graph from every staged `router_include` claim (M9 T3: one entry per DISTINCT mount;
byte-identical (parent, child, prefix) duplicates -- even from two different files --
dedup to one), plus a `router_symbol -> own declared prefix` map from every
`router_decl` claim (M8 review Important-1). For each `route_decl` claim, walk UP
the graph from its own `router_symbol` to every reachable root, composing at each
mount [include-kwarg prefix] + [mounted router's own declared prefix] in
root-to-leaf order, then + prefix_local + path -- ONE template per surviving mount
chain (M9 T3: a router mounted N times, or sitting below an ancestor mounted N
times, yields N templates -> N Channels + N HANDLES onto the SAME handler,
cross-producting through every multi-mounted hop; a single-mount chain degenerates
to the old scalar behavior byte-for-byte). The per-mount order is REAL FastAPI
semantics, verified empirically against fastapi 0.140.0 (see
linking/router_prefix.py's own docstring for the raw OpenAPI-schema proof):
`app.include_router(B, prefix="/ia")` where `B = APIRouter(prefix="/pb")` and
`B.include_router(A, prefix="/ib")` with `A = APIRouter(prefix="/pa")` + route
"/x" serves `/ia/pb/ib/pa/x`.

HONESTY RULE (no guessing, ever -- mirrors http_routes.py's own "NO static/1.0
without anchor" binding constraint), M9 T3 shape -- failures are per-MOUNT: a mount
whose own chain hits a CYCLE, an unresolvable parent (parent_symbol None), or a
parent with a MISSING/CONFLICTING router_decl (own declared prefix unknown -- M8
review Important-1: an intermediate aggregator's own `APIRouter(prefix="/v2")` is
part of the real path, so composing without knowing it would mint a
confident-but-INCOMPLETE template) contributes nothing, WITHOUT poisoning sibling
mounts of the same child. The composed prefix is discarded ENTIRELY (fallback to
`prefix_local + path` alone, byte-identical to the pre-M8 `_template` output, +
`route_prefix_unresolved`) ONLY when nothing survives at all: `router_symbol`
itself None (unresolvable at extraction time), zero surviving mounts, or the
>_MAX_TEMPLATES(16)-alternative cap (runaway/malformed include graph -- the one
discard shape that ALSO logs a WARNING; never a truncated-but-partial subset).
Multi-mount itself is NOT a failure anymore -- M9 T3 lifted M8's deliberate
under-approximation that discarded on ANY second include claim naming one child:
two DISTINCT parents, or one parent under two prefixes, are legitimate live mounts
(real FastAPI serves both), each composed independently. `_AMBIGUOUS` survives
only for genuinely conflicting router_decl prefix_local values of one symbol.

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
http_routes.py's own unresolved-fallback channel) instead of "fastapi", carrying the
route_decl claim's own evidence_file/evidence_line (M8 review Important-2 -- the same
claim-evidence pass-through http_routes.py's CALLS_HTTP edges already do).
`evalx.edges_eval` does not compare extractor/resolution at all (verified by reading
EdgeTuple's own construction) -- only (type, src_service, src_qualified,
dst_channel_id) for HANDLES -- so this relocation cannot itself shift any golden
HANDLES/CALLS_HTTP tuple.
"""

from __future__ import annotations

from codegraph.config.models import HttpExposure, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import NodeRec
from codegraph.linking import http_routes, router_prefix
from codegraph.stores.staging import Staging


def _route_decl(
    staging: Staging, service: str, relpath: str, *,
    router_symbol: str | None, verb: str, path: str,
    handler_node_id: str, prefix_local: str = "", evidence_line: int | None = None,
) -> None:
    staging.add_claims(service, relpath, "route_decl", [{
        "router_symbol": router_symbol, "verb": verb, "path": path,
        "handler_node_id": handler_node_id, "prefix_local": prefix_local,
        "evidence_line": evidence_line,
    }])


def _router_include(
    staging: Staging, service: str, relpath: str, *,
    parent_symbol: str | None, child_symbol: str | None, prefix: str | None,
) -> None:
    staging.add_claims(service, relpath, "router_include", [{
        "parent_symbol": parent_symbol, "child_symbol": child_symbol, "prefix": prefix,
    }])


def _router_decl_claim(
    staging: Staging, service: str, relpath: str, *,
    router_symbol: str, prefix_local: str = "",
) -> None:
    """M8 review Important-1: one router_decl claim per `X = APIRouter(...)`/
    `X = FastAPI(...)` assignment -- the hop-parent own-prefix source
    _resolve_prefixes folds in at every mount (see module docstring)."""
    staging.add_claims(service, relpath, "router_decl", [{
        "router_symbol": router_symbol, "prefix_local": prefix_local,
    }])


def _fn(id_: str) -> NodeRec:
    return NodeRec(id=id_, kind="Function", service="svc", name="h", qualified_name="q.h")


def _handler_node(id_: str, *, path_template: str, http_method: str = "GET") -> NodeRec:
    """M9 T2: a RouteHandler-shaped NodeRec carrying the S5-staged LOCAL
    path_template/http_method props `extractors/fastapi_ext.py` would have set
    (see its own docstring) -- the compose-back patch's own precondition."""
    return NodeRec(
        id=id_, kind="Function", service="svc", name="h", qualified_name="q.h",
        roles=("RouteHandler",),
        props={"http_method": http_method, "path_template": path_template},
    )


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
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v1/steps/{id}"
    assert chan.props["path_template"] == "/api/v1/steps/{id}"


# -- 3-level chain: the REAL pilot shape (rerun-2 open-gaps R4's own repro) -----------
# router = APIRouter() (leaf, file A) -> parent.include_router(leaf.router) (file B,
# NO prefix) -> app.include_router(parent.router, prefix="/api/v1") (file C).
# This is ALSO the coordinator-review "aggregator WITHOUT own prefix" pin: ROUTER_B's
# own router_decl carries prefix_local="" -- composition result unchanged from pre-fix.


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
    _router_decl_claim(st, "worker", "app/routes/steps/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="")
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

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
    _router_decl_claim(st, "worker", "app/routes/steps/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="")
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)

    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v1/steps/{step_uid}"


# -- M8 review Important-1: intermediate aggregator's OWN APIRouter(prefix=...) -------
# The versioned-aggregator-in-__init__.py convention: B = APIRouter(prefix="/v2") has
# no routes of its own, includes A, and is included by the app. Empirically verified
# against real FastAPI 0.140.0 (OpenAPI schema): the served path is /api/v2/x.


def test_intermediate_aggregator_router_own_prefix_is_composed(tmp_path):
    """THE review Important-1 pin (RED against the pre-review code, which silently
    composed /api/x -- confident, counter 0, WRONG -- because no claim form carried
    B's own /v2): FastAPI's real per-mount order is [include-kwarg prefix] + [mounted
    router's own declared prefix], so the full chain here is /api (include of B) +
    /v2 (B's own) + "" (include of A) + "" (A's own prefix_local) + /x."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/a.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/api/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix=None,
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/api",
    )
    _router_decl_claim(st, "worker", "app/api/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="/v2")
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /api/v2/x"
    assert report["route_prefix_unresolved"] == 0


def test_hop_parent_without_router_decl_discards_and_counts(tmp_path):
    """Review Important-1's honesty companion (failure shape 4): a hop parent whose
    own declared prefix is UNKNOWN (no router_decl claim for its symbol -- e.g. a
    router built by a factory call rather than a visible APIRouter(...)/FastAPI(...)
    assignment) poisons the WHOLE chain -- the composed prefix is discarded entirely
    (never partially applied), local template, counter. Before the review fix this
    exact scenario composed /api/v1/steps/{id} confidently -- but only by silently
    ASSUMING the unknown parent's own prefix was empty."""
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
    # NO router_decl for APP.

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /steps/{id}"


def test_conflicting_router_decls_for_hop_parent_discard_and_count(tmp_path):
    """Two router_decl claims for the SAME hop-parent symbol with DIFFERENT
    prefix_local values (a genuine same-symbol re-declaration ambiguity) -- never
    silently pick one; whole-prefix discard + counter, same spirit as the
    multi-parent include ambiguity."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/a.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/api/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix=None,
    )
    _router_decl_claim(st, "worker", "app/api/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="/v2")
    _router_decl_claim(st, "worker", "app/api/legacy.py",
                       router_symbol=ROUTER_B, prefix_local="/v3")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert chan.id == "chan:http:worker:GET /x"


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
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

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
    # BOTH hop parents carry router_decl claims, so the CYCLE (not a missing hop
    # decl, failure shape 4) is what this test actually exercises.
    _router_include(
        st, "worker", "app/routes/a.py",
        parent_symbol=ROUTER_A, child_symbol=ROUTER_B, prefix="/b",
    )
    _router_include(
        st, "worker", "app/routes/b.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix="/a",
    )
    _router_decl_claim(st, "worker", "app/routes/a.py", router_symbol=ROUTER_A, prefix_local="")
    _router_decl_claim(st, "worker", "app/routes/b.py", router_symbol=ROUTER_B, prefix_local="")

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


# -- M9 T3: two distinct parents both including the same child is a LEGITIMATE
# double-mount now, not an ambiguity -- retires the M8 under-approximation this
# exact scenario used to pin (shape 3: "ANY second include claim naming the
# identical child_symbol", even two DISTINCT parents, used to be discarded
# outright). See the dedicated "-- M9 T3: multi-mount --" section below for the
# rest of the cross-product/dedup/cap scenarios. ---------------------------------


def test_two_distinct_parents_for_same_child_is_legitimate_double_mount(tmp_path):
    """Two distinct router/app OBJECTS independently including the SAME child
    router is structurally no different from one object including it twice --
    real FastAPI serves both mounts live -- so this now composes as an ordinary
    2-way multi-mount, exactly like the same-parent-two-prefixes case, provided
    each parent's own hop resolves (both have router_decl claims here). Before
    this task, ANY second include claim naming ROUTER_A -- even from a totally
    distinct parent -- immediately discarded the whole chain (honesty-rule shape
    3); that under-approximation is what this test now proves lifted."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    other_parent = "sym:worker:`app.other`/router."
    _route_decl(
        st, "worker", "app/routes/steps/detail.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/routes/steps/__init__.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_include(
        st, "worker", "app/other.py",
        parent_symbol=other_parent, child_symbol=ROUTER_A, prefix="/other",
    )
    _router_decl_claim(st, "worker", "app/routes/steps/__init__.py",
                       router_symbol=APP, prefix_local="")
    _router_decl_claim(st, "worker", "app/other.py",
                       router_symbol=other_parent, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {
        "chan:http:worker:GET /api/v1/x",
        "chan:http:worker:GET /other/x",
    }
    handles = [e for e in st.iter_edges() if e.type == "HANDLES"]
    assert len(handles) == 2
    assert all(h.dst == HANDLER for h in handles)


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


def test_handles_edge_carries_evidence_file_and_line(tmp_path):
    """M8 review Important-2: HANDLES must carry the route DECORATOR site's own
    evidence -- evidence_file from the claim's _relpath (injected by claims_for),
    evidence_line from route_decl's own evidence_line (the handler def's start_line,
    same value the pre-M8 direct-emission HANDLES carried) -- the identical
    claim-evidence pass-through http_routes.py's CALLS_HTTP edges already do."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "orders-api", "app/routes/orders.py",
        router_symbol=None, verb="POST", path="", handler_node_id=HANDLER,
        prefix_local="/orders", evidence_line=11,
    )
    router_prefix.link(st)
    handles = next(e for e in st.iter_edges() if e.type == "HANDLES")
    assert handles.evidence_file == "app/routes/orders.py"
    assert handles.evidence_line == 11


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
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")
    _client_claim(st)

    router_prefix.link(st)
    http_stats = http_routes.link(_cfg(), st)

    assert http_stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 0}
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

    assert http_stats == {"calls_http": 1, "calls_http_unresolved": 1, "calls_http_external": 0}
    calls_http = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert calls_http.resolution == "heuristic" and calls_http.confidence == 0.5


# -- M9 T2: compose-back -- the handler node's OWN path_template prop (staged
# LOCAL-only by fastapi_ext.py in S5) gets patched to the S7-composed template,
# via staging.update_node_props, so cards/get_source/retrieval consumers that read
# the handler node directly see the real, composed path -- not just the Channel. --


def test_composed_patch_lands_on_handler_node_props(tmp_path):
    """Non-trivial chain (2-level, brief's own literal scenario): the handler
    node's own path_template prop must end up equal to the COMPOSED template, not
    the local-only one S5 originally staged it with. Other pre-existing keys
    (http_method) survive the shallow merge untouched."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/steps/{id}")])
    _route_decl(
        st, "worker", "app/routes/steps.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{id}", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)

    node = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert node.props["path_template"] == "/api/v1/steps/{id}"
    assert node.props["http_method"] == "GET"
    # M9 T3: single-mount chains never gain a path_templates key -- that key
    # exists ONLY when a route composes to more than one live template.
    assert "path_templates" not in node.props


def test_trivial_chain_does_not_patch_handler_node_props(tmp_path, monkeypatch):
    """No cross-file include chain (chain_prefix == "", the genuine-root case) ->
    the composed template is byte-identical to the local one already staged -- the
    compose-back patch must not even ATTEMPT a write (staging.update_node_props
    itself is never called), not merely "write the same value harmlessly". This is
    the explicit "avoid no-op writes" case from router_prefix.link's own
    docstring: every M2/M6/M7 fixture route composes through exactly this path."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/orders", http_method="POST")])
    _route_decl(
        st, "orders-api", "app/routes/orders.py",
        router_symbol=ROUTER_A, verb="POST", path="", handler_node_id=HANDLER,
        prefix_local="/orders",
    )
    calls: list[tuple] = []
    original = st.update_node_props
    monkeypatch.setattr(
        st, "update_node_props",
        lambda *a, **k: (calls.append((a, k)), original(*a, **k))[1],
    )

    router_prefix.link(st)

    assert calls == []
    node = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert node.props == {"http_method": "POST", "path_template": "/orders"}


def test_unresolvable_router_symbol_does_not_patch_handler_node_props(tmp_path, monkeypatch):
    """The OTHER template==local_template case: an unresolvable router_symbol
    falls back to the local template too (honesty rule, route_prefix_unresolved
    counted) -- also must not attempt a patch."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/orders", http_method="POST")])
    _route_decl(
        st, "orders-api", "app/routes/orders.py",
        router_symbol=None, verb="POST", path="", handler_node_id=HANDLER,
        prefix_local="/orders",
    )
    calls: list[tuple] = []
    original = st.update_node_props
    monkeypatch.setattr(
        st, "update_node_props",
        lambda *a, **k: (calls.append((a, k)), original(*a, **k))[1],
    )

    router_prefix.link(st)

    assert calls == []
    node = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert node.props["path_template"] == "/orders"


def test_double_link_is_idempotent_for_handler_node_props(tmp_path):
    """double link() run -> same result (INSERT OR REPLACE / shallow-merge
    semantics: re-applying the identical composed value a second time converges,
    never drifts)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/steps/{id}")])
    _route_decl(
        st, "worker", "app/routes/steps.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{id}", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)
    first = next(n for n in st.iter_nodes() if n.id == HANDLER).props
    router_prefix.link(st)
    second = next(n for n in st.iter_nodes() if n.id == HANDLER).props

    assert first == second == {"http_method": "GET", "path_template": "/api/v1/steps/{id}"}


def test_stale_file_reanalyze_then_relink_recomposes_handler_node_props(tmp_path):
    """Incremental coherence (M9 plan): the S7 patch happens on EVERY link() run,
    identically in full and incremental (S7 always runs in full) -- but the
    handler node itself belongs to its ORIGIN service and gets fully re-emitted by
    S5 (fastapi_ext.py) with the LOCAL-only value whenever ITS OWN file goes stale,
    via upsert_nodes' INSERT OR REPLACE (which wipes any earlier S7 patch's props
    entirely -- it replaces the whole row, not a merge). Staging-level simulation
    of the full sequence: patch -> simulated stale re-analyze (S5 re-stages LOCAL)
    -> re-link -> composed again, byte-identical to the first time."""
    st = Staging(tmp_path / "s.db")
    local_node = _handler_node(HANDLER, path_template="/steps/{id}")
    st.upsert_nodes([local_node])
    _route_decl(
        st, "worker", "app/routes/steps.py",
        router_symbol=ROUTER_A, verb="GET", path="/steps/{id}", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)
    patched = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert patched.props["path_template"] == "/api/v1/steps/{id}"

    # Simulated stale-file re-analyze: S5's fastapi_ext always re-emits the SAME
    # LOCAL-only NodeRec (unaware of any S7 patch); upsert_nodes' INSERT OR REPLACE
    # wipes the earlier composed value wholesale.
    st.upsert_nodes([local_node])
    reset = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert reset.props["path_template"] == "/steps/{id}"  # sanity: really reset

    router_prefix.link(st)
    recomposed = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert recomposed.props["path_template"] == "/api/v1/steps/{id}"


# -- M9 T3: multi-mount -- lifts the M8 under-approximation (honesty-rule shape
# 3): a router legitimately included more than once (same parent + different
# include-kwarg prefixes, OR distinct parents -- see the dedicated test above)
# now composes ONE template PER mount instead of discarding the whole chain.
# One route_decl x N resolved mounts -> N Channels + N HANDLES onto the SAME
# handler (ids differ by template -- naturally distinct). --------------------


def test_double_mount_composes_two_templates_two_channels_same_handler(tmp_path):
    """THE brief's own literal scenario: `app.include_router(r, prefix="/v1")` +
    `app.include_router(r, prefix="/legacy")` -- both paths are legitimately
    LIVE per real FastAPI semantics (a common versioning idiom), not an
    ambiguity to discard. Each mount composes independently via the SAME
    per-mount order as any single-mount chain (include-kwarg prefix + leaf's
    own prefix_local + path) -- prefix_local="/items" here proves the order
    holds for BOTH alternatives, not just a trivial no-prefix case."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/items.py",
        router_symbol=ROUTER_A, verb="GET", path="/{item_id}", handler_node_id=HANDLER,
        prefix_local="/items",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {
        "chan:http:worker:GET /v1/items/{item_id}",
        "chan:http:worker:GET /legacy/items/{item_id}",
    }
    handles = [e for e in st.iter_edges() if e.type == "HANDLES"]
    assert len(handles) == 2
    assert {(h.src, h.dst) for h in handles} == {
        ("chan:http:worker:GET /v1/items/{item_id}", HANDLER),
        ("chan:http:worker:GET /legacy/items/{item_id}", HANDLER),
    }
    assert all(h.resolution == "static" and h.confidence == 1.0 for h in handles)


def test_byte_identical_duplicate_includes_dedup_to_one_mount(tmp_path):
    """Two DIFFERENT files independently emit the exact same (parent, child,
    prefix) `router_include` claim -- these must dedup to ONE mount, not a
    phantom double-mount. Distinct from the claims-table PK (service, relpath,
    kind, payload), which only dedups WITHIN one file: this dedup is
    include-graph-level, across files, keyed on (parent, child, prefix)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_include(
        st, "worker", "app/main_reexport.py",  # different file, byte-identical mount
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/api/v1",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1
    assert chans[0].id == "chan:http:worker:GET /api/v1/x"
    handles = [e for e in st.iter_edges() if e.type == "HANDLES"]
    assert len(handles) == 1


def test_ancestor_double_mount_propagates_to_descendant_routes(tmp_path):
    """The route's OWN router (A) has just ONE mount, into B; B ITSELF is what's
    double-mounted (into APP, two distinct include-kwarg prefixes). The
    multiplicity from higher up the chain still yields 2 templates for A's own
    routes -- multi-mount is not limited to a route's immediate parent hop."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/a.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/api/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix=None,
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/api/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="")
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {
        "chan:http:worker:GET /v1/x",
        "chan:http:worker:GET /legacy/x",
    }


def test_triple_nested_cross_product_of_two_double_mounts(tmp_path):
    """A is double-mounted into B (2 distinct include-kwarg prefixes at that
    hop) AND B is itself double-mounted into APP (2 distinct prefixes at THAT
    hop) -- routes on A get the full 2x2=4 cross product, one template per
    (A-mount, B-mount) combination."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/a.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/api/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix="/a1",
    )
    _router_include(
        st, "worker", "app/api/__init__.py",
        parent_symbol=ROUTER_B, child_symbol=ROUTER_A, prefix="/a2",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/b1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_B, prefix="/b2",
    )
    _router_decl_claim(st, "worker", "app/api/__init__.py",
                       router_symbol=ROUTER_B, prefix_local="")
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {
        "chan:http:worker:GET /b1/a1/x",
        "chan:http:worker:GET /b1/a2/x",
        "chan:http:worker:GET /b2/a1/x",
        "chan:http:worker:GET /b2/a2/x",
    }
    handles = [e for e in st.iter_edges() if e.type == "HANDLES"]
    assert len(handles) == 4
    assert all(h.dst == HANDLER for h in handles)


def test_one_hop_failure_mount_does_not_poison_sibling_valid_mount(tmp_path):
    """A router mounted twice: once through a parent with NO router_decl claim
    (own prefix unknown -- an ordinary hop failure, shape 4), once through a
    parent that resolves cleanly. Per-mount independence (the whole point of
    lifting the M8 under-approximation): the failing mount contributes nothing,
    the valid mount still composes -- ONE template survives, not a whole-route
    discard, and it is NOT counted as unresolved (something real WAS resolved,
    the honesty rule only fires when NOTHING resolves at all)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    ghost_parent = "sym:worker:`app.ghost`/router."
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/ghost.py",
        parent_symbol=ghost_parent, child_symbol=ROUTER_A, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")
    # NO router_decl claim for ghost_parent -- that mount's own prefix is unknown.

    report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1
    assert chans[0].id == "chan:http:worker:GET /v1/x"


def test_multi_mount_cap_exceeded_discards_counts_and_logs(tmp_path, caplog):
    """17 distinct mounts for one router_symbol -- one more than the module's
    own sane cap (16, "a legit app won't 16-mount a router") -- is a runaway/
    malformed include-graph guard, not a legitimate scenario: the WHOLE
    composed prefix is discarded (never a truncated-but-silently-partial
    16-of-17 set), falls back to the local template, counts in
    route_prefix_unresolved same as any other honesty-rule failure, AND
    (unlike the other discard shapes, which are unremarkable/expected) logs a
    warning -- this one signals a suspiciously-shaped include graph worth a
    human's attention, per the module's own OOM-guard rationale."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    for i in range(17):
        _router_include(
            st, "worker", "app/main.py",
            parent_symbol=APP, child_symbol=ROUTER_A, prefix=f"/v{i}",
        )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    with caplog.at_level("WARNING"):
        report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 1
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1  # local template only, no partial/truncated spray
    assert chans[0].id == "chan:http:worker:GET /x"
    assert len(caplog.records) == 1
    assert caplog.records[0].name == "codegraph.linking.router_prefix"
    assert caplog.records[0].levelname == "WARNING"


# -- M9 T3 compose-back: multi-mount props get the FIRST template by
# lexicographic sort as path_template, plus a path_templates list of ALL of
# them -- but ONLY when there's more than one (single-mount stays exactly the
# T2 shape, pinned above). --------------------------------------------------


def test_compose_back_multi_mount_props_first_sorted_plus_full_list(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/x")])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)

    node = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert node.props["path_template"] == "/legacy/x"  # "/legacy" < "/v1" lexicographically
    assert node.props["path_templates"] == ["/legacy/x", "/v1/x"]
    assert node.props["http_method"] == "GET"


def test_double_link_is_idempotent_for_multi_mount(tmp_path):
    """Re-running link() over an unchanged multi-mount include graph converges
    to the identical channel/HANDLES set -- no drift, no duplicate/ghost edges
    (relies on upsert_edges' own INSERT-OR-REPLACE PK semantics, same as the
    pre-existing single-mount idempotency pin)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)
    first_chans = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    first_handles = {(e.src, e.dst) for e in st.iter_edges() if e.type == "HANDLES"}

    router_prefix.link(st)
    second_chans = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    second_handles = {(e.src, e.dst) for e in st.iter_edges() if e.type == "HANDLES"}

    expected_chans = {"chan:http:worker:GET /v1/x", "chan:http:worker:GET /legacy/x"}
    assert first_chans == second_chans == expected_chans
    assert first_handles == second_handles
    assert len(second_handles) == 2


def test_multi_mount_cap_boundary_exactly_16_composes_all(tmp_path, caplog):
    """Boundary companion to the 17-mount overflow test: EXACTLY _MAX_TEMPLATES
    (16) distinct mounts is still a legitimate (if extreme) multi-mount -- all 16
    templates compose, nothing is discarded, the counter stays 0, and no warning
    is logged. Pins the cap's comparison as strictly-greater-than (an off-by-one
    to >= would silently discard a legal 16-mount)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_fn(HANDLER)])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    for i in range(16):
        _router_include(
            st, "worker", "app/main.py",
            parent_symbol=APP, child_symbol=ROUTER_A, prefix=f"/m{i:02d}",
        )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    with caplog.at_level("WARNING"):
        report = router_prefix.link(st)

    assert report["route_prefix_unresolved"] == 0
    chan_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan_ids == {f"chan:http:worker:GET /m{i:02d}/x" for i in range(16)}
    assert len([e for e in st.iter_edges() if e.type == "HANDLES"]) == 16
    assert caplog.records == []


def test_remount_removal_purges_stale_path_templates_key(tmp_path):
    """THE review repro (M9 T3 review item 1): double-mount -> link (props gain
    path_template + path_templates) -> one mount is DELETED from source; the
    mount's own file re-analyzes (delete_file_layer wipes its claims, the
    surviving ones re-stage -- the real incremental primitive pipeline/analyze.py
    uses) while the HANDLER's file stays untouched (its node keeps the previously
    patched props) -> re-link. The re-link composes a single template again;
    shallow-merge alone would overwrite path_template but leave the now-dead
    path_templates list on the node FOREVER (the handler file never goes stale,
    so S5 never wipes it either) -- the `remove` kwarg is what actively deletes
    it in the same UPDATE. End state must be byte-identical to a node that was
    only ever single-mounted: fresh path_template, NO path_templates key. (The
    old /legacy CHANNEL's retirement is clear_workspace_layer + gc_orphan_
    channels' job in the real link_workspace sequence -- covered by the M8-era
    GC tests, deliberately not re-asserted here.)"""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_handler_node(HANDLER, path_template="/x")])
    _route_decl(
        st, "worker", "app/routes/x.py",
        router_symbol=ROUTER_A, verb="GET", path="/x", handler_node_id=HANDLER,
        prefix_local="",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/legacy",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)
    patched = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert patched.props["path_templates"] == ["/legacy/x", "/v1/x"]  # sanity

    # Simulated stale re-analyze of app/main.py with the /legacy mount deleted:
    # wipe that file's claims wholesale, re-stage the surviving ones -- exactly
    # what pipeline/analyze.py's incremental branch does to a stale file.
    st.delete_file_layer("worker", {"app/main.py"}, drop_calls_evidence=set())
    _router_include(
        st, "worker", "app/main.py",
        parent_symbol=APP, child_symbol=ROUTER_A, prefix="/v1",
    )
    _router_decl_claim(st, "worker", "app/main.py", router_symbol=APP, prefix_local="")

    router_prefix.link(st)

    node = next(n for n in st.iter_nodes() if n.id == HANDLER)
    assert node.props["path_template"] == "/v1/x"
    assert "path_templates" not in node.props
