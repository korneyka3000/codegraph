"""M8 T1 (rerun-2 R4 -- docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
composes FastAPI route path templates across `include_router` chains that span file
(and, structurally, service) boundaries -- the identity a route's Channel(http_route)
node and HANDLES edge are built from can no longer be computed inside fastapi_ext.py's
own single-file pass (see that module's own docstring for the full "why").

Consumes the three per-file claim kinds fastapi_ext.py now emits instead of a direct
Channel/HANDLES:
  - route_decl: {router_symbol, verb, path, handler_node_id, prefix_local,
    evidence_line} -- one per matched route decorator.
  - router_include: {parent_symbol, child_symbol, prefix} -- one per
    `<parent>.include_router(<child>, prefix=...)` call.
  - router_decl (M8 review Important-1): {router_symbol, prefix_local} -- one per
    `X = APIRouter(...)`/`X = FastAPI(...)` assignment (routes or not) -- each
    router's OWN declared prefix, the piece neither of the other two claim kinds
    carries for an INTERMEDIATE chain hop.

FASTAPI COMPOSITION ORDER (M8 review Important-1 -- empirically verified against real
FastAPI 0.140.0 via its own OpenAPI schema, this task's report has the raw probe):
`APIRouter.add_api_route` registers a route at `self.prefix + path`, and
`include_router(child, prefix=ip)` re-registers each of child's routes at
`self.prefix + ip + child_route.path` -- the child's own declared prefix is already
baked into `child_route.path` by ITS OWN registration/include time. Flattened
root-to-leaf, the served path is therefore, per mount: [mounting include-kwarg
prefix] + [mounted router's own declared prefix], ..., ending with [leaf's own
prefix_local] + [decorator path]. Probe: `app.include_router(B, prefix="/ia")`,
`B = APIRouter(prefix="/pb")`, `B.include_router(A, prefix="/ib")`,
`A = APIRouter(prefix="/pa")`, route "/x" -> served at `/ia/pb/ib/pa/x`; the review's
own versioned-aggregator shape (bare A, `B = APIRouter(prefix="/v2")` with no routes,
`app.include_router(B, prefix="/api")`) -> `/api/v2/x`.

ALGORITHM (`link`): build a `child_symbol -> (parent_symbol, prefix)` graph from every
staged `router_include` claim plus a `router_symbol -> own declared prefix` map from
every `router_decl` claim (workspace-wide, no per-service scoping needed -- every
symbol already bakes `service` into its own id, see resolvers/scip/symbols.
symbol_to_node_id, so two different services' routers can never collide or cross-link
by construction). For each `route_decl` claim, DFS from its own `router_symbol` UP the
graph (child -> parent, repeatedly) to a root (nobody's `include_router` call ever
named it as arg0 -- a bare `FastAPI()` app object, or a router that simply isn't
nested inside anything else), composing via the recurrence
`g(X) = g(parent) + parent_own_prefix + include_prefix`, `g(root) = ""` -- which
flattens to exactly the verified order above -- then template =
`g(leaf) + prefix_local + path`. The leaf's own prefix comes from
route_decl.prefix_local and is NEVER double-counted (g() only ever adds PARENT hops'
own prefixes; the leaf's own router_decl claim exists but is only consulted when the
leaf serves as a parent for an even deeper router). `prefix_local + path` itself is
computed identically to fastapi_ext.py's pre-M8 direct-template behavior, see
`_template` below, a deliberate byte-for-byte copy of the function this module's own
logic superseded.

HONESTY RULE (mirrors linking/http_routes.py's own binding "NO static/1.0 without
anchor, ever" constraint -- no guessing, ever): FOUR distinct failure shapes ALL
collapse to the exact same outcome -- the composed (ancestor) prefix is DISCARDED
ENTIRELY, not partially applied, falling back to `prefix_local + path` alone, and the
route is counted in `route_prefix_unresolved`:
  1. `router_symbol` itself is None (unresolvable at extraction time -- no SCIP wired,
     a degraded/heuristic-fallback run -- resolvers/fallback.py never lays a def at an
     assignment target at all, only at class/function defs -- or a genuine SCIP miss).
  2. A CYCLE in the include graph (A includes B, B includes A -- never structurally
     valid FastAPI, but claims are per-file and blind to the workspace-wide graph
     shape, so this module must guard against it explicitly rather than recursing
     forever).
  3. An UNRESOLVABLE or AMBIGUOUS hop partway up the chain: a router_include claim
     whose own parent_symbol is None (the file that includes this router couldn't
     itself be identified), or two DISTINCT parents both naming the identical
     child_symbol (a genuine include-graph ambiguity -- silently picking one would
     repeat the exact "false match worse than absence" mistake M7 T3's own funnel fix
     was written to prevent, just one layer up the stack).
  4. (M8 review Important-1) A hop PARENT with NO router_decl claim for its symbol
     (its own declared prefix is simply unknown -- e.g. a factory-built router,
     `router = create_router()`, whose assignment matches no APIRouter/FastAPI
     callee), or with CONFLICTING router_decl claims (two different prefix_local
     values for one symbol -- a same-symbol re-declaration ambiguity). Composing
     while silently ASSUMING an unknown parent's own prefix is empty is precisely
     the incomplete-confident-template bug the review caught -- the versioned
     aggregator (`B = APIRouter(prefix="/v2")`, no routes of its own) is an ordinary
     FastAPI convention, and its /v2 was invisible to every claim form before
     router_decl existed.

The TRIVIAL case -- no `router_include` claim anywhere names this router_symbol as a
child at all (a genuine root: same-file `APIRouter(prefix=...)`, zero cross-file
`include_router` involvement) -- is NOT a failure: the accumulated ancestor prefix is
simply "", giving `prefix_local + path` again, but WITHOUT bumping the counter. THIS is
the CRITICAL CONSTRAINT case: every M2/M6/M7 golden fixture route composes through the
""-ancestor-prefix path today (proven empirically by decoding fixtures/.codegraph/
scip's own orders-api index -- `app.include_router(orders_router)` carries no `prefix=`
kwarg and `app = FastAPI(...)` has no prefix concept, so even orders-api's own real
cross-file chain composes an empty ancestor prefix, now THROUGH the app's own
router_decl claim rather than around it) -- golden HANDLES/CALLS_HTTP tuples must not
shift by one byte.

Channel/HANDLES creation mirrors fastapi_ext.py's OLD direct-emission shape exactly
(`make_channel_node("http_route", ...)` + HANDLES chan->handler, evidence_file/
evidence_line restored from the claim's own _relpath/evidence_line -- M8 review
Important-2, the identical claim-evidence pass-through linking/http_routes.py's
CALLS_HTTP edges already do) -- just relocated here, `extractor="linking"`/
`origin=None` instead of "fastapi" (cleared by `clear_workspace_layer`, rebuilt fresh
every S7 run, same as CALLS_HTTP/NEXT_SEGMENT already are -- Channel-GC continues to
work exactly as documented, just now doing a "GC-then-recreate" pass over EVERY
http_route channel each run instead of only the rare unresolved-fallback one; see
`stores.staging.Staging.gc_orphan_channels`'s own docstring for why that pattern is
harmless, not data loss). `evalx.edges_eval` does not compare extractor/resolution at
all for its golden-tuple gate (verified by reading `found_edges`/`load_golden_edges`
directly, per this task's own Step 1) -- only `(type, src_service, src_qualified,
dst_channel_id)` for HANDLES -- so this relocation alone cannot shift any golden
HANDLES/CALLS_HTTP tuple.

Wired into `linking.workspace.link_workspace`, BEFORE `http_routes.link` (that stage's
own `_route_table` scan reads whatever Channel(http_route) nodes are ALREADY staged --
this module is what stages them now, where fastapi_ext.py used to)."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

# Sentinel, two uses: (a) 2+ DISTINCT parents independently claim to include the
# identical child_symbol (include-graph ambiguity, honesty-rule failure shape 3);
# (b) 2+ router_decl claims carry DIFFERENT prefix_local values for one symbol
# (own-prefix ambiguity, failure shape 4) -- never silently resolved either way.
_AMBIGUOUS = object()


def _template(prefix: str, path: str) -> str:
    """Byte-for-byte copy of fastapi_ext.py's OLD (pre-M8) `_template` -- prefix +
    path; empty path -> prefix alone; both empty -> "/" (root). Duplicated rather
    than imported: this two-line pure function is not real logic to keep in sync (no
    fastapi_ext.py import here keeps this module's own dependency surface limited to
    core.schema/stores.staging, matching linking/http_routes.py's own precedent of
    never importing a domain extractor module)."""
    if not path:
        return prefix if prefix else "/"
    return prefix + path


class _IncludeEntry:
    """One resolved (non-ambiguous) `router_include` claim, graphed by its own
    child_symbol -- see `_build_include_graph`."""

    __slots__ = ("parent", "prefix")

    def __init__(self, parent: str | None, prefix: str) -> None:
        self.parent = parent
        self.prefix = prefix


def _build_include_graph(staging: Staging) -> dict[str, _IncludeEntry | object]:
    """child_symbol -> _IncludeEntry(parent_symbol, prefix) | _AMBIGUOUS.

    Claims with child_symbol=None carry no identity to graph anything under at all --
    dropped outright (not an error at THIS stage: an unrelated route_decl elsewhere
    is never affected by a claim that names no child, see the module's own unusable-
    claim test). A parent_symbol=None entry IS kept, graphed under its own
    child_symbol -- `_resolve_prefix` below treats a None parent as a resolution
    FAILURE at that hop (not as "no entry at all", which would wrongly read as "this
    is a root")."""
    graph: dict[str, _IncludeEntry | object] = {}
    for claim in staging.claims_for("router_include"):
        child = claim.get("child_symbol")
        if child is None:
            continue
        entry = _IncludeEntry(claim.get("parent_symbol"), claim.get("prefix") or "")
        graph[child] = _AMBIGUOUS if child in graph else entry
    return graph


def _build_own_prefix_map(staging: Staging) -> dict[str, str | object]:
    """M8 review Important-1: router_symbol -> its OWN declared prefix (router_decl
    claims) | _AMBIGUOUS (conflicting prefix_local values for one symbol). A symbol
    entirely ABSENT from this map means "own prefix unknown" -- `_resolve_prefix`
    treats that as a hop failure whenever the symbol serves as a PARENT (honesty-rule
    failure shape 4), never as an implicit ''. Duplicate claims with the IDENTICAL
    prefix are naturally idempotent (the claims-table PK already collapses
    byte-identical payloads per (service, relpath); cross-file re-declarations of the
    same symbol with the same prefix are also fine -- same value, no conflict)."""
    own: dict[str, str | object] = {}
    for claim in staging.claims_for("router_decl"):
        sym = claim.get("router_symbol")
        if sym is None:
            continue
        prefix = claim.get("prefix_local") or ""
        if sym not in own:
            own[sym] = prefix
        elif own[sym] != prefix:
            own[sym] = _AMBIGUOUS
    return own


def _resolve_prefix(
    symbol: str,
    graph: dict[str, _IncludeEntry | object],
    own_prefix: dict[str, str | object],
    memo: dict[str, str | None],
    in_progress: set[str],
) -> str | None:
    """Accumulated prefix from every router that (transitively) includes `symbol`,
    root-first, hop-parents' OWN declared prefixes included -- the recurrence
    `g(symbol) = g(parent) + parent_own_prefix + include_prefix`, `g(root) = ""`
    (see module docstring's FASTAPI COMPOSITION ORDER section for the empirical
    derivation). "" if `symbol` is itself a root. None signals a resolution FAILURE
    (cycle / ambiguous include / unresolvable parent / missing-or-conflicting
    parent router_decl) -- the caller (`link`) treats None as "give up on the WHOLE
    chain", never a partial prefix (see module docstring's honesty rule). Memoized
    across the whole `link()` call: a cycle or failure discovered from one route's
    own walk is remembered identically for every OTHER route that shares any node of
    that same walk."""
    if symbol in memo:
        return memo[symbol]
    if symbol in in_progress:
        return None  # cycle: this symbol is its own (transitive) ancestor
    in_progress.add(symbol)

    entry = graph.get(symbol)
    if entry is None:
        result: str | None = ""  # root -- nobody includes this router
    elif entry is _AMBIGUOUS:
        result = None
    else:
        assert isinstance(entry, _IncludeEntry)
        parent = entry.parent
        # The parent's OWN declared prefix (router_decl) is part of the real served
        # path (see module docstring) -- missing (None) or conflicting (_AMBIGUOUS)
        # fails the hop BEFORE any recursion; an unknown own-prefix must never be
        # silently assumed empty (M8 review Important-1's exact bug).
        parent_own = own_prefix.get(parent) if parent is not None else None
        if parent is None or parent_own is None or parent_own is _AMBIGUOUS:
            result = None
        else:
            assert isinstance(parent_own, str)
            parent_prefix = _resolve_prefix(parent, graph, own_prefix, memo, in_progress)
            result = (
                None if parent_prefix is None
                else parent_prefix + parent_own + entry.prefix
            )

    in_progress.discard(symbol)
    memo[symbol] = result
    return result


def link(staging: Staging) -> dict:
    """S7 entry point (called from linking.workspace.link_workspace, BEFORE
    http_routes.link). staging-only (no FalkorDB access), mirrors http_routes.link's
    own signature shape minus the (unneeded here) WorkspaceConfig parameter -- no
    env/service-registry concerns, purely a claims -> graph -> Channel/HANDLES
    composition. Returns {"route_prefix_unresolved": <count>} -- the number of
    route_decl claims whose composition fell back to the local-only template (see
    module docstring's honesty rule for the four failure shapes this counts)."""
    graph = _build_include_graph(staging)
    own_prefix = _build_own_prefix_map(staging)
    memo: dict[str, str | None] = {}

    channels: dict[str, NodeRec] = {}
    edges: list[EdgeRec] = []
    unresolved = 0

    for claim in staging.claims_for("route_decl"):
        prefix_local = claim["prefix_local"]
        path = claim["path"]
        router_symbol = claim.get("router_symbol")

        chain_prefix = (
            _resolve_prefix(router_symbol, graph, own_prefix, memo, set())
            if router_symbol is not None else None
        )
        if chain_prefix is None:
            template = _template(prefix_local, path)
            unresolved += 1
        else:
            template = _template(chain_prefix + prefix_local, path)

        method = claim["verb"]
        chan = make_channel_node(
            "http_route", owner_service=claim["_service"], method=method, template=template,
            http_method=method, path_template=template,
        )
        channels[chan.id] = chan
        edges.append(EdgeRec(
            src=chan.id, dst=claim["handler_node_id"], type="HANDLES",
            resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
            # M8 review Important-2: evidence restored from the claim itself --
            # evidence_file from claims_for's injected _relpath, evidence_line from
            # route_decl's own field (the handler def's start_line, the exact value
            # the pre-M8 direct-emission HANDLES carried) -- mirrors
            # http_routes.py's own CALLS_HTTP claim-evidence pass-through.
            evidence_file=claim.get("_relpath"),
            evidence_line=claim.get("evidence_line"),
        ))

    if channels:
        staging.upsert_nodes(list(channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {"route_prefix_unresolved": unresolved}
