"""M8 T1 (rerun-2 R4 -- docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
composes FastAPI route path templates across `include_router` chains that span file
(and, structurally, service) boundaries -- the identity a route's Channel(http_route)
node and HANDLES edge are built from can no longer be computed inside fastapi_ext.py's
own single-file pass (see that module's own docstring for the full "why").

Consumes the two per-file claim kinds fastapi_ext.py now emits instead of a direct
Channel/HANDLES:
  - route_decl: {router_symbol, verb, path, handler_node_id, prefix_local} -- one per
    matched route decorator.
  - router_include: {parent_symbol, child_symbol, prefix} -- one per
    `<parent>.include_router(<child>, prefix=...)` call.

ALGORITHM (`link`): build a `child_symbol -> (parent_symbol, prefix)` graph from every
staged `router_include` claim (workspace-wide, no per-service scoping needed -- every
symbol already bakes `service` into its own id, see resolvers/scip/symbols.
symbol_to_node_id, so two different services' routers can never collide or cross-link
by construction). For each `route_decl` claim, DFS from its own `router_symbol` UP the
graph (child -> parent, repeatedly), concatenating each hop's `prefix` in root-to-leaf
order, until a node with NO parent is reached (a root: nobody's `include_router` call
ever named it as arg0 -- a bare `FastAPI()` app object, or a router that simply isn't
nested inside anything else). That accumulated ancestor prefix is prepended to
`prefix_local + path` (the route's own same-file `APIRouter(prefix=...)` and literal
decorator path -- computed identically to fastapi_ext.py's pre-M8 direct-template
behavior, see `_template` below, a deliberate byte-for-byte copy of the function this
module's own logic superseded).

HONESTY RULE (mirrors linking/http_routes.py's own binding "NO static/1.0 without
anchor, ever" constraint -- no guessing, ever): three distinct failure shapes ALL
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

The TRIVIAL case -- no `router_include` claim anywhere names this router_symbol as a
child at all (a genuine root: same-file `APIRouter(prefix=...)`, zero cross-file
`include_router` involvement) -- is NOT a failure: the accumulated ancestor prefix is
simply "", giving `prefix_local + path` again, but WITHOUT bumping the counter. THIS is
the CRITICAL CONSTRAINT case: every M2/M6/M7 golden fixture route composes through
exactly this path today (proven empirically by decoding fixtures/.codegraph/scip's own
orders-api index -- `app.include_router(orders_router)` carries no `prefix=` kwarg at
all, so even orders-api's own real cross-file chain contributes an empty ancestor
prefix) -- golden HANDLES/CALLS_HTTP tuples must not shift by one byte.

DELIBERATE SCOPE BOUNDARY (documented, not an oversight): only the LEAF router's own
`prefix_local` (the one that actually owns the route) is composed with ancestor
`include_router(..., prefix=...)` arguments -- an INTERMEDIATE router's OWN
`APIRouter(prefix=...)` (set at its own declaration, independent of how a later
`include_router` call re-includes it) is NOT separately folded in, because
router_include's own claim shape (`{parent_symbol, child_symbol, prefix}`, this task's
own binding interface spec) carries no "child's own declared prefix" field -- doing so
would need a THIRD claim shape this task does not introduce. Every real shape this
task's own proof evidence covers (the rerun-2 R4 pilot's 3-level chain, and this
module's own two/three-level tests) has EMPTY intermediate-router prefixes, so this
scope cut costs nothing there; a workspace with a NON-empty intermediate-router prefix
would under-compose (missing that one segment) rather than crash or silently guess --
a real, but narrower and differently-shaped, limitation than the one this task fixes,
left for a future task if it ever proves to matter on real code.

Channel/HANDLES creation mirrors fastapi_ext.py's OLD direct-emission shape exactly
(`make_channel_node("http_route", ...)` + HANDLES chan->handler) -- just relocated
here, `extractor="linking"`/`origin=None` instead of "fastapi" (cleared by
`clear_workspace_layer`, rebuilt fresh every S7 run, same as CALLS_HTTP/NEXT_SEGMENT
already are -- Channel-GC continues to work exactly as documented, just now doing a
"GC-then-recreate" pass over EVERY http_route channel each run instead of only the rare
unresolved-fallback one; see `stores.staging.Staging.gc_orphan_channels`'s own
docstring for why that pattern is harmless, not data loss). `evalx.edges_eval` does not
compare extractor/resolution at all for its golden-tuple gate (verified by reading
`found_edges`/`load_golden_edges` directly, per this task's own Step 1) -- only
`(type, src_service, src_qualified, dst_channel_id)` for HANDLES -- so this relocation
alone cannot shift any golden HANDLES/CALLS_HTTP tuple.

Wired into `linking.workspace.link_workspace`, BEFORE `http_routes.link` (that stage's
own `_route_table` scan reads whatever Channel(http_route) nodes are ALREADY staged --
this module is what stages them now, where fastapi_ext.py used to)."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

# Sentinel: 2+ DISTINCT parents independently claim to include the identical
# child_symbol -- a real include-graph ambiguity, never silently resolved to either
# one (see module docstring's honesty rule, failure shape 3).
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


def _resolve_prefix(
    symbol: str,
    graph: dict[str, _IncludeEntry | object],
    memo: dict[str, str | None],
    in_progress: set[str],
) -> str | None:
    """Accumulated prefix from every router that (transitively) includes `symbol`,
    root-first -- "" if `symbol` is itself a root. None signals a resolution FAILURE
    (cycle / ambiguous / unresolvable parent partway up) -- the caller (`link`) treats
    None as "give up on the WHOLE chain", never a partial prefix (see module
    docstring's honesty rule). Memoized across the whole `link()` call: a cycle or
    failure discovered from one route's own walk is remembered identically for every
    OTHER route that shares any node of that same walk."""
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
        if entry.parent is None:
            result = None
        else:
            parent_prefix = _resolve_prefix(entry.parent, graph, memo, in_progress)
            result = None if parent_prefix is None else parent_prefix + entry.prefix

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
    module docstring's honesty rule for the three failure shapes this counts)."""
    graph = _build_include_graph(staging)
    memo: dict[str, str | None] = {}

    channels: dict[str, NodeRec] = {}
    edges: list[EdgeRec] = []
    unresolved = 0

    for claim in staging.claims_for("route_decl"):
        prefix_local = claim["prefix_local"]
        path = claim["path"]
        router_symbol = claim.get("router_symbol")

        chain_prefix = (
            _resolve_prefix(router_symbol, graph, memo, set())
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
            evidence_file=claim.get("_relpath"),
        ))

    if channels:
        staging.upsert_nodes(list(channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {"route_prefix_unresolved": unresolved}
