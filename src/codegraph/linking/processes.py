"""M2 T7 / M3 T2: materializes BusinessProcess anchors + PART_OF_PROCESS traces.

Two sources of anchors:
  1. `cfg.processes` (user-authored `ProcessDecl`s): `entrypoint` is a selector string
     "<service>:<rest>", parsed by `core.selectors.parse_selector` into two shapes --
       - RouteSelector ("<service>:<METHOD> <path>") -> resolved by finding the staged
         Channel(http_route) with matching (owner_service, http_method, path_template)
         [EXACT match, unlike http_routes.link's fuzzy segment matching -- a config
         selector names a route the user already knows exists verbatim] and following
         its HANDLES edge to the handler node.
       - QualifiedSelector ("<service>:<dotted.qualified.name>") -> resolved directly
         against the staged nodes table by (service, qualified_name).
     source="config". Selectors that resolve to nothing are skipped (counted in
     processes_unresolved), not raised -- a typo in one process declaration shouldn't
     abort the whole linking pass.
  2. every staged node carrying the TemporalWorkflow role gets its OWN anchor
     automatically, source="temporal", entrypoint = the workflow node itself. Slug is
     SERVICE-QUALIFIED (`slugify(f"{service} {name}")`), unlike config anchors (slugified
     from the user's own, presumably-workspace-unique, process name) -- workflow CLASS
     names are a much more plausible collision surface across independently-developed
     services (e.g. two services both defining a class literally called "Workflow"),
     and proc: ids collide silently (upsert REPLACE) if two anchors reduce to the same
     slug, so auto-anchors get the extra qualifier defensively.

PART_OF_PROCESS derivation (M3 T2 rework -- see .superpowers/sdd/m3-task-2-report.md for
the live-probe evidence backing every claim below):

  THE BUG this replaces: the M2 version BFS'd directly over NEXT_SEGMENT edges, starting
  from each anchor's OWN entrypoint node. That is inert on every real graph: NEXT_SEGMENT
  edges are derived by segments.py from channel-BOUNDARY edges (PRODUCES/CALLS_HTTP on the
  producer side), and a channel-boundary edge's src is whichever function/method literally
  makes the producing call -- almost never the segment's own entry node. E.g. the "Order
  KYC onboarding" process's entrypoint is `create_order` (the route handler), but the
  PRODUCES edge into the "OrderCreated" event channel has src=`OrderService.place` --
  called DIRECTLY by `create_order` (`service.place(req)`, one call below the handler),
  but a DIFFERENT node nonetheless. `create_order` itself has NO outgoing NEXT_SEGMENT
  edge -- the BFS looked at an empty adjacency list and stopped at order 0, every time,
  on every graph shaped like this (which is all of them).

  THE FIX: before grouping NEXT_SEGMENT edges into a walkable adjacency, each edge's SRC is
  first climbed UP to its owning segment entry via `_entry_of` -- reverse-adjacency over
  the staged INTRA edges (CALLS/DEPENDS_ON/INVOKES_ACTIVITY, including a temporal_start-
  tagged CALLS -- workspace.py's `_apply_temporal_start_marks` writes exactly this type) --
  stopping at the first node (starting at the src itself) that either carries a
  RouteHandler/MessageConsumer/TemporalWorkflow role, or has no incoming intra edge at all
  (a "root" of its own local call subgraph -- the climb has nowhere further to go, so
  that's the best available entry). `_entry_graph` then builds an entry->entry adjacency:
  for every NEXT_SEGMENT edge (src -> dst), an entry-graph edge (_entry_of(src) -> dst) --
  dst is used AS-IS, never climbed, because segments.py's own pairing rules already
  guarantee dst is a segment entry by construction (a CONSUMES edge's src, or a HANDLES
  edge's dst -- see segments.py's module docstring). The SAME per-anchor BFS then walks
  this entry->entry graph instead of the raw NEXT_SEGMENT adjacency; everything about the
  BFS itself (order = hop count, first-seen wins on rediscovery, resolution/confidence
  copied as-is from the discovering edge) is unchanged from M2.

  Ambiguity: if a node being climbed has MORE than one intra-edge predecessor (more than
  one caller), there is no principled way to know which caller is "the" path back to the
  segment entry -- the climb picks the lexicographically-first predecessor id
  (deterministic, matches every other tie-break in this codebase) and counts the pick in
  `part_of_process_ambiguous` (surfaced in both `materialize`'s and `link_workspace`'s
  returned stats) so a controller can see how often this happened on a real graph without
  it silently corrupting a trace.

  Cycles: `_entry_of` tracks visited nodes for the CURRENT climb only (caller-owned `visited`
  set, fresh per NEXT_SEGMENT edge) -- a node already visited on this climb is returned
  as-is instead of re-descending into it, guaranteeing termination.

  Safety cap: `_trace_segments`' BFS stops claiming new PART_OF_PROCESS members once
  `_MAX_PART_OF_PROCESS_NODES` (100) nodes have been claimed for a single anchor -- a
  defensive bound against a pathological/huge real graph, not expected to ever fire on
  these fixtures (max order on the "Order KYC onboarding" fixture chain is 2).

  Resolver-quality dependency (live-reproduced fact: forced-ScipRunError analyze x3 +
  link_workspace on fixtures/workspace.yaml -> max PART_OF_PROCESS order == 0 across
  ALL processes; reviewer-verified, pinned by the degraded test in
  tests/integration/test_processes_real_shape.py): the climb is only as good as the
  staged intra edges, and the heuristic degraded resolver (resolvers/fallback.py, used
  when scip-python is unavailable) only ever builds refs for calls whose callee NAME is
  a bare top-level def (same file or direct from-import) -- no type inference, no
  method-call resolution, no refs for non-call name references. On these fixtures that
  breaks the chain at BOTH critical points: (a) `service.place(req)` -- a method call
  on a locally-typed variable -- is unresolvable, so degraded's only CALLS edge out of
  `create_order` targets the OrderService CLASS ctor, `OrderService.place` gets NO
  incoming intra edge at all, `_entry_of` returns place itself (its own disconnected
  root), and the NEXT_SEGMENT edge it produces is keyed away from `create_order`'s BFS
  entirely; (b) kyc-worker's dispatch_dict consumer handler (a bare name used as a dict
  VALUE, not a call site) never gets a ref either, so `handle_order_created` has no
  CONSUMES edge/MessageConsumer role and everything past it is likewise unreachable.
  Degraded mode therefore stalls at max order 0 -- real scip-python is REQUIRED for the
  full real-shape chain (max order 2, the scip-marked test in the same file).

Edge direction is node -PART_OF_PROCESS-> process ("this node is part of this process",
readable subject-first). resolution/confidence for order 0 is ("static", 1.0) -- the
entrypoint's membership is a structural fact of config/role, not a probabilistic
derivation; for order >= 1 they are copied AS-IS from the single NEXT_SEGMENT edge that
first discovered that node (a per-hop view, not a cumulative product across the whole
path, and NOT discounted by the climb that reattributed its src -- the climb is a
structural re-pointing, not a probabilistic hop of its own -- kept simple to match the
"лёгкий BFS" the brief asks for; T8's trace_process is where a full weighted path view
belongs).
"""

from __future__ import annotations

from codegraph.config.models import WorkspaceConfig
from codegraph.core import ids
from codegraph.core.schema import EdgeRec, NodeRec, make_process_node
from codegraph.core.selectors import QualifiedSelector, RouteSelector, parse_selector
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_INTRA_EDGE_TYPES = frozenset({"CALLS", "DEPENDS_ON", "INVOKES_ACTIVITY"})
_ENTRY_ROLES = frozenset({"RouteHandler", "MessageConsumer", "TemporalWorkflow"})
_MAX_PART_OF_PROCESS_NODES = 100


def _qualified_index(staging: Staging) -> dict[tuple[str, str], str]:
    index: dict[tuple[str, str], str] = {}
    for n in sorted(staging.iter_nodes(), key=lambda n: n.id):
        index.setdefault((n.service, n.qualified_name), n.id)
    return index


def _route_index(staging: Staging) -> dict[tuple[str, str, str], str]:
    """(owner_service, http_method, path_template) -> channel_id, EXACT match only --
    see module docstring for why this differs from http_routes.link's fuzzy matching."""
    index: dict[tuple[str, str, str], str] = {}
    for n in sorted(staging.iter_nodes(), key=lambda n: n.id):
        if n.kind != "Channel" or n.props.get("channel_kind") != "http_route":
            continue
        owner, method, template = (
            n.props.get("owner_service"), n.props.get("http_method"), n.props.get("path_template"),
        )
        if owner is None or method is None or template is None:
            continue
        index.setdefault((owner, method, template), n.id)
    return index


def _handles_index(staging: Staging) -> dict[str, str]:
    """channel_id -> handler node id, first (sorted) HANDLES edge per channel."""
    index: dict[str, str] = {}
    for e in sorted(staging.iter_edges(), key=lambda e: (e.src, e.dst)):
        if e.type == "HANDLES":
            index.setdefault(e.src, e.dst)
    return index


def _resolve_entrypoint(
    selector: str,
    qualified_index: dict[tuple[str, str], str],
    route_index: dict[tuple[str, str, str], str],
    handles_index: dict[str, str],
) -> str | None:
    parsed = parse_selector(selector)
    if parsed is None:
        return None
    if isinstance(parsed, RouteSelector):
        channel_id = route_index.get((parsed.service, parsed.method, parsed.path))
        if channel_id is None:
            return None
        return handles_index.get(channel_id)
    assert isinstance(parsed, QualifiedSelector)
    return qualified_index.get((parsed.service, parsed.qualified))


def resolve_selector(staging: Staging, selector: str) -> str | None:
    """Public single-selector resolver reused by CLI `trace` (see cli.py's `trace`
    command) -- same "<service>:<METHOD> <path>" / "<service>:qualified.name"
    grammar and resolution as materialize()'s own cfg.processes loop, exposed
    standalone so cli.py doesn't reimplement the selector parser a second time
    (see module docstring). Builds the three lookup indices fresh from staging on
    every call -- materialize() amortizes that cost across many decls in one pass,
    but a CLI invocation resolves exactly one selector, so the fresh-scan cost here
    is a non-issue and keeps this function trivially independent of any caller
    state."""
    qualified_index = _qualified_index(staging)
    route_index = _route_index(staging)
    handles_index = _handles_index(staging)
    return _resolve_entrypoint(selector, qualified_index, route_index, handles_index)


def _temporal_workflow_nodes(staging: Staging) -> list[NodeRec]:
    return sorted(
        (n for n in staging.iter_nodes() if "TemporalWorkflow" in n.roles),
        key=lambda n: n.id,
    )


def _intra_reverse_adjacency(staging: Staging) -> dict[str, list[str]]:
    """dst -> [src, ...] over staged intra edges (CALLS/DEPENDS_ON/INVOKES_ACTIVITY --
    _INTRA_EDGE_TYPES), i.e. "who calls/depends-on/invokes this node" -- the graph
    `_entry_of` climbs UP through. A temporal_start-tagged CALLS (props.mechanism ==
    "temporal_start", written by workspace.py's `_apply_temporal_start_marks`) is a
    perfectly ordinary CALLS edge at this layer -- included same as any other, not
    filtered by props (unlike evalx's found_edges, which excludes it for a DIFFERENT
    purpose -- golden-fixture symmetry, not chain derivation; see core/schema.py
    SCHEMA_VERSION's M3 T1 history entry)."""
    adj: dict[str, list[str]] = {}
    for e in staging.iter_edges():
        if e.type in _INTRA_EDGE_TYPES:
            adj.setdefault(e.dst, []).append(e.src)
    return adj


def _roles_index(staging: Staging) -> dict[str, tuple[str, ...]]:
    return {n.id: n.roles for n in staging.iter_nodes()}


def _entry_of(
    node_id: str,
    intra_reverse_adj: dict[str, list[str]],
    roles_by_id: dict[str, tuple[str, ...]],
    visited: set[str],
    ambiguous: list[int] | None = None,
) -> str:
    """Climbs from `node_id` up reverse-intra-edges (predecessor -> node_id, see
    `_intra_reverse_adjacency`) to the nearest node that either carries an
    `_ENTRY_ROLES` role or has no incoming intra edge at all -- starting with
    `node_id` itself, so a node that already qualifies (either way) is returned
    unchanged with zero hops.

    `visited` is caller-owned and MUST be fresh per top-level call (one climb): it
    accumulates every node visited on THIS climb, so a cycle in the intra-edge graph
    (a <-> b calling each other) terminates the climb AT the first already-visited
    node instead of looping forever, rather than raising or silently mis-resolving.

    `ambiguous`, if given, is a mutable single-element list used as an int counter
    (not a plain int -- ints are immutable in Python, this function has no other way
    to report a count back through a chain of climbs sharing one counter): incremented
    once per climb STEP where more than one intra-edge predecessor exists (more than
    one caller -- there's no principled way to know which one is "the" path back to
    the segment entry). The tie itself is broken deterministically by picking the
    lexicographically-first predecessor id, same convention as every other
    ambiguous-match tie-break in this codebase (e.g. http_routes.py's `_candidates`)."""
    current = node_id
    while True:
        if current in visited:
            return current
        visited.add(current)
        if _ENTRY_ROLES.intersection(roles_by_id.get(current, ())):
            return current
        preds = intra_reverse_adj.get(current)
        if not preds:
            return current
        if len(preds) > 1 and ambiguous is not None:
            ambiguous[0] += 1
        current = sorted(preds)[0]


def _entry_graph(staging: Staging) -> tuple[dict[str, list[EdgeRec]], int]:
    """Builds the entry->entry adjacency `_trace_segments` walks: for every staged
    NEXT_SEGMENT edge (src -> dst), an entry-graph edge keyed by `_entry_of(src)` (the
    segment ENTRY that src is nested under) pointing at dst UNCHANGED -- dst is never
    climbed, because segments.py's own pairing rules already guarantee it's a segment
    entry by construction (a CONSUMES edge's src, or a HANDLES edge's dst; see
    segments.py's module docstring). Each adjacency list is sorted by dst for
    deterministic BFS traversal in `_trace_segments` (same convention the old
    src-keyed adjacency used).

    Returns (adjacency, ambiguous_climb_count) -- the second element sums every
    ambiguous pick (see `_entry_of`) across ALL climbs performed while building this
    graph, for `materialize`'s returned stats."""
    intra_reverse_adj = _intra_reverse_adjacency(staging)
    roles_by_id = _roles_index(staging)
    ambiguous = [0]

    adj: dict[str, list[EdgeRec]] = {}
    for e in staging.iter_edges():
        if e.type != "NEXT_SEGMENT":
            continue
        entry_id = _entry_of(e.src, intra_reverse_adj, roles_by_id, set(), ambiguous)
        adj.setdefault(entry_id, []).append(e)

    for lst in adj.values():
        lst.sort(key=lambda e: e.dst)
    return adj, ambiguous[0]


def _trace_segments(
    entry_adj: dict[str, list[EdgeRec]], entry_id: str, proc_id: str,
) -> list[EdgeRec]:
    """BFS over the entry->entry graph (see `_entry_graph`) from `entry_id` (an
    anchor's own entrypoint node -- already a segment entry by construction, either a
    resolved RouteHandler/qualified target or a TemporalWorkflow node, so it never
    itself needs `_entry_of` climbing). order = BFS hop count, first-seen order kept
    on rediscovery (cycle/diamond-reconvergence safe, same as M2). Capped at
    `_MAX_PART_OF_PROCESS_NODES` claimed nodes -- a defensive bound against an
    unexpectedly huge/pathological real graph, not expected to fire on any current
    fixture."""
    seen: dict[str, tuple[int, EdgeRec | None]] = {entry_id: (0, None)}
    frontier = [entry_id]
    while frontier and len(seen) < _MAX_PART_OF_PROCESS_NODES:
        next_frontier = []
        for node_id in frontier:
            for edge in entry_adj.get(node_id, []):
                if len(seen) >= _MAX_PART_OF_PROCESS_NODES:
                    break
                if edge.dst not in seen:
                    seen[edge.dst] = (seen[node_id][0] + 1, edge)
                    next_frontier.append(edge.dst)
            if len(seen) >= _MAX_PART_OF_PROCESS_NODES:
                break
        frontier = next_frontier

    out = []
    for node_id, (order, via_edge) in seen.items():
        resolution, confidence = (
            ("static", 1.0) if via_edge is None else (via_edge.resolution, via_edge.confidence)
        )
        out.append(EdgeRec(
            src=node_id, dst=proc_id, type="PART_OF_PROCESS",
            resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
            props={"order": order},
        ))
    return out


def materialize(cfg: WorkspaceConfig, staging: Staging) -> dict:
    qualified_index = _qualified_index(staging)
    route_index = _route_index(staging)
    handles_index = _handles_index(staging)
    entry_adj, ambiguous = _entry_graph(staging)

    process_nodes: list[NodeRec] = []
    part_of_edges: list[EdgeRec] = []
    unresolved = 0

    for decl in cfg.processes:
        entry_id = _resolve_entrypoint(decl.entrypoint, qualified_index, route_index, handles_index)
        if entry_id is None:
            unresolved += 1
            continue
        proc = make_process_node(ids.slugify(decl.name), decl.name, entry_id, "config")
        process_nodes.append(proc)
        part_of_edges.extend(_trace_segments(entry_adj, entry_id, proc.id))

    for workflow in _temporal_workflow_nodes(staging):
        slug = ids.slugify(f"{workflow.service} {workflow.name}")
        proc = make_process_node(slug, workflow.name, workflow.id, "temporal")
        process_nodes.append(proc)
        part_of_edges.extend(_trace_segments(entry_adj, workflow.id, proc.id))

    if process_nodes:
        staging.upsert_nodes(process_nodes)
    if part_of_edges:
        staging.upsert_edges(part_of_edges)

    return {
        "processes": len(process_nodes),
        "processes_unresolved": unresolved,
        "part_of_process_ambiguous": ambiguous,
    }
