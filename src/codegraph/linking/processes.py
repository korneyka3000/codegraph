"""M2 T7: materializes BusinessProcess anchors + PART_OF_PROCESS traces.

Two sources of anchors:
  1. `cfg.processes` (user-authored `ProcessDecl`s): `entrypoint` is a selector string
     "<service>:<rest>", two shapes --
       - http-route: "<service>:<METHOD> <path>" (rest splits on the FIRST space into an
         uppercase HTTP verb + an exact template) -> resolved by finding the staged
         Channel(http_route) with matching (owner_service, http_method, path_template)
         [EXACT match, unlike http_routes.link's fuzzy segment matching -- a config
         selector names a route the user already knows exists verbatim] and following
         its HANDLES edge to the handler node.
       - qualified: "<service>:<dotted.qualified.name>" (no recognized verb prefix) ->
         resolved directly against the staged nodes table by (service, qualified_name).
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

PART_OF_PROCESS: from each anchor's entrypoint node, a lightweight BFS over the
(already-derived, by the time this runs -- see workspace.py's pipeline order) NEXT_SEGMENT
edges assigns order 0 (the entrypoint itself) .. N (each further segment entry, by BFS
hop count, first-seen order kept on rediscovery so cycles terminate safely). Edge
direction is node -PART_OF_PROCESS-> process ("this node is part of this process",
readable subject-first). resolution/confidence for order 0 is ("static", 1.0) -- the
entrypoint's membership is a structural fact of config/role, not a probabilistic
derivation; for order >= 1 they are copied AS-IS from the single NEXT_SEGMENT edge that
first discovered that node (a per-hop view, not a cumulative product across the whole
path -- kept simple to match the "лёгкий BFS" the brief asks for; T8's trace_process is
where a full weighted path view belongs).
"""

from __future__ import annotations

from codegraph.config.models import WorkspaceConfig
from codegraph.core import ids
from codegraph.core.schema import EdgeRec, NodeRec, make_process_node
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_HTTP_VERBS = frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"})


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
    service, sep, rest = selector.partition(":")
    if not sep:
        return None
    verb, space, template = rest.partition(" ")
    if space and verb.upper() in _HTTP_VERBS:
        channel_id = route_index.get((service, verb.upper(), template))
        if channel_id is None:
            return None
        return handles_index.get(channel_id)
    return qualified_index.get((service, rest))


def _temporal_workflow_nodes(staging: Staging) -> list[NodeRec]:
    return sorted(
        (n for n in staging.iter_nodes() if "TemporalWorkflow" in n.roles),
        key=lambda n: n.id,
    )


def _next_segment_adjacency(staging: Staging) -> dict[str, list[EdgeRec]]:
    adj: dict[str, list[EdgeRec]] = {}
    for e in staging.iter_edges():
        if e.type == "NEXT_SEGMENT":
            adj.setdefault(e.src, []).append(e)
    for lst in adj.values():
        lst.sort(key=lambda e: e.dst)
    return adj


def _trace_segments(
    next_segment_adj: dict[str, list[EdgeRec]], entry_id: str, proc_id: str,
) -> list[EdgeRec]:
    seen: dict[str, tuple[int, EdgeRec | None]] = {entry_id: (0, None)}
    frontier = [entry_id]
    while frontier:
        next_frontier = []
        for node_id in frontier:
            for edge in next_segment_adj.get(node_id, []):
                if edge.dst not in seen:
                    seen[edge.dst] = (seen[node_id][0] + 1, edge)
                    next_frontier.append(edge.dst)
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
    next_segment_adj = _next_segment_adjacency(staging)

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
        part_of_edges.extend(_trace_segments(next_segment_adj, entry_id, proc.id))

    for workflow in _temporal_workflow_nodes(staging):
        slug = ids.slugify(f"{workflow.service} {workflow.name}")
        proc = make_process_node(slug, workflow.name, workflow.id, "temporal")
        process_nodes.append(proc)
        part_of_edges.extend(_trace_segments(next_segment_adj, workflow.id, proc.id))

    if process_nodes:
        staging.upsert_nodes(process_nodes)
    if part_of_edges:
        staging.upsert_edges(part_of_edges)

    return {"processes": len(process_nodes), "processes_unresolved": unresolved}
