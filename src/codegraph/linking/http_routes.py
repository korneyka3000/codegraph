"""M2 T7: matches staged `http_call` claims (T6's http_client_ext) onto the cross-
service http_route Channel table (T4's fastapi_ext output, staged per-service, unified
here simply by reading across ALL services at once -- no separate dedup step needed,
see workspace.py's module docstring for why a distinct linking/channels.py isn't
warranted).

Route table: every staged Channel node with props.channel_kind == "http_route" AND a
resolved owner_service (i.e. NOT a previous run's own owner="?" fallback channel --
see below) contributes one route, keyed by (owner_service, http_method, path_template)
-- all three are staged verbatim in the Channel's own props by fastapi_ext (module docstring
core/schema.py's make_channel_node: method/template only land in props if the caller passes
them as extra kwargs, which fastapi_ext does).

Matching a claim against the table:
  1. verb: exact match against props.http_method.
  2. candidate narrowing: if the claim carries a base_url_env, only routes owned by a
     service whose `ServiceConfig.http.base_url_env` equals it are considered; a claim
     with base_url_env=None considers every service's routes (no narrowing).
  3. template: segment-wise comparison (split on "/"), same segment COUNT required;
     each segment pair must be literally equal OR at least one side is a `{param}`-shaped
     placeholder (bidirectional wildcard -- the client's own path param name need not
     match the route's, and a client-side literal segment against a route placeholder is
     also accepted, e.g. a hardcoded `/orders/42` call against route `/orders/{order_id}`).

On a match: CALLS_HTTP(src=claim.src_id, dst=route.channel_id), resolution=claim's own
resolution_hint ("static"/"heuristic" from http_client_ext), confidence 1.0/0.6
respectively, extractor="linking".

On NO match (ambiguous zero-candidate case; ties are broken deterministically by sorting
candidates, see `_candidates`): a synthetic Channel(http_route, owner=None -> id renders
"?", unresolved=True prop) is created/reused (id is deterministic on (verb, path_template)
alone, so two unrelated unresolved claims that happen to share verb+path collapse onto the
SAME node via upsert's INSERT OR REPLACE -- not a bug, just the same "id-determinism is
the dedup" pattern used everywhere else in M2) + CALLS_HTTP to it, resolution="heuristic",
confidence 0.5 (deliberately below every resolved tier -- an unresolved match is a WEAKER
claim than even a heuristic-tier resolved one), and the claim is counted in
calls_http_unresolved.
"""

from __future__ import annotations

from typing import NamedTuple

from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_UNRESOLVED_RESOLUTION = "heuristic"
_UNRESOLVED_CONFIDENCE = 0.5
_RESOLUTION_CONFIDENCE = {"static": 1.0, "heuristic": 0.6}


class _Route(NamedTuple):
    channel_id: str
    owner_service: str
    method: str
    template: str


def _route_table(staging: Staging) -> list[_Route]:
    routes = []
    for n in staging.iter_nodes():
        if n.kind != "Channel" or n.props.get("channel_kind") != "http_route":
            continue
        owner = n.props.get("owner_service")
        method = n.props.get("http_method")
        template = n.props.get("path_template")
        # owner_service absent -- an unresolved fallback channel from a PRIOR run (or a
        # malformed/foreign channel); not a real route to match new claims against.
        if owner is None or method is None or template is None:
            continue
        routes.append(_Route(channel_id=n.id, owner_service=owner, method=method,
                              template=template))
    return routes


def _is_placeholder(segment: str) -> bool:
    return len(segment) > 2 and segment.startswith("{") and segment.endswith("}")


def _templates_match(route_template: str, claim_template: str) -> bool:
    route_segs = route_template.split("/")
    claim_segs = claim_template.split("/")
    if len(route_segs) != len(claim_segs):
        return False
    return all(
        r == c or _is_placeholder(r) or _is_placeholder(c)
        for r, c in zip(route_segs, claim_segs, strict=True)
    )


def _allowed_services(cfg: WorkspaceConfig, base_url_env: str | None) -> set[str] | None:
    """None means "no narrowing" (claim carried no base_url_env)."""
    if base_url_env is None:
        return None
    return {
        svc.name for svc in cfg.services
        if svc.http is not None and svc.http.base_url_env == base_url_env
    }


def _candidates(routes: list[_Route], claim: dict, cfg: WorkspaceConfig) -> list[_Route]:
    allowed = _allowed_services(cfg, claim.get("base_url_env"))
    matches = [
        r for r in routes
        if r.method == claim["verb"]
        and (allowed is None or r.owner_service in allowed)
        and _templates_match(r.template, claim["path_template"])
    ]
    # Deterministic tie-break for the (unlikely, but structurally possible) ambiguous
    # case: more than one route matches verb+env-narrowing+template shape.
    return sorted(matches, key=lambda r: (r.owner_service, r.method, r.template, r.channel_id))


def _resolved_edge(claim: dict, route: _Route) -> EdgeRec:
    resolution = claim.get("resolution_hint") or "heuristic"
    confidence = _RESOLUTION_CONFIDENCE.get(resolution, _RESOLUTION_CONFIDENCE["heuristic"])
    return EdgeRec(
        src=claim["src_id"], dst=route.channel_id, type="CALLS_HTTP",
        resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
        evidence_file=claim.get("_relpath"), evidence_line=claim.get("evidence_line"),
    )


def _unresolved_channel_and_edge(claim: dict) -> tuple[NodeRec, EdgeRec]:
    chan = make_channel_node(
        "http_route", method=claim["verb"], template=claim["path_template"],
        http_method=claim["verb"], path_template=claim["path_template"], unresolved=True,
    )
    edge = EdgeRec(
        src=claim["src_id"], dst=chan.id, type="CALLS_HTTP",
        resolution=_UNRESOLVED_RESOLUTION, confidence=_UNRESOLVED_CONFIDENCE,
        extractor=_EXTRACTOR, evidence_file=claim.get("_relpath"),
        evidence_line=claim.get("evidence_line"),
    )
    return chan, edge


def link(cfg: WorkspaceConfig, staging: Staging) -> dict:
    routes = _route_table(staging)
    claims = staging.claims_for("http_call")

    edges: list[EdgeRec] = []
    unresolved_channels: dict[str, NodeRec] = {}  # id -> node, dedup within this call
    unresolved = 0

    for claim in claims:
        candidates = _candidates(routes, claim, cfg)
        if candidates:
            edges.append(_resolved_edge(claim, candidates[0]))
        else:
            chan, edge = _unresolved_channel_and_edge(claim)
            unresolved_channels[chan.id] = chan
            edges.append(edge)
            unresolved += 1

    if unresolved_channels:
        staging.upsert_nodes(list(unresolved_channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {"calls_http": len(edges), "calls_http_unresolved": unresolved}
