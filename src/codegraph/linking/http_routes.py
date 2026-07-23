"""M2 T7: matches staged `http_call` claims (T6's http_client_ext) onto the cross-
service http_route Channel table (T4's fastapi_ext output, staged per-service, unified
here simply by reading across ALL services at once -- no separate dedup step needed,
see workspace.py's module docstring for why a distinct linking/channels.py isn't
warranted).

M7 T3 (OPEN R1 -- docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md): two
binding changes over the M2 design below, both load-bearing enough to warrant their own
account up front.

STRICT FORM (`_templates_match`): the `{param}` wildcard is now ROUTE-SIDE ONLY. A
route's own `{param}` segment still matches ANY claim segment (a client hardcoding
`/orders/42` against route `/orders/{order_id}` is exactly as valid a match as before),
but a CLAIM's `{param}` segment against a route's STATIC segment is no longer a match.
The OLD bidirectional rule ("equal OR EITHER side is a placeholder") is exactly the
mechanism that funneled three unrelated real client paths (`/api/v1/steps/{step_uid}`,
`/api/v1/requests/{verification_uid}`, `/api/v1/customer-info/{customer_uid}`) onto an
unrelated service's `/{a}/{b}/{c}/parsed-data` route in the pilot: every LEADING segment
matched via the route's OWN placeholders (legitimate, unchanged by this fix), and the
FINAL segment matched only because the old rule ALSO treated the claim's own trailing
`{step_uid}`-shaped placeholder as a wildcard against the route's static `parsed-data`
tail -- a false match no reasonable reviewer would accept if asked directly ("does a
call to a literal `parsed-data` endpoint free-associate with `.../{step_uid}`?").
Dropping that one direction breaks nothing the M2 suite already establishes as a wanted
match (route-side wildcard) while making this exact funnel structurally impossible --
see test_linking_http_routes.py's own pinned regression, using the report's own three
real path pairs verbatim.

ANCHORING TIERS (`_target`/`link`): confidence is now a function of target-service
ANCHORING plus match UNIQUENESS alone, NEVER of the claim's own `resolution_hint` (which
describes only how the PATH TEXT was built, e.g. whether an f-string had a leading
`<base>`-marker interpolation) -- the OLD design let `resolution_hint` alone drive
static/1.0 even for a claim naming no target service whatsoever, which is exactly how
the funnel's three false edges got minted at the graph's highest confidence tier. Three
tiers, evaluated per claim by `_target`:

  1. ANCHORED -- claim.base_url_env is set (explicitly by the idiom, or auto-anchored
     via http_client_ext.py's own self.host-assignment join, see that module's
     docstring) AND resolves to a real workspace service. Two sources, tried in order:
     (a) the PRE-EXISTING `ServiceConfig.http.base_url_env` registry (unchanged --
     keeps every M2/M6 fixture claim byte-identical, since their env already matches a
     configured service through it); (b) failing that, the `env_sources`-derived
     env->service map (`linking/env_map.py`, M7 T3) as an ADDITIVE fallback. Routes are
     narrowed to that ONE resolved service; a UNIQUE form-match -> resolution="static",
     confidence=1.0. Multiple form-matches within that single already-anchored service
     (a real route-shape ambiguity, e.g. two overlapping templates) -> unresolved,
     exactly like zero matches -- silently picking one would repeat the funnel bug's
     own mistake at a narrower scale.
  2. ENV KNOWN, UNMAPPED -- claim.base_url_env is set but NEITHER source above can name
     a service for it (genuinely external, or simply not modeled in this workspace).
     Unconditionally unresolved, no matching attempted at all: a coincidental path-
     shape match against an unrelated MODELED service would be actively wrong, not
     merely uncertain, so this tier never even reaches `_candidates`. The synthetic
     Channel additionally carries `config_ref=<env name>` (doctor/graph-inspection
     visibility -- same convention kafka_ext.py's own config_ref channels already use).
  3. UNANCHORED -- claim.base_url_env is None, no target-service evidence at all.
     Matched against EVERY service's routes (no narrowing, same as the M2 design); a
     UNIQUE form-match -> resolution="heuristic", confidence=0.7 (never "static",
     regardless of resolution_hint -- "NO static/1.0 without anchor, ever" is this
     task's own binding global constraint). 2+ form-matches -> unresolved (an
     unanchored claim can never disambiguate between same-shaped routes living in
     different services -- guessing one would silently repeat the funnel bug's own
     mistake).

A separate `calls_http_ambiguous` counter (distinguishing the 2+-candidate cases above
from a genuine zero-candidate miss) was considered and DELIBERATELY DROPPED: both
`link()`'s and `link_workspace()`'s return-dict shapes are pinned with exact `==`
equality by a wide swath of pre-existing tests well outside this module's own concern
(cli/reindex/pipeline-report suites) that have nothing to do with this task's own
scenarios -- threading one more key through all of them for a diagnostic nicety neither
the funnel-bug fix nor the M2/M6 gates need would be exactly the kind of scope bloat
this task's own brief explicitly sanctions skipping ("skip with a note if it bloats").
Both cases fold correctly into the pre-existing `calls_http_unresolved` counter --
unchanged shape, same honest "did not silently guess" signal either way.

Route table: every staged Channel node with props.channel_kind == "http_route" AND a
resolved owner_service (i.e. NOT a previous run's own owner="?" fallback channel --
see below) contributes one route, keyed by (owner_service, http_method, path_template)
-- all three are staged verbatim in the Channel's own props by fastapi_ext (module docstring
core/schema.py's make_channel_node: method/template only land in props if the caller passes
them as extra kwargs, which fastapi_ext does).

On a match: CALLS_HTTP(src=claim.src_id, dst=route.channel_id), resolution/confidence
per the anchoring tier above, extractor="linking".

On NO match (ambiguous zero-or-multi-candidate case; ties -- structurally no longer
reachable within a single anchored service now that ambiguity itself is unresolved, but
`_candidates`' own sort stays for determinism regardless): a synthetic Channel(http_route,
owner=None -> id renders "?", unresolved=True prop) is created/reused (id is deterministic
on (verb, path_template) alone, so two unrelated unresolved claims that happen to share
verb+path collapse onto the SAME node via upsert's INSERT OR REPLACE -- not a bug, just the
same "id-determinism is the dedup" pattern used everywhere else in M2) + CALLS_HTTP to it,
resolution="heuristic", confidence 0.5 (deliberately below every resolved tier -- an
unresolved match is a WEAKER claim than even the unanchored heuristic/0.7 tier), and the
claim is counted in calls_http_unresolved.
"""

from __future__ import annotations

from typing import NamedTuple

from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking.env_map import build_env_service_map
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_UNRESOLVED_RESOLUTION = "heuristic"
_UNRESOLVED_CONFIDENCE = 0.5
_ANCHORED_RESOLUTION = "static"
_ANCHORED_CONFIDENCE = 1.0
_UNANCHORED_RESOLUTION = "heuristic"
_UNANCHORED_CONFIDENCE = 0.7


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
    """Segment-wise; wildcard is ROUTE-SIDE ONLY (M7 T3, see module docstring) -- a
    route `{param}` segment matches ANY claim segment, but a claim `{param}` segment
    against a route STATIC segment is no longer a match (this asymmetry is the whole
    fix for the OPEN R1 funnel: a route made of nothing but placeholders can no
    longer "absorb" an unrelated claim's own placeholder tail)."""
    route_segs = route_template.split("/")
    claim_segs = claim_template.split("/")
    if len(route_segs) != len(claim_segs):
        return False
    return all(
        r == c or _is_placeholder(r)
        for r, c in zip(route_segs, claim_segs, strict=True)
    )


def _registry_services(cfg: WorkspaceConfig, base_url_env: str) -> set[str]:
    """Tier 1(a) -- the PRE-EXISTING, PRIMARY env->service source: services whose OWN
    `ServiceConfig.http.base_url_env` equals `base_url_env`. An empty set means "no
    configured service claims this env via the registry" -- `_target` falls back to
    the env_map (1(b)) from there, NOT an immediate tier-2 miss."""
    return {
        svc.name for svc in cfg.services
        if svc.http is not None and svc.http.base_url_env == base_url_env
    }


class _Target(NamedTuple):
    """One claim's resolved target-service anchor -- see module docstring's three
    tiers. `kind`: "anchored" (`allowed` is a non-empty frozenset -- narrow + a
    unique match is static/1.0), "unmapped" (env known, no service found for it --
    unconditionally unresolved with config_ref=env_name, `allowed` unused), or
    "unanchored" (no env at all -- `allowed` is None, meaning "every service", a
    unique match is heuristic/0.7)."""

    kind: str  # "anchored" | "unmapped" | "unanchored"
    allowed: frozenset[str] | None
    env_name: str | None


def _target(
    claim: dict, cfg: WorkspaceConfig, env_service_map: dict[str, str],
) -> _Target:
    env_name = claim.get("base_url_env")
    if env_name is None:
        return _Target("unanchored", None, None)
    registry = _registry_services(cfg, env_name)
    if registry:
        return _Target("anchored", frozenset(registry), env_name)
    mapped = env_service_map.get(env_name)
    if mapped is not None and any(svc.name == mapped for svc in cfg.services):
        return _Target("anchored", frozenset({mapped}), env_name)
    return _Target("unmapped", None, env_name)


def _candidates(
    routes: list[_Route], claim: dict, allowed: frozenset[str] | None,
) -> list[_Route]:
    matches = [
        r for r in routes
        if r.method == claim["verb"]
        and (allowed is None or r.owner_service in allowed)
        and _templates_match(r.template, claim["path_template"])
    ]
    # Deterministic ordering (kept even though ambiguity -- 2+ matches -- is now
    # ALWAYS unresolved, never "pick candidates[0]": a stable order still makes the
    # ambiguous-vs-unique split itself deterministic across runs).
    return sorted(matches, key=lambda r: (r.owner_service, r.method, r.template, r.channel_id))


def _resolved_edge(claim: dict, route: _Route, anchored: bool) -> EdgeRec:
    resolution = _ANCHORED_RESOLUTION if anchored else _UNANCHORED_RESOLUTION
    confidence = _ANCHORED_CONFIDENCE if anchored else _UNANCHORED_CONFIDENCE
    return EdgeRec(
        src=claim["src_id"], dst=route.channel_id, type="CALLS_HTTP",
        resolution=resolution, confidence=confidence, extractor=_EXTRACTOR,
        evidence_file=claim.get("_relpath"), evidence_line=claim.get("evidence_line"),
    )


def _unresolved_channel_and_edge(
    claim: dict, config_ref: str | None = None,
) -> tuple[NodeRec, EdgeRec]:
    extra: dict[str, object] = {"config_ref": config_ref} if config_ref is not None else {}
    chan = make_channel_node(
        "http_route", method=claim["verb"], template=claim["path_template"],
        http_method=claim["verb"], path_template=claim["path_template"], unresolved=True,
        **extra,
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
    # Built ONCE per link() call (S7 runs this once per `codegraph index`) -- see
    # env_map.py's own docstring for the harvest contract.
    env_service_map = build_env_service_map(
        cfg.env_sources, {svc.name for svc in cfg.services},
    )

    edges: list[EdgeRec] = []
    unresolved_channels: dict[str, NodeRec] = {}  # id -> node, dedup within this call
    unresolved = 0

    for claim in claims:
        target = _target(claim, cfg, env_service_map)
        if target.kind == "unmapped":
            # Tier 2: no matching attempted at all -- see module docstring.
            chan, edge = _unresolved_channel_and_edge(claim, config_ref=target.env_name)
            unresolved_channels[chan.id] = chan
            edges.append(edge)
            unresolved += 1
            continue

        candidates = _candidates(routes, claim, target.allowed)
        if len(candidates) == 1:
            edges.append(_resolved_edge(claim, candidates[0], anchored=target.kind == "anchored"))
        else:
            # 0 candidates (no shape/verb match at all) and 2+ candidates (a real
            # ambiguity within the resolved candidate set) both fold into the SAME
            # honest unresolved fallback -- see module docstring's note on why a
            # separate calls_http_ambiguous counter was deliberately dropped.
            chan, edge = _unresolved_channel_and_edge(claim)
            unresolved_channels[chan.id] = chan
            edges.append(edge)
            unresolved += 1

    if unresolved_channels:
        staging.upsert_nodes(list(unresolved_channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {"calls_http": len(edges), "calls_http_unresolved": unresolved}
