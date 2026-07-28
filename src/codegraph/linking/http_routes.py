"""M2 T7: matches staged `http_call` claims (T6's http_client_ext) onto the cross-
service http_route Channel table (T4's fastapi_ext output, staged per-service, unified
here simply by reading across ALL services at once -- no separate dedup step needed,
see workspace.py's module docstring for why a distinct linking/channels.py isn't
warranted).

M7 T3 (OPEN R1 -- docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md): two
binding changes over the M2 design below, both load-bearing enough to warrant their own
account up front.

M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): a THIRD binding change,
additive on top of both of the above -- the "ENV KNOWN, UNMAPPED" tier below splits into
an "external" sub-tier (a real, known hostname outside the workspace -- honest boundary
knowledge, counted via its own `calls_http_external`) and the unchanged plain "unmapped"
sub-tier (env_sources has nothing at all for this env). Unresolved claims still fall
back to a synthetic owner="?" Channel + low-confidence (heuristic/0.5) CALLS_HTTP so no
claim is silently dropped -- the split changes WHICH counter + which extra props a given
miss carries, never the id form or the edge's own resolution/confidence. See "ANCHORING
TIERS" tier 2 below for the full split, and query/traverse.py's own module docstring for
the trace-level payoff (external exit-hops don't drag down a trace's aggregate
confidence the way a genuine modeling gap does).

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
     a service for it. M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3,
     "внешний (api-gateway), честно не заякорен") splits this tier in two, using
     linking/env_map.py's `build_env_hostname_map` -- the SAME env_sources data,
     harvested WITHOUT filtering to workspace-service-matching hostnames (see that
     function's own docstring):
       2a. EXTERNAL -- env_sources HAS a URL-shaped value for this env name (a real,
           known hostname), it just doesn't match any workspace service's own name.
           This is HONEST KNOWLEDGE of a boundary (a real hostname, e.g.
           "api-gateway.prod.svc.cluster.local", genuinely outside this workspace's
           service set) -- not modeling uncertainty, and the pilot's own dominant
           unresolved shape. Still unconditionally unresolved, no matching attempted
           (same reasoning as 2b: a coincidental path-shape match against an
           unrelated MODELED service would be actively wrong): the synthetic Channel
           carries `external=True` + `external_host=<hostname>` ADDITIVELY alongside
           the pre-existing `unresolved=True` + `config_ref=<env name>` -- id form
           UNCHANGED (owner=None -> "?", see `_route_table`'s own owner-is-None skip:
           an external target is still not a real in-workspace route future claims
           should match against). Counted separately, in `calls_http_external`, NOT
           `calls_http_unresolved` -- see `link`'s own docstring. Resolution/
           confidence stay heuristic/0.5 -- IDENTICAL to 2b, "no unearned confidence"
           (this module's binding constraint, unchanged by the split); what changes
           is how a TRACE reads this edge afterward -- see query/traverse.py's own
           updated aggregate-confidence docstring for the compensating mechanism
           (external exit-hops are excluded from the trace-confidence floor, never
           given a higher number here).
       2b. UNMAPPED -- env_sources has NOTHING usable for this env name at all
           (absent entirely, a non-string value, or a string that doesn't parse as a
           URL with a hostname -- see env_map.py's own docstring for the exact
           non-cases). Unchanged from the pre-M9 design: unconditionally unresolved,
           `config_ref=<env name>` only (no `external`/`external_host` props at
           all), counted in `calls_http_unresolved`.
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

SHARED-CHANNEL PROPS MERGE (M9 final review Important-1, .superpowers/sdd/
m9-final-fix-report.md): the id-determinism-is-the-dedup pattern just above means claims
from DIFFERENT tiers -- external (2a), unmapped (2b), AND the zero-or-multi-candidate
ambiguous case -- can all collapse onto the SAME shared id, as long as they share (verb,
path_template) verbatim; nothing about the id form distinguishes WHY a claim was
unresolved. `link()`'s own `unresolved_channels[chan.id] = chan` is last-writer-wins over
`claims_for`'s (service, relpath, payload_json) order -- a plain last-write on the WHOLE
node therefore let one claim's `external`/`external_host` either leak onto a sibling claim
that never resolved a hostname at all (unearned: that sibling's own trace exit would then
be wrongly EXCLUDED from the confidence floor, see query/traverse.py's `is_external_exit`),
or vanish because a later non-external sibling overwrote it (lost: an earlier claim's
genuine external knowledge would render with no trace of why) -- both directions
ORDER-DEPENDENT on an order `claims_for` never promises. `_reconcile_shared_channel_
external_props` (called from `link`, right before the upsert) closes this FAIL-CLOSED:
`external`/`external_host` survive on a shared node ONLY when EVERY claim that ever mapped
onto that id was itself external AND named the SAME host; any non-external sibling or any
hostname disagreement strips both props from the node unconditionally -- see that
function's own docstring for the full mechanism. `calls_http_external`/
`calls_http_unresolved` are counted per-claim, before this reconciliation runs, so neither
counter is affected -- only the shared NODE's own props can degrade. The proper fix --
`external`/`external_host` on the CALLS_HTTP EDGE instead (inherently per-claim, so it can
never collide the way a shared NODE can), with traverse.py reading the edge instead of the
neighbor node -- is out of scope for this fix-batch; tracked for M10.
"""

from __future__ import annotations

import dataclasses
from typing import NamedTuple

from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking.env_map import build_env_hostname_map, build_env_service_map
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
    """One claim's resolved target-service anchor -- see module docstring's tiers.
    `kind`: "anchored" (`allowed` is a non-empty frozenset -- narrow + a unique
    match is static/1.0), "external" (M9 T1 -- env known, env_sources names a REAL
    hostname for it, but that hostname is no workspace service -- `allowed` unused,
    `external_host` carries the hostname text for the synthetic channel's own
    props), "unmapped" (env known, no hostname derivable for it at all --
    unconditionally unresolved with config_ref=env_name, `allowed`/`external_host`
    both unused), or "unanchored" (no env at all -- `allowed` is None, meaning
    "every service", a unique match is heuristic/0.7)."""

    kind: str  # "anchored" | "external" | "unmapped" | "unanchored"
    allowed: frozenset[str] | None
    env_name: str | None
    external_host: str | None = None


def _target(
    claim: dict, cfg: WorkspaceConfig, env_service_map: dict[str, str],
    env_hostname_map: dict[str, str],
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
    # M9 T1: env_service_map (built from the SAME env_sources data, see link()
    # below) already failed to name a service for env_name above -- if
    # env_hostname_map (the UNFILTERED raw harvest) still has a hostname for it,
    # that hostname's own first DNS label structurally can't match a workspace
    # service (build_env_service_map would have found it too, by construction) --
    # so reaching here with a hit means "external", never a redundant re-check.
    hostname = env_hostname_map.get(env_name)
    if hostname is not None:
        return _Target("external", None, env_name, hostname)
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
    claim: dict, config_ref: str | None = None, external_host: str | None = None,
) -> tuple[NodeRec, EdgeRec]:
    """Shared by BOTH the plain-unmapped (tier 2b) and external (tier 2a, M9 T1)
    fallbacks, and the zero/multi-candidate ambiguous case below -- all four honest
    misses need the IDENTICAL id form / resolution / confidence / extractor /
    evidence shape, differing only in which extra props ride along. `external_host`
    (only ever passed together with `config_ref=<the SAME env name>`, from tier 2a)
    additively sets `external=True` + `external_host=<hostname>` on the synthetic
    channel -- see module docstring's tier 2a for the full reasoning; id form,
    resolution, confidence are UNCHANGED regardless of whether it's passed."""
    extra: dict[str, object] = {"config_ref": config_ref} if config_ref is not None else {}
    if external_host is not None:
        extra["external"] = True
        extra["external_host"] = external_host
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


def _reconcile_shared_channel_external_props(
    unresolved_channels: dict[str, NodeRec], host_markers: dict[str, set[str | None]],
) -> dict[str, NodeRec]:
    """M9 final review Important-1 (.superpowers/sdd/m9-final-fix-report.md) -- the
    FAIL-CLOSED merge rule for a shared unresolved-channel id, applied once, right
    before `link`'s own upsert (see module docstring's "SHARED-CHANNEL PROPS MERGE"
    section for the full motivation).

    `host_markers[chan_id]` carries one marker PER CLAIM that ever mapped onto
    `chan_id` (via `link`'s own per-branch `.setdefault(chan.id, set()).add(...)`):
    a hostname string for an external (tier 2a) claim, `None` for an unmapped
    (2b) or zero-or-multi-candidate ambiguous claim -- both of the latter contribute
    the SAME `None` marker on purpose, they are equally "not external" for this
    rule's own purposes. `external`/`external_host` survive on `chan_id`'s node
    ONLY when that set is a single-element, all-string set (every contributing
    claim was external, and every one of them named the identical host) -- NEVER a
    majority vote, never "keep whichever claim wrote last anyway": one dissenting
    `None`, or two DIFFERENT hostnames, strips both props from the node
    unconditionally. Every OTHER prop (`unresolved`, `config_ref`, `channel_kind`,
    `http_method`, `path_template`) is left exactly as `link`'s own last-write
    already set it -- this function touches ONLY the two props this task's review
    scoped; `config_ref` in particular can still legitimately show whichever
    contributing claim's env name happened to write last, unchanged from before
    this fix (out of scope here, see the module docstring's own M10 note for the
    real per-edge fix that would also resolve this).

    `NodeRec` is frozen (core/schema.py) -- a disqualified node is rebuilt via
    `dataclasses.replace` with a fresh `props` dict rather than mutated in place.
    A node whose props never carried either key to begin with (the common,
    no-collision case: an unmapped or ambiguous claim's OWN channel, or a single
    external claim with no colliding sibling) is left as the SAME object,
    untouched -- no allocation, no upsert-order change, for the overwhelming
    majority of claims that never collide at all."""
    for chan_id, node in unresolved_channels.items():
        markers = host_markers.get(chan_id, set())
        all_external_same_host = None not in markers and len(markers) == 1
        if all_external_same_host:
            continue  # every contributing claim agrees -- node already correct
        if "external" not in node.props and "external_host" not in node.props:
            continue  # nothing to strip -- avoid a needless allocation
        stripped_props = {
            k: v for k, v in node.props.items() if k not in ("external", "external_host")
        }
        unresolved_channels[chan_id] = dataclasses.replace(node, props=stripped_props)
    return unresolved_channels


def link(cfg: WorkspaceConfig, staging: Staging) -> dict:
    routes = _route_table(staging)
    claims = staging.claims_for("http_call")
    # Built ONCE EACH per link() call (S7 runs this once per `codegraph index`) --
    # see env_map.py's own docstring for the harvest contract of both. M9 T1:
    # env_hostname_map is the ADDITIVE raw harvest (unfiltered by service_names)
    # tier 2a needs to tell "external" (a real, known hostname) apart from tier 2b
    # "unmapped" (env_sources has nothing at all) -- see module docstring.
    env_service_map = build_env_service_map(
        cfg.env_sources, {svc.name for svc in cfg.services},
    )
    env_hostname_map = build_env_hostname_map(cfg.env_sources)

    edges: list[EdgeRec] = []
    unresolved_channels: dict[str, NodeRec] = {}  # id -> node, dedup within this call
    # M9 final review Important-1: one marker PER CONTRIBUTING CLAIM (never per
    # node) for a given shared id -- a hostname string for an external claim,
    # `None` for an unmapped/ambiguous one -- consumed by
    # _reconcile_shared_channel_external_props below (see that function's own
    # docstring for the fail-closed merge rule this drives, and the module
    # docstring's "SHARED-CHANNEL PROPS MERGE" section for the full motivation).
    channel_host_markers: dict[str, set[str | None]] = {}
    unresolved = 0
    external = 0

    for claim in claims:
        target = _target(claim, cfg, env_service_map, env_hostname_map)
        if target.kind == "external":
            # Tier 2a (M9 T1): a real, known hostname outside the workspace -- no
            # matching attempted (same reasoning as 2b), but the channel additively
            # names WHY it's unresolved -- see module docstring.
            chan, edge = _unresolved_channel_and_edge(
                claim, config_ref=target.env_name, external_host=target.external_host,
            )
            unresolved_channels[chan.id] = chan
            channel_host_markers.setdefault(chan.id, set()).add(target.external_host)
            edges.append(edge)
            external += 1
            continue
        if target.kind == "unmapped":
            # Tier 2b: no matching attempted at all -- see module docstring.
            chan, edge = _unresolved_channel_and_edge(claim, config_ref=target.env_name)
            unresolved_channels[chan.id] = chan
            channel_host_markers.setdefault(chan.id, set()).add(None)
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
            channel_host_markers.setdefault(chan.id, set()).add(None)
            edges.append(edge)
            unresolved += 1

    if unresolved_channels:
        unresolved_channels = _reconcile_shared_channel_external_props(
            unresolved_channels, channel_host_markers,
        )
        staging.upsert_nodes(list(unresolved_channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {
        "calls_http": len(edges),
        "calls_http_unresolved": unresolved,
        # M9 T1: tier 2a's own separate counter -- NEVER folded into
        # calls_http_unresolved (see module docstring's tier 2a paragraph for why
        # these two honest-miss shapes must not be conflated).
        "calls_http_external": external,
    }
