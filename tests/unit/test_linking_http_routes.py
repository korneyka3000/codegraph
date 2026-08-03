"""M2 T7 / M7 T3: linking.http_routes.link -- matches staged http_call claims (T6) onto
the cross-service http_route Channel table (T4's fastapi_ext output), emitting
CALLS_HTTP; unresolved claims fall back to a synthetic owner="?" Channel + low-
confidence CALLS_HTTP so no claim is silently dropped.

M7 T3 (OPEN R1 -- docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md): two
binding changes over the M2 design, both exercised below.

STRICT FORM: the `{param}` wildcard is now ROUTE-SIDE ONLY. A route's own `{param}`
segment still matches any claim segment (unchanged), but a claim's `{param}` segment
against a route's STATIC segment is no longer a match -- this is precisely the
mechanism that let three unrelated real client paths funnel onto an unrelated
service's all-placeholder route in the pilot (see the funnel tests below, pinning the
exact three path pairs from the report's own table).

ANCHORING TIERS: confidence is now a function of target-service ANCHORING + match
UNIQUENESS alone, never of the claim's own `resolution_hint` (how the path TEXT was
built) -- "NO static/1.0 without anchor, ever" is the task's own binding rule.
  1. ANCHORED (base_url_env set, explicitly or auto-anchored, AND resolves to a real
     workspace service -- via the pre-existing ServiceConfig.http.base_url_env
     registry first, the env_sources-derived env_map as an additive fallback second):
     routes narrowed to that ONE service; a UNIQUE form-match -> static/1.0. 2+
     matches within that one service -> unresolved (a real config ambiguity).
  2. ENV KNOWN, UNMAPPED (base_url_env set but neither source names a service):
     unconditionally unresolved, no matching attempted -- the CALLS_HTTP edge
     (M10 T4 -- per-claim, not the shared synthetic channel any more) carries
     `config_ref=<env name>` for doctor/graph-inspection visibility.
  3. UNANCHORED (no base_url_env at all): matched across every service; a UNIQUE
     form-match -> heuristic/0.7 (never static, regardless of resolution_hint). 2+
     matches -> unresolved.

A separate `calls_http_ambiguous` counter for the 2+-candidate cases was considered and
deliberately dropped -- both `link()`'s and `link_workspace()`'s return-dict shapes are
pinned with exact `==` equality by a wide swath of pre-existing tests well outside this
module (cli/reindex/pipeline-report suites); folding ambiguous cases into the existing
`calls_http_unresolved` counter (same honest "did not silently guess" signal, unchanged
shape) avoids that ripple for a diagnostic nicety neither the funnel-bug fix nor the
M2/M6 gates need.
"""

from __future__ import annotations

import pytest

from codegraph.config.models import HttpExposure, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import make_channel_node
from codegraph.linking import http_routes
from codegraph.query import traverse
from codegraph.stores.staging import Staging


def _cfg(*services: ServiceConfig, env_sources: list | None = None) -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="g", services=list(services), env_sources=env_sources or [],
    )


def _svc(name: str, base_url_env: str | None = None) -> ServiceConfig:
    http = HttpExposure(base_url_env=base_url_env) if base_url_env else None
    return ServiceConfig(name=name, path=__file__, http=http)  # path unused by linking


def _route_channel(owner: str, method: str, template: str) -> object:
    return make_channel_node(
        "http_route", owner_service=owner, method=method, template=template,
        http_method=method, path_template=template,
    )


def _claim(staging: Staging, service: str, relpath: str, src_id: str, verb: str,
           path_template: str, base_url_env: str | None = None,
           resolution_hint: str = "static", evidence_line: int = 1) -> None:
    staging.add_claims(service, relpath, "http_call", [{
        "src_id": src_id, "verb": verb, "path_template": path_template,
        "base_url_env": base_url_env, "resolution_hint": resolution_hint,
        "evidence_line": evidence_line,
    }])


# -- resolved matches: anchored (tier 1) --


def test_static_claim_matches_exact_route_conf_1_0(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("document-management", "GET", "/documents/{doc_id}")
    st.upsert_nodes([chan])
    _claim(st, "kyc-worker", "app/clients/document_management_client.py",
           "sym:kyc-worker:client.get_document", "GET", "/documents/{doc_id}",
           base_url_env="DOCUMENT_MANAGEMENT_URL", resolution_hint="static")

    stats = http_routes.link(_cfg(_svc("document-management", "DOCUMENT_MANAGEMENT_URL")), st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    e = edges[0]
    assert e.src == "sym:kyc-worker:client.get_document"
    assert e.dst == chan.id
    assert e.resolution == "static"
    assert e.confidence == 1.0
    assert e.extractor == "linking"
    assert e.evidence_file == "app/clients/document_management_client.py"
    assert e.evidence_line == 1
    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 0}


# -- segment matching: wildcard is ROUTE-SIDE ONLY (M7 T3) --


def test_route_placeholder_matches_client_literal_segment(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/{order_id}")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/42")

    http_routes.link(_cfg(_svc("svc")), st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id


def test_claim_placeholder_against_route_static_segment_is_rejected(tmp_path):
    """The core strict-form rule in miniature: a route's STATIC segment no longer
    accepts a claim-side `{param}` wildcard. INVERTS the pre-M7 behavior this exact
    scenario used to pin (a claim `{order_id}` placeholder against a route literal
    "42" used to match under the old bidirectional rule) -- that old direction is
    exactly the mechanism the OPEN R1 funnel bug exploited."""
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/42")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/{order_id}")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats["calls_http_unresolved"] == 1


def test_differing_static_segment_does_not_match(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/list")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/detail")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats["calls_http_unresolved"] == 1


def test_differing_segment_count_does_not_match(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/{order_id}")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/{order_id}/items")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats["calls_http_unresolved"] == 1


def test_verb_mismatch_does_not_match(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "POST", "/x")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats["calls_http_unresolved"] == 1


# -- THE pilot regression (OPEN R1): funnel must yield ZERO candidates ------------
#
# Real client paths (verified correct, per the report's own table) vs the unrelated
# all-placeholder route they used to funnel onto -- every leading segment matches via
# the route's OWN placeholders (legitimate, unchanged), but the route's static
# "parsed-data" TAIL now requires the claim's own tail to be that literal text, which
# it never is (it's always the claim's OWN trailing `{param}`).

FUNNEL_ROUTE = "/{initiator_ref}/{vendor}/{vendor_version}/parsed-data"

FUNNEL_CLAIM_PATHS = [
    "/api/v1/steps/{step_uid}",
    "/api/v1/requests/{verification_uid}",
    "/api/v1/customer-info/{customer_uid}",
]


@pytest.mark.parametrize(
    "claim_path", FUNNEL_CLAIM_PATHS, ids=["steps", "requests", "customer-info"],
)
def test_open_r1_pilot_funnel_pinned_zero_candidates(tmp_path, claim_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("dm", "GET", FUNNEL_ROUTE)
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", claim_path)

    stats = http_routes.link(_cfg(_svc("dm")), st)

    assert stats["calls_http_unresolved"] == 1
    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    assert edges[0].resolution == "heuristic" and edges[0].confidence == 0.5
    # the funnel route survives untouched as its OWN node; the claim gets a
    # DISTINCT synthetic unresolved channel, never collapsed onto the funnel route.
    chans = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert chan.id in chans
    assert edges[0].dst != chan.id


def test_route_side_wildcard_still_matches_when_claim_tail_is_literal(tmp_path):
    """Same all-placeholder-ish route as the funnel above -- proves the fix is
    surgical: a claim whose OWN tail genuinely IS the literal "parsed-data" still
    matches (route placeholders legitimately wildcard the leading segments)."""
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("dm", "GET", FUNNEL_ROUTE)
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/init123/acme/v2/parsed-data")

    http_routes.link(_cfg(_svc("dm")), st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id


# -- base_url_env candidate narrowing / anchoring tiers (M7 T3) --


def test_anchored_unique_match_is_static_1_0_only_that_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    wrong = _route_channel("service-a", "GET", "/x")
    right = _route_channel("service-b", "GET", "/x")
    st.upsert_nodes([wrong, right])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="B_URL")

    cfg = _cfg(_svc("service-a", "A_URL"), _svc("service-b", "B_URL"))
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    assert edges[0].dst == right.id
    assert edges[0].resolution == "static" and edges[0].confidence == 1.0


def test_anchored_multiple_matches_in_same_service_is_unresolved(tmp_path):
    """Two DIFFERENT routes, same anchored owner service, BOTH form-match the claim
    -- a real config ambiguity. Picking one silently would repeat the funnel bug's
    own mistake at a smaller scale -- must fall back to unresolved, not
    `candidates[0]`."""
    st = Staging(tmp_path / "s.db")
    r1 = _route_channel("svc", "GET", "/x/{a}")
    r2 = _route_channel("svc", "GET", "/{b}/y")
    st.upsert_nodes([r1, r2])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x/y", base_url_env="X_URL")

    stats = http_routes.link(_cfg(_svc("svc", "X_URL")), st)

    assert stats["calls_http_unresolved"] == 1
    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    assert edges[0].resolution == "heuristic" and edges[0].confidence == 0.5


def test_base_url_env_unmapped_is_unresolved_with_config_ref(tmp_path):
    """Tier 2: base_url_env is set, but NEITHER the ServiceConfig.http.base_url_env
    registry NOR the (empty, here) env_sources-derived env_map can name a service
    for it -- unconditionally unresolved (no matching attempted at all: a
    coincidental path-shape match against an unrelated modeled service would be
    actively wrong, not merely uncertain). M10 T4: config_ref=<env name> rides the
    CALLS_HTTP EDGE (per-claim), not the shared synthetic channel -- see
    linking/http_routes.py's own module docstring for why."""
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="OTHER_URL")

    cfg = _cfg(_svc("service-a", "A_URL"))
    stats = http_routes.link(cfg, st)
    assert stats == {"calls_http": 1, "calls_http_unresolved": 1, "calls_http_external": 0}

    unresolved_chan = next(n for n in st.iter_nodes() if n.kind == "Channel" and n.id != chan.id)
    assert "config_ref" not in unresolved_chan.props  # M10 T4: never a node prop any more
    assert "external" not in unresolved_chan.props
    assert "external_host" not in unresolved_chan.props
    edge = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert edge.dst == unresolved_chan.id
    assert edge.props.get("config_ref") == "OTHER_URL"
    # M9 T1: "OTHER_URL" isn't in env_sources at all (empty here) -- NOT the same as
    # a hostname that fails to match a workspace service (that's the "external" tier
    # below) -- no external prop at all on this genuinely-unmapped claim's edge.
    assert "external" not in edge.props
    assert "external_host" not in edge.props


def test_claim_without_base_url_env_considers_all_services(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env=None)

    cfg = _cfg(_svc("service-a", "A_URL"))
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id
    assert edges[0].resolution == "heuristic" and edges[0].confidence == 0.7


def test_unanchored_unique_match_is_heuristic_0_7_never_static(tmp_path):
    """Tier 3: no base_url_env at all -- matched across every service, a UNIQUE
    form-match is heuristic/0.7 REGARDLESS of the claim's own resolution_hint (M7
    T3's binding rule: confidence is a function of anchoring + uniqueness only,
    never of how the path text itself was built). resolution_hint="static" here on
    purpose, to prove the TIER -- not the hint -- now drives the outcome (this
    exact combination is how the funnel bug's false edges got minted at static/1.0
    in the first place)."""
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "POST", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "POST", "/x", resolution_hint="static")

    http_routes.link(_cfg(_svc("svc")), st)

    e = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert e.resolution == "heuristic"
    assert e.confidence == 0.7


def test_unanchored_two_candidates_across_services_is_unresolved(tmp_path):
    """No base_url_env, and the SAME verb+form matches routes in TWO DIFFERENT
    services -- an unanchored claim can never disambiguate between them."""
    st = Staging(tmp_path / "s.db")
    a = _route_channel("service-a", "GET", "/x")
    b = _route_channel("service-b", "GET", "/x")
    st.upsert_nodes([a, b])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x")

    stats = http_routes.link(_cfg(_svc("service-a"), _svc("service-b")), st)
    assert stats["calls_http_unresolved"] == 1


# -- env_map fallback (tier 1(b)): env_sources-derived hostname->service map, used
# ONLY when the pre-existing ServiceConfig.http.base_url_env registry (tier 1(a))
# finds no owner for the claim's env -- this is what keeps every M2/M6 fixture claim
# byte-identical (their env already resolves via the registry, never reaching here).


def test_env_map_fallback_anchors_when_registry_does_not_know_the_env(tmp_path):
    helm = tmp_path / "values.yaml"
    helm.write_text(
        'SERVICE_VERIFICATION_REQUESTS_URL: '
        '"http://verification-requests.kyc.svc.cluster.local:8000"\n'
    )
    st = Staging(tmp_path / "s.db")
    right = _route_channel("verification-requests", "GET", "/api/v1/steps/{step_uid}")
    wrong = _route_channel("other-service", "GET", "/api/v1/steps/{step_uid}")
    st.upsert_nodes([right, wrong])
    _claim(
        st, "caller", "a.py", "sym:caller:f", "GET", "/api/v1/steps/{step_uid}",
        base_url_env="SERVICE_VERIFICATION_REQUESTS_URL",
    )

    cfg = _cfg(_svc("verification-requests"), _svc("other-service"), env_sources=[helm])
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    assert edges[0].dst == right.id
    assert edges[0].resolution == "static" and edges[0].confidence == 1.0


def test_registry_source_takes_priority_over_env_map_when_both_could_resolve(tmp_path):
    """Tier 1(a) (the pre-existing ServiceConfig.http.base_url_env registry) is
    tried FIRST; env_map (1(b)) is an ADDITIVE fallback only consulted when the
    registry finds NO owner at all."""
    helm = tmp_path / "values.yaml"
    helm.write_text('X_URL: "http://decoy.ns.svc.cluster.local"\n')  # would map to "decoy"
    st = Staging(tmp_path / "s.db")
    real = _route_channel("service-b", "GET", "/x")
    decoy = _route_channel("decoy", "GET", "/x")
    st.upsert_nodes([real, decoy])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="X_URL")

    cfg = _cfg(_svc("service-b", "X_URL"), _svc("decoy"), env_sources=[helm])
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == real.id


def test_malformed_env_sources_file_does_not_crash_link(tmp_path):
    """M7 T3 review Important-1 (link-level pin, complements test_env_map.py's own
    build-level one): env_map.build_env_service_map is called INSIDE link() -- i.e.
    deep in S7, after every service's expensive analyze already ran -- so a malformed
    env_sources file (the realistic shape: an UNRENDERED helm template, invalid YAML)
    crashing there would lose the whole index run at its last stage. It must instead
    be skipped (warn-and-continue inside env_map.py) with link() completing normally;
    anchoring that never needed the env_map (tier 1(a) registry, here) stays
    byte-identical."""
    bad = tmp_path / "bad-values.yaml"
    bad.write_text("SERVICE_X_URL: {{ .Values.host }}\n")
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-b", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="B_URL")

    cfg = _cfg(_svc("service-b", "B_URL"), env_sources=[bad])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 0}
    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id
    assert edges[0].resolution == "static" and edges[0].confidence == 1.0


def test_env_map_hostname_label_not_a_workspace_service_falls_to_unmapped(tmp_path):
    """PRE-M9 T1 this was tier 2 (unmapped); M9 T1 splits it into the NEW
    "external" tier instead (see test_env_map_hostname_not_a_workspace_service_is_
    external below, and the module docstring's updated tier list) -- a hostname
    env_map DOES know, that simply names no workspace service, is now honest
    knowledge of a boundary, not an unmodeled miss. This test's name/docstring is
    kept (not deleted) specifically to make that inversion visible in history."""
    helm = tmp_path / "values.yaml"
    helm.write_text('SOME_URL: "http://not-a-real-service.ns.svc.cluster.local"\n')
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="SOME_URL")

    cfg = _cfg(_svc("service-a"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 1}
    unresolved_chan = next(n for n in st.iter_nodes() if n.kind == "Channel" and n.id != chan.id)
    assert "config_ref" not in unresolved_chan.props  # M10 T4: moved to the edge
    edge = next(
        e for e in st.iter_edges() if e.type == "CALLS_HTTP" and e.dst == unresolved_chan.id
    )
    assert edge.props.get("config_ref") == "SOME_URL"


# -- M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): tier-2 split --
# env known AND env_sources HAS a hostname for it, but that hostname names no
# workspace service -- "external" (honest boundary knowledge), counted separately
# from the plain "unmapped" tier (env_sources has NOTHING usable for the env at all).
# Channel id form, resolution, and confidence are ALL unchanged from plain unmapped
# -- only props (`external`, `external_host`) and the counter differ.


def test_env_map_hostname_not_a_workspace_service_is_external_tier(tmp_path):
    """M10 T4: `external`/`external_host`/`config_ref` all ride the CALLS_HTTP EDGE
    now (EdgeRec.props -- kafka's own `_props_for`/config_ref-on-edge convention is
    the precedent, see linking/http_routes.py's module docstring) -- the channel
    node itself stays a clean, claim-agnostic `?`-owner unresolved node, carrying
    only what's fully determined by (verb, path_template) alone."""
    helm = tmp_path / "values.yaml"
    helm.write_text('SOME_URL: "http://api-gateway.prod.svc.cluster.local"\n')
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="SOME_URL")

    cfg = _cfg(_svc("service-a"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 1}
    ext_chan = next(n for n in st.iter_nodes() if n.kind == "Channel" and n.id != chan.id)
    assert ext_chan.id == "chan:http:?:GET /x"  # id form UNCHANGED (owner=None -> "?")
    assert ext_chan.props.get("unresolved") is True
    # nothing claim-specific lives on the node any more -- the whole point of the fix.
    assert "config_ref" not in ext_chan.props
    assert "external" not in ext_chan.props
    assert "external_host" not in ext_chan.props

    edge = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert edge.dst == ext_chan.id
    # "no unearned confidence" -- IDENTICAL to the plain unmapped tier, M9 T1's
    # binding constraint (the trace-level payoff is in query/traverse.py instead).
    assert edge.resolution == "heuristic" and edge.confidence == 0.5
    assert edge.props.get("config_ref") == "SOME_URL"
    assert edge.props.get("external") is True
    assert edge.props.get("external_host") == "api-gateway.prod.svc.cluster.local"


def test_env_known_value_not_url_shaped_is_unmapped_not_external(tmp_path):
    """M9 T1 review Minor pin (link()-level -- complements test_env_map.py's own
    build-level non-URL cases): env_sources HAS an entry for the claim's env name,
    but its value doesn't parse as a URL with a hostname (plain text here; the
    scheme-less host:port quirk is the same non-case, see env_map.py) -- there is
    no REAL hostname to name a boundary with, so this must land in the generic
    UNMAPPED bucket (calls_http_unresolved), NEVER the external tier."""
    helm = tmp_path / "values.yaml"
    helm.write_text("SOME_URL: just-a-plain-string\n")
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="SOME_URL")

    cfg = _cfg(_svc("service-a"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 1, "calls_http_external": 0}
    chan = next(n for n in st.iter_nodes() if n.kind == "Channel")
    assert "config_ref" not in chan.props  # M10 T4: moved to the edge
    assert "external" not in chan.props
    assert "external_host" not in chan.props
    edge = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert edge.props.get("config_ref") == "SOME_URL"
    assert "external" not in edge.props
    assert "external_host" not in edge.props


def test_anchored_via_env_map_wins_over_external_even_though_hostname_also_present(
    tmp_path,
):
    """Tier ordering pin: a hostname that DOES match a workspace service resolves
    anchored (tier 1b) and never reaches the external tier at all -- even though
    build_env_hostname_map's own raw harvest technically "has" this exact hostname
    too (build_env_service_map -- tried first -- already claims it)."""
    helm = tmp_path / "values.yaml"
    helm.write_text('SERVICE_WORKER_URL: "http://worker.kyc.svc.cluster.local:9000"\n')
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("worker", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="SERVICE_WORKER_URL")

    cfg = _cfg(_svc("worker"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 0}
    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert edges[0].dst == chan.id and edges[0].resolution == "static"


def test_calls_http_total_counts_resolved_unresolved_and_external(tmp_path):
    helm = tmp_path / "values.yaml"
    helm.write_text('GATEWAY_URL: "http://api-gateway.prod.svc.cluster.local"\n')
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/ok")
    st.upsert_nodes([chan])
    _claim(st, "a", "a.py", "sym:a:f", "GET", "/ok")
    _claim(st, "b", "b.py", "sym:b:g", "GET", "/missing")
    _claim(st, "c", "c.py", "sym:c:h", "GET", "/gw", base_url_env="GATEWAY_URL")

    stats = http_routes.link(_cfg(_svc("svc"), env_sources=[helm]), st)
    assert stats == {"calls_http": 3, "calls_http_unresolved": 1, "calls_http_external": 1}


def test_malformed_env_sources_file_does_not_crash_link_and_external_claim_still_resolves(
    tmp_path,
):
    """Same crash-safety pin as test_malformed_env_sources_file_does_not_crash_link
    above, exercising the NEW build_env_hostname_map read too (M9 T1 -- link() now
    calls both env_map builders once each): a malformed file must not prevent an
    unrelated, otherwise-external claim from resolving correctly."""
    bad = tmp_path / "bad-values.yaml"
    bad.write_text("SERVICE_X_URL: {{ .Values.host }}\n")
    good = tmp_path / "good-values.yaml"
    good.write_text('GATEWAY_URL: "http://api-gateway.prod.svc.cluster.local"\n')
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="GATEWAY_URL")

    cfg = _cfg(env_sources=[bad, good])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 0, "calls_http_external": 1}


# -- unresolved fallback --


def test_unresolved_creates_owner_unknown_channel_and_low_confidence_edge(tmp_path):
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/nowhere")

    stats = http_routes.link(_cfg(), st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 1, "calls_http_external": 0}
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1
    chan = chans[0]
    assert chan.id == "chan:http:?:GET /nowhere"
    assert chan.props["unresolved"] is True
    assert "owner_service" not in chan.props
    assert "config_ref" not in chan.props

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1
    assert edges[0].dst == chan.id
    assert edges[0].resolution == "heuristic"
    assert edges[0].confidence == 0.5


def test_unresolved_channel_id_is_deterministic_and_deduplicates_across_claims(tmp_path):
    """Two distinct claims that both fail to resolve to the SAME (verb, path) collapse
    onto the SAME synthetic Channel node (deterministic id + upsert REPLACE), not two."""
    st = Staging(tmp_path / "s.db")
    _claim(st, "a", "a.py", "sym:a:f", "GET", "/nowhere")
    _claim(st, "b", "b.py", "sym:b:g", "GET", "/nowhere")

    http_routes.link(_cfg(), st)

    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1


# -- no-op / totals --


def test_no_claims_returns_zero_counts_and_writes_nothing(tmp_path):
    st = Staging(tmp_path / "s.db")
    stats = http_routes.link(_cfg(), st)
    assert stats == {"calls_http": 0, "calls_http_unresolved": 0, "calls_http_external": 0}
    assert list(st.iter_nodes()) == []
    assert list(st.iter_edges()) == []


def test_calls_http_total_counts_both_resolved_and_unresolved(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/ok")
    st.upsert_nodes([chan])
    _claim(st, "a", "a.py", "sym:a:f", "GET", "/ok")
    _claim(st, "b", "b.py", "sym:b:g", "GET", "/missing")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats == {"calls_http": 2, "calls_http_unresolved": 1, "calls_http_external": 0}


# -- M10 T4 (per-edge external props -- the PROPER fix replacing the M9 final
# review's fail-closed merge palliative, `_reconcile_shared_channel_external_
# props`, now DELETED): two+ claims that share (verb, path_template) still
# collapse onto ONE synthetic channel id (see linking/http_routes.py's own "On
# NO match" docstring paragraph), even when they come from DIFFERENT tiers
# (external/unmapped/ambiguous) -- but `external`/`external_host`/`config_ref`
# no longer live on that shared NODE at all (EdgeRec.props instead, kafka's own
# `_props_for` convention -- see the module docstring's updated "SHARED-CHANNEL"
# section). Each claim's CALLS_HTTP edge carries its OWN props independently, so
# there is nothing left for two colliding claims to clobber or disagree about --
# the collision class the M9 palliative had to fail-closed-merge around is GONE,
# not merely handled more carefully. These four scenarios (a/b/c/d) are kept,
# renamed/rewritten, specifically to make that disappearance visible in history:
# same setups the M9 final review used to prove the merge rule correct, now
# proving there is no merge rule left to need. --

_COLLISION_VERB = "POST"
_COLLISION_TEMPLATE = "/events"
_GW_HOST = "api-gateway.prod.svc.cluster.local"


class _MiniTraceStore:
    """Minimal get_nodes/neighbors duck-type -- query.traverse's own store
    contract -- ported in miniature from test_traverse.py's own FakeStore (this
    codebase's established self-contained-test-module convention; see e.g.
    tests/eval/test_m9_gate.py's own docstring on porting, not importing, test
    helpers across modules). Scoped to exactly what the pins below need: one
    entry node, ONE CALLS_HTTP edge into a REAL http_routes.link()-produced
    channel (props/edge taken from actual staging output below, never
    fabricated)."""

    def __init__(self, entry_id: str, channel: dict, edge_props: dict) -> None:
        self._entry_id = entry_id
        self._channel = channel
        self._edge_props = edge_props

    def get_nodes(self, ids):
        nodes = {
            self._entry_id: {"id": self._entry_id, "service": "caller"},
            self._channel["id"]: self._channel,
        }
        return [nodes[i] for i in ids if i in nodes]

    def neighbors(self, node_id, edge_types, direction, limit):
        if node_id != self._entry_id or direction not in ("out", "both"):
            return []
        if edge_types and "CALLS_HTTP" not in edge_types:
            return []
        return [("CALLS_HTTP", dict(self._edge_props), self._channel, "out")]


def _only_channel(st: Staging) -> object:
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1, f"expected exactly one shared channel, got {chans}"
    return chans[0]


def _flat_edge_props(edge) -> dict:
    """Mirrors pipeline/load.py's `_edge_props` flattening (core fields + e.props
    merged into one dict) -- the shape `neighbors()` actually hands back off a
    real store (see stores/falkordb/store.py's `_one_way`: `e.properties`)."""
    return {**edge.props, "confidence": edge.confidence, "resolution": edge.resolution}


@pytest.mark.parametrize(
    "order", ["external-then-unmapped", "unmapped-then-external"],
)
def test_collision_external_and_unmapped_edges_keep_independent_props(tmp_path, order):
    """(a)/(b): an external (tier 2a) and an unmapped (tier 2b) claim share the
    SAME (verb, path_template) -> SAME synthetic channel id, in BOTH claims_for
    orders. The shared NODE carries NEITHER external/external_host/config_ref
    (nothing claim-specific lives there any more, regardless of order -- no
    last-writer-wins to be order-dependent about), while EACH EDGE independently
    keeps its own claim's props. Trace payoff (the fix's whole point): the
    external claim's OWN edge is now correctly EXCLUDED from a trace's confidence
    floor -- the M9 palliative had to give this up (fail-closed: strip on ANY
    sibling disagreement, since the shared NODE alone could never tell which hop
    was the external one); per-edge, there's no ambiguity to be conservative
    about any more. The unmapped sibling's own edge is symmetric proof: it does
    NOT wrongly inherit an exclusion it never earned."""
    helm = tmp_path / "values.yaml"
    helm.write_text(f'GW_URL: "http://{_GW_HOST}"\n')
    st = Staging(tmp_path / "s.db")
    ext_src, unm_src = "sym:caller:ext", "sym:caller:unm"
    ext_relpath, unm_relpath = (
        ("a_first.py", "b_second.py") if order == "external-then-unmapped"
        else ("b_second.py", "a_first.py")
    )
    _claim(st, "caller", ext_relpath, ext_src, _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="GW_URL")
    _claim(st, "caller", unm_relpath, unm_src, _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="MYSTERY_URL")

    cfg = _cfg(_svc("svc"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 2, "calls_http_unresolved": 1, "calls_http_external": 1}
    chan = _only_channel(st)
    assert "external" not in chan.props
    assert "external_host" not in chan.props
    assert "config_ref" not in chan.props  # never a node prop any more, either tier

    ext_edge = next(e for e in st.iter_edges() if e.src == ext_src)
    assert ext_edge.dst == chan.id
    assert ext_edge.resolution == "heuristic" and ext_edge.confidence == 0.5  # honest, unchanged
    assert ext_edge.props.get("external") is True
    assert ext_edge.props.get("external_host") == _GW_HOST
    assert ext_edge.props.get("config_ref") == "GW_URL"

    unm_edge = next(e for e in st.iter_edges() if e.src == unm_src)
    assert unm_edge.dst == chan.id
    assert unm_edge.props.get("config_ref") == "MYSTERY_URL"
    assert "external" not in unm_edge.props
    assert "external_host" not in unm_edge.props

    ext_store = _MiniTraceStore(ext_src, {"id": chan.id, **chan.props}, _flat_edge_props(ext_edge))
    ext_result = traverse.trace_process(ext_store, ext_src, max_segments=12, min_confidence=0.3)
    # Excluded: the "no edges to doubt" trivial default (1.0), not the edge's own
    # honest 0.5 -- proves the external claim's OWN edge gets its deserved
    # exclusion now, order-independent.
    assert ext_result["confidence"] == 1.0
    assert ext_result["external_exit_count"] == 1

    unm_store = _MiniTraceStore(unm_src, {"id": chan.id, **chan.props}, _flat_edge_props(unm_edge))
    unm_result = traverse.trace_process(unm_store, unm_src, max_segments=12, min_confidence=0.3)
    # NOT excluded: the unmapped sibling never earned it -- would wrongly read 1.0
    # had it leaked an exclusion from the external sibling sharing this node.
    assert unm_result["confidence"] == 0.5
    assert unm_result["external_exit_count"] == 0


def test_collision_two_external_edges_keep_their_own_distinct_hosts(tmp_path):
    """(c): two external (tier 2a) claims share (verb, path_template) but resolve
    to DIFFERENT real hostnames. Under the OLD node-prop design this was a genuine
    disagreement the shared node had to arbitrate (fail-closed: strip both).
    Per-edge, there is nothing to arbitrate -- each edge simply keeps its own
    claim's host, independently."""
    helm = tmp_path / "values.yaml"
    helm.write_text(
        f'GW_URL: "http://{_GW_HOST}"\n'
        'OTHER_URL: "http://billing.ext.prod.env"\n'
    )
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a_first.py", "sym:caller:a", _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="GW_URL")
    _claim(st, "caller", "b_second.py", "sym:caller:b", _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="OTHER_URL")

    cfg = _cfg(_svc("svc"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 2, "calls_http_unresolved": 0, "calls_http_external": 2}
    chan = _only_channel(st)
    assert "external" not in chan.props
    assert "external_host" not in chan.props

    edge_a = next(e for e in st.iter_edges() if e.src == "sym:caller:a")
    edge_b = next(e for e in st.iter_edges() if e.src == "sym:caller:b")
    assert edge_a.props.get("external") is True and edge_b.props.get("external") is True
    assert edge_a.props.get("external_host") == _GW_HOST
    assert edge_b.props.get("external_host") == "billing.ext.prod.env"


def test_collision_two_external_edges_with_same_host_are_independent_not_merged(tmp_path):
    """(d): two DIFFERENT env names that both happen to resolve to the IDENTICAL
    real hostname -- still two fully independent edges (no merge/dedup of props
    ever attempted, unlike the old node-level design's positive "agreement" case).
    Each edge keeps its OWN `config_ref` (the env name that produced IT) even
    though `external_host` happens to coincide -- proving this isn't secretly
    still a shared-node write under the hood."""
    helm = tmp_path / "values.yaml"
    helm.write_text(
        f'GW_URL: "http://{_GW_HOST}"\n'
        f'GW_URL_ALIAS: "http://{_GW_HOST}"\n'
    )
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a_first.py", "sym:caller:a", _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="GW_URL")
    _claim(st, "caller", "b_second.py", "sym:caller:b", _COLLISION_VERB, _COLLISION_TEMPLATE,
           base_url_env="GW_URL_ALIAS")

    cfg = _cfg(_svc("svc"), env_sources=[helm])
    stats = http_routes.link(cfg, st)

    assert stats == {"calls_http": 2, "calls_http_unresolved": 0, "calls_http_external": 2}
    chan = _only_channel(st)
    assert "external" not in chan.props
    assert "external_host" not in chan.props

    edge_a = next(e for e in st.iter_edges() if e.src == "sym:caller:a")
    edge_b = next(e for e in st.iter_edges() if e.src == "sym:caller:b")
    assert edge_a.props.get("external") is True and edge_b.props.get("external") is True
    assert edge_a.props.get("external_host") == _GW_HOST
    assert edge_b.props.get("external_host") == _GW_HOST
    assert edge_a.props.get("config_ref") == "GW_URL"
    assert edge_b.props.get("config_ref") == "GW_URL_ALIAS"
