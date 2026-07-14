"""M2 T7: linking.http_routes.link -- matches staged http_call claims (T6) onto the
cross-service http_route Channel table (T4's fastapi_ext output), emitting CALLS_HTTP;
unresolved claims fall back to a synthetic owner="?" Channel + low-confidence CALLS_HTTP
so no claim is silently dropped (see http_routes.py module docstring for the full
matching contract: verb exact, segment-wise template comparison with `{param}` as a
bidirectional wildcard, base_url_env-based candidate narrowing)."""

from __future__ import annotations

from codegraph.config.models import HttpExposure, ServiceConfig, WorkspaceConfig
from codegraph.core.schema import make_channel_node
from codegraph.linking import http_routes
from codegraph.stores.staging import Staging


def _cfg(*services: ServiceConfig) -> WorkspaceConfig:
    return WorkspaceConfig(graph_name="g", services=list(services))


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


# -- resolved matches --


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
    assert stats == {"calls_http": 1, "calls_http_unresolved": 0}


def test_heuristic_claim_matches_with_confidence_0_6(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "POST", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "POST", "/x", resolution_hint="heuristic")

    http_routes.link(_cfg(_svc("svc")), st)

    e = next(e for e in st.iter_edges() if e.type == "CALLS_HTTP")
    assert e.resolution == "heuristic"
    assert e.confidence == 0.6


# -- segment matching: placeholders as bidirectional wildcards --


def test_route_placeholder_matches_client_literal_segment(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/{order_id}")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/42")

    http_routes.link(_cfg(_svc("svc")), st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id


def test_client_placeholder_matches_route_literal_segment(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/orders/42")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/orders/{order_id}")

    http_routes.link(_cfg(_svc("svc")), st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id


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


# -- base_url_env candidate narrowing --


def test_base_url_env_narrows_to_matching_service_among_ambiguous_routes(tmp_path):
    st = Staging(tmp_path / "s.db")
    wrong = _route_channel("service-a", "GET", "/x")
    right = _route_channel("service-b", "GET", "/x")
    st.upsert_nodes([wrong, right])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="B_URL")

    cfg = _cfg(_svc("service-a", "A_URL"), _svc("service-b", "B_URL"))
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == right.id


def test_base_url_env_excludes_route_from_unrelated_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env="OTHER_URL")

    cfg = _cfg(_svc("service-a", "A_URL"))
    stats = http_routes.link(cfg, st)
    assert stats["calls_http_unresolved"] == 1


def test_claim_without_base_url_env_considers_all_services(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("service-a", "GET", "/x")
    st.upsert_nodes([chan])
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/x", base_url_env=None)

    cfg = _cfg(_svc("service-a", "A_URL"))
    http_routes.link(cfg, st)

    edges = [e for e in st.iter_edges() if e.type == "CALLS_HTTP"]
    assert len(edges) == 1 and edges[0].dst == chan.id


# -- unresolved fallback --


def test_unresolved_creates_owner_unknown_channel_and_low_confidence_edge(tmp_path):
    st = Staging(tmp_path / "s.db")
    _claim(st, "caller", "a.py", "sym:caller:f", "GET", "/nowhere")

    stats = http_routes.link(_cfg(), st)

    assert stats == {"calls_http": 1, "calls_http_unresolved": 1}
    chans = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert len(chans) == 1
    chan = chans[0]
    assert chan.id == "chan:http:?:GET /nowhere"
    assert chan.props["unresolved"] is True
    assert "owner_service" not in chan.props

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
    assert stats == {"calls_http": 0, "calls_http_unresolved": 0}
    assert list(st.iter_nodes()) == []
    assert list(st.iter_edges()) == []


def test_calls_http_total_counts_both_resolved_and_unresolved(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = _route_channel("svc", "GET", "/ok")
    st.upsert_nodes([chan])
    _claim(st, "a", "a.py", "sym:a:f", "GET", "/ok")
    _claim(st, "b", "b.py", "sym:b:g", "GET", "/missing")

    stats = http_routes.link(_cfg(_svc("svc")), st)
    assert stats == {"calls_http": 2, "calls_http_unresolved": 1}
