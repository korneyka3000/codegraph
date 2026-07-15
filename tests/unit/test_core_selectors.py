"""core.selectors.parse_selector (M3 T2): the shared "<service>:<METHOD> <path>" /
"<service>:<dotted.qualified.name>" grammar, extracted out of linking/processes.py so
both the S7 staging-side resolver (linking.processes) and the M3 graph-side resolver
(query.api.GraphQuery.resolve_selector) parse selectors identically -- see this
module's own docstring."""

from __future__ import annotations

from codegraph.core.selectors import QualifiedSelector, RouteSelector, parse_selector


def test_route_form_parses_service_method_and_path():
    assert parse_selector("orders-api:POST /orders") == RouteSelector(
        service="orders-api", method="POST", path="/orders"
    )


def test_route_form_uppercases_lowercase_verb():
    assert parse_selector("orders-api:post /orders") == RouteSelector(
        service="orders-api", method="POST", path="/orders"
    )


def test_route_form_path_can_contain_spaces_after_the_first():
    # partition(" ") splits on the FIRST space only -- everything after stays in path.
    assert parse_selector("svc:GET /a b") == RouteSelector(service="svc", method="GET", path="/a b")


def test_qualified_form_parses_service_and_dotted_name():
    assert parse_selector("kyc-worker:app.workflows.kyc.KycWorkflow") == QualifiedSelector(
        service="kyc-worker", qualified="app.workflows.kyc.KycWorkflow"
    )


def test_qualified_form_when_first_token_is_not_a_known_http_verb():
    assert parse_selector("svc:Frobnicate /thing") == QualifiedSelector(
        service="svc", qualified="Frobnicate /thing"
    )


def test_qualified_form_when_rest_has_no_space_at_all():
    assert parse_selector("svc:bare.name") == QualifiedSelector(
        service="svc", qualified="bare.name"
    )


def test_malformed_selector_without_colon_returns_none():
    assert parse_selector("not-a-selector") is None


def test_empty_string_returns_none():
    assert parse_selector("") is None
