"""M7 T3 (OPEN R1): linking.env_map.build_env_service_map -- env-var-name -> workspace
service-name, harvested from helm-values-shaped YAML files (WorkspaceConfig.env_sources).
Feeds linking/http_routes.py's tier 1(b) anchoring fallback -- consulted only when the
pre-existing ServiceConfig.http.base_url_env registry (tier 1(a)) finds no owner for a
claim's base_url_env (see that module's own docstring for the full tiering contract).

Real-world shape this mirrors (docs/superpowers/reports/2026-07-23-pilot-rerun-open-
gaps.md R1): `.helm/values/<env>/values.yaml` carries
`SERVICE_VERIFICATION_REQUESTS_URL: "http://verification-requests.kyc.svc.cluster.local:8000"`
-- the KEY is already the env-var name verbatim (SCREAMING_SNAKE, no case transform
needed), the VALUE's hostname's FIRST DNS label ("verification-requests") is checked for
an EXACT match against the workspace's own configured service names -- no fuzzy
matching, an unmatched label is honestly absent from the map, never guessed.
"""

from __future__ import annotations

from pathlib import Path

from codegraph.linking.env_map import build_env_service_map


def _write(tmp_path: Path, name: str, text: str) -> Path:
    p = tmp_path / name
    p.write_text(text)
    return p


def test_flat_top_level_mapping_hostname_label_matches_service(tmp_path):
    p = _write(
        tmp_path, "values.yaml",
        'SERVICE_VERIFICATION_REQUESTS_URL: '
        '"http://verification-requests.kyc.svc.cluster.local:8000"\n',
    )
    result = build_env_service_map([p], {"verification-requests", "other-service"})
    assert result == {"SERVICE_VERIFICATION_REQUESTS_URL": "verification-requests"}


def test_env_block_nesting_is_walked(tmp_path):
    p = _write(
        tmp_path, "values.yaml",
        "replicaCount: 2\n"
        "env:\n"
        '  SERVICE_LEGACYLIZER_URL: "http://legacylizer.kyc.svc.cluster.local:8080"\n',
    )
    result = build_env_service_map([p], {"legacylizer"})
    assert result == {"SERVICE_LEGACYLIZER_URL": "legacylizer"}


def test_deeply_nested_yaml_walk(tmp_path):
    p = _write(
        tmp_path, "values.yaml",
        "a:\n  b:\n    c:\n      SERVICE_DEEP_URL: "
        '"http://deep-svc.ns.svc.cluster.local"\n',
    )
    result = build_env_service_map([p], {"deep-svc"})
    assert result == {"SERVICE_DEEP_URL": "deep-svc"}


def test_hostname_label_with_no_matching_service_is_absent_no_fuzzy(tmp_path):
    p = _write(
        tmp_path, "values.yaml",
        'SERVICE_UNKNOWN_URL: "http://totally-unrelated.ns.svc.cluster.local"\n',
    )
    result = build_env_service_map([p], {"verification-requests"})
    assert result == {}


def test_non_url_string_value_ignored_for_service_mapping(tmp_path):
    """A string value that ISN'T URL-shaped at all (no scheme/netloc) is ignored for
    service-mapping purposes, even if it happens to spell a real service name --
    only genuine hostname-bearing URL values are harvested."""
    p = _write(
        tmp_path, "values.yaml",
        "SERVICE_NAME: just-a-plain-string\n"
        'SERVICE_X_URL: "http://x-service.ns.svc.cluster.local"\n',
    )
    result = build_env_service_map([p], {"x-service", "just-a-plain-string"})
    assert result == {"SERVICE_X_URL": "x-service"}


def test_non_string_yaml_values_excluded_naturally(tmp_path):
    p = _write(tmp_path, "values.yaml", "REPLICAS: 3\nFEATURE_FLAG: true\n")
    result = build_env_service_map([p], {"3", "true", "REPLICAS", "FEATURE_FLAG"})
    assert result == {}


def test_port_in_url_does_not_affect_hostname_label(tmp_path):
    p = _write(
        tmp_path, "values.yaml", 'SERVICE_X_URL: "http://svc-x.ns.svc.cluster.local:9000"\n',
    )
    result = build_env_service_map([p], {"svc-x"})
    assert result == {"SERVICE_X_URL": "svc-x"}


def test_empty_env_sources_list_yields_empty_map():
    assert build_env_service_map([], {"any-service"}) == {}


def test_multiple_files_merge_last_file_wins_on_key_collision(tmp_path):
    p1 = _write(tmp_path, "a.yaml", 'SERVICE_X_URL: "http://service-a.ns.svc.cluster.local"\n')
    p2 = _write(tmp_path, "b.yaml", 'SERVICE_X_URL: "http://service-b.ns.svc.cluster.local"\n')
    result = build_env_service_map([p1, p2], {"service-a", "service-b"})
    assert result == {"SERVICE_X_URL": "service-b"}


def test_multiple_files_distinct_keys_all_present(tmp_path):
    p1 = _write(tmp_path, "a.yaml", 'SERVICE_A_URL: "http://service-a.ns.svc.cluster.local"\n')
    p2 = _write(tmp_path, "b.yaml", 'SERVICE_B_URL: "http://service-b.ns.svc.cluster.local"\n')
    result = build_env_service_map([p1, p2], {"service-a", "service-b"})
    assert result == {
        "SERVICE_A_URL": "service-a", "SERVICE_B_URL": "service-b",
    }


def test_missing_file_skipped_defensively(tmp_path):
    """Real production loads validate env_sources existence at config LOAD time
    (config/loader.py's own resolution step); this module stays defensive at RUN
    time too (a WorkspaceConfig built directly in-memory -- every unit test in this
    repo's own style, not just this one -- bypasses that validation entirely) -- a
    missing path is silently skipped here, not a crash."""
    missing = tmp_path / "does_not_exist.yaml"
    assert build_env_service_map([missing], {"svc"}) == {}


def test_empty_yaml_file_yields_empty_map(tmp_path):
    p = _write(tmp_path, "empty.yaml", "")
    assert build_env_service_map([p], {"svc"}) == {}
