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

import logging
from pathlib import Path

from codegraph.linking.env_map import build_env_hostname_map, build_env_service_map


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


def test_malformed_yaml_file_skipped_with_warning_others_still_contribute(tmp_path, caplog):
    """M7 T3 review Important-1: a malformed env_sources file (the REALISTIC shape:
    an UNRENDERED helm template -- `{{ .Values.host }}` is invalid YAML, raising
    yaml.YAMLError on load) must NOT crash -- before this fix it propagated uncaught
    out of build_env_service_map, i.e. deep inside S7's link() AFTER all the
    expensive per-service scip/analyze work, contradicting this module's own
    "defensive at read time" contract (the missing-file precedent right next to it).
    Now: skip the file, warn (logger.warning, path + error visible), and keep every
    OTHER file's contribution intact."""
    bad = _write(tmp_path, "bad.yaml", "SERVICE_X_URL: {{ .Values.host }}\n")
    good = _write(
        tmp_path, "good.yaml", 'SERVICE_Y_URL: "http://y-service.ns.svc.cluster.local"\n',
    )
    with caplog.at_level(logging.WARNING, logger="codegraph.linking.env_map"):
        result = build_env_service_map([bad, good], {"x-service", "y-service"})

    assert result == {"SERVICE_Y_URL": "y-service"}
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "bad.yaml" in warnings[0].getMessage()


# ============================================================================
# M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): build_env_
# hostname_map -- the SAME env_sources data, harvested WITHOUT filtering to
# hostnames that happen to name a workspace service. Feeds linking/http_routes.py's
# tier-2 split: a claim whose base_url_env resolves to a hostname THROUGH THIS MAP,
# but names no workspace service, is a documented "external" boundary -- distinct
# from an env name this map (like build_env_service_map above) has NOTHING for at
# all (the pre-existing, unchanged "unmapped" tier).
# ============================================================================


def test_hostname_map_includes_hostnames_that_match_no_service(tmp_path):
    """The core behavioral difference from build_env_service_map: a hostname whose
    first DNS label names NO configured workspace service is still harvested here
    (full hostname, not the label) -- this is exactly the "external" case."""
    p = _write(
        tmp_path, "values.yaml",
        'SERVICE_API_GATEWAY_URL: "http://api-gateway.prod.svc.cluster.local:8000"\n',
    )
    result = build_env_hostname_map([p])
    assert result == {"SERVICE_API_GATEWAY_URL": "api-gateway.prod.svc.cluster.local"}


def test_hostname_map_returns_full_hostname_not_first_label(tmp_path):
    """Unlike build_env_service_map (which reduces to the first DNS label for
    service-name matching), this map keeps the FULL hostname -- the text a trace's
    external-exit rendering shows verbatim (see query/traverse.py, cli.py)."""
    p = _write(
        tmp_path, "values.yaml",
        'SERVICE_X_URL: "http://x.deep.ns.svc.cluster.local"\n',
    )
    result = build_env_hostname_map([p])
    assert result == {"SERVICE_X_URL": "x.deep.ns.svc.cluster.local"}


def test_hostname_map_also_includes_hostnames_that_do_match_a_service(tmp_path):
    """No service-name filtering happens here at all -- a hostname that WOULD also
    satisfy build_env_service_map is harvested too (http_routes.py's own tier
    ordering, not this function, is what makes the "anchored" tier win first)."""
    p = _write(
        tmp_path, "values.yaml",
        'SERVICE_WORKER_URL: "http://worker.kyc.svc.cluster.local:9000"\n',
    )
    result = build_env_hostname_map([p])
    assert result == {"SERVICE_WORKER_URL": "worker.kyc.svc.cluster.local"}


def test_hostname_map_port_stripped_same_as_service_map(tmp_path):
    p = _write(tmp_path, "values.yaml", 'X_URL: "http://host.ns.svc.cluster.local:9000"\n')
    result = build_env_hostname_map([p])
    assert result == {"X_URL": "host.ns.svc.cluster.local"}


def test_hostname_map_non_url_string_value_ignored(tmp_path):
    p = _write(tmp_path, "values.yaml", "SERVICE_NAME: just-a-plain-string\n")
    result = build_env_hostname_map([p])
    assert result == {}


def test_hostname_map_schemeless_host_port_value_absent(tmp_path):
    """urlparse quirk (see env_map.py module docstring): a SCHEME-LESS `host:port`
    value parses with hostname=None (the text before ":" reads as a scheme, not a
    host) -- absent from this map too, same honest degradation as
    build_env_service_map."""
    p = _write(tmp_path, "values.yaml", 'X_URL: "verification-requests.kyc:8000"\n')
    result = build_env_hostname_map([p])
    assert result == {}


def test_hostname_map_empty_env_sources_list_yields_empty_map():
    assert build_env_hostname_map([]) == {}


def test_hostname_map_missing_file_skipped_defensively(tmp_path):
    missing = tmp_path / "does_not_exist.yaml"
    assert build_env_hostname_map([missing]) == {}


def test_hostname_map_multiple_files_merge_last_file_wins_on_key_collision(tmp_path):
    p1 = _write(tmp_path, "a.yaml", 'X_URL: "http://host-a.ns.svc.cluster.local"\n')
    p2 = _write(tmp_path, "b.yaml", 'X_URL: "http://host-b.ns.svc.cluster.local"\n')
    result = build_env_hostname_map([p1, p2])
    assert result == {"X_URL": "host-b.ns.svc.cluster.local"}


def test_hostname_map_malformed_yaml_file_skipped_with_warning_others_still_contribute(
    tmp_path, caplog,
):
    bad = _write(tmp_path, "bad.yaml", "SERVICE_X_URL: {{ .Values.host }}\n")
    good = _write(
        tmp_path, "good.yaml", 'SERVICE_Y_URL: "http://y-service.ns.svc.cluster.local"\n',
    )
    with caplog.at_level(logging.WARNING, logger="codegraph.linking.env_map"):
        result = build_env_hostname_map([bad, good])

    assert result == {"SERVICE_Y_URL": "y-service.ns.svc.cluster.local"}
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "bad.yaml" in warnings[0].getMessage()
