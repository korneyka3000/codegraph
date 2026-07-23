"""S10 report: build_report агрегирует per-service отчёты analyze_service + load_stats
(load_graph) в единый JSON-совместимый dict; write_report/print_report — соответственно
запись на диск и rich-печать. per_service элементы — dict'ы формы analyze_service()
report (ключ "service" analyze_service отдаёт сам, первым ключом)."""

from __future__ import annotations

import json

from rich.console import Console

from codegraph.pipeline.report import build_report, print_report, write_report

SERVICE_OK = {
    "service": "orders-api",
    "files": 5, "defs": 10, "refs": 12, "malformed_ranges": 0,
    "nodes": 8, "edges": 9, "imports_external": 1,
    "calls_joined": 6, "calls_unresolved": 2, "calls_external": 1,
    "degraded": False, "reason": None, "from_cache": True,
}
SERVICE_DEGRADED = {
    "service": "kyc-worker",
    "files": 3, "defs": 4, "refs": 4, "malformed_ranges": 1,
    "nodes": 5, "edges": 4, "imports_external": 0,
    "calls_joined": 2, "calls_unresolved": 1, "calls_external": 0,
    "degraded": True, "reason": "scip-python timeout", "from_cache": False,
}
LOAD_STATS = {
    "nodes_written": 13,
    "nodes_written_by_label": {"Sym:Module": 5, "Sym:Function": 6, "Service": 2},
    "edges_written": 12,
    "edges_written_by_type": {"CONTAINS": 8, "CALLS": 4},
    "edges_dropped_missing_endpoint": 1,
    "edges_dropped_by_type": {"CALLS": 1, "CONTAINS": 0},
}
LINK_STATS = {
    "calls_http": 5, "calls_http_unresolved": 1,
    "next_segments": 3, "processes": 2, "marks": 1, "channels_gc": 4,
}
CHUNK_STATS = {
    "chunks_total": 42, "embedded": 40, "embedded_fresh": 30,
    "embedded_from_cache": 10, "reused": 2, "skipped_no_embedder": 0,
}


# -- build_report: aggregation --

def test_build_report_sums_totals_across_services():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["totals"] == {
        "files": 8, "nodes": 13, "edges": 13,
        "calls_joined": 8, "calls_unresolved": 3, "calls_external": 1,
    }


def test_build_report_passes_through_services_and_load():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["services"] == [SERVICE_OK, SERVICE_DEGRADED]
    assert report["load"] == LOAD_STATS


def test_build_report_health_pct_unresolved_calls():
    # joined=8, unresolved=3, external=1 -> denom=12 -> 3/12 = 25%
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["health"]["pct_unresolved_calls"] == 25.0


def test_build_report_health_dropped_edges_from_load_stats():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["health"]["dropped_edges"] == 1
    assert report["health"]["dropped_edges_by_type"] == {"CALLS": 1, "CONTAINS": 0}


# -- M5 Task 1 (pilot Bug B): health.staged_calls_with_valid_dst_pct -- CALLS
# written/(written+dropped) at load, from load_stats' own edges_written_by_type/
# edges_dropped_by_type (load.py already tracks both; this is pure aggregation, same
# "no new counting logic" shape as dropped_edges/dropped_edges_by_type above).


def test_build_report_health_staged_calls_with_valid_dst_pct():
    # LOAD_STATS: CALLS written=4, dropped=1 -> 4/(4+1) = 80%
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["health"]["staged_calls_with_valid_dst_pct"] == 80.0


def test_build_report_staged_calls_with_valid_dst_pct_zero_when_no_calls_staged():
    load_stats_no_calls = {
        **LOAD_STATS,
        "edges_written_by_type": {"CONTAINS": 8},
        "edges_dropped_by_type": {"CONTAINS": 0},
    }
    report = build_report([SERVICE_OK], load_stats_no_calls)
    assert report["health"]["staged_calls_with_valid_dst_pct"] == 0.0


def test_build_report_staged_calls_pct_defaults_when_load_stats_missing_by_type_keys():
    """A load_stats dict predating this metric (no edges_written_by_type/
    edges_dropped_by_type at all) must not KeyError; the metric degrades to 0.0, same
    defensive `.get(..., {})` convention as dropped_edges_by_type's own read just
    above this in build_report."""
    pre_m5_load_stats = {
        "nodes_written": 13, "nodes_written_by_label": {},
        "edges_written": 12, "edges_dropped_missing_endpoint": 1,
    }
    report = build_report([SERVICE_OK], pre_m5_load_stats)
    assert report["health"]["staged_calls_with_valid_dst_pct"] == 0.0


def test_build_report_degraded_services_list():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    assert report["health"]["degraded_services"] == [
        {"service": "kyc-worker", "reason": "scip-python timeout"}
    ]


def test_build_report_no_degraded_services_is_empty_list():
    report = build_report([SERVICE_OK], LOAD_STATS)
    assert report["health"]["degraded_services"] == []


def test_build_report_zero_calls_denominator_does_not_divide_by_zero():
    empty_service = {**SERVICE_OK, "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0}
    report = build_report([empty_service], LOAD_STATS)
    assert report["health"]["pct_unresolved_calls"] == 0.0


def test_build_report_empty_per_service_list():
    report = build_report([], LOAD_STATS)
    assert report["totals"] == {
        "files": 0, "nodes": 0, "edges": 0,
        "calls_joined": 0, "calls_unresolved": 0, "calls_external": 0,
    }
    assert report["services"] == []
    assert report["health"]["degraded_services"] == []
    assert report["health"]["pct_unresolved_calls"] == 0.0


# -- M2 T7: build_report link_stats (additive third parameter) --


def test_build_report_without_link_stats_has_no_linking_key():
    """Backward compatibility: every pre-M2-T7 2-positional-arg call site (this file's
    own tests above included) must see an IDENTICAL report shape -- no "linking" key at
    all when link_stats is omitted, not even an empty dict."""
    report = build_report([SERVICE_OK], LOAD_STATS)
    assert "linking" not in report


def test_build_report_with_link_stats_adds_linking_key():
    report = build_report([SERVICE_OK], LOAD_STATS, LINK_STATS)
    assert report["linking"] == LINK_STATS
    # additive: every pre-existing key/shape is untouched.
    assert report["totals"] == {
        "files": 5, "nodes": 8, "edges": 9,
        "calls_joined": 6, "calls_unresolved": 2, "calls_external": 1,
    }


def test_build_report_link_stats_none_is_same_as_omitted():
    assert build_report([SERVICE_OK], LOAD_STATS, None) == build_report([SERVICE_OK], LOAD_STATS)


# -- M3 T6: build_report chunk_stats (additive fourth parameter, mirrors link_stats) --


def test_build_report_without_chunk_stats_has_no_chunking_key():
    """Backward compatibility: every pre-M3-T6 call site (2/3-positional-arg, this
    file's own tests above included) must see an IDENTICAL report shape -- no
    "chunking" key at all when chunk_stats is omitted, not even an empty dict."""
    report = build_report([SERVICE_OK], LOAD_STATS, LINK_STATS)
    assert "chunking" not in report


def test_build_report_with_chunk_stats_adds_chunking_key():
    report = build_report([SERVICE_OK], LOAD_STATS, LINK_STATS, CHUNK_STATS)
    assert report["chunking"] == CHUNK_STATS
    # additive: every pre-existing key/shape is untouched.
    assert report["linking"] == LINK_STATS
    assert report["totals"] == {
        "files": 5, "nodes": 8, "edges": 9,
        "calls_joined": 6, "calls_unresolved": 2, "calls_external": 1,
    }


def test_build_report_chunk_stats_none_is_same_as_omitted():
    assert build_report([SERVICE_OK], LOAD_STATS, LINK_STATS, None) == build_report(
        [SERVICE_OK], LOAD_STATS, LINK_STATS
    )


def test_build_report_chunk_stats_works_without_link_stats():
    """chunk_stats doesn't require link_stats to also be present -- both are
    independent additive parameters (a caller could theoretically have one without
    the other, even though cli.index's real wiring always supplies both)."""
    report = build_report([SERVICE_OK], LOAD_STATS, None, CHUNK_STATS)
    assert "linking" not in report
    assert report["chunking"] == CHUNK_STATS


# -- write_report: JSON round-trip --

def test_write_report_round_trips_via_json(tmp_path):
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    path = tmp_path / "report.json"
    write_report(report, path)
    assert path.exists()
    assert json.loads(path.read_text()) == report


def test_write_report_creates_parent_dirs(tmp_path):
    report = build_report([SERVICE_OK], LOAD_STATS)
    path = tmp_path / "nested" / "dir" / "report.json"
    write_report(report, path)
    assert path.exists()
    assert json.loads(path.read_text()) == report


# -- print_report: rich smoke test via Console(record=True) --

def test_print_report_smoke_shows_services_and_totals():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    # per-service rows present
    assert "orders-api" in text
    assert "kyc-worker" in text
    # per-service metrics (files/nodes/edges/joined/unresolved/external) show up somewhere
    for value in ("5", "8", "9", "6", "2", "1"):  # SERVICE_OK's row values
        assert value in text


def test_print_report_smoke_shows_health_and_load_summary():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    assert "25.0" in text  # pct_unresolved_calls
    assert "13" in text  # nodes_written
    assert "12" in text  # edges_written


# -- M5 Task 1: print_report staged_calls_with_valid_dst_pct (additive to the
# existing health line, .get()-defaulted -- same backward-compatible-dict precedent
# as chunking's embedded_fresh/embedded_from_cache, M4 T1) --


def test_print_report_smoke_shows_staged_calls_with_valid_dst_pct():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "80.0" in text  # staged_calls_with_valid_dst_pct (4 written / (4+1))


def test_print_report_staged_calls_pct_defaults_when_health_key_absent():
    """Backward compatibility: a report dict whose health sub-dict predates this
    metric (e.g. a report.json written by pre-M5 code) must not KeyError --
    print_report degrades to showing 0.0% rather than crashing, same `.get(key,
    default)` convention as every other M3/M4 additive report field this module
    already reads."""
    report = build_report([SERVICE_OK], LOAD_STATS)
    del report["health"]["staged_calls_with_valid_dst_pct"]
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise
    text = console.export_text()
    assert "0.0%" in text


def test_print_report_smoke_shows_degraded_block_when_present():
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    assert "kyc-worker" in text
    assert "scip-python timeout" in text


def test_print_report_degraded_reason_with_markup_renders_literally():
    # live-verified bug: an unescaped "["-bearing reason either crashes Console.print
    # with rich.errors.MarkupError (e.g. a stray "[/bad]" closing tag) or silently
    # swallows the bracketed text as an unrecognized style tag -- escape() must make
    # it render as inert literal text instead, with no exception.
    service_degraded_markup = {**SERVICE_DEGRADED, "reason": "[/bad]markup[bold]"}
    report = build_report([SERVICE_OK, service_degraded_markup], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise MarkupError
    text = console.export_text()
    assert "[/bad]markup[bold]" in text


def test_print_report_smoke_no_degraded_block_when_absent():
    report = build_report([SERVICE_OK], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    # the per-service table always has a "degraded" column header (SERVICE_OK's row
    # says "no"); the distinct summary block ("degraded services: ...") must be absent
    # since nothing is degraded here.
    assert "degraded services" not in text.lower()


# -- M6 T2 review Important-1: print_report http idiom failure counters line --
# Summed straight from report["services"] with .get(key, 0) (same precedent as the
# fallback_services block: services-list iteration, defensive .get) -- a pre-M6
# per-service dict without these keys contributes 0 and must never KeyError.


def test_print_report_shows_http_idiom_misses_line_when_nonzero():
    svc_with_misses = {
        **SERVICE_OK,
        "http_url_unresolved": 2, "http_verb_unresolved": 1, "http_route_unresolved": 3,
    }
    report = build_report([svc_with_misses, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "http idiom misses" in text
    assert "url_unresolved = 2" in text
    assert "verb_unresolved = 1" in text
    assert "route_unresolved = 3" in text


def test_print_report_http_idiom_misses_line_sums_across_services():
    svc_a = {**SERVICE_OK, "http_verb_unresolved": 1}
    svc_b = {**SERVICE_DEGRADED, "http_verb_unresolved": 2}
    report = build_report([svc_a, svc_b], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "verb_unresolved = 3" in text


def test_print_report_no_http_idiom_misses_line_when_all_zero():
    svc = {
        **SERVICE_OK,
        "http_url_unresolved": 0, "http_verb_unresolved": 0, "http_route_unresolved": 0,
    }
    report = build_report([svc], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    assert "http idiom misses" not in console.export_text()


def test_print_report_http_idiom_misses_defaults_when_keys_absent():
    """SERVICE_OK/SERVICE_DEGRADED (pre-M6 shapes, no http_* keys at all) must not
    KeyError -- .get(0) makes them contribute nothing and the line stays absent."""
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise
    assert "http idiom misses" not in console.export_text()


# -- M6 T3: print_report kafka base_class honest-miss counter line -- same precedent
# as the http idiom misses block just above (services-list iteration, defensive
# .get(key, 0), no line at all when every count is zero/absent).


def test_print_report_shows_kafka_base_class_no_generic_line_when_nonzero():
    svc_with_misses = {**SERVICE_OK, "consumer_base_class_no_generic": 2}
    report = build_report([svc_with_misses, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "kafka idiom misses" in text
    assert "base_class_no_generic = 2" in text


def test_print_report_kafka_base_class_no_generic_line_sums_across_services():
    svc_a = {**SERVICE_OK, "consumer_base_class_no_generic": 1}
    svc_b = {**SERVICE_DEGRADED, "consumer_base_class_no_generic": 2}
    report = build_report([svc_a, svc_b], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "base_class_no_generic = 3" in text


def test_print_report_no_kafka_base_class_line_when_zero():
    svc = {**SERVICE_OK, "consumer_base_class_no_generic": 0}
    report = build_report([svc], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    assert "kafka idiom misses" not in console.export_text()


def test_print_report_kafka_base_class_line_defaults_when_key_absent():
    """SERVICE_OK/SERVICE_DEGRADED (pre-M6 T3 shapes, no consumer_base_class_no_generic
    key at all) must not KeyError -- .get(0) makes them contribute nothing and the
    line stays absent."""
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise
    assert "kafka idiom misses" not in console.export_text()


# -- M2 T7: print_report linking summary (additive) --


def test_print_report_shows_linking_summary_when_present():
    report = build_report([SERVICE_OK], LOAD_STATS, LINK_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    assert "linking" in text.lower()
    # calls_http/unresolved/next_segments/processes/channels_gc
    for value in ("5", "1", "3", "2", "4"):
        assert value in text


def test_print_report_no_linking_line_when_absent():
    """Backward compatibility: a report built without link_stats (or hand-built without
    a "linking" key at all, as every pre-T7 report was) must not print anything new."""
    report = build_report([SERVICE_OK], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise (no KeyError on missing "linking")
    text = console.export_text()
    assert "linking" not in text.lower()


# -- M3 T6: print_report chunking summary (additive) --


def test_print_report_shows_chunking_summary_when_present():
    report = build_report([SERVICE_OK], LOAD_STATS, chunk_stats=CHUNK_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    assert "chunking" in text.lower()
    # chunks_total, embedded_fresh, embedded_from_cache, reused
    for value in ("42", "30", "10", "2"):
        assert value in text
    assert "fresh" in text.lower()
    assert "cached" in text.lower()


def test_print_report_shows_skipped_no_embedder_when_nonzero():
    degraded_chunk_stats = {
        "chunks_total": 10, "embedded": 0, "embedded_fresh": 0,
        "embedded_from_cache": 0, "reused": 0, "skipped_no_embedder": 10,
    }
    report = build_report([SERVICE_OK], LOAD_STATS, chunk_stats=degraded_chunk_stats)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "skipped_no_embedder" in text.lower()
    assert "10" in text


def test_print_report_hides_skipped_no_embedder_when_zero():
    report = build_report([SERVICE_OK], LOAD_STATS, chunk_stats=CHUNK_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "skipped_no_embedder" not in text.lower()


def test_print_report_chunking_summary_defaults_missing_fresh_cache_keys_to_zero():
    """M4 T1 added embedded_fresh/embedded_from_cache -- a chunk_stats dict that
    doesn't carry them (e.g. a pre-M4 report.json, or any future caller that only
    ever knew about the older key set) must not KeyError; the printed line degrades
    to "0 fresh + 0 cached" rather than crashing (same defensive `.get(..., 0)`
    convention as every other field this function reads)."""
    pre_m4_shaped_chunk_stats = {
        "chunks_total": 5, "embedded": 5, "reused": 0, "skipped_no_embedder": 0,
    }
    report = build_report([SERVICE_OK], LOAD_STATS, chunk_stats=pre_m4_shaped_chunk_stats)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise
    text = console.export_text()
    assert "0 fresh" in text
    assert "0 cached" in text


def test_print_report_no_chunking_line_when_absent():
    """Backward compatibility: a report built without chunk_stats must not print
    anything new (no KeyError on the missing "chunking" key)."""
    report = build_report([SERVICE_OK], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "chunking" not in text.lower()


# -- M4 T7: print_report per-service mode/reason/stale_files (additive) --


def test_print_report_mode_column_defaults_to_full_for_pre_m4_dicts():
    """SERVICE_OK/SERVICE_DEGRADED (this file's own module-level fixtures) predate
    M4 T7 and carry no "mode" key at all -- must not KeyError, must print "full"
    (today's only possible pre-M4 mode), same `.get(key, default)` defensive
    convention as every other M3/M4 additive report field this module already reads
    (T1 precedent: embedded_fresh/embedded_from_cache default to 0)."""
    report = build_report([SERVICE_OK, SERVICE_DEGRADED], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise
    text = console.export_text()
    assert "full" in text.lower()


def test_print_report_mode_column_shows_incremental_with_stale_count():
    service_incremental = {**SERVICE_OK, "mode": "incremental", "stale_files": 3}
    report = build_report([service_incremental], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "incremental" in text.lower()
    assert "3" in text


def test_print_report_mode_column_shows_skipped():
    service_skipped = {**SERVICE_OK, "mode": "skipped"}
    report = build_report([service_skipped], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "skipped" in text.lower()


def test_print_report_shows_fallback_reason_line_for_non_degraded_full_mode():
    service_fallback = {
        **SERVICE_OK, "mode": "full", "reason": "fingerprint mismatch", "degraded": False,
    }
    report = build_report([service_fallback], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "fingerprint mismatch" in text
    assert "incremental fallback" in text.lower()


def test_print_report_fallback_reason_line_absent_when_every_service_is_full_with_no_reason():
    report = build_report([SERVICE_OK], LOAD_STATS)  # mode absent -> "full", reason=None
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "incremental fallback" not in text.lower()


def test_print_report_fallback_reason_line_excludes_degraded_services():
    """A degraded service's reason already surfaces via the pre-existing yellow
    "degraded services" block -- the new fallback-reason line must not duplicate it,
    even though degraded reports also carry mode="full" with a non-None reason."""
    service_degraded_full = {
        **SERVICE_DEGRADED, "mode": "full", "reason": "scip-python timeout",
    }
    report = build_report([service_degraded_full], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()
    assert "scip-python timeout" in text  # still shown once, via the degraded block
    assert "incremental fallback" not in text.lower()


def test_print_report_fallback_reason_with_markup_renders_literally():
    # Same live-verified class of bug as the degraded-reason escape test above: an
    # unescaped "["-bearing fallback reason must render literally, not crash or be
    # silently swallowed as a style tag.
    service_fallback = {
        **SERVICE_OK, "mode": "full", "reason": "[/bad]markup[bold]", "degraded": False,
    }
    report = build_report([service_fallback], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise MarkupError
    text = console.export_text()
    assert "[/bad]markup[bold]" in text
