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
    "next_segments": 3, "processes": 2, "marks": 1,
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


# -- M2 T7: print_report linking summary (additive) --


def test_print_report_shows_linking_summary_when_present():
    report = build_report([SERVICE_OK], LOAD_STATS, LINK_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)
    text = console.export_text()

    assert "linking" in text.lower()
    for value in ("5", "1", "3", "2"):  # calls_http/unresolved/next_segments/processes
        assert value in text


def test_print_report_no_linking_line_when_absent():
    """Backward compatibility: a report built without link_stats (or hand-built without
    a "linking" key at all, as every pre-T7 report was) must not print anything new."""
    report = build_report([SERVICE_OK], LOAD_STATS)
    console = Console(record=True, width=200)
    print_report(report, console)  # must not raise (no KeyError on missing "linking")
    text = console.export_text()
    assert "linking" not in text.lower()
