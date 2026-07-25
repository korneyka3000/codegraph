"""S10 report: агрегирует per-service отчёты (analyze.analyze_service -- каждый
уже содержит ключ "service") и load_stats (load.load_graph) в единый
JSON-совместимый dict.

build_report -- чистая агрегация (без side-effects); write_report -- JSON на диск;
print_report -- rich-таблицы для CLI (`index`/`load`). "Не изобретай много" (бриф
m1b-task-5): ровно то, что нужно для милestone-проверки качества графа M1 -- без
каналов/роутов (это M2), без orphan-метрик (нет ещё соответствующих узлов/рёбер).
"""

from __future__ import annotations

import json
from pathlib import Path

from rich.console import Console
from rich.markup import escape
from rich.table import Table

_TOTAL_FIELDS = ("files", "nodes", "edges", "calls_joined", "calls_unresolved", "calls_external")


def _pct(numerator: int, denominator: int) -> float:
    return 100.0 * numerator / denominator if denominator else 0.0


def _staged_calls_valid_dst_pct(load_stats: dict) -> float:
    """M5 Task 1 (pilot Bug B, docs/superpowers/reports/2026-07-18-m4-pilot.md §10.2
    recommendation): what fraction of staged CALLS edges actually had a valid dst at
    load time -- written/(written+dropped), CALLS only. `pct_unresolved_calls` (the
    pre-existing health metric) counts unresolved/external call-SITES at JOIN time
    (S6) and was never a measure of first-party call-graph quality on a real repo with
    third-party deps (see calls.py's own module docstring for why) -- this metric
    instead measures the very end of the pipeline (S9 load), where a "joined" CALLS
    edge either lands in the graph or silently vanishes ("missing endpoint", load.py).
    No new counting logic needed: load.py already tracks both `edges_written_by_type`
    and `edges_dropped_by_type`, keyed by edge type -- this is pure aggregation, same
    as `dropped_edges`/`dropped_edges_by_type` just below. `.get(..., {})` on both
    dicts (not a bare index) so a load_stats shape predating this metric (no
    edges_written_by_type/edges_dropped_by_type keys at all) degrades to 0.0 via
    `_pct`'s own zero-denominator convention, rather than KeyError."""
    written = load_stats.get("edges_written_by_type", {}).get("CALLS", 0)
    dropped = load_stats.get("edges_dropped_by_type", {}).get("CALLS", 0)
    return _pct(written, written + dropped)


def build_report(
    per_service: list[dict],
    load_stats: dict,
    link_stats: dict | None = None,
    chunk_stats: dict | None = None,
) -> dict:
    """per_service: список dict'ов analyze_service()-report (ключ "service" включён).
    load_stats: возврат load.load_graph() (nodes_written/edges_written/
    edges_dropped_missing_endpoint + by-type/by-label разбивки). link_stats (M2 T7,
    аддитивный параметр -- дефолт None сохраняет ПОЛНОСТЬЮ идентичный dict для каждого
    существующего 2-позиционного вызова): возврат linking.workspace.link_workspace()
    (calls_http/calls_http_unresolved/next_segments/processes/marks/channels_gc --
    последний M2 final review: orphan-Channel-узлы, выметенные в конце link_workspace).
    chunk_stats (M3 T6, ещё один аддитивный параметр, тем же принципом что link_stats
    -- дефолт None сохраняет идентичный dict для каждого существующего 2/3-позиционного
    вызова): возврат pipeline.chunk_embed.run() (chunks_total/embedded/embedded_fresh/
    embedded_from_cache/reused/skipped_no_embedder -- embedded_fresh/embedded_from_cache
    -- аддитивная разбивка M4 T1, embedded остаётся их суммой -- S8, между
    S7-линковкой и S9-load в cli.index).

    Возврат -- JSON-сериализуемый dict: {"services", "totals", "load", "health"} + ключ
    "linking" (ТОЛЬКО если link_stats передан) + ключ "chunking" (ТОЛЬКО если
    chunk_stats передан -- оба независимы друг от друга). "health": pct_unresolved_calls
    = unresolved/(joined+unresolved+external) * 100 (0.0 при нулевом знаменателе --
    сервисы без единого call-сайта, не делить на 0), dropped_edges(+by_type) из
    load_stats, staged_calls_with_valid_dst_pct (M5 Task 1, pilot §10.2: CALLS
    written/(written+dropped) at load -- see `_staged_calls_valid_dst_pct`'s own
    docstring for why this and pct_unresolved_calls measure two different, both
    honest, things), degraded_services -- список {service, reason} для сервисов с
    degraded=True (эвристический fallback вместо SCIP, см. analyze.py).
    """
    totals = {field: sum(s.get(field, 0) for s in per_service) for field in _TOTAL_FIELDS}
    denom = totals["calls_joined"] + totals["calls_unresolved"] + totals["calls_external"]
    degraded_services = [
        {"service": s.get("service"), "reason": s.get("reason")}
        for s in per_service
        if s.get("degraded")
    ]
    report = {
        "services": list(per_service),
        "totals": totals,
        "load": dict(load_stats),
        "health": {
            "pct_unresolved_calls": _pct(totals["calls_unresolved"], denom),
            "dropped_edges": load_stats.get("edges_dropped_missing_endpoint", 0),
            "dropped_edges_by_type": dict(load_stats.get("edges_dropped_by_type", {})),
            "staged_calls_with_valid_dst_pct": _staged_calls_valid_dst_pct(load_stats),
            "degraded_services": degraded_services,
        },
    }
    if link_stats is not None:
        report["linking"] = dict(link_stats)
    if chunk_stats is not None:
        report["chunking"] = dict(chunk_stats)
    return report


def write_report(report: dict, path: Path) -> None:
    """JSON-дамп report на диск (создаёт родительские директории при необходимости)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))


_SERVICE_COLUMNS = (
    ("service", "service"),
    ("files", "files"),
    ("nodes", "nodes"),
    ("edges", "edges"),
    ("calls_joined", "joined"),
    ("calls_unresolved", "unresolved"),
    ("calls_external", "external"),
)


def _mode_cell(s: dict) -> str:
    """M4 T7: per-service mode column text -- `.get("mode", "full")` (T1 precedent:
    every pre-M4 per-service dict, e.g. this file's own pre-M4 test fixtures, has no
    "mode" key at all, and "full" is the only mode that ever existed before this
    task). "incremental" additionally shows its `stale_files` COUNT (a plain int,
    never foreign text -- no escape() needed here, unlike `reason` below)."""
    mode = s.get("mode", "full")
    stale = s.get("stale_files")
    if mode == "incremental" and stale is not None:
        return f"{mode} ({stale} stale)"
    return mode


def print_report(report: dict, console: Console) -> None:
    """rich-печать: таблица по сервисам (+ TOTAL-строка = "итого"), сводка load,
    веха здоровья (% unresolved calls, dropped edges), жёлтый блок degraded-сервисов
    если есть хоть один, циан-строка причин fallback-to-full (M4 T7) если есть хоть
    одна."""
    svc_table = Table(title="services")
    for _, header in _SERVICE_COLUMNS:
        svc_table.add_column(header)
    svc_table.add_column("degraded")
    svc_table.add_column("mode")

    for s in report["services"]:
        row = [str(s.get(key, 0)) for key, _ in _SERVICE_COLUMNS]
        degraded = bool(s.get("degraded"))
        row.append("[yellow]yes[/]" if degraded else "no")
        row.append(_mode_cell(s))
        svc_table.add_row(*row)

    totals = report["totals"]
    total_row = ["[bold]TOTAL[/]"] + [
        f"[bold]{totals.get(key, 0)}[/]" for key, _ in _SERVICE_COLUMNS[1:]
    ]
    total_row.append("")  # degraded
    total_row.append("")  # mode
    svc_table.add_row(*total_row)
    console.print(svc_table)

    load = report["load"]
    load_table = Table(title="load")
    load_table.add_column("nodes_written")
    load_table.add_column("edges_written")
    load_table.add_column("edges_dropped")
    load_table.add_row(
        str(load.get("nodes_written", 0)),
        str(load.get("edges_written", 0)),
        str(load.get("edges_dropped_missing_endpoint", 0)),
    )
    console.print(load_table)

    health = report["health"]
    dropped_by_type = health.get("dropped_edges_by_type", {})
    nonzero_dropped = {t: n for t, n in dropped_by_type.items() if n}
    dropped_detail = (
        f" ({', '.join(f'{t}={n}' for t, n in sorted(nonzero_dropped.items()))})"
        if nonzero_dropped else ""
    )
    # staged_calls_with_valid_dst_pct (M5 Task 1): .get()-defaulted, not a bare
    # index, unlike pct_unresolved_calls/dropped_edges above -- those are as old as
    # build_report's own health dict and can be relied on unconditionally, but this
    # key can be absent from a report dict predating this task (a pre-M5 report.json,
    # or a hand-built dict in an older test) -- same backward-compatible-dict
    # precedent as chunking's embedded_fresh/embedded_from_cache (M4 T1).
    console.print(
        f"health: unresolved calls = {health['pct_unresolved_calls']:.1f}%, "
        f"dropped edges = {health['dropped_edges']}{dropped_detail}, "
        f"staged CALLS with valid dst = "
        f"{health.get('staged_calls_with_valid_dst_pct', 0.0):.1f}%"
    )

    # M6 T2 review Important-1 (additive): http idiom failure counters, summed straight
    # from the per-service dicts with .get(key, 0) -- same services-list iteration +
    # defensive-.get precedent as the fallback_services block below; a pre-M6
    # per-service dict (no http_* keys at all) contributes 0 and never KeyErrors. Line
    # only appears when at least one counter is nonzero (same "no noise when clean"
    # convention as skipped_no_embedder / the degraded block). Yellow: each count is a
    # would-be http_call claim that silently died inside the http_client extractor
    # (unresolvable URL arg / route decorator arg / Request verb) -- a coverage
    # warning, not an error. Plain ints, no foreign text -- no escape() needed.
    http_url = sum(s.get("http_url_unresolved", 0) for s in report["services"])
    http_verb = sum(s.get("http_verb_unresolved", 0) for s in report["services"])
    http_route = sum(s.get("http_route_unresolved", 0) for s in report["services"])
    if http_url or http_verb or http_route:
        console.print(
            f"[yellow]http idiom misses: url_unresolved = {http_url}, "
            f"verb_unresolved = {http_verb}, route_unresolved = {http_route}[/]"
        )

    # M6 T3 (additive, same precedent as the http idiom misses block just above):
    # kafka's own base_class honest-miss counter -- a class matches a base_class
    # idiom's target base but has no usable generic argument at all (bare
    # `class C(Base):`, or event_type_from.generic_arg past the end of what the
    # subscript carries). Yellow: a would-be CONSUMES claim that silently died
    # inside kafka_ext, a coverage warning rather than an error. Plain int, no
    # foreign text -- no escape() needed.
    #
    # M6 T4 (GAPS §6/pilot gap 5, same line, same precedent): producer_unresolved_
    # channel -- a producer call matched an idiom but its configured topic/
    # event_type source (arg/kwarg/env/const) resolved to nothing usable (a missing
    # kwarg, a dynamic attribute expression like `payload.topic_name`, ...); see
    # kafka_ext.py's _emit_kafka_topic_produces/_emit_event_type_produces. A would-be
    # PRODUCES claim that silently died the same way, so it shares the SAME yellow
    # line (one line per extractor family) rather than getting its own -- gated on
    # EITHER counter being nonzero, same "any nonzero" convention as the http trio.
    kafka_base_class_no_generic = sum(
        s.get("consumer_base_class_no_generic", 0) for s in report["services"]
    )
    kafka_producer_unresolved_channel = sum(
        s.get("producer_unresolved_channel", 0) for s in report["services"]
    )
    if kafka_base_class_no_generic or kafka_producer_unresolved_channel:
        console.print(
            f"[yellow]kafka idiom misses: "
            f"base_class_no_generic = {kafka_base_class_no_generic}, "
            f"producer_unresolved_channel = {kafka_producer_unresolved_channel}[/]"
        )

    # M7 T4 (OPEN R3, same precedent as the http/kafka blocks above): temporal's own
    # signal-sender honest-miss counter -- a `.signal(...)` call site that LOOKED
    # like a genuine signal-name reference (a bare variable/attribute) but couldn't
    # be resolved to a concrete channel identity (extractors/temporal_ext.py's
    # `_resolve_signal_arg0`). A would-be PRODUCES claim that silently died this
    # way, same "coverage warning, not an error" framing as every counter above.
    temporal_signal_name_unresolved = sum(
        s.get("signal_name_unresolved", 0) for s in report["services"]
    )
    if temporal_signal_name_unresolved:
        console.print(
            f"[yellow]temporal idiom misses: "
            f"signal_name_unresolved = {temporal_signal_name_unresolved}[/]"
        )

    degraded_services = health.get("degraded_services", [])
    if degraded_services:
        detail = ", ".join(
            f"{d['service']} ({d['reason']})" if d.get("reason") else str(d["service"])
            for d in degraded_services
        )
        # detail -- rendered service/reason text, ЛЮБОЕ из которых может прийти из
        # эвристики/исключения scip-python (произвольный текст, потенциально с "["/"]")
        # -- escape() ОБЯЗАН обернуть его целиком перед интерполяцией в markup-строку
        # ниже, иначе "[...]"-подстрока либо валит Console.print MarkupError, либо
        # молча съедается как (невалидный) style-тег (live-verified).
        console.print(f"[yellow]degraded services: {escape(detail)}[/]")

    # M4 T7 (additive): services --incremental forced back to a full re-analyze for a
    # NON-degraded reason (fingerprint mismatch / first run -- see cli.py's own
    # orchestration) -- excludes degraded=True services on purpose, even though a
    # degraded service also always carries mode="full" with a non-None reason: that
    # reason already has its own dedicated yellow block immediately above, and this
    # line would otherwise duplicate it verbatim for every degraded service.
    fallback_services = [
        s for s in report["services"]
        if s.get("mode") == "full" and s.get("reason") and not s.get("degraded")
    ]
    if fallback_services:
        fallback_detail = ", ".join(
            f"{s.get('service')} ({s['reason']})" for s in fallback_services
        )
        # Same live-verified markup hazard as the degraded-services block above --
        # `reason` here is CLI-authored ("first run"/"fingerprint mismatch") today,
        # but nothing prevents it from carrying a degraded-style reason string on a
        # future path, so it gets the identical escape() treatment defensively.
        console.print(f"[cyan]incremental fallback to full: {escape(fallback_detail)}[/]")

    # M2 T7 (additive): "linking" key only present when build_report was given
    # link_stats -- absent for every pre-T7 report shape, so .get() + a truthy guard
    # keeps this a strict no-op (no new output, no KeyError) for those callers.
    linking = report.get("linking")
    if linking:
        console.print(
            f"linking: channels calls_http = {linking.get('calls_http', 0)} "
            f"(unresolved={linking.get('calls_http_unresolved', 0)}), "
            f"next_segments = {linking.get('next_segments', 0)}, "
            f"processes = {linking.get('processes', 0)}, "
            f"channels_gc = {linking.get('channels_gc', 0)}, "
            # M8 T1 (rerun-2 R4): router_prefix.link's own honest-miss counter --
            # a route whose cross-file include_router chain didn't fully compose
            # (unresolvable router_symbol, a cycle, or an unresolvable/ambiguous
            # hop -- see linking/router_prefix.py's own docstring), so it fell back
            # to its local-only template. Same "always present, 0 when nothing
            # failed" convention as calls_http_unresolved just above.
            f"route_prefix_unresolved = {linking.get('route_prefix_unresolved', 0)}, "
            # M8 T2 (rerun-2 R5): linking.signal_send.link's own honest-miss counter
            # -- a temporal_signal_send claim (temporal_ext.py's typed-sender arg0
            # resolution, `handle.signal(Cls.method, ...)`) whose own method_symbol
            # resolved but names no method with a live CONSUMES edge into a
            # temporal_signal channel -- see linking/signal_send.py's own docstring.
            # Same "always present, 0-defaulted" convention as route_prefix_unresolved.
            f"signal_send_unlinked = {linking.get('signal_send_unlinked', 0)}"
        )

    # M3 T6 (additive, same "absent -> strict no-op" contract as linking above):
    # "chunking" key only present when build_report was given chunk_stats.
    #
    # M4 T1: the embedded count is broken down into fresh (genuine provider calls)
    # vs. cached (persistent embedding_cache reuses, zero cost) -- .get(..., 0)
    # defaults keep this a no-op shape change for a pre-M4 chunk_stats dict that
    # happens not to carry these two keys (same defensive convention as every other
    # field read here).
    chunking = report.get("chunking")
    if chunking:
        line = (
            f"chunking: chunks = {chunking.get('chunks_total', 0)} "
            f"(embedded: {chunking.get('embedded_fresh', 0)} fresh + "
            f"{chunking.get('embedded_from_cache', 0)} cached, "
            f"reused: {chunking.get('reused', 0)})"
        )
        skipped = chunking.get("skipped_no_embedder", 0)
        if skipped:
            line += f" [yellow](skipped_no_embedder = {skipped})[/]"
        console.print(line)
