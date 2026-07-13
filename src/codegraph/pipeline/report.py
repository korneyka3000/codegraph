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
from rich.table import Table

_TOTAL_FIELDS = ("files", "nodes", "edges", "calls_joined", "calls_unresolved", "calls_external")


def _pct(numerator: int, denominator: int) -> float:
    return 100.0 * numerator / denominator if denominator else 0.0


def build_report(per_service: list[dict], load_stats: dict) -> dict:
    """per_service: список dict'ов analyze_service()-report (ключ "service" включён).
    load_stats: возврат load.load_graph() (nodes_written/edges_written/
    edges_dropped_missing_endpoint + by-type/by-label разбивки).

    Возврат -- JSON-сериализуемый dict: {"services", "totals", "load", "health"}.
    "health": pct_unresolved_calls = unresolved/(joined+unresolved+external) * 100
    (0.0 при нулевом знаменателе -- сервисы без единого call-сайта, не делить на 0),
    dropped_edges(+by_type) из load_stats, degraded_services -- список {service, reason}
    для сервисов с degraded=True (эвристический fallback вместо SCIP, см. analyze.py).
    """
    totals = {field: sum(s.get(field, 0) for s in per_service) for field in _TOTAL_FIELDS}
    denom = totals["calls_joined"] + totals["calls_unresolved"] + totals["calls_external"]
    degraded_services = [
        {"service": s.get("service"), "reason": s.get("reason")}
        for s in per_service
        if s.get("degraded")
    ]
    return {
        "services": list(per_service),
        "totals": totals,
        "load": dict(load_stats),
        "health": {
            "pct_unresolved_calls": _pct(totals["calls_unresolved"], denom),
            "dropped_edges": load_stats.get("edges_dropped_missing_endpoint", 0),
            "dropped_edges_by_type": dict(load_stats.get("edges_dropped_by_type", {})),
            "degraded_services": degraded_services,
        },
    }


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


def print_report(report: dict, console: Console) -> None:
    """rich-печать: таблица по сервисам (+ TOTAL-строка = "итого"), сводка load,
    веха здоровья (% unresolved calls, dropped edges), жёлтый блок degraded-сервисов
    если есть хоть один."""
    svc_table = Table(title="services")
    for _, header in _SERVICE_COLUMNS:
        svc_table.add_column(header)
    svc_table.add_column("degraded")

    for s in report["services"]:
        row = [str(s.get(key, 0)) for key, _ in _SERVICE_COLUMNS]
        degraded = bool(s.get("degraded"))
        row.append("[yellow]yes[/]" if degraded else "no")
        svc_table.add_row(*row)

    totals = report["totals"]
    total_row = ["[bold]TOTAL[/]"] + [
        f"[bold]{totals.get(key, 0)}[/]" for key, _ in _SERVICE_COLUMNS[1:]
    ]
    total_row.append("")
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
    console.print(
        f"health: unresolved calls = {health['pct_unresolved_calls']:.1f}%, "
        f"dropped edges = {health['dropped_edges']}{dropped_detail}"
    )

    degraded_services = health.get("degraded_services", [])
    if degraded_services:
        detail = ", ".join(
            f"{d['service']} ({d['reason']})" if d.get("reason") else str(d["service"])
            for d in degraded_services
        )
        console.print(f"[yellow]degraded services: {detail}[/]")
