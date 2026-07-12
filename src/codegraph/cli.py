"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from codegraph.config.loader import load_workspace
from codegraph.doctor import run_env_checks, run_store_probes

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


@app.callback()
def _callback() -> None:
    """codegraph: граф знаний кода для Python-микросервисов (CLI-индексатор + MCP-сервер)."""
    # Пустой callback нужен, чтобы Typer не схлопывал единственную команду
    # (doctor) в безымянную корневую — иначе `codegraph doctor` не работает,
    # т.к. Typer/Click при ровно одной команде и отсутствии callback
    # регистрирует её напрямую как корневую команду (см. typer.main.get_command).
    # Уйдёт само по себе, когда появятся index/load/stats/... (Task 8-10).


def _render(results, title: str) -> bool:
    table = Table(title=title)
    table.add_column("check")
    table.add_column("status")
    table.add_column("detail")
    all_ok = True
    for r in results:
        all_ok &= r.ok
        table.add_row(r.name, "[green]OK[/]" if r.ok else "[red]FAIL[/]", r.detail)
    console.print(table)
    return all_ok


@app.command()
def doctor(
    config: Path | None = typer.Option(None, "--config", "-c"),  # noqa: B008 -- typer marker call, idiomatic
    probe_scip: bool = typer.Option(False, "--probe-scip"),
    skip_store: bool = typer.Option(False, "--skip-store"),
) -> None:
    """Проверить окружение (python/node/scip-python) и возможности FalkorDB."""
    # Path.cwd() читается здесь, а не в default параметра: default-выражения
    # typer вычисляются один раз при импорте модуля (ruff B008), а не при
    # каждом вызове команды.
    cfg = load_workspace(config if config is not None else Path.cwd())
    ok = _render(run_env_checks(cfg.scip, probe_scip=probe_scip), "environment")
    if not skip_store:
        from codegraph.stores.falkordb.connection import connect

        ok &= _render(
            run_store_probes(lambda: connect(cfg.storage.falkordb)),
            f"falkordb {cfg.storage.falkordb.host}:{cfg.storage.falkordb.port}",
        )
    raise typer.Exit(0 if ok else 1)


STAGES = [
    ("S1", "discover", "конфиг / zero-config, валидация путей"),
    ("S2", "scan", "обход .py, sha256"),
    ("S3", "resolve", "scip-python per service"),
    ("S4", "read-scip", "protobuf → defs/refs"),
    ("S5", "parse+extract", "tree-sitter, идиомы → claims"),
    ("S6", "join", "SCIP refs × call-sites → CALLS"),
    ("S7", "link", "каналы, роуты, NEXT_SEGMENT, процессы"),
    ("S8", "chunk+embed", "AST-чанки + эмбеддинги"),
    ("S9", "load", "UNWIND-батчи → FalkorDB (blue/green)"),
    ("S10", "report", "качество графа"),
]

TEMPLATE = Path(__file__).parent.parent.parent / "codegraph.example.yaml"


@app.command()
def index(
    target: Path = typer.Argument(Path.cwd()),  # noqa: B008 -- typer marker call, idiomatic
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Построить граф workspace (M0: только --dry-run)."""
    cfg = load_workspace(target)
    if not dry_run:
        console.print("[yellow]index is not implemented until M1; use --dry-run[/]")
        raise typer.Exit(2)
    from codegraph.config.loader import effective_idioms

    stage_table = Table(title=f"pipeline plan · graph={cfg.graph_name}")
    stage_table.add_column("stage")
    stage_table.add_column("name")
    stage_table.add_column("what")
    for sid, name, what in STAGES:
        stage_table.add_row(sid, name, what)
    console.print(stage_table)

    svc_table = Table(title="services")
    for col in ("service", "path", "producers", "consumers", "http_clients"):
        svc_table.add_column(col)
    for svc in cfg.services:
        idioms = effective_idioms(cfg, svc)
        svc_table.add_row(
            svc.name, str(svc.path),
            str(len(idioms.producers)), str(len(idioms.consumers)),
            str(len(idioms.http_clients)),
        )
    console.print(svc_table)


@app.command()
def init(target: Path = typer.Argument(Path.cwd())) -> None:  # noqa: B008 -- typer marker call, idiomatic
    """Создать codegraph.yaml из прокомментированного шаблона."""
    dest = target / "codegraph.yaml"
    if dest.exists():
        console.print(f"[red]{dest} already exists[/]")
        raise typer.Exit(1)
    dest.write_text(TEMPLATE.read_text())
    console.print(f"created {dest}")


def _stub(milestone: str) -> None:
    console.print(f"[yellow]planned for {milestone}[/]")
    raise typer.Exit(2)


@app.command()
def stats() -> None:
    """Статистика графа (M1)."""
    _stub("M1")


@app.command()
def load() -> None:
    """Загрузка в FalkorDB из staging (M1)."""
    _stub("M1")


@app.command()
def trace() -> None:
    """Трассировка бизнес-процесса (M2)."""
    _stub("M2")


@app.command()
def serve() -> None:
    """MCP-сервер (M1: v0)."""
    _stub("M1")


@app.command()
def eval() -> None:
    """Оценка качества графа/retrieval (M2)."""
    _stub("M2")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
