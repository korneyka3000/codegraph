"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from codegraph.config.loader import ConfigError, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.doctor import run_env_checks, run_store_probes
from codegraph.pipeline.stages import STAGES

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


@app.callback()
def _callback() -> None:
    """codegraph: граф знаний кода для Python-микросервисов (CLI-индексатор + MCP-сервер)."""
    # Пустой callback нужен, чтобы Typer не схлопывал единственную команду
    # (doctor) в безымянную корневую — иначе `codegraph doctor` не работает,
    # т.к. Typer/Click при ровно одной команде и отсутствии callback
    # регистрирует её напрямую как корневую команду (см. typer.main.get_command).
    # Остаётся навсегда: без него Typer схлопывает единственную команду в
    # безымянную корневую (см. выше), а докстринг здесь — это текст `--help`
    # всего приложения.


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


def _load(target: Path) -> WorkspaceConfig:
    try:
        return load_workspace(target)
    except ConfigError as e:
        console.print(f"[red]config error:[/] {e}")
        raise typer.Exit(1) from e


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
    cfg = _load(config if config is not None else Path.cwd())
    ok = _render(run_env_checks(cfg.scip, probe_scip=probe_scip), "environment")
    if not skip_store:
        from codegraph.stores.falkordb.connection import connect

        ok &= _render(
            run_store_probes(lambda: connect(cfg.storage.falkordb)),
            f"falkordb {cfg.storage.falkordb.host}:{cfg.storage.falkordb.port}",
        )
    raise typer.Exit(0 if ok else 1)


TEMPLATE = Path(__file__).parent.parent.parent / "codegraph.example.yaml"


@app.command()
def index(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Построить граф workspace (M0: только --dry-run)."""
    # Path.cwd() читается здесь, а не в default параметра: default-выражения
    # typer вычисляются один раз при импорте модуля, а не при каждом вызове
    # (см. комментарий в doctor).
    cfg = _load(target if target is not None else Path.cwd())
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
def init(target: Path | None = typer.Argument(None)) -> None:  # noqa: B008 -- typer marker call, idiomatic
    """Создать codegraph.yaml из прокомментированного шаблона."""
    # Path.cwd() — в теле функции, не в default (см. комментарий в doctor).
    dest = (target if target is not None else Path.cwd()) / "codegraph.yaml"
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
