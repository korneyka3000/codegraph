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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
