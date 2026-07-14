"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from codegraph.config.loader import ConfigError, effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.doctor import run_env_checks, run_store_probes
from codegraph.mcp.server import build_server
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.load import load_graph
from codegraph.pipeline.report import build_report, print_report, write_report
from codegraph.pipeline.stages import STAGES
from codegraph.stores.falkordb.connection import StoreError, StoreUnavailable
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

# analyze_service/load_graph/FalkorStore/Staging/build_server импортированы по имени
# (не через module-алиас) НАМЕРЕННО: юнит-тесты (tests/unit/test_cli_m1b.py)
# monkeypatch'ат ровно эти module-level имена (`codegraph.cli.analyze_service` и т.д.),
# подставляя фейки вместо реального SCIP/FalkorDB/MCP -- сработает только если имя
# резолвится из ГЛОБАЛЬНОГО namespace codegraph.cli на момент вызова, а не из
# локального импорта внутри тела команды (см. существующий паттерн лениво
# импортируемого `connect` в doctor() -- та же техника здесь была бы непатчибельной).
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
        # escape(): ConfigError-текст может содержать необработанное сообщение
        # pydantic ValidationError (`str(e)` включает "[type=missing, ...]"-подобные
        # фрагменты) -- без escape() такая "["-подстрока либо валит Console.print
        # MarkupError, либо молча съедается как (невалидный) style-тег (live-verified).
        console.print(f"[red]config error:[/] {escape(str(e))}")
        raise typer.Exit(1) from e


def _workspace_dir(cfg: WorkspaceConfig, target: Path) -> Path:
    """Каталог для `.codegraph/` (staging.db, scip-кэш, report.json): та же
    директория, что config.loader.load_workspace использует как базу для
    относительных путей сервисов -- рядом с codegraph.yaml, если target на него
    указывает (напрямую или через каталог, где он лежит), иначе (zero-config)
    корень индексируемого репозитория. cfg сейчас не читается (нет config-поля,
    переопределяющего расположение workspace), параметр зарезервирован под
    контракт из брифа m1b-task-6 и возможное будущее переопределение."""
    target = target.resolve()
    return target if target.is_dir() else target.parent


def _resolve_graph_name(cfg: WorkspaceConfig, graph: str | None) -> str:
    return graph if graph is not None else cfg.graph_name


def _store_guard(fn):
    """Единая граница store-недоступности для команд, трогающих FalkorDB
    (index/load: load_graph; stats: graph_exists/stats): недостижимый инстанс ->
    красный однострочник + exit 1. Except УЗКИЙ -- ровно (StoreError=redis.
    RedisError, StoreUnavailable): любое другое исключение -- настоящий баг и
    обязано падать traceback'ом, а не маскироваться дружелюбным сообщением."""
    try:
        return fn()
    except (StoreError, StoreUnavailable) as e:
        # escape(): redis/StoreUnavailable-текст -- сообщение из внешней библиотеки,
        # не наш контролируемый литерал -- та же live-verified markup-ловушка, что и
        # в _load выше (bracketed substring -> MarkupError либо тихая потеря текста).
        console.print(f"[red]falkordb unreachable:[/] {escape(str(e))}")
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
    graph: str | None = typer.Option(None, "--graph"),
) -> None:
    """Построить граф workspace: scan → resolve → extract → join → load → report
    (`--dry-run` — только план пайплайна, без записи)."""
    # Path.cwd() читается здесь, а не в default параметра: default-выражения
    # typer вычисляются один раз при импорте модуля, а не при каждом вызове
    # (см. комментарий в doctor).
    target_path = target if target is not None else Path.cwd()
    cfg = _load(target_path)
    graph_name = _resolve_graph_name(cfg, graph)

    if dry_run:
        stage_table = Table(title=f"pipeline plan · graph={graph_name}")
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
        return

    # полный прогон: S1–S6 (analyze_service, per service) → S9 (load_graph,
    # blue/green) → S10 (report). Деградация отдельных сервисов (SCIP недоступен →
    # эвристический fallback) НЕ валит exit — print_report печатает жёлтый блок,
    # но код возврата остаётся 0 (см. self-review брифа m1b-task-6).
    codegraph_dir = _workspace_dir(cfg, target_path) / ".codegraph"
    # active_idioms (M2 T4): включает доменные экстракторы S5 (сейчас — fastapi/temporal)
    # по workspace-списку builtin-идиом; cfg.builtin_idioms уже провалидирован
    # load_workspace (resolve_builtins), незнакомое имя сюда не доедет.
    active_idioms = frozenset(cfg.builtin_idioms)
    with Staging(codegraph_dir / "staging.db") as staging:
        per_service = [
            analyze_service(
                svc, staging, codegraph_dir / "scip",
                active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
            )
            for svc in cfg.services
        ]
        load_stats = _store_guard(lambda: load_graph(
            staging, lambda name: FalkorStore(cfg.storage.falkordb, name), graph_name
        ))

    report = build_report(per_service, load_stats)
    write_report(report, codegraph_dir / "report.json")
    print_report(report, console)


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
def stats(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    graph: str | None = typer.Option(None, "--graph"),
) -> None:
    """Статистика графа: узлы по kind, рёбра по type (FalkorStore.stats())."""
    cfg = _load(target if target is not None else Path.cwd())
    graph_name = _resolve_graph_name(cfg, graph)
    store = FalkorStore(cfg.storage.falkordb, graph_name)
    # Существование проверяется ДО первого GRAPH.QUERY: (a) «не индексировали вовсе»
    # отличается от «индексировали, но граф пуст» (пустой существующий -- честные
    # нули ниже, exit 0); (b) сам stats()-запрос на несуществующем имени auto-vivify'ил
    # бы пустой граф-ключ как побочный эффект (см. FalkorStore.graph_exists docstring).
    if not _store_guard(store.graph_exists):
        console.print(
            f"[red]graph {graph_name!r} not found — run 'codegraph index' first[/]"
        )
        raise typer.Exit(1)
    data = _store_guard(store.stats)

    nodes_table = Table(title=f"nodes by kind · graph={graph_name}")
    nodes_table.add_column("kind")
    nodes_table.add_column("count", justify="right")
    for kind, count in sorted(data.get("nodes", {}).items()):
        nodes_table.add_row(kind, str(count))
    console.print(nodes_table)

    edges_table = Table(title="edges by type")
    edges_table.add_column("type")
    edges_table.add_column("count", justify="right")
    for edge_type, count in sorted(data.get("edges", {}).items()):
        edges_table.add_row(edge_type, str(count))
    console.print(edges_table)


@app.command()
def load(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    graph: str | None = typer.Option(None, "--graph"),
) -> None:
    """Загрузить существующий staging (SQLite, из предыдущего `index`) в FalkorDB
    (blue/green), без повторного анализа."""
    target_path = target if target is not None else Path.cwd()
    cfg = _load(target_path)
    staging_path = _workspace_dir(cfg, target_path) / ".codegraph" / "staging.db"
    if not staging_path.exists():
        console.print(
            f"[red]no staging DB at {staging_path}; run 'codegraph index' first[/]"
        )
        raise typer.Exit(1)

    graph_name = _resolve_graph_name(cfg, graph)
    with Staging(staging_path) as staging:
        load_stats = _store_guard(lambda: load_graph(
            staging, lambda name: FalkorStore(cfg.storage.falkordb, name), graph_name
        ))

    table = Table(title=f"load · graph={graph_name}")
    table.add_column("nodes_written")
    table.add_column("edges_written")
    table.add_column("edges_dropped")
    table.add_row(
        str(load_stats.get("nodes_written", 0)),
        str(load_stats.get("edges_written", 0)),
        str(load_stats.get("edges_dropped_missing_endpoint", 0)),
    )
    console.print(table)


@app.command()
def trace() -> None:
    """Трассировка бизнес-процесса (M2)."""
    _stub("M2")


@app.command()
def serve(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    graph: str | None = typer.Option(None, "--graph"),
) -> None:
    """MCP-сервер (stdio, M1 v0): graph_stats/get_source/expand_neighbors/who_calls."""
    target_path = target if target is not None else Path.cwd()
    cfg = _load(target_path)
    graph_name = _resolve_graph_name(cfg, graph)

    # graph_exists() идёт через _store_guard как и в stats() -- недостижимость самого
    # FalkorDB остаётся красной границей exit 1 (единый контракт со всеми
    # store-командами). Но САМ факт "граф ещё не существует" -- НЕ ошибка здесь (в
    # отличие от stats): index может отработать позже, пока MCP-сервер уже поднят и
    # ждёт запросов (store_factory в build_server пересоздаёт FalkorStore на каждый
    # tool-call -- см. query.api.GraphQuery докстринг), поэтому это жёлтое
    # предупреждение, а не отказ стартовать.
    store = FalkorStore(cfg.storage.falkordb, graph_name)
    if not _store_guard(store.graph_exists):
        console.print(
            f"[yellow]graph {graph_name!r} not found yet -- run 'codegraph index' "
            "whenever ready; server is starting anyway[/]"
        )
    build_server(cfg, graph_name).run()


@app.command()
def eval() -> None:
    """Оценка качества графа/retrieval (M2)."""
    _stub("M2")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
