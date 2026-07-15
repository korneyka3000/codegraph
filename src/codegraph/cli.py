"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

import warnings
from pathlib import Path

import typer
from authlib.deprecate import AuthlibDeprecationWarning
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from rich.tree import Tree

from codegraph.config.loader import ConfigError, effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.core.errors import CodegraphError
from codegraph.doctor import run_env_checks, run_store_probes
from codegraph.embedding.factory import make_embedder
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.load import load_graph
from codegraph.pipeline.report import build_report, print_report, write_report
from codegraph.pipeline.stages import STAGES
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.connection import StoreError, StoreUnavailable
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

# `fastmcp` (imported below via codegraph.mcp.server, needed for `serve`) transitively
# imports `fastmcp.server.auth.providers.jwt`, whose own top-level code does
# `from authlib.jose import ...` -- and authlib.jose's OWN top-level code raises
# AuthlibDeprecationWarning (authlib 1.7.2: "authlib.jose module is deprecated, please
# use joserfc instead") on that very import, so every `codegraph` invocation printed it
# on stderr, unconditionally, before this fix (empirically confirmed: `codegraph doctor
# --help` alone triggers it, since this module always imports mcp.server at load time).
#
# A filter registered BEFORE this point does NOT work here, verified directly: authlib's
# OWN `authlib/deprecate.py` runs `warnings.simplefilter("always", AuthlibDeprecationWarning)`
# at ITS OWN import time (a forced, unconditional override for its own warning class) --
# since `warnings.filterwarnings`/`simplefilter` both PREPEND to `warnings.filters` by
# default (most-recently-registered checked FIRST), whichever filter is registered LAST
# wins. Importing `authlib.deprecate` ourselves, above (harmless on its own -- it does
# NOT import authlib.jose or raise anything by itself), runs that override first; our
# own "ignore" filter, registered immediately below -- and therefore chronologically
# LAST -- takes priority over it. Placing this in `main()` would be too late: every
# import in this module (including the one below) already ran once, at module-import
# time, before main() is ever invoked (this IS the "app-инициализации" location, not
# main() -- see this task's own fix report for the two candidate locations named and
# why only this one actually works).
warnings.filterwarnings("ignore", category=AuthlibDeprecationWarning)

from codegraph.mcp.server import build_server  # noqa: E402

# analyze_service/link_workspace/load_graph/FalkorStore/Staging/build_server
# импортированы по имени (не через module-алиас) НАМЕРЕННО: юнит-тесты
# (tests/unit/test_cli_m1b.py) monkeypatch'ат ровно эти module-level имена
# (`codegraph.cli.analyze_service` и т.д.), подставляя фейки вместо реального
# SCIP/FalkorDB/MCP -- сработает только если имя резолвится из ГЛОБАЛЬНОГО namespace
# codegraph.cli на момент вызова, а не из локального импорта внутри тела команды (см.
# существующий паттерн лениво импортируемого `connect` в doctor() -- та же техника
# здесь была бы непатчибельной).
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


def _require_staging(cfg: WorkspaceConfig, target_path: Path) -> Path:
    """staging.db path for `load` -- needs a PRIOR `codegraph index` run (staging
    persists on disk after index completes; `load` re-derives the graph from it).
    Missing file -> red one-liner + exit 1.

    M3 T2: `trace` USED to be this function's other caller (it re-derived an
    entrypoint id from staging the same way `load` re-derives the graph) -- no
    longer: selector resolution moved to query.api.GraphQuery.resolve_selector
    (graph-side, see cli.py's `trace` command), closing the M2 final review
    carry-item that `codegraph trace` hard-required a staging.db on disk purely to
    resolve a selector string even though the trace walk itself was always
    graph-only."""
    path = _workspace_dir(cfg, target_path) / ".codegraph" / "staging.db"
    if not path.exists():
        console.print(f"[red]no staging DB at {path}; run 'codegraph index' first[/]")
        raise typer.Exit(1)
    return path


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


def _make_embedder_or_warn(cfg: WorkspaceConfig):
    """Lazy, graceful S8 embedder construction (M3 T6). `embedding.factory.
    make_embedder` raises `CodegraphError` (with an actionable hint baked into its own
    message -- "uv sync --extra local-emb", "OPENAI_API_KEY not set", ...) for every
    "can't build an embedder right now" case it knows about: provider=local's
    sentence-transformers extra not installed; provider=openai/voyage missing its API
    key or SDK package. Any of those degrades S8 to "chunk without embedding"
    (`run_chunk_embed(..., embedder=None)` still builds Chunk nodes + headers, just
    skips the embed step -- see its own report's `skipped_no_embedder` counter) rather
    than failing the whole `codegraph index` run -- mirrors this CLI's other
    zero-config-friendly degradation path (`analyze_service`'s own SCIP-unavailable
    fallback). Only ever called when the user did NOT pass `--no-embed` -- that flag's
    own skip is deliberate, not a degradation, and prints no warning (see `index`)."""
    try:
        return make_embedder(cfg.embedding)
    except CodegraphError as e:
        console.print(f"[yellow]S8: embeddings skipped ({escape(str(e))})[/]")
        return None


@app.command()
def index(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    dry_run: bool = typer.Option(False, "--dry-run"),
    graph: str | None = typer.Option(None, "--graph"),
    no_embed: bool = typer.Option(False, "--no-embed"),
) -> None:
    """Построить граф workspace: scan → resolve → extract → join → chunk+embed → load →
    report (`--dry-run` — только план пайплайна, без записи; `--no-embed` — пропустить
    S8 chunk+embed целиком)."""
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

    # полный прогон: S1–S6 (analyze_service, per service) → S7 (link_workspace,
    # cross-service derivation) → S8 (chunk_embed, chunk+augment+embed) → S9
    # (load_graph, blue/green) → S10 (report). Деградация отдельных сервисов (SCIP
    # недоступен → эвристический fallback) НЕ валит exit — print_report печатает
    # жёлтый блок, но код возврата остаётся 0 (см. self-review брифа m1b-task-6);
    # S8's own embedder-construction degradation (см. _make_embedder_or_warn) follows
    # the identical zero-config-friendly contract.
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
        # M2 T7: link_workspace ПОСЛЕ цикла analyze (нужны staged каналы/claims ВСЕХ
        # сервисов) и ДО load_graph (S9 должен снапшотить staging уже вместе с
        # derived-слоем — NEXT_SEGMENT/CALLS_HTTP/PART_OF_PROCESS). staging-only --
        # FalkorDB не трогает, поэтому без _store_guard (в отличие от load_graph ниже).
        link_report = link_workspace(cfg, staging)
        # M3 T6: S8 между link_workspace и load_graph — augmentation-заголовки читают
        # graph-позицию из staged рёбер (включая S7-derived), а load_graph должен
        # снапшотить staging уже вместе с чанками/эмбеддингами. --no-embed — явный,
        # тихий (без предупреждения) пропуск; иначе — ленивая, деградирующая сборка
        # эмбеддера (см. _make_embedder_or_warn).
        embedder = None if no_embed else _make_embedder_or_warn(cfg)
        chunk_report = run_chunk_embed(cfg, staging, embedder)
        load_stats = _store_guard(lambda: load_graph(
            staging, lambda name: FalkorStore(cfg.storage.falkordb, name), graph_name
        ))

    report = build_report(per_service, load_stats, link_report, chunk_report)
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
    staging_path = _require_staging(cfg, target_path)

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


_TRACE_FORMATS = ("text", "mermaid")


def _node_label(node: dict) -> str:
    """qualified_name preferred over name (more specific -- e.g. "app.routes.
    create_order" vs "create_order") UNLESS it's just a repeat of id: Channel
    nodes set qualified_name == id verbatim (core.schema.make_channel_node --
    channels have no nested structure, id already uniquely identifies them), so
    for a Channel showing qualified_name would mean showing the raw
    "chan:event_type:OrderCreated" id instead of the friendly "OrderCreated"
    name -- live-verified against a real trace (see tests/unit/test_cli_trace.py)."""
    qualified = node.get("qualified_name")
    if qualified and qualified != node.get("id"):
        return qualified
    return node.get("name") or node.get("id") or "?"


def _trace_tree(result: dict) -> Tree:
    """rich-дерево: корень -- итоговая confidence/truncated; один узел на сегмент
    (роли entry-узла, если есть -- см. pipeline/load._node_props/roles-в-props
    фикс, M2 T8), под ним плоский список steps (edge_type -> node) и exits
    (channel -> next_entry_ids), без вложенной по call-графу структуры -- steps
    сам по себе плоский список (см. query/traverse.py: без "from"-поля, шаг --
    просто (edge_type, props, node, direction))."""
    conf = result.get("confidence", 1.0)
    title = f"trace (confidence={conf:.2f})"
    if result.get("truncated"):
        title += " [yellow](truncated)[/]"
    root = Tree(title)
    for i, seg in enumerate(result.get("segments", [])):
        entry = seg.get("entry", {})
        roles = entry.get("roles") or []
        role_prefix = f"({escape('/'.join(roles))}) " if roles else ""
        seg_line = (
            f"S{i} {escape(str(seg.get('service', '')))}: "
            f"{role_prefix}{escape(_node_label(entry))}"
        )
        if seg.get("truncated"):
            seg_line += " [yellow](truncated)[/]"
        seg_node = root.add(seg_line)
        for step in seg.get("steps", []):
            seg_node.add(
                f"{escape(str(step.get('edge_type', '')))} -> "
                f"{escape(_node_label(step.get('node', {})))}"
            )
        for ex in seg.get("exits", []):
            chan_label = escape(_node_label(ex.get("channel", {})))
            next_ids = ex.get("next_entry_ids") or []
            dest = escape(", ".join(next_ids)) if next_ids else "unresolved"
            seg_node.add(f"channel {chan_label} -> {dest}")
    return root


def _trace_mermaid(result: dict) -> str:
    """flowchart TD: один узел на сегмент (`S{i}["service: entry"]`), одна стрелка
    на next_entry_id (`S{i} -->|channel| S{j}`) -- next_entry_ids, не указывающие
    ни на один сегмента этого трейса (dangling/за пределами max_segments),
    молча пропускаются (тот сегмент просто не появится как узел стрелки).
    `"`/`|` внутри меток заменяются -- эти символы ломают mermaid-синтаксис узла/
    метки ребра; смоук-уровень экранирования (валиден для рендера, не
    исчерпывающий)."""
    segments = result.get("segments", [])
    entry_to_index = {
        seg["entry"]["id"]: i
        for i, seg in enumerate(segments)
        if seg.get("entry", {}).get("id") is not None
    }
    lines = ["flowchart TD"]
    for i, seg in enumerate(segments):
        label = f"{seg.get('service', '')}: {_node_label(seg.get('entry', {}))}"
        lines.append(f'    S{i}["{label.replace(chr(34), chr(39))}"]')
    for i, seg in enumerate(segments):
        for ex in seg.get("exits", []):
            chan_label = _node_label(ex.get("channel", {})).replace("|", "/").replace('"', "'")
            for next_id in ex.get("next_entry_ids", []):
                j = entry_to_index.get(next_id)
                if j is not None:
                    lines.append(f"    S{i} -->|{chan_label}| S{j}")
    return "\n".join(lines)


@app.command()
def trace(
    selector: str = typer.Argument(...),  # noqa: B008 -- typer marker call, idiomatic
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    graph: str | None = typer.Option(None, "--graph"),
    output_format: str = typer.Option("text", "--format"),
) -> None:
    """Трассировка бизнес-процесса от selector (route-форма "<service>:<METHOD>
    <path>" или qualified "<service>:<dotted.name>", как в cfg.processes -- см.
    core.selectors.parse_selector) через FalkorDB: rich-дерево сегментов (--format
    text, по умолчанию) или mermaid flowchart (--format mermaid).

    M3 T2: selector резолвится ПРЯМО из графа (query.api.GraphQuery.resolve_selector),
    staging.db НЕ читается и не обязан существовать -- закрывает M2-final-review
    carry-item (trace раньше требовал staging.db исключительно ради резолва
    selector'а, хотя сам трейс всегда был graph-only)."""
    if output_format not in _TRACE_FORMATS:
        console.print(
            f"[red]invalid --format: {escape(output_format)!r} (expected "
            f"{'|'.join(_TRACE_FORMATS)})[/]"
        )
        raise typer.Exit(1)

    target_path = target if target is not None else Path.cwd()
    cfg = _load(target_path)
    graph_name = _resolve_graph_name(cfg, graph)

    service_paths = {svc.name: svc.path for svc in cfg.services}
    gq = GraphQuery(
        store_factory=lambda: FalkorStore(cfg.storage.falkordb, graph_name),
        service_paths=service_paths,
    )
    # GraphQuery отвечает своим собственным error-dict-контрактом (store
    # недоступен/selector не резолвится/entrypoint не найден в графе) -- без
    # _store_guard, как и в MCP server.py: тут нечего перехватывать, только
    # проверить ключ "error".
    resolved = gq.resolve_selector(selector)
    if "error" in resolved:
        console.print(f"[red]{escape(resolved['error'])}[/]")
        raise typer.Exit(1)

    result = gq.trace_process(resolved["node_id"])
    if "error" in result:
        console.print(f"[red]{escape(result['error'])}[/]")
        raise typer.Exit(1)

    if output_format == "mermaid":
        # markup=False: mermaid-синтаксис -- сплошные "[...]"/"|...|", которые
        # rich иначе распарсил бы как markup-теги (тот же класс бага, что escape()
        # предотвращает у остальных команд -- здесь весь вывод не наш текст с
        # вкраплениями данных, а plain-text формат целиком, проще выключить
        # разметку для этого print целиком, чем экранировать каждую скобку).
        console.print(_trace_mermaid(result), markup=False)
    else:
        console.print(_trace_tree(result))


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
