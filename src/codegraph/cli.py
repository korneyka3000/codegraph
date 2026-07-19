"""CLI codegraph: index | load | doctor | stats | trace | serve | eval | init."""

from __future__ import annotations

import importlib.metadata
import importlib.resources
import warnings
from pathlib import Path

import typer
import yaml
from authlib.deprecate import AuthlibDeprecationWarning
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from rich.tree import Tree

from codegraph.config.loader import ConfigError, effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.core.errors import CodegraphError
from codegraph.doctor import check_chunk_vector_index, run_env_checks, run_store_probes
from codegraph.embedding.factory import make_embedder
from codegraph.evalx.retrieval_eval import load_questions, run_questions
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.diff import config_fingerprint, service_delta
from codegraph.pipeline.load import load_graph
from codegraph.pipeline.report import build_report, print_report, write_report
from codegraph.pipeline.scan import scan_service
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


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"codegraph {importlib.metadata.version('codegraph')}")
        raise typer.Exit()


@app.callback()
def _callback(
    version: bool = typer.Option(  # noqa: B008 -- typer marker call, idiomatic
        False, "--version", callback=_version_callback, is_eager=True,
        help="Показать версию codegraph и выйти.",
    ),
) -> None:
    """codegraph: граф знаний кода для Python-микросервисов (CLI-индексатор + MCP-сервер)."""
    # Пустой callback нужен, чтобы Typer не схлопывал единственную команду
    # (doctor) в безымянную корневую — иначе `codegraph doctor` не работает,
    # т.к. Typer/Click при ровно одной команде и отсутствии callback
    # регистрирует её напрямую как корневую команду (см. typer.main.get_command).
    # Остаётся навсегда: без него Typer схлопывает единственную команду в
    # безымянную корневую (см. выше), а докстринг здесь — это текст `--help`
    # всего приложения.
    #
    # --version: eager option (`is_eager=True`) -- Typer/Click обрабатывает eager-
    # параметры ДО валидации остальных опций/аргументов, включая required SELECTOR
    # у `trace` -- поэтому `codegraph --version` (без под-команды) и `codegraph
    # --version trace` (под-команда указана, но её аргументы не провалидированы и
    # не важны) оба печатают версию и выходят, не долетая до тела под-команды. Сам
    # вывод и exit -- в _version_callback (Typer/Click own контракт для option-
    # callback: обычный return НЕ останавливает обработку — нужен explicit `raise
    # typer.Exit()`). Источник версии -- importlib.metadata (реально
    # УСТАНОВЛЕННЫЙ пакет), а не отдельная константа `codegraph.__version__` --
    # для wheel/editable-инсталляции это ровно то же значение, что покажет `uv pip
    # show codegraph`, и оно физически не может разъехаться с тем, что реально
    # установлено (в отличие от независимо поддерживаемой константы).


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
    graph: str | None = typer.Option(None, "--graph"),
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

        results = run_store_probes(lambda: connect(cfg.storage.falkordb))
        # M5 T7 (M3 backlog "no-index marker -> doctor probe"): one more row about the
        # TARGET workspace graph's OWN vector-index health (`--graph`, same override
        # convention as stats/load/index/trace/serve via `_resolve_graph_name`) --
        # appended to the SAME probes list/table, not a separate command-level
        # concern. Only attempted once every capability probe above is already green
        # (`all(r.ok for r in results)`): an unreachable/degraded FalkorDB is already
        # reported by "ping"/etc. above, and `check_chunk_vector_index` has no
        # try/except of its own -- it trusts a store it can already reach, the same
        # "connect once, then trust it" contract as this file's other post-connect
        # sequences (e.g. stats()'s own graph_exists()-then-stats()). A `None` result
        # (see that function's own docstring -- graph not indexed yet, no embedded
        # Chunk anywhere, or the index is already there) adds no row at all: only the
        # genuine "live embeddings, no covering index" gap is worth a line here.
        if all(r.ok for r in results):
            graph_name = _resolve_graph_name(cfg, graph)
            vector_check = check_chunk_vector_index(
                FalkorStore(cfg.storage.falkordb, graph_name)
            )
            if vector_check is not None:
                results = [*results, vector_check]
        ok &= _render(
            results, f"falkordb {cfg.storage.falkordb.host}:{cfg.storage.falkordb.port}",
        )
    raise typer.Exit(0 if ok else 1)


# M4 T2 (wheel-safe packaging): reads from the INSTALLED package's own data dir
# (src/codegraph/data/codegraph.example.yaml -- ships as package data, see
# pyproject.toml), not a repo-root-relative Path(__file__).parent.parent.parent --
# that old construction reached OUTSIDE the package entirely and raised
# FileNotFoundError from a real wheel install (no repo checkout sits beside
# site-packages/codegraph/). A byte-identical copy stays at the repo root for humans
# browsing the checkout (see tests/unit/test_cli.py's drift-guard test) -- the
# packaged copy above is the source of truth.
TEMPLATE = importlib.resources.files("codegraph") / "data" / "codegraph.example.yaml"


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


def _svc_fingerprint_key(name: str) -> str:
    return f"svc_fingerprint:{name}"


def _fp_to_persist(report: dict, fp: str) -> str:
    """The value `_analyze_services` should persist under `_svc_fingerprint_key` for a
    just-produced report: `fp` (the freshly computed `config_fingerprint`) for a clean
    report, or `""` for a DEGRADED one (M4 final review, IMPORTANT-1 -- scip
    unavailable this run, `analyze_service`/`_analyze_full` fell back to heuristic
    defs/refs/edges, resolution="heuristic"/confidence=0.6). Persisting the REAL `fp`
    for a degraded report would let a LATER `--incremental` run see fingerprint_ok=True
    together with an empty on-disk `service_delta` and take the "skipped" branch
    (`_skip_report` hardcodes `degraded=False` unconditionally) -- pinning those
    heuristic results forever, even once scip-python is repaired, and silently
    misreporting them as non-degraded from that point on. `""` can never equal a real
    fingerprint (`config_fingerprint` always returns a 64-char sha256 hex digest), so
    a later run's `stored_fp == fp` comparison always fails -- forcing "full" (via the
    "fingerprint mismatch"/"first run" reason logic below) and genuinely retrying scip,
    which is the only way a degraded service can ever self-heal back into a
    fingerprint-eligible state."""
    return "" if report.get("degraded") else fp


def _analyze_services(
    cfg: WorkspaceConfig,
    staging: Staging,
    cache_dir: Path,
    active_idioms: frozenset[str],
    incremental: bool,
) -> tuple[list[dict], dict[str, set[str]]]:
    """S1-S6 per-service orchestration for `index()`, M4 T7. `analyze_service` is
    called by its bare imported name (never `codegraph.pipeline.analyze.
    analyze_service`) for the same reason this whole module always does that -- see
    the module-level comment above the `mcp.server` import: tests/unit/test_cli_m1b.py
    and friends monkeypatch exactly `codegraph.cli.analyze_service`.

    `incremental=False` (`--incremental` absent, the default): every OTHER concern
    (per-service call shape/count/order, exceptions) is byte-identical to pre-M4 --
    `analyze_service` is called with the SAME two kwargs as always, nothing more. The
    ONE additive side effect (Global Constraint, M4 plan) is that the JUST-computed
    config fingerprint is written to staging AFTER each call (via `_fp_to_persist` --
    an empty `""` sentinel instead of the real fingerprint when the report came back
    degraded, M4 final review IMPORTANT-1, see that function's own docstring), so a
    LATER `--incremental` run has a real baseline instead of unconditionally treating
    every service as "first run" -- and a scip outage this run can never get pinned as
    a skip-eligible baseline. `changed_files` is always `{}` in this mode -- callers
    that care (only `index()`, gated on its own `incremental` flag) must pass `None`
    to `run_chunk_embed` themselves rather than trust this return value, since `{}`
    (a non-None, merely EMPTY dict) means something completely different to
    `run_chunk_embed` than `None` does (every service's WHOLE chunk loop skipped, vs.
    every service chunked in full -- see that function's own module docstring).

    `incremental=True`, per service:
      1. cheap `scan_service` (BEFORE analyze -- "скан+дифф до analyze" per the plan)
         + `service_delta` against `staging.files_for_service`'s PRIOR snapshot (still
         whatever the PREVIOUS run staged -- this call hasn't touched it yet).
      2. `fp_ok = (stored fingerprint == config_fingerprint(svc, idioms,
         active_idioms))`, ALWAYS computed fresh here -- never left at
         `analyze_service`'s own permissive `fingerprint_ok=True` default (binding
         carry-item from the T5 review: a caller that forgets this lets skip mode
         fire on a genuinely changed config). A never-before-seen service has no
         stored fingerprint at all (`get_meta` -> None), which simply compares
         unequal to any real fingerprint -- "first run" needs no separate branch.
      3. NOT fp_ok -> full, bypassing analyze_service's OWN incremental dispatch
         entirely (`incremental=False`, the plan's "fingerprint mismatch (или
         отсутствует) -> full" case) -- `report["reason"]` is only ever populated by
         analyze_service for a SCIP degradation, so this function fills it in itself
         ("first run" / "fingerprint mismatch"), but ONLY when analyze_service left
         it None: a config-stale service that ALSO degrades this same run keeps its
         real (more actionable) degraded reason untouched.
         fp_ok -> `incremental=True, prior_delta=delta, fingerprint_ok=True` straight
         through; analyze_service's own skip/incremental dispatch (T5) takes it from
         there (including the case where ITS OWN scip attempt degrades mid-flight and
         falls back to `mode="full"` on its own -- indistinguishable here from case 3
         above by the time this function reads `report["mode"]` back, and correctly
         handled identically either way, see point 5).
      4. Fingerprint written back UNLESS `report["mode"] == "skipped"` -- a skip
         means the stored fingerprint already matches (fp_ok was True and nothing
         changed) by construction, so re-writing it would be a no-op; skipped purely
         to keep the skip path's "ZERO staging writes" contract honest end to end,
         not for correctness. The value written is `_fp_to_persist(report, fp)`, not
         unconditionally `fp` -- `_skip_report` hardcodes `degraded=False` so
         "skipped" itself can never carry a degraded report, but "full" CAN (either
         half of case 3 above: its own "NOT fp_ok" branch, or its "fp_ok" branch's
         OWN scip-degrades-mid-flight fallback to mode="full"): writing the real `fp`
         for THAT degraded report would let the NEXT --incremental run see
         fingerprint_ok=True plus an empty delta and take the skip branch, pinning
         heuristic (resolution="heuristic", confidence=0.6) results forever even once
         scip recovers (M4 final review, IMPORTANT-1). `_fp_to_persist` writes `""`
         instead for a degraded report -- never equal to a real fingerprint, so the
         next run's `fp_ok` always fails, forcing "full" (retrying scip) until a
         clean run finally succeeds.
      5. `changed_files[svc.name]`, keyed off `report["mode"]` (regardless of which
         branch above produced it): "skipped" -> key absent entirely (not an empty
         set -- see `run_chunk_embed`'s own module docstring: an ABSENT key skips its
         per-file loop outright, correct since nothing needs re-chunking). "full" ->
         EVERY relpath this run's own pre-scan just saw for this service --
         `analyze_service`'s full path (`begin_service`) unconditionally wipes this
         service's ENTIRE `chunks` table, so every file genuinely needs re-chunking,
         not just whichever ones happened to change on disk. "incremental" -> the
         report's own `stale_relpaths` (already exactly `changed | added | ref_dirty`,
         see pipeline/analyze.py) -- deliberately NOT unioned with anything else here,
         since it is already a complete stale set on its own.
    """
    per_service: list[dict] = []
    changed_files: dict[str, set[str]] = {}

    for svc in cfg.services:
        idioms = effective_idioms(cfg, svc)

        if not incremental:
            report = analyze_service(
                svc, staging, cache_dir, active_idioms=active_idioms, idioms=idioms,
            )
            fp = config_fingerprint(svc, idioms, active_idioms)
            staging.set_meta(_svc_fingerprint_key(svc.name), _fp_to_persist(report, fp))
            per_service.append(report)
            continue

        scanned, _ = scan_service(svc.path, svc.exclude)
        staged = dict(staging.files_for_service(svc.name))
        delta = service_delta(staged, scanned)
        all_relpaths = {rp for rp, _, _ in scanned}

        fp = config_fingerprint(svc, idioms, active_idioms)
        stored_fp = staging.get_meta(_svc_fingerprint_key(svc.name))
        fp_ok = stored_fp == fp

        if fp_ok:
            report = analyze_service(
                svc, staging, cache_dir, active_idioms=active_idioms, idioms=idioms,
                incremental=True, prior_delta=delta, fingerprint_ok=True,
            )
        else:
            report = analyze_service(
                svc, staging, cache_dir, active_idioms=active_idioms, idioms=idioms,
                incremental=False, fingerprint_ok=False,
            )
            if report.get("reason") is None:
                report = {
                    **report,
                    "reason": "first run" if stored_fp is None else "fingerprint mismatch",
                }

        mode = report.get("mode")
        if mode != "skipped":
            staging.set_meta(_svc_fingerprint_key(svc.name), _fp_to_persist(report, fp))
        if mode == "incremental":
            changed_files[svc.name] = set(report["stale_relpaths"])
        elif mode == "full":
            changed_files[svc.name] = all_relpaths
        # "skipped": absent from changed_files entirely (see docstring point 5).

        per_service.append(report)

    return per_service, changed_files


@app.command()
def index(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    dry_run: bool = typer.Option(False, "--dry-run"),
    graph: str | None = typer.Option(None, "--graph"),
    no_embed: bool = typer.Option(False, "--no-embed"),
    incremental: bool = typer.Option(False, "--incremental"),
) -> None:
    """Построить граф workspace: scan → resolve → extract → join → chunk+embed → load →
    report (`--dry-run` — только план пайплайна, без записи; `--no-embed` — пропустить
    только embedding-шаг S8 -- чанки и headers всё равно строятся и грузятся;
    `--incremental` -- per-service skip/incremental/full решение по config-fingerprint
    и scan-diff вместо безусловного полного пере-анализа, см. `_analyze_services`)."""
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
        per_service, changed_files = _analyze_services(
            cfg, staging, codegraph_dir / "scip", active_idioms, incremental,
        )
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
        # M4 T7: `changed_files` only ever passed (as a 4th positional arg) when
        # `--incremental` is set -- the non-incremental call below keeps the EXACT
        # pre-M4 3-positional-arg shape, not a 4th arg whose VALUE happens to be None:
        # `run_chunk_embed`'s own default (omitted entirely) and an explicit `None`
        # are behaviorally identical to IT, but every pre-M4 test/caller's fake
        # narrowly accepts only 3 positional params -- passing a 4th (even None)
        # unconditionally would TypeError every one of them. `_analyze_services`
        # itself returns `{}` (not None) for the non-incremental case regardless, and
        # `{}` is NOT interchangeable with `None` to `run_chunk_embed` either way (an
        # empty dict skips EVERY service's chunk loop; `None` chunks every service in
        # full, the actual pre-M4 behavior this command must stay byte-identical to).
        chunk_report = (
            run_chunk_embed(cfg, staging, embedder, changed_files)
            if incremental else run_chunk_embed(cfg, staging, embedder)
        )
        load_stats = _store_guard(lambda: load_graph(
            staging, lambda name: FalkorStore(cfg.storage.falkordb, name), graph_name
        ))

    report = build_report(per_service, load_stats, link_report, chunk_report)
    write_report(report, codegraph_dir / "report.json")
    print_report(report, console)
    # Paid-provider notice (M3 final review; M4 T1 updated for the persistent
    # embedding cache): a local embedder is a one-time cost (the model runs on this
    # machine), but openai/voyage bill per API call. M4 T1 adds a persistent,
    # cross-run `embedding_cache` table (staging.db) keyed on the exact embedder
    # input -- so an UNCHANGED re-index no longer re-embeds anything, even paid-API
    # chunks. `chunk_report["embedded_fresh"]` (not the combined `embedded`, which
    # also counts free cache reuses) is what actually gates this warning now: it
    # counts real, billed provider calls THIS run, so a repeat run that served
    # everything from cache (`embedded_fresh == 0`) correctly prints nothing, even
    # though `embedded` itself may be > 0. Surfaced here, not inside print_report
    # itself, so print_report (also called from `load`-adjacent report plumbing/
    # tests) stays embedding-provider-agnostic -- this is CLI-command-level context
    # (cfg.embedding), not part of the plain chunk_stats dict every other consumer of
    # build_report sees.
    if chunk_report["embedded_fresh"] > 0 and cfg.embedding.provider != "local":
        console.print(
            f"[yellow]{chunk_report['embedded_fresh']} chunk(s) embedded via "
            f"{cfg.embedding.provider} API this run (billed); "
            f"{chunk_report.get('embedded_from_cache', 0)} more served from the "
            "persistent embedding cache at zero cost. Re-running 'codegraph index' "
            "unchanged will re-embed 0 of them[/]"
        )


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
            collapsed = step.get("collapsed")
            if collapsed is not None:
                # M5 T5: a synthetic marker (query/traverse.py's _compact_steps) --
                # no real edge_type/node to render, just the hidden count.
                seg_node.add(f"⋯ {collapsed} внутренних вызовов")
                continue
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


def _segment_collapsed_total(seg: dict) -> int:
    """Sum of every collapsed-marker step's own count within one segment (M5 T5) --
    a segment can carry more than one such marker (e.g. a role-bearing step splits
    one long run into two, see query/traverse.py's _compact_steps), each with its
    own count. 0 for a segment with no markers at all (compact=False, or every run
    was short enough to survive uncollapsed)."""
    return sum(
        step["collapsed"] for step in seg.get("steps", []) if step.get("collapsed") is not None
    )


def _trace_mermaid(result: dict) -> str:
    """flowchart TD: один узел на сегмент (`S{i}["service: entry"]`), одна стрелка
    на next_entry_id (`S{i} -->|channel| S{j}`) -- next_entry_ids, не указывающие
    ни на один сегмента этого трейса (dangling/за пределами max_segments),
    молча пропускаются (тот сегмент просто не появится как узел стрелки).
    `"`/`|` внутри меток заменяются -- эти символы ломают mermaid-синтаксис узла/
    метки ребра; смоук-уровень экранирования (валиден для рендера, не
    исчерпывающий).

    M5 T5: mermaid has no per-step nodes (unlike the text tree, it only ever
    renders one node per SEGMENT -- steps/exits within a segment aren't drawn at
    all), so a collapsed run has nowhere of its own to attach to; its count is
    folded into that segment's OWN label instead (kept simple per this task's
    brief) -- 0 collapsed steps (the common case: short segments, or --full)
    leaves the label exactly as before."""
    segments = result.get("segments", [])
    entry_to_index = {
        seg["entry"]["id"]: i
        for i, seg in enumerate(segments)
        if seg.get("entry", {}).get("id") is not None
    }
    lines = ["flowchart TD"]
    for i, seg in enumerate(segments):
        label = f"{seg.get('service', '')}: {_node_label(seg.get('entry', {}))}"
        collapsed_total = _segment_collapsed_total(seg)
        if collapsed_total:
            label += f" (⋯{collapsed_total})"
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
    full: bool = typer.Option(
        False, "--full",
        help=(
            "Отключить компактный режим (M5 T5): показать КАЖДЫЙ шаг сегмента "
            "без схлопывания длинных линейных цепочек в «⋯ N внутренних "
            "вызовов». По умолчанию сегменты длиннее 15 шагов схлопываются -- "
            "роли/ветвления/exit-шаги никогда не схлопываются."
        ),
    ),
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

    result = gq.trace_process(resolved["node_id"], compact=not full)
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


# M4 T2 (wheel-safe packaging, same treatment as TEMPLATE above): the default
# --questions file ships INSIDE the package (src/codegraph/data/questions.yaml) --
# the old Path(__file__).parent.parent.parent / "fixtures" / ... construction reached
# outside the package and raised FileNotFoundError from a real wheel install.
# fixtures/golden/questions.yaml stays put as the LIVE golden set (the M3 gate in
# tests/eval/test_m3_gate.py reads it directly, and it stays hand-editable there);
# the packaged file is the byte-identical copy `eval retrieval` ships as its default
# (drift-guarded in tests/unit/test_cli_eval.py).
_DEFAULT_QUESTIONS = importlib.resources.files("codegraph") / "data" / "questions.yaml"

# `eval` is a command GROUP (Typer sub-app), not a flat command -- `codegraph eval
# retrieval [target] [--graph] [--k] [--questions PATH]` (M3 T8's own contract).
# `no_args_is_help=True` mirrors the top-level `app`'s own callback comment: bare
# `codegraph eval` prints help instead of erroring.
eval_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(eval_app, name="eval")


@eval_app.command("retrieval")
def eval_retrieval(
    target: Path | None = typer.Argument(None),  # noqa: B008 -- typer marker call, idiomatic
    graph: str | None = typer.Option(None, "--graph"),
    k: int | None = typer.Option(
        None, "--k", help="override every question's own k (default: each question's own)"
    ),
    questions_path: Path = typer.Option(  # noqa: B008 -- typer marker call, idiomatic
        _DEFAULT_QUESTIONS, "--questions",
        help=(
            "golden questions YAML ({question, accept: [{service, symbol}], k} rows). "
            "Defaults to codegraph's OWN golden questions, bundled inside the package "
            "(data/questions.yaml -- works from a wheel install too) -- there is no "
            "auto-discovered '<workspace>/questions.yaml' convention (a real "
            "workspace's golden questions are necessarily hand-authored against ITS "
            "OWN symbols; pass --questions explicitly to point at one)."
        ),
    ),
    exact: bool = typer.Option(
        False, "--exact",
        help=(
            "deterministic full-scan cosine distance (vec.cosineDistance) instead of "
            "FalkorDB's ANN vector index (db.idx.vector.queryNodes/HNSW, which "
            "rebuilds unseeded on every graph load -- hit@k against it is not "
            "reproducible run to run, see README «Ограничения»). For CI/"
            "comparisons between runs; slower than ANN on large graphs. Production/"
            "MCP search (search_code) is unaffected either way -- this flag only "
            "changes what THIS eval command does."
        ),
    ),
) -> None:
    """Прогон golden-вопросов (hit@k) через search_code(mode="hybrid") на УЖЕ
    существующем графе (нужен предварительный `codegraph index`) -- rich-таблица
    question/hit/rank/top-1. Отчёт, не гейт: exit 0 при ЛЮБОМ исходе hit/miss --
    жёсткий гейт для CI живёт в tests/eval/test_m3_gate.py, не здесь. Инфраструктурные
    ошибки (store недоступен, граф не найден, questions-файл не читается) остаются
    exit 1, как и у остальных команд (stats/load/trace).

    `--exact` (M5 T2, pilot Bug A): routes search_code's vector leg through
    FalkorStore.search_vector_chunks_exact (full Cypher scan, no ANN index) instead
    of the default ANN search -- deterministic hit@k across identical runs, at the
    cost of an O(n) scan instead of HNSW's approximate speed. Use it for CI gates or
    before/after comparisons where run-to-run vector-ranking noise would otherwise
    make a hit@k delta meaningless; leave it off for a quick manual check on a large
    graph, where ANN's speed matters more than exact reproducibility."""
    target_path = target if target is not None else Path.cwd()
    cfg = _load(target_path)
    graph_name = _resolve_graph_name(cfg, graph)

    try:
        questions = load_questions(questions_path)
    except (OSError, yaml.YAMLError) as e:
        console.print(
            f"[red]failed to read questions file {questions_path}:[/] {escape(str(e))}"
        )
        raise typer.Exit(1) from e
    if k is not None:
        questions = [{**q, "k": k} for q in questions]

    store = FalkorStore(cfg.storage.falkordb, graph_name)
    if not _store_guard(store.graph_exists):
        console.print(
            f"[red]graph {graph_name!r} not found — run 'codegraph index' first[/]"
        )
        raise typer.Exit(1)

    # Same catch-CodegraphError-and-degrade contract as _make_embedder_or_warn/
    # mcp.server._default_embedder_factory -- provider package not installed/API key
    # missing degrades search_code to text-only (mode_used="text" per question) rather
    # than failing this command; worded for THIS command's own context rather than
    # reusing _make_embedder_or_warn's "S8:"-prefixed message (there is no S8 here).
    try:
        embedder = make_embedder(cfg.embedding)
    except CodegraphError as e:
        console.print(
            f"[yellow]retrieval eval: vector mode unavailable ({escape(str(e))}), "
            "degrading to text-only[/]"
        )
        embedder = None

    gq = GraphQuery(
        store_factory=lambda: FalkorStore(cfg.storage.falkordb, graph_name),
        service_paths={svc.name: svc.path for svc in cfg.services},
        embedder_factory=lambda: embedder,
    )
    results = run_questions(
        lambda q, kk: gq.search_code(q, k=kk, mode="hybrid", exact=exact), questions
    )

    table = Table(title=f"retrieval eval · graph={graph_name}")
    table.add_column("question")
    table.add_column("hit")
    table.add_column("rank")
    table.add_column("top-1")
    hits = 0
    for r in results:
        hits += 1 if r["hit"] else 0
        top1 = r["top"][0] if r["top"] else None
        top1_label = (
            "(no results)" if top1 is None else (top1["qualified_name"] or top1["symbol_id"] or "?")
        )
        table.add_row(
            escape(r["question"]),
            "[green]HIT[/]" if r["hit"] else "[red]MISS[/]",
            str(r["rank"]) if r["rank"] is not None else "-",
            escape(top1_label),
        )
    console.print(table)
    console.print(f"hit@k: {hits}/{len(results)}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
