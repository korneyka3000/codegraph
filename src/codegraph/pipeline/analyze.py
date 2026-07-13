"""S1–S6 per-service анализ: scan → resolve (реальный SCIP или деградированный
эвристический fallback) → parse/extract (tree-sitter + python_core) → join (CALLS).

Деградация: `ScipRunError` из `ScipRunner.run` — единственный триггер (сеть/npx/venv
недоступны или таймаут scip-python). В этом случае defs/refs строятся эвристически
(`fallback.resolve_service`) вместо SCIP, а join получает resolution="heuristic"/
confidence=0.6 вместо "static"/1.0 — вся остальная оркестрация (facts, extract,
Service-узел, состав отчёта) идентична обоим путям.
"""

from __future__ import annotations

from functools import cache, partial
from pathlib import Path

from codegraph.config.models import ServiceConfig
from codegraph.core.schema import make_service_node
from codegraph.extractors.base import FileContext
from codegraph.extractors.calls import build_calls
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.facts import build_file_facts
from codegraph.resolvers import fallback
from codegraph.resolvers.scip.reader import read_scip_into_staging
from codegraph.resolvers.scip.runner import ScipRunError, ScipRunner
from codegraph.stores.staging import Staging

from .scan import scan_service


def _venv_for(svc: ServiceConfig) -> Path | None:
    if svc.python is not None:
        return svc.path / svc.python
    default_venv = svc.path / ".venv"
    return default_venv if default_venv.exists() else None


def analyze_service(
    svc: ServiceConfig,
    staging: Staging,
    cache_dir: Path,
    runner: ScipRunner | None = None,
) -> dict:
    runner = runner if runner is not None else ScipRunner()

    # 1. begin_service; scan -> add_files
    staging.begin_service(svc.name)
    rows, tree_hash = scan_service(svc.path, svc.exclude)
    staging.add_files(svc.name, rows)
    relpaths = [rp for rp, _, _ in rows]  # scan_service гарантирует сортировку

    files = {rp: (svc.path / rp).read_bytes() for rp in relpaths}
    # 4. facts по отсортированным relpath (bytes из files выше). Вычислены здесь, ДО resolve
    # (шаг 2), потому что деградированный fallback (шаг 3) тоже нуждается в facts_by_file —
    # строим единожды и переиспользуем в шаге 3 (если degraded) и шаге 5 (всегда).
    facts_by_file = {rp: build_file_facts(rp, files[rp]) for rp in relpaths}

    # 2. resolve: реальный SCIP через runner, иначе деградация
    degraded = False
    reason: str | None = None
    from_cache = False
    defs_count = refs_count = malformed_ranges = 0
    try:
        result = runner.run(svc.name, svc.path, _venv_for(svc), cache_dir, tree_hash)
        from_cache = result.from_cache
        reader_stats = read_scip_into_staging(result.scip_path, svc.name, svc.path, staging)
        defs_count = reader_stats.defs
        refs_count = reader_stats.refs
        malformed_ranges = reader_stats.malformed_ranges
    except ScipRunError as e:
        degraded = True
        reason = str(e)

    # 3. degraded -> эвристический fallback (файлы уже прочитаны выше)
    if degraded:
        def_rows, ref_rows = fallback.resolve_service(svc.name, files, facts_by_file)
        staging.add_defs(svc.name, def_rows)
        staging.add_refs(svc.name, ref_rows)
        defs_count, refs_count = len(def_rows), len(ref_rows)

    # 5. extract: python_core на каждый файл + Service-узел (carry-фикс «Service-узлы»)
    module_set = staging.module_set(svc.name)
    def_symbol_lookup = partial(staging.def_symbol_at, svc.name)
    module_exists = module_set.__contains__

    nodes = [make_service_node(svc.name)]
    edges = []
    imports_external = 0
    for rp in relpaths:
        ctx = FileContext(
            service=svc.name, relpath=rp, source=files[rp], facts=facts_by_file[rp],
            def_symbol_lookup=def_symbol_lookup, module_exists=module_exists,
        )
        res = extract_python_core(ctx)
        nodes.extend(res.nodes)
        edges.extend(res.edges)
        imports_external += res.stats["imports_external"]
    staging.upsert_nodes(nodes)
    staging.upsert_edges(edges)

    # 6. join: CALLS (static/1.0 либо heuristic/0.6 при деградации); local_defs_for_file
    # хоистится за пределы build_calls, чтобы не бить SQL на каждый local-символ-callsite —
    # per-relpath набор считается максимум один раз за вызов analyze_service.
    @cache
    def local_defs_for_file(relpath: str) -> set[str]:
        return staging.local_def_symbols(svc.name, relpath)

    resolution, confidence = ("heuristic", 0.6) if degraded else ("static", 1.0)
    join_stats = build_calls(
        svc.name, staging, facts_by_file, def_symbol_lookup,
        local_defs_for_file=local_defs_for_file,
        resolution=resolution, confidence=confidence,
    )

    # 7. отчёт ("service" -- первым ключом: report.build_report ожидает его в каждом
    # per_service-элементе, и его источник -- сам analyze_service, не вызывающая сторона)
    return {
        "service": svc.name,
        "files": len(relpaths),
        "defs": defs_count,
        "refs": refs_count,
        "malformed_ranges": malformed_ranges,
        "nodes": len(nodes),
        "edges": len(edges),
        "imports_external": imports_external,
        "calls_joined": join_stats.calls_joined,
        "calls_unresolved": join_stats.calls_unresolved,
        "calls_external": join_stats.calls_external,
        "degraded": degraded,
        "reason": reason,
        "from_cache": from_cache,
    }
