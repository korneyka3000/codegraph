"""S1–S6 per-service анализ: scan → resolve (реальный SCIP или деградированный
эвристический fallback) → parse/extract (tree-sitter + python_core + доменные
экстракторы) → join (CALLS).

Деградация: `ScipRunError` из `ScipRunner.run` — единственный триггер (сеть/npx/venv
недоступны или таймаут scip-python). В этом случае defs/refs строятся эвристически
(`fallback.resolve_service`) вместо SCIP, а join получает resolution="heuristic"/
confidence=0.6 вместо "static"/1.0 — вся остальная оркестрация (facts, extract,
Service-узел, состав отчёта) идентична обоим путям.

M2 T4: доменные экстракторы (сейчас — fastapi) запускаются в S5 ПОСЛЕ python_core,
за флагом активных builtin-идиом (`active_idioms`, по умолчанию пусто — опциональное
расширение, ни на один существующий вызов analyze_service не влияет). roles/node_props
мёржатся в уже построенные python_core NodeRec'и ДО staging.upsert_nodes; channels/edges
идут в те же upsert_nodes/upsert_edges вызовы. `active_idioms` — НЕ то же самое, что
per-service ServiceIdioms (config/models.py): это подмножество cfg.builtin_idioms
(workspace-level), просто проверка "какие структурные доменные экстракторы включать".

M2 T5: kafka_ext/temporal_ext добавлены в тот же S5-проход. kafka — ДАННЫЕ-идиома: её
активация НЕ читает active_idioms вовсе, а зависит от параметра `idioms` (effective
ServiceIdioms — builtin aiokafka/faststream/confluent, если они в cfg.builtin_idioms,
смёрженные с собственными идиомами сервиса, см. config.loader.effective_idioms) —
активна, если там есть хоть один producer/consumer; `idioms=None` (дефолт — как и
`active_idioms=frozenset()`, ни на один существующий вызов не влияет) эквивалентен
пустой ServiceIdioms. temporal — структурный экстрактор как fastapi: активен, если
"temporal" ∈ active_idioms (builtin_idioms.py держит его ServiceIdioms пустым намеренно
— паттерны декораторов зашиты в temporal_ext.py, не в идиом-DSL). node_ids -- та же
def-index -> node-id карта, что уже строилась для fastapi, дополненная ОДНИМ новым
ключом `None -> Module-node-id`: CallFact.enclosing_def уже использует None как маркер
"вызов на уровне модуля", так что `node_ids.get(call.enclosing_def)` прозрачно
резолвится в Module-узел для module-level producer/consumer вызовов без отдельной ветки
в самих экстракторах (пример — document_management-подобный `producer = Foo(); producer.send(...)`
на уровне модуля; ни один текущий фикстурный файл этого не требует, но kafka_ext
контрактно это поддерживает). cli.py передаёт `idioms=effective_idioms(cfg, svc)` —
ПЕР-сервисно (в отличие от `active_idioms`, который один на весь workspace).
"""

from __future__ import annotations

from dataclasses import replace
from functools import cache, partial
from pathlib import Path

from codegraph.config.models import ServiceConfig, ServiceIdioms
from codegraph.core.schema import EdgeRec, NodeRec, make_service_node
from codegraph.extractors.base import FileContext
from codegraph.extractors.calls import build_calls
from codegraph.extractors.fastapi_ext import extract_fastapi
from codegraph.extractors.kafka_ext import extract_kafka
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.extractors.temporal_ext import extract_temporal
from codegraph.parsing.consts import ConstTable
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


def _apply_role_props_patch(
    n: NodeRec, roles: dict[str, set[str]], props: dict[str, dict],
) -> NodeRec:
    """roles/node_props-патч доменного экстрактора -> новый NodeRec (frozen -- нельзя
    мутировать на месте). roles объединяются с уже существующими (union, отсортировано
    для детерминизма); props — shallow merge, патч побеждает при коллизии ключей. n без
    патча по её id -- возвращается как есть (без лишней аллокации)."""
    extra_roles = roles.get(n.id)
    extra_props = props.get(n.id)
    if not extra_roles and not extra_props:
        return n
    return replace(
        n,
        roles=tuple(sorted({*n.roles, *extra_roles})) if extra_roles else n.roles,
        props={**n.props, **extra_props} if extra_props else n.props,
    )


def analyze_service(
    svc: ServiceConfig,
    staging: Staging,
    cache_dir: Path,
    runner: ScipRunner | None = None,
    active_idioms: frozenset[str] = frozenset(),
    idioms: ServiceIdioms | None = None,
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
    # + доменные экстракторы (fastapi и т.д.) за активной builtin-идиомой.
    module_set = staging.module_set(svc.name)
    def_symbol_lookup = partial(staging.def_symbol_at, svc.name)
    ref_symbol_lookup = partial(staging.ref_symbol_at, svc.name)
    module_exists = module_set.__contains__
    fastapi_active = "fastapi" in active_idioms
    # T5: kafka is DATA-driven (effective ServiceIdioms), not active_idioms-gated --
    # see module docstring. idioms=None (default, matches active_idioms=frozenset()'s
    # own "no existing caller affected" convention) behaves like an empty ServiceIdioms.
    kafka_idioms = idioms if idioms is not None else ServiceIdioms()
    kafka_active = bool(kafka_idioms.producers or kafka_idioms.consumers)
    temporal_active = "temporal" in active_idioms
    domain_active = fastapi_active or kafka_active or temporal_active

    nodes = [make_service_node(svc.name)]
    edges = []
    imports_external = 0
    domain_roles: dict[str, set[str]] = {}
    domain_node_props: dict[str, dict] = {}
    domain_channels: list[NodeRec] = []
    domain_edges: list[EdgeRec] = []
    for rp in relpaths:
        ctx = FileContext(
            service=svc.name, relpath=rp, source=files[rp], facts=facts_by_file[rp],
            def_symbol_lookup=def_symbol_lookup, module_exists=module_exists,
            ref_symbol_lookup=ref_symbol_lookup,
        )
        res = extract_python_core(ctx)
        nodes.extend(res.nodes)
        edges.extend(res.edges)
        imports_external += res.stats["imports_external"]

        if domain_active:
            # def-index -> node id, из ЭТОГО ЖЕ python_core-прогона: nodes[0] всегда
            # Service (общий по сервису, не per-file), res.nodes[0] — Module этого
            # файла, res.nodes[1:] — ровно по одному узлу на facts.defs[rp], в том же
            # порядке (python_core.extract строит их именно так, один append на def).
            # None -> Module id (T5): CallFact.enclosing_def уже None для module-level
            # вызовов, так что .get(call.enclosing_def) резолвится сюда без спецветки.
            node_ids: dict[int | None, str] = {
                d.index: n.id
                for d, n in zip(facts_by_file[rp].defs, res.nodes[1:], strict=True)
            }
            node_ids[None] = res.nodes[0].id

            if fastapi_active:
                fr = extract_fastapi(ctx, node_ids)
                for nid, rs in fr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                for nid, props in fr.node_props.items():
                    domain_node_props.setdefault(nid, {}).update(props)
                domain_channels.extend(fr.channels)
                domain_edges.extend(fr.edges)

            if kafka_active:
                consts = ConstTable.build(facts_by_file[rp], files[rp])
                kr = extract_kafka(ctx, node_ids, kafka_idioms, consts)
                for nid, rs in kr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                domain_channels.extend(kr.channels)
                domain_edges.extend(kr.edges)

            if temporal_active:
                tr = extract_temporal(ctx, node_ids)
                for nid, rs in tr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                for nid, props in tr.node_props.items():
                    domain_node_props.setdefault(nid, {}).update(props)
                domain_edges.extend(tr.edges)
                # temporal_start_mark: per-file claim, consumed later by S7 (T7) via
                # staging.claims_for + update_edge_props on the matching CALLS edge.
                staging.add_claims(svc.name, rp, "temporal_start_mark", tr.claims)

    if domain_roles or domain_node_props:
        nodes = [_apply_role_props_patch(n, domain_roles, domain_node_props) for n in nodes]
    nodes.extend(domain_channels)
    edges.extend(domain_edges)

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
