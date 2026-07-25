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

M8 T1 (rerun-2 R4, docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
fastapi's OWN contribution to "channels/edges идут в те же upsert_nodes/upsert_edges
вызовы" above no longer holds -- a route's full identity now needs the cross-file
`include_router` chain a single file's own S5 pass can't see, so fastapi_ext.py emits
ONLY DEPENDS_ON edges directly (domain_edges, unchanged) plus two per-file CLAIM kinds
("route_decl"/"router_include", staged via staging.add_claims -- same http_call/
temporal_start_mark pattern T6/T7 already established below), consumed later by
linking/router_prefix.py (S7, workspace.py). roles/node_props are UNCHANGED (file-local,
still merged here exactly as this paragraph describes).

M2 T5: kafka_ext/temporal_ext добавлены в тот же S5-проход. kafka — ДАННЫЕ-идиома: её
активация НЕ читает active_idioms вовсе, а зависит от параметра `idioms` (effective
ServiceIdioms — builtin aiokafka/faststream/confluent, если они в cfg.builtin_idioms,
смёрженные с собственными идиомами сервиса, см. config.loader.effective_idioms) —
активна, если там есть хоть один producer/consumer; `idioms=None` (дефолт — как и
`active_idioms=frozenset()`, ни на один существующий вызов не влияет) эквивалентен
пустой ServiceIdioms. temporal — структурный экстрактор как fastapi: активен, если
"temporal" ∈ active_idioms (builtin_idioms.py держит его ServiceIdioms пустым намеренно
— паттерны декораторов зашиты в temporal_ext.py, не в идиом-DSL).

M2 T6: http_client_ext — ТОЖЕ данные-идиома, тем же принципом что kafka: активен, если
`idioms.http_clients` непуст (active_idioms не читает вовсе). В отличие от kafka/fastapi/
temporal, ничего не пишет в domain_roles/domain_node_props/domain_edges/domain_channels
-- extract_http_client(ctx, node_ids, idioms, consts) возвращает ТОЛЬКО claims (никаких
рёбер -- CALLS_HTTP делает S7/T7) + stats; claims идут в staging.add_claims(svc.name, rp,
"http_call", hr.claims) сразу же по каждому файлу, тем же паттерном что temporal_start_mark
(kind — отдельный позиционный параметр add_claims, не внутри payload). Не нуждается в
ref_symbol_lookup вовсе (consts + resolve_arg, чисто структурно), так что
резолвится идентично что в degraded fallback, что при реальном SCIP -- см. модульный
докстринг extractors/http_client_ext.py. T6-ревью фикс: ConstTable строится ОДИН раз на
файл и передаётся во ВСЕ consts-потребляющие экстракторы (изначально kafka+http_client;
M7 T4 добавил temporal третьим -- гейт теперь
kafka_active|http_client_active|temporal_active, см. сам ternary ниже). node_ids -- та же
def-index -> node-id карта, что уже строилась для fastapi, дополненная ОДНИМ новым
ключом `None -> Module-node-id`: CallFact.enclosing_def уже использует None как маркер
"вызов на уровне модуля", так что `node_ids.get(call.enclosing_def)` прозрачно
резолвится в Module-узел для module-level producer/consumer вызовов без отдельной ветки
в самих экстракторах (пример — document_management-подобный `producer = Foo(); producer.send(...)`
на уровне модуля; ни один текущий фикстурный файл этого не требует, но kafka_ext
контрактно это поддерживает). cli.py передаёт `idioms=effective_idioms(cfg, svc)` —
ПЕР-сервисно (в отличие от `active_idioms`, который один на весь workspace).

M4 T5: incremental analyze_service (`incremental=True`) -- three modes reported via
report["mode"]: "full" (today's behavior, still the default -- every pre-existing
caller stays byte-identical), "skipped" (prior_delta.empty AND fingerprint_ok -- ZERO
staging writes, report reads whatever this service's CURRENT per-service counts
already are), "incremental" (re-extract+re-join only the STALE subset of files). The
full path is one piece of code (`_analyze_full`), reachable two ways: incremental=False
(the ordinary call), or as the incremental branch's own fallback when scip-python fails
mid-attempt (ScipRunError -- see below) -- so "byte-identical to today" is not just a
goal for the direct call, it falls out of literally sharing the function.

Correctness argument (binding -- keep it true): CALLS/IMPORTS/domain-claims of file X
are a function of ONLY (X's content, X's refs, service-wide defs). scip-python is not
file-incremental, so S3 always re-runs over the WHOLE service (the ScipRunner's own
tree_hash-keyed cache absorbs the "nothing changed" case for free -- see
`resolvers/scip/runner.py`) and S4 rewrites ALL defs/refs every time (cheap). Only the
expensive stages -- S5 (tree-sitter parse+extract) and S6 (join) -- run over `stale =
changed | added | ref_dirty`, where `ref_dirty` is the set of files whose
`refs_hash_by_file` hash changed across THIS run's S4 rewrite even though their OWN
bytes never moved: pyright/scip-python itself propagates a symbol rename in file A into
every file B that references it (B's occurrence at that import/call site now resolves
to a different symbol, or to none), so B lands in ref_dirty and gets re-extracted/
re-joined even though `service_delta` (which only ever compares file content hashes)
would never flag it on its own.

Incremental step order (load-bearing, not cosmetic):
  1. Snapshot `old_files = dict(staging.files_for_service(svc.name))`,
     `old_refs = staging.refs_hash_by_file(svc.name)` and (M7 T1 review Important-1)
     `old_class_attrs_digest = _class_attrs_digest(...)` -- BEFORE anything wipes
     them.
  2. `staging.clear_scip_layer(svc.name)` (files/scip_defs/scip_refs only -- nodes/
     edges/claims of non-stale files must survive this call untouched).
  3. Fresh `scan_service` -> `add_files` -> `service_delta(old_files, scanned)`.
  4. S3 (`runner.run`) + S4 (`read_scip_into_staging`), full, over the FRESH scan --
     same calls the full path itself makes. ScipRunError here means an immediate,
     total abandonment of the incremental attempt: delegate to `_analyze_full` (which
     re-scans, re-resolves -- hitting the SAME ScipRunError again -- and degrades
     exactly as it always has, tagged mode="full"). There is no sound "degraded
     incremental" middle ground: the heuristic fallback resolver gives no stable
     refs-diff to compute ref_dirty from.
  5. `new_refs = staging.refs_hash_by_file(svc.name)`; `ref_dirty` = the
     unchanged-content relpaths whose old vs new refs hash differ.
  6. `stale = changed | added | ref_dirty`; `dead = deleted`.
  7. `staging.delete_file_layer(svc.name, stale | dead, drop_calls_evidence=stale |
     dead)` -- BEFORE S5: S5's own per-file `add_claims` calls (temporal_start_mark/
     http_call) must not collide with a stale claim row from the file's PREVIOUS
     analysis.
  8. (8a, M7 T1 review Important-1 -- the settings-staleness escalation) Harvest the
     stale files' class_attrs claims, then compare the SERVICE-WIDE class_attrs
     digest against the step-1 snapshot: refs_hash is blind to attribute VALUES, so
     a Settings-default/enum-member edit makes ONLY the Settings file itself stale
     while every consumer file's T2/T3-derived edges (which bake those values in)
     would silently stay stale forever. A changed digest (with non-stale files left
     to protect) escalates THIS run: stale widens to ALL scanned files, step 7's
     deletion re-runs over the widened set, and the report says
     stale_escalation="class_attrs_changed". Then S5+S6
     (`_extract_join_and_stage`, the exact function the full path also calls)
     over `stale` -- `def_symbol_lookup`/`ref_symbol_lookup`/
     `local_defs_for_file` all query the FRESH, service-wide (not stale-scoped)
     staging tables step 4 just wrote, so a stale file referencing a def in a
     NON-stale file still resolves correctly. The Service node is always
     re-emitted (id stable, INSERT OR REPLACE is a no-op when nothing changed).

`delete_file_layer`'s edge deletion is keyed by (origin_service, evidence_file), which
depends on EVERY S5-emitted edge actually carrying its emitting file as evidence_file.
python_core.py's CONTAINS edges did not before this task (only IMPORTS did -- the old
`add_edge` helper tied evidence_file to whether a line number was passed at all,
conflating two unrelated concerns) -- fixed as part of this same task (see
`extractors/python_core.py`'s own `add_edge` comment): without that fix, a stale
CONTAINS edge pointing at a since-renamed/removed node would survive incremental
re-analyze forever, since nothing would ever match it for deletion.

M7 T1 (class_attrs harvesting, open-gaps R1/R2 foundation): `_extract_join_and_stage`
gained a pre-loop pass, BEFORE its own python_core/domain-extractor loop, that
harvests every `relpaths` file's class-body literals (pydantic-Settings fields,
Enum/StrEnum member values -- `parsing.class_attrs.harvest_class_attrs`) into
per-file "class_attrs" claims and assembles the resulting SERVICE-WIDE
`ClassAttrIndex` from those claims (`staging.claims_for`) -- reused claims plumbing,
no schema bump; see that function's own docstring for the full incremental-
coherence argument (claims are per-file-keyed and already survive/get wiped
correctly across full and incremental runs alike). The index is threaded into every
file's `FileContext.class_attr_index` this call builds. No consumer reads it yet
(T2/T3, later in this milestone, will) -- this task ships only the harvester, the
index, and this wiring.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from functools import cache, partial
from pathlib import Path

from codegraph.config.models import ServiceConfig, ServiceIdioms
from codegraph.core.schema import EdgeRec, NodeRec, make_service_node
from codegraph.extractors.base import FileContext
from codegraph.extractors.calls import build_calls
from codegraph.extractors.fastapi_ext import extract_fastapi
from codegraph.extractors.http_client_ext import extract_http_client
from codegraph.extractors.kafka_ext import extract_kafka
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.extractors.temporal_ext import extract_temporal
from codegraph.parsing.class_attrs import build_class_attr_index, harvest_class_attrs
from codegraph.parsing.consts import ConstTable
from codegraph.parsing.facts import FileFacts, build_file_facts
from codegraph.resolvers import fallback
from codegraph.resolvers.scip.reader import read_scip_into_staging
from codegraph.resolvers.scip.runner import ScipRunError, ScipRunner
from codegraph.stores.staging import Staging

from .diff import ServiceDelta, service_delta
from .scan import scan_service


def _venv_for(svc: ServiceConfig) -> Path | None:
    if svc.python is not None:
        return svc.path / svc.python
    default_venv = svc.path / ".venv"
    return default_venv if default_venv.exists() else None


def _class_attrs_digest(staging: Staging, service: str) -> str:
    """sha256 over the service's CURRENT staged class_attrs claims (M7 T1 review
    Important-1) -- the incremental path's "did any class-body literal change"
    fingerprint. Byte-deterministic because `claims_for` orders rows by (relpath,
    payload_json) (staging.py, M7 T1 review Minor-2 -- an order flip here would read
    as a phantom change and escalate for no reason) and each payload dict round-trips
    through sort_keys=True JSON on both write (`add_claims`) and re-dump (here).
    Includes claims_for's injected "_service"/"_relpath" keys -- constant-per-row
    metadata that only ever changes when the row itself does, so it never distorts
    the comparison."""
    rows = staging.claims_for("class_attrs", service)
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


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


def _build_facts_by_file(relpaths: list[str], files: dict[str, bytes]) -> dict[str, FileFacts]:
    """`build_file_facts(relpath, source_bytes)` (parsing/facts.py) over every file in
    `relpaths`, collected into one `{relpath: FileFacts}` dict -- extracted so
    `_analyze_full` (relpaths == every scanned file) and `_analyze_incremental`
    (relpaths == the stale subset only, M4 T5) share ONE copy of this loop instead of
    two independently-maintained dict comprehensions that could drift apart.

    M4 T8: measured, not assumed, whether fanning this out across a
    `ThreadPoolExecutor` (tree-sitter's own C parse releases the GIL, so it's
    THEORETICALLY parallelizable) is worth doing -- verdict: NO, reverted to the plain
    sequential comprehension below. `build_file_facts` spends most of its wall time in
    a recursive, pure-Python AST walk building `DefFact`/`CallFact`/`ImportFact`/
    `AssignFact` objects (dataclass construction, string decode, list/dict work) --
    that part does NOT release the GIL, so threads mostly serialize on it anyway,
    while still paying real `ThreadPoolExecutor` overhead (thread startup, GIL
    handoff/contention, executor bookkeeping) on top. Measured (scratch
    `perf_counter` harness, `codegraph.parsing.facts.build_file_facts` +
    `codegraph.pipeline.scan.scan_service`, 3 repetitions, median -- see task-8's own
    report for the full table): on the 3 fixture services (29 files, ~6.7KB total,
    summed) sequential beat threaded by 53-87% across three independent runs --
    genuinely small per-file work, where thread-pool overhead alone dominates. Per the
    brief, that result alone is ambiguous-enough-to-check-further ("noise-level"
    reads as "possibly just too small a corpus", not obviously a clean win OR loss),
    so this repo's own `src/` tree (78 files, ~650KB -- deliberately bigger than the
    fixtures, to see whether a larger, more realistic corpus flips the verdict) was
    ALSO measured: -4.9%, +2.2%, +1.6% win across three independent runs -- a wash,
    nowhere near the required >=15% threshold either direction. Kept as a plain
    sequential dict comprehension -- the shared-helper extraction (this function
    existing at all, replacing two independently-inlined loops) is the part of this
    task that stands regardless of the parallelism verdict.

    `relpaths` is used as-is for both the loop order AND (via `dict(...)` insertion
    order) the returned dict's own iteration order -- both callers already pass a
    SORTED list (`scan_service` sorts its rows; `_analyze_incremental` passes
    `sorted(stale)`), and `extractors.calls.build_calls` iterates `facts_by_file.
    items()` directly (its per-(src,dst)-pair "first call site wins" evidence
    aggregation) -- so preserving `relpaths`' own order here, rather than e.g.
    completion order under a hypothetical parallel implementation, is load-bearing
    for deterministic evidence attribution, not just a cosmetic nicety."""
    return {rp: build_file_facts(rp, files[rp]) for rp in relpaths}


def _extract_join_and_stage(
    svc: ServiceConfig,
    staging: Staging,
    relpaths: list[str],
    files: dict[str, bytes],
    facts_by_file: dict[str, FileFacts],
    active_idioms: frozenset[str],
    idioms: ServiceIdioms | None,
    degraded: bool,
) -> dict:
    """S5 (python_core + domain extractors) + S6 (build_calls), shared verbatim by
    the full path (`relpaths` == every scanned file) and the incremental path
    (`relpaths` == the stale subset only, M4 T5) -- extracted so the two paths can
    never drift apart on this large, delicate chunk of wiring. Always stages a fresh
    Service node (id stable, INSERT OR REPLACE is a no-op) regardless of whether
    `relpaths` is empty. `def_symbol_lookup`/`ref_symbol_lookup`/`local_defs_for_file`/
    `def_symbols` (M5 Task 1) below are built from `staging.module_set`/
    `def_symbol_at`/`ref_symbol_at`/`local_def_symbols`/`def_symbols` -- service-wide
    queries, not scoped to `relpaths` -- so cross-file resolution (a stale file
    referencing a def in a file NOT in `relpaths`) works correctly as long as the
    caller already rewrote defs/refs for the whole service (S4) before calling this.

    Returns {"nodes": int, "edges": int, "imports_external": int, "join_stats":
    JoinStats, "http_url_unresolved": int, "http_verb_unresolved": int,
    "http_route_unresolved": int, "consumer_base_class_no_generic": int,
    "producer_unresolved_channel": int, "signal_name_unresolved": int} (the http_*
    trio: M6 T2 review Important-1; the next one: M6 T3; the next: M6 T4 (GAPS §6/
    pilot gap 5), same precedent -- kafka_ext's own base_class/producer honest-miss
    counters summed across `relpaths`, always present, 0 when kafka is inactive or
    nothing failed to resolve; the last one: M7 T4 (OPEN R3) -- temporal_ext's own
    signal-sender honest-miss counter, same precedent, 0 when temporal is inactive
    or every `.signal(...)` call resolved/was silently skipped as non-signal-shaped
    noise) -- everything the caller's own report dict still needs to assemble
    (service/files/defs/refs/malformed_ranges/degraded/reason/from_cache/mode/
    stale_files are all OUTSIDE this function's concern -- it only ever runs S5/S6
    and reports what THAT did).

    M7 T1 (class_attrs harvesting foundation, open-gaps R1/R2): a pre-loop pass,
    BEFORE the per-file python_core/domain-extractor loop below, harvests every
    `relpaths` file's class-body literals (`parsing.class_attrs.harvest_class_attrs`)
    into per-file "class_attrs" claims (`staging.add_claims`, the same claims table
    T6/T7/kafka/http_client already use -- no schema bump) and assembles the
    resulting SERVICE-WIDE `ClassAttrIndex` from `staging.claims_for("class_attrs",
    svc.name)` -- reading claims back rather than using the just-harvested list
    in-memory is what makes this identical in full and incremental mode: incremental
    `relpaths` is only the STALE subset, but `claims_for` still returns every
    unchanged file's PERSISTED claims alongside this call's freshly-rewritten stale
    ones (claims are per-file-keyed and `delete_file_layer` already wipes them by
    relpath before this runs -- see that method's own docstring). The two-pass split
    (harvest ALL of `relpaths` first, THEN loop) is load-bearing, not cosmetic: T2/T3
    (later in M7) will consume the index from WITHIN the same per-file loop that
    python_core/kafka_ext/http_client_ext already run in, so the index must be fully
    assembled before file 1 of that loop even starts -- a class defined in file B
    must be visible from file A's extractor call, including when A sorts (and is
    therefore processed) before B. `class_attr_index` is then threaded into every
    `FileContext` below (one field, same value, every file) -- this task itself reads
    it from nowhere else (no consumer yet, T2/T3 ship later)."""
    module_set = staging.module_set(svc.name)
    def_symbol_lookup = partial(staging.def_symbol_at, svc.name)
    ref_symbol_lookup = partial(staging.ref_symbol_at, svc.name)
    module_exists = module_set.__contains__

    for rp in relpaths:
        staging.add_claims(
            svc.name, rp, "class_attrs", harvest_class_attrs(rp, facts_by_file[rp]),
        )
    class_attr_index = build_class_attr_index(staging.claims_for("class_attrs", svc.name))
    fastapi_active = "fastapi" in active_idioms
    # T5: kafka is DATA-driven (effective ServiceIdioms), not active_idioms-gated --
    # see module docstring. idioms=None (default, matches active_idioms=frozenset()'s
    # own "no existing caller affected" convention) behaves like an empty ServiceIdioms.
    svc_idioms = idioms if idioms is not None else ServiceIdioms()
    kafka_active = bool(svc_idioms.producers or svc_idioms.consumers)
    temporal_active = "temporal" in active_idioms
    # T6: http_client, like kafka, is DATA-driven off the same effective idioms object
    # -- active_idioms plays no role here either.
    http_client_active = bool(svc_idioms.http_clients)
    domain_active = fastapi_active or kafka_active or temporal_active or http_client_active

    nodes = [make_service_node(svc.name)]
    edges = []
    imports_external = 0
    # M6 T2 review Important-1: http_client_ext's failure counters, aggregated across
    # files exactly like imports_external just above -- before this, hr.stats was
    # discarded entirely (only hr.claims was consumed), so a decorator-SDK method
    # whose route/verb couldn't be resolved vanished without a trace. Keys always
    # present (0 when the extractor is inactive) so both report shapes stay uniform.
    http_stats = {
        "http_url_unresolved": 0, "http_verb_unresolved": 0, "http_route_unresolved": 0,
    }
    # M6 T3: kafka_ext's own base_class honest-miss counter, aggregated across files
    # the SAME way -- before this, kr.stats was discarded entirely (only kr.roles/
    # channels/edges were consumed), so a class matching a base_class idiom's target
    # base but lacking a usable generic argument vanished without a trace.
    # M6 T4 (GAPS §6/pilot gap 5): producer_unresolved_channel joins the same dict,
    # same reasoning -- kafka_ext's producer paths (_emit_kafka_topic_produces/
    # _emit_event_type_produces) already bump this stat on any unresolved topic/
    # event_type value (a missing kwarg, a dynamic attribute expression like
    # `payload.topic_name`, ...); before this, only consumer_base_class_no_generic
    # was ever picked up from kr.stats, so a would-be PRODUCES claim that died this
    # way vanished from the report exactly as silently as the consumer-side gap did.
    kafka_stats = {"consumer_base_class_no_generic": 0, "producer_unresolved_channel": 0}
    # M7 T4 (OPEN R3): temporal_ext's own signal-sender honest-miss counter, same
    # aggregation precedent as http_stats/kafka_stats just above -- before this,
    # tr.stats was discarded entirely (only tr.roles/node_props/channels/edges/claims
    # were consumed), so a `.signal(...)` call site that looked like a genuine (if
    # unresolvable) signal reference vanished from the report without a trace.
    temporal_stats = {"signal_name_unresolved": 0}
    domain_roles: dict[str, set[str]] = {}
    domain_node_props: dict[str, dict] = {}
    domain_channels: list[NodeRec] = []
    domain_edges: list[EdgeRec] = []
    for rp in relpaths:
        ctx = FileContext(
            service=svc.name, relpath=rp, source=files[rp], facts=facts_by_file[rp],
            def_symbol_lookup=def_symbol_lookup, module_exists=module_exists,
            ref_symbol_lookup=ref_symbol_lookup, class_attr_index=class_attr_index,
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
            # T6 review fix: ONE ConstTable per file, shared by both consts-consuming
            # extractors (kafka + http_client) -- was built inside kafka's own branch
            # (and internally by extract_http_client), i.e. twice when both active.
            # M7 T4: temporal joins the same disjunction -- its own signal-sender arg0
            # resolution needs consts too (brief: "Consumes: consts (arg0-литерал
            # имени)"), same one-ConstTable-per-file sharing, now across three
            # extractors instead of two.
            consts = (
                ConstTable.build(facts_by_file[rp], files[rp])
                if kafka_active or http_client_active or temporal_active else None
            )

            if fastapi_active:
                fr = extract_fastapi(ctx, node_ids)
                for nid, rs in fr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                for nid, props in fr.node_props.items():
                    domain_node_props.setdefault(nid, {}).update(props)
                # M8 T1 (rerun-2 R4): DEPENDS_ON is the only edge type fastapi_ext
                # still emits directly (file-local, unchanged) -- route/HANDLES
                # identity now needs the FULL cross-file include_router chain, which
                # a single file can't see, so it's staged as two per-file claim
                # kinds instead and composed later by linking/router_prefix.py (S7,
                # consumed via staging.claims_for -- same pattern http_call/
                # temporal_start_mark already use).
                domain_edges.extend(fr.edges)
                staging.add_claims(svc.name, rp, "route_decl", fr.route_decl_claims)
                staging.add_claims(svc.name, rp, "router_include", fr.router_include_claims)

            if kafka_active:
                # consts is typed `ConstTable | None` above (the ternary's else-branch),
                # but extract_kafka declares its own `consts: ConstTable` parameter
                # (non-Optional) -- a real mismatch under a strict type-checker, even
                # though it can never actually BE None here: `kafka_active` being True
                # means the `if kafka_active or http_client_active` condition above was
                # already True, so consts was built, not left None. assert narrows the
                # type for the checker without changing runtime behavior at all (M2
                # final review, item 5).
                assert consts is not None
                kr = extract_kafka(ctx, node_ids, svc_idioms, consts)
                for nid, rs in kr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                domain_channels.extend(kr.channels)
                domain_edges.extend(kr.edges)
                for key in kafka_stats:
                    kafka_stats[key] += kr.stats[key]

            if temporal_active:
                # Same narrowing as the kafka_active/http_client_active branches above,
                # same reasoning: temporal_active being True is one of the three
                # disjuncts (M7 T4) that guarantee consts was built just above.
                assert consts is not None
                tr = extract_temporal(ctx, node_ids, consts)
                for nid, rs in tr.roles.items():
                    domain_roles.setdefault(nid, set()).update(rs)
                for nid, props in tr.node_props.items():
                    domain_node_props.setdefault(nid, {}).update(props)
                # M7 T4: signal/update handlers and their `.signal(...)` senders now
                # emit Channel(temporal_signal) nodes too -- temporal was the one
                # domain extractor with no `channels` output before this task.
                domain_channels.extend(tr.channels)
                domain_edges.extend(tr.edges)
                # temporal_start_mark: per-file claim, consumed later by S7 (T7) via
                # staging.claims_for + update_edge_props on the matching CALLS edge.
                staging.add_claims(svc.name, rp, "temporal_start_mark", tr.claims)
                for key in temporal_stats:
                    temporal_stats[key] += tr.stats[key]

            if http_client_active:
                # Same narrowing as the kafka_active branch above, same reasoning: this
                # boolean is one of the two disjuncts that guarantee consts was built.
                assert consts is not None
                hr = extract_http_client(ctx, node_ids, svc_idioms, consts)
                # http_call: per-file claim, consumed later by S7 (T7) via
                # staging.claims_for + the cross-service http_route table (CALLS_HTTP).
                staging.add_claims(svc.name, rp, "http_call", hr.claims)
                for key in http_stats:
                    http_stats[key] += hr.stats[key]

    if domain_roles or domain_node_props:
        nodes = [_apply_role_props_patch(n, domain_roles, domain_node_props) for n in nodes]
    nodes.extend(domain_channels)
    edges.extend(domain_edges)

    staging.upsert_nodes(nodes)
    # origin_service=svc.name (M2 final review fix): tags this WHOLE batch (python_core
    # + fastapi/kafka/temporal domain edges) as emitted by THIS service's own S5 run, so
    # a later begin_service(svc.name) (full path) or delete_file_layer(svc.name, ...)
    # (incremental path, M4 T5) can find and delete it on re-index -- regardless of
    # whether an individual edge's OWN src happens to be chan:-prefixed (HANDLES,
    # kafka CONTAINS), which carries no derivable service of its own (see
    # Staging.upsert_edges/begin_service docstrings for the bug this closes).
    staging.upsert_edges(edges, origin_service=svc.name)

    # 6. join: CALLS (static/1.0 либо heuristic/0.6 при деградации); local_defs_for_file
    # хоистится за пределы build_calls, чтобы не бить SQL на каждый local-символ-callsite —
    # per-relpath набор считается максимум один раз за вызов _extract_join_and_stage (т.е.
    # максимум один раз за вызов analyze_service — full и incremental вызывают её ровно
    # один раз каждый).
    @cache
    def local_defs_for_file(relpath: str) -> set[str]:
        return staging.local_def_symbols(svc.name, relpath)

    # M5 Task 1 (pilot Bug B): def_symbols -- the service-WIDE set every non-local ref
    # symbol's first-party status is now decided against (see calls.py's own docstring
    # for why parsed.package == service stopped being a reliable criterion). Hoisted
    # here the same way local_defs_for_file is -- ONE query per analyze_service call
    # (full or incremental, each calls _extract_join_and_stage exactly once), not
    # per-file/per-call-site -- and, load-bearingly, called AFTER S4 (`add_defs`/
    # `read_scip_into_staging` above, or the degraded fallback's own `add_defs`) has
    # already written this run's defs: def_symbols must see the FRESH staged defs, not
    # a stale set from before this analyze call.
    def_symbols = staging.def_symbols(svc.name)

    resolution, confidence = ("heuristic", 0.6) if degraded else ("static", 1.0)
    join_stats = build_calls(
        svc.name, staging, facts_by_file, def_symbol_lookup, def_symbols,
        local_defs_for_file=local_defs_for_file,
        resolution=resolution, confidence=confidence,
    )

    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "imports_external": imports_external,
        "join_stats": join_stats,
        **http_stats,
        **kafka_stats,
        **temporal_stats,
    }


def _analyze_full(
    svc: ServiceConfig,
    staging: Staging,
    cache_dir: Path,
    runner: ScipRunner,
    active_idioms: frozenset[str],
    idioms: ServiceIdioms | None,
) -> dict:
    """The pre-M4-T5 analyze_service body, unchanged: begin_service -> scan ->
    add_files -> facts for ALL files -> resolve (real SCIP or degraded fallback) ->
    S5/S6 over ALL files. Called directly for `incremental=False` (today's default,
    every existing caller) AND as the incremental branch's own fallback when
    scip-python fails mid-attempt -- both routes report mode="full" (added by the
    caller, `analyze_service`), the ONLY new key relative to before M4 T5."""
    # 1. begin_service; scan -> add_files
    staging.begin_service(svc.name)
    rows, tree_hash = scan_service(svc.path, svc.exclude)
    staging.add_files(svc.name, rows)
    relpaths = [rp for rp, _, _ in rows]  # scan_service гарантирует сортировку

    files = {rp: (svc.path / rp).read_bytes() for rp in relpaths}
    # 4. facts по отсортированным relpath (bytes из files выше). Вычислены здесь, ДО resolve
    # (шаг 2), потому что деградированный fallback (шаг 3) тоже нуждается в facts_by_file —
    # строим единожды и переиспользуем в шаге 3 (если degraded) и шаге 5 (всегда).
    facts_by_file = _build_facts_by_file(relpaths, files)

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

    # 5+6: extract (python_core + domain extractors) + join (CALLS), shared with the
    # incremental path -- see _extract_join_and_stage's own docstring.
    ej = _extract_join_and_stage(
        svc, staging, relpaths, files, facts_by_file, active_idioms, idioms, degraded,
    )

    # 7. отчёт ("service" -- первым ключом: report.build_report ожидает его в каждом
    # per_service-элементе, и его источник -- сам _analyze_full, не вызывающая сторона)
    return {
        "service": svc.name,
        "files": len(relpaths),
        "defs": defs_count,
        "refs": refs_count,
        "malformed_ranges": malformed_ranges,
        "nodes": ej["nodes"],
        "edges": ej["edges"],
        "imports_external": ej["imports_external"],
        "calls_joined": ej["join_stats"].calls_joined,
        "calls_unresolved": ej["join_stats"].calls_unresolved,
        "calls_external": ej["join_stats"].calls_external,
        "http_url_unresolved": ej["http_url_unresolved"],
        "http_verb_unresolved": ej["http_verb_unresolved"],
        "http_route_unresolved": ej["http_route_unresolved"],
        "consumer_base_class_no_generic": ej["consumer_base_class_no_generic"],
        "producer_unresolved_channel": ej["producer_unresolved_channel"],
        "signal_name_unresolved": ej["signal_name_unresolved"],
        "degraded": degraded,
        "reason": reason,
        "from_cache": from_cache,
    }


def _skip_report(svc: ServiceConfig, staging: Staging) -> dict:
    """mode="skipped": prior_delta.empty AND the config fingerprint still matches --
    nothing to do. Does ZERO staging writes; every count comes straight from
    `counts_for_service` (SQL COUNT scoped to this service, per the brief), i.e.
    whatever this service's staged state already was BEFORE this call, from
    whichever earlier full/incremental run produced it."""
    c = staging.counts_for_service(svc.name)
    return {
        "service": svc.name,
        "files": c["files"],
        "defs": c["defs"],
        "refs": c["refs"],
        "malformed_ranges": 0,
        "nodes": c["nodes"],
        "edges": c["edges"],
        "imports_external": 0,
        "calls_joined": 0,
        "calls_unresolved": 0,
        "calls_external": 0,
        # M6/M7 counters: hardcoded zeros like the pre-M6 stats above, keeping all
        # three report shapes (full/incremental/skipped) key-uniform -- a skipped
        # service ran no extractors, so every miss-counter is definitionally 0
        # (M6 final review, Minor-1: report.py's .get() tolerated the omission,
        # but a future direct-index consumer would KeyError only on this shape).
        "http_url_unresolved": 0,
        "http_verb_unresolved": 0,
        "http_route_unresolved": 0,
        "consumer_base_class_no_generic": 0,
        "producer_unresolved_channel": 0,
        # M7 T4 (OPEN R3): same precedent, temporal's own signal-sender counter.
        "signal_name_unresolved": 0,
        "degraded": False,
        "reason": None,
        "from_cache": False,
        "mode": "skipped",
    }


def _analyze_incremental(
    svc: ServiceConfig,
    staging: Staging,
    cache_dir: Path,
    runner: ScipRunner,
    active_idioms: frozenset[str],
    idioms: ServiceIdioms | None,
) -> dict:
    """The real incremental algorithm -- see this module's own docstring for the
    full step-by-step correctness argument and ordering rationale. Reached only when
    the skip precondition (prior_delta.empty and fingerprint_ok) did NOT hold."""
    # 1. snapshot BEFORE clear_scip_layer wipes files/scip_refs. The class_attrs
    # digest (M7 T1 review Important-1) must also be snapshotted HERE -- before
    # step 7's delete_file_layer wipes the stale files' claim rows, or the "before"
    # side of the escalation comparison (step 8a) would already be missing them.
    old_files = dict(staging.files_for_service(svc.name))
    old_refs = staging.refs_hash_by_file(svc.name)
    old_class_attrs_digest = _class_attrs_digest(staging, svc.name)

    # 2-3. make room, then re-scan+add_files fresh (same source of truth the full
    # path always re-scans from) and diff against the just-snapshotted OLD state.
    staging.clear_scip_layer(svc.name)
    rows, tree_hash = scan_service(svc.path, svc.exclude)
    staging.add_files(svc.name, rows)
    delta = service_delta(old_files, rows)

    # 4. S3+S4, full, over the fresh scan -- same calls _analyze_full itself makes.
    try:
        result = runner.run(svc.name, svc.path, _venv_for(svc), cache_dir, tree_hash)
        from_cache = result.from_cache
        reader_stats = read_scip_into_staging(result.scip_path, svc.name, svc.path, staging)
        defs_count = reader_stats.defs
        refs_count = reader_stats.refs
        malformed_ranges = reader_stats.malformed_ranges
    except ScipRunError:
        # No sound "degraded incremental" middle ground -- the heuristic fallback
        # resolver gives no stable refs-diff to compute ref_dirty from. Abandon this
        # attempt entirely and delegate to the ordinary full path, which will hit the
        # SAME ScipRunError again and degrade exactly as it always has.
        return {**_analyze_full(svc, staging, cache_dir, runner, active_idioms, idioms),
                "mode": "full"}

    # 5-6. ref_dirty over unchanged-content files only (added/changed are already
    # unconditionally stale); stale/dead.
    new_refs = staging.refs_hash_by_file(svc.name)
    ref_dirty = {rp for rp in delta.unchanged if old_refs.get(rp) != new_refs.get(rp)}
    stale = set(delta.changed) | set(delta.added) | ref_dirty
    dead = set(delta.deleted)

    # 7. narrow deletion -- BEFORE S5 (its own per-file add_claims calls must not
    # collide with a stale claim row left over from this same file's last analysis).
    staging.delete_file_layer(svc.name, stale | dead, drop_calls_evidence=stale | dead)

    # 8. S5+S6 over stale only -- facts built ONLY for stale (the entire point).
    stale_sorted = sorted(stale)
    files = {rp: (svc.path / rp).read_bytes() for rp in stale_sorted}
    facts_by_file = _build_facts_by_file(stale_sorted, files)

    # 8a. M7 T1 review Important-1 (settings-staleness hole, empirically proven):
    # refs_hash is blind to attribute VALUES -- editing a Settings default/env or an
    # enum member changes NOTHING about any consumer file's own content or refs, so
    # only the Settings file itself lands in `stale`, while every OTHER file's
    # T2/T3-derived edges (which bake class_attrs VALUES in) would silently stay
    # stale forever. Detection: harvest the stale files' class_attrs claims NOW
    # (step 7 already wiped their old rows, so this write is the clean post-state)
    # and compare the SERVICE-WIDE digest (stale fresh + unchanged persisted; a dead
    # file's wiped rows correctly read as a change too) against the step-1 snapshot.
    # Changed AND there are non-stale files left to protect -> escalate THIS run to
    # a full re-extract: widen stale to ALL scanned files, wipe + rebuild
    # files/facts for the widened set, and report it (stale_escalation). The
    # widened `_extract_join_and_stage` call re-harvests every file's claims itself
    # (its own pre-loop pass; the delete below wipes the rows this check just
    # wrote), so claims end up complete either way -- the double harvest of the
    # original stale files is a couple of cheap pure-Python loops, not a re-parse.
    # No widening needed when stale already covers every scanned file (e.g. a
    # first-run incremental, where the digest ALWAYS moves from empty-to-populated):
    # the marker means "this run re-extracted more than the file delta demanded",
    # not "the digest moved".
    stale_escalation: str | None = None
    for rp in stale_sorted:
        staging.add_claims(
            svc.name, rp, "class_attrs", harvest_class_attrs(rp, facts_by_file[rp]),
        )
    if _class_attrs_digest(staging, svc.name) != old_class_attrs_digest:
        all_scanned = {rp for rp, _, _ in rows}
        if all_scanned - stale:
            stale_escalation = "class_attrs_changed"
            stale = all_scanned
            stale_sorted = sorted(stale)
            staging.delete_file_layer(
                svc.name, stale | dead, drop_calls_evidence=stale | dead,
            )
            files = {rp: (svc.path / rp).read_bytes() for rp in stale_sorted}
            facts_by_file = _build_facts_by_file(stale_sorted, files)

    ej = _extract_join_and_stage(
        svc, staging, stale_sorted, files, facts_by_file, active_idioms, idioms,
        degraded=False,
    )

    return {
        "service": svc.name,
        "files": len(rows),
        "defs": defs_count,
        "refs": refs_count,
        "malformed_ranges": malformed_ranges,
        "nodes": ej["nodes"],
        "edges": ej["edges"],
        "imports_external": ej["imports_external"],
        "calls_joined": ej["join_stats"].calls_joined,
        "calls_unresolved": ej["join_stats"].calls_unresolved,
        "calls_external": ej["join_stats"].calls_external,
        "http_url_unresolved": ej["http_url_unresolved"],
        "http_verb_unresolved": ej["http_verb_unresolved"],
        "http_route_unresolved": ej["http_route_unresolved"],
        "consumer_base_class_no_generic": ej["consumer_base_class_no_generic"],
        "producer_unresolved_channel": ej["producer_unresolved_channel"],
        "signal_name_unresolved": ej["signal_name_unresolved"],
        "degraded": False,
        "reason": None,
        "from_cache": from_cache,
        "mode": "incremental",
        "stale_files": len(stale),
        # T7 (CLI --incremental): the CALLER needs the actual stale relpath SET, not
        # just its count, to scope S8's chunk_embed(..., changed_files=...) -- this is
        # exactly `changed | added | ref_dirty` (== `stale`, see step 6 above), already
        # sorted (reuses `stale_sorted` from step 8, no recomputation). Deliberately
        # excludes `dead` (deleted files no longer exist to chunk -- T7's own contract
        # note: "NOT deleted"). Absent from BOTH the full and skipped report shapes
        # (there is no "stale set" concept in either -- see
        # test_analyze_full_and_skipped_reports_have_no_stale_relpaths_key).
        #
        # M7 T1 review Important-1: under a class_attrs escalation (step 8a) this IS
        # the WIDENED set (all scanned files) -- load-bearing for S8 coherence, not
        # just reporting: delete_file_layer wiped the widened set's chunk rows too,
        # so chunk_embed must re-chunk exactly this set.
        "stale_relpaths": tuple(stale_sorted),
        # M7 T1 review Important-1: non-None ("class_attrs_changed") iff step 8a
        # actually WIDENED the stale set beyond what the file delta demanded --
        # incremental-shape-only key, same family as stale_files/stale_relpaths.
        "stale_escalation": stale_escalation,
    }


def analyze_service(
    svc: ServiceConfig,
    staging: Staging,
    cache_dir: Path,
    runner: ScipRunner | None = None,
    active_idioms: frozenset[str] = frozenset(),
    idioms: ServiceIdioms | None = None,
    incremental: bool = False,
    prior_delta: ServiceDelta | None = None,
    fingerprint_ok: bool = True,
) -> dict:
    """Dispatches to one of three modes (report["mode"]): "full" (incremental=False,
    the default -- every pre-existing caller is unaffected), "skipped" (incremental
    branch's own precondition: `prior_delta.empty and fingerprint_ok`), or
    "incremental" (otherwise, once incremental=True). See this module's own
    docstring for the full incremental design and correctness argument.

    `prior_delta`/`fingerprint_ok` are consulted ONLY for the skip decision --
    analyze_service does not own fingerprint persistence (T7 does: it reads/writes
    the stored fingerprint and computes the current one via
    `pipeline.diff.config_fingerprint`, then passes the boolean comparison result in
    as `fingerprint_ok`). The incremental branch itself, once entered, computes its
    OWN fresh delta internally (`service_delta` against a `files_for_service`
    snapshot taken before any wipe) rather than trusting `prior_delta` for the
    actual stale-set math -- by the time this call runs, `prior_delta` may already
    be stale itself (computed by the caller from an earlier scan). `prior_delta`
    absent (None) simply means skip can never be evaluated, not an error."""
    runner = runner if runner is not None else ScipRunner()

    if not incremental:
        return {**_analyze_full(svc, staging, cache_dir, runner, active_idioms, idioms),
                "mode": "full"}

    if prior_delta is not None and prior_delta.empty and fingerprint_ok:
        return _skip_report(svc, staging)

    return _analyze_incremental(svc, staging, cache_dir, runner, active_idioms, idioms)
