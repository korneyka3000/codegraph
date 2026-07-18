"""M4 T7 gate: `codegraph index --incremental`, dump-equivalence + perf, end to end
through the REAL CLI (`typer.testing.CliRunner`, in-process -- no monkeypatching, real
scip-python + a live FalkorDB) against tmp copies of all three fixture services. The
supreme M4 invariant this gate exists to prove: staging + FalkorDB state after an
edit-then-`--incremental` run is byte-identical to a FULL reindex of the SAME edited
tree from scratch.

`--no-embed` throughout: `FakeEmbedder` (used everywhere else in this codebase to keep
retrieval tests offline) is wired in only via `GraphQuery(embedder_factory=...)` or a
monkeypatched `codegraph.cli.make_embedder` -- neither applies to a genuine end-to-end
`codegraph index` CliRunner invocation, which always resolves a REAL embedder factory
from `cfg.embedding` (local/openai/voyage). `--no-embed` still builds chunks + graph-
aware headers (S8's chunk loop and `augment.fill_headers_all` run unconditionally --
only the embed pass itself is skipped), so the equivalence this gate proves covers
everything except embedding vectors themselves (which stay NULL throughout, trivially
equal either side of the comparison). This is a deliberate, disclosed scope reduction
(see this task's own report) -- NOT a weakening of the staging/graph structural
equivalence claim, which is exactly what --incremental risks breaking.

Markers: scip (real scip-python via npx) + falkordb (live instance, `docker compose up
-d`) -- no `emb` marker, since no embedding provider is ever exercised.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from codegraph.cli import app
from codegraph.core import ids
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = [pytest.mark.scip, pytest.mark.falkordb]

FIXTURES = Path(__file__).parents[2] / "fixtures"
SERVICES_SRC = FIXTURES / "services"
SOURCE_WORKSPACE = FIXTURES / "workspace.yaml"

GRAPH_NAME = "__m4_incremental_gate__"

runner = CliRunner()


# ============================================================================
# -- workspace/service setup: tmp copies, sibling workspaces --
# ============================================================================


def _copy_services(dest_root: Path) -> dict[str, Path]:
    """tmp-copy of ALL THREE fixture services -- returns {service_name: abs_path},
    derived from the SOURCE workspace.yaml's own `path` basenames (not hardcoded), so
    a future rename of a fixture directory can't silently desync this mapping."""
    services_dest = dest_root / "services"
    shutil.copytree(SERVICES_SRC, services_dest)
    raw = yaml.safe_load(SOURCE_WORKSPACE.read_text())
    return {svc["name"]: services_dest / Path(svc["path"]).name for svc in raw["services"]}


def _write_workspace_yaml(dest_dir: Path, service_dirs: dict[str, Path], graph_name: str) -> Path:
    """A workspace.yaml pointing at the given (already-copied) service directories --
    idioms/http/processes carried over VERBATIM from fixtures/workspace.yaml, only
    `path` (rewritten to each service_dirs[name], absolute) and `graph_name` (this
    call's own scope-isolating identity) are touched. Multiple calls with DIFFERENT
    dest_dir/graph_name but the SAME service_dirs let a full reindex-from-scratch run
    (its own fresh `.codegraph`, own graph) happen against the identical on-disk
    source tree an `--incremental` run elsewhere is also using, without either one
    disturbing the other's staging.db/graph."""
    raw = yaml.safe_load(SOURCE_WORKSPACE.read_text())
    raw["graph_name"] = graph_name
    for svc in raw["services"]:
        svc["path"] = str(service_dirs[svc["name"]])
    dest_dir.mkdir(parents=True, exist_ok=True)
    ws_path = dest_dir / "workspace.yaml"
    ws_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return ws_path


def _invoke_index(ws_path: Path, *, incremental: bool = False) -> tuple[float, object]:
    args = ["index", str(ws_path), "--no-embed"]
    if incremental:
        args.append("--incremental")
    t0 = time.perf_counter()
    result = runner.invoke(app, args)
    elapsed = time.perf_counter() - t0
    assert result.exit_code == 0, result.output
    return elapsed, result


def _load_report(ws_path: Path) -> dict:
    return json.loads((ws_path.parent / ".codegraph" / "report.json").read_text())


def _assert_not_degraded(report: dict) -> None:
    degraded = [s["service"] for s in report["services"] if s.get("degraded")]
    assert not degraded, f"real scip expected for all fixture services, degraded: {degraded}"


# ============================================================================
# -- canonical dumps --
# ============================================================================


def _freeze(value):
    """Recursively converts dict/list into hashable, canonically-ordered tuples --
    e.g. a route handler's `decorators` prop is a plain `list[str]` (see pipeline/
    load.py's own module docstring: "Списки строк (decorators)... отправляются как
    есть"). A plain `tuple(sorted(d.items()))` (one level only) still leaves such a
    LIST nested inside, which is unhashable -- fine for the `==` equality assert
    itself (list `==`/`<` both work without hashing), but breaks `_dump_diff`'s own
    `set(...)` symmetric-difference debug aid below. Freezing all the way down avoids
    that trap entirely and costs nothing for the (overwhelmingly common) all-scalar
    props case."""
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def _props(raw_json: str) -> tuple:
    """Parses a props/labels JSON column back into a canonically-sorted tuple rather
    than comparing raw TEXT: `staging.py`'s `json.dumps(...)` calls for nodes/edges
    do NOT pass `sort_keys=True`, so two logically-identical props dicts built via
    different code paths (full-reindex vs incremental) could in principle serialize
    with a different key-insertion order -- normalizing here means only a GENUINE
    content difference can ever fail the equivalence assert below, never an
    incidental (harmless) ordering one."""
    return _freeze(json.loads(raw_json))


def _staging_dump(staging: Staging) -> dict:
    """Canonical, sorted dump of the ENTIRE workspace staging state -- nodes/edges
    read via RAW SQL (not the public `iter_nodes()`/`iter_edges()`), because those
    don't surface `origin_service`/`via_channel` at all (see `stores/staging.py`'s own
    `upsert_edges` docstring) -- both load-bearing for a genuine equivalence proof (a
    wrong `origin_service` on an incrementally-re-emitted edge would break a LATER
    re-index's own cleanup silently; a dropped `via_channel` would conflate two
    parallel channels' NEXT_SEGMENT edges). `chunks` -- every column EXCEPT the
    `embedding` BLOB itself, per the brief's own dump contract; `input_hash`/
    `embed_model` ARE included (M4 final review, MINOR-4 correction: NOT "both
    NULL-but-present under --no-embed" -- only `embed_model` is (no embedder ever ran
    to set it, trivially equal either side). `input_hash` is populated regardless --
    `fill_headers_all` computes and writes it unconditionally for every staged chunk
    whether or not an embedder ran this call, see `chunk_embed.py`'s own module
    docstring -- included here precisely BECAUSE it's populated: a non-NULL
    `input_hash` is what makes this dump actually cover the exact embedder INPUT
    (augmented text + header) a real embed pass would have hashed, not merely two
    sides that are trivially equal for being equally empty)."""
    db = staging._db  # noqa: SLF001 -- test-only max-rigor introspection, see docstring above.

    nodes = sorted(
        (id_, kind, labels, service, relpath, sb, eb, sl, el, name, qn, ch, _props(props))
        for (id_, kind, labels, service, relpath, sb, eb, sl, el, name, qn, ch, props) in
        db.execute(
            "SELECT id, kind, labels, service, relpath, start_byte, end_byte, start_line, "
            "end_line, name, qualified_name, content_hash, props FROM nodes"
        ).fetchall()
    )
    edges = sorted(
        (src, dst, type_, via, res, conf, ext, ef, el, _props(props), origin)
        for (src, dst, type_, via, res, conf, ext, ef, el, props, origin) in
        db.execute(
            "SELECT src, dst, type, via_channel, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props, origin_service FROM edges"
        ).fetchall()
    )
    chunks = sorted(
        db.execute(
            "SELECT chunk_id, symbol_id, service, relpath, ord, text, start_line, "
            "end_line, content_hash, context_header, embed_model, embedded_hash, "
            "input_hash FROM chunks"
        ).fetchall()
    )
    return {"nodes": nodes, "edges": edges, "chunks": chunks}


def _document_management_chunk_rows(staging: Staging) -> list:
    """Full chunk ROWS (every column, INCLUDING the embedding BLOB -- unlike
    `_staging_dump`) for document-management alone: the brief's own isolation check.
    This service is never touched by the orders-api-only edit between dump A and dump
    B, so its rows must come out byte-identical, blob included (NULL under --no-embed
    in both -- compared anyway per the brief's own instruction)."""
    db = staging._db  # noqa: SLF001
    return sorted(
        db.execute(
            "SELECT chunk_id, symbol_id, service, relpath, ord, text, start_line, "
            "end_line, content_hash, context_header, embedding, embed_model, "
            "embedded_hash, input_hash FROM chunks WHERE service=?",
            ("document-management",),
        ).fetchall()
    )


def _snapshot(ws_path: Path) -> tuple[dict, list]:
    staging = Staging(ws_path.parent / ".codegraph" / "staging.db")
    try:
        return _staging_dump(staging), _document_management_chunk_rows(staging)
    finally:
        staging.close()


def _graph_dump(store: FalkorStore) -> dict:
    """FalkorDB: stats() + sorted node ids + sorted edge (src_id, type, dst_id)
    triples -- `n.id`/`type(e)` are real, queryable properties/relationship-types for
    every loaded node/edge (confirmed via `pipeline/load.py`'s own `_NODE_CORE_FIELDS`
    including "id", and Cypher's own MERGE-pattern-property semantics)."""
    stats = store.stats()
    node_ids = sorted(row[0] for row in store.raw("MATCH (n) RETURN n.id").result_set)
    edge_triples = sorted(
        tuple(row)
        for row in store.raw("MATCH (a)-[e]->(b) RETURN a.id, type(e), b.id").result_set
    )
    return {"stats": stats, "node_ids": node_ids, "edge_triples": edge_triples}


def _dump_diff(a: dict, b: dict) -> str:
    """Debug aid for a failing equivalence assert: symmetric-difference summary per
    dump section, capped at 5 example rows each side -- raw `assert a == b` on a
    multi-thousand-tuple dict is otherwise unreadable."""
    lines = []
    for key in a:
        sa, sb = set(a[key]), set(b[key])
        only_a, only_b = sa - sb, sb - sa
        if only_a or only_b:
            lines.append(f"{key}: only-in-first={len(only_a)} only-in-second={len(only_b)}")
            for row in list(only_a)[:5]:
                lines.append(f"  -A {row}")
            for row in list(only_b)[:5]:
                lines.append(f"  +B {row}")
    return "\n".join(lines) if lines else "(dicts differ only in key set or ordering)"


# ============================================================================
# -- fixture-tree edits --
# ============================================================================


def _edit_orders_api_function_body(orders_api_dir: Path) -> None:
    """One function BODY edit (no rename, no signature change) -- `OrderService.get`
    in app/services/order.py."""
    order_service = orders_api_dir / "app" / "services" / "order.py"
    original = order_service.read_text()
    edited = original.replace('status="unknown"', 'status="not_found"')
    assert edited != original  # sanity: the replace actually matched something
    order_service.write_text(edited)


def _delete_and_add_file(kyc_worker_dir: Path) -> None:
    """Deletes app/consumer_main.py (confirmed unreferenced by any other fixture
    file -- an entrypoint module, same "nothing imports it" shape as
    tests/integration/test_analyze_incremental.py's own main.py deletion) and adds a
    brand new, trivially valid app/health.py."""
    (kyc_worker_dir / "app" / "consumer_main.py").unlink()
    (kyc_worker_dir / "app" / "health.py").write_text(
        "def healthcheck() -> bool:\n    return True\n"
    )


# -- M5 T4: residual-gap edits (closes the M4-T7 KNOWN RESIDUAL GAP for real --
# per-origin shared-edge rows, SCHEMA_VERSION 6, see core/schema.py's own history
# entry and stores/staging.py's upsert_edges docstring). Both edits below are
# plain, targeted CALL-SITE removals -- pure incremental FILE edits, never an
# idiom/config change (which would flip that service's fingerprint and force a
# FULL re-analyze instead, see pipeline/analyze.py's own module docstring).


def _remove_orders_api_producer_call_site(orders_api_dir: Path) -> None:
    """Removes the ONE call site orders-api's own `outbox` producer idiom
    (fixtures/workspace.yaml: event_type_from={arg: 0}, topic={const:
    "orders.events"}) matches -- `OrderService.place`'s `outbox.add_event(
    "OrderCreated", ...)` call in app/services/order.py. After this, orders-api's
    own re-extraction of this file no longer emits its PRODUCES(place ->
    event_type:OrderCreated) edge NOR its own row of the shared
    CONTAINS(kafka_topic:orders.events -> event_type:OrderCreated) edge -- while
    kyc-worker's INDEPENDENTLY-derived row for the IDENTICAL CONTAINS edge (from
    its own dispatch_dict consumer idiom, app/consumers/orders.py) is left
    completely untouched, exactly the M4-T7 residual-gap scenario."""
    order_service = orders_api_dir / "app" / "services" / "order.py"
    original = order_service.read_text()
    edited = original.replace(
        '        outbox = OutboxRepository(self._db)\n'
        '        await outbox.add_event(\n'
        '            "OrderCreated",\n'
        '            {"order_id": order.id, "customer_id": order.customer_id},\n'
        '        )\n',
        "",
    )
    assert edited != original  # sanity: the replace actually matched something
    order_service.write_text(edited)


def _remove_kyc_worker_consumer_registration(kyc_worker_dir: Path) -> None:
    """Removes kyc-worker's ONE dispatch_dict registration call site
    (`register_handlers({"OrderCreated": handle_order_created})`,
    app/consumers/orders.py) -- the call its own `dispatch-map` consumer idiom
    matches. After both this AND `_remove_orders_api_producer_call_site` above have
    run, NEITHER service asserts the shared CONTAINS edge any more -- it must
    vanish from staging AND the graph entirely."""
    consumers_orders = kyc_worker_dir / "app" / "consumers" / "orders.py"
    original = consumers_orders.read_text()
    edited = original.replace(
        'register_handlers({"OrderCreated": handle_order_created})\n', "",
    )
    assert edited != original
    consumers_orders.write_text(edited)


def _contains_pairs_with_origin(dump: dict) -> dict[tuple[str, str], set[str]]:
    """(src,dst) -> {origin_service, ...} for every staged CONTAINS row -- local
    helper for the residual-gap assertions below, built off `_staging_dump`'s own
    edge tuple shape (src,dst,type,via,res,conf,ext,ef,el,props,origin)."""
    out: dict[tuple[str, str], set[str]] = {}
    for src, dst, type_, *_rest, origin in dump["edges"]:
        if type_ == "CONTAINS":
            out.setdefault((src, dst), set()).add(origin)
    return out


# ============================================================================
# -- the gate --
# ============================================================================


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_incremental_dump_equivalence_and_perf(tmp_path, falkordb_cfg, record_property):
    service_dirs = _copy_services(tmp_path / "src")
    ws_path = _write_workspace_yaml(tmp_path / "main", service_dirs, GRAPH_NAME)

    graph_names: set[str] = set()

    def _store(name: str) -> FalkorStore:
        graph_names.add(name)
        return FalkorStore(falkordb_cfg, name)

    try:
        # ==================================================================
        # 1. Full index (fresh .codegraph, fresh graph) -> dump A.
        # ==================================================================
        t_full_cold_initial, _ = _invoke_index(ws_path)
        report_a = _load_report(ws_path)
        _assert_not_degraded(report_a)
        assert all(s["mode"] == "full" for s in report_a["services"])

        dump_a, dm_chunks_a = _snapshot(ws_path)
        graph_a = _graph_dump(_store(GRAPH_NAME))
        assert dump_a["nodes"] and dump_a["edges"] and dump_a["chunks"], (
            "empty staging dump -- broken fixture copy or pipeline?"
        )
        assert graph_a["node_ids"], "empty graph dump -- broken load?"

        # ==================================================================
        # 2/3. Edit ONE function body in orders-api -> --incremental -> dump B.
        # Full reindex from scratch (sibling workspace, fresh .codegraph/staging.db,
        # own graph) of the SAME edited service_dirs -> dump C. ASSERT B == C.
        # ==================================================================
        _edit_orders_api_function_body(service_dirs["orders-api"])

        t_incremental_edit, _ = _invoke_index(ws_path, incremental=True)
        report_b = _load_report(ws_path)
        _assert_not_degraded(report_b)
        modes_b = {s["service"]: s["mode"] for s in report_b["services"]}
        assert modes_b["orders-api"] == "incremental"
        assert modes_b["document-management"] == "skipped"
        assert modes_b["kyc-worker"] == "skipped"

        dump_b, dm_chunks_b = _snapshot(ws_path)
        graph_b = _graph_dump(_store(GRAPH_NAME))

        full_c_name = f"{GRAPH_NAME}__full_c"
        ws_c = _write_workspace_yaml(tmp_path / "full_check_1", service_dirs, full_c_name)
        # This IS the brief's "full-cold = fresh staging + cleared scip cache same
        # edited copy" measurement, for free: ws_c's OWN .codegraph (incl. its scip
        # cache subdir) does not exist before this call -- genuinely cold -- and it
        # points at the identical, already-edited service_dirs `--incremental` above
        # just ran against. Reused below as this gate's t_full_cold_edited.
        t_full_cold_edited, _ = _invoke_index(ws_c)
        report_c = _load_report(ws_c)
        _assert_not_degraded(report_c)
        assert all(s["mode"] == "full" for s in report_c["services"])

        dump_c, _ = _snapshot(ws_c)
        graph_c = _graph_dump(_store(full_c_name))

        assert dump_b == dump_c, (
            "SUPREME INVARIANT violated: --incremental staging state != full reindex "
            f"of the same edited tree.\n{_dump_diff(dump_b, dump_c)}"
        )
        assert graph_b == graph_c, (
            f"SUPREME INVARIANT violated (FalkorDB): {_dump_diff(graph_b, graph_c)}"
        )

        # ==================================================================
        # 4. Isolation: document-management chunk rows (incl. embedding BLOBs --
        # NULL under --no-embed, compared anyway) byte-identical between A and B.
        # ==================================================================
        assert dm_chunks_a == dm_chunks_b, (
            "document-management chunks changed even though only orders-api was "
            "edited -- incremental scoping leaked across services"
        )

        # ==================================================================
        # 5. Delete a file + add a file (kyc-worker) -> --incremental -> dump ==
        # fresh-full dump again (dump D vs dump E, same pattern as B vs C).
        # ==================================================================
        _delete_and_add_file(service_dirs["kyc-worker"])

        _invoke_index(ws_path, incremental=True)
        report_d = _load_report(ws_path)
        _assert_not_degraded(report_d)
        modes_d = {s["service"]: s["mode"] for s in report_d["services"]}
        assert modes_d["kyc-worker"] == "incremental"
        assert modes_d["orders-api"] == "skipped"
        assert modes_d["document-management"] == "skipped"

        dump_d, _ = _snapshot(ws_path)
        graph_d = _graph_dump(_store(GRAPH_NAME))

        full_e_name = f"{GRAPH_NAME}__full_e"
        ws_e = _write_workspace_yaml(tmp_path / "full_check_2", service_dirs, full_e_name)
        _invoke_index(ws_e)
        report_e = _load_report(ws_e)
        _assert_not_degraded(report_e)

        dump_e, _ = _snapshot(ws_e)
        graph_e = _graph_dump(_store(full_e_name))

        assert dump_d == dump_e, (
            "SUPREME INVARIANT violated after delete+add: "
            f"{_dump_diff(dump_d, dump_e)}"
        )
        assert graph_d == graph_e, (
            f"SUPREME INVARIANT violated after delete+add (FalkorDB): "
            f"{_dump_diff(graph_d, graph_e)}"
        )
        # a deleted file's node/evidence must not survive anywhere in the dump.
        assert not any(
            row[4] == "app/consumer_main.py" for row in dump_d["nodes"]
        ), "deleted file's nodes survived incremental re-analyze"

        # ==================================================================
        # 6. No-change --incremental -> all services mode=skipped; report shows it;
        # faster than the INITIAL full (loose sanity, not the strict perf gate).
        # ==================================================================
        t_noop, _ = _invoke_index(ws_path, incremental=True)
        report_f = _load_report(ws_path)
        assert all(s["mode"] == "skipped" for s in report_f["services"]), report_f["services"]
        assert t_noop < t_full_cold_initial, (
            f"no-op --incremental ({t_noop:.2f}s) not faster than the initial full "
            f"index ({t_full_cold_initial:.2f}s)"
        )

        # ==================================================================
        # 7. Perf gate: t_incremental (1-file edit, warm caches) < 0.5 * t_full_cold
        # (fresh staging + cleared scip cache, SAME edited copy) -- reuses the two
        # measurements already taken above (step 2/3's own --incremental call and
        # ws_c's own fresh-from-scratch call), per the plan's documented <50%
        # threshold (a deliberate relaxation from the master plan's <20%: scip-python
        # is not file-incremental, see the M4 plan's Global Constraints).
        # ==================================================================
        ratio = t_incremental_edit / t_full_cold_edited
        print(
            f"\n[M4 T7 perf gate] t_full_cold_edited={t_full_cold_edited:.2f}s "
            f"t_incremental(1 file edit, warm)={t_incremental_edit:.2f}s "
            f"ratio={ratio:.1%} (threshold: <50%)"
        )
        record_property("m4_t_full_cold_edited_seconds", f"{t_full_cold_edited:.3f}")
        record_property("m4_t_incremental_edit_seconds", f"{t_incremental_edit:.3f}")
        record_property("m4_incremental_vs_full_ratio", f"{ratio:.3f}")
        assert t_incremental_edit < 0.5 * t_full_cold_edited, (
            f"perf gate failed: t_incremental={t_incremental_edit:.2f}s is not < 50% "
            f"of t_full_cold={t_full_cold_edited:.2f}s (ratio={ratio:.1%})"
        )
    finally:
        for name in graph_names:
            FalkorStore(falkordb_cfg, name).delete_graph()
            FalkorStore(falkordb_cfg, f"{name}__build").delete_graph()


# ============================================================================
# -- M5 T4: residual-gap sub-case (closes the M4-T7 KNOWN RESIDUAL GAP for real,
# per-origin shared-edge rows, SCHEMA_VERSION 6 -- see core/schema.py's own
# history entry and stores/staging.py's upsert_edges docstring) --
# ============================================================================

RESIDUAL_GAP_GRAPH_NAME = "__m5_t4_residual_gap_gate__"
ORDERS_TOPIC = ids.chan_kafka("orders.events")
ORDER_CREATED_EVENT = ids.chan_event("OrderCreated")


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_residual_gap_shared_edge_survives_sibling_removal_then_vanishes_when_both_stop(
    tmp_path, falkordb_cfg, record_property,
):
    """M4-T7's own KNOWN RESIDUAL GAP scenario, reproduced live through the REAL
    CLI and closed for good by this task's per-origin edge rows: kafka CONTAINS
    (chan:kafka_topic:orders.events -> chan:event_type:OrderCreated) is asserted by
    BOTH orders-api (its `outbox` producer idiom, app/services/order.py) AND
    kyc-worker (its `dispatch-map` consumer idiom, app/consumers/orders.py) --
    independently, each from its own idiom config (see fixtures/workspace.yaml).

      1. Full index (all 3 fixtures) -> the CONTAINS edge is present in the graph;
         staging holds ONE row per origin (both "orders-api" and "kyc-worker").
      2. Remove ONLY orders-api's producing CALL SITE (a plain incremental file
         edit, not an idiom/config change) -> `--incremental` (kyc-worker is
         SKIPPED, never reprocessed) -> the CONTAINS edge MUST SURVIVE, because
         kyc-worker's own row was never touched by orders-api's re-index. Under
         the PRE-M5-T4 single-row scheme this is exactly the bug M4 T7 left open:
         whichever origin "owned" the one shared row, losing it on a re-index left
         nothing behind for the still-asserting sibling to inherit. Dump-
         equivalence against a fresh full reindex of the identically-edited tree
         must still hold.
      3. THEN also remove kyc-worker's consumer registration -> `--incremental`
         (orders-api now SKIPPED in turn) -> the edge is finally gone, since
         NEITHER service asserts it any more. Dump-equivalence against a fresh
         full reindex of the now-doubly-edited tree must still hold.
    """
    service_dirs = _copy_services(tmp_path / "src")
    ws_path = _write_workspace_yaml(tmp_path / "main", service_dirs, RESIDUAL_GAP_GRAPH_NAME)

    graph_names: set[str] = set()

    def _store(name: str) -> FalkorStore:
        graph_names.add(name)
        return FalkorStore(falkordb_cfg, name)

    try:
        # ==================================================================
        # 1. Full index -> the shared CONTAINS edge exists, one row per origin.
        # ==================================================================
        _invoke_index(ws_path)
        report_a = _load_report(ws_path)
        _assert_not_degraded(report_a)
        assert all(s["mode"] == "full" for s in report_a["services"])

        dump_a, _ = _snapshot(ws_path)
        contains_a = _contains_pairs_with_origin(dump_a)
        assert (ORDERS_TOPIC, ORDER_CREATED_EVENT) in contains_a, (
            "fixture precondition: orders-api + kyc-worker must both assert the "
            f"shared CONTAINS edge before either is edited; staged CONTAINS pairs: "
            f"{sorted(contains_a)}"
        )
        assert contains_a[(ORDERS_TOPIC, ORDER_CREATED_EVENT)] == {"orders-api", "kyc-worker"}, (
            "fixture precondition: expected a per-origin row from BOTH services, got "
            f"{contains_a[(ORDERS_TOPIC, ORDER_CREATED_EVENT)]}"
        )

        graph_a = _graph_dump(_store(RESIDUAL_GAP_GRAPH_NAME))
        assert (ORDERS_TOPIC, "CONTAINS", ORDER_CREATED_EVENT) in graph_a["edge_triples"]

        # ==================================================================
        # 2. orders-api stops asserting the edge (file edit, not config) ->
        # --incremental (kyc-worker skipped) -> the edge MUST survive via
        # kyc-worker's own row. Dump-equivalence vs a fresh full reindex of the
        # SAME edited tree.
        # ==================================================================
        _remove_orders_api_producer_call_site(service_dirs["orders-api"])

        _invoke_index(ws_path, incremental=True)
        report_b = _load_report(ws_path)
        _assert_not_degraded(report_b)
        modes_b = {s["service"]: s["mode"] for s in report_b["services"]}
        assert modes_b["orders-api"] == "incremental"
        assert modes_b["kyc-worker"] == "skipped"
        assert modes_b["document-management"] == "skipped"

        dump_b, _ = _snapshot(ws_path)
        contains_b = _contains_pairs_with_origin(dump_b)
        assert (ORDERS_TOPIC, ORDER_CREATED_EVENT) in contains_b, (
            "RESIDUAL GAP REGRESSION: the shared CONTAINS edge vanished after "
            "orders-api's own re-index, even though kyc-worker (skipped this run) "
            f"still legitimately asserts it. staged CONTAINS pairs: {sorted(contains_b)}"
        )
        assert contains_b[(ORDERS_TOPIC, ORDER_CREATED_EVENT)] == {"kyc-worker"}, (
            "expected only kyc-worker's own row to survive, got "
            f"{contains_b[(ORDERS_TOPIC, ORDER_CREATED_EVENT)]}"
        )

        graph_b = _graph_dump(_store(RESIDUAL_GAP_GRAPH_NAME))
        assert (ORDERS_TOPIC, "CONTAINS", ORDER_CREATED_EVENT) in graph_b["edge_triples"], (
            "RESIDUAL GAP REGRESSION (FalkorDB): the shared CONTAINS edge is missing "
            "from the loaded graph even though kyc-worker still asserts it in staging"
        )

        full_c_name = f"{RESIDUAL_GAP_GRAPH_NAME}__full_c"
        ws_c = _write_workspace_yaml(tmp_path / "full_check_1", service_dirs, full_c_name)
        _invoke_index(ws_c)
        report_c = _load_report(ws_c)
        _assert_not_degraded(report_c)
        assert all(s["mode"] == "full" for s in report_c["services"])

        dump_c, _ = _snapshot(ws_c)
        graph_c = _graph_dump(_store(full_c_name))

        assert dump_b == dump_c, (
            "SUPREME INVARIANT violated (residual-gap stage 1): --incremental "
            f"staging state != full reindex of the same edited tree.\n{_dump_diff(dump_b, dump_c)}"
        )
        assert graph_b == graph_c, (
            "SUPREME INVARIANT violated (residual-gap stage 1, FalkorDB): "
            f"{_dump_diff(graph_b, graph_c)}"
        )

        # ==================================================================
        # 3. kyc-worker ALSO stops asserting the edge -> --incremental
        # (orders-api now skipped in turn) -> the edge is finally gone entirely.
        # Dump-equivalence vs a fresh full reindex of the now-doubly-edited tree.
        # ==================================================================
        _remove_kyc_worker_consumer_registration(service_dirs["kyc-worker"])

        _invoke_index(ws_path, incremental=True)
        report_d = _load_report(ws_path)
        _assert_not_degraded(report_d)
        modes_d = {s["service"]: s["mode"] for s in report_d["services"]}
        assert modes_d["kyc-worker"] == "incremental"
        assert modes_d["orders-api"] == "skipped"
        assert modes_d["document-management"] == "skipped"

        dump_d, _ = _snapshot(ws_path)
        contains_d = _contains_pairs_with_origin(dump_d)
        assert (ORDERS_TOPIC, ORDER_CREATED_EVENT) not in contains_d, (
            "the shared CONTAINS edge survived even though NEITHER service asserts "
            f"it any more; staged CONTAINS pairs: {sorted(contains_d)}"
        )

        graph_d = _graph_dump(_store(RESIDUAL_GAP_GRAPH_NAME))
        assert (ORDERS_TOPIC, "CONTAINS", ORDER_CREATED_EVENT) not in graph_d["edge_triples"], (
            "the shared CONTAINS edge survived in the loaded graph even though "
            "neither service asserts it any more"
        )

        full_e_name = f"{RESIDUAL_GAP_GRAPH_NAME}__full_e"
        ws_e = _write_workspace_yaml(tmp_path / "full_check_2", service_dirs, full_e_name)
        _invoke_index(ws_e)
        report_e = _load_report(ws_e)
        _assert_not_degraded(report_e)

        dump_e, _ = _snapshot(ws_e)
        graph_e = _graph_dump(_store(full_e_name))

        assert dump_d == dump_e, (
            "SUPREME INVARIANT violated (residual-gap stage 2): "
            f"{_dump_diff(dump_d, dump_e)}"
        )
        assert graph_d == graph_e, (
            "SUPREME INVARIANT violated (residual-gap stage 2, FalkorDB): "
            f"{_dump_diff(graph_d, graph_e)}"
        )
    finally:
        for name in graph_names:
            FalkorStore(falkordb_cfg, name).delete_graph()
            FalkorStore(falkordb_cfg, f"{name}__build").delete_graph()
