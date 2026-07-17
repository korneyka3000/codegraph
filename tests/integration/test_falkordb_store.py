"""Интеграционный тест FalkorStore (stores/graph.py Protocol + stores/falkordb/store.py)
на живом FalkorDB (Step 1 брифа m1b-task-3).

Первый тест гоняет полный сценарий: upsert в build-граф -> blue/green swap_in в final-граф
(причём final заранее занят "мусорными" данными -- чтобы доказать перезапись, а не слияние,
и чтобы заставить FalkorStore закэшировать Graph-обёртку СТАРОГО состояния до swap_in) ->
get_nodes/neighbors(out/in/both/фильтр/limit)/stats на final. Второй тест: neighbors на
несуществующем id -> [].
"""

from __future__ import annotations

import pytest

from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore

pytestmark = pytest.mark.falkordb

BUILD = "__t3__build"
FINAL = "__t3__"
EMPTY = "__t3__empty"


def _cleanup(cfg, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # RENAME уже мог унести build-ключ; final мог не существовать вовсе


def test_falkordb_store_upsert_swap_neighbors_stats(falkordb_cfg):
    # Заранее занимаем целевое (final) имя графа "мусорным" узлом+ребром: живым тестом
    # проверяем (a) что swap_in атомарно ПЕРЕЗАПИСЫВАЕТ существующий final-граф, а не
    # сливается с ним; (b) что final_store, "прогретый" чтением ДО swap_in, после
    # RENAME не возвращает данные, декодированные по старой схеме. Ребро типа OLD_REL
    # -- единственное в этом графе, поэтому гарантированно получает
    # relationship-type index 0 (подтверждено живым замером DB.RELATIONSHIPTYPES) --
    # ту же позицию, что CALLS получит в свежем build-графе (первый когда-либо
    # созданный тип ребра там же). Если swap_in не инвалидирует Graph-обёртку,
    # FalkorDB python-клиент (кэширующий id->имя по индексу без версионирования через
    # RENAME -- см. GraphSchema.get_relation) декодирует новый CALLS обратно в
    # "OLD_REL"; заголовок ниже проверяет ровно это.
    seed_db = connect(falkordb_cfg)
    seed_db.select_graph(FINAL).query(
        "MERGE (a:Junk {id: 'old-junk-a'})-[:OLD_REL {x: 0}]->(b:Junk {id: 'old-junk-b'}) "
        "SET a.kind = 'OldStuff', b.kind = 'OldStuff'"
    )

    final_store = FalkorStore(falkordb_cfg, FINAL)
    try:
        # Прогреваем final_store._g.schema ДО swap_in через neighbors(): в отличие от
        # stats() (которая отдаёт только скалярные n.kind/count(n) и схемы вообще не
        # трогает), neighbors() разбирает полноценные Node/Edge объекты и тем самым
        # заполняет кэш label/property-key/relationship-type id->имя старым состоянием.
        pre_hops = final_store.neighbors("old-junk-a", None, "out", limit=10)
        assert pre_hops == [
            ("OLD_REL", {"x": 0}, {"id": "old-junk-b", "kind": "OldStuff"}, "out")
        ]

        pre_stats = final_store.stats()
        assert pre_stats == {"nodes": {"OldStuff": 2}, "edges": {"OLD_REL": 1}}

        build_store = FalkorStore(falkordb_cfg, BUILD)
        build_store.ensure_schema()

        func_a = {"id": "sym:svc:funcA", "props": {"kind": "Function", "name": "funcA"}}
        func_b = {"id": "sym:svc:funcB", "props": {"kind": "Function", "name": "funcB"}}
        svc = {"id": "svc:svc", "props": {"kind": "Service", "name": "svc"}}
        assert build_store.upsert_nodes(("Sym", "Function"), [func_a, func_b]) == 2
        assert build_store.upsert_nodes(("Service",), [svc]) == 1

        known_ids = {func_a["id"], func_b["id"], svc["id"]}
        calls_row = {"src": func_a["id"], "dst": func_b["id"], "props": {"callsite_count": 1}}
        contains_row = {"src": svc["id"], "dst": func_a["id"], "props": {}}
        assert build_store.upsert_edges("CALLS", [calls_row], known_ids) == (1, 0)
        assert build_store.upsert_edges("CONTAINS", [contains_row], known_ids) == (1, 0)

        # --- blue/green swap: __t3__build -> __t3__ (перезаписывает старый final) ---
        final_store.swap_in(BUILD)

        # старый junk исчез -- перезаписан, не слит с новыми данными
        assert final_store.get_nodes(["old-junk-a", "old-junk-b"]) == []

        # build-ключ исчез (RENAME забрал его на месте), final -- на месте
        graphs_after = seed_db.list_graphs()
        assert BUILD not in graphs_after
        assert FINAL in graphs_after

        # --- get_nodes: находит существующие, молча пропускает отсутствующие ---
        nodes = final_store.get_nodes([func_a["id"], func_b["id"], "does-not-exist"])
        assert {n["id"] for n in nodes} == {func_a["id"], func_b["id"]}
        assert all(n["kind"] == "Function" for n in nodes)

        # --- neighbors: out, отфильтрованные по CALLS -- Hop 4-кортеж, direction="out" ---
        out_hops = final_store.neighbors(func_a["id"], ["CALLS"], "out", limit=10)
        assert len(out_hops) == 1
        edge_type, edge_props, node_dict, direction = out_hops[0]
        assert edge_type == "CALLS"
        assert edge_props == {"callsite_count": 1}
        assert node_dict["id"] == func_b["id"]
        assert direction == "out"

        # --- neighbors: in, без фильтра -- CONTAINS от svc, direction="in" ---
        in_hops = final_store.neighbors(func_a["id"], None, "in", limit=10)
        assert len(in_hops) == 1
        assert in_hops[0][0] == "CONTAINS"
        assert in_hops[0][2]["id"] == svc["id"]
        assert in_hops[0][3] == "in"

        # --- neighbors: both, без фильтра -- CALLS(out) + CONTAINS(in) объединены;
        # КАЖДЫЙ hop несёт СВОЁ истинное направление после слияния (не одно значение
        # на весь результат) -- это и есть живая проверка both-режима из watch-item 3. ---
        both_hops = final_store.neighbors(func_a["id"], None, "both", limit=10)
        assert len(both_hops) == 2
        assert {h[0] for h in both_hops} == {"CALLS", "CONTAINS"}
        direction_by_edge_type = {h[0]: h[3] for h in both_hops}
        assert direction_by_edge_type == {"CALLS": "out", "CONTAINS": "in"}

        # --- neighbors: фильтр по типу сужает "both" до одного CALLS-хопа (out) ---
        filtered_hops = final_store.neighbors(func_a["id"], ["CALLS"], "both", limit=10)
        assert len(filtered_hops) == 1
        assert filtered_hops[0][0] == "CALLS"
        assert filtered_hops[0][3] == "out"

        # --- neighbors: limit применяется к сумме out+in ---
        limited_hops = final_store.neighbors(func_a["id"], None, "both", limit=1)
        assert len(limited_hops) == 1

        # --- stats: соответствует записанному ---
        assert final_store.stats() == {
            "nodes": {"Function": 2, "Service": 1},
            "edges": {"CALLS": 1, "CONTAINS": 1},
        }
    finally:
        _cleanup(falkordb_cfg, BUILD, FINAL)


def test_neighbors_on_nonexistent_node_returns_empty(falkordb_cfg):
    store = FalkorStore(falkordb_cfg, EMPTY)
    try:
        store.ensure_schema()
        assert store.neighbors("no-such-id", None, "out", limit=10) == []
        assert store.neighbors("no-such-id", None, "in", limit=10) == []
        assert store.neighbors("no-such-id", None, "both", limit=10) == []
    finally:
        _cleanup(falkordb_cfg, EMPTY)


NEXT_SEGMENT_GRAPH = "__t3nextseg__"


def test_upsert_edges_key_props_keeps_parallel_channel_next_segment_distinct_and_idempotent(
    falkordb_cfg,
):
    """M3 T1 live proof: batch.upsert_edges' key_props widens the MERGE key beyond
    (src,dst) for NEXT_SEGMENT (via_channel_id) -- two edges between the SAME (src,dst)
    pair, reached via two DIFFERENT channels, must both persist as DISTINCT
    relationships in FalkorDB (not one silently overwriting the other -- the old
    (src,dst)-only MERGE key's failure mode, see core/schema.py's SCHEMA_VERSION
    "2 -> 3" history comment for the staging-side half of the same fix), AND a repeat
    upsert of the exact same rows must not duplicate them (still 2, not 4) -- true
    MERGE-idempotency at the store level, not merely "load_graph's blue/green happens
    to start from an empty build graph every run" (see test_pipeline_load.py for that
    separate, higher-level proof)."""
    store = FalkorStore(falkordb_cfg, NEXT_SEGMENT_GRAPH)
    try:
        store.ensure_schema()
        node_a = {"id": "sym:t3ns:a", "props": {"kind": "Function", "name": "a"}}
        node_b = {"id": "sym:t3ns:b", "props": {"kind": "Function", "name": "b"}}
        store.upsert_nodes(("Sym", "Function"), [node_a, node_b])
        known_ids = {node_a["id"], node_b["id"]}

        rows = [
            {"src": node_a["id"], "dst": node_b["id"],
             "via_channel_id": "chan:kafka_topic:orders",
             "props": {"via_channel_id": "chan:kafka_topic:orders"}},
            {"src": node_a["id"], "dst": node_b["id"],
             "via_channel_id": "chan:kafka_topic:shipping",
             "props": {"via_channel_id": "chan:kafka_topic:shipping"}},
        ]

        written, dropped = store.upsert_edges(
            "NEXT_SEGMENT", rows, known_ids, key_props=("via_channel_id",)
        )
        assert (written, dropped) == (2, 0)

        hops = store.neighbors(node_a["id"], ["NEXT_SEGMENT"], "out", limit=10)
        assert len(hops) == 2
        via_ids = {h[1]["via_channel_id"] for h in hops}
        assert via_ids == {"chan:kafka_topic:orders", "chan:kafka_topic:shipping"}

        # Repeat upsert of the SAME rows -- MERGE must land back on the same two
        # edges, not create two more.
        written2, dropped2 = store.upsert_edges(
            "NEXT_SEGMENT", rows, known_ids, key_props=("via_channel_id",)
        )
        assert (written2, dropped2) == (2, 0)
        hops_again = store.neighbors(node_a["id"], ["NEXT_SEGMENT"], "out", limit=10)
        assert len(hops_again) == 2
    finally:
        _cleanup(falkordb_cfg, NEXT_SEGMENT_GRAPH)


EXISTS = "__t6exists__"


FIND_QUALIFIED = "__t2findq__"


def test_find_by_qualified_matches_service_and_qualified_name(falkordb_cfg):
    """M3 T2 live proof: query/api.GraphQuery.resolve_selector's qualified-selector
    form resolves through this method (no Cypher outside stores/falkordb -- see
    query/api.py). MATCH is scoped to (service, qualified_name) BOTH -- a node in a
    DIFFERENT service sharing the same qualified_name must not match."""
    store = FalkorStore(falkordb_cfg, FIND_QUALIFIED)
    try:
        store.ensure_schema()
        target = {
            "id": "sym:kyc-worker:KycWorkflow", "props": {
                "kind": "Class", "service": "kyc-worker",
                "qualified_name": "app.workflows.kyc.KycWorkflow",
            },
        }
        other_service = {
            "id": "sym:other-svc:KycWorkflow", "props": {
                "kind": "Class", "service": "other-svc",
                "qualified_name": "app.workflows.kyc.KycWorkflow",
            },
        }
        store.upsert_nodes(("Sym", "Class"), [target, other_service])

        found = store.find_by_qualified("kyc-worker", "app.workflows.kyc.KycWorkflow")
        assert found is not None
        assert found["id"] == "sym:kyc-worker:KycWorkflow"

        assert store.find_by_qualified("kyc-worker", "app.nope.Nothing") is None
        assert store.find_by_qualified("no-such-service", "app.workflows.kyc.KycWorkflow") is None
    finally:
        _cleanup(falkordb_cfg, FIND_QUALIFIED)


def test_find_by_qualified_picks_lowest_id_when_multiple_match(falkordb_cfg):
    """ORDER BY id LIMIT 1 (brief's own contract) -- deterministic pick, not
    store-dependent iteration order, on the (should-never-happen-by-construction, but
    defensively covered) case of a duplicate (service, qualified_name) pair."""
    store = FalkorStore(falkordb_cfg, FIND_QUALIFIED)
    try:
        store.ensure_schema()
        dup_hi = {
            "id": "sym:svc:z-dup", "props": {
                "kind": "Function", "service": "svc", "qualified_name": "app.dup",
            },
        }
        dup_lo = {
            "id": "sym:svc:a-dup", "props": {
                "kind": "Function", "service": "svc", "qualified_name": "app.dup",
            },
        }
        store.upsert_nodes(("Sym", "Function"), [dup_hi, dup_lo])

        found = store.find_by_qualified("svc", "app.dup")
        assert found is not None
        assert found["id"] == "sym:svc:a-dup"
    finally:
        _cleanup(falkordb_cfg, FIND_QUALIFIED)


OR_FALLBACK = "__m4t3_or_fallback__"


def test_search_text_chunks_or_fallback_finds_mixed_language_query(falkordb_cfg):
    """M4 T3 Step 1: RediSearch's implicit AND over "создание OrderCreated заказа"'s
    3 sanitized tokens finds nothing in an English-identifier corpus -- only
    "OrderCreated" ever appears literally anywhere in it, "создание"/"заказа" match
    NO chunk at all, so the AND-only first pass returns [] even though the chunk an
    engineer actually wants (the one containing "OrderCreated") is right there. The
    OR-joined second pass ("создание | OrderCreated | заказа") matches on
    "OrderCreated" alone and surfaces it -- this is the exact scenario from the M3
    final review finding (mixed RU/EN NL queries going fulltext-dead) this task
    fixes."""
    store = FalkorStore(falkordb_cfg, OR_FALLBACK)
    try:
        store.ensure_schema()
        store.upsert_nodes(("Chunk",), [
            {
                "id": "chunk:order-created",
                "props": {
                    "service": "orders-api",
                    "text": "def create_order(): emit(OrderCreated(order_id=order.id))",
                    "context_header": "symbol: app.services.order.OrderService.create (Function)",
                },
            },
        ])

        results = store.search_text_chunks("создание OrderCreated заказа", k=5)
        assert any(props["id"] == "chunk:order-created" for props, _score in results)

        # purely-Cyrillic query: EVERY token misses this English-only corpus in
        # EITHER pass -- OR of all-misses is still a miss, so this stays [] (the M3
        # gate's own Q1-Q5 expectation, see .superpowers/sdd/task-3-brief.md).
        assert store.search_text_chunks("создание заказа", k=5) == []
    finally:
        _cleanup(falkordb_cfg, OR_FALLBACK)


def test_search_text_chunks_and_success_does_not_widen_with_or(falkordb_cfg):
    """Binding contract (brief's Interfaces section): an AND-successful query is
    ZERO behavior change -- the fallback must never even run once the first pass
    already found something. "widget" alone would also match a second, unrelated
    chunk that has no "orders" token at all; if the OR-fallback incorrectly ran
    anyway (e.g. a bug that doesn't check the first pass's row count), that second
    chunk would leak into the result. It must not."""
    store = FalkorStore(falkordb_cfg, OR_FALLBACK)
    try:
        store.ensure_schema()
        store.upsert_nodes(("Chunk",), [
            {
                "id": "chunk:both-tokens",
                "props": {"service": "svc-a", "text": "orders widget helper"},
            },
            {
                "id": "chunk:widget-only",
                "props": {"service": "svc-a", "text": "widget gadget thing, no relation"},
            },
        ])

        results = store.search_text_chunks("orders widget", k=5)
        assert {props["id"] for props, _score in results} == {"chunk:both-tokens"}
    finally:
        _cleanup(falkordb_cfg, OR_FALLBACK)


def test_search_fulltext_or_fallback_finds_mixed_language_query(falkordb_cfg):
    """Same OR-fallback contract as the search_text_chunks tests above, mirrored on
    the Sym fulltext leg (name/qualified_name/docstring) -- this task's brief
    modifies BOTH search_fulltext (Sym) and search_text_chunks (Chunk), not just
    one of the two fulltext methods."""
    store = FalkorStore(falkordb_cfg, OR_FALLBACK)
    try:
        store.ensure_schema()
        store.upsert_nodes(("Sym", "Function"), [
            {
                "id": "sym:orders-api:create_order",
                "props": {
                    "kind": "Function", "name": "create_order",
                    "qualified_name": "app.routes.orders.create_order",
                    "docstring": "Creates a new order and emits OrderCreated.",
                },
            },
        ])

        results = store.search_fulltext("создание OrderCreated заказа", k=5)
        assert any(r["id"] == "sym:orders-api:create_order" for r in results)

        assert store.search_fulltext("создание заказа", k=5) == []
    finally:
        _cleanup(falkordb_cfg, OR_FALLBACK)


def test_graph_exists_is_read_only_and_flips_after_write(falkordb_cfg):
    """graph_exists() (M1b T6 fix A): False для никогда не индексированного имени,
    и -- критично -- БЕЗ auto-vivify побочного эффекта (в отличие от GRAPH.QUERY,
    который создаёт пустой граф-ключ даже на чистом MATCH; живьём наблюдалось в T6
    при stats()-пробе несуществующего графа). После первой записи -- True."""
    store = FalkorStore(falkordb_cfg, EXISTS)
    try:
        assert store.graph_exists() is False
        # read-only доказательство: сам вызов graph_exists не создал ключ
        assert EXISTS not in connect(falkordb_cfg).list_graphs()
        assert store.graph_exists() is False

        store.upsert_nodes(("Service",), [{"id": "svc:t6e", "props": {"kind": "Service"}}])
        assert store.graph_exists() is True
    finally:
        _cleanup(falkordb_cfg, EXISTS)
