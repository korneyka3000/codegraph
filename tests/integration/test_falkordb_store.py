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


EXISTS = "__t6exists__"


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
