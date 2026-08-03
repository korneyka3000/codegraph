"""Юниты query.api.GraphQuery на fake store (Step 1 брифа m1b-task-7): BFS
depth/limit/visited (expand_neighbors/who_calls), stale-детекция get_source на
tmp-файле, path-валидация (amendment 2: относительный relpath, никаких абсолютных
или ".."), error-dict контракт вместо исключений (amendment 3), fresh-store-per-call
(amendment 1). Живой FalkorDB не нужен -- контракт MCP-схем/сети живёт в
tests/integration/test_mcp_contract.py (marker falkordb).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from codegraph.embedding.fake import FakeEmbedder
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.connection import StoreError, StoreUnavailable


class FakeStore:
    """Duck-typed GraphStore для юнитов: только get_nodes/neighbors/stats -- ровно то
    подмножество Protocol, которое GraphQuery реально вызывает (никакого upsert/schema --
    GraphQuery -- read-only слой). neighbors() воспроизводит задокументированную
    семантику FalkorStore.neighbors (both = out+in, слить, срезать по limit; см.
    stores/falkordb/store.py) поверх простого списка рёбер в памяти."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict] = {}
        self.edges: list[tuple[str, str, dict, str]] = []  # (src, edge_type, props, dst)
        self.stats_result: dict = {"nodes": {}, "edges": {}}
        self.raise_error: Exception | None = None
        self.neighbor_calls: list[tuple] = []
        self.fulltext_calls: list[tuple] = []
        self.fulltext_result: list[dict] = []
        # -- M3 T7: search_code's two new store primitives (see stores/graph.py) --
        self.text_chunk_calls: list[tuple] = []
        self.text_chunk_result: list[tuple[dict, float]] = []
        self.vector_chunk_calls: list[tuple] = []
        self.vector_chunk_result: list[tuple[dict, float]] = []

    def add_node(self, node_id: str, **props) -> None:
        self.nodes[node_id] = {"id": node_id, **props}

    def add_edge(self, src: str, edge_type: str, dst: str, **edge_props) -> None:
        self.edges.append((src, edge_type, edge_props, dst))

    def get_nodes(self, ids):
        if self.raise_error:
            raise self.raise_error
        return [self.nodes[i] for i in ids if i in self.nodes]

    def neighbors(self, node_id, edge_types, direction, limit):
        # Hop -- 4-кортеж (M2, добавлено direction): каждый hop несёт СВОЁ истинное
        # направление ("out"/"in"), не эхо параметра direction -- воспроизводит
        # задокументированную семантику FalkorStore._one_way/both-merge (см.
        # stores/falkordb/store.py) поверх простого списка рёбер в памяти.
        if self.raise_error:
            raise self.raise_error
        self.neighbor_calls.append((node_id, edge_types, direction, limit))
        out = [(et, dict(ep), self.nodes[d], "out") for (s, et, ep, d) in self.edges
               if s == node_id and (not edge_types or et in edge_types)]
        inn = [(et, dict(ep), self.nodes[s], "in") for (s, et, ep, d) in self.edges
               if d == node_id and (not edge_types or et in edge_types)]
        merged = out if direction == "out" else inn if direction == "in" else out + inn
        return merged[:limit]

    def stats(self):
        if self.raise_error:
            raise self.raise_error
        return self.stats_result

    def get_nodes_by_kind(self, kind):
        if self.raise_error:
            raise self.raise_error
        return [n for n in self.nodes.values() if n.get("kind") == kind]

    def search_fulltext(self, query, k, kinds=None):
        if self.raise_error:
            raise self.raise_error
        self.fulltext_calls.append((query, k, kinds))
        return self.fulltext_result

    def search_text_chunks(self, query, k, service=None):
        if self.raise_error:
            raise self.raise_error
        self.text_chunk_calls.append((query, k, service))
        return self.text_chunk_result

    def search_vector_chunks(self, vec, k, service=None):
        if self.raise_error:
            raise self.raise_error
        self.vector_chunk_calls.append((vec, k, service))
        return self.vector_chunk_result

    def find_by_qualified(self, service, qualified):
        if self.raise_error:
            raise self.raise_error
        matches = sorted(
            (n for n in self.nodes.values()
             if n.get("service") == service and n.get("qualified_name") == qualified),
            key=lambda n: n.get("id") or "",
        )
        return matches[0] if matches else None


def _factory(store: FakeStore, calls: list[FakeStore] | None = None):
    """store_factory-стаб: всегда отдаёт один и тот же fake (adjacency настраивается
    один раз до вызова), но каждый вызов -- отдельное обращение к factory; `calls`
    (опционально) -- спай для теста fresh-store-per-call (amendment 1)."""

    def factory():
        if calls is not None:
            calls.append(store)
        return store

    return factory


def _write(root: Path, relpath: str, content: bytes) -> Path:
    p = root / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


# -- graph_stats --


def test_graph_stats_delegates_to_store():
    store = FakeStore()
    store.stats_result = {"nodes": {"Function": 3}, "edges": {"CALLS": 2}}
    q = GraphQuery(_factory(store), {})
    assert q.graph_stats() == {"nodes": {"Function": 3}, "edges": {"CALLS": 2}}


def test_graph_stats_store_unreachable_returns_error_dict_not_exception():
    store = FakeStore()
    store.raise_error = StoreError("connection refused")
    q = GraphQuery(_factory(store), {})
    result = q.graph_stats()
    assert "falkordb unreachable" in result["error"]


def test_graph_stats_store_factory_failure_also_caught():
    def failing_factory():
        raise StoreUnavailable("down")

    q = GraphQuery(failing_factory, {})
    result = q.graph_stats()
    assert "falkordb unreachable" in result["error"]


# -- get_source --


def test_get_source_happy_path_not_stale(tmp_path):
    root = tmp_path / "svcroot"
    content = b"line1\ndef foo():\n    pass\nline4\n"
    _write(root, "mod.py", content)
    start_byte = content.index(b"def foo")
    end_byte = start_byte + len(b"def foo():\n    pass")
    node_hash = hashlib.sha256(content[start_byte:end_byte]).hexdigest()

    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath="mod.py", start_byte=start_byte, end_byte=end_byte,
        start_line=2, end_line=3, content_hash=node_hash,
    )
    q = GraphQuery(_factory(store), {"svc": root})

    result = q.get_source("n1")
    assert result["stale"] is False
    assert result["source"] == "def foo():\n    pass"
    assert result["start_line"] == 2
    assert result["end_line"] == 3
    assert result["file"] == str((root / "mod.py").resolve())


def test_get_source_stale_when_content_hash_mismatches(tmp_path):
    root = tmp_path / "svcroot"
    content = b"def foo():\n    pass\n"
    _write(root, "mod.py", content)
    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath="mod.py", start_byte=0, end_byte=len(content),
        start_line=1, end_line=2, content_hash="not-the-real-hash",
    )
    q = GraphQuery(_factory(store), {"svc": root})
    assert q.get_source("n1")["stale"] is True


def test_get_source_context_lines_expands_slice(tmp_path):
    root = tmp_path / "svcroot"
    content = b"line1\nline2\ndef foo():\n    pass\nline5\nline6\n"
    _write(root, "mod.py", content)
    start_byte = content.index(b"def foo")
    end_byte = start_byte + len(b"def foo():\n    pass")
    node_hash = hashlib.sha256(content[start_byte:end_byte]).hexdigest()
    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath="mod.py", start_byte=start_byte, end_byte=end_byte,
        start_line=3, end_line=4, content_hash=node_hash,
    )
    q = GraphQuery(_factory(store), {"svc": root})

    result = q.get_source("n1", context_lines=1)
    assert result["start_line"] == 2
    assert result["end_line"] == 5
    assert result["source"] == "line2\ndef foo():\n    pass\nline5"
    assert result["stale"] is False  # staleness всегда по исходному, не расширенному span


def test_get_source_node_not_found():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.get_source("missing")
    assert "error" in result


def test_get_source_service_node_has_no_relpath(tmp_path):
    # Service-узлы из FalkorStore.get_nodes() не несут ключ relpath вовсе (см.
    # m1b-task-5-report.md: "у Service-узла нет relpath/start_line/content_hash") --
    # .get() тут возвращает None ровно как для отсутствующего ключа.
    store = FakeStore()
    store.add_node("svc:x", service="x", kind="Service")
    q = GraphQuery(_factory(store), {"x": tmp_path})
    result = q.get_source("svc:x")
    assert "error" in result


def test_get_source_unknown_service_in_node():
    store = FakeStore()
    store.add_node(
        "n1", service="ghost-service", relpath="mod.py", start_byte=0, end_byte=1,
        start_line=1, end_line=1, content_hash="x",
    )
    q = GraphQuery(_factory(store), {})  # service_paths не знает "ghost-service"
    result = q.get_source("n1")
    assert "error" in result


def test_get_source_rejects_absolute_relpath(tmp_path):
    root = tmp_path / "svcroot"
    root.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_bytes(b"secret = 1\n")  # реально существует и читаем -- см. ниже
    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath=str(outside), start_byte=0, end_byte=6,
        start_line=1, end_line=1, content_hash="x",
    )
    q = GraphQuery(_factory(store), {"svc": root})
    result = q.get_source("n1")
    # Доказывает, что guard действительно нужен: без явной проверки на абсолютность
    # (root / relpath) в pathlib отбрасывает root целиком, если relpath абсолютный --
    # get_source прочитал бы outside.py, а не упал бы "по случайности" (файла нет).
    assert "error" in result


def test_get_source_rejects_dotdot_escape(tmp_path):
    root = tmp_path / "svcroot"
    root.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_bytes(b"secret = 1\n")  # существует -- иначе тест прошёл бы "случайно"
    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath="../outside.py", start_byte=0, end_byte=6,
        start_line=1, end_line=1, content_hash="x",
    )
    q = GraphQuery(_factory(store), {"svc": root})
    result = q.get_source("n1")
    assert "error" in result


def test_get_source_missing_file_on_disk(tmp_path):
    root = tmp_path / "svcroot"
    root.mkdir()
    store = FakeStore()
    store.add_node(
        "n1", service="svc", relpath="ghost.py", start_byte=0, end_byte=1,
        start_line=1, end_line=1, content_hash="x",
    )
    q = GraphQuery(_factory(store), {"svc": root})
    result = q.get_source("n1")
    assert "error" in result


def test_get_source_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.get_source("n1")
    assert "falkordb unreachable" in result["error"]


# -- expand_neighbors --


def _chain_store(n: int) -> FakeStore:
    """n0 -CALLS-> n1 -CALLS-> n2 -> ... -> n(n-1), линейная цепочка."""
    store = FakeStore()
    for i in range(n):
        store.add_node(f"n{i}")
    for i in range(n - 1):
        store.add_edge(f"n{i}", "CALLS", f"n{i + 1}")
    return store


def test_expand_neighbors_depth_1_direct_only():
    store = _chain_store(4)  # n0->n1->n2->n3
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("n0", direction="out", depth=1)
    assert {n["id"] for n in result["nodes"]} == {"n1"}
    assert result["truncated"] is False


def test_expand_neighbors_depth_clamped_to_3_maximum():
    store = _chain_store(6)  # n0..n5
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("n0", direction="out", depth=99, limit=100)
    assert {n["id"] for n in result["nodes"]} == {"n1", "n2", "n3"}  # depth clamped to 3


def test_expand_neighbors_depth_clamped_to_1_minimum():
    store = _chain_store(3)
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("n0", direction="out", depth=0, limit=100)
    assert {n["id"] for n in result["nodes"]} == {"n1"}  # depth<1 -> 1 hop, not 0


def test_expand_neighbors_visited_set_handles_cycle_without_hanging():
    store = FakeStore()
    store.add_node("a")
    store.add_node("b")
    store.add_edge("a", "CALLS", "b")
    store.add_edge("b", "CALLS", "a")  # цикл
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("a", direction="out", depth=3, limit=100)
    assert {n["id"] for n in result["nodes"]} == {"a", "b"}
    assert result["truncated"] is False


def test_expand_neighbors_limit_truncates_and_sets_flag():
    store = FakeStore()
    store.add_node("hub")
    for i in range(5):
        store.add_node(f"leaf{i}")
        store.add_edge("hub", "CALLS", f"leaf{i}")
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("hub", direction="out", depth=1, limit=2)
    assert len(result["hops"]) == 2
    assert result["truncated"] is True


def test_expand_neighbors_edge_types_and_direction_passed_through():
    store = FakeStore()
    store.add_node("a")
    store.add_node("b")
    store.add_edge("a", "CALLS", "b")
    q = GraphQuery(_factory(store), {})
    q.expand_neighbors("a", edge_types=["CALLS"], direction="in", depth=1, limit=10)
    assert store.neighbor_calls[0][1] == ["CALLS"]
    assert store.neighbor_calls[0][2] == "in"


def test_expand_neighbors_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("n0")
    assert "falkordb unreachable" in result["error"]


# -- M2: Hop direction field + invalid-direction validation --


def test_expand_neighbors_hop_includes_direction_out():
    store = FakeStore()
    store.add_node("a")
    store.add_node("b")
    store.add_edge("a", "CALLS", "b")
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("a", direction="out", depth=1)
    assert len(result["hops"]) == 1
    assert result["hops"][0]["direction"] == "out"


def test_expand_neighbors_hop_direction_correct_in_both_mode():
    # a -CALLS-> b (out from a), c -CALLS-> a (in to a) -- both-mode must tag each
    # hop with ITS OWN true direction after the out+in merge, not a single value.
    store = FakeStore()
    for name in "abc":
        store.add_node(name)
    store.add_edge("a", "CALLS", "b")
    store.add_edge("c", "CALLS", "a")
    q = GraphQuery(_factory(store), {})
    result = q.expand_neighbors("a", direction="both", depth=1, limit=10)
    direction_by_neighbor = {h["neighbor"]: h["direction"] for h in result["hops"]}
    assert direction_by_neighbor == {"b": "out", "c": "in"}


def test_expand_neighbors_invalid_direction_returns_error_before_store_factory_call():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    result = q.expand_neighbors("a", direction="sideways")
    assert result == {"error": "invalid direction: 'sideways'"}
    assert calls == []  # store_factory must never be called


# -- who_calls --


def test_who_calls_direct_only_by_default():
    store = FakeStore()
    for name in "abc":
        store.add_node(name)
    store.add_edge("a", "CALLS", "b")  # a calls b
    store.add_edge("b", "CALLS", "c")  # b calls c (transitive caller of c)
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("c")
    assert {n["id"] for n in result["callers"]} == {"b"}
    assert result["truncated"] is False


def test_who_calls_transitive_bfs_up_to_max_depth():
    store = FakeStore()
    for name in "abcd":
        store.add_node(name)
    store.add_edge("a", "CALLS", "b")
    store.add_edge("b", "CALLS", "c")
    store.add_edge("c", "CALLS", "d")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("d", transitive=True, max_depth=2)
    assert {n["id"] for n in result["callers"]} == {"b", "c"}


def test_who_calls_max_depth_clamped_to_5():
    names = [f"n{i}" for i in range(8)]
    store = FakeStore()
    for name in names:
        store.add_node(name)
    for i in range(len(names) - 1):
        store.add_edge(names[i], "CALLS", names[i + 1])
    q = GraphQuery(_factory(store), {})
    result = q.who_calls(names[-1], transitive=True, max_depth=99)
    # max_depth клампится к 5: от n7 назад -- n6,n5,n4,n3,n2 (ровно 5 уровней)
    assert {n["id"] for n in result["callers"]} == {"n6", "n5", "n4", "n3", "n2"}


def test_who_calls_only_uses_calls_edge_type_and_in_direction():
    store = FakeStore()
    store.add_node("a")
    store.add_node("b")
    store.add_edge("a", "CONTAINS", "b")  # НЕ calls-ребро -- не должно попасть в callers
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("b")
    assert result["callers"] == []
    assert store.neighbor_calls[0][1] == ["CALLS"]
    assert store.neighbor_calls[0][2] == "in"


def test_who_calls_truncates_at_internal_cap(monkeypatch):
    import codegraph.query.api as api_mod

    monkeypatch.setattr(api_mod, "_DEFAULT_CALLER_LIMIT", 2)
    store = FakeStore()
    store.add_node("target")
    for i in range(4):
        store.add_node(f"caller{i}")
        store.add_edge(f"caller{i}", "CALLS", "target")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("target")
    assert len(result["callers"]) <= 2
    assert result["truncated"] is True


def test_who_calls_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreUnavailable("down")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("x")
    assert "falkordb unreachable" in result["error"]


# -- M10 T2 (pilot §4.3): who_calls x INVOKES_ACTIVITY -- a Temporal activity's
# "real" callers are workflows invoking it via execute_activity_method, which
# resolves to an INVOKES_ACTIVITY edge, not CALLS -- who_calls used to walk CALLS
# only and silently returned 0 for an activity with no direct CALLS in-edge (an
# agent asking "who calls this" would wrongly conclude dead code). node_id's OWN
# role (fetched once via store.get_nodes([node_id]) before the walk) decides,
# for the WHOLE call, whether INVOKES_ACTIVITY is treated as a call-equivalent
# in-edge type alongside CALLS -- see query.api.GraphQuery.who_calls' own
# docstring for the full semantics this pins.


def test_who_calls_activity_target_surfaces_invokes_activity_source_with_mechanism():
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("workflow")
    store.add_edge("workflow", "INVOKES_ACTIVITY", "activity")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity")
    assert result["truncated"] is False
    assert len(result["callers"]) == 1
    caller = result["callers"][0]
    assert caller["id"] == "workflow"
    assert caller["mechanism"] == "invokes_activity"


def test_who_calls_multi_role_target_still_surfaces_invokes_activity_source():
    """M10 T2 backlog pin (multi-role, "nice-to-have" per the M10-T2 review): a
    target's roles list carries TemporalActivity ALONGSIDE another role (e.g. a
    node that is BOTH a Temporal activity and a kafka message consumer handler)
    -- the activity-hood check (`"TemporalActivity" in (target_nodes[0].get
    ("roles") or ())`, see GraphQuery.who_calls' own docstring) is a MEMBERSHIP
    test over the whole roles list, not an equality/first-element check, so
    INVOKES_ACTIVITY sources are still surfaced exactly as for a single-role
    activity target."""
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity", "MessageConsumer"])
    store.add_node("workflow")
    store.add_edge("workflow", "INVOKES_ACTIVITY", "activity")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity")
    assert result["truncated"] is False
    assert len(result["callers"]) == 1
    caller = result["callers"][0]
    assert caller["id"] == "workflow"
    assert caller["mechanism"] == "invokes_activity"


def test_who_calls_activity_target_calls_caller_carries_no_mechanism():
    """An activity can ALSO have an ordinary CALLS caller (e.g. a unit test
    invoking the activity function directly, not through Temporal) -- that
    caller is found via CALLS, not INVOKES_ACTIVITY, so it carries no mechanism
    key at all (absent, not null/false -- same additive convention as
    TraceExit.channel's external/external_host props)."""
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("direct_caller")
    store.add_edge("direct_caller", "CALLS", "activity")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity")
    assert len(result["callers"]) == 1
    caller = result["callers"][0]
    assert caller["id"] == "direct_caller"
    assert "mechanism" not in caller


def test_who_calls_activity_target_queries_both_edge_types_in_direction():
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("workflow")
    store.add_edge("workflow", "INVOKES_ACTIVITY", "activity")
    q = GraphQuery(_factory(store), {})
    q.who_calls("activity")
    assert store.neighbor_calls[0][1] == ["CALLS", "INVOKES_ACTIVITY"]
    assert store.neighbor_calls[0][2] == "in"


def test_who_calls_ordinary_function_target_unaffected_by_activity_handling():
    """Pinned: a target WITHOUT the TemporalActivity role gets byte-identical
    pre-T2 behavior -- edge_types queried stays exactly ["CALLS"], and no
    mechanism key ever appears, even when an INVOKES_ACTIVITY edge happens to
    exist into this (non-activity) node (defensive: the check is node_id's OWN
    role, not "does an INVOKES_ACTIVITY in-edge exist")."""
    store = FakeStore()
    store.add_node("b", roles=["RouteHandler"])  # has A role, just not the one that matters
    store.add_node("a")
    store.add_edge("a", "CALLS", "b")
    store.add_edge("a", "INVOKES_ACTIVITY", "b")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("b")
    assert store.neighbor_calls[0][1] == ["CALLS"]
    assert len(result["callers"]) == 1
    assert "mechanism" not in result["callers"][0]


def test_who_calls_missing_target_node_defaults_to_calls_only():
    """node_id absent from the graph entirely (store.get_nodes returns []) --
    same graceful empty-result, CALLS-only behavior as pre-T2 (who_calls never
    hard-errors on an unknown node_id, see GraphQuery module docstring)."""
    store = FakeStore()
    store.add_node("caller")
    store.add_edge("caller", "CALLS", "ghost")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("ghost")
    assert store.neighbor_calls[0][1] == ["CALLS"]
    assert {c["id"] for c in result["callers"]} == {"caller"}


def test_who_calls_transitive_crosses_invokes_activity_hop_symmetrically():
    """Transitive semantics (M10 plan): activity <-INVOKES_ACTIVITY- workflow
    <-CALLS(mechanism=temporal_start)- starter. who_calls(activity,
    transitive=True) sees BOTH levels: workflow (mechanism="invokes_activity")
    and, one hop further out, starter -- reached over workflow's OWN ordinary
    CALLS in-edge ("as before": temporal_start is an existing CALLS-edge prop,
    not a distinct edge type -- see linking/workspace.py), so starter carries no
    mechanism key of its own."""
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("workflow")
    store.add_node("starter")
    store.add_edge("workflow", "INVOKES_ACTIVITY", "activity")
    store.add_edge("starter", "CALLS", "workflow", mechanism="temporal_start")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity", transitive=True, max_depth=2)
    by_id = {c["id"]: c for c in result["callers"]}
    assert set(by_id) == {"workflow", "starter"}
    assert by_id["workflow"]["mechanism"] == "invokes_activity"
    assert "mechanism" not in by_id["starter"]


def test_who_calls_activity_target_direct_mode_does_not_cross_second_hop():
    """transitive=False (default): even for an activity target, only depth-1
    in-edges are walked -- the starter behind the workflow (2 hops away) is NOT
    surfaced, exactly the pre-T2 direct-mode depth contract."""
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("workflow")
    store.add_node("starter")
    store.add_edge("workflow", "INVOKES_ACTIVITY", "activity")
    store.add_edge("starter", "CALLS", "workflow")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity")
    assert {c["id"] for c in result["callers"]} == {"workflow"}


def test_who_calls_activity_target_caller_reachable_via_both_edge_types_keeps_mechanism():
    """Edge case: the same caller_id has BOTH an INVOKES_ACTIVITY and a CALLS
    edge straight into the activity target -- mechanism is sticky (order of
    edge-type discovery during the walk must not change the result, see
    GraphQuery.who_calls docstring)."""
    store = FakeStore()
    store.add_node("activity", roles=["TemporalActivity"])
    store.add_node("both")
    store.add_edge("both", "INVOKES_ACTIVITY", "activity")
    store.add_edge("both", "CALLS", "activity")
    q = GraphQuery(_factory(store), {})
    result = q.who_calls("activity")
    assert len(result["callers"]) == 1
    assert result["callers"][0]["mechanism"] == "invokes_activity"


# -- amendment 1: fresh store per call (T3 stale-handle finding) --


def test_each_public_method_call_gets_a_freshly_constructed_store():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    q.graph_stats()
    q.graph_stats()
    q.who_calls("x")
    q.trace_process("x")
    q.find_paths("x", "y")
    q.list_processes()
    q.find_entrypoint("x")
    q.search_code("x")  # M3 T7 -- store_factory-freshness applies to this too
    assert len(calls) == 8  # store_factory вызван по разу на публичный вызов, не кэшируется


# -- M2 T8: trace_process --


def test_trace_process_delegates_to_traverse_with_clamped_max_segments(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []

    def fake_trace_process(store, entrypoint_id, max_segments, min_confidence, compact=True):
        calls.append((entrypoint_id, max_segments, min_confidence))
        return {"segments": [], "confidence": 1.0, "truncated": False}

    monkeypatch.setattr(api_mod.traverse, "trace_process", fake_trace_process)
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.trace_process("e1", max_segments=999, min_confidence=0.42)
    assert calls == [("e1", 20, 0.42)]  # clamped to 20
    assert result == {"segments": [], "confidence": 1.0, "truncated": False}


def test_trace_process_clamps_max_segments_minimum_to_1(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.traverse, "trace_process",
        lambda store, entrypoint_id, max_segments, min_confidence, compact=True: (
            calls.append(max_segments)
            or {"segments": [], "confidence": 1.0, "truncated": False}
        ),
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.trace_process("e1", max_segments=0)
    assert calls == [1]


def test_trace_process_invalid_direction_returns_error_before_store_factory_call():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    result = q.trace_process("e1", direction="sideways")
    assert result == {"error": "invalid direction: 'sideways'"}
    assert calls == []


def test_trace_process_upstream_not_supported_in_m2():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    result = q.trace_process("e1", direction="upstream")
    assert "error" in result
    assert "upstream" in result["error"].lower()
    assert "m2" in result["error"].lower()
    assert calls == []  # not-supported short-circuits before touching the store


def test_trace_process_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.trace_process("e1")
    assert "falkordb unreachable" in result["error"]


def test_trace_process_propagates_traverse_error_dict_unchanged(monkeypatch):
    import codegraph.query.api as api_mod

    monkeypatch.setattr(
        api_mod.traverse, "trace_process",
        lambda store, entrypoint_id, max_segments, min_confidence, compact=True: {
            "error": "entrypoint not found: e1"
        },
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.trace_process("e1")
    assert result == {"error": "entrypoint not found: e1"}


def test_trace_process_include_source_attaches_source_to_reachable_nodes_only(
    tmp_path, monkeypatch
):
    import codegraph.query.api as api_mod

    def fake_trace_process(store, entrypoint_id, max_segments, min_confidence, compact=True):
        return {
            "segments": [{
                "service": "svc", "entry": {"id": "e1"},
                "steps": [
                    {"edge_type": "CALLS", "props": {}, "node": {"id": "s1"}, "direction": "out"}
                ],
                "exits": [{"channel": {"id": "c1"}, "next_entry_ids": []}],
                "truncated": False,
            }],
            "confidence": 1.0, "truncated": False,
        }

    monkeypatch.setattr(api_mod.traverse, "trace_process", fake_trace_process)

    root = tmp_path / "svcroot"
    content = b"def foo():\n    pass\n"
    _write(root, "mod.py", content)
    node_hash = hashlib.sha256(content).hexdigest()

    store = FakeStore()
    store.add_node(
        "e1", service="svc", relpath="mod.py", start_byte=0, end_byte=len(content),
        start_line=1, end_line=2, content_hash=node_hash,
    )
    # s1/c1 deliberately absent from the store -- get_source fails for them, and
    # include_source must skip silently (best-effort), not blow up the whole trace.
    q = GraphQuery(_factory(store), {"svc": root})

    result = q.trace_process("e1", include_source=True)
    seg = result["segments"][0]
    assert seg["entry"]["source"] == "def foo():\n    pass"  # get_source's own line-span slicing
    assert "source" not in seg["steps"][0]["node"]
    assert "source" not in seg["exits"][0]["channel"]


def test_trace_process_include_source_false_by_default_does_not_call_get_source(monkeypatch):
    import codegraph.query.api as api_mod

    monkeypatch.setattr(
        api_mod.traverse, "trace_process",
        lambda store, entrypoint_id, max_segments, min_confidence, compact=True: {
            "segments": [{
                "service": "svc", "entry": {"id": "e1"},
                "steps": [], "exits": [], "truncated": False,
            }],
            "confidence": 1.0, "truncated": False,
        },
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.trace_process("e1")
    assert "source" not in result["segments"][0]["entry"]


# -- M5 T5: compact passthrough --


def test_trace_process_compact_defaults_true_and_is_passed_through(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []

    # No default on `compact` here, deliberately: if GraphQuery.trace_process
    # forgot to pass it at all, this fake would raise TypeError (missing
    # required argument) rather than silently reporting a default -- the test
    # must prove GraphQuery explicitly passed compact=True, not just that SOME
    # value ended up true by coincidence.
    def fake_trace_process(store, entrypoint_id, max_segments, min_confidence, compact):
        calls.append(compact)
        return {"segments": [], "confidence": 1.0, "truncated": False}

    monkeypatch.setattr(api_mod.traverse, "trace_process", fake_trace_process)
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.trace_process("e1")
    assert calls == [True]


def test_trace_process_compact_false_is_passed_through(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []

    def fake_trace_process(store, entrypoint_id, max_segments, min_confidence, compact):
        calls.append(compact)
        return {"segments": [], "confidence": 1.0, "truncated": False}

    monkeypatch.setattr(api_mod.traverse, "trace_process", fake_trace_process)
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.trace_process("e1", compact=False)
    assert calls == [False]


# -- M2 T8: find_paths --


def test_find_paths_delegates_to_traverse_with_clamped_max_hops(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []

    def fake_find_paths(store, from_id, to_id, max_hops, edge_types):
        calls.append((from_id, to_id, max_hops, edge_types))
        return {"path": None}

    monkeypatch.setattr(api_mod.traverse, "find_paths", fake_find_paths)
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.find_paths("a", "b", max_hops=999, edge_types=["CALLS"])
    assert calls == [("a", "b", 12, ["CALLS"])]  # clamped to 12
    assert result == {"path": None}


def test_find_paths_clamps_max_hops_minimum_to_1(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.traverse, "find_paths",
        lambda store, from_id, to_id, max_hops, edge_types: calls.append(max_hops)
        or {"path": None},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.find_paths("a", "b", max_hops=0)
    assert calls == [1]


def test_find_paths_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.find_paths("a", "b")
    assert "falkordb unreachable" in result["error"]


# -- M2 T8: list_processes --


def test_list_processes_delegates_to_get_nodes_by_kind_business_process_sorted_by_id():
    store = FakeStore()
    store.add_node("proc:b", kind="BusinessProcess", name="B", entrypoint_id="sym:x",
                    source="config")
    store.add_node("proc:a", kind="BusinessProcess", name="A", entrypoint_id="sym:y",
                    source="temporal")
    store.add_node("sym:x", kind="Function")  # not a BusinessProcess -- excluded
    q = GraphQuery(_factory(store), {})
    result = q.list_processes()
    assert [p["id"] for p in result["processes"]] == ["proc:a", "proc:b"]
    assert result["processes"][0]["entrypoint_id"] == "sym:y"
    assert result["processes"][0]["source"] == "temporal"


def test_list_processes_empty_graph_returns_empty_list():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    assert q.list_processes() == {"processes": []}


def test_list_processes_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.list_processes()
    assert "falkordb unreachable" in result["error"]


# -- M2 T8: find_entrypoint --


def test_find_entrypoint_delegates_to_search_fulltext_with_clamped_k():
    store = FakeStore()
    store.fulltext_result = [{"id": "sym:a:x", "score": 1.5}]
    q = GraphQuery(_factory(store), {})
    result = q.find_entrypoint("create order", k=999)
    # v2 (M3 T7): same "results" shape as M2, plus "mode_used" -- no embedder_factory
    # configured on this GraphQuery, so find_entrypoint degrades to its M2-identical
    # pure-fulltext behavior here (mode_used="text"), see query.retrieval.find_entrypoint.
    assert result == {
        "results": [{"id": "sym:a:x", "score": 1.5}], "mode_used": "text",
    }
    assert store.fulltext_calls == [("create order", 20, None)]  # clamped to 20


def test_find_entrypoint_k_clamped_to_minimum_1():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.find_entrypoint("x", k=0)
    assert store.fulltext_calls[0][1] == 1


def test_find_entrypoint_passes_kinds_through():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.find_entrypoint("x", kinds=["Function"])
    assert store.fulltext_calls[0][2] == ["Function"]


def test_find_entrypoint_empty_result_is_not_an_error():
    store = FakeStore()
    store.fulltext_result = []
    q = GraphQuery(_factory(store), {})
    result = q.find_entrypoint("gibberish query with no matches")
    assert result == {"results": [], "mode_used": "text"}


# -- M3 T2: resolve_selector -- graph-side selector resolution (no staging.db needed,
# see cli.py's `trace` command): route-form via Channel(http_route) props + HANDLES,
# qualified-form via store.find_by_qualified. --


def _add_http_route_channel(
    store: FakeStore, chan_id: str, owner_service: str, method: str, path: str,
) -> None:
    store.add_node(
        chan_id, kind="Channel", channel_kind="http_route",
        owner_service=owner_service, http_method=method, path_template=path,
    )


def test_resolve_selector_route_form_resolves_channel_handles_to_handler():
    store = FakeStore()
    _add_http_route_channel(store, "chan:http:orders-api:POST /orders", "orders-api", "POST",
                             "/orders")
    store.add_node("sym:orders-api:create_order", kind="Function", name="create_order")
    store.add_edge("chan:http:orders-api:POST /orders", "HANDLES", "sym:orders-api:create_order")

    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("orders-api:POST /orders")

    assert result == {"node_id": "sym:orders-api:create_order"}


def test_resolve_selector_route_form_ignores_channels_for_other_services_or_methods():
    store = FakeStore()
    _add_http_route_channel(store, "chan:http:orders-api:POST /orders", "orders-api", "POST",
                             "/orders")
    _add_http_route_channel(store, "chan:http:orders-api:GET /orders", "orders-api", "GET",
                             "/orders")
    _add_http_route_channel(store, "chan:http:other-svc:POST /orders", "other-svc", "POST",
                             "/orders")
    store.add_node("sym:orders-api:create_order", kind="Function", name="create_order")
    store.add_edge("chan:http:orders-api:POST /orders", "HANDLES", "sym:orders-api:create_order")

    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("orders-api:POST /orders")

    assert result == {"node_id": "sym:orders-api:create_order"}


def test_resolve_selector_route_form_channel_with_no_handles_edge_is_not_found():
    store = FakeStore()
    _add_http_route_channel(store, "chan:http:orders-api:POST /orders", "orders-api", "POST",
                             "/orders")
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("orders-api:POST /orders")
    assert "error" in result
    assert "not found" in result["error"].lower()


def test_resolve_selector_route_form_no_matching_channel_returns_not_found_error():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("orders-api:POST /missing")
    assert "error" in result
    assert "orders-api:POST /missing" in result["error"]


def test_resolve_selector_qualified_form_delegates_to_find_by_qualified():
    store = FakeStore()
    store.add_node("sym:kyc-worker:KycWorkflow", kind="Class", service="kyc-worker",
                    qualified_name="app.workflows.kyc.KycWorkflow")
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("kyc-worker:app.workflows.kyc.KycWorkflow")
    assert result == {"node_id": "sym:kyc-worker:KycWorkflow"}


def test_resolve_selector_qualified_form_unresolved_returns_not_found_error():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("kyc-worker:app.nope.Nothing")
    assert "error" in result
    assert "kyc-worker:app.nope.Nothing" in result["error"]


def test_resolve_selector_malformed_selector_without_colon_is_not_found_error():
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("not-a-selector")
    assert "error" in result


def test_resolve_selector_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.resolve_selector("orders-api:POST /orders")
    assert "falkordb unreachable" in result["error"]


def test_resolve_selector_store_factory_failure_also_caught():
    def failing_factory():
        raise StoreUnavailable("down")

    q = GraphQuery(failing_factory, {})
    result = q.resolve_selector("orders-api:POST /orders")
    assert "falkordb unreachable" in result["error"]


def test_resolve_selector_malformed_selector_returns_error_before_store_factory_call():
    """Same amendment-1-adjacent principle as expand_neighbors'/trace_process's own
    direction validation (see api.py module docstring): a cheap, pure precondition
    that's already known to fail should reject BEFORE paying for a store connection."""
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    result = q.resolve_selector("not-a-selector")
    assert "error" in result
    assert calls == []  # store_factory must never be called


def test_resolve_selector_fresh_store_per_call():
    calls: list[FakeStore] = []
    store = FakeStore()
    q = GraphQuery(_factory(store, calls), {})
    q.resolve_selector("orders-api:POST /missing")
    q.resolve_selector("kyc-worker:app.nope.Nothing")
    assert len(calls) == 2


def test_find_entrypoint_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.find_entrypoint("x")
    assert "falkordb unreachable" in result["error"]


# -- M3 T7: find_entrypoint v2 is a thin wrapper over retrieval.find_entrypoint --
# (the RRF-fusion/degradation MATH itself is covered by tests/unit/test_retrieval.py
# against a minimal fake store; these tests monkeypatch retrieval.find_entrypoint as a
# spy -- same technique as the existing trace_process/find_paths tests above -- to
# isolate GraphQuery's OWN concerns: k-clamp, embedder resolution, StoreError boundary).


def test_find_entrypoint_delegates_to_retrieval_with_clamped_k_and_kinds(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "find_entrypoint",
        lambda store, embedder, query, k, kinds: calls.append((query, k, kinds))
        or {"results": [], "mode_used": "text"},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.find_entrypoint("create order", k=999, kinds=["Function"])
    assert calls == [("create order", 20, ["Function"])]  # clamped to 20
    assert result == {"results": [], "mode_used": "text"}


def test_find_entrypoint_clamps_k_minimum_to_1(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "find_entrypoint",
        lambda store, embedder, query, k, kinds: calls.append(k)
        or {"results": [], "mode_used": "text"},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.find_entrypoint("x", k=0)
    assert calls == [1]


def test_find_entrypoint_passes_resolved_embedder_to_retrieval(monkeypatch):
    import codegraph.query.api as api_mod

    seen = []
    monkeypatch.setattr(
        api_mod.retrieval, "find_entrypoint",
        lambda store, embedder, query, k, kinds: seen.append(embedder)
        or {"results": [], "mode_used": "hybrid"},
    )
    store = FakeStore()
    embedder = FakeEmbedder(dim=4)
    q = GraphQuery(_factory(store), {}, embedder_factory=lambda: embedder)
    q.find_entrypoint("x")
    assert seen == [embedder]


# -- M3 T7: search_code (9th MCP tool) --


def test_search_code_delegates_to_retrieval_with_clamped_k(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: calls.append(
            (query, k, service, mode, exact)
        )
        or {"items": [], "mode_used": mode},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    result = q.search_code("create order", k=999, service="svc-a", mode="text")
    assert calls == [("create order", 20, "svc-a", "text", False)]  # clamped to 20
    assert result == {"items": [], "mode_used": "text"}


def test_search_code_clamps_k_minimum_to_1(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: calls.append(k)
        or {"items": [], "mode_used": "text"},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.search_code("x", k=0)
    assert calls == [1]


def test_search_code_passes_exact_flag_through_to_retrieval(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: calls.append(exact)
        or {"items": [], "mode_used": mode},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.search_code("x", mode="vector", exact=True)
    assert calls == [True]


def test_search_code_exact_defaults_to_false(monkeypatch):
    import codegraph.query.api as api_mod

    calls = []
    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: calls.append(exact)
        or {"items": [], "mode_used": mode},
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {})
    q.search_code("x", mode="vector")
    assert calls == [False]


def test_search_code_invalid_mode_returns_error_before_store_factory_call():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    result = q.search_code("x", mode="sideways")
    assert result == {"error": "invalid search mode: 'sideways'"}
    assert calls == []  # store_factory must never be called


def test_search_code_store_unreachable_returns_error_dict():
    store = FakeStore()
    store.raise_error = StoreError("boom")
    q = GraphQuery(_factory(store), {})
    result = q.search_code("x")
    assert "falkordb unreachable" in result["error"]


def test_search_code_store_factory_failure_also_caught():
    def failing_factory():
        raise StoreUnavailable("down")

    q = GraphQuery(failing_factory, {})
    result = q.search_code("x")
    assert "falkordb unreachable" in result["error"]


def test_search_code_text_mode_never_constructs_an_embedder(monkeypatch):
    import codegraph.query.api as api_mod

    factory_calls = []

    def embedder_factory():
        factory_calls.append(1)
        return FakeEmbedder(dim=4)

    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: {
            "items": [], "mode_used": "text",
        },
    )
    store = FakeStore()
    q = GraphQuery(_factory(store), {}, embedder_factory=embedder_factory)
    q.search_code("x", mode="text")
    assert factory_calls == []  # mode="text" never needs an embedder at all


def test_search_code_non_text_mode_resolves_embedder_via_factory(monkeypatch):
    import codegraph.query.api as api_mod

    seen = []
    monkeypatch.setattr(
        api_mod.retrieval, "search_code",
        lambda store, embedder, query, k, service, mode, exact: seen.append(embedder)
        or {"items": [], "mode_used": "hybrid"},
    )
    store = FakeStore()
    embedder = FakeEmbedder(dim=4)
    q = GraphQuery(_factory(store), {}, embedder_factory=lambda: embedder)
    q.search_code("x", mode="hybrid")
    assert seen == [embedder]


# -- M3 T7: embedder caching -- deliberately NOT fresh-per-call (see GraphQuery's own
# class docstring/_get_embedder docstring for why this is a DIFFERENT policy axis from
# store_factory's fresh-per-call rule, not a violation of it) --


def test_get_embedder_caches_after_first_successful_creation():
    calls = []

    def factory():
        calls.append(1)
        return FakeEmbedder(dim=4)

    q = GraphQuery(_factory(FakeStore()), {}, embedder_factory=factory)
    first = q._get_embedder()
    second = q._get_embedder()
    assert first is second
    assert len(calls) == 1  # constructed once, cached thereafter


def test_get_embedder_retries_factory_until_first_success_then_stops():
    calls = []

    def factory():
        calls.append(1)
        return None if len(calls) < 3 else FakeEmbedder(dim=4)

    q = GraphQuery(_factory(FakeStore()), {}, embedder_factory=factory)
    assert q._get_embedder() is None
    assert q._get_embedder() is None
    embedder = q._get_embedder()
    assert embedder is not None
    assert len(calls) == 3
    assert q._get_embedder() is embedder
    assert len(calls) == 3  # cached now -- no further factory calls


def test_get_embedder_returns_none_without_a_factory_configured_at_all():
    q = GraphQuery(_factory(FakeStore()), {})  # embedder_factory defaults to None
    assert q._get_embedder() is None
