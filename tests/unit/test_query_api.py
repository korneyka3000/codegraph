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

    def add_node(self, node_id: str, **props) -> None:
        self.nodes[node_id] = {"id": node_id, **props}

    def add_edge(self, src: str, edge_type: str, dst: str, **edge_props) -> None:
        self.edges.append((src, edge_type, edge_props, dst))

    def get_nodes(self, ids):
        if self.raise_error:
            raise self.raise_error
        return [self.nodes[i] for i in ids if i in self.nodes]

    def neighbors(self, node_id, edge_types, direction, limit):
        if self.raise_error:
            raise self.raise_error
        self.neighbor_calls.append((node_id, edge_types, direction, limit))
        out = [(et, dict(ep), self.nodes[d]) for (s, et, ep, d) in self.edges
               if s == node_id and (not edge_types or et in edge_types)]
        inn = [(et, dict(ep), self.nodes[s]) for (s, et, ep, d) in self.edges
               if d == node_id and (not edge_types or et in edge_types)]
        merged = out if direction == "out" else inn if direction == "in" else out + inn
        return merged[:limit]

    def stats(self):
        if self.raise_error:
            raise self.raise_error
        return self.stats_result


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


# -- amendment 1: fresh store per call (T3 stale-handle finding) --


def test_each_public_method_call_gets_a_freshly_constructed_store():
    store = FakeStore()
    calls: list[FakeStore] = []
    q = GraphQuery(_factory(store, calls), {})
    q.graph_stats()
    q.graph_stats()
    q.who_calls("x")
    assert len(calls) == 3  # store_factory вызван по разу на публичный вызов, не кэшируется
