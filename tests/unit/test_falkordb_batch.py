"""Юнит-тесты batch.py: батчинг, бисекция при ошибке, prefilter рёбер, валидация — на fake graph."""

from __future__ import annotations

import logging

import pytest

from codegraph.core.errors import InvariantError
from codegraph.stores.falkordb.batch import upsert_edges, upsert_nodes


class FakeGraph:
    """Записывает каждый query() как (cypher, params); падает, если подстрока-маркер
    из fail_on встречается в str(params) — т.е. "плохая" строка определяется по её
    содержимому (id/src/dst), а не по конкретному номеру батча."""

    def __init__(self, fail_on: frozenset[str] = frozenset()):
        self.fail_on = fail_on
        self.calls: list[tuple[str, dict]] = []

    def query(self, cypher: str, params: dict | None = None):
        self.calls.append((cypher, params))
        haystack = str(params)
        for marker in self.fail_on:
            if marker in haystack:
                raise RuntimeError(f"fake failure: marker {marker!r} in params")
        return None


def _node_rows(n: int) -> list[dict]:
    return [{"id": f"sym:a:{i}", "props": {}} for i in range(n)]


def test_upsert_nodes_batches_and_returns_count():
    g = FakeGraph()
    written = upsert_nodes(g, ("Sym",), _node_rows(2500), batch_size=1000)
    assert written == 2500
    assert len(g.calls) == 3
    assert [len(params["rows"]) for _, params in g.calls] == [1000, 1000, 500]
    assert all("MERGE (n:Sym {id: r.id})" in cypher for cypher, _ in g.calls)


def test_upsert_nodes_bisects_to_single_bad_row(caplog):
    g = FakeGraph(fail_on={"BAD"})
    rows = [{"id": "a", "props": {}}, {"id": "BAD", "props": {}},
            {"id": "c", "props": {}}, {"id": "d", "props": {}}]
    with caplog.at_level(logging.WARNING):
        written = upsert_nodes(g, ("Sym",), rows, batch_size=4)
    # 4 rows in, 1 bad -> 3 written, exact (no partial/duplicate counting from bisection)
    assert written == 3
    # bisection must have bottomed out at a single-row batch containing exactly the bad id
    singleton_bad_batches = [
        params["rows"] for _, params in g.calls
        if params["rows"] == [{"id": "BAD", "props": {}}]
    ]
    assert len(singleton_bad_batches) == 1
    # the bad row was logged as a warning and skipped, not raised
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("BAD" in r.message for r in warnings)


def test_upsert_edges_prefilters_missing_endpoints():
    g = FakeGraph()
    rows = [{"src": "a", "dst": "b", "props": {}},
            {"src": "a", "dst": "ghost", "props": {}}]
    written, dropped = upsert_edges(g, "CALLS", rows, known_ids={"a", "b"})
    assert written == 1
    assert dropped == 1
    # the ghost endpoint must never reach a query's params
    for _, params in g.calls:
        for r in params["rows"]:
            assert r["src"] != "ghost"
            assert r["dst"] != "ghost"


def test_edge_type_validated():
    g = FakeGraph()
    rows = [{"src": "a", "dst": "b", "props": {}}]
    with pytest.raises(InvariantError):
        upsert_edges(g, "EVIL' DELETE", rows, known_ids={"a", "b"})
    assert g.calls == []  # validation happens before any query


def test_labels_validated():
    g = FakeGraph()
    rows = [{"id": "x", "props": {}}]
    with pytest.raises(InvariantError):
        upsert_nodes(g, ("Sym", "Nope"), rows)
    assert g.calls == []  # validation happens before any query


# -- M2: label allowlist grows with roles/Channel/BusinessProcess --


def test_upsert_nodes_allows_role_label():
    g = FakeGraph()
    rows = [{"id": "sym:a:f", "props": {}}]
    written = upsert_nodes(g, ("Sym", "Function", "RouteHandler"), rows)
    assert written == 1
    assert "MERGE (n:Sym:Function:RouteHandler {id: r.id})" in g.calls[0][0]


def test_upsert_nodes_allows_all_five_role_labels():
    g = FakeGraph()
    roles = ("RouteHandler", "MessageConsumer", "MessageProducer",
              "TemporalWorkflow", "TemporalActivity")
    for role in roles:
        written = upsert_nodes(g, ("Sym", "Function", role), [{"id": "x", "props": {}}])
        assert written == 1


def test_upsert_nodes_allows_channel_label():
    g = FakeGraph()
    written = upsert_nodes(g, ("Channel",), [{"id": "chan:kafka_topic:x", "props": {}}])
    assert written == 1


def test_upsert_nodes_allows_business_process_label():
    g = FakeGraph()
    written = upsert_nodes(g, ("BusinessProcess",), [{"id": "proc:x", "props": {}}])
    assert written == 1


def test_upsert_nodes_rejects_unknown_role_label():
    g = FakeGraph()
    with pytest.raises(InvariantError):
        upsert_nodes(g, ("Sym", "Function", "NotARole"), [{"id": "x", "props": {}}])
    assert g.calls == []


def test_upsert_edges_allows_new_m2_edge_types():
    g = FakeGraph()
    for edge_type in ("HANDLES", "DEPENDS_ON", "PRODUCES", "CONSUMES",
                       "INVOKES_ACTIVITY", "CALLS_HTTP", "NEXT_SEGMENT", "PART_OF_PROCESS"):
        written, dropped = upsert_edges(
            g, edge_type, [{"src": "a", "dst": "b", "props": {}}], known_ids={"a", "b"}
        )
        assert (written, dropped) == (1, 0)
