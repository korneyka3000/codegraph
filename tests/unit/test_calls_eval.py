"""Юниты для codegraph.evalx.calls_eval: precision_recall (синтетика), load_golden_calls
(mechanism-фильтр, channel-пропуск) и found_calls (JOIN по id в nodes, dangling-счётчик).
Быстрые, без scip/network — реальный E2E-гейт с настоящим scip живёт в
tests/eval/test_calls_gate.py (marker scip)."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.evalx.calls_eval import found_calls, load_golden_calls, precision_recall
from codegraph.stores.staging import Staging

A = ("svc", "app.a", "svc", "app.b")
B = ("svc", "app.b", "svc", "app.c")

# -- precision_recall --------------------------------------------------------


def test_precision_recall_perfect_match():
    pr = precision_recall({A, B}, {A, B})
    assert pr == {"precision": 1.0, "recall": 1.0, "tp": 2, "fp_list": [], "fn_list": []}


def test_precision_recall_false_positive():
    pr = precision_recall({A, B}, {A})  # B — лишний found, не в golden
    assert pr["precision"] == 0.5
    assert pr["recall"] == 1.0
    assert pr["tp"] == 1
    assert pr["fp_list"] == [B]
    assert pr["fn_list"] == []


def test_precision_recall_false_negative():
    pr = precision_recall({A}, {A, B})  # B — пропущен, есть в golden, нет в found
    assert pr["precision"] == 1.0
    assert pr["recall"] == 0.5
    assert pr["tp"] == 1
    assert pr["fp_list"] == []
    assert pr["fn_list"] == [B]


def test_precision_recall_empty_found_nonempty_golden():
    # 0/0 для precision -- конвенция 1.0 (см. docstring precision_recall); recall
    # честно 0.0 (ничего не найдено при непустом golden).
    pr = precision_recall(set(), {A})
    assert pr["precision"] == 1.0
    assert pr["recall"] == 0.0
    assert pr["tp"] == 0
    assert pr["fp_list"] == []
    assert pr["fn_list"] == [A]


def test_precision_recall_nonempty_found_empty_golden():
    # 0/0 для recall -- конвенция 1.0; precision честно 0.0 (все found -- лишние).
    pr = precision_recall({A}, set())
    assert pr["precision"] == 0.0
    assert pr["recall"] == 1.0
    assert pr["tp"] == 0
    assert pr["fp_list"] == [A]
    assert pr["fn_list"] == []


def test_precision_recall_both_empty():
    pr = precision_recall(set(), set())
    assert pr == {"precision": 1.0, "recall": 1.0, "tp": 0, "fp_list": [], "fn_list": []}


# -- load_golden_calls --------------------------------------------------------

GOLDEN_YAML = """
version: 1
edges:
  - src: {service: svc, symbol: app.a}
    type: CALLS
    dst: {service: svc, symbol: app.b}
  - src: {service: svc, symbol: app.b}
    type: CALLS
    dst: {service: svc, symbol: app.workflow}
    mechanism: temporal_start
  - src: {service: svc, symbol: app.c}
    type: PRODUCES
    dst: {channel: "chan:x"}
  - src: {service: svc, symbol: app.d}
    type: CALLS
    dst: {channel: "chan:y"}
"""


def test_load_golden_calls_filters_mechanism_and_channel_and_type(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    result = load_golden_calls(path)
    # только запись #1 проходит все три фильтра: type==CALLS, без mechanism, dst.symbol
    assert result == {("svc", "app.a", "svc", "app.b")}


# -- found_calls --------------------------------------------------------


def _node(id_: str, service: str, qualified_name: str) -> NodeRec:
    return NodeRec(
        id=id_, kind="Function", service=service,
        name=qualified_name.rsplit(".", 1)[-1], qualified_name=qualified_name,
    )


def _edge(src: str, dst: str, type_: str = "CALLS") -> EdgeRec:
    return EdgeRec(src=src, dst=dst, type=type_, resolution="static", confidence=1.0,
                    extractor="calls")


def test_found_calls_resolves_both_endpoints_via_nodes(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes([_node("sym:svc:a", "svc", "app.a"), _node("sym:svc:b", "svc", "app.b")])
    st.upsert_edges([_edge("sym:svc:a", "sym:svc:b")])

    result = found_calls(st)

    assert result.edges == {("svc", "app.a", "svc", "app.b")}
    assert result.skipped_dangling == 0


def test_found_calls_skips_edge_with_dangling_dst(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes([_node("sym:svc:a", "svc", "app.a")])  # "sym:svc:ghost" -- нет узла
    st.upsert_edges([_edge("sym:svc:a", "sym:svc:ghost")])

    result = found_calls(st)

    assert result.edges == set()
    assert result.skipped_dangling == 1


def test_found_calls_skips_edge_with_dangling_src(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes([_node("sym:svc:b", "svc", "app.b")])  # "sym:svc:ghost" -- нет узла
    st.upsert_edges([_edge("sym:svc:ghost", "sym:svc:b")])

    result = found_calls(st)

    assert result.edges == set()
    assert result.skipped_dangling == 1


def test_found_calls_treats_missing_qualified_name_as_dangling(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes([
        _node("sym:svc:a", "svc", "app.a"),
        NodeRec(id="sym:svc:b", kind="Function", service="svc", name="b", qualified_name=""),
    ])
    st.upsert_edges([_edge("sym:svc:a", "sym:svc:b")])

    result = found_calls(st)

    assert result.edges == set()
    assert result.skipped_dangling == 1


def test_found_calls_ignores_non_calls_edges(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes([_node("sym:svc:a", "svc", "app.a")])
    # CONTAINS с висячим dst -- вне области found_calls: не в edges, не в счётчике.
    st.upsert_edges([_edge("sym:svc:a", "sym:svc:ghost", type_="CONTAINS")])

    result = found_calls(st)

    assert result.edges == set()
    assert result.skipped_dangling == 0
