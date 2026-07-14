"""Юниты для codegraph.evalx.edges_eval: generalized typed-edge eval (M2), поверх
M1's calls_eval.precision_recall (реэкспортирован, не переизобретён -- см. edges_eval
модульный докстринг). Покрывает: load_golden_edges (symbol-dst/channel-dst формы кортежа,
types-фильтр, mechanism-фильтр ТОЛЬКО для CALLS) и found_edges (inner-join к nodes для
sym-концов, chan-концы напрямую как id без join, HANDLES-нормализация направления,
dangling-счётчик, types-фильтр). Быстрые, без scip/falkordb -- реальный E2E-гейт с
настоящим scip+FalkorDB живёт в tests/eval/test_m2_gate.py (маркеры scip+falkordb)."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.evalx import calls_eval
from codegraph.evalx.edges_eval import found_edges, load_golden_edges, precision_recall
from codegraph.stores.staging import Staging

# -- precision_recall reuse --------------------------------------------------


def test_precision_recall_is_reexported_from_calls_eval_not_reimplemented():
    # edges_eval must not fork the P/R math -- a single source of truth in calls_eval.
    assert precision_recall is calls_eval.precision_recall


# -- load_golden_edges --------------------------------------------------------

GOLDEN_YAML = """
version: 1
edges:
  - src: {service: orders-api, symbol: app.routes.orders.create_order}
    type: DEPENDS_ON
    dst: {service: orders-api, symbol: app.db.session.get_db}
  - src: {service: orders-api, symbol: app.services.order.OrderService.place}
    type: PRODUCES
    dst: {channel: "chan:event_type:OrderCreated"}
  - src: {service: document-management, symbol: app.routes.documents.get_document}
    type: HANDLES
    dst: {channel: "chan:http:document-management:GET /documents/{doc_id}"}
  - src: {service: kyc-worker, symbol: app.consumers.orders.handle_order_created}
    type: CALLS
    dst: {service: kyc-worker, symbol: app.workflows.kyc.KycWorkflow.run}
    mechanism: temporal_start
  - src: {service: orders-api, symbol: app.routes.orders.create_order}
    type: CALLS
    dst: {service: orders-api, symbol: app.services.order.OrderService}
  - src: {service: kyc-worker, symbol: app.workflows.kyc.KycWorkflow.run}
    type: INVOKES_ACTIVITY
    dst: {service: kyc-worker, symbol: app.activities.documents.verify_documents}
    mechanism: not_a_real_thing_but_proves_scoping
"""


def test_load_golden_edges_symbol_dst_shape_is_five_tuple(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    result = load_golden_edges(path, {"DEPENDS_ON"})
    assert result == {
        ("DEPENDS_ON", "orders-api", "app.routes.orders.create_order",
         "orders-api", "app.db.session.get_db"),
    }


def test_load_golden_edges_channel_dst_shape_is_four_tuple(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    result = load_golden_edges(path, {"PRODUCES"})
    assert result == {
        ("PRODUCES", "orders-api", "app.services.order.OrderService.place",
         "chan:event_type:OrderCreated"),
    }


def test_load_golden_edges_handles_is_already_channel_shaped_no_special_case_needed(tmp_path):
    # golden records HANDLES as "code(handler) -- channel" (src=handler, dst=channel) --
    # the generic dst.channel branch already produces the correct normalized tuple
    # (type, handler_service, handler_qualified, chan_id); see module docstring.
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    result = load_golden_edges(path, {"HANDLES"})
    assert result == {
        ("HANDLES", "document-management", "app.routes.documents.get_document",
         "chan:http:document-management:GET /documents/{doc_id}"),
    }


def test_load_golden_edges_filters_by_requested_types(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    # DEPENDS_ON present in file but not requested -- excluded entirely.
    result = load_golden_edges(path, {"PRODUCES", "HANDLES"})
    assert all(e[0] in ("PRODUCES", "HANDLES") for e in result)
    assert len(result) == 2


def test_load_golden_edges_mechanism_filter_applies_only_to_calls(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    # Of the two CALLS records in GOLDEN_YAML: the temporal_start one carries
    # mechanism -> excluded (mirrors calls_eval); the plain create_order->OrderService
    # one carries no mechanism key -> stays included.
    calls = load_golden_edges(path, {"CALLS"})
    assert calls == {
        ("CALLS", "orders-api", "app.routes.orders.create_order",
         "orders-api", "app.services.order.OrderService"),
    }
    # INVOKES_ACTIVITY record ALSO carries a (synthetic, test-only) mechanism key --
    # but the filter is scoped to type==CALLS only, so it must NOT be excluded.
    activity = load_golden_edges(path, {"INVOKES_ACTIVITY"})
    assert activity == {
        ("INVOKES_ACTIVITY", "kyc-worker", "app.workflows.kyc.KycWorkflow.run",
         "kyc-worker", "app.activities.documents.verify_documents"),
    }


def test_load_golden_edges_multiple_types_mix_tuple_shapes_in_one_set(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    result = load_golden_edges(path, {"DEPENDS_ON", "PRODUCES"})
    assert len(result) == 2
    lengths = {len(e) for e in result}
    assert lengths == {5, 4}  # DEPENDS_ON (symbol dst) vs PRODUCES (channel dst)


def test_load_golden_edges_empty_types_returns_empty_set(tmp_path):
    path = tmp_path / "edges.yaml"
    path.write_text(GOLDEN_YAML)
    assert load_golden_edges(path, set()) == set()


# -- found_edges --------------------------------------------------------


def _node(id_: str, service: str, qualified_name: str, kind: str = "Function") -> NodeRec:
    return NodeRec(
        id=id_, kind=kind, service=service,
        name=qualified_name.rsplit(".", 1)[-1], qualified_name=qualified_name,
    )


def _chan_node(id_: str) -> NodeRec:
    return NodeRec(id=id_, kind="Channel", service="", name=id_, qualified_name=id_)


def _edge(src: str, dst: str, type_: str, extractor: str = "x") -> EdgeRec:
    return EdgeRec(src=src, dst=dst, type=type_, resolution="static", confidence=1.0,
                    extractor=extractor)


def test_found_edges_symbol_both_ends_resolves_via_node_join(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([
        _node("sym:orders-api:a", "orders-api", "app.routes.orders.create_order"),
        _node("sym:orders-api:b", "orders-api", "app.db.session.get_db"),
    ])
    st.upsert_edges([_edge("sym:orders-api:a", "sym:orders-api:b", "DEPENDS_ON")])

    edges, dangling = found_edges(st, {"DEPENDS_ON"})

    assert edges == {
        ("DEPENDS_ON", "orders-api", "app.routes.orders.create_order",
         "orders-api", "app.db.session.get_db"),
    }
    assert dangling == 0


def test_found_edges_channel_dst_uses_id_directly_without_node_join(tmp_path):
    # chan-концы берутся как id напрямую (без join) -- proven here by NOT staging any
    # Channel node at all: the edge must still surface, dst used verbatim as the id.
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([_node("sym:orders-api:place", "orders-api",
                           "app.services.order.OrderService.place")])
    st.upsert_edges([
        _edge("sym:orders-api:place", "chan:event_type:OrderCreated", "PRODUCES"),
    ])

    edges, dangling = found_edges(st, {"PRODUCES"})

    assert edges == {
        ("PRODUCES", "orders-api", "app.services.order.OrderService.place",
         "chan:event_type:OrderCreated"),
    }
    assert dangling == 0


def test_found_edges_handles_normalizes_channel_to_handler_direction(tmp_path):
    # staged direction: src=channel id, dst=handler sym id (fastapi_ext convention,
    # Channel -HANDLES-> RouteHandler) -- must normalize to (type, handler_service,
    # handler_qualified, chan_id), matching golden's "code--channel" tuple shape.
    st = Staging(tmp_path / "s.db")
    st.begin_service("document-management")
    st.upsert_nodes([
        _chan_node("chan:http:document-management:GET /documents/{doc_id}"),
        _node("sym:document-management:h", "document-management",
              "app.routes.documents.get_document"),
    ])
    st.upsert_edges([
        _edge("chan:http:document-management:GET /documents/{doc_id}",
              "sym:document-management:h", "HANDLES"),
    ])

    edges, dangling = found_edges(st, {"HANDLES"})

    assert edges == {
        ("HANDLES", "document-management", "app.routes.documents.get_document",
         "chan:http:document-management:GET /documents/{doc_id}"),
    }
    assert dangling == 0


def test_found_edges_handles_dangling_handler_is_counted_channel_end_never_dangles(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("document-management")
    st.upsert_nodes([_chan_node("chan:http:document-management:GET /x")])  # handler never staged
    st.upsert_edges([
        _edge("chan:http:document-management:GET /x", "sym:document-management:ghost", "HANDLES"),
    ])

    edges, dangling = found_edges(st, {"HANDLES"})

    assert edges == set()
    assert dangling == 1


def test_found_edges_dangling_symbol_src_is_counted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([_node("sym:orders-api:b", "orders-api", "app.db.session.get_db")])
    st.upsert_edges([_edge("sym:orders-api:ghost", "sym:orders-api:b", "DEPENDS_ON")])

    edges, dangling = found_edges(st, {"DEPENDS_ON"})

    assert edges == set()
    assert dangling == 1


def test_found_edges_dangling_symbol_dst_is_counted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([_node("sym:orders-api:a", "orders-api", "app.routes.orders.create_order")])
    st.upsert_edges([_edge("sym:orders-api:a", "sym:orders-api:ghost", "DEPENDS_ON")])

    edges, dangling = found_edges(st, {"DEPENDS_ON"})

    assert edges == set()
    assert dangling == 1


def test_found_edges_channel_dst_src_dangling_is_counted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_edges([_edge("sym:orders-api:ghost", "chan:event_type:OrderCreated", "PRODUCES")])

    edges, dangling = found_edges(st, {"PRODUCES"})

    assert edges == set()
    assert dangling == 1


def test_found_edges_treats_missing_qualified_name_as_dangling(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([
        _node("sym:orders-api:a", "orders-api", "app.routes.orders.create_order"),
        NodeRec(id="sym:orders-api:b", kind="Function", service="orders-api",
                name="b", qualified_name=""),
    ])
    st.upsert_edges([_edge("sym:orders-api:a", "sym:orders-api:b", "DEPENDS_ON")])

    edges, dangling = found_edges(st, {"DEPENDS_ON"})

    assert edges == set()
    assert dangling == 1


def test_found_edges_filters_by_requested_types_excludes_from_edges_and_dangling(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    # CONTAINS with a dangling dst -- outside the requested type set: must not appear
    # in edges NOR bump the dangling counter (calls_eval.found_calls precedent: ignored
    # types are invisible end-to-end, see test_found_calls_ignores_non_calls_edges).
    st.upsert_nodes([_node("sym:orders-api:a", "orders-api", "app.routes.orders.create_order")])
    st.upsert_edges([_edge("sym:orders-api:a", "sym:orders-api:ghost", "CONTAINS")])

    edges, dangling = found_edges(st, {"DEPENDS_ON"})

    assert edges == set()
    assert dangling == 0


def test_found_edges_multiple_types_at_once(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("orders-api")
    st.upsert_nodes([
        _node("sym:orders-api:a", "orders-api", "app.routes.orders.create_order"),
        _node("sym:orders-api:b", "orders-api", "app.db.session.get_db"),
        _node("sym:orders-api:place", "orders-api", "app.services.order.OrderService.place"),
    ])
    st.upsert_edges([
        _edge("sym:orders-api:a", "sym:orders-api:b", "DEPENDS_ON"),
        _edge("sym:orders-api:place", "chan:event_type:OrderCreated", "PRODUCES"),
    ])

    edges, dangling = found_edges(st, {"DEPENDS_ON", "PRODUCES"})

    assert edges == {
        ("DEPENDS_ON", "orders-api", "app.routes.orders.create_order",
         "orders-api", "app.db.session.get_db"),
        ("PRODUCES", "orders-api", "app.services.order.OrderService.place",
         "chan:event_type:OrderCreated"),
    }
    assert dangling == 0
