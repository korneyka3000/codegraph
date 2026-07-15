"""Юнит-тест pipeline.load._labels_for_kind/_node_props (M2): чистые функции
kind/roles -> labels-кортеж и NodeRec -> props-dict, без живого FalkorDB (тот
сценарий -- tests/integration/test_pipeline_load.py, marker falkordb, реальный
MERGE с multi-label)."""

from __future__ import annotations

import pytest

from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.pipeline.load import _edge_row, _key_props_for, _labels_for_kind, _node_props


def test_code_kind_without_roles():
    assert _labels_for_kind("Function") == ("Sym", "Function")
    assert _labels_for_kind("Class") == ("Sym", "Class")
    assert _labels_for_kind("Module") == ("Sym", "Module")


def test_code_kind_with_roles_appended_in_order():
    assert _labels_for_kind("Function", ("RouteHandler",)) == ("Sym", "Function", "RouteHandler")
    assert _labels_for_kind("Function", ("MessageConsumer", "TemporalActivity")) == (
        "Sym", "Function", "MessageConsumer", "TemporalActivity",
    )


def test_service_kind_ignores_roles():
    assert _labels_for_kind("Service") == ("Service",)
    assert _labels_for_kind("Service", ("RouteHandler",)) == ("Service",)


def test_channel_kind():
    assert _labels_for_kind("Channel") == ("Channel",)
    assert _labels_for_kind("Channel", ("RouteHandler",)) == ("Channel",)


def test_business_process_kind():
    assert _labels_for_kind("BusinessProcess") == ("BusinessProcess",)


def test_unknown_kind_raises_invariant_error():
    with pytest.raises(InvariantError):
        _labels_for_kind("Nope")


# -- _node_props: roles mirrored into props (M2 T8, traverse.py needs them --
# store.get_nodes()/neighbors() only ever return n.properties, never labels(n),
# so a role-carrying node's roles must ALSO live as an explicit prop, redundant
# with the graph labels _labels_for_kind produces) --


def test_node_props_includes_roles_list_when_present():
    n = NodeRec(
        id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f",
        roles=("RouteHandler",),
    )
    assert _node_props(n)["roles"] == ["RouteHandler"]


def test_node_props_preserves_role_order_for_multiple_roles():
    n = NodeRec(
        id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f",
        roles=("MessageConsumer", "TemporalActivity"),
    )
    assert _node_props(n)["roles"] == ["MessageConsumer", "TemporalActivity"]


def test_node_props_omits_roles_key_when_no_roles():
    n = NodeRec(id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f")
    assert "roles" not in _node_props(n)


def test_node_props_omits_roles_key_for_channel_and_service_kinds():
    # Channel/Service/BusinessProcess NodeRecs never carry roles (roles are only
    # meaningful for code kinds) -- default roles=() -- same omission as above,
    # exercised on the non-code kinds specifically since those are the ones
    # _labels_for_kind ignores roles for entirely.
    from codegraph.core.schema import make_service_node

    assert "roles" not in _node_props(make_service_node("svc"))


# -- M3 T1: _edge_row/_key_props_for -- NEXT_SEGMENT's via_channel_id promoted to the
# row's TOP level (alongside src/dst), since batch.upsert_edges' MERGE-key Cypher reads
# key_props as r.<k>, not r.props.<k> (see stores/falkordb/batch.py's upsert_edges
# docstring and core/schema.py's SCHEMA_VERSION "2 -> 3" history comment) --


def test_key_props_for_next_segment_is_via_channel_id():
    assert _key_props_for("NEXT_SEGMENT") == ("via_channel_id",)


def test_key_props_for_other_types_is_empty():
    assert _key_props_for("CALLS") == ()
    assert _key_props_for("PRODUCES") == ()


def test_edge_row_promotes_via_channel_id_to_top_level_for_next_segment():
    e = EdgeRec(src="a", dst="b", type="NEXT_SEGMENT", resolution="derived", confidence=0.9,
                extractor="linking",
                props={"via_channel_id": "chan:kafka_topic:orders", "derived": True})
    row = _edge_row(e)
    assert row["src"] == "a" and row["dst"] == "b"
    assert row["via_channel_id"] == "chan:kafka_topic:orders"
    # still inside props too -- that's what SET e += r.props actually persists as a
    # real graph edge property (the top-level copy only feeds the MERGE key).
    assert row["props"]["via_channel_id"] == "chan:kafka_topic:orders"


def test_edge_row_normalizes_missing_via_channel_id_to_empty_string():
    e = EdgeRec(src="a", dst="b", type="NEXT_SEGMENT", resolution="derived", confidence=0.9,
                extractor="linking")
    row = _edge_row(e)
    assert row["via_channel_id"] == ""


def test_edge_row_other_types_get_no_via_channel_id_key_at_all():
    e = EdgeRec(src="a", dst="b", type="CALLS", resolution="static", confidence=1.0,
                extractor="calls")
    row = _edge_row(e)
    assert "via_channel_id" not in row
    assert set(row) == {"src", "dst", "props"}
