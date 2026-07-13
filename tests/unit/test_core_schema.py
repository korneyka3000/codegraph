import dataclasses

import pytest

from codegraph.core.schema import EdgeRec, NodeRec, make_service_node


def test_node_rec_frozen():
    n = NodeRec(id="x", kind="Function", service="s", name="f", qualified_name="m.f")
    with pytest.raises(dataclasses.FrozenInstanceError):
        n.id = "y"


def test_make_service_node():
    n = make_service_node("orders-api")
    assert n.id == "svc:orders-api" and n.kind == "Service"


def test_edge_defaults():
    e = EdgeRec(src="a", dst="b", type="CALLS", resolution="static",
                confidence=1.0, extractor="calls")
    assert e.props == {} and e.evidence_line is None
