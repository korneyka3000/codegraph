from pathlib import Path

import yaml

from codegraph.config.loader import load_workspace

FIXTURES = Path(__file__).parents[2] / "fixtures"

EDGE_TYPES = {
    "CONTAINS", "IMPORTS", "CALLS", "HANDLES", "DEPENDS_ON",
    "PRODUCES", "CONSUMES", "INVOKES_ACTIVITY", "CALLS_HTTP",
}


def test_fixture_workspace_loads():
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    assert {s.name for s in cfg.services} == {
        "orders-api", "kyc-worker", "document-management",
    }
    orders = next(s for s in cfg.services if s.name == "orders-api")
    assert orders.idioms.producers[0].name == "outbox"
    assert cfg.processes[0].entrypoint == "orders-api:POST /orders"


def test_golden_edges_wellformed():
    data = yaml.safe_load((FIXTURES / "golden" / "edges.yaml").read_text())
    assert data["version"] == 1
    assert len(data["edges"]) >= 28
    for e in data["edges"]:
        assert e["type"] in EDGE_TYPES, e
        assert "service" in e["src"] and "symbol" in e["src"]
        assert ("channel" in e["dst"]) != ("symbol" in e["dst"])  # ровно одно
        if "mechanism" in e:
            assert isinstance(e["mechanism"], str) and e["mechanism"]


def test_golden_traces_reference_channels_from_edges():
    edges = yaml.safe_load((FIXTURES / "golden" / "edges.yaml").read_text())
    traces = yaml.safe_load((FIXTURES / "golden" / "traces.yaml").read_text())
    known_channels = {
        e["dst"]["channel"] for e in edges["edges"] if "channel" in e["dst"]
    }
    trace = traces["traces"][0]
    assert trace["entrypoint"] == "orders-api:POST /orders"
    assert len(trace["segments"]) == 3
    for seg in trace["segments"][1:]:
        assert seg["via_channel"] in known_channels
