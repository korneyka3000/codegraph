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


def test_golden_questions_wellformed():
    data = yaml.safe_load((FIXTURES / "golden" / "questions.yaml").read_text())
    assert data["version"] == 1
    assert len(data["questions"]) == 5
    known_services = {"orders-api", "kyc-worker", "document-management"}
    for q in data["questions"]:
        assert isinstance(q["question"], str) and q["question"]
        assert q["k"] == 3
        assert q["accept"], q  # non-empty OR-set
        for a in q["accept"]:
            assert a["service"] in known_services, a
            assert isinstance(a["symbol"], str) and a["symbol"]


def test_golden_traces_reference_channels_from_edges():
    edges = yaml.safe_load((FIXTURES / "golden" / "edges.yaml").read_text())
    traces = yaml.safe_load((FIXTURES / "golden" / "traces.yaml").read_text())
    known_channels = {
        e["dst"]["channel"] for e in edges["edges"] if "channel" in e["dst"]
    }
    trace = traces["traces"][0]
    assert trace["entrypoint"] == "orders-api:POST /orders"
    # 4 сегмента с M2 T9 fix wave: containment fan-out (T7 segments.derive) добавил
    # run_consumer-ветку (см. комментарий в traces.yaml) к исходной линейной цепочке.
    assert len(trace["segments"]) == 4
    for seg in trace["segments"][1:]:
        assert seg["via_channel"] in known_channels
