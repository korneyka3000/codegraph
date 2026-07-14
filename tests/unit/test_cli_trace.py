"""CLI `codegraph trace` (M2 T8): CliRunner + monkeypatch codegraph.cli.resolve_selector
(selector -> entrypoint id, bypassing a real Staging scan) and codegraph.cli.GraphQuery
(fake trace_process, bypassing real FalkorDB) -- no live store, no SCIP (that's
tests/eval's M2 gate, marker scip+falkordb). Only codegraph.cli.resolve_selector/
GraphQuery are monkeypatched by NAME (not via codegraph.linking.processes/
codegraph.query.api) -- see cli.py's own module docstring on why: patching must target
the name as resolved from cli.py's global namespace at call time."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import _node_label, app
from codegraph.stores.staging import Staging

runner = CliRunner()


# -- _node_label: found live (verify skill, manual CLI run against a real FalkorDB
# mini-graph) that Channel nodes set qualified_name == id (see
# core.schema.make_channel_node docstring) -- preferring qualified_name unconditionally
# made every channel label in trace output render as the raw "chan:event_type:..." id
# instead of the friendlier "name" (e.g. "OrderCreated"). --


def test_node_label_prefers_qualified_name_when_it_differs_from_id():
    node = {"id": "sym:a:f", "name": "f", "qualified_name": "app.f"}
    assert _node_label(node) == "app.f"


def test_node_label_prefers_name_when_qualified_name_equals_id():
    node = {"id": "chan:event_type:X", "name": "X", "qualified_name": "chan:event_type:X"}
    assert _node_label(node) == "X"


def test_node_label_falls_back_through_name_then_id_then_placeholder():
    assert _node_label({"id": "n1", "name": "n"}) == "n"
    assert _node_label({"id": "n1"}) == "n1"
    assert _node_label({}) == "?"


def _write_workspace(tmp_path: Path, graph_name: str = "wsgraph") -> Path:
    root = tmp_path / "ws"
    (root / "svc0").mkdir(parents=True)
    (root / "codegraph.yaml").write_text(
        f"version: 1\ngraph_name: {graph_name}\nservices:\n  - name: svc0\n    path: ./svc0\n"
    )
    return root


def _with_staging(root: Path) -> Path:
    staging_path = root / ".codegraph" / "staging.db"
    Staging(staging_path).close()
    return staging_path


_SUCCESS_RESULT = {
    "segments": [
        {
            "service": "orders-api",
            "entry": {
                "id": "e1",
                "name": "create_order",
                "qualified_name": "app.routes.create_order",
            },
            "steps": [
                {
                    "edge_type": "CALLS",
                    "props": {},
                    "node": {"id": "s1", "name": "save_order"},
                    "direction": "out",
                },
            ],
            "exits": [
                {"channel": {"id": "c1", "name": "OrderCreated"}, "next_entry_ids": ["e2"]},
            ],
            "truncated": False,
        },
        {
            "service": "kyc-worker",
            "entry": {"id": "e2", "name": "handle_order_created"},
            "steps": [],
            "exits": [],
            "truncated": False,
        },
    ],
    "confidence": 0.9,
    "truncated": False,
}


def _fake_graph_query(result: dict):
    class _FakeGraphQuery:
        def __init__(self, store_factory, service_paths):
            self.store_factory = store_factory
            self.service_paths = service_paths

        def trace_process(self, entrypoint_id, **kwargs):
            return result

    return _FakeGraphQuery


def _fake_resolve_selector(entrypoint_id: str | None):
    def fn(staging, selector):
        return entrypoint_id

    return fn


# -- text format (default) --


def test_trace_text_format_prints_segment_chain(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "orders-api" in result.output
    assert "create_order" in result.output
    assert "save_order" in result.output
    assert "kyc-worker" in result.output
    assert "handle_order_created" in result.output
    assert "OrderCreated" in result.output


def test_trace_text_is_default_format(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "flowchart TD" not in result.output


# -- mermaid format --


def test_trace_mermaid_format_prints_valid_flowchart(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert "flowchart TD" in result.output
    # node label prefers qualified_name over name (see cli._node_label) --
    # entry has both, qualified_name wins.
    assert 'S0["orders-api: app.routes.create_order"]' in result.output
    assert 'S1["kyc-worker: handle_order_created"]' in result.output
    assert "S0 -->|OrderCreated| S1" in result.output


def test_trace_invalid_format_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root), "--format", "yaml"])
    assert result.exit_code == 1
    assert "invalid" in result.output.lower()
    assert "Traceback" not in result.output


def test_trace_mermaid_escapes_quotes_and_pipes_in_labels(tmp_path, monkeypatch):
    # T8 review fast-follow (reviewer's crafted-name scenario): a raw `"` inside a
    # node label closes mermaid's quoted node text early; a raw `|` inside an edge
    # label closes the |label| early -- pin the smoke-level escaping _trace_mermaid
    # applies: `"` -> `'` in node labels, `|` -> `/` and `"` -> `'` in edge labels.
    crafted = {
        "segments": [
            {
                "service": "orders-api",
                "entry": {"id": "e1", "name": 'we"ird'},
                "steps": [],
                "exits": [
                    {"channel": {"id": "c1", "name": 'Order|Created"x'}, "next_entry_ids": ["e2"]},
                ],
                "truncated": False,
            },
            {
                "service": "kyc-worker",
                "entry": {"id": "e2", "name": "handler"},
                "steps": [],
                "exits": [],
                "truncated": False,
            },
        ],
        "confidence": 1.0,
        "truncated": False,
    }
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(crafted))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert 'S0["orders-api: we\'ird"]' in result.output  # `"` in node label -> `'`
    assert "S0 -->|Order/Created'x| S1" in result.output  # `|` -> `/`, `"` -> `'` in edge label
    assert 'we"ird' not in result.output  # no raw quote survives anywhere
    assert 'Order|Created"x' not in result.output  # no raw pipe/quote in the edge label


# -- not-found entrypoint --


def test_trace_entrypoint_not_found_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector(None))
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /nope", str(root)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()
    assert "orders-api:POST /nope" in result.output
    assert "Traceback" not in result.output


# -- trace_process itself returns an error dict (e.g. store unreachable) --


def test_trace_process_error_dict_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    _with_staging(root)
    monkeypatch.setattr("codegraph.cli.resolve_selector", _fake_resolve_selector("e1"))
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery", _fake_graph_query({"error": "falkordb unreachable: boom"})
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


# -- missing staging DB --


def test_trace_missing_staging_db_is_red_error_exit_1(tmp_path):
    root = _write_workspace(tmp_path)  # no staging.db ever created
    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 1
    assert "staging" in result.output.lower()
    assert "Traceback" not in result.output
