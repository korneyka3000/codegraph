"""CLI `codegraph trace` (M2 T8; M3 T2 rework): CliRunner + monkeypatch
codegraph.cli.GraphQuery (fake resolve_selector + trace_process, bypassing real
FalkorDB) -- no live store, no SCIP (that's tests/eval's gate, marker scip+falkordb).
Only codegraph.cli.GraphQuery is monkeypatched by NAME (not via codegraph.query.api)
-- see cli.py's own module docstring on why: patching must target the name as
resolved from cli.py's global namespace at call time.

M3 T2: `trace` no longer touches Staging/staging.db AT ALL -- selector resolution
moved from linking.processes.resolve_selector (staging-side) to
query.api.GraphQuery.resolve_selector (graph-side), closing the M2 final review
carry-item that `codegraph trace` hard-required a prior `codegraph index` run's
staging.db on disk purely to resolve the selector string, even though the trace walk
itself was always graph-only. See test_trace_works_without_a_staging_db below --
the regression anchor for that fix."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from codegraph.cli import _node_label, app

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


def _fake_graph_query(
    trace_result: dict, resolve_result: dict | None = None, resolve_calls: list | None = None,
    trace_calls: list | None = None,
):
    """Fake replacing codegraph.cli.GraphQuery: resolve_selector returns
    `resolve_result` (default: a successful resolve to node id "e1", matching
    `_SUCCESS_RESULT`'s own entry id), trace_process returns `trace_result` unchanged.
    `resolve_calls`/`trace_calls`, if given, record every call's argument (spy) --
    used to prove trace_process is skipped when resolve_selector errors."""
    if resolve_result is None:
        resolve_result = {"node_id": "e1"}

    class _FakeGraphQuery:
        def __init__(self, store_factory, service_paths):
            self.store_factory = store_factory
            self.service_paths = service_paths

        def resolve_selector(self, selector):
            if resolve_calls is not None:
                resolve_calls.append(selector)
            return resolve_result

        def trace_process(self, entrypoint_id, **kwargs):
            if trace_calls is not None:
                trace_calls.append(entrypoint_id)
            return trace_result

    return _FakeGraphQuery


# -- text format (default) --


def test_trace_text_format_prints_segment_chain(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
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
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "flowchart TD" not in result.output


# -- mermaid format --


def test_trace_mermaid_format_prints_valid_flowchart(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
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
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(crafted))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert 'S0["orders-api: we\'ird"]' in result.output  # `"` in node label -> `'`
    assert "S0 -->|Order/Created'x| S1" in result.output  # `|` -> `/`, `"` -> `'` in edge label
    assert 'we"ird' not in result.output  # no raw quote survives anywhere
    assert 'Order|Created"x' not in result.output  # no raw pipe/quote in the edge label


# -- not-found entrypoint (resolve_selector returns an error dict) --


def test_trace_entrypoint_not_found_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    not_found = {"error": "entrypoint not found for selector: orders-api:POST /nope"}
    trace_calls: list = []
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery",
        _fake_graph_query(_SUCCESS_RESULT, resolve_result=not_found, trace_calls=trace_calls),
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /nope", str(root)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()
    assert "orders-api:POST /nope" in result.output
    assert "Traceback" not in result.output
    assert trace_calls == []  # trace_process must never be called once resolve fails


def test_trace_resolve_selector_store_unreachable_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    unreachable = {"error": "falkordb unreachable: connection refused"}
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT, resolve_result=unreachable)
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


# -- trace_process itself returns an error dict (e.g. store unreachable) --


def test_trace_process_error_dict_is_red_error_exit_1(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery",
        _fake_graph_query({"error": "falkordb unreachable: boom"}),
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 1
    assert "falkordb unreachable" in result.output
    assert "Traceback" not in result.output


# -- M3 T2 regression: trace no longer requires a staging.db at all --


def test_trace_works_without_a_staging_db(tmp_path, monkeypatch):
    """M2 final review carry-item, closed by M3 T2: `codegraph trace` used to require
    `.codegraph/staging.db` from a prior `codegraph index` run purely to resolve the
    selector string (linking.processes.resolve_selector, staging-side), even though
    the actual trace walk was always graph-only. Selector resolution now goes through
    query.api.GraphQuery.resolve_selector (graph-side) -- trace must succeed even when
    `.codegraph/` doesn't exist at all."""
    root = _write_workspace(tmp_path)  # no .codegraph/ directory, no staging.db, ever
    assert not (root / ".codegraph").exists()
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])

    assert result.exit_code == 0, result.output
    assert "create_order" in result.output
    assert not (root / ".codegraph").exists()  # trace still didn't create one
