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
    trace_calls: list | None = None, trace_kwargs: list | None = None,
):
    """Fake replacing codegraph.cli.GraphQuery: resolve_selector returns
    `resolve_result` (default: a successful resolve to node id "e1", matching
    `_SUCCESS_RESULT`'s own entry id), trace_process returns `trace_result` unchanged.
    `resolve_calls`/`trace_calls`, if given, record every call's argument (spy) --
    used to prove trace_process is skipped when resolve_selector errors.
    `trace_kwargs`, if given, records each call's full kwargs dict (M5 T5 --
    proving `--full` maps to compact=False without also having to touch every
    OTHER existing `trace_calls`-only caller of this helper)."""
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
            if trace_kwargs is not None:
                trace_kwargs.append(kwargs)
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


# -- M9 T1 (docs/superpowers/reports/2026-07-24-pilot-rerun-3.md §3): external
# exits ("channel VERB /path -> external <host>") -- text + mermaid, both renderers.

_EXTERNAL_EXIT_RESULT = {
    "segments": [
        {
            "service": "orders-api",
            "entry": {"id": "e1", "name": "create_order"},
            "steps": [],
            "exits": [
                {
                    "channel": {
                        "id": "chan:http:?:POST /api/v1/users/legal-entities",
                        "name": "POST /api/v1/users/legal-entities",
                        "external": True,
                        "external_host": "api-gateway.prod.svc.cluster.local",
                    },
                    "next_entry_ids": [],
                },
            ],
            "truncated": False,
        },
    ],
    "confidence": 1.0,
    "truncated": False,
}


def test_trace_text_format_renders_external_exit_as_external_host(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_EXTERNAL_EXIT_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    # whitespace-stripped (see tests/eval/test_m2_gate.py's own _cli_output_flat):
    # rich's Tree wraps long unbroken tokens under CliRunner's narrow non-tty width,
    # which can otherwise split "external api-gateway...local" across two lines.
    flat = "".join(result.output.split())
    assert "POST/api/v1/users/legal-entities" in flat
    assert "externalapi-gateway.prod.svc.cluster.local" in flat
    assert "unresolved" not in result.output.lower()


def test_trace_text_format_renders_plain_unresolved_exit_unchanged(tmp_path, monkeypatch):
    """Regression pin: a dead-end exit with NO external flag still renders the
    pre-existing "unresolved" fallback text, byte-identical to before this task."""
    plain_unresolved_result = {
        "segments": [
            {
                "service": "orders-api",
                "entry": {"id": "e1", "name": "create_order"},
                "steps": [],
                "exits": [{"channel": {"id": "c1", "name": "GET /nowhere"}, "next_entry_ids": []}],
                "truncated": False,
            },
        ],
        "confidence": 0.5,
        "truncated": False,
    }
    root = _write_workspace(tmp_path)
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery", _fake_graph_query(plain_unresolved_result)
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "channel GET /nowhere -> unresolved" in result.output


def test_trace_text_format_prefers_resolved_next_ids_over_external_flag(tmp_path, monkeypatch):
    """Defensive edge case: a channel that's BOTH flagged external AND (however
    unexpectedly) has a resolved next_entry_ids shows the REAL resolved
    destination -- "external <host>" is only ever the fallback label for a
    dead-end exit, never shown when a real next hop is known."""
    result_dict = {
        "segments": [
            {
                "service": "orders-api",
                "entry": {"id": "e1", "name": "create_order"},
                "steps": [],
                "exits": [
                    {
                        "channel": {
                            "id": "c1", "name": "X", "external": True, "external_host": "h",
                        },
                        "next_entry_ids": ["e2"],
                    },
                ],
                "truncated": False,
            },
            {
                "service": "b", "entry": {"id": "e2", "name": "handler"},
                "steps": [], "exits": [], "truncated": False,
            },
        ],
        "confidence": 1.0,
        "truncated": False,
    }
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(result_dict))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "e2" in result.output
    assert "external" not in result.output.lower()


def test_trace_mermaid_format_renders_external_exit_leaf_node(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_EXTERNAL_EXIT_RESULT))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert "flowchart TD" in result.output
    assert 'external api-gateway.prod.svc.cluster.local"]' in result.output
    assert "S0 -->|POST /api/v1/users/legal-entities|" in result.output


def test_trace_mermaid_no_external_leaf_for_plain_unresolved_exit(tmp_path, monkeypatch):
    """Regression pin: mermaid draws NO arrow/node at all for a plain (non-
    external) dead-end exit -- unchanged pre-existing behavior (see
    _trace_mermaid's own module docstring: dangling/unresolved next hops are
    silently dropped, not drawn as a leaf)."""
    plain_unresolved_result = {
        "segments": [
            {
                "service": "orders-api",
                "entry": {"id": "e1", "name": "create_order"},
                "steps": [],
                "exits": [{"channel": {"id": "c1", "name": "GET /nowhere"}, "next_entry_ids": []}],
                "truncated": False,
            },
        ],
        "confidence": 0.5,
        "truncated": False,
    }
    root = _write_workspace(tmp_path)
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery", _fake_graph_query(plain_unresolved_result)
    )

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert result.output.count('S0["') == 1  # only the segment node, no extra leaf
    assert "-->" not in result.output


def test_trace_mermaid_escapes_quotes_in_external_host_label(tmp_path, monkeypatch):
    crafted = {
        "segments": [
            {
                "service": "orders-api",
                "entry": {"id": "e1", "name": "create_order"},
                "steps": [],
                "exits": [
                    {
                        "channel": {
                            "id": "c1", "name": 'X"y',
                            "external": True, "external_host": 'weird"host',
                        },
                        "next_entry_ids": [],
                    },
                ],
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
    assert 'weird"host' not in result.output
    assert "weird'host" in result.output


# -- M5 T5: compact rendering (collapsed marker steps) + --full CLI flag --

_COLLAPSED_RESULT = {
    "segments": [
        {
            "service": "orders-api",
            "entry": {"id": "e1", "name": "create_order"},
            "steps": [
                {
                    "edge_type": "CALLS", "props": {},
                    "node": {"id": "s1", "name": "first_call"}, "direction": "out",
                },
                {"collapsed": 35},
                {
                    "edge_type": "CALLS", "props": {},
                    "node": {"id": "s2", "name": "last_call"}, "direction": "out",
                },
            ],
            "exits": [],
            "truncated": False,
        },
    ],
    "confidence": 1.0,
    "truncated": False,
}


def test_trace_text_format_renders_collapsed_marker_as_ellipsis_count(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_COLLAPSED_RESULT))

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert "⋯ 35 внутренних вызовов" in result.output
    # the real steps flanking the marker still render normally
    assert "first_call" in result.output
    assert "last_call" in result.output


def test_trace_mermaid_annotates_segment_label_with_collapsed_total(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_COLLAPSED_RESULT))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert "⋯35" in result.output
    # mermaid still shows exactly one node per segment (no per-step nodes) --
    # the collapse total is folded into that SAME segment label, not a new node.
    assert result.output.count('S0["') == 1


def test_trace_mermaid_no_annotation_when_nothing_collapsed(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    monkeypatch.setattr("codegraph.cli.GraphQuery", _fake_graph_query(_SUCCESS_RESULT))

    result = runner.invoke(
        app, ["trace", "orders-api:POST /orders", str(root), "--format", "mermaid"]
    )
    assert result.exit_code == 0, result.output
    assert "⋯" not in result.output


def test_trace_default_passes_compact_true_to_trace_process(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    trace_kwargs: list = []
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery",
        _fake_graph_query(_SUCCESS_RESULT, trace_kwargs=trace_kwargs),
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root)])
    assert result.exit_code == 0, result.output
    assert trace_kwargs[-1].get("compact") is True


def test_trace_full_flag_passes_compact_false_to_trace_process(tmp_path, monkeypatch):
    root = _write_workspace(tmp_path)
    trace_kwargs: list = []
    monkeypatch.setattr(
        "codegraph.cli.GraphQuery",
        _fake_graph_query(_SUCCESS_RESULT, trace_kwargs=trace_kwargs),
    )

    result = runner.invoke(app, ["trace", "orders-api:POST /orders", str(root), "--full"])
    assert result.exit_code == 0, result.output
    assert trace_kwargs[-1].get("compact") is False


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
