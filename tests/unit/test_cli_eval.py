"""M3 T8: cli.py `eval retrieval` command -- questions-file resolution (default vs
`--questions` override), `--k` override plumbing, store-unreachable/graph-not-found/
questions-not-found error boundaries (exit 1, consistent with stats/load/trace),
hit/miss table rendering + exit 0 regardless of hit-rate ("report, not gate" --
gate semantics live in tests/eval/test_m3_gate.py instead).

Same monkeypatch-the-module-level-name technique as test_cli_chunk_embed.py/
test_cli_m1b.py: `codegraph.cli.FalkorStore`/`GraphQuery`/`make_embedder`/
`load_questions`/`run_questions` are all imported by name into cli.py, so tests
replace exactly those names rather than reaching into codegraph.evalx/query.api
directly."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import codegraph
from codegraph.cli import _DEFAULT_QUESTIONS, app
from codegraph.core.errors import CodegraphError
from codegraph.evalx.retrieval_eval import load_questions
from codegraph.stores.falkordb.connection import StoreUnavailable

runner = CliRunner()

REPO_ROOT = Path(__file__).parents[2]
FIXTURES_WS = REPO_ROOT / "fixtures" / "workspace.yaml"

_QUESTIONS = [
    {"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.a"}]},
    {"question": "q2", "k": 3, "accept": [{"service": "svc", "symbol": "app.b"}]},
]


def _fake_store_cls(*, exists: bool = True, unreachable: bool = False):
    class _Store:
        def __init__(self, cfg, name):
            self.cfg = cfg
            self.name = name

        def graph_exists(self) -> bool:
            if unreachable:
                raise StoreUnavailable("falkordb down")
            return exists

    return _Store


class _FakeGraphQuery:
    """Records construction args; `.search_code` echoes back k/mode/exact so a test
    can prove the CLI wired hybrid-mode (and, M5 T2, --exact) search through to
    `run_questions`' search_fn without needing a real store/embedder."""

    instances: list[_FakeGraphQuery] = []

    def __init__(self, store_factory, service_paths, embedder_factory=None):
        self.store_factory = store_factory
        self.service_paths = service_paths
        self.embedder_factory = embedder_factory
        _FakeGraphQuery.instances.append(self)

    def search_code(self, query, k=8, service=None, mode="hybrid", exact=False):
        return {"items": [], "mode_used": mode, "_query": query, "_k": k, "_exact": exact}


def _patch_common(monkeypatch, *, store_exists=True, store_unreachable=False):
    monkeypatch.setattr(
        "codegraph.cli.FalkorStore",
        _fake_store_cls(exists=store_exists, unreachable=store_unreachable),
    )
    _FakeGraphQuery.instances = []
    monkeypatch.setattr("codegraph.cli.GraphQuery", _FakeGraphQuery)
    monkeypatch.setattr("codegraph.cli.make_embedder", lambda cfg: object())


# ======================================================================================
# -- questions path resolution: default vs --questions override --
# ======================================================================================


def test_eval_retrieval_defaults_to_bundled_golden_questions_path(monkeypatch):
    seen: list[Path] = []

    def fake_load_questions(path):
        seen.append(path)
        return []

    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", fake_load_questions)
    monkeypatch.setattr("codegraph.cli.run_questions", lambda search_fn, qs: [])

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert seen == [_DEFAULT_QUESTIONS]
    assert _DEFAULT_QUESTIONS.name == "questions.yaml"


def test_eval_retrieval_explicit_questions_flag_overrides_default(monkeypatch, tmp_path):
    custom = tmp_path / "my_questions.yaml"
    custom.write_text("version: 1\nquestions: []\n")
    seen: list[Path] = []

    def fake_load_questions(path):
        seen.append(path)
        return []

    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", fake_load_questions)
    monkeypatch.setattr("codegraph.cli.run_questions", lambda search_fn, qs: [])

    result = runner.invoke(
        app, ["eval", "retrieval", str(FIXTURES_WS), "--questions", str(custom)]
    )

    assert result.exit_code == 0, result.output
    assert seen == [custom]


# -- M4 T2: _DEFAULT_QUESTIONS must be wheel-safe, exactly like cli.TEMPLATE (same
# repo-root-escaping Path(__file__).parent.parent.parent shape, same FileNotFoundError
# from a real wheel install where no repo checkout sits beside site-packages/).
# fixtures/golden/questions.yaml stays put as the LIVE golden set (tests/eval/
# test_m3_gate.py, test_golden_wellformed.py read it directly) -- what moves into
# package data is the COPY the CLI ships as `eval retrieval`'s default.


def test_default_questions_lives_inside_the_package_not_via_repo_root_escape():
    package_root = str(Path(codegraph.__file__).parent)
    assert str(_DEFAULT_QUESTIONS).startswith(package_root)


def test_default_questions_resources_read_works_and_rows_are_golden_shaped():
    """The shipped default must actually load through the same load_questions()
    path `eval retrieval` uses, and parse into non-empty golden-shaped rows --
    proves the resources read end-to-end, not just that the file exists. (Note:
    this test also passed against the OLD fixtures-path default -- the RED driver
    for the move is the structural test above plus the drift guard below.)"""
    questions = load_questions(_DEFAULT_QUESTIONS)
    assert len(questions) > 0
    for q in questions:
        assert {"question", "accept", "k"} <= q.keys()


def test_packaged_default_questions_byte_identical_to_live_golden_set():
    """Drift guard, same pattern as codegraph.example.yaml's: the packaged default
    (src/codegraph/data/questions.yaml, what a wheel-installed `eval retrieval`
    reads) must never silently diverge from the live golden set
    (fixtures/golden/questions.yaml, what the M3 gate runs) -- today they are the
    same questions by design (cli.py's --questions help says the default IS the
    bundled golden file). Only catches divergence; doesn't enforce which to edit."""
    packaged = REPO_ROOT / "src" / "codegraph" / "data" / "questions.yaml"
    golden = REPO_ROOT / "fixtures" / "golden" / "questions.yaml"
    assert packaged.read_bytes() == golden.read_bytes()


# ======================================================================================
# -- --k override plumbing --
# ======================================================================================


def test_eval_retrieval_k_override_rewrites_every_question(monkeypatch):
    received: list[dict] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)

    def fake_run_questions(search_fn, questions):
        received.extend(questions)
        return []

    monkeypatch.setattr("codegraph.cli.run_questions", fake_run_questions)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS), "--k", "7"])

    assert result.exit_code == 0, result.output
    assert [q["k"] for q in received] == [7, 7]
    # question/accept content untouched by the override -- only k changes.
    assert [q["question"] for q in received] == ["q1", "q2"]


def test_eval_retrieval_without_k_flag_keeps_each_questions_own_k(monkeypatch):
    received: list[dict] = []

    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)

    def fake_run_questions(search_fn, questions):
        received.extend(questions)
        return []

    monkeypatch.setattr("codegraph.cli.run_questions", fake_run_questions)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert [q["k"] for q in received] == [3, 3]


# ======================================================================================
# -- error boundaries: exit 1, consistent with stats/load/trace --
# ======================================================================================


def test_eval_retrieval_graph_not_found_exits_1_and_never_calls_run_questions(monkeypatch):
    called = []
    _patch_common(monkeypatch, store_exists=False)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)
    monkeypatch.setattr(
        "codegraph.cli.run_questions", lambda search_fn, qs: called.append(1) or []
    )

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 1
    assert "not found" in result.output.lower()
    assert called == []


def test_eval_retrieval_store_unreachable_exits_1(monkeypatch):
    _patch_common(monkeypatch, store_unreachable=True)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)
    monkeypatch.setattr("codegraph.cli.run_questions", lambda search_fn, qs: [])

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 1
    assert "falkordb" in result.output.lower()


def test_eval_retrieval_questions_file_not_found_exits_1(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.run_questions", lambda search_fn, qs: [])
    missing = tmp_path / "does_not_exist.yaml"

    result = runner.invoke(
        app, ["eval", "retrieval", str(FIXTURES_WS), "--questions", str(missing)]
    )

    assert result.exit_code == 1
    assert str(missing) in result.output or "does_not_exist" in result.output


# ======================================================================================
# -- table rendering + exit 0 regardless of hit-rate ("report, not gate") --
# ======================================================================================


def test_eval_retrieval_reports_hits_and_misses_and_exits_0_even_on_full_miss(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)
    monkeypatch.setattr(
        "codegraph.cli.run_questions",
        lambda search_fn, qs: [
            {
                "question": "q1", "hit": True, "rank": 0,
                "top": [{"symbol_id": "sym:svc:a", "qualified_name": "app.a", "score": 0.9}],
            },
            {
                "question": "q2", "hit": False, "rank": None,
                "top": [{"symbol_id": "sym:svc:x", "qualified_name": "app.x", "score": 0.5}],
            },
        ],
    )

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert "q1" in result.output and "q2" in result.output
    assert "app.a" in result.output  # top-1 shown for the hit
    assert "app.x" in result.output  # top-1 shown for the miss too (diagnostic)
    assert "1/2" in result.output or "1 / 2" in result.output


def test_eval_retrieval_no_results_top1_shows_placeholder_not_a_crash(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS[:1])
    monkeypatch.setattr(
        "codegraph.cli.run_questions",
        lambda search_fn, qs: [{"question": "q1", "hit": False, "rank": None, "top": []}],
    )

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert "q1" in result.output


# ======================================================================================
# -- embedder degradation: CodegraphError -> warn, proceed (still exit 0) --
# ======================================================================================


def test_eval_retrieval_embedder_construction_failure_warns_and_still_exits_0(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS)
    monkeypatch.setattr("codegraph.cli.run_questions", lambda search_fn, qs: [])

    def _raise(cfg):
        raise CodegraphError("uv sync --extra local-emb")

    monkeypatch.setattr("codegraph.cli.make_embedder", _raise)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert "uv sync --extra local-emb" in result.output


# ======================================================================================
# -- search_fn wiring: GraphQuery.search_code called with mode="hybrid" --
# ======================================================================================


def test_eval_retrieval_search_fn_calls_graphquery_search_code_hybrid_mode(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS[:1])
    captured: dict = {}

    def fake_run_questions(search_fn, questions):
        captured["result"] = search_fn(questions[0]["question"], questions[0]["k"])
        return []

    monkeypatch.setattr("codegraph.cli.run_questions", fake_run_questions)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert captured["result"]["mode_used"] == "hybrid"
    assert captured["result"]["_query"] == "q1"
    assert captured["result"]["_k"] == 3
    assert len(_FakeGraphQuery.instances) == 1
    assert set(_FakeGraphQuery.instances[0].service_paths) == {
        "orders-api", "kyc-worker", "document-management",
    }


# ======================================================================================
# -- M5 T2 (pilot Bug A): --exact flag reaches GraphQuery.search_code(exact=...) --
# ======================================================================================


def test_eval_retrieval_exact_flag_passes_true_to_search_code(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS[:1])
    captured: dict = {}

    def fake_run_questions(search_fn, questions):
        captured["result"] = search_fn(questions[0]["question"], questions[0]["k"])
        return []

    monkeypatch.setattr("codegraph.cli.run_questions", fake_run_questions)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS), "--exact"])

    assert result.exit_code == 0, result.output
    assert captured["result"]["_exact"] is True
    assert captured["result"]["mode_used"] == "hybrid"  # --exact doesn't change mode


def test_eval_retrieval_without_exact_flag_defaults_to_false(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr("codegraph.cli.load_questions", lambda path: _QUESTIONS[:1])
    captured: dict = {}

    def fake_run_questions(search_fn, questions):
        captured["result"] = search_fn(questions[0]["question"], questions[0]["k"])
        return []

    monkeypatch.setattr("codegraph.cli.run_questions", fake_run_questions)

    result = runner.invoke(app, ["eval", "retrieval", str(FIXTURES_WS)])

    assert result.exit_code == 0, result.output
    assert captured["result"]["_exact"] is False
