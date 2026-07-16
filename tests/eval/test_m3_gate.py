"""M3 gate: the REAL pipeline (analyze_service x3, runner=None -> real scip-python;
link_workspace; S8 chunk_embed with the REAL LocalEmbedder jina-embeddings-v2-base-
code, not FakeEmbedder) -> load into a live FalkorDB -> all 6 golden questions
(fixtures/golden/questions.yaml -- 5 Russian NL + 1 fulltext-coverage question, see
that file's own header) hit@3 via GraphQuery.search_code(mode="hybrid") --
the M3 milestone retrieval gate. Mirrors M2's tests/eval/test_m2_gate.py conventions
(module docstring, marker set, `shutil.which("npx")` skip, tmp_path staging,
print-then-assert diagnostics collecting every finding into one `problems` list
rather than failing at the first miss) and adds:

  (a) an EXTRA runtime skip beyond the `emb` marker itself: `-m 'not emb'` (default
      addopts) already DESELECTS this whole module without ever importing
      sentence_transformers, but an explicit `-m emb` run (or `-m "scip and falkordb
      and emb"`, this gate's own invocation) still needs the `local-emb` extra
      actually INSTALLED to construct a real embedder at all -- `find_spec` +
      (defensively) a `CodegraphError` catch around `make_embedder` both degrade to
      `pytest.skip` with an actionable reason, never a hard failure, per the T8
      brief ("ЕСЛИ модель недоступна (пакет) — тест SKIP с внятной причиной").
  (b) a cache-gate: a SECOND `chunk_embed.run` over the SAME (unchanged) staging must
      find embedded==0 / reused==chunks_total -- the M3 counterpart to M1/M2's own
      re-run idempotency proofs (see e.g. test_calls_gate.py's cache assertions).
  (c) a live contract check that vector-mode `search_code` denormalizes a real
      `qualified_name` (never `None`) for a symbolic chunk on an ACTUALLY loaded
      graph -- the T7 review fix (`pipeline/load.py`'s `_chunk_props` qualified_names
      join), exercised end to end here rather than only against the hand-built
      mini-graphs `tests/integration/test_retrieval_live.py` uses.

ANTI-CURVE-FIT (binding contract -- do not violate on a future edit): every accept
list in fixtures/golden/questions.yaml was verified against ACTUAL fixture code and
the real T3/T4 chunking pipeline BEFORE ever looking at model output (see that
file's own header comment for the two corrections this required and the one
documented question rephrase); a failing question gets AT MOST one documented human
rephrase, never a widened accept list to match whatever the model's actual top-k
happened to return. Gate is NOT weakened on failure -- see m3-task-8-report.md
"Concerns" if any part of this doesn't pass for real.
"""

from __future__ import annotations

import shutil
from importlib.util import find_spec
from pathlib import Path

import pytest

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.config.models import WorkspaceConfig
from codegraph.core.errors import CodegraphError
from codegraph.embedding.factory import make_embedder
from codegraph.evalx.retrieval_eval import load_questions, run_questions
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.chunk_embed import run as run_chunk_embed
from codegraph.pipeline.load import load_graph
from codegraph.query.api import GraphQuery
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = [pytest.mark.scip, pytest.mark.falkordb, pytest.mark.emb]

FIXTURES = Path(__file__).parents[2] / "fixtures"
GOLDEN_QUESTIONS = FIXTURES / "golden" / "questions.yaml"
GRAPH_NAME = "__m3_gate__"
HIT_K = 3
EXPECTED_QUESTION_COUNT = 6

# -- vector-mode contract probe (item c above): a query guaranteed to land at least
# one orders-api chunk in the top-5 nearest neighbours regardless of semantic
# quality (FalkorDB's vector index always returns its k nearest, unconditionally --
# see store.search_vector_chunks' own docstring) -- this check is about the
# qualified_name JOIN being wired, not about retrieval quality (that's what the 5
# golden questions below are for). --
VECTOR_PROBE_QUERY = "OrderService"
VECTOR_PROBE_SERVICE = "orders-api"


def _run_pipeline(cfg: WorkspaceConfig, staging: Staging, cache_dir: Path) -> None:
    """Identical shape to test_m2_gate.py's own `_run_pipeline` -- kept as its own
    private copy here rather than imported (tests/eval isn't a shared library
    module; each milestone gate owns its exact pipeline invocation independently,
    same minimal-blast-radius principle as tests/eval/conftest.py's duplicated
    `falkordb_cfg` fixture, see that file's own docstring)."""
    active_idioms = frozenset(cfg.builtin_idioms)
    for svc in cfg.services:
        report = analyze_service(
            svc, staging, cache_dir, runner=None,
            active_idioms=active_idioms, idioms=effective_idioms(cfg, svc),
        )
        assert not report["degraded"], (
            f"real scip expected for all fixture services, got degraded "
            f"{svc.name!r}: {report['reason']}"
        )
    link_workspace(cfg, staging)


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
@pytest.mark.skipif(
    find_spec("sentence_transformers") is None,
    reason="sentence-transformers not installed (uv sync --extra local-emb)",
)
def test_m3_gate(tmp_path, falkordb_cfg):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    cache_dir = tmp_path / "scip-cache"
    staging_path = tmp_path / "staging.db"

    # Belt-and-suspenders beyond the module-level skipif above: covers a
    # transformers-version incompatibility / HF-Hub network hiccup / bad model name
    # at LOAD time, not just "package not installed" (see LocalEmbedder's own "VERSION
    # COMPATIBILITY" docstring for exactly this class of failure) -- same
    # "model unavailable -> SKIP with a clear reason" contract the brief asks for,
    # just covering the other half of "unavailable".
    try:
        embedder = make_embedder(cfg.embedding)
    except CodegraphError as e:
        pytest.skip(f"real local embedder unavailable: {e}")

    questions = load_questions(GOLDEN_QUESTIONS)
    assert len(questions) == EXPECTED_QUESTION_COUNT, (
        f"expected {EXPECTED_QUESTION_COUNT} golden questions in {GOLDEN_QUESTIONS}, "
        f"got {len(questions)}"
    )
    assert all(q["k"] == HIT_K for q in questions), (
        f"every golden question must use this gate's own HIT_K={HIT_K} (every "
        f"diagnostic message below prints 'hit@{HIT_K}' verbatim, regardless of a "
        f"question's own k) -- got {[q['k'] for q in questions]}"
    )

    problems: list[str] = []
    staging = Staging(staging_path)
    store = FalkorStore(falkordb_cfg, GRAPH_NAME)
    build_store = FalkorStore(falkordb_cfg, f"{GRAPH_NAME}__build")
    try:
        _run_pipeline(cfg, staging, cache_dir)

        # -- S8, 1st run: real embedder, nothing cached yet --
        chunk_report = run_chunk_embed(cfg, staging, embedder)
        print(f"\n[M3 gate][chunk_embed 1st run] {chunk_report}")
        if chunk_report["chunks_total"] == 0:
            problems.append(f"no chunks staged at all -- broken fixture/pipeline? {chunk_report}")
        if chunk_report["embedded"] != chunk_report["chunks_total"]:
            problems.append(
                f"1st chunk_embed run should embed EVERY chunk (nothing cached yet): "
                f"{chunk_report}"
            )

        load_stats = load_graph(staging, lambda name: FalkorStore(falkordb_cfg, name), GRAPH_NAME)
        print(f"\n[M3 gate][load_stats] {load_stats}")

        gq = GraphQuery(
            store_factory=lambda: FalkorStore(falkordb_cfg, GRAPH_NAME),
            service_paths={svc.name: svc.path for svc in cfg.services},
            embedder_factory=lambda: embedder,
        )

        # -- (c) contract check: vector-mode search_code carries a real qualified_name
        # for a symbolic chunk, live (not a hand-built mini-graph) --
        vector_probe = gq.search_code(
            VECTOR_PROBE_QUERY, k=5, mode="vector", service=VECTOR_PROBE_SERVICE
        )
        if "error" in vector_probe:
            problems.append(
                f"vector-mode search_code errored on a freshly loaded graph: {vector_probe}"
            )
        else:
            probe_items = vector_probe["items"]
            if not probe_items:
                problems.append(
                    f"vector-mode search_code returned zero items for service="
                    f"{VECTOR_PROBE_SERVICE!r} on a freshly loaded, fully-embedded graph"
                )
            none_qualified = [i for i in probe_items if i["qualified_name"] is None]
            if none_qualified:
                problems.append(
                    "vector-mode search_code returned item(s) with qualified_name=None "
                    f"on a freshly loaded graph (T7 load-time join not wired?): {none_qualified}"
                )

        # -- (main gate) 6 golden questions, hit@3, via hybrid search --
        results = run_questions(lambda q, k: gq.search_code(q, k=k, mode="hybrid"), questions)
        by_question = {q["question"]: q for q in questions}
        hits = 0
        for r in results:
            hits += 1 if r["hit"] else 0
            print(
                f"\n[M3 gate][question] {r['question']!r} hit={r['hit']} rank={r['rank']}"
            )
            for i, item in enumerate(r["top"]):
                print(f"    #{i} {item}")
            if not r["hit"]:
                accept = by_question[r["question"]]["accept"]
                problems.append(
                    f"MISS hit@{HIT_K}: {r['question']!r}\n"
                    f"  expected one of (service, symbol) in: {accept}\n"
                    f"  got top-{HIT_K}: {r['top']}"
                )
        print(f"\n[M3 gate] hit@{HIT_K}: {hits}/{len(results)}")

        # -- cache-gate: 2nd chunk_embed.run over the SAME staging, nothing on disk
        # changed -- must embed nothing, everything reused --
        chunk_report_2 = run_chunk_embed(cfg, staging, embedder)
        print(f"\n[M3 gate][chunk_embed 2nd run] {chunk_report_2}")
        if chunk_report_2["embedded"] != 0:
            problems.append(
                f"cache gate: 2nd chunk_embed.run embedded={chunk_report_2['embedded']} "
                f"(want 0 -- every chunk should be reused via the content_hash-keyed "
                f"cache): {chunk_report_2}"
            )
        if chunk_report_2["reused"] != chunk_report_2["chunks_total"]:
            problems.append(
                f"cache gate: 2nd run reused={chunk_report_2['reused']} != "
                f"chunks_total={chunk_report_2['chunks_total']}: {chunk_report_2}"
            )
    finally:
        staging.close()
        store.delete_graph()
        build_store.delete_graph()

    assert not problems, "\n\n".join(problems)
