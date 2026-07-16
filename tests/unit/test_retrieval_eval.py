"""Юниты для codegraph.evalx.retrieval_eval: load_questions (YAML parsing) и
run_questions (hit@k over a fake search_fn — no graph/embedder/model needed). Real
E2E gate with the real jina-code model + a live FalkorDB graph lives in
tests/eval/test_m3_gate.py (markers scip+falkordb+emb)."""

from __future__ import annotations

from codegraph.evalx.retrieval_eval import load_questions, run_questions

# -- load_questions -----------------------------------------------------------

QUESTIONS_YAML = """
version: 1
questions:
  - question: "where is the order created"
    accept:
      - {service: orders-api, symbol: app.services.order.OrderService}
      - {service: orders-api, symbol: app.routes.orders.create_order}
    k: 3
  - question: "who handles OrderCreated"
    accept:
      - {service: kyc-worker, symbol: app.consumers.orders.handle_order_created}
    k: 5
"""


def test_load_questions_parses_question_accept_k(tmp_path):
    path = tmp_path / "questions.yaml"
    path.write_text(QUESTIONS_YAML)

    questions = load_questions(path)

    assert len(questions) == 2
    assert questions[0]["question"] == "where is the order created"
    assert questions[0]["k"] == 3
    assert questions[0]["accept"] == [
        {"service": "orders-api", "symbol": "app.services.order.OrderService"},
        {"service": "orders-api", "symbol": "app.routes.orders.create_order"},
    ]
    assert questions[1]["k"] == 5


def test_load_questions_empty_file_returns_empty_list(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text("")
    assert load_questions(path) == []


# -- run_questions --------------------------------------------------------------


def _item(symbol_id: str, qualified_name: str | None, service: str, score: float) -> dict:
    return {
        "chunk_id": f"{symbol_id}#c0", "symbol_id": symbol_id,
        "qualified_name": qualified_name, "service": service,
        "relpath": "mod.py", "start_line": 1, "end_line": 2,
        "snippet": "...", "score": score,
    }


def _search_fn(items_by_query: dict[str, list[dict]]):
    """Fake search_fn(query, k) -> {"items": [...]} -- the same shape
    GraphQuery.search_code returns. Ignores k itself (run_questions is responsible
    for capping to k, see the defensive-slice test below), records nothing --
    tests assert purely on run_questions' OWN output."""

    def fn(query: str, k: int) -> dict:
        return {"items": items_by_query.get(query, []), "mode_used": "hybrid"}

    return fn


def test_run_questions_hit_at_rank_zero():
    q = [{
        "question": "q1", "k": 3,
        "accept": [{"service": "svc", "symbol": "app.a"}],
    }]
    search_fn = _search_fn({
        "q1": [_item("sym:svc:a", "app.a", "svc", 0.9), _item("sym:svc:b", "app.b", "svc", 0.5)],
    })

    results = run_questions(search_fn, q)

    assert len(results) == 1
    assert results[0]["question"] == "q1"
    assert results[0]["hit"] is True
    assert results[0]["rank"] == 0
    assert results[0]["top"][0] == {
        "symbol_id": "sym:svc:a", "qualified_name": "app.a", "score": 0.9,
    }


def test_run_questions_hit_at_later_rank_records_correct_rank():
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.c"}]}]
    search_fn = _search_fn({
        "q1": [
            _item("sym:svc:a", "app.a", "svc", 0.9),
            _item("sym:svc:b", "app.b", "svc", 0.8),
            _item("sym:svc:c", "app.c", "svc", 0.7),
        ],
    })

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is True
    assert results[0]["rank"] == 2


def test_run_questions_miss_when_accept_absent_from_top_k():
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.nope"}]}]
    search_fn = _search_fn({
        "q1": [_item("sym:svc:a", "app.a", "svc", 0.9), _item("sym:svc:b", "app.b", "svc", 0.5)],
    })

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is False
    assert results[0]["rank"] is None
    # top still reports what WAS actually returned, for diagnostics on a miss.
    assert [t["qualified_name"] for t in results[0]["top"]] == ["app.a", "app.b"]


def test_run_questions_or_semantics_across_multiple_accept_entries():
    # Neither the first nor the (missing) second accept symbol is top-1, but the
    # SECOND accept entry matches the top item that's actually there -- any() match.
    q = [{
        "question": "q1", "k": 3,
        "accept": [
            {"service": "svc", "symbol": "app.not-present"},
            {"service": "svc", "symbol": "app.b"},
        ],
    }]
    search_fn = _search_fn({"q1": [_item("sym:svc:b", "app.b", "svc", 0.9)]})

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is True
    assert results[0]["rank"] == 0


def test_run_questions_service_must_also_match_not_just_qualified_name():
    # Same qualified_name, WRONG service -- must not count as a hit (accept is
    # keyed on (service, qualified_name), not qualified_name alone).
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc-a", "symbol": "app.x"}]}]
    search_fn = _search_fn({"q1": [_item("sym:svc-b:x", "app.x", "svc-b", 0.9)]})

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is False
    assert results[0]["rank"] is None


def test_run_questions_none_qualified_name_never_hits_but_symbol_id_preserved_in_top():
    # A chunk whose owning symbol wasn't in staged nodes (pre-T7 graph / defensive
    # edge case, see query.retrieval._chunk_item) -- qualified_name None can never
    # match an accept entry (accept always names a real qualified_name), but the
    # diagnostic `top` list must still show symbol_id so a human reading a miss can
    # tell what was actually retrieved.
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.a"}]}]
    search_fn = _search_fn({"q1": [_item("sym:svc:ghost", None, "svc", 0.9)]})

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is False
    assert results[0]["rank"] is None
    assert results[0]["top"] == [
        {"symbol_id": "sym:svc:ghost", "qualified_name": None, "score": 0.9}
    ]


def test_run_questions_defensively_caps_top_to_k_even_if_search_fn_overreturns():
    q = [{"question": "q1", "k": 2, "accept": [{"service": "svc", "symbol": "app.c"}]}]
    search_fn = _search_fn({
        "q1": [
            _item("sym:svc:a", "app.a", "svc", 0.9),
            _item("sym:svc:b", "app.b", "svc", 0.8),
            _item("sym:svc:c", "app.c", "svc", 0.7),  # rank 2 -- outside k=2
        ],
    })

    results = run_questions(search_fn, q)

    assert len(results[0]["top"]) == 2
    assert results[0]["hit"] is False  # app.c exists but was cut by the k=2 cap
    assert results[0]["rank"] is None


def test_run_questions_missing_items_key_treated_as_empty_not_a_crash():
    # e.g. an {"error": ...} dict from search_code (mode="vector" with no usable
    # embedder) -- run_questions must degrade to hit=False, not raise.
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.a"}]}]
    search_fn = lambda query, k: {"error": "no embedder available for vector search"}  # noqa: E731

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is False
    assert results[0]["rank"] is None
    assert results[0]["top"] == []


def test_run_questions_empty_top_k_list():
    q = [{"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.a"}]}]
    search_fn = _search_fn({"q1": []})

    results = run_questions(search_fn, q)

    assert results[0]["hit"] is False
    assert results[0]["rank"] is None
    assert results[0]["top"] == []


def test_run_questions_multiple_questions_independent_and_in_order():
    q = [
        {"question": "q1", "k": 3, "accept": [{"service": "svc", "symbol": "app.a"}]},
        {"question": "q2", "k": 3, "accept": [{"service": "svc", "symbol": "app.zzz"}]},
    ]
    search_fn = _search_fn({
        "q1": [_item("sym:svc:a", "app.a", "svc", 0.9)],
        "q2": [_item("sym:svc:b", "app.b", "svc", 0.9)],
    })

    results = run_questions(search_fn, q)

    assert [r["question"] for r in results] == ["q1", "q2"]
    assert results[0]["hit"] is True
    assert results[1]["hit"] is False


def test_run_questions_passes_per_question_k_to_search_fn():
    seen: list[tuple[str, int]] = []

    def search_fn(query: str, k: int) -> dict:
        seen.append((query, k))
        return {"items": []}

    q = [
        {"question": "q1", "k": 3, "accept": []},
        {"question": "q2", "k": 7, "accept": []},
    ]
    run_questions(search_fn, q)

    assert seen == [("q1", 3), ("q2", 7)]
