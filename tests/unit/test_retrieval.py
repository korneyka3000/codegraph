"""Юниты query.retrieval (M3 T7, Step 1 брифа): rrf (математика/tie-break/пустые
списки) + search_code/find_entrypoint ветки (mode/деградация/Meta-мисматч/snippet
truncation) на fake store + fake embedder -- без живого FalkorDB (тот сценарий --
tests/integration/test_retrieval_live.py и test_mcp_contract.py, marker falkordb).
"""

from __future__ import annotations

import pytest

from codegraph.embedding.fake import FakeEmbedder
from codegraph.query import retrieval


class _FakeStore:
    """Duck-typed GraphStore, только методы retrieval.py реально вызывает: get_nodes
    (Meta-lookup + find_entrypoint's missing-id backfill), search_text_chunks,
    search_vector_chunks, search_vector_chunks_exact (M5 T2 -- exact=True routes the
    vector leg through this method instead), search_fulltext. Возвращённые списки уже
    в "best-first" порядке -- ровно то, что реальный FalkorStore гарантирует (ORDER BY
    score DESC/ASC, см. store.py); retrieval.py доверяет этому порядку, а не сырым score."""

    def __init__(self) -> None:
        self.meta: dict | None = None
        self.nodes_by_id: dict[str, dict] = {}
        self.text_chunks: list[tuple[dict, float]] = []
        self.vector_chunks: list[tuple[dict, float]] = []
        self.vector_exact_chunks: list[tuple[dict, float]] = []
        self.sym_fulltext: list[dict] = []
        self.text_calls: list[tuple] = []
        self.vector_calls: list[tuple] = []
        self.vector_exact_calls: list[tuple] = []
        self.fulltext_calls: list[tuple] = []
        self.get_nodes_calls: list[list[str]] = []

    def get_nodes(self, ids):
        self.get_nodes_calls.append(list(ids))
        out = []
        for i in ids:
            if i == "meta" and self.meta is not None:
                out.append(self.meta)
            elif i in self.nodes_by_id:
                out.append(self.nodes_by_id[i])
        return out

    def search_text_chunks(self, query, k, service=None):
        self.text_calls.append((query, k, service))
        return self.text_chunks[:k]

    def search_vector_chunks(self, vec, k, service=None):
        self.vector_calls.append((vec, k, service))
        return self.vector_chunks[:k]

    def search_vector_chunks_exact(self, vec, k, service=None):
        self.vector_exact_calls.append((vec, k, service))
        return self.vector_exact_chunks[:k]

    def search_fulltext(self, query, k, kinds=None):
        self.fulltext_calls.append((query, k, kinds))
        return self.sym_fulltext[:k]


class _PoisonEmbedder:
    """embed_query/embed_batch raise -- proves a code path never touches the
    embedder at all (rather than merely "happens not to care about its output")."""

    model_id = "poison"
    dim = 4

    def embed_batch(self, texts):
        raise AssertionError("embed_batch should not have been called")

    def embed_query(self, text):
        raise AssertionError("embed_query should not have been called")


class _QueryOnlyEmbedder:
    """M5 T6: unlike `FakeEmbedder` (whose `embed_query(t)`/`embed_batch([t])[0]`
    happen to produce IDENTICAL output for the same text, by construction -- see its
    own module docstring), this fake makes the two methods OBSERVABLY different:
    `embed_batch` raises, `embed_query` records what it was called with. A test
    built only on `FakeEmbedder` output can't tell whether retrieval.py's query leg
    actually calls `embed_query` or falls back to `embed_batch([query])[0]` --
    THIS class can, which is the whole point of pinning "the query leg uses
    embed_query" as its own regression guard rather than trusting it stays true by
    accident."""

    model_id = "fake-4d"
    dim = 4

    def __init__(self) -> None:
        self.embed_query_calls: list[str] = []

    def embed_query(self, text):
        self.embed_query_calls.append(text)
        return [0.1, 0.2, 0.3, 0.4]

    def embed_batch(self, texts):
        raise AssertionError(
            "retrieval.py's query leg must call embed_query, never embed_batch"
        )


def _chunk(chunk_id: str, **extra) -> dict:
    return {
        "id": chunk_id, "kind": "Chunk", "symbol_id": extra.pop("symbol_id", f"sym:{chunk_id}"),
        "service": "svc", "relpath": "mod.py", "start_line": 1, "end_line": 2,
        "text": extra.pop("text", "def f(): pass"), **extra,
    }


# -- rrf: математика/tie-break/пустые списки --


def test_rrf_empty_rankings_returns_empty_list():
    assert retrieval.rrf([]) == []


def test_rrf_list_of_empty_rankings_returns_empty_list():
    assert retrieval.rrf([[], []]) == []


def test_rrf_single_ranking_preserves_order_descending_score():
    result = retrieval.rrf([["a", "b", "c"]])
    assert [item_id for item_id, _score in result] == ["a", "b", "c"]
    scores = [score for _id, score in result]
    assert scores == sorted(scores, reverse=True)


def test_rrf_document_in_both_lists_beats_rank0_of_a_single_list():
    # a: rank0 in list1 only. c: rank0 in list2 only. b: rank1 in BOTH lists.
    # score(b) = 2/(60+1+1) = 2/62 ~= 0.03226; score(a) = score(c) = 1/(60+0+1) = 1/61
    # ~= 0.01639. b must win despite never being rank0 anywhere -- the textbook RRF
    # "present everywhere beats first-place-once" result (brief's own "документ в
    # обоих списках выигрывает" sanity, replicated as pure math here too).
    # (Reviewer minor fix: rankings were [["a","b"],["b","c"]], which didn't match
    # this comment's math -- there b was rank0 in list2 and c rank1; the corrected
    # data below is the scenario the comment/title always described.)
    result = retrieval.rrf([["a", "b"], ["c", "b"]])
    assert result[0][0] == "b"
    assert result[0][1] > result[1][1]
    # And the exact worked numbers hold: b = 2/62, a = c = 1/61 (tie, id-ordered).
    scores = dict(result)
    assert scores["b"] == pytest.approx(2 / 62)
    assert scores["a"] == scores["c"] == pytest.approx(1 / 61)


def test_rrf_tie_break_by_id_ascending_for_equal_scores():
    # x and y: each rank0 in exactly one, disjoint, single-element ranking -- exactly
    # equal scores (1/(60+0+1) each) by construction; only id ordering can break the
    # tie, and it must be deterministic (not dict/hash-order dependent).
    result = retrieval.rrf([["zzz"], ["aaa"]])
    assert [item_id for item_id, _score in result] == ["aaa", "zzz"]


def test_rrf_custom_k_still_orders_by_rank():
    result = retrieval.rrf([["a", "b", "c"]], k=1)
    assert [item_id for item_id, _score in result] == ["a", "b", "c"]


def test_rrf_id_absent_from_a_ranking_contributes_nothing_from_it():
    # "x" only in the first (of two) rankings -- its score is the SAME whether or not
    # a second, x-free ranking exists at all (no phantom worst-rank contribution).
    solo = retrieval.rrf([["x"]])
    with_extra = retrieval.rrf([["x"], ["a", "b", "c"]])
    assert dict(solo)["x"] == dict(with_extra)["x"]


# -- search_code: mode="text" (always available, never touches embedder) --


def test_search_code_text_mode_returns_items_with_rrf_score_and_mode_used():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 5.0), (_chunk("c2"), 1.0)]
    result = retrieval.search_code(store, None, "create order", mode="text")
    assert result["mode_used"] == "text"
    assert [i["chunk_id"] for i in result["items"]] == ["c1", "c2"]
    assert result["items"][0]["score"] > result["items"][1]["score"]  # RRF, desc


def test_search_code_text_mode_never_touches_embedder_even_if_one_is_given():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 5.0)]
    result = retrieval.search_code(store, _PoisonEmbedder(), "q", mode="text")
    assert result["mode_used"] == "text"
    assert store.vector_calls == []


def test_search_code_text_mode_passes_k_and_service_through():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]
    retrieval.search_code(store, None, "q", k=3, service="svc-a", mode="text")
    assert store.text_calls == [("q", 3, "svc-a")]


def test_search_code_snippet_truncated_to_600_chars():
    store = _FakeStore()
    long_text = "x" * 1000
    store.text_chunks = [(_chunk("c1", text=long_text), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert len(result["items"][0]["snippet"]) == 600
    assert result["items"][0]["snippet"] == "x" * 600


def test_search_code_snippet_short_text_left_untouched():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1", text="short"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["snippet"] == "short"


def test_search_code_item_shape_has_exactly_the_contract_fields():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert set(result["items"][0]) == {
        "chunk_id", "symbol_id", "qualified_name", "enclosing_symbol", "chunk_kind",
        "service", "relpath", "start_line", "end_line", "snippet", "score",
    }


def test_search_code_item_carries_qualified_name_from_chunk_props():
    # qualified_name is denormalized onto the Chunk node at load time
    # (pipeline/load._chunk_props) -- retrieval just reads the property through.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1", qualified_name="app.orders.create_order"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["qualified_name"] == "app.orders.create_order"


def test_search_code_item_qualified_name_none_when_chunk_lacks_the_property():
    # Pre-T7-loaded graph / symbol absent from staged nodes: property missing on the
    # Chunk node -> honest None in the item, not a KeyError.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]  # _chunk() sets no qualified_name
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["qualified_name"] is None


# -- M10 T3 (pilot §4.1): enclosing_symbol/chunk_kind -- a search hit self-describes
# WHICH symbol (method vs. class) it covers, closing the "right class, wrong
# sub-chunk" gap the pilot's §4.1/§4.2 misses trace back to (big classes split into
# N sibling chunks sharing one qualified_name for their header/gap pieces --
# chunking/splitter.py rule 3 -- while method-level pieces already carry the
# METHOD's own qualified_name; nothing on the item made that distinction explicit).


def test_search_code_item_enclosing_symbol_mirrors_qualified_name():
    # enclosing_symbol is the SAME value as qualified_name (both are "the owning
    # node's qualified_name", already denormalized onto the Chunk node at load time
    # -- see pipeline/load._chunk_props) -- an explicit, unambiguously-named
    # additive field rather than a second store lookup or a rename of the existing
    # (back-compat) qualified_name key.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1", qualified_name="app.orders.create_order"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["enclosing_symbol"] == "app.orders.create_order"
    assert result["items"][0]["enclosing_symbol"] == result["items"][0]["qualified_name"]


def test_search_code_item_enclosing_symbol_none_when_chunk_lacks_qualified_name():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]  # _chunk() sets no qualified_name
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["enclosing_symbol"] is None


def test_search_code_item_carries_chunk_kind_from_chunk_props():
    # chunk_kind is denormalized onto the Chunk node at load time (pipeline/
    # load._chunk_props, M10 T3, mirroring the qualified_name join) -- the owning
    # node's kind ("Module"/"Class"/"Function"), NOT the chunk's own "kind": "Chunk".
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1", chunk_kind="Function"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["chunk_kind"] == "Function"


def test_search_code_item_carries_chunk_kind_module_for_module_level_chunk():
    # M10 T3 backlog pin: "Module" is the third real chunk_kind value alongside
    # "Class"/"Function" (pipeline/load.py's own _CODE_KINDS = {"Module", "Class",
    # "Function"}) -- a chunk covering module-level content (imports/top-level
    # code, chunking/splitter.py's own module-header chunk) whose owning node is
    # the Module node itself. retrieval._chunk_item is a raw prop pass-through
    # (no special-casing of specific kind strings), so this end-to-end-at-the-
    # retrieval-unit-level pin confirms "Module" flows through identically to the
    # already-pinned "Class"/"Function" values.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1", chunk_kind="Module"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["chunk_kind"] == "Module"


def test_search_code_item_chunk_kind_none_when_chunk_lacks_the_property():
    # Pre-T3-loaded graph / symbol absent from staged nodes: honest None, not a
    # KeyError -- same defensive convention as qualified_name.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]  # _chunk() sets no chunk_kind
    result = retrieval.search_code(store, None, "q", mode="text")
    assert result["items"][0]["chunk_kind"] is None


def test_search_code_item_chunk_kind_distinguishes_method_from_class_level_chunk():
    # The concrete pilot scenario (§4.1): a big class split into a class-level
    # header/gap chunk (chunk_kind="Class", qualified_name is the bare class name)
    # and a method-level chunk (chunk_kind="Function", qualified_name is
    # "ClassName.method_name") are now distinguishable without guessing from
    # qualified_name's shape alone.
    store = _FakeStore()
    store.text_chunks = [
        (_chunk("class#c0", qualified_name="DocumentStorageClient", chunk_kind="Class"), 2.0),
        (
            _chunk(
                "method#c0",
                qualified_name="DocumentStorageClient.upload_file_for_document",
                chunk_kind="Function",
            ),
            1.0,
        ),
    ]
    result = retrieval.search_code(store, None, "q", mode="text")
    by_id = {i["chunk_id"]: i for i in result["items"]}
    assert by_id["class#c0"]["chunk_kind"] == "Class"
    assert by_id["method#c0"]["chunk_kind"] == "Function"
    assert (
        by_id["method#c0"]["enclosing_symbol"]
        == "DocumentStorageClient.upload_file_for_document"
    )


def test_search_code_invalid_mode_returns_error_dict():
    store = _FakeStore()
    result = retrieval.search_code(store, None, "q", mode="sideways")
    assert "error" in result
    assert "sideways" in result["error"]


# -- search_code: mode="vector" --


def _meta(model_id: str) -> dict:
    return {"id": "meta", "kind": "Meta", "embed_model": model_id, "schema_version": 4}


def test_search_code_vector_mode_no_embedder_returns_error_dict():
    store = _FakeStore()
    result = retrieval.search_code(store, None, "q", mode="vector")
    assert "error" in result
    assert "no embedder" in result["error"].lower()
    assert store.vector_calls == []


def test_search_code_vector_mode_meta_mismatch_returns_error_dict():
    store = _FakeStore()
    store.meta = _meta("real-model")
    embedder = FakeEmbedder(dim=4, model_id="other-model")
    result = retrieval.search_code(store, embedder, "q", mode="vector")
    assert "error" in result
    assert "reindex" in result["error"].lower()
    assert store.vector_calls == []  # rejected before ever querying the vector index


def test_search_code_vector_mode_no_meta_node_at_all_is_a_mismatch():
    store = _FakeStore()  # store.meta is None -- graph never went through M3 load_graph
    embedder = FakeEmbedder(dim=4, model_id="whatever")
    result = retrieval.search_code(store, embedder, "q", mode="vector")
    assert "error" in result


def test_search_code_vector_mode_happy_path_never_touches_text_search():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.vector_chunks = [(_chunk("c1"), 0.01), (_chunk("c2"), 0.5)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.search_code(store, embedder, "q", mode="vector")
    assert result["mode_used"] == "vector"
    assert [i["chunk_id"] for i in result["items"]] == ["c1", "c2"]
    assert store.text_calls == []


def test_search_code_vector_leg_uses_embed_query_not_embed_batch():
    # M5 T6: pins the query-side vector leg to embedder.embed_query specifically
    # (see _QueryOnlyEmbedder's own docstring for why FakeEmbedder alone can't
    # distinguish this from an embed_batch([query])[0] fallback).
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.vector_chunks = [(_chunk("c1"), 0.1)]
    embedder = _QueryOnlyEmbedder()
    result = retrieval.search_code(store, embedder, "где создаётся инцидент", mode="vector")
    assert result["mode_used"] == "vector"
    assert embedder.embed_query_calls == ["где создаётся инцидент"]


# -- search_code: mode="hybrid" --


def test_search_code_hybrid_no_embedder_degrades_to_text_silently():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="hybrid")
    assert result["mode_used"] == "text"
    assert [i["chunk_id"] for i in result["items"]] == ["c1"]


def test_search_code_hybrid_meta_mismatch_degrades_to_text_silently_not_an_error():
    store = _FakeStore()
    store.meta = _meta("real-model")
    store.text_chunks = [(_chunk("c1"), 1.0)]
    embedder = FakeEmbedder(dim=4, model_id="other-model")
    result = retrieval.search_code(store, embedder, "q", mode="hybrid")
    assert "error" not in result
    assert result["mode_used"] == "text"


def test_search_code_hybrid_fuses_both_rankings_document_in_both_wins():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    # text ranking: a (rank0), c (rank1) -- b absent
    store.text_chunks = [(_chunk("a"), 9.0), (_chunk("c"), 1.0)]
    # vector ranking: b (rank0), c (rank1) -- a absent
    store.vector_chunks = [(_chunk("b"), 0.01), (_chunk("c"), 0.4)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.search_code(store, embedder, "q", k=2, mode="hybrid")
    assert result["mode_used"] == "hybrid"
    # c: rank1 in both (2/62) beats a/b: rank0 in exactly one list (1/61) -- see the
    # identical worked example in the pure-rrf tests above.
    assert result["items"][0]["chunk_id"] == "c"


def test_search_code_hybrid_calls_both_text_and_vector_search():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.text_chunks = [(_chunk("a"), 1.0)]
    store.vector_chunks = [(_chunk("b"), 0.1)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    retrieval.search_code(store, embedder, "q", k=5, service="svc-a", mode="hybrid")
    assert store.text_calls == [("q", 5, "svc-a")]
    assert store.vector_calls == [(embedder.embed_query("q"), 5, "svc-a")]


# -- search_code: exact=True (M5 T2, pilot Bug A) -- vector leg routes through
# search_vector_chunks_exact instead of the ANN search_vector_chunks; RRF fusion
# itself is untouched (rrf() only ever sees a ranking of ids either way).


def test_search_code_vector_mode_exact_true_calls_exact_method_not_ann():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.vector_exact_chunks = [(_chunk("c1"), 0.0), (_chunk("c2"), 1.0)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.search_code(store, embedder, "q", mode="vector", exact=True)
    assert result["mode_used"] == "vector"
    assert [i["chunk_id"] for i in result["items"]] == ["c1", "c2"]
    assert store.vector_calls == []  # ANN method never touched
    assert store.vector_exact_calls == [(embedder.embed_query("q"), 8, None)]


def test_search_code_vector_mode_exact_defaults_to_false_uses_ann_method():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.vector_chunks = [(_chunk("c1"), 0.1)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.search_code(store, embedder, "q", mode="vector")
    assert result["items"][0]["chunk_id"] == "c1"
    assert store.vector_exact_calls == []  # exact method never touched


def test_search_code_hybrid_exact_true_routes_vector_leg_through_exact_method():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.text_chunks = [(_chunk("a"), 9.0)]
    store.vector_exact_chunks = [(_chunk("b"), 0.0)]
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.search_code(store, embedder, "q", k=2, mode="hybrid", exact=True)
    assert result["mode_used"] == "hybrid"
    assert {i["chunk_id"] for i in result["items"]} == {"a", "b"}
    assert store.vector_calls == []
    assert store.vector_exact_calls == [(embedder.embed_query("q"), 2, None)]


def test_search_code_text_mode_ignores_exact_flag_never_touches_any_vector_method():
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]
    result = retrieval.search_code(store, _PoisonEmbedder(), "q", mode="text", exact=True)
    assert result["mode_used"] == "text"
    assert store.vector_calls == []
    assert store.vector_exact_calls == []


def test_search_code_hybrid_no_embedder_exact_true_still_degrades_to_text():
    # _vector_unusable_reason short-circuits before either vector method is ever
    # consulted -- exact=True must not change that degradation.
    store = _FakeStore()
    store.text_chunks = [(_chunk("c1"), 1.0)]
    result = retrieval.search_code(store, None, "q", mode="hybrid", exact=True)
    assert result["mode_used"] == "text"
    assert store.vector_exact_calls == []


# -- find_entrypoint v2 --


def _sym(node_id: str, kind: str = "Function", **extra) -> dict:
    return {"id": node_id, "kind": kind, "name": node_id, **extra}


def test_find_entrypoint_degraded_no_embedder_matches_m2_shape_plus_mode_used():
    store = _FakeStore()
    store.sym_fulltext = [{"id": "sym:a:x", "score": 1.5}]
    result = retrieval.find_entrypoint(store, None, "create order", k=5)
    assert result == {"results": [{"id": "sym:a:x", "score": 1.5}], "mode_used": "text"}
    assert store.fulltext_calls == [("create order", 5, None)]  # plain k, not a pool


def test_find_entrypoint_degraded_passes_kinds_straight_into_fulltext_call():
    store = _FakeStore()
    result = retrieval.find_entrypoint(store, None, "x", k=5, kinds=["Function"])
    assert result["mode_used"] == "text"
    assert store.fulltext_calls[0][2] == ["Function"]


def test_find_entrypoint_meta_mismatch_degrades_to_text_not_an_error():
    store = _FakeStore()
    store.meta = _meta("real-model")
    store.sym_fulltext = [{"id": "sym:a:x", "score": 1.0}]
    embedder = FakeEmbedder(dim=4, model_id="other-model")
    result = retrieval.find_entrypoint(store, embedder, "x")
    assert "error" not in result
    assert result["mode_used"] == "text"


def test_find_entrypoint_hybrid_aggregates_chunk_vector_ranking_to_best_symbol():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = []
    # two chunks belonging to the SAME symbol -- best (rank0) chunk's occurrence must
    # be the one that counts; the worse (rank1) duplicate must not re-add/displace it.
    store.vector_chunks = [
        (_chunk("sym1#c0", symbol_id="sym:a:one"), 0.01),
        (_chunk("sym1#c1", symbol_id="sym:a:one"), 0.2),
        (_chunk("sym2#c0", symbol_id="sym:a:two"), 0.3),
    ]
    store.nodes_by_id = {
        "sym:a:one": _sym("sym:a:one"),
        "sym:a:two": _sym("sym:a:two"),
    }
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.find_entrypoint(store, embedder, "q", k=5)
    assert result["mode_used"] == "hybrid"
    assert [r["id"] for r in result["results"]] == ["sym:a:one", "sym:a:two"]


def test_find_entrypoint_hybrid_fuses_sym_fulltext_and_vector_symbol_ranking():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = [_sym("sym:a", **{"score": 9.0}), _sym("sym:c", **{"score": 1.0})]
    store.vector_chunks = [
        (_chunk("cb", symbol_id="sym:b"), 0.01),
        (_chunk("cc", symbol_id="sym:c"), 0.3),
    ]
    store.nodes_by_id = {"sym:b": _sym("sym:b")}
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.find_entrypoint(store, embedder, "q", k=2)
    assert result["mode_used"] == "hybrid"
    # sym:c is rank1 in BOTH the fulltext and vector rankings -- same "wins by being
    # in both lists" RRF property as search_code's hybrid test above.
    assert result["results"][0]["id"] == "sym:c"


def test_find_entrypoint_hybrid_kinds_filter_applied_after_fusion():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = []
    store.vector_chunks = [
        (_chunk("ca", symbol_id="sym:class-a"), 0.01),  # a Class -- must be filtered out
        (_chunk("cb", symbol_id="sym:fn-b"), 0.2),
    ]
    store.nodes_by_id = {
        "sym:class-a": _sym("sym:class-a", kind="Class"),
        "sym:fn-b": _sym("sym:fn-b", kind="Function"),
    }
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.find_entrypoint(store, embedder, "q", k=5, kinds=["Function"])
    assert [r["id"] for r in result["results"]] == ["sym:fn-b"]


def test_find_entrypoint_hybrid_truncates_to_k_after_fusion():
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = []
    store.vector_chunks = [
        (_chunk(f"c{i}", symbol_id=f"sym:{i}"), float(i)) for i in range(5)
    ]
    store.nodes_by_id = {f"sym:{i}": _sym(f"sym:{i}") for i in range(5)}
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.find_entrypoint(store, embedder, "q", k=2)
    assert len(result["results"]) == 2


def test_find_entrypoint_vector_leg_uses_embed_query_not_embed_batch():
    # M5 T6: same regression guard as search_code's own version above, for
    # find_entrypoint's independent embed_query call site.
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = []
    store.vector_chunks = [(_chunk("c1", symbol_id="sym:a"), 0.1)]
    store.nodes_by_id = {"sym:a": _sym("sym:a")}
    embedder = _QueryOnlyEmbedder()
    result = retrieval.find_entrypoint(store, embedder, "кто вызывает X", k=5)
    assert result["mode_used"] == "hybrid"
    assert embedder.embed_query_calls == ["кто вызывает X"]


def test_find_entrypoint_stale_vector_ranking_id_skipped_not_crashed():
    # symbol_id from a chunk that no longer resolves to any node (deleted symbol,
    # stale staging.db) -- must be silently skipped, not KeyError/crash.
    store = _FakeStore()
    store.meta = _meta("fake-4d")
    store.sym_fulltext = []
    store.vector_chunks = [(_chunk("c1", symbol_id="sym:ghost"), 0.01)]
    store.nodes_by_id = {}  # "sym:ghost" resolves to nothing
    embedder = FakeEmbedder(dim=4, model_id="fake-4d")
    result = retrieval.find_entrypoint(store, embedder, "q", k=5)
    assert result["results"] == []
    assert result["mode_used"] == "hybrid"
