"""Юнит-тесты чистых хелперов FalkorStore (M2 T8): sanitize для fulltext-запроса +
early-return search_fulltext (пустая строка после sanitize -> [] БЕЗ обращения к
FalkorDB) + error-conversion в search_vector_chunks_exact (M5 T2 review: fake graph
вместо сети) -- без сети/живого инстанса (тот сценарий -- tests/integration/
test_falkordb_ddl.py, marker falkordb: реальный индекс + запрос по загруженному
мини-графу)."""

from __future__ import annotations

import pytest
import redis.exceptions

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.connection import StoreError
from codegraph.stores.falkordb.store import (
    FalkorStore,
    _fulltext_or_query,
    _sanitize_fulltext_query,
)


def test_sanitize_replaces_redisearch_special_chars_with_space():
    assert _sanitize_fulltext_query("create@order") == "create order"


def test_sanitize_collapses_whitespace_and_trims():
    assert _sanitize_fulltext_query('  foo   "bar"  ') == "foo bar"


def test_sanitize_hyphen_splits_into_separate_tokens_not_glued():
    # a bare hyphen IS a RediSearch special char (NOT-operator) -- "orders-api"
    # sanitizes to two tokens, not one glued "ordersapi" word.
    assert _sanitize_fulltext_query("orders-api") == "orders api"


def test_sanitize_all_special_chars_yields_empty_string():
    assert _sanitize_fulltext_query('@{}|()~*"$:%-<>') == ""


def test_sanitize_empty_and_whitespace_only_yields_empty_string():
    assert _sanitize_fulltext_query("") == ""
    assert _sanitize_fulltext_query("   ") == ""


def test_sanitize_leaves_plain_alnum_query_untouched():
    assert _sanitize_fulltext_query("create_order") == "create_order"


def test_search_fulltext_short_circuits_before_touching_store_when_query_sanitizes_to_empty():
    store = FalkorStore(FalkorDBConfig(), "does-not-matter")
    assert store.search_fulltext("@{}~*", k=5) == []
    # _connect() was never called -- proves the empty-after-sanitize path returns
    # BEFORE any FalkorDB access, not merely that the (possibly network-dependent)
    # query happened to come back empty.
    assert store._db is None


def test_search_text_chunks_short_circuits_before_touching_store_when_query_sanitizes_to_empty():
    # M3 T7: search_text_chunks reuses the SAME _sanitize_fulltext_query helper/
    # short-circuit discipline as search_fulltext above, just over Chunk instead of Sym.
    store = FalkorStore(FalkorDBConfig(), "does-not-matter")
    assert store.search_text_chunks("@{}~*", k=5) == []
    assert store._db is None


# -- M4 T3: _fulltext_or_query, the pure helper that builds the second-pass
# (AND -> OR) RediSearch query text -- live proof that the fallback actually finds
# mixed-language results (and leaves single-token/AND-successful queries alone) is
# an integration test against a real index (tests/integration/test_falkordb_store.py,
# falkordb marker); this is just the query-string construction, no network.


def test_fulltext_or_query_joins_multi_token_sanitized_query_with_pipe():
    assert _fulltext_or_query("создание OrderCreated заказа") == "создание | OrderCreated | заказа"


def test_fulltext_or_query_is_none_for_single_token():
    # nothing to widen -- a single token's AND and OR forms are identical, so
    # callers must skip the second pass entirely rather than re-run it verbatim.
    assert _fulltext_or_query("OrderCreated") is None


def test_fulltext_or_query_is_none_for_empty_string():
    # defensive: _sanitize_fulltext_query's own "" output never reaches here in
    # practice (search_fulltext/search_text_chunks short-circuit first), but
    # "".split() == [] must not raise or produce a bogus "" OR-query.
    assert _fulltext_or_query("") is None


# -- M5 T2 review (Important): search_vector_chunks_exact dimension-mismatch
# conversion. vec.cosineDistance evaluates per row and hard-raises
# ResponseError("Vector dimension mismatch, expected N but got M" -- captured live
# on FalkorDB v4.18.11) on the first stored embedding whose length differs from the
# query vector's, where the ANN twin silently EXCLUDES wrong-dim vectors (they never
# enter its index -- live-verified asymmetry). Unreachable through the real pipeline
# (pipeline/load._chunk_node_batches dim-gates every chunk), but the method must
# still convert that raw redis error into the store's established StoreError
# discipline rather than leak it. Fake graph injected via store._graph -- no
# network, same charter as the rest of this module; the live mixed-dim scenario is
# tests/integration/test_falkordb_ddl.py (falkordb marker).


class _RaisingGraph:
    """Stands in for FalkorStore._graph: every query raises the configured error."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def query(self, cypher, params=None):
        raise self.exc


def test_search_vector_chunks_exact_converts_dim_mismatch_to_store_error():
    store = FalkorStore(FalkorDBConfig(), "does-not-matter")
    raw = redis.exceptions.ResponseError("Vector dimension mismatch, expected 6 but got 4")
    store._graph = _RaisingGraph(raw)

    with pytest.raises(StoreError) as exc_info:
        store.search_vector_chunks_exact([1.0, 0.0, 0.0, 0.0], k=5)

    # The CONVERTED form, not a raw passthrough: ResponseError IS a StoreError
    # subclass, so `raises(StoreError)` alone can't discriminate -- pin the exact
    # type (the base StoreError/RedisError itself) plus the actionable-cause text
    # a raw redis message can never contain, plus the exception chain back to the
    # original error.
    assert type(exc_info.value) is StoreError
    assert "mixed-model" in str(exc_info.value)
    assert "Vector dimension mismatch, expected 6 but got 4" in str(exc_info.value)
    assert exc_info.value.__cause__ is raw


def test_search_vector_chunks_exact_other_response_errors_propagate_unchanged():
    # Same discipline as the ANN method's _NO_VECTOR_INDEX_MARKER catch: a
    # substring match on the one empirically-captured message, NOT a blind
    # catch-all -- a malformed query or genuinely different server-side failure
    # must still propagate as-is.
    store = FalkorStore(FalkorDBConfig(), "does-not-matter")
    raw = redis.exceptions.ResponseError("Invalid input 'GARBAGE': expected ...")
    store._graph = _RaisingGraph(raw)

    with pytest.raises(redis.exceptions.ResponseError) as exc_info:
        store.search_vector_chunks_exact([1.0, 0.0, 0.0, 0.0], k=5)

    assert exc_info.value is raw  # the very same object, untouched
    assert "mixed-model" not in str(exc_info.value)
