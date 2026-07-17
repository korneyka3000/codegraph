"""Юнит-тесты чистых хелперов FalkorStore (M2 T8): sanitize для fulltext-запроса +
early-return search_fulltext (пустая строка после sanitize -> [] БЕЗ обращения к
FalkorDB) -- без сети/живого инстанса (тот сценарий -- tests/integration/
test_falkordb_ddl.py, marker falkordb: реальный индекс + запрос по загруженному
мини-графу)."""

from __future__ import annotations

from codegraph.config.models import FalkorDBConfig
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
