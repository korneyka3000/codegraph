"""Юнит-тесты чистых хелперов FalkorStore (M2 T8): sanitize для fulltext-запроса +
early-return search_fulltext (пустая строка после sanitize -> [] БЕЗ обращения к
FalkorDB) -- без сети/живого инстанса (тот сценарий -- tests/integration/
test_falkordb_ddl.py, marker falkordb: реальный индекс + запрос по загруженному
мини-графу)."""

from __future__ import annotations

from codegraph.config.models import FalkorDBConfig
from codegraph.stores.falkordb.store import FalkorStore, _sanitize_fulltext_query


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
