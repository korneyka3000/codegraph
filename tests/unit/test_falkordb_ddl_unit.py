"""Unit test for ddl.ensure_schema's pure `dim` validation (M3 T6 code-review fix) --
runs BEFORE any FalkorDB connection is touched (`db.select_graph` isn't called until
after the check), so this needs no live FalkorDB at all, unlike
tests/integration/test_falkordb_ddl.py (marker falkordb, real DDL/index creation).

Named `..._unit.py` (not a bare `test_falkordb_ddl.py`) specifically to avoid a pytest
"import file mismatch" collision with tests/integration/test_falkordb_ddl.py -- this
repo's test tree has no `__init__.py` files (implicit namespace packages), so pytest
requires every test module basename to be unique across the WHOLE tree, not just
within one directory (confirmed live: an earlier same-named unit/test_falkordb_ddl.py
made collection fail outright)."""

from __future__ import annotations

import pytest

from codegraph.core.errors import InvariantError
from codegraph.stores.falkordb.ddl import ensure_schema


def test_ensure_schema_rejects_zero_dim():
    with pytest.raises(InvariantError, match="positive int"):
        ensure_schema(db=None, graph_name="x", dim=0)


def test_ensure_schema_rejects_negative_dim():
    with pytest.raises(InvariantError, match="positive int"):
        ensure_schema(db=None, graph_name="x", dim=-5)


def test_ensure_schema_dim_none_does_not_raise_before_touching_db():
    """dim=None must reach the `db.select_graph(...)` call (i.e. NOT be rejected by
    the positivity check) -- passing a deliberately-broken `db` stub whose
    `select_graph` raises a distinctive error proves the validation didn't short
    -circuit before it, and that a real (positive) dim doesn't raise either."""

    class _BoomDB:
        def select_graph(self, name):
            raise RuntimeError("select_graph reached")

    with pytest.raises(RuntimeError, match="select_graph reached"):
        ensure_schema(db=_BoomDB(), graph_name="x", dim=None)
    with pytest.raises(RuntimeError, match="select_graph reached"):
        ensure_schema(db=_BoomDB(), graph_name="x", dim=8)
