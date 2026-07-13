from codegraph.parsing.facts import build_file_facts

SRC = b'''"""Module doc."""
import os
from app.db.session import Session, get_db
from . import sibling


class OrderService:
    """Svc doc."""

    def __init__(self, db):
        self._db = db

    async def place(self, req):
        order = self._build(req)
        await self._persist(order)
        outbox = OutboxRepository(self._db)
        await outbox.add_event("OrderCreated", {})
        return order

    def _build(self, req):
        return req


def helper():
    print(os.getpid())
'''


def _facts():
    return build_file_facts("app/services/order.py", SRC)


def test_module_docstring_and_imports():
    f = _facts()
    assert f.module_docstring == "Module doc."
    targets = [(i.target_module, i.names) for i in f.imports]
    assert ("os", []) in targets
    assert ("app.db.session", ["Session", "get_db"]) in targets
    assert (".", ["sibling"]) in targets


def test_def_hierarchy_and_flags():
    f = _facts()
    by_name = {d.name: d for d in f.defs}
    cls = by_name["OrderService"]
    place = by_name["place"]
    assert cls.kind == "class" and cls.parent is None
    assert place.kind == "function" and f.defs[place.parent] is cls
    assert place.is_async and not by_name["helper"].is_async
    assert by_name["helper"].parent is None
    assert cls.docstring == "Svc doc."
    assert place.signature.startswith("def place(") or "place(self, req)" in place.signature


def test_name_token_span_points_at_name():
    f = _facts()
    place = next(d for d in f.defs if d.name == "place")
    assert SRC[place.name_start_byte:place.name_end_byte] == b"place"


def test_calls_with_enclosing():
    f = _facts()
    calls = {(c.callee_name, c.enclosing_def) for c in f.calls}
    place_i = next(d.index for d in f.defs if d.name == "place")
    helper_i = next(d.index for d in f.defs if d.name == "helper")
    assert ("_build", place_i) in calls
    assert ("_persist", place_i) in calls
    assert ("OutboxRepository", place_i) in calls
    assert ("add_event", place_i) in calls
    assert ("print", helper_i) in calls
    assert ("getpid", helper_i) in calls


def test_callee_token_span_is_last_segment():
    f = _facts()
    add_event = next(c for c in f.calls if c.callee_name == "add_event")
    assert SRC[add_event.callee_start_byte:add_event.callee_end_byte] == b"add_event"


def test_smoke_all_fixture_files_parse():
    from pathlib import Path

    fixtures = Path(__file__).parents[2] / "fixtures" / "services"
    for f in fixtures.rglob("*.py"):
        facts = build_file_facts(str(f), f.read_bytes())
        assert facts is not None
