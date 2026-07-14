"""Тесты ярусного idiom-матчера (M2 T3): match_calls (STATIC/RECEIVER/IMPORT_NAME,
приоритет, дедуп) и match_decorators — по контракту брифа. Где осмысленно, факты
берутся из РЕАЛЬНЫХ фикстурных файлов (events/producer.py, consumer_main.py,
db/outbox.py + services/order.py, routes/documents.py, workflows/kyc.py); синтетические
source-сниппеты — только там, где реальные фикстуры не покрывают конкретную ветку
(RECEIVER-позитив с совпадающим именем receiver'а, ctor+module-import+attr-вызов,
негатив "нет вообще никакой эвиденции", стек декораторов)."""

from pathlib import Path

from codegraph.extractors.idiom_match import (
    CallMatch,
    MatchTier,
    match_calls,
    match_decorators,
)
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"


def _fixture_facts(relpath: str):
    path = FIXTURES / relpath
    return build_file_facts(str(path), path.read_bytes())


def _no_qualified(call):
    return None


# -- MatchTier / CallMatch: contract shape -------------------------------------------


def test_match_tier_static_resolution_and_confidence():
    assert MatchTier.STATIC.resolution == "static"
    assert MatchTier.STATIC.confidence == 1.0


def test_match_tier_receiver_resolution_and_confidence():
    assert MatchTier.RECEIVER.resolution == "heuristic"
    assert MatchTier.RECEIVER.confidence == 0.8


def test_match_tier_import_name_resolution_and_confidence():
    assert MatchTier.IMPORT_NAME.resolution == "heuristic"
    assert MatchTier.IMPORT_NAME.confidence == 0.6


def test_call_match_field_order_matches_contract():
    facts = _fixture_facts("orders_api/app/db/outbox.py")
    call = facts.calls[0]
    m = CallMatch(call, MatchTier.STATIC, "static", 1.0)
    assert m.call is call
    assert m.tier is MatchTier.STATIC
    assert m.resolution == "static"
    assert m.confidence == 1.0


# -- STATIC tier ----------------------------------------------------------------------


def test_static_tier_exact_qualified_match_on_outbox_fixture():
    facts = _fixture_facts("orders_api/app/services/order.py")
    add_event = next(c for c in facts.calls if c.callee_name == "add_event")

    def qualified_of(call):
        return "app.db.outbox.OutboxRepository.add_event" if call is add_event else None

    matches = match_calls("app.db.outbox.OutboxRepository.add_event", facts, qualified_of)
    assert len(matches) == 1
    m = matches[0]
    assert m.call is add_event
    assert m.tier is MatchTier.STATIC
    assert m.resolution == "static" and m.confidence == 1.0


def test_static_tier_matches_pattern_without_module_prefix_via_star_dot():
    facts = _fixture_facts("orders_api/app/services/order.py")
    add_event = next(c for c in facts.calls if c.callee_name == "add_event")

    def qualified_of(call):
        return "app.db.outbox.OutboxRepository.add_event" if call is add_event else None

    matches = match_calls("OutboxRepository.add_event", facts, qualified_of)
    assert len(matches) == 1
    assert matches[0].tier is MatchTier.STATIC


# -- RECEIVER tier ----------------------------------------------------------------------

RECEIVER_POSITIVE_SRC = b'''from aiokafka import AIOKafkaProducer


async def emit() -> None:
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.send("t", b"x")
'''


def test_receiver_tier_positive_synthetic_matching_target_name():
    facts = build_file_facts("m.py", RECEIVER_POSITIVE_SRC)
    send = next(c for c in facts.calls if c.callee_name == "send")

    matches = match_calls("aiokafka.AIOKafkaProducer.send", facts, _no_qualified)
    assert len(matches) == 1
    m = matches[0]
    assert m.call is send
    assert m.tier is MatchTier.RECEIVER
    assert m.resolution == "heuristic" and m.confidence == 0.8


def test_receiver_tier_outbox_fixture_when_static_absent():
    facts = _fixture_facts("orders_api/app/services/order.py")
    add_event = next(c for c in facts.calls if c.callee_name == "add_event")

    matches = match_calls("app.db.outbox.OutboxRepository.add_event", facts, _no_qualified)
    m = next(m for m in matches if m.call is add_event)
    assert m.tier is MatchTier.RECEIVER
    assert m.confidence == 0.8


def test_receiver_tier_fails_on_real_producer_fixture_name_mismatch():
    """`_producer = AIOKafkaProducer(...)` (глобальный target) vs `producer.send(...)`
    (receiver "producer", присвоенный ЧЕРЕЗ `producer = await get_producer()`) — имена
    target'ов не совпадают, поэтому RECEIVER не срабатывает (см. self-review брифа)."""
    facts = _fixture_facts("document_management/app/events/producer.py")
    matches = match_calls("aiokafka.AIOKafkaProducer.send", facts, _no_qualified)
    send_matches = [m for m in matches if m.call.callee_name == "send"]
    assert len(send_matches) == 1
    assert send_matches[0].tier is not MatchTier.RECEIVER


# -- IMPORT_NAME tier ----------------------------------------------------------------------


def test_import_name_tier_method_form_on_real_producer_fixture():
    """Прямое продолжение предыдущего теста: раз RECEIVER не сработал, `producer.send`
    доказывается самым слабым ярусом — файл импортирует aiokafka.AIOKafkaProducer."""
    facts = _fixture_facts("document_management/app/events/producer.py")
    matches = match_calls("aiokafka.AIOKafkaProducer.send", facts, _no_qualified)
    send_matches = [m for m in matches if m.call.callee_name == "send"]
    assert len(send_matches) == 1
    m = send_matches[0]
    assert m.tier is MatchTier.IMPORT_NAME
    assert m.resolution == "heuristic" and m.confidence == 0.6


def test_import_name_tier_ctor_form_on_real_consumer_main_fixture():
    facts = _fixture_facts("kyc_worker/app/consumer_main.py")
    matches = match_calls("aiokafka.AIOKafkaConsumer", facts, _no_qualified)
    ctor_matches = [m for m in matches if m.call.callee_name == "AIOKafkaConsumer"]
    assert len(ctor_matches) == 1
    m = ctor_matches[0]
    assert m.tier is MatchTier.IMPORT_NAME
    assert m.confidence == 0.6


CTOR_ATTR_CALL_SRC = b'''import aiokafka


def make():
    return aiokafka.AIOKafkaConsumer("t")
'''


def test_import_name_ctor_form_module_import_plus_attr_call():
    """Ветка (б) без from-import: `import aiokafka` + `aiokafka.AIOKafkaConsumer(...)`
    (attr-вызов) — класс нигде не в ImportFact.names, но модуль импортирован и вызов
    атрибутный."""
    facts = build_file_facts("m.py", CTOR_ATTR_CALL_SRC)
    matches = match_calls("aiokafka.AIOKafkaConsumer", facts, _no_qualified)
    assert len(matches) == 1
    assert matches[0].tier is MatchTier.IMPORT_NAME


def test_negative_no_import_no_assign_no_qualified_is_empty():
    src = b'''def emit():
    client.send("x")
'''
    facts = build_file_facts("m.py", src)
    matches = match_calls("aiokafka.AIOKafkaProducer.send", facts, _no_qualified)
    assert matches == []


# -- Приоритет ярусов + дедуп ----------------------------------------------------------


def test_priority_static_wins_over_receiver_and_import_name():
    """outbox.add_event(...) одновременно "подходит" под STATIC (qualified-стаб),
    RECEIVER (AssignFact outbox=OutboxRepository) И IMPORT_NAME (from-import) —
    результат должен быть РОВНО один матч (дедуп по call), тира STATIC (высший приоритет)."""
    facts = _fixture_facts("orders_api/app/services/order.py")
    add_event = next(c for c in facts.calls if c.callee_name == "add_event")

    def qualified_of(call):
        return "app.db.outbox.OutboxRepository.add_event" if call is add_event else None

    matches = match_calls("app.db.outbox.OutboxRepository.add_event", facts, qualified_of)
    add_event_matches = [m for m in matches if m.call is add_event]
    assert len(add_event_matches) == 1  # не 3 отдельных матча на 3 яруса
    assert add_event_matches[0].tier is MatchTier.STATIC


def test_priority_import_name_wins_when_receiver_class_mismatches():
    """Тот же call/файл, но класс в паттерне НЕ совпадает с AssignFact.callee_name
    ("outbox = OutboxRepository(...)") -> RECEIVER не срабатывает; IMPORT_NAME всё
    равно срабатывает (условие (i) не зависит от класса, только от первого сегмента
    паттерна и метода)."""
    facts = _fixture_facts("orders_api/app/services/order.py")
    matches = match_calls("app.db.outbox.WrongClassName.add_event", facts, _no_qualified)
    add_event_matches = [m for m in matches if m.call.callee_name == "add_event"]
    assert len(add_event_matches) == 1
    assert add_event_matches[0].tier is MatchTier.IMPORT_NAME


# -- match_decorators ----------------------------------------------------------------------


def test_match_decorators_bare_equality_workflow_defn():
    facts = _fixture_facts("kyc_worker/app/workflows/kyc.py")
    results = match_decorators("workflow.defn", facts.defs)
    assert len(results) == 1
    d, text = results[0]
    assert d.name == "KycWorkflow"
    assert text == "workflow.defn"


def test_match_decorators_bare_equality_workflow_run():
    facts = _fixture_facts("kyc_worker/app/workflows/kyc.py")
    results = match_decorators("workflow.run", facts.defs)
    assert len(results) == 1
    d, text = results[0]
    assert d.name == "run"
    assert text == "workflow.run"


def test_match_decorators_call_form_prefix_router_get():
    facts = _fixture_facts("document_management/app/routes/documents.py")
    results = match_decorators("router.get", facts.defs)
    assert len(results) == 1
    d, text = results[0]
    assert d.name == "get_document"
    assert text == 'router.get("/{doc_id}")'


def test_match_decorators_glob_pattern_matches_both_router_verbs():
    facts = _fixture_facts("document_management/app/routes/documents.py")
    results = match_decorators("router.*", facts.defs)
    names = sorted(d.name for d, _ in results)
    assert names == ["create_document", "get_document"]


def test_match_decorators_negative_no_match():
    facts = _fixture_facts("kyc_worker/app/workflows/kyc.py")
    assert match_decorators("app.on_event", facts.defs) == []


STACKED_DECORATOR_SRC = b'''@a.x
@b.y("z")
def f():
    pass
'''


def test_match_decorators_stacked_decorators_matched_independently():
    facts = build_file_facts("m.py", STACKED_DECORATOR_SRC)
    assert [t for _, t in match_decorators("a.x", facts.defs)] == ["a.x"]
    assert [t for _, t in match_decorators("b.y", facts.defs)] == ['b.y("z")']
    assert match_decorators("c.z", facts.defs) == []
