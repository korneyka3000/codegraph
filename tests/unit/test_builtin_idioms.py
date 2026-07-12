import pytest

from codegraph.config.builtin_idioms import BUILTIN_IDIOMS, resolve_builtins
from codegraph.config.models import DEFAULT_BUILTIN_IDIOMS, ServiceIdioms


def test_registry_covers_all_defaults():
    assert set(BUILTIN_IDIOMS) == set(DEFAULT_BUILTIN_IDIOMS)


def test_all_builtins_are_valid_service_idioms():
    for name, idioms in BUILTIN_IDIOMS.items():
        assert isinstance(idioms, ServiceIdioms), name


def test_aiokafka_producer_send_topic_from_arg0():
    prods = BUILTIN_IDIOMS["aiokafka"].producers
    send = next(p for p in prods if "send" in p.call)
    assert send.channel.kind == "kafka_topic"
    assert send.channel.name_from.arg == 0


def test_faststream_uses_decorators():
    fs = BUILTIN_IDIOMS["faststream"]
    assert any(c.kind == "decorator" for c in fs.consumers)


def test_resolve_builtins_merges_and_rejects_unknown():
    merged = resolve_builtins(["aiokafka", "faststream"])
    assert len(merged.producers) >= 2
    with pytest.raises(KeyError, match="unknown builtin idiom"):
        resolve_builtins(["kafka-python"])
