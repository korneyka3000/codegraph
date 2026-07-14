"""Юнит-тест pipeline.load._labels_for_kind (M2): чистая функция kind/roles ->
labels-кортеж, без живого FalkorDB (тот сценарий -- tests/integration/
test_pipeline_load.py, marker falkordb, реальный MERGE с multi-label)."""

from __future__ import annotations

import pytest

from codegraph.core.errors import InvariantError
from codegraph.pipeline.load import _labels_for_kind


def test_code_kind_without_roles():
    assert _labels_for_kind("Function") == ("Sym", "Function")
    assert _labels_for_kind("Class") == ("Sym", "Class")
    assert _labels_for_kind("Module") == ("Sym", "Module")


def test_code_kind_with_roles_appended_in_order():
    assert _labels_for_kind("Function", ("RouteHandler",)) == ("Sym", "Function", "RouteHandler")
    assert _labels_for_kind("Function", ("MessageConsumer", "TemporalActivity")) == (
        "Sym", "Function", "MessageConsumer", "TemporalActivity",
    )


def test_service_kind_ignores_roles():
    assert _labels_for_kind("Service") == ("Service",)
    assert _labels_for_kind("Service", ("RouteHandler",)) == ("Service",)


def test_channel_kind():
    assert _labels_for_kind("Channel") == ("Channel",)
    assert _labels_for_kind("Channel", ("RouteHandler",)) == ("Channel",)


def test_business_process_kind():
    assert _labels_for_kind("BusinessProcess") == ("BusinessProcess",)


def test_unknown_kind_raises_invariant_error():
    with pytest.raises(InvariantError):
        _labels_for_kind("Nope")
