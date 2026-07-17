"""pipeline.diff: per-service scan-vs-staged file delta (added/changed/deleted/
unchanged, sorted regardless of input order; ServiceDelta.empty) + config_fingerprint
sensitivity (idioms/excludes/schema change it, svc.path does not)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from codegraph.config.models import (
    ChannelSpec,
    ProducerIdiom,
    ServiceConfig,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.core.schema import SCHEMA_VERSION
from codegraph.pipeline.diff import ServiceDelta, config_fingerprint, service_delta


def _svc(**overrides) -> ServiceConfig:
    defaults = {"name": "orders-api", "path": Path("/repo/orders-api")}
    return ServiceConfig(**{**defaults, **overrides})


def _idioms() -> ServiceIdioms:
    return ServiceIdioms(
        producers=[
            ProducerIdiom(
                name="outbox",
                call="app.db.outbox.OutboxRepository.add_event",
                channel=ChannelSpec(kind="event_type", event_type_from=ValueSpec(arg=0)),
            )
        ]
    )


# -- service_delta --


def test_service_delta_added_file_not_in_staged():
    delta = service_delta({}, [("app/main.py", "sha-a", 10)])
    assert delta.added == ("app/main.py",)
    assert delta.changed == ()
    assert delta.deleted == ()
    assert delta.unchanged == ()


def test_service_delta_changed_file_sha_differs():
    delta = service_delta({"app/main.py": "sha-old"}, [("app/main.py", "sha-new", 10)])
    assert delta.changed == ("app/main.py",)
    assert delta.added == ()
    assert delta.deleted == ()
    assert delta.unchanged == ()


def test_service_delta_deleted_file_missing_from_scanned():
    delta = service_delta({"app/gone.py": "sha-a"}, [])
    assert delta.deleted == ("app/gone.py",)
    assert delta.added == ()
    assert delta.changed == ()
    assert delta.unchanged == ()


def test_service_delta_unchanged_file_same_sha():
    delta = service_delta({"app/main.py": "sha-a"}, [("app/main.py", "sha-a", 10)])
    assert delta.unchanged == ("app/main.py",)
    assert delta.added == ()
    assert delta.changed == ()
    assert delta.deleted == ()


def test_service_delta_all_four_categories_together():
    staged = {"kept.py": "sha-kept", "edited.py": "sha-edited-old", "gone.py": "sha-gone"}
    scanned = [
        ("edited.py", "sha-edited-new", 5),
        ("kept.py", "sha-kept", 5),
        ("new.py", "sha-new", 5),
    ]
    delta = service_delta(staged, scanned)
    assert delta.added == ("new.py",)
    assert delta.changed == ("edited.py",)
    assert delta.deleted == ("gone.py",)
    assert delta.unchanged == ("kept.py",)


def test_service_delta_sorted_regardless_of_input_order():
    staged = {"z.py": "sha-z", "b.py": "sha-b-old"}
    scanned = [
        ("m.py", "sha-m", 1),
        ("a.py", "sha-a", 1),
        ("b.py", "sha-b-new", 1),
    ]
    delta = service_delta(staged, scanned)
    assert delta.added == ("a.py", "m.py")
    assert delta.changed == ("b.py",)
    assert delta.deleted == ("z.py",)


def test_service_delta_empty_true_when_only_unchanged_or_nothing_at_all():
    assert service_delta({"a.py": "sha-a"}, [("a.py", "sha-a", 1)]).empty is True
    assert service_delta({}, []).empty is True


def test_service_delta_empty_false_when_added_changed_or_deleted_present():
    assert service_delta({}, [("a.py", "sha-a", 1)]).empty is False
    assert service_delta({"a.py": "old"}, [("a.py", "new", 1)]).empty is False
    assert service_delta({"a.py": "sha-a"}, []).empty is False


def test_service_delta_returns_service_delta_instance():
    assert isinstance(service_delta({}, []), ServiceDelta)


# -- config_fingerprint --


def test_config_fingerprint_deterministic_for_same_inputs():
    svc, idioms, active = _svc(), _idioms(), frozenset({"fastapi"})
    assert config_fingerprint(svc, idioms, active) == config_fingerprint(svc, idioms, active)


def test_config_fingerprint_changes_when_idioms_edited():
    svc, active = _svc(), frozenset({"fastapi"})
    fp_before = config_fingerprint(svc, ServiceIdioms(), active)
    fp_after = config_fingerprint(svc, _idioms(), active)
    assert fp_before != fp_after


def test_config_fingerprint_changes_when_exclude_edited():
    idioms, active = ServiceIdioms(), frozenset()
    fp_before = config_fingerprint(_svc(exclude=[]), idioms, active)
    fp_after = config_fingerprint(_svc(exclude=["tests/**"]), idioms, active)
    assert fp_before != fp_after


def test_config_fingerprint_changes_when_active_idioms_edited():
    svc, idioms = _svc(), ServiceIdioms()
    fp_before = config_fingerprint(svc, idioms, frozenset())
    fp_after = config_fingerprint(svc, idioms, frozenset({"fastapi"}))
    assert fp_before != fp_after


def test_config_fingerprint_unchanged_when_only_path_differs():
    idioms, active = _idioms(), frozenset({"fastapi"})
    fp_a = config_fingerprint(_svc(path=Path("/repo/orders-api")), idioms, active)
    fp_b = config_fingerprint(_svc(path=Path("/elsewhere/checkout-2")), idioms, active)
    assert fp_a == fp_b


def test_config_fingerprint_same_excludes_different_order_same_fingerprint():
    idioms, active = ServiceIdioms(), frozenset()
    fp_a = config_fingerprint(_svc(exclude=["a/**", "b/**"]), idioms, active)
    fp_b = config_fingerprint(_svc(exclude=["b/**", "a/**"]), idioms, active)
    assert fp_a == fp_b


def test_config_fingerprint_matches_documented_canonical_json_formula():
    # Pins the exact contract from the M4 T4 brief: sha256 of
    # {"exclude": sorted, "idioms": idioms.model_dump(mode="json"), "active": sorted,
    # "schema": SCHEMA_VERSION}, JSON-dumped with sort_keys=True.
    svc = _svc(exclude=["tests/**", "scripts/**"])
    idioms = _idioms()
    active = frozenset({"temporal", "fastapi"})
    expected_payload = {
        "exclude": sorted(svc.exclude),
        "idioms": idioms.model_dump(mode="json"),
        "active": sorted(active),
        "schema": SCHEMA_VERSION,
    }
    expected = hashlib.sha256(
        json.dumps(expected_payload, sort_keys=True).encode()
    ).hexdigest()
    assert config_fingerprint(svc, idioms, active) == expected


def test_config_fingerprint_returns_hex_sha256_string():
    fp = config_fingerprint(_svc(), ServiceIdioms(), frozenset())
    assert isinstance(fp, str)
    assert len(fp) == 64
    int(fp, 16)  # raises ValueError if not valid hex
