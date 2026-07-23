"""class_attrs: ClassAttrIndex/SettingsField harvesting from class-body literals
(pydantic-Settings fields, Enum/StrEnum member values) -- M7 T1, the milestone's
foundation (open-gaps R1/R2, see docs/superpowers/reports/2026-07-23-pilot-rerun-
open-gaps.md and docs/superpowers/plans/2026-07-23-m7-settings-http-signals.md).
Consumed later by T2 (kafka producer/consumer topic literals) and T3 (HTTP
self.host auto-anchoring) -- this task ships no consumer changes.

Synthetic sources throughout: no fixtures/services/* file has a Settings/Enum class
yet (T6 adds one to fixtures/realstack) -- mirrors test_kafka_extractor.py's own
precedent ("Synthetic sources cover branches no real fixture reaches").

Two independent construction paths are exercised on purpose (`_claims`/`_index`
build straight from FileFacts; the staging-round-trip tests below go through
`Staging.add_claims`/`claims_for`, the ACTUAL path analyze.py's wiring uses) -- both
funnel through the same `build_class_attr_index(claims: list[dict])`, so the round
trip is a real, load-bearing equality check, not just a construction convenience.
"""

from __future__ import annotations

from codegraph.parsing.class_attrs import (
    ClassAttrIndex,
    SettingsField,
    build_class_attr_index,
    harvest_class_attrs,
)
from codegraph.parsing.facts import build_file_facts


def _claims(relpath: str, src: bytes) -> list[dict]:
    facts = build_file_facts(relpath, src)
    return harvest_class_attrs(relpath, facts)


def _index(relpath: str, src: bytes) -> ClassAttrIndex:
    return build_class_attr_index(_claims(relpath, src))


# -- SettingsField: contract shape (verbatim field order per brief) --


def test_settings_field_positional_order_matches_contract():
    sf = SettingsField("app.x.Y", "f", "d", "ENV")
    assert sf.class_fqn == "app.x.Y"
    assert sf.field == "f"
    assert sf.default == "d"
    assert sf.env_name == "ENV"


# -- Settings fields: default + env_prefix --

SETTINGS_SRC = b'''from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="service_")

    verification_requests_url: str = "http://localhost:8000"
    legacylizer_url: str
'''


def test_settings_field_with_default_and_env_prefix():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    sf = idx.settings_field("ServiceSettings", "verification_requests_url")
    assert sf == SettingsField(
        class_fqn="app.config.services.ServiceSettings",
        field="verification_requests_url",
        default="http://localhost:8000",
        env_name="SERVICE_VERIFICATION_REQUESTS_URL",
    )


def test_settings_field_without_default_has_default_none_env_name_present():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    sf = idx.settings_field("ServiceSettings", "legacylizer_url")
    assert sf is not None
    assert sf.default is None
    assert sf.env_name == "SERVICE_LEGACYLIZER_URL"


def test_settings_field_full_fqn_also_matches_not_just_suffix():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    sf = idx.settings_field(
        "app.config.services.ServiceSettings", "verification_requests_url",
    )
    assert sf is not None and sf.default == "http://localhost:8000"


def test_settings_field_unknown_class_or_field_is_none():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    assert idx.settings_field("NoSuchClass", "verification_requests_url") is None
    assert idx.settings_field("ServiceSettings", "no_such_field") is None


def test_model_config_itself_is_not_indexed_as_a_settings_field():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    assert idx.settings_field("ServiceSettings", "model_config") is None
    assert idx.field_by_name("model_config") is None


# -- alias/validation_alias wins over env_prefix --

ALIAS_SRC = b'''from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="worker_")

    topic: str = Field(default="orders.events", alias="ORDERS_TOPIC")
    other: str = Field(default="x", validation_alias="OTHER_ALIAS")
'''


def test_settings_field_alias_wins_over_env_prefix():
    idx = _index("app/config/worker.py", ALIAS_SRC)
    sf = idx.settings_field("WorkerSettings", "topic")
    assert sf.default == "orders.events"
    assert sf.env_name == "ORDERS_TOPIC"  # NOT "WORKER_TOPIC"


def test_settings_field_validation_alias_also_wins_over_env_prefix():
    idx = _index("app/config/worker.py", ALIAS_SRC)
    sf = idx.settings_field("WorkerSettings", "other")
    assert sf.default == "x"
    assert sf.env_name == "OTHER_ALIAS"


# -- no env_prefix at all --

NO_PREFIX_SRC = b'''class PlainSettings:
    host: str = "localhost"
'''


def test_settings_field_no_model_config_env_name_none():
    idx = _index("app/config/plain.py", NO_PREFIX_SRC)
    sf = idx.settings_field("PlainSettings", "host")
    assert sf.default == "localhost"
    assert sf.env_name is None


# -- enum values --

ENUM_SRC = b'''from enum import StrEnum


class KycTopicName(StrEnum):
    STEP_CHANGED = "kyc.camunda.step_changed"
    RESTRICTIONS_CHANGED = "kyc.camunda.restrictions_changed"
'''


def test_enum_values_happy_path():
    idx = _index("app/models/enums.py", ENUM_SRC)
    assert idx.enum_values("KycTopicName") == (
        "kyc.camunda.step_changed", "kyc.camunda.restrictions_changed",
    )


def test_enum_values_full_fqn_also_matches():
    idx = _index("app/models/enums.py", ENUM_SRC)
    assert idx.enum_values("app.models.enums.KycTopicName") == (
        "kyc.camunda.step_changed", "kyc.camunda.restrictions_changed",
    )


NOT_ENUM_SRC = b'''class Plain:
    x = "a"
    y = "b"
'''


def test_enum_values_none_for_non_enum_class():
    idx = _index("app/models/plain.py", NOT_ENUM_SRC)
    assert idx.enum_values("Plain") is None


NON_STRING_ENUM_SRC = b'''from enum import Enum


class MixedEnum(Enum):
    A = "a"
    B = 2
'''


def test_enum_values_none_when_any_member_not_string_literal():
    idx = _index("app/models/mixed.py", NON_STRING_ENUM_SRC)
    assert idx.enum_values("MixedEnum") is None


def test_enum_members_are_not_also_indexed_as_settings_fields():
    idx = _index("app/models/enums.py", ENUM_SRC)
    assert idx.settings_field("KycTopicName", "STEP_CHANGED") is None
    assert idx.field_by_name("STEP_CHANGED") is None


def test_class_with_no_class_body_literals_is_absent_from_index():
    idx = _index("app/x.py", b"class Empty:\n    def f(self):\n        pass\n")
    assert idx.settings_field("Empty", "anything") is None
    assert idx.enum_values("Empty") is None


# -- field_by_name: unique across the service index, honest collision -> None (T3
# auto-join safety net). M7 T1 review Important-4: the by-name join is ENV-GATED --
# only fields actually carrying an env_name (env_prefix'd Settings classes, or an
# explicit alias) participate at all; a plain DTO/model class (no model_config, no
# alias) never pollutes the join surface. Both collision fixtures below therefore
# carry env_prefix ON PURPOSE: without it they'd be excluded outright and the
# collision assertion would pass vacuously ("not found" instead of "ambiguous").


def test_field_by_name_unique_resolves():
    idx = _index("app/config/services.py", SETTINGS_SRC)
    sf = idx.field_by_name("verification_requests_url")
    assert sf is not None
    assert sf.class_fqn == "app.config.services.ServiceSettings"
    assert sf.env_name == "SERVICE_VERIFICATION_REQUESTS_URL"


COLLIDING_SRC_A = b'''from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsA(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="a_")

    host: str = "a-host"
'''
COLLIDING_SRC_B = b'''from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsB(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="b_")

    host: str = "b-host"
'''


def test_field_by_name_collision_across_two_env_carrying_classes_is_none():
    claims = _claims("app/a.py", COLLIDING_SRC_A) + _claims("app/b.py", COLLIDING_SRC_B)
    idx = build_class_attr_index(claims)
    # sanity: BOTH fields really are env-carrying (i.e. both genuinely participate
    # in the by-name join -- the ambiguity below is a real collision between two
    # joinable fields, not a vacuous "neither was ever joinable").
    assert idx.settings_field("SettingsA", "host").env_name == "A_HOST"
    assert idx.settings_field("SettingsB", "host").env_name == "B_HOST"
    assert idx.field_by_name("host") is None
    # the honest ambiguity is scoped to field_by_name -- per-class lookup (the caller
    # already knows which class it means) is unaffected.
    assert idx.settings_field("SettingsA", "host").default == "a-host"
    assert idx.settings_field("SettingsB", "host").default == "b-host"


DTO_SRC = b'''class OrderDTO:
    status: str = "pending"
'''


def test_field_by_name_excludes_fields_without_env_name():
    """M7 T1 review Important-4 pin: a DTO-shaped class (class-body string default,
    but NO model_config/env_prefix and NO alias -> env_name=None) is still harvested
    and per-class-queryable, but does NOT participate in the by-name join at all --
    T3's auto-anchor joins THROUGH env (self.host -> field -> env_name -> service),
    so a field carrying no env name could never anchor anything anyway; keeping it
    out kills the DTO collision-pollution surface the reviewer measured (2/6
    field-name collisions even on the tiny fixture set)."""
    idx = _index("app/dto.py", DTO_SRC)
    assert idx.settings_field("OrderDTO", "status").default == "pending"
    assert idx.settings_field("OrderDTO", "status").env_name is None
    assert idx.field_by_name("status") is None


def test_field_by_name_env_carrying_field_not_shadowed_by_dto_same_name():
    """The flip side of the exclusion: a DTO field sharing a Settings field's NAME
    is not a collision any more (pre-Important-4 it was -- the by-name join went
    ambiguous-None and T3's auto-anchor would have refused) -- the env-carrying
    field stays uniquely joinable."""
    settings_src = (
        b"from pydantic_settings import BaseSettings, SettingsConfigDict\n"
        b"\n"
        b"\n"
        b"class HostSettings(BaseSettings):\n"
        b'    model_config = SettingsConfigDict(env_prefix="svc_")\n'
        b"\n"
        b'    status: str = "enabled"\n'
    )
    claims = _claims("app/config.py", settings_src) + _claims("app/dto.py", DTO_SRC)
    idx = build_class_attr_index(claims)
    sf = idx.field_by_name("status")
    assert sf is not None
    assert sf.class_fqn == "app.config.HostSettings"
    assert sf.env_name == "SVC_STATUS"


# -- M7 T1 review Important-3: INHERITED model_config/env_prefix (real pydantic
# semantics: a subclass of a Settings base inherits its model_config) is NOT visible
# to this per-class, per-file harvest -- tracked limitation, documented in
# class_attrs.py's module docstring (workarounds there: repeat model_config in the
# subclass, or put an explicit alias on the field). This test PINS the current
# honest-None behavior so a future inheritance-aware fix has to consciously flip it.


INHERITED_PREFIX_SRC = b'''from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="service_")


class ChildSettings(BaseServiceSettings):
    child_url: str = "http://child:8000"
'''


def test_inherited_env_prefix_not_seen_env_name_none_documented_limitation():
    idx = _index("app/config/inherited.py", INHERITED_PREFIX_SRC)
    sf = idx.settings_field("ChildSettings", "child_url")
    assert sf is not None
    assert sf.default == "http://child:8000"
    # real pydantic would derive SERVICE_CHILD_URL here via the INHERITED
    # env_prefix -- the harvest honestly reports None instead of guessing.
    assert sf.env_name is None
    # and, per Important-4's env-gating, an env-less field is not by-name joinable.
    assert idx.field_by_name("child_url") is None


# -- M7 T1 review Minor-1: positional Field("value", alias=...) default --


POSITIONAL_FIELD_SRC = b'''from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PosSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="pos_")

    topic: str = Field("orders.events", alias="ORDERS_TOPIC")
    plain: str = Field("v")
'''


def test_settings_field_positional_field_default_is_read():
    """pydantic's Field accepts the default as its FIRST POSITIONAL argument
    (`Field("orders.events", alias=...)`) just as legitimately as `default=` --
    before this fix the positional spelling silently lost the default (None)."""
    idx = _index("app/config/pos.py", POSITIONAL_FIELD_SRC)
    sf = idx.settings_field("PosSettings", "topic")
    assert sf.default == "orders.events"
    assert sf.env_name == "ORDERS_TOPIC"  # alias still wins over pos_ prefix
    plain = idx.settings_field("PosSettings", "plain")
    assert plain.default == "v"
    assert plain.env_name == "POS_PLAIN"  # no alias -> prefix-derived


# -- suffix-matching ambiguity (same "last segments" convention as M6's base_class
# matching, kafka_ext.py's `_match_base_tier`) extended, for the same false-match-
# worse-than-absence reason, to settings_field/enum_values -- not just field_by_name --
# when a SHORT suffix matches more than one class. For settings_field specifically,
# ambiguity requires BOTH classes to match the suffix AND actually define the queried
# field -- two same-suffix classes where only one has the field is NOT ambiguous (the
# other one was never a candidate for THIS field to begin with), so both fixtures
# below share the exact same field name on purpose.


SAME_NAME_SRC_A = b'''class Config:
    host: str = "a-host"
'''
SAME_NAME_SRC_B = b'''class Config:
    host: str = "b-host"
'''


def test_settings_field_ambiguous_class_suffix_is_none_full_fqn_disambiguates():
    claims = _claims("svc/a/config.py", SAME_NAME_SRC_A) + _claims(
        "svc/b/config.py", SAME_NAME_SRC_B,
    )
    idx = build_class_attr_index(claims)
    assert idx.settings_field("Config", "host") is None
    assert idx.settings_field("svc.a.config.Config", "host").default == "a-host"


_STATUS_A = b'''from enum import StrEnum
class Status(StrEnum):
    A = "a"
'''
_STATUS_B = b'''from enum import StrEnum
class Status(StrEnum):
    B = "b"
'''


def test_enum_values_ambiguous_class_suffix_is_none_full_fqn_disambiguates():
    claims = _claims("svc/a/enums.py", _STATUS_A) + _claims("svc/b/enums.py", _STATUS_B)
    idx = build_class_attr_index(claims)
    assert idx.enum_values("Status") is None
    assert idx.enum_values("svc.a.enums.Status") == ("a",)


# -- claims round trip: staging-assembled index matches direct (in-memory) construction --


def test_claims_round_trip_via_staging_matches_direct_construction(tmp_path):
    from codegraph.stores.staging import Staging

    facts_by_file = {
        "app/config/services.py": build_file_facts(
            "app/config/services.py", SETTINGS_SRC,
        ),
        "app/models/enums.py": build_file_facts("app/models/enums.py", ENUM_SRC),
    }
    direct_claims = [
        c for rp, f in facts_by_file.items() for c in harvest_class_attrs(rp, f)
    ]
    direct_index = build_class_attr_index(direct_claims)

    st = Staging(tmp_path / "s.db")
    for rp, f in facts_by_file.items():
        st.add_claims("svc-a", rp, "class_attrs", harvest_class_attrs(rp, f))
    staged_claims = st.claims_for("class_attrs", "svc-a")
    staged_index = build_class_attr_index(staged_claims)

    assert staged_index == direct_index
    # sanity: the round trip actually carried real content, not two empty indexes.
    assert staged_index.settings_field(
        "ServiceSettings", "verification_requests_url",
    ) == SettingsField(
        class_fqn="app.config.services.ServiceSettings",
        field="verification_requests_url",
        default="http://localhost:8000",
        env_name="SERVICE_VERIFICATION_REQUESTS_URL",
    )
    assert staged_index.enum_values("KycTopicName") == (
        "kyc.camunda.step_changed", "kyc.camunda.restrictions_changed",
    )


def test_claims_for_injected_service_relpath_keys_are_ignored_by_the_builder():
    """`Staging.claims_for` injects "_service"/"_relpath" into every payload dict
    (its own documented contract) -- build_class_attr_index must not choke on, or be
    influenced by, those extra keys."""
    claims = _claims("app/config/services.py", SETTINGS_SRC)
    decorated = [{**c, "_service": "svc-a", "_relpath": c["class_fqn"]} for c in claims]
    idx = build_class_attr_index(decorated)
    assert idx.settings_field(
        "ServiceSettings", "verification_requests_url",
    ).default == "http://localhost:8000"


# -- incremental coherence: an unchanged file's claims persist across a run that only
# re-harvests a DIFFERENT (stale) file -- staging.claims_for reads service-wide, not
# scoped to whatever this call's harvest pass touched (see analyze.py's own wiring).


def test_incremental_coherence_unchanged_file_claims_persist_across_stale_only_harvest(
    tmp_path,
):
    from codegraph.stores.staging import Staging

    facts_a = build_file_facts("app/config/services.py", SETTINGS_SRC)
    facts_b_v1 = build_file_facts("app/other.py", b'class Other:\n    x = "v1"\n')

    st = Staging(tmp_path / "s.db")
    st.add_claims(
        "svc-a", "app/config/services.py", "class_attrs",
        harvest_class_attrs("app/config/services.py", facts_a),
    )
    st.add_claims(
        "svc-a", "app/other.py", "class_attrs",
        harvest_class_attrs("app/other.py", facts_b_v1),
    )

    # "incremental" run: only app/other.py is stale (content changed) --
    # delete_file_layer wipes its OLD claims row(s) first (staging.py's own per-
    # relpath contract), then the fresh harvest re-writes just that file's claims.
    # app/config/services.py is untouched this call (not in the stale set at all).
    st.delete_file_layer("svc-a", {"app/other.py"}, drop_calls_evidence=set())
    facts_b_v2 = build_file_facts("app/other.py", b'class Other:\n    x = "v2"\n')
    st.add_claims(
        "svc-a", "app/other.py", "class_attrs",
        harvest_class_attrs("app/other.py", facts_b_v2),
    )

    index = build_class_attr_index(st.claims_for("class_attrs", "svc-a"))
    assert index.settings_field(
        "ServiceSettings", "verification_requests_url",
    ).default == "http://localhost:8000"
    assert index.settings_field("Other", "x").default == "v2"
