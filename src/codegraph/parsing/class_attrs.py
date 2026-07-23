"""class_attrs: service-wide index of class-body literal assignments -- pydantic-
Settings fields (default + env name) and Enum/StrEnum member values -- harvested from
`FileFacts.class_attrs` (parsing/facts.py, M7 T1 sanctioned extension) and
`DefFact.base_exprs` (M6 T3, enum-base detection). M7 milestone foundation: consumed
LATER (T2/T3, not this task) by kafka_ext (`settings:`/`enum:` ValueSpec topic
sources) and http_client_ext (`self.host` -> Settings field -> env auto-anchor) --
see docs/superpowers/plans/2026-07-23-m7-settings-http-signals.md and
docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md (R1/R2). This module
ships ONLY the harvester + index + (via pipeline/analyze.py) the wiring that builds
one `ClassAttrIndex` per service and exposes it through `FileContext` -- no consumer
reads it yet.

-- Settings-field semantics --

A class-body attribute (`field: Type = "default"`, `field = "default"`, or a bare
`field: Type` with no value) becomes a `SettingsField` candidate for EVERY class in
the service, not just actual `BaseSettings` subclasses -- there is no base-class gate
here (contrast the enum path below, which DOES gate on `Enum`/`StrEnum` in
`base_exprs`). Two reasons: (1) the harvest is naturally narrow already -- only
class-body assignments the brief calls out (string-literal defaults, bare
annotations, or a `Field(...)`/`SettingsConfigDict(...)` call RHS) produce anything
at all; a class with no such attributes is never even entered into the index (see
`harvest_class_attrs`). (2) the by-name join surface is separately protected: since
the M7 T1 review (Important-4), `field_by_name` considers ONLY env-carrying fields
(env_name non-None -- i.e. classes with an env_prefix'd model_config, or fields with
an explicit alias), so a DTO/model class harvested by this no-gate policy can never
pollute T3's auto-anchor join at all; among the fields that DO participate, a shared
name is still an honest collision -> `None` ("false match worse than no match", the
M7 plan's own Global Constraint). Per-class lookups (`settings_field`) stay ungated
-- a caller that already names the class (T2's `settings:` source) legitimately
reads an env-less field's default too.

`model_config = SettingsConfigDict(env_prefix="...")` is detected by attribute NAME
alone (`model_config`, pydantic v2's own reserved class attribute), not by requiring
the RHS callee text to literally be `"SettingsConfigDict"` -- robust to import
aliasing (`from pydantic_settings import SettingsConfigDict as SCD`) at zero extra
cost, since the attribute name is already a strong, specific-enough signal on its
own. `model_config` itself is excluded from the harvested fields (it is metadata, not
a data field) -- see `test_model_config_itself_is_not_indexed_as_a_settings_field`.

env_name precedence (brief order, "alias/validation_alias wins over prefix"): a
`Field(...)` call RHS carrying a string `alias` or `validation_alias` keyword wins
outright (used VERBATIM, no case transform -- it already IS the literal env-var name
the author chose); otherwise, if `model_config`'s `env_prefix` was found,
`env_name = (prefix + field).upper()` (pydantic-settings' own default derivation);
otherwise `env_name = None` ("prefix not found"). A field's `default` is the RHS
string literal if there is one, else a `Field(...)` call's `default=` keyword (or,
M7 T1 review Minor-1, its FIRST POSITIONAL argument -- pydantic's own `Field`
signature puts `default` in the first positional slot, so `Field("v", alias=...)`
is as legitimate as `Field(default="v", ...)`) IF that value is itself a string
literal, else `None` -- covering "no default at all" and "a default that isn't a
plain string" identically (both honestly `None`, per the brief's
`default: str | None`).

TRACKED LIMITATION (M7 T1 review Important-3) -- inherited model_config: real
pydantic INHERITS `model_config` (and therefore `env_prefix`) down a Settings class
hierarchy (`class ChildSettings(BaseServiceSettings)` where only the base carries
`SettingsConfigDict(env_prefix=...)` still derives prefixed env names at runtime).
This per-class, per-file harvest cannot see that: resolving it would need the
service-wide class hierarchy (base_exprs give only base-name TEXT, possibly defined
in another file entirely) plus MRO-ordered config merging -- deliberately out of
scope for the foundation task. A subclass whose own body has no `model_config` (and
whose fields have no explicit alias) therefore gets `env_name=None`, honestly --
never a guessed prefix -- and, per the env-gating above, its fields simply don't
participate in `field_by_name`. Pinned by
`test_inherited_env_prefix_not_seen_env_name_none_documented_limitation`.
Workarounds for real codebases: repeat `model_config` in the subclass, or put an
explicit `alias`/`validation_alias` on the field.

-- Enum semantics --

A class IS gated here: `any("Enum" in be for be in class_def.base_exprs)` (textual
substring -- catches `Enum`/`StrEnum`/`IntEnum`/`enum.Enum` alike; `Flag`/`IntFlag`
are out of scope, per the brief's literal "Enum/StrEnum" wording). Given that gate,
`enum_values` is the tuple of every OTHER class-body attribute's string-literal value,
in declaration order, IFF every one of them has a string-literal value -- a single
non-string (or valueless) member fails the whole class to `None` (honest: "this
doesn't look like a pure string enum, don't guess at eventually-inconsistent data").
A class passing the base gate but with NO class-body attributes at all never reaches
`enum_values` in the first place (see `harvest_class_attrs`'s early per-class skip
below) -- querying it reads as "not found" (`None`), same as never having been
indexed, not as a vacuous empty tuple; this module does not need to special-case
that distinction anywhere.

-- Class FQN (the index's key) --

`_class_fqn` reproduces `extractors/python_core.py`'s OWN structural qualified_name
formula byte-for-byte (`dotted_module_path + "." + ".".join(name for _, name in
nesting_chain)`) -- so a class harvested here and that same class's `NodeRec.
qualified_name` from python_core's own extraction always agree, without either
module depending on the other (the tiny nesting-chain walk is duplicated here on
purpose: `parsing/` sits BELOW `extractors/` in this codebase's layering --
`extractors/python_core.py` imports `parsing.facts`, never the reverse -- so reaching
into `extractors.python_core` for its `nesting_chain` helper would invert that
dependency for the sake of six lines).

-- Suffix matching --

`settings_field(class_fqn_suffix, ...)` / `enum_values(class_fqn_suffix)` match a
caller-given suffix against the RIGHTMOST dotted segments of an indexed class's FQN --
the same convention M6's base_class consumer idiom already established for matching
a shorter idiom-configured name against a longer actual FQN (`kafka_ext.py`'s
`_match_base_tier`: `fnmatchcase(qualified, pattern) or fnmatchcase(qualified, "*." +
pattern)`, reused verbatim here as `_matches_suffix`) -- so a caller MAY also pass a
glob pattern, though the common case is a plain dotted suffix
("app.config.services.ServiceSettings" or just "ServiceSettings"). Ambiguity (more
than one indexed class matches the SAME suffix) resolves to `None` for all three
query methods, not just `field_by_name` -- the brief only spells this out for
`field_by_name` ("уникальный по имени ... None при коллизии"), but the identical
"false match worse than absence" reasoning applies just as much to a short class
suffix matching two unrelated classes, so the same safety net is extended to
`settings_field`/`enum_values` too (see the ambiguous-suffix tests in
test_class_attrs.py).

-- Wiring / claims reuse (documented decision, per this task's own instructions) --

The index is assembled from CLAIMS (`staging.claims_for("class_attrs", service)`),
not an in-memory-only per-analyze-call structure: `pipeline/analyze.py`'s S5 pass
harvests per-file claims (`harvest_class_attrs`) and writes them via the EXISTING
`Staging.add_claims`/`claims_for` machinery (kind="class_attrs") -- claims are
already per-file-keyed and `delete_file_layer` already wipes them by relpath, so
incremental re-analyze (which only re-harvests the STALE file subset) gets
cross-run coherence for free: an unchanged file's claims simply survive untouched in
the `claims` table, and `build_class_attr_index(staging.claims_for(...))` reads the
WHOLE service's rows regardless of which relpaths this particular call re-harvested.
This needed NO schema bump (SCHEMA_VERSION untouched) and NO new staged table -- see
`build_class_attr_index`'s own docstring for why claims-shaped `list[dict]` (rather
than `ClassAttrFact`/`SettingsField` objects directly) is this module's one
assembly entry point, used identically for both the staging round trip and direct
(in-memory) construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase

from codegraph.core.ids import relpath_to_module
from codegraph.parsing.facts import ArgFact, ClassAttrFact, DefFact, FileFacts

_MODEL_CONFIG_ATTR = "model_config"
_ENV_PREFIX_KW = "env_prefix"
_ALIAS_KWARGS = ("alias", "validation_alias")
_DEFAULT_KW = "default"


@dataclass(frozen=True)
class SettingsField:
    """One pydantic-Settings-shaped class-body field. Field order is the brief's own
    contract (`class_fqn; field; default; env_name`) -- pinned positionally by
    test_settings_field_positional_order_matches_contract."""

    class_fqn: str
    field: str
    default: str | None
    env_name: str | None


@dataclass(frozen=True)
class ClassAttrIndex:
    """Service-wide: structural class FQN -> harvested settings fields / enum values.
    Built exclusively by `build_class_attr_index` (see module docstring for the
    claims-based assembly analyze.py's own wiring uses) -- the three query methods
    below are the only public contract; the three fields are this module's own
    assembly detail, freely constructible directly in tests (e.g. `ClassAttrIndex({},
    {}, {})` for an empty index)."""

    settings_by_class: dict[str, dict[str, SettingsField]]
    enums_by_class: dict[str, tuple[str, ...]]
    field_index: dict[str, SettingsField | None]

    def settings_field(self, class_fqn_suffix: str, field: str) -> SettingsField | None:
        matches = [
            fields[field]
            for class_fqn, fields in self.settings_by_class.items()
            if field in fields and _matches_suffix(class_fqn, class_fqn_suffix)
        ]
        return matches[0] if len(matches) == 1 else None

    def enum_values(self, class_fqn_suffix: str) -> tuple[str, ...] | None:
        matches = [
            values
            for class_fqn, values in self.enums_by_class.items()
            if _matches_suffix(class_fqn, class_fqn_suffix)
        ]
        return matches[0] if len(matches) == 1 else None

    def field_by_name(self, field: str) -> SettingsField | None:
        """T3's auto-anchor join key. ENV-GATED (M7 T1 review Important-4): only
        fields that actually CARRY an env_name participate -- see
        `build_class_attr_index`'s own inline comment for the full rationale. Among
        those, unique name -> the field; collision -> None (honest ambiguity); a
        name known only from env-less (DTO-shaped) fields -> None (never joinable)."""
        return self.field_index.get(field)


def _matches_suffix(fqn: str, suffix: str) -> bool:
    """Same "last segments" convention as M6 base_class matching (kafka_ext.py's
    `_match_base_tier` STATIC-tier check) -- reused verbatim."""
    return fnmatchcase(fqn, suffix) or fnmatchcase(fqn, "*." + suffix)


def _nesting_chain(defs: list[DefFact], d: DefFact) -> list[tuple[str, str]]:
    """Duplicates extractors/python_core.py's own `nesting_chain` (see module
    docstring for why: avoiding a parsing-depends-on-extractors layering inversion
    for six lines)."""
    chain = []
    cur = d
    while cur is not None:
        chain.append(("class" if cur.kind == "class" else "function", cur.name))
        cur = defs[cur.parent] if cur.parent is not None else None
    return list(reversed(chain))


def _class_fqn(relpath: str, defs: list[DefFact], d: DefFact) -> str:
    dotted = relpath_to_module(relpath)
    nesting = _nesting_chain(defs, d)
    return dotted + "." + ".".join(name for _, name in nesting)


def _kwargs_by_name(attr: ClassAttrFact) -> dict[str, ArgFact]:
    return {arg.keyword: arg for arg in (attr.call_args or [])}


def _env_prefix(attrs: list[ClassAttrFact]) -> str | None:
    model_config = next((a for a in attrs if a.name == _MODEL_CONFIG_ATTR), None)
    if model_config is None or model_config.call_args is None:
        return None
    for arg in model_config.call_args:
        if arg.keyword == _ENV_PREFIX_KW and arg.value_kind == "string":
            return arg.string_value
    return None


def _field_default_and_alias(attr: ClassAttrFact) -> tuple[str | None, str | None]:
    """(literal default if a plain string RHS or a `Field(...)` string default;
    literal alias/validation_alias if `Field(...)` carries one) for ONE class-body
    attribute. A call-shaped RHS (`call_args is not None`) is checked for
    `Field(...)`-style arguments; a plain string RHS falls back to `string_value`
    directly -- the two are mutually exclusive per ClassAttrFact's own construction
    (a call RHS never also sets `string_value`).

    M7 T1 review Minor-1: the call-shaped default is read from `default=` OR,
    failing that, the FIRST POSITIONAL argument -- pydantic's own `Field` signature
    is `Field(default=..., ...)` with `default` as the first positional slot, so
    `Field("orders.events", alias=...)` is exactly as legitimate a spelling as
    `Field(default="orders.events", ...)` (was silently lost before this fix)."""
    if attr.call_args is not None:
        by_kw = _kwargs_by_name(attr)
        default_arg = by_kw.get(_DEFAULT_KW)
        if default_arg is None:
            default_arg = next((a for a in attr.call_args if a.index == 0), None)
        default = (
            default_arg.string_value
            if default_arg is not None and default_arg.value_kind == "string"
            else None
        )
        alias_env = None
        for kw in _ALIAS_KWARGS:
            alias_arg = by_kw.get(kw)
            if alias_arg is not None and alias_arg.value_kind == "string":
                alias_env = alias_arg.string_value
                break
        return default, alias_env
    return attr.string_value, None


def _harvest_settings_fields(
    class_fqn: str, attrs: list[ClassAttrFact],
) -> dict[str, SettingsField]:
    env_prefix = _env_prefix(attrs)
    result: dict[str, SettingsField] = {}
    for a in attrs:
        if a.name == _MODEL_CONFIG_ATTR:
            continue
        default, alias_env = _field_default_and_alias(a)
        if alias_env is not None:
            env_name = alias_env
        elif env_prefix is not None:
            env_name = f"{env_prefix}{a.name}".upper()
        else:
            env_name = None
        result[a.name] = SettingsField(class_fqn, a.name, default, env_name)
    return result


def _harvest_enum_values(attrs: list[ClassAttrFact]) -> tuple[str, ...] | None:
    members = [a for a in attrs if a.name != _MODEL_CONFIG_ATTR]
    if not members or not all(a.string_value is not None for a in members):
        return None
    return tuple(a.string_value for a in members)  # type: ignore[misc]


def harvest_class_attrs(relpath: str, facts: FileFacts) -> list[dict]:
    """Per-file harvest -> JSON-serializable claim payloads (`{"class_fqn": ...,
    "fields": {name: {"default": ..., "env_name": ...}, ...}, "enum_values": [...] |
    None}`), one per class that has SOMETHING worth indexing -- a class with zero
    class-body attributes at all (the overwhelming majority of classes in any
    codebase) is skipped outright, not emitted as an empty claim. `pipeline/
    analyze.py`'s S5 pass calls this once per (stale, in incremental mode) file and
    feeds the result straight to `staging.add_claims(svc.name, rp, "class_attrs",
    ...)`.

    A class is EITHER an enum candidate (base_exprs says so) OR a settings-field
    candidate, never both -- gating on the enum base FIRST keeps enum members
    (`STEP_CHANGED = "..."` ) from also polluting `field_by_name`/`settings_field`
    as if they were pydantic fields."""
    claims: list[dict] = []
    for d in facts.defs:
        if d.kind != "class":
            continue
        attrs = [a for a in facts.class_attrs if a.enclosing_def == d.index]
        if not attrs:
            continue
        class_fqn = _class_fqn(relpath, facts.defs, d)
        if any("Enum" in be for be in d.base_exprs):
            fields: dict[str, SettingsField] = {}
            enum_values = _harvest_enum_values(attrs)
        else:
            fields = _harvest_settings_fields(class_fqn, attrs)
            enum_values = None
        if not fields and enum_values is None:
            continue
        claims.append({
            "class_fqn": class_fqn,
            "fields": {
                name: {"default": sf.default, "env_name": sf.env_name}
                for name, sf in fields.items()
            },
            "enum_values": list(enum_values) if enum_values is not None else None,
        })
    return claims


def build_class_attr_index(claims: list[dict]) -> ClassAttrIndex:
    """The one assembly entry point, source-agnostic: `claims` may come straight from
    `harvest_class_attrs` (in-memory, full or incremental analyze) or from
    `staging.claims_for("class_attrs", service)` (the actual analyze.py wiring,
    round-tripped through JSON + claims_for's own "_service"/"_relpath" injection --
    both silently ignored here, only "class_fqn"/"fields"/"enum_values" are read).

    Two claims sharing the same class_fqn (a rare, documented-elsewhere edge case --
    e.g. the SAME class name redefined in mutually-exclusive if/elif branches within
    one file, see extractors/python_core.py's own `_raw_id_occurrences` comment for
    the general phenomenon) simply merge, later claim's fields winning per-name on
    overlap -- no fixture needs anything more precise than that."""
    settings_by_class: dict[str, dict[str, SettingsField]] = {}
    enums_by_class: dict[str, tuple[str, ...]] = {}
    for claim in claims:
        class_fqn = claim["class_fqn"]
        fields = claim.get("fields") or {}
        if fields:
            settings_by_class.setdefault(class_fqn, {}).update({
                name: SettingsField(
                    class_fqn=class_fqn, field=name,
                    default=payload.get("default"), env_name=payload.get("env_name"),
                )
                for name, payload in fields.items()
            })
        enum_values = claim.get("enum_values")
        if enum_values is not None:
            enums_by_class[class_fqn] = tuple(enum_values)

    by_name_classes: dict[str, set[str]] = {}
    by_name_value: dict[str, SettingsField] = {}
    for class_fqn, fields in settings_by_class.items():
        for name, sf in fields.items():
            # M7 T1 review Important-4: the by-name join is ENV-GATED -- a field
            # carrying no env_name (DTO/model classes: no model_config, no alias)
            # never participates. T3's auto-anchor joins THROUGH env (self.host ->
            # field -> env_name -> env->service map), so an env-less field could
            # never anchor anything anyway -- excluding it here removes the whole
            # DTO collision-pollution surface (reviewer measured 2/6 field-name
            # collisions even on the tiny fixture set) without weakening the honest
            # collision->None policy among fields that ARE joinable. Per-class
            # lookups (`settings_field`) are deliberately NOT gated -- a caller that
            # already knows the class (T2's `settings:` source) legitimately wants
            # an env-less field's default too.
            if sf.env_name is None:
                continue
            by_name_classes.setdefault(name, set()).add(class_fqn)
            by_name_value[name] = sf
    field_index = {
        name: (by_name_value[name] if len(classes) == 1 else None)
        for name, classes in by_name_classes.items()
    }
    return ClassAttrIndex(settings_by_class, enums_by_class, field_index)
