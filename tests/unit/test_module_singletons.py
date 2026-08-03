"""module_singletons: service-wide index of module-level `name = ClassName(...)`
singleton assigns (M10 T1 -- pilot report §5, docs/superpowers/reports/
2026-08-03-mcp-pilot.md: `registry = _DBRegistry(config.database.dsn)` at module level
+ `registry.session()` call-sites elsewhere resolve to a node-less `module.attr`
symbol and get dropped -- 79/149 [53%] of the pilot's dropped CALLS, all this ONE
pattern). Mirrors class_attrs.py's own M7 T1 shape (harvest -> claims -> service-wide
index, claims-based assembly so incremental re-analyze gets cross-run coherence for
free) plus the extra join-candidate resolution class_attrs.py itself doesn't need
(settings/enum consumers read VALUES; this index's consumer, extractors/calls.py,
needs to build and VERIFY a candidate node id).

Synthetic sources throughout, same precedent as test_class_attrs.py/
test_kafka_extractor.py ("Synthetic sources cover branches no real fixture reaches")
-- no fixtures/services/* file has this pattern yet (brief: existing fixtures have NO
singleton pattern, M1 gate P/R stays byte-identical)."""

from __future__ import annotations

from codegraph.parsing.facts import build_file_facts
from codegraph.parsing.module_singletons import (
    SingletonDispatch,
    SingletonEntry,
    SingletonIndex,
    build_singleton_index,
    harvest_module_singletons,
    resolve_singleton_call,
)

_SVC = "scip-python python svc 0.1"
_DB_REGISTRY_CLASS_SYM = f"{_SVC} `app.db.registry`/_DBRegistry#"
_DB_REGISTRY_SESSION_SYM = f"{_SVC} `app.db.registry`/_DBRegistry#session()."


def _claims(relpath: str, src: bytes, lookup=None) -> list[dict]:
    facts = build_file_facts(relpath, src)
    return harvest_module_singletons(relpath, facts, lookup)


# -- harvest: module-level ctor-form assigns only --


def test_module_level_ctor_assign_produces_heuristic_claim_without_lookup():
    src = b"registry = _DBRegistry(config.database.dsn)\n"
    claims = _claims("app/db/registry.py", src, lookup=None)
    assert claims == [{
        "name": "registry",
        "class_symbol": None,
        "class_name_text": "_DBRegistry",
        "resolution_tier": "heuristic",
    }]


def test_module_level_ctor_assign_resolves_static_via_ref_lookup():
    src = b"registry = _DBRegistry(config.database.dsn)\n"
    facts = build_file_facts("app/db/registry.py", src)
    assign = next(a for a in facts.assigns if a.target == "registry")

    def lookup(rp, sb):
        return _DB_REGISTRY_CLASS_SYM if sb == assign.callee_start_byte else None

    claims = harvest_module_singletons("app/db/registry.py", facts, lookup)
    assert claims == [{
        "name": "registry",
        "class_symbol": _DB_REGISTRY_CLASS_SYM,
        "class_name_text": "_DBRegistry",
        "resolution_tier": "static",
    }]


def test_function_level_ctor_assign_produces_no_claim():
    src = b'''def build():
    registry = _DBRegistry(x)
    return registry
'''
    assert _claims("app/db/registry.py", src) == []


def test_class_body_ctor_assign_produces_no_claim():
    src = b'''class Container:
    registry = _DBRegistry(x)
'''
    assert _claims("app/db/registry.py", src) == []


def test_lowercase_callee_is_not_ctor_form_no_claim():
    """RHS ctor-form heuristic (brief: last callee segment capitalized, mirrors
    idiom_match._is_ctor_pattern's "CamelCase-class" convention): a lowercase callee
    (a plain factory FUNCTION, not a class) never produces a singleton claim."""
    src = b"producer = make_producer(x)\n"
    assert _claims("app/x.py", src) == []


def test_leading_underscore_private_class_is_still_ctor_form():
    """The pilot's OWN motivating example (`_DBRegistry`, docs/superpowers/reports/
    2026-08-03-mcp-pilot.md §5) is a "private" class -- leading-underscore Python
    convention. A literal `s[0].isupper()` (idiom_match._is_ctor_pattern's own check)
    would misclassify it as non-ctor-form; harvest strips leading underscores first."""
    src = b"registry = _DBRegistry(x)\n"
    claims = _claims("app/db/registry.py", src)
    assert len(claims) == 1 and claims[0]["class_name_text"] == "_DBRegistry"


def test_leading_underscore_private_function_is_not_ctor_form():
    """The underscore-stripping is about CASE, not privacy -- a private snake_case
    factory FUNCTION is still correctly excluded."""
    src = b"client = _make_client(x)\n"
    assert _claims("app/x.py", src) == []


def test_non_call_rhs_produces_no_claim():
    """A plain literal/attribute RHS never even produces an AssignFact (facts.py's
    own contract) -- module_singletons has nothing to harvest."""
    src = b"NAME = 'literal'\n"
    assert _claims("app/x.py", src) == []


def test_annotated_module_level_ctor_assign_still_produces_claim():
    """Step 1 finding (brief: 'read AssignFact capabilities'): AssignFact fires for
    `name: Type = Callee(...)` too -- the `type` field is simply not consulted by its
    own construction site in facts.py. Confirms module_singletons harvest sees an
    annotated singleton assign exactly like an unannotated one."""
    src = b"registry: DBRegistry = _DBRegistry(config.database.dsn)\n"
    claims = _claims("app/db/registry.py", src, lookup=None)
    assert claims == [{
        "name": "registry",
        "class_symbol": None,
        "class_name_text": "_DBRegistry",
        "resolution_tier": "heuristic",
    }]


def test_dotted_ctor_callee_uses_last_segment_as_class_name_text():
    src = b"registry = db_mod.DBRegistry(x)\n"
    claims = _claims("app/x.py", src, lookup=None)
    assert claims[0]["class_name_text"] == "DBRegistry"


def test_class_ref_resolving_to_local_symbol_falls_back_to_heuristic():
    """A same-file class reference pyright degrades to `local N` (M1b precedent,
    calls.py's own module docstring) cannot be turned into a cross-service-stable
    descriptor (local ids are file-scoped, `sym:<svc>:<relpath>:<local>` -- no room to
    append a method segment) -- honest fallback to the textual tier, never a crash."""
    src = b"registry = _DBRegistry(x)\n"
    claims = _claims("app/db/registry.py", src, lookup=lambda rp, sb: "local 3")
    assert claims[0]["resolution_tier"] == "heuristic"
    assert claims[0]["class_symbol"] is None


def test_multiple_module_level_singletons_in_one_file():
    src = b'''registry = _DBRegistry(x)
cache = _Cache(y)
'''
    claims = _claims("app/db/registry.py", src, lookup=None)
    assert {c["name"] for c in claims} == {"registry", "cache"}


def test_factory_function_resolved_symbol_falls_back_to_heuristic_claim():
    """M10 review Important-2: `pool = MakePool(x)` where MakePool is a CapWords
    FACTORY FUNCTION -- the ctor-form heuristic can't tell (CamelCase text is all it
    sees), but the ref lookup CAN: the resolved symbol's descriptors end "()."
    (function), not "#" (class). Trusting it as a static claim would let the join
    build `MakePool().<method>().` -- which can GENUINELY exist as a staged def (a
    nested function inside the factory), a real false-positive path (see the
    resolve_singleton_call pin below). Harvest must fall back to the heuristic tier
    (class_symbol=None), whose textual scan is structurally class-shape-safe (its
    suffix contains "#" -- see _heuristic_candidate's own docstring)."""
    src = b"pool = MakePool(x)\n"
    facts = build_file_facts("app/pools.py", src)
    assign = next(a for a in facts.assigns if a.target == "pool")
    factory_fn_sym = f"{_SVC} `app.factories`/MakePool()."

    def lookup(rp, sb):
        return factory_fn_sym if sb == assign.callee_start_byte else None

    claims = harvest_module_singletons("app/pools.py", facts, lookup)
    assert claims == [{
        "name": "pool",
        "class_symbol": None,
        "class_name_text": "MakePool",
        "resolution_tier": "heuristic",
    }]


# -- SingletonEntry / SingletonIndex: assembly + collisions --


def test_build_singleton_index_static_tier_lookup():
    claims = [{
        "name": "registry", "class_symbol": _DB_REGISTRY_CLASS_SYM,
        "class_name_text": "_DBRegistry", "resolution_tier": "static",
    }]
    idx = build_singleton_index(claims)
    assert idx.get("registry") == SingletonEntry(
        name="registry", class_symbol=_DB_REGISTRY_CLASS_SYM,
        class_name_text="_DBRegistry", resolution_tier="static",
    )


def test_build_singleton_index_unknown_name_is_none():
    idx = build_singleton_index([])
    assert idx.get("nope") is None


def test_build_singleton_index_collision_across_files_resolves_to_none():
    """Two DIFFERENT files each define a module-level singleton under the SAME
    variable name but a DIFFERENT class -- ambiguous, "false match worse than no
    match" (same policy as ClassAttrIndex.field_by_name, class_attrs.py)."""
    claims = [
        {"name": "registry", "class_symbol": None, "class_name_text": "_DBRegistry",
         "resolution_tier": "heuristic"},
        {"name": "registry", "class_symbol": None, "class_name_text": "_OtherRegistry",
         "resolution_tier": "heuristic"},
    ]
    idx = build_singleton_index(claims)
    assert idx.get("registry") is None


def test_build_singleton_index_duplicate_identical_claim_not_a_collision():
    """The SAME claim harvested twice (e.g. re-run on an unchanged file under
    incremental analyze) is not an ambiguity -- mirrors build_class_attr_index's own
    "merge, no fixture needs anything more precise" precedent."""
    claim = {"name": "registry", "class_symbol": _DB_REGISTRY_CLASS_SYM,
              "class_name_text": "_DBRegistry", "resolution_tier": "static"}
    idx = build_singleton_index([claim, dict(claim)])
    assert idx.get("registry") is not None


# -- resolve_singleton_call: join-time candidate resolution (calls.py's consumer) --


def _def_symbols(*extra: str) -> set[str]:
    return {_DB_REGISTRY_SESSION_SYM, *extra}


def test_resolve_singleton_call_static_tier_builds_method_node_id_when_method_exists():
    idx = build_singleton_index([{
        "name": "registry", "class_symbol": _DB_REGISTRY_CLASS_SYM,
        "class_name_text": "_DBRegistry", "resolution_tier": "static",
    }])
    dispatch = resolve_singleton_call(idx, "registry", "session", _def_symbols(), "svc")
    assert dispatch == SingletonDispatch(
        dst_id="sym:svc:`app.db.registry`/_DBRegistry#session().",
        resolution="static", confidence=1.0,
    )


def test_resolve_singleton_call_static_tier_method_does_not_exist_never_guesses():
    idx = build_singleton_index([{
        "name": "registry", "class_symbol": _DB_REGISTRY_CLASS_SYM,
        "class_name_text": "_DBRegistry", "resolution_tier": "static",
    }])
    dispatch = resolve_singleton_call(
        idx, "registry", "nonexistent_method", _def_symbols(), "svc",
    )
    assert dispatch is None


def test_resolve_singleton_call_static_tier_function_shaped_class_symbol_refused():
    """M10 review Important-2, the defensive join-side twin of the harvest-side pin
    above (claims are also built directly, not only by harvest_module_singletons):
    a claim whose class_symbol is FUNCTION-shaped (descriptors end "().", not "#")
    must be refused even when the would-be candidate `MakePool().session().`
    ACTUALLY EXISTS in def_symbols -- a nested `def session()` inside the factory is
    a real, stageable def, so without the shape check this would join a
    `pool.session()` call to the factory's inner helper with static/1.0 confidence:
    the strongest false-positive form of this bug, not just a mislabeled tier."""
    factory_fn_sym = f"{_SVC} `app.factories`/MakePool()."
    nested_fn_sym = f"{_SVC} `app.factories`/MakePool().session()."
    idx = build_singleton_index([{
        "name": "pool", "class_symbol": factory_fn_sym,
        "class_name_text": "MakePool", "resolution_tier": "static",
    }])
    dispatch = resolve_singleton_call(idx, "pool", "session", {nested_fn_sym}, "svc")
    assert dispatch is None


def test_resolve_singleton_call_heuristic_tier_builds_method_node_id_via_textual_scan():
    idx = build_singleton_index([{
        "name": "registry", "class_symbol": None,
        "class_name_text": "_DBRegistry", "resolution_tier": "heuristic",
    }])
    dispatch = resolve_singleton_call(idx, "registry", "session", _def_symbols(), "svc")
    assert dispatch == SingletonDispatch(
        dst_id="sym:svc:`app.db.registry`/_DBRegistry#session().",
        resolution="heuristic", confidence=0.6,
    )


def test_resolve_singleton_call_heuristic_tier_ambiguous_class_name_never_guesses():
    """Two DIFFERENT classes across the service share the simple name `_DBRegistry`
    -- the textual tier cannot tell which one the singleton actually is, so it
    refuses rather than pick one (NEVER GUESS, brief's own binding phrase)."""
    other_sym = f"{_SVC} `app.other`/_DBRegistry#session()."
    idx = build_singleton_index([{
        "name": "registry", "class_symbol": None,
        "class_name_text": "_DBRegistry", "resolution_tier": "heuristic",
    }])
    dispatch = resolve_singleton_call(
        idx, "registry", "session", _def_symbols(other_sym), "svc",
    )
    assert dispatch is None


def test_resolve_singleton_call_unknown_receiver_returns_none():
    idx = build_singleton_index([])
    assert resolve_singleton_call(idx, "not_a_singleton", "session", _def_symbols(), "svc") is None


def test_resolve_singleton_call_ambiguous_index_entry_returns_none():
    """A receiver name the INDEX itself already collapsed to None (cross-file
    collision, see the build_singleton_index test above) never reaches candidate
    resolution at all."""
    claims = [
        {"name": "registry", "class_symbol": None, "class_name_text": "_DBRegistry",
         "resolution_tier": "heuristic"},
        {"name": "registry", "class_symbol": None, "class_name_text": "_OtherRegistry",
         "resolution_tier": "heuristic"},
    ]
    idx = build_singleton_index(claims)
    assert resolve_singleton_call(idx, "registry", "session", _def_symbols(), "svc") is None


# -- claims round trip: staging-assembled index matches direct (in-memory) construction --


def test_claims_round_trip_via_staging_matches_direct_construction(tmp_path):
    from codegraph.stores.staging import Staging

    src = b"registry = _DBRegistry(config.database.dsn)\n"
    facts = build_file_facts("app/db/registry.py", src)
    direct_claims = harvest_module_singletons("app/db/registry.py", facts, None)
    direct_index = build_singleton_index(direct_claims)

    st = Staging(tmp_path / "s.db")
    st.add_claims("svc-a", "app/db/registry.py", "module_singletons", direct_claims)
    staged_claims = st.claims_for("module_singletons", "svc-a")
    staged_index = build_singleton_index(staged_claims)

    assert staged_index == direct_index
    assert staged_index.get("registry") == SingletonEntry(
        name="registry", class_symbol=None, class_name_text="_DBRegistry",
        resolution_tier="heuristic",
    )


def test_claims_for_injected_service_relpath_keys_are_ignored_by_the_builder():
    claims = _claims("app/db/registry.py", b"registry = _DBRegistry(x)\n")
    decorated = [{**c, "_service": "svc-a", "_relpath": "app/db/registry.py"} for c in claims]
    idx = build_singleton_index(decorated)
    assert idx.get("registry").class_name_text == "_DBRegistry"


# -- SingletonIndex is a real dataclass (equality, direct construction) --


def test_singleton_index_direct_construction_and_equality():
    assert SingletonIndex({}) == SingletonIndex({})
