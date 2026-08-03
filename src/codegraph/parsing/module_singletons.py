"""module_singletons: service-wide index of module-level `name = ClassName(...)`
singleton assigns -- M10 T1, the MCP-pilot's biggest single graph win (docs/
superpowers/reports/2026-08-03-mcp-pilot.md §5): `registry = _DBRegistry(config.
database.dsn)` at module level, then `registry.session()` call-sites elsewhere,
resolve to a node-less `module.attr` symbol (scip can't connect the attribute access
back to `_DBRegistry`'s own `session` method through the module-level instance
binding) and get silently dropped at load -- 79 of 149 dropped CALLS (53%) on the
pilot's real corpus, all this ONE pattern. Hedge (T5's own raw-scip-occurrence
finding, fixtures/realstack/services/worker/app/services/doc_store.py's own
docstring): this scip limitation is narrower in simple, self-contained synthetics
than the phrasing above suggests -- a minimal two-file case resolves straight
through the module-level binding with no drop at all; real-world messiness (a
gradual-typing `: Any` escape hatch, confirmed empirically) is what still triggers
it.

Mirrors class_attrs.py's own M7 T1 shape almost exactly (harvest -> per-file claims
kind="module_singletons" -> service-wide index assembled from `staging.claims_for`,
so incremental re-analyze gets cross-run coherence for free -- see that module's own
docstring for the full claims-reuse argument, identical here) with one addition
class_attrs.py's consumers (T2/T3, settings/enum VALUE lookups) never needed: this
index's own consumer (extractors/calls.py) has to build a CANDIDATE node id from the
index and VERIFY it actually exists among this service's staged defs before ever
using it as an edge's dst -- `resolve_singleton_call` is that verification, kept
here (not in calls.py) so the two symbol-reconstruction tiers (static/heuristic) stay
next to the SingletonEntry shape they read.

-- Harvest (module-level ctor-form assigns only) --

A module-level `name = ClassName(...)` assign (`AssignFact.enclosing_def is None` --
M10 T1 sanctioned extension, parsing/facts.py) is CTOR-FORM iff its callee's last
segment, with any LEADING UNDERSCORES stripped, starts with an uppercase letter
(`_is_ctor_form` -- the same "CamelCase class, not a snake_case method/function"
heuristic `idiom_match._is_ctor_pattern` already applies to idiom-configured
patterns, EXTENDED by the underscore-stripping: the pilot's own motivating example
is `_DBRegistry` -- a "private" class, leading-underscore Python convention, which
`_is_ctor_pattern`'s literal `s[0].isupper()` would misclassify as non-ctor-form and
silently harvest NOTHING for the pilot's #1 real-world case. `idiom_match`'s own
version never needed this: it checks a USER-WRITTEN idiom pattern segment (e.g.
"aiokafka.AIOKafkaConsumer"), never a real codebase's own private-by-convention class
name. Duplicated here as a one-liner rather than imported, since idiom_match's
version operates on pattern SEGMENTS, not a decoded AssignFact.callee_name, and this
module has no other reason to depend on idiom_match). `AssignFact.callee_name` is
already reduced to the callee's LAST segment (facts.py's own `_call_callee_name`
convention, identical for a bare identifier or a dotted chain), so
`harvest_module_singletons` never needs to see the receiver/module prefix at all --
`db_mod.DBRegistry(x)` and `DBRegistry(x)` harvest identically.

Function/class-body ctor assigns (`enclosing_def is not None`) are skipped outright:
the pilot's own pattern, and the join this index feeds (extractors/calls.py), is
specifically about a NAME reachable from OTHER FILES via a bare module-level
binding -- a local variable inside a function has no such cross-file reach, and
idiom_match's own RECEIVER tier already covers the same-file, any-scope case for
idiom-configured call patterns.

`AssignFact` fires for an ANNOTATED assign too (`registry: DBRegistry =
_DBRegistry(...)`)  -- its own construction site in facts.py never consults the
`type` field at all -- so this harvester needs no special-casing for that shape.

-- ClassName resolution: static (scip/structural) tier, then textual fallback --

`ref_symbol_lookup(relpath, assign.callee_start_byte)` (a REFERENCE-occurrence
lookup, same shape `ctx.ref_symbol_lookup` already provides every FileContext,
M2 T4) is tried FIRST -- this resolves in BOTH real-scip and degraded/heuristic-
fallback analyze modes alike (resolvers/fallback.py's own `resolve_service` stages a
proper structural ref for a same-file or from-imported class ctor reference too, the
identical "scip-python python <svc> 0.0 <descriptor>" shape a real scip run
produces), so "static tier" here means "THIS occurrence resolved", not "real scip was
running" (the RUN-level honesty ladder is enforced separately, at edge emission --
see the join-time paragraph below). A LOCAL symbol (`parsed.is_local`) is honestly
rejected even when found -- a `local N` symbol has no descriptors to append a method
segment to (ids are file-scoped: `sym:<svc>:<relpath>:<local>`) and there is no
sound way to widen it service-wide. A NON-CLASS-shaped symbol is rejected too (M10
review Important-2, `_is_class_symbol`: descriptors must end "#") -- the ctor-form
heuristic only sees CamelCase TEXT, so `pool = MakePool(x)` with a CapWords factory
FUNCTION passes it and resolves to `.../MakePool().` here; trusting that as static
would let the join build `MakePool().<method>().`, which can GENUINELY exist as a
staged def (a nested function inside the factory) -- a real false-positive join
target the def-existence check alone cannot catch. Resolution failure (no lookup
wired, lookup returns None, a local symbol, or a non-class-shaped symbol) falls
back to the TEXTUAL tier: `class_name_text` (the ctor callee's own
decoded text, ALWAYS carried in the claim regardless of tier) is all a join-time
consumer gets to work with -- `resolve_singleton_call`'s heuristic branch scans this
service's OWN `def_symbols` for a UNIQUE class body ending `<class_name_text>#
<method>().`; more than one match (two same-named classes anywhere in the service)
is an honest ambiguity -> None, "false match worse than no match" (the exact same
policy class_attrs.py's `field_by_name`/`settings_field`/`enum_values` already
apply to their own suffix collisions). The textual tier needs no separate
class-shape check: its suffix literally contains the class-descriptor "#", so a
factory function's own (or nested) descriptors structurally cannot match -- see
`_heuristic_candidate`'s docstring.

-- Claims / index assembly --

Claim shape (JSON-serializable, per module docstring convention `class_attrs.py`
already established): `{"name": str, "class_symbol": str | None, "class_name_text":
str | None, "resolution_tier": "static" | "heuristic"}`. `build_singleton_index`
reads ONLY these four keys via `.get`/`[...]` (never `**claim` unpacking), so it is
unaffected by `staging.claims_for`'s own injected "_service"/"_relpath" metadata
keys, mirroring `build_class_attr_index`'s identical tolerance.

Collision policy: two claims sharing the same `name` but a DIFFERENT identity
(`(class_symbol, class_name_text)`) -- e.g. two unrelated files each defining a
module-level singleton under the same variable name -- resolve to `None` for that
name (never guess which one a THIRD file's `name.method()` call meant). Repeated
IDENTICAL claims (the same file re-harvested, e.g. across incremental re-runs) are
not a collision -- the identity set collapses to size 1, same "merge, no fixture
needs anything more precise" precedent as `build_class_attr_index`.

-- Join-time candidate resolution (extractors/calls.py's own consumer) --

`resolve_singleton_call(index, receiver, method, def_symbols, service)` combines an
indexed `SingletonEntry` with the ACTUAL call site's own method name (only known at
the call site, never at harvest time) into a `SingletonDispatch(dst_id, resolution,
confidence)` -- or `None` if nothing verifiable was found, in which case the caller's
existing (pre-M10) behavior is completely unchanged (NEVER GUESS, the brief's own
binding phrase: a candidate this module cannot verify against `def_symbols` is never
handed back, not even as a low-confidence guess). `def_symbols` is exactly the
service-wide `def_symbols` set M5 Task 1 already introduced (extractors/calls.py's
own module docstring) -- reused here as-is, no new staged table/index.

Static-tier verification re-checks class shape (`_is_class_symbol`, the defensive
twin of the harvest-side gate -- claims also reach this code constructed directly),
then reconstructs the FULL scip symbol string for the candidate method (same
scheme/manager/package/version as the resolved class_symbol, itself a real, staged,
verified symbol -- never invented) and checks it against `def_symbols` membership,
exactly the same "def-existence, not just parseable" criterion `extractors/calls.py
::build_calls` already uses for its own first-party join decision. Success
confidence is a fixed (static, 1.0)/(heuristic, 0.6) pair keyed by which tier
resolved the CLASS name; the RUN-level honesty ladder is NOT this module's concern
-- `build_calls` itself caps a dispatch at its own base resolution/confidence (M10
review Important-1: the M1a ladder is a PROVENANCE rule -- a degraded run's S6 CALLS
are heuristic/0.6 because the fallback resolver produced them, so no redirect may
exceed the run's base tier; see the cap comment in extractors/calls.py)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from codegraph.core import ids
from codegraph.parsing.facts import FileFacts
from codegraph.resolvers.scip.symbols import ParsedSymbol, parse_symbol

ResolutionTier = Literal["static", "heuristic"]


@dataclass(frozen=True)
class SingletonEntry:
    """One unambiguous module-level singleton, indexed by its module-level variable
    `name`. `class_symbol` is the raw scip/structural symbol string of the ctor'd
    class when the static tier resolved it (`None` for a pure textual-tier entry);
    `class_name_text` is the ctor callee's own decoded text, ALWAYS present
    (harvested regardless of tier) -- the join-time textual fallback's only input."""

    name: str
    class_symbol: str | None
    class_name_text: str | None
    resolution_tier: ResolutionTier


@dataclass(frozen=True)
class SingletonDispatch:
    """A VERIFIED join candidate -- `resolve_singleton_call`'s only return shape
    besides `None`. `dst_id` is a real node id this module confirmed exists among
    `def_symbols` (never a guess); `resolution`/`confidence` are ready-to-use EdgeRec
    values, keyed by which tier resolved the singleton's OWN class (see module
    docstring)."""

    dst_id: str
    resolution: str
    confidence: float


@dataclass(frozen=True)
class SingletonIndex:
    """Service-wide: module-level variable name -> its unambiguous SingletonEntry
    (`None` for a name no claim carries, OR a name multiple DIFFERENT claims
    collided on -- see module docstring). Built exclusively by
    `build_singleton_index`; `get` is the one query method extractors/calls.py's own
    per-call-site lookup needs."""

    by_name: dict[str, SingletonEntry | None]

    def get(self, name: str) -> SingletonEntry | None:
        return self.by_name.get(name)


def _is_ctor_form(name: str) -> bool:
    """"CamelCase class, not snake_case function" heuristic, mirrors
    `idiom_match._is_ctor_pattern` but strips LEADING UNDERSCORES first -- a
    "private" class (`_DBRegistry`, the pilot's own motivating example) is still
    ctor-form; a private snake_case factory function (`_make_client`) is not. See
    module docstring for why this deviates from `_is_ctor_pattern`'s literal
    `s[0].isupper()`."""
    stripped = name.lstrip("_")
    return bool(stripped) and stripped[0].isupper()


def _is_class_symbol(parsed: ParsedSymbol) -> bool:
    """M10 review Important-2: a symbol the static tier may trust as a CLASS --
    non-local, with descriptors ending "#" (scip's own class-descriptor suffix,
    mirrored by `ids.structural_descriptor` and every def-id path). A CapWords
    FACTORY FUNCTION (`.../MakePool().`) resolves via the same ref lookup but is
    NOT class-shaped -- appending a method segment to it would produce a
    nested-function descriptor (`MakePool().session().`), which can genuinely be a
    staged def, i.e. a real false-positive join target, not just a mislabeled
    tier."""
    return (
        not parsed.is_local
        and parsed.descriptors is not None
        and parsed.descriptors.endswith("#")
    )


def harvest_module_singletons(
    relpath: str,
    facts: FileFacts,
    ref_symbol_lookup: Callable[[str, int], str | None] | None,
) -> list[dict]:
    """Per-file harvest -> JSON-serializable claim payloads, one per module-level
    ctor-form assign (see module docstring for the full shape/tier contract).
    `pipeline/analyze.py`'s S5 pre-loop pass calls this once per (stale, in
    incremental mode) file, mirroring `harvest_class_attrs`'s own calling
    convention exactly -- the one difference is this harvester also needs
    `ref_symbol_lookup` (class-name resolution), which `harvest_class_attrs` never
    required."""
    claims: list[dict] = []
    for a in facts.assigns:
        if a.enclosing_def is not None:
            continue  # not module-level -- the pilot pattern is module-scope only
        if a.callee_name is None or not _is_ctor_form(a.callee_name):
            continue
        class_symbol: str | None = None
        if ref_symbol_lookup is not None and a.callee_start_byte is not None:
            sym = ref_symbol_lookup(relpath, a.callee_start_byte)
            # M10 review Important-2: shape-gate the static tier -- the ctor-form
            # heuristic only ever sees CamelCase TEXT, so a CapWords factory
            # FUNCTION passes it and resolves here to a function symbol; only a
            # genuinely class-shaped symbol (`_is_class_symbol`) earns a static
            # claim, anything else falls back to the textual tier honestly.
            if sym is not None and _is_class_symbol(parse_symbol(sym)):
                class_symbol = sym
        claims.append({
            "name": a.target,
            "class_symbol": class_symbol,
            "class_name_text": a.callee_name,
            "resolution_tier": "static" if class_symbol is not None else "heuristic",
        })
    return claims


def build_singleton_index(claims: list[dict]) -> SingletonIndex:
    """The one assembly entry point, source-agnostic (mirrors
    `build_class_attr_index`'s own docstring precedent): `claims` may come straight
    from `harvest_module_singletons` (in-memory) or `staging.claims_for
    ("module_singletons", service)` (the real analyze.py wiring) -- both funnel
    through this same function, reading only "name"/"class_symbol"/
    "class_name_text"/"resolution_tier", so `claims_for`'s injected "_service"/
    "_relpath" keys are silently ignored."""
    by_name_identities: dict[str, set[tuple[str | None, str | None]]] = {}
    by_name_entry: dict[str, SingletonEntry] = {}
    for claim in claims:
        name = claim["name"]
        class_symbol = claim.get("class_symbol")
        class_name_text = claim.get("class_name_text")
        identity = (class_symbol, class_name_text)
        by_name_identities.setdefault(name, set()).add(identity)
        by_name_entry[name] = SingletonEntry(
            name=name, class_symbol=class_symbol, class_name_text=class_name_text,
            resolution_tier=claim["resolution_tier"],
        )
    by_name: dict[str, SingletonEntry | None] = {
        name: (by_name_entry[name] if len(identities) == 1 else None)
        for name, identities in by_name_identities.items()
    }
    return SingletonIndex(by_name)


def _static_candidate(
    class_symbol: str, method: str, def_symbols: set[str], service: str,
) -> SingletonDispatch | None:
    parsed = parse_symbol(class_symbol)
    # M10 review Important-2: `_is_class_symbol` here is the DEFENSIVE twin of the
    # harvest-side gate (harvest_module_singletons already refuses to write a
    # static claim for a non-class-shaped symbol) -- claims also reach this code
    # constructed directly (tests, any future claim producer), and appending
    # `<method>().` to a FUNCTION-shaped descriptor builds a nested-function id
    # that can genuinely exist in def_symbols, so the def-existence check alone
    # cannot catch it.
    if not _is_class_symbol(parsed):
        return None
    descriptors = f"{parsed.descriptors}{method}()."
    candidate_symbol = (
        f"{parsed.scheme} {parsed.manager} {parsed.package} {parsed.version} {descriptors}"
    )
    if candidate_symbol not in def_symbols:
        return None  # NEVER GUESS -- the class has no such method staged
    return SingletonDispatch(ids.node_id(service, descriptors), "static", 1.0)


def _heuristic_candidate(
    class_name_text: str, method: str, def_symbols: set[str], service: str,
) -> SingletonDispatch | None:
    """Textual tier. Class shape needs NO extra check here (M10 review Important-2,
    documented decision): the scanned suffix `<class_name_text>#<method>().`
    CONTAINS the class-descriptor "#" itself, so it can only ever match a method
    inside a genuine class body -- a CapWords factory function's descriptors
    (`MakePool().`, and its nested defs' `MakePool().session().`) structurally
    cannot end with `MakePool#session().`. The shape requirement is enforced by the
    suffix format, not risk-accepted."""
    suffix = f"{class_name_text}#{method}()."
    matches: set[str] = set()
    for sym in def_symbols:
        parsed = parse_symbol(sym)
        if parsed.is_local or parsed.descriptors is None:
            continue
        if parsed.descriptors.endswith(suffix):
            i = len(parsed.descriptors) - len(suffix) - 1
            if i >= 0 and parsed.descriptors[i] in "/#":
                matches.add(parsed.descriptors)
    if len(matches) != 1:
        return None  # not found, or ambiguous (2+ same-named classes) -- never guess
    return SingletonDispatch(ids.node_id(service, next(iter(matches))), "heuristic", 0.6)


def resolve_singleton_call(
    index: SingletonIndex,
    receiver: str,
    method: str,
    def_symbols: set[str],
    service: str,
) -> SingletonDispatch | None:
    """extractors/calls.py's own join-time hook: `receiver` is a call site's bare-name
    receiver text (e.g. "registry" in `registry.session()`), `method` its callee name
    (e.g. "session"). `None` whenever the receiver isn't a known, UNAMBIGUOUS
    singleton, or a candidate was built but could not be verified against
    `def_symbols` -- the caller's existing (pre-M10) resolution path is then left
    completely untouched."""
    entry = index.get(receiver)
    if entry is None:
        return None
    if entry.resolution_tier == "static" and entry.class_symbol is not None:
        return _static_candidate(entry.class_symbol, method, def_symbols, service)
    if entry.class_name_text is not None:
        return _heuristic_candidate(entry.class_name_text, method, def_symbols, service)
    return None
