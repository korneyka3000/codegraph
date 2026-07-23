"""http_client_ext: aiohttp client-SDK extractor -- http_call claims (M2 T6).

Scope (data-driven, like kafka_ext -- NOT structural like fastapi_ext/temporal_ext):
for each `HttpClientIdiom` in the effective `ServiceIdioms.http_clients`, a file is in
scope if its relpath matches `idiom.file_glob` (fnmatchcase); within that file, a call
is in scope if it sits directly inside a method (any function def, however deeply
nested -- e.g. a local helper closure inside a method is still "inside") whose own
DefFact.parent chain eventually reaches a class matching `idiom.class_glob`. A
module-level call, or a call made directly in a class body (not inside any method at
all), is out of scope -- see `_enclosing_method_and_class`.

Call pattern: an attribute-call (`call.receiver_text is not None`) whose callee is one
of the 5 tracked HTTP verbs (`get`/`post`/`put`/`delete`/`patch` -- no head/options,
unlike fastapi_ext's route-decorator verb set). The brief's own initial wording floated
gating on the receiver text containing "session" (aiohttp's common `session.get(...)`
convention); self-corrected in review to "receiver_text любой" (ANY) once "s.post(...)"
was considered (its receiver "s" doesn't even contain the substring "session") -- real
aiohttp client code binds the session under all sorts of names (`session`, `s`,
`self._session`, ...), so this extractor does NOT filter on the receiver's literal
text at all, only on it being present (an attribute-call, as opposed to a bare
module-level `get(...)`).

arg0 resolution goes through `consts.resolve_arg`. The `ConstTable` arrives as a
parameter (same position as kafka_ext's `extract_kafka`), built ONCE per file by
analyze.py's S5 wiring and shared between both consts-consuming extractors -- T6 review
fix: the original 3-parameter signature built its own ConstTable internally, re-parsing
the file's top-level assignments a second time whenever kafka was active on the same
file. Three accepted shapes, see `_resolve_path`:
  - fstring template with a LEADING interpolation (`resolve_arg`'s own `<base>` marker,
    e.g. `f"{self._base_url}/documents/{doc_id}"` -> `"<base>/documents/{doc_id}"`) ->
    path = the tail after the marker, resolution_hint "static" (the base_url anchor
    makes this a deterministic template).
  - a plain string/const-resolved literal starting with "/" -> path = value as-is,
    "static" (a literal is as deterministic as it gets).
  - an fstring template with NO leading interpolation but still starting with "/" (e.g.
    `f"/documents/{doc_id}"`, or a bare `f"/documents"` with no interpolation at all)
    -> path = value as-is, "heuristic" -- lower confidence than the two cases above:
    there's no base_url evidence tying this call to the idiom's declared base_url at
    all, just a string that happens to look like an absolute path.
  - anything else (config_ref, unresolved, or a template/value NOT starting with "/")
    -> `stats["http_url_unresolved"] += 1`, no claim.

IMPORTANT documented assumption on `<base>` (carried forward from a T2 review note):
`resolve_arg`'s `<base>` marker means "this fstring's FIRST interpolation had no
literal text before it" -- purely a syntactic fact about the f-string, NOT a semantic
guarantee that the interpolated expression actually IS `idiom.base_url.attr` (or even
`self`-attribute-shaped at all -- `f"{some_unrelated_var}/x"` also produces a leading
`<base>` marker). This extractor accepts `<base>` as "this is the client's base_url"
ONLY because the call already had to pass the file_glob + class_glob scope gate above
(i.e. we are already confident this is a method of a class the idiom identifies as an
HTTP client SDK) -- treating a leading interpolation as base_url is a reasonable
approximation *in that scope*, not a general truth `resolve_arg` itself asserts. Outside
a matched client class, resolve_arg's `<base>` marker still just means "leading
interpolation", nothing more.

No SCIP dependency at all: unlike fastapi_ext's DEPENDS_ON, kafka_ext's STATIC tier, or
temporal_ext's INVOKES_ACTIVITY/start_workflow, this extractor never calls
`ctx.ref_symbol_lookup` -- everything it needs (file/class/def structure, call verb,
arg0 text) comes from T2's own structural facts + `consts.resolve_arg`'s partial
constant evaluation. It therefore resolves identically under the degraded fallback
path and a real SCIP run (proven by test_pipeline_analyze.py's wiring test using the
`_AlwaysFailRunner`, no stubbing needed -- a first for a T4-T6 domain extractor).

Cross-idiom dedup mirrors kafka_ext's own documented producer dedup (progress.md carry-
forward, T5's own self-review item): `idioms.http_clients` is walked in list order and
each call's `callee_start_byte` is claimed by at most the FIRST idiom whose file_glob +
class_glob scope matches it; a later idiom silently skips an already-claimed call. List
order is therefore load-bearing, and `config.loader.effective_idioms` deliberately
places a service's OWN idioms BEFORE the builtins (T6 review fix -- service idioms
shadow builtin conventions): a real merged run over kyc-worker (custom `default-sdk`
idiom with base_url_env=DOCUMENT_MANAGEMENT_URL first, THEN the env-less builtin
`aiohttp-client-convention`, both globs matching the same real fixture class) resolves
every call through the CUSTOM idiom, keeping its base_url_env -- pinned by
test_real_effective_idioms_custom_sdk_shadows_builtin_base_url_env, with two synthetic
tests additionally pinning the raw first-idiom-wins dedup in both explicit list orders,
and the two idiom variants also exercised separately (each its own single-idiom
`ServiceIdioms`, matching the brief's literal "два claim'а ... base_url_env=...; и
builtin-вариант без env").

No roles, no edges, no node_props, no channels: per the master plan's own explicit
"Роль MessageProducer НЕ ставится; роли для клиентов не вводим" -- `HttpClientResult`
is deliberately just `(claims, stats)`, matching the task's literal top-line signature
verbatim (unlike FastapiResult/KafkaResult/TemporalResult, none of which needed this
deviation note -- this is the first M2 domain extractor whose actual field list matches
the plan doc's abbreviated signature exactly). CALLS_HTTP edges are NOT created here --
S7 (T7, not in scope) needs the full cross-service http_route table (staged Channel
nodes from EVERY service's fastapi_ext run) before it can match a claim's path_template
to an actual route, which a single per-file extractor pass cannot see.

Claim payload shape (mirrors temporal_ext's temporal_start_mark claim, per this task's
own explicit instruction to reconcile with T1's `add_claims` contract): `kind="http_call"`
is passed as `add_claims`'s own 3rd positional argument by analyze.py's S5 wiring, NOT
embedded in the payload dict itself. `evidence_line` is a bare int (`call.start_line`)
-- not a combined "relpath+line" string as an earlier draft of this task's brief floated
-- because `claims_for()` already injects `_relpath` from the claims table's own
`relpath` column at read time (see `Staging.claims_for`), so a duplicate relpath inside
the payload would be redundant.

M6 T2 (pilot GAPS §2 gap 1): decorator-SDK mode, a SECOND candidate-discovery mode this
same extractor supports per-idiom, active whenever `HttpClientIdiom.route_from` is set
(config/models.py fail-closes the DSL as all-or-nothing, review Important-2:
`route_from`/`call`/`verb_from` must be set together or not at all -- a partial config
either can't locate the call-site, can never resolve a verb (zero claims forever), or
carries silently-inert fields, so every partial cell is a load-time ValidationError).
Real convention that motivated this (camunda-gateway app/clients/*.py, class
`*Client(BaseClient)`):
    @path_template("/v1/dmout/user_hv/uuid/{customer_uid}")     # <- route: DECORATOR
    async def get_client_hv_sign(self, customer_uid, **kwargs):
        request = Request(Method.GET, self.host, ...)            # <- verb: enum arg0
        return await self.driver.fetch_content(request, ...)     # <- the CALL itself
None of verb-mode's own gating applies here at all (`_is_candidate_call`'s `_VERBS`
membership, `_resolve_path`'s arg0-is-a-URL assumption) -- `fetch_content`/`fetch` name
nothing HTTP-shaped, and arg0 is a `Request` object, not a URL. Per-idiom algorithm
(`_extract_decorator_sdk`), driven off `ctx.facts.defs` (methods) rather than
`ctx.facts.calls` (call-sites) as the outer loop, unlike verb-mode:
  1. Every `function` DefFact with a non-empty `.decorators`, whose nearest class
     ancestor matches `idiom.class_glob` (file_glob already gates the whole idiom, same
     as verb-mode).
  2. `_decorator_route`: mini-parse (`_mini_call`, ports fastapi_ext.py's own
     `_mini_call` precedent -- decorators are never real CallFacts, see facts.py's
     decorated_definition handling) each decorator string looking for one whose callee
     name is `route_from.decorator`; its `route_from.arg`-th positional arg is resolved
     through the SAME `resolve_arg` + `_resolve_path` pipeline verb-mode's own arg0 URL
     goes through (module docstring above) -- a plain string literal (the overwhelming
     common case: `@path_template("/v1/...")`) resolves via the "value, starts with /"
     branch, `{param}`-shaped placeholders passing through untouched as ordinary string
     content (no interpolation to strip, unlike an f-string) -- but a module-const name
     or an f-string decorator arg would resolve too, free, via the identical machinery.
     No decorator on this method matches BY NAME -> method skipped silently (the idiom
     simply doesn't apply -- covers "method without decorator -> no claim"); a
     name-MATCHING decorator whose arg can't be resolved to a path (kwarg-only call,
     non-string expression, missing arg) -> no claim + `http_route_unresolved` bump
     (review Important-3: a matched-but-unreadable route is a countable miss, unlike a
     name mismatch) -- `_decorator_route`'s tri-state return separates the two.
  3. The call-site: `idiom.call`'s `|`-separated alternatives (`_call_alternatives`),
     each a receiver-tail dotted path (e.g. "driver.fetch_content"). `_matches_call_alt`
     compares against a call's OWN full dotted path (`receiver_text` + "." +
     `callee_name`, e.g. "self.driver.fetch_content") by SUFFIX on "."-segments, so the
     alt need not spell out `self` (or any deeper prefix a real SDK might have, e.g.
     "self._impl.driver.fetch_content" -- the tail still matches "driver.fetch_content")
     -- but the segment immediately preceding the callee must match literally, which is
     exactly why "self.other.fetch_content" does NOT match the "driver.fetch_content"
     alternative even though the callee name alone coincides. Candidates are narrowed to
     calls textually nested anywhere inside the decorated method (`_is_within_def`, a
     DefFact.parent walk from the call's own enclosing_def up to the method's index --
     handles a call sitting in a nested closure inside the method too, though no fixture
     here actually nests one). No call-site matches -> method skipped, no claim (the
     decorator alone is not sufficient evidence of an HTTP call).
  4. Verb: `_find_verb` scans `ctx.facts.calls` for a call matching
     `verb_from.request_ctor` (e.g. "Request"; M7 T5: "|"-separated alternatives too,
     e.g. "Request|ProxyRequest" -- the SAME `_call_alternatives`/`_matches_call_alt`
     pair step 3's `call` field already uses, reused verbatim rather than
     reimplemented) nested inside the SAME method (`_is_within_def` again), whose OWN
     arg0 is an attribute expression -- `enum_part.rpartition(".")` splits e.g.
     "Method.GET" into ("Method", "GET"); the enum_part must equal `verb_from.enum`
     exactly. See "the null-verb decision" below for what happens when no such call, or
     no such arg0 shape, is found anywhere in the method.
  5. Claim emission mirrors verb-mode's own `_emit_claim` exactly in SHAPE (same six
     keys, same base_url_env/evidence_line semantics -- `evidence_line` is the DRIVER
     call's own `start_line`, i.e. where the actual network call happens, not the
     decorator's or the Request-ctor's line) and in the missing-node-id defensive stat.

Cross-idiom dedup: the driver call-site's OWN `callee_start_byte` is added to the SAME
`claimed_starts` set verb-mode idioms populate, right after the call-site is found to
match -- NOT "before path/verb resolution", as an earlier revision of this paragraph
claimed. Route/path resolution (`_decorator_route`, step 2 above) runs BEFORE the
call-site search even starts, and a method whose route doesn't resolve (`route is
None`, or `_ROUTE_ARG_UNRESOLVED`) `continue`s straight past the call-site search
without ever reaching it -- so path resolution has ALREADY succeeded by the time a
byte is claimed, and a route-unresolved method never claims a byte at all. Only verb
resolution (`_find_verb`, step 4, called right after the claim below) is genuinely
still pending at the claim point. This still mirrors verb-mode's own "claims the byte
once its call-site structurally matches, even if the one resolution step still
outstanding then fails" convention (see `_emit_claim`'s own call sites in the
unmodified verb-mode loop below, where arg0/path resolution is that outstanding step
instead, deferred entirely to AFTER verb-mode's own claim) -- kept consistent so a
workspace mixing a decorator-SDK idiom and a verb-mode idiom over overlapping globs
still dedups by one single, uniform "claim on structural match" rule, just gated by a
different set of preconditions per mode.

THE NULL-VERB DECISION (M6 T2 brief explicitly asks this be read from the code and
documented, not assumed): read `linking/http_routes.py` before deciding whether a
"verb=null" claim could ride the existing S7 path/unresolved-Channel machinery.
Evidence, in order:
  - `http_routes._candidates` filters `r.method == claim["verb"]`; every STAGED route's
    `method` is always a real non-empty string (fastapi_ext.py always uppercases a real
    HTTP verb into `props["http_method"]`) -- `r.method == None` can therefore never be
    True. A verb=None claim would thus NEVER find a candidate route, unconditionally,
    regardless of how well its path_template happens to match -- it always falls to the
    "no candidates" branch.
  - That branch (`_unresolved_channel_and_edge`) calls `core.schema.make_channel_node(
    "http_route", method=claim["verb"], ...)`, and `make_channel_node` RAISES
    `ValueError("... requires method and template")` whenever `not method` -- `None` is
    falsy, so this is not a degraded-but-working path, it is a crash. There is no
    existing "conf-penalty, path-only" matching tier for a null verb; the brief's own
    "null-verb claim with conf-penalty IF SUPPORTED" branch is therefore not available.
  DECISION: when no verb can be found, `_extract_decorator_sdk` emits NO claim at all
  (never `verb=None`) and increments `stats["http_verb_unresolved"]` instead -- the same
  "no claim, dedicated unresolved counter" shape verb-mode already uses for its own
  `http_url_unresolved` (path can't be resolved -> no claim, count it) and
  `http_call_missing_node_id` (src node missing -> no claim, count it). This keeps
  `http_routes.link` byte-identical (it never sees a null-verb claim to crash on) and
  keeps `HttpCallClaim`'s shape's own invariant intact: every EMITTED claim has a real
  string verb, exactly as before this task.

VISIBILITY (M6 T2 review Important-1): `http_url_unresolved`/`http_verb_unresolved`/
`http_route_unresolved` are no longer stats-dict-only -- pipeline/analyze.py's
`_extract_join_and_stage` sums them across files (the `imports_external` precedent) and
both the full and incremental per-service report dicts carry all three (always present,
0 when this extractor is inactive; the "skipped" report shape predates them and
report.py reads with `.get(key, 0)`); pipeline/report.py's `print_report` shows a
yellow "http idiom misses" line whenever any counter is nonzero. `codegraph doctor`
remains out of the loop by design -- the analyze report dict is the surface these
belong to, same as `calls_unresolved`.

AUTO-ANCHOR (M7 T3, OPEN R1 -- docs/superpowers/reports/2026-07-23-pilot-rerun-open-
gaps.md): `base_url_env` was previously ALWAYS `idiom.base_url.env if idiom.base_url
else None` -- a pure idiom-config lookup, blind to the call site. The pilot's own
regression: a real idiom named `base_url: {attr: self.host}` with NO `env` (the client's
host is a dynamic Settings-backed attribute, resolved only at runtime) -- every claim
from that idiom was therefore PERMANENTLY unanchored, which is what let linking/
http_routes.py's S7 stage match those claims' paths against EVERY service's routes with
no narrowing at all (see that module's own docstring for the false-match consequence).
`base_url_env` is now `_claim_base_url_env(idiom, cls, ...)`: explicit config
(`base_url.env`, or M7 T3's new `base_url.settings` -- a per-class ClassAttrIndex.
settings_field lookup) still wins outright when present; otherwise, `_self_attr_env`
looks for a `self.<host_attr> = <dotted-chain-or-name>` assignment (SelfAttrFact, M7 T3
sanctioned additive extension to parsing/facts.py -- neither AssignFact nor ClassAttrFact
could see an ATTRIBUTE assignment target at all before this) ANYWHERE in the matched
client class's own body, and joins the RHS's LAST identifier through the service-wide
ClassAttrIndex.field_by_name (already env-gated by T1 -- an ambiguous or env-less field
name is honestly absent, never guessed). `host_attr` (idiom field, default "host") makes
the target attribute name configurable, since real SDKs vary (`self.host`, `self._host`,
...). Both verb-mode (`_emit_claim`'s caller) and decorator-SDK mode compute this
identically, once per matched class per call-site -- see `_claim_base_url_env`'s own
docstring for the full precedence and `_self_attr_env`'s for the join mechanics. A claim
that still resolves to `base_url_env=None` after all of this (no assignment found, RHS
not a dotted-chain, or the field join misses) stays honestly unanchored -- linking/
http_routes.py's own anchoring tiers (M7 T3) then refuse to award it static/1.0 confidence
regardless, so an incomplete auto-anchor degrades to a lower-confidence match, never a
false one.
"""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase

from codegraph.config.models import (
    HttpClientIdiom,
    HttpRouteFromSpec,
    HttpVerbFromSpec,
    ServiceIdioms,
)
from codegraph.parsing.class_attrs import ClassAttrIndex
from codegraph.parsing.consts import ConstTable, Resolved, resolve_arg
from codegraph.parsing.facts import CallFact, DefFact, SelfAttrFact, build_file_facts

from .base import FileContext

_VERBS = frozenset({"get", "post", "put", "delete", "patch"})
_BASE_MARKER = "<base>"


@dataclass(frozen=True)
class HttpClientResult:
    claims: list[dict]
    stats: dict[str, int]


def _stats() -> dict[str, int]:
    return {
        "http_calls_resolved": 0,
        "http_url_unresolved": 0,
        "http_call_missing_node_id": 0,
        # M6 T2: decorator-SDK mode only -- no Request(Method.X, ...) ctor found (or
        # none shaped as expected) inside the method body. See module docstring's "THE
        # NULL-VERB DECISION": no claim is emitted for this method, ever with verb=None;
        # this counter is the record of the miss (surfaced in analyze.py's per-service
        # report dict, M6 T2 review Important-1).
        "http_verb_unresolved": 0,
        # M6 T2 review Important-3, decorator-SDK mode only: a decorator whose NAME
        # matched route_from.decorator was found on a candidate method, but its
        # route_from.arg-th positional arg could not be resolved to a path (kwarg-only
        # decorator call, non-string/non-const expression, missing arg, non-"/" value).
        # A decorator-name MISmatch is deliberately NOT counted -- that is the idiom
        # simply not matching (correct silence), not a resolution failure.
        "http_route_unresolved": 0,
    }


def _is_candidate_call(call: CallFact) -> bool:
    """Attribute-call (receiver_text любой -- see module docstring) whose callee is one
    of the 5 tracked HTTP verbs; NOT gated on the receiver's literal text."""
    return call.receiver_text is not None and call.callee_name in _VERBS


def _enclosing_method_and_class(
    defs_by_index: dict[int, DefFact], call: CallFact,
) -> tuple[DefFact, DefFact] | None:
    """None if `call` is at module level, sits directly in a class body (not inside any
    function at all), or its function nest never reaches a class (a plain top-level
    function) -- otherwise (method, class): `method` is `call`'s OWN immediate
    enclosing def (per the "src_id = id метода (enclosing def через node_ids)"
    convention T4/T5 already established -- may itself be nested inside another
    function inside the class, not necessarily the outermost one), `class` is the
    nearest class ancestor found by walking DefFact.parent upward from `method`."""
    if call.enclosing_def is None:
        return None
    method = defs_by_index.get(call.enclosing_def)
    if method is None or method.kind != "function":
        return None
    idx = method.parent
    while idx is not None:
        d = defs_by_index.get(idx)
        if d is None:
            return None
        if d.kind == "class":
            return method, d
        idx = d.parent
    return None


def _resolve_path(resolved: Resolved) -> tuple[str | None, str]:
    """(path, resolution_hint), or (None, "") if arg0 doesn't look like a URL template
    at all -- see module docstring for the three accepted shapes and the `<base>`
    assumption."""
    if resolved.kind == "template" and resolved.value is not None:
        if resolved.value.startswith(_BASE_MARKER):
            return resolved.value[len(_BASE_MARKER):], "static"
        if resolved.value.startswith("/"):
            return resolved.value, "heuristic"
        return None, ""
    if resolved.kind == "value" and resolved.value is not None and resolved.value.startswith("/"):
        return resolved.value, "static"
    return None, ""


# -- M6 T2: decorator-SDK mode helpers (route_from/call/verb_from) ------------------


def _enclosing_class(defs_by_index: dict[int, DefFact], d: DefFact) -> DefFact | None:
    """Nearest class ancestor of `d` itself (as opposed to `_enclosing_method_and_class`,
    which starts from a CALL's enclosing def) -- walks DefFact.parent upward from `d`."""
    idx = d.parent
    while idx is not None:
        parent = defs_by_index.get(idx)
        if parent is None:
            return None
        if parent.kind == "class":
            return parent
        idx = parent.parent
    return None


# -- M7 T3 (OPEN R1): base_url_env resolution -- explicit idiom config, THEN
# self.<host_attr>-assignment auto-anchor. See http_client_ext's own module docstring
# addendum below `extract_http_client` for the full narrative; this is the mechanism.


def _explicit_base_url_env(
    idiom: HttpClientIdiom, class_attr_index: ClassAttrIndex | None,
) -> str | None:
    """Explicit idiom-configured anchor -- `base_url.env` (verbatim) or
    `base_url.settings` (M7 T3: "<ClassFQN>.<field>", a PER-CLASS
    ClassAttrIndex.settings_field lookup -- mirrors ValueSpec.settings, see
    parsing/consts.py's resolve_settings_source) -- either WINS over self.host
    auto-anchoring below, per this task's own "explicit beats auto" rule. `env` is
    checked first (cheapest, needs no index at all); `settings` degrades to None
    when no index is wired, same convention resolve_settings_source itself uses."""
    if idiom.base_url is None:
        return None
    if idiom.base_url.env is not None:
        return idiom.base_url.env
    if idiom.base_url.settings is not None and class_attr_index is not None:
        class_fqn, _, field_name = idiom.base_url.settings.rpartition(".")
        field = class_attr_index.settings_field(class_fqn, field_name)
        if field is not None:
            return field.env_name
    return None


def _self_attr_env(
    cls: DefFact, defs_by_index: dict[int, DefFact],
    self_attr_assigns: list[SelfAttrFact], host_attr: str,
    class_attr_index: ClassAttrIndex | None,
) -> str | None:
    """Auto-anchor (M7 T3, OPEN R1): the FIRST `self.<host_attr> = <dotted-chain>`
    assignment found anywhere in `cls`'s own body (any method, AST-walk order --
    mirrors `_find_verb`'s own first-match precedent) whose RHS tail joins, BY NAME,
    a real env-carrying Settings field in the service-wide ClassAttrIndex
    (`field_by_name` -- already env-gated: an ambiguous or env-less name is honestly
    absent, see class_attrs.py). Returns on the FIRST structurally-matching
    assignment regardless of whether ITS join succeeds -- deterministic, and matches
    the realistic shape (one ctor, one `self.host = ...` line) this is modeled on;
    None whenever no matching assignment exists in this class at all, its RHS isn't
    a plain dotted-chain/bare-name, or the join misses/collides -- "no auto-anchor"
    is a perfectly normal, tested outcome (claim stays unanchored, honest), never a
    guess.

    TRACKED LIMITATION (M7 T3 review Important-2) -- inherited self.<host_attr>:
    real client hierarchies often assign the host in a SHARED base ctor (`class
    StepsClient(BaseClient)` where only BaseClient's `__init__` does `self.host =
    config...` -- the OPEN R1 pilot's own real shape). This lookup performs NO
    inheritance walk AT ALL: an assignment anchors a claim only when its own
    enclosing class IS the claim's matched class (the `owner.index != cls.index`
    check below -- DefFact identity, not name/base_exprs), so a base class's
    assignment is excluded even when the base is defined in the SAME file.
    Resolving it would need the service-wide class hierarchy (base_exprs give only
    base-name TEXT, possibly defined in another file entirely) plus MRO walking --
    the identical, deliberately-out-of-scope reasoning class_attrs.py's own
    inherited-model_config TRACKED LIMITATION already documents (M7 T1 review
    Important-3). A subclass whose own body has no `self.<host_attr>` assignment
    therefore auto-anchors to None, honestly -- never a guessed anchor -- and its
    claims stay unanchored (linking/http_routes.py tier 3: heuristic/0.7 at best,
    unique-match required, never a false static/1.0). Pinned by
    test_tracked_limitation_base_class_self_host_assign_not_seen. Workaround for
    real codebases: explicit `base_url: {env: ...}` (or `{settings: ...}`) on the
    idiom -- explicit config bypasses this lookup entirely."""
    if class_attr_index is None:
        return None
    for fact in self_attr_assigns:
        if fact.attr != host_attr or fact.rhs_tail is None or fact.enclosing_def is None:
            continue
        enclosing = defs_by_index.get(fact.enclosing_def)
        owner = _enclosing_class(defs_by_index, enclosing) if enclosing is not None else None
        if owner is None or owner.index != cls.index:
            continue
        field = class_attr_index.field_by_name(fact.rhs_tail)
        return field.env_name if field is not None else None
    return None


def _claim_base_url_env(
    idiom: HttpClientIdiom, cls: DefFact, defs_by_index: dict[int, DefFact],
    self_attr_assigns: list[SelfAttrFact], class_attr_index: ClassAttrIndex | None,
) -> str | None:
    """One claim's base_url_env: explicit idiom config wins outright; self.host
    auto-anchoring is the fallback when explicit resolution yields nothing."""
    explicit = _explicit_base_url_env(idiom, class_attr_index)
    if explicit is not None:
        return explicit
    return _self_attr_env(cls, defs_by_index, self_attr_assigns, idiom.host_attr, class_attr_index)


def _is_within_def(defs_by_index: dict[int, DefFact], call: CallFact, target_idx: int) -> bool:
    """True if `call` sits anywhere inside def #`target_idx`'s body -- directly, or
    nested through any number of intervening function defs (closures), by walking
    DefFact.parent upward from `call.enclosing_def` looking for `target_idx`."""
    idx = call.enclosing_def
    while idx is not None:
        if idx == target_idx:
            return True
        d = defs_by_index.get(idx)
        if d is None:
            return False
        idx = d.parent
    return False


def _mini_call(dec_text: str) -> CallFact | None:
    """Re-parses one decorator's raw text as a standalone snippet to get a real
    CallFact with `.args` -- ports fastapi_ext.py's own `_mini_call` (decorators are
    never visited as CallFacts by build_file_facts: the decorator expression lives
    outside `body`, see facts.py's decorated_definition handling). A bare/non-call
    decorator (e.g. "staticmethod") mini-parses to zero calls -> None."""
    mini = build_file_facts("<decorator>", dec_text.encode("utf-8") + b"\n")
    return mini.calls[0] if mini.calls else None


_ROUTE_ARG_UNRESOLVED = "unresolved"


def _decorator_route(
    method: DefFact, route_from: HttpRouteFromSpec, consts: ConstTable,
) -> tuple[str, str] | str | None:
    """(path_template, resolution_hint) from the FIRST decorator on `method` whose
    callee name is `route_from.decorator`, resolved through the SAME resolve_arg +
    _resolve_path pipeline verb-mode's own arg0 URL goes through (see module docstring).
    Tri-state return (M6 T2 review Important-3 -- the two failure shapes must be
    distinguishable because only one of them is a counted miss):
      - (path, hint)          -- a name-matching decorator resolved to a path.
      - _ROUTE_ARG_UNRESOLVED -- at least one decorator MATCHED by name, but none of
                                 the matching ones resolved a path (kwarg-only call,
                                 non-string arg, missing arg, non-"/" value) -- the
                                 caller bumps http_route_unresolved.
      - None                  -- no decorator matched by name at all: the idiom simply
                                 does not apply to this method (correct silence)."""
    name_matched = False
    for dec_text in method.decorators:
        call = _mini_call(dec_text)
        if call is None or call.callee_name != route_from.decorator:
            continue
        name_matched = True
        arg = next((a for a in call.args if a.index == route_from.arg), None)
        resolved = resolve_arg(arg, consts) if arg is not None else Resolved(kind="unresolved")
        path, hint = _resolve_path(resolved)
        if path is not None:
            return path, hint
    return _ROUTE_ARG_UNRESOLVED if name_matched else None


def _call_alternatives(call_spec: str) -> list[list[str]]:
    """"driver.fetch_content|driver.fetch" -> [["driver", "fetch_content"], ["driver", "fetch"]]."""
    return [alt.split(".") for alt in call_spec.split("|")]


def _full_call_path(call: CallFact) -> list[str]:
    """Full dotted receiver+callee path, e.g. "self.driver" + "fetch_content" ->
    ["self", "driver", "fetch_content"]; a receiver-less call contributes just
    [callee_name]."""
    receiver_segments = call.receiver_text.split(".") if call.receiver_text else []
    return [*receiver_segments, call.callee_name]


def _matches_call_alt(call: CallFact, alternatives: list[list[str]]) -> bool:
    """True if `call`'s full dotted path ENDS WITH any alternative's segments, aligned
    on "."-boundaries -- e.g. ["self", "driver", "fetch_content"] matches alternative
    ["driver", "fetch_content"] (tail of 2), but NOT ["self", "other", "fetch_content"]
    (the segment right before the callee name differs: other != driver)."""
    path = _full_call_path(call)
    return any(len(alt) <= len(path) and path[len(path) - len(alt):] == alt for alt in alternatives)


def _find_verb(
    defs_by_index: dict[int, DefFact], calls: list[CallFact], method_idx: int,
    verb_from: HttpVerbFromSpec,
) -> str | None:
    """Scans ALL calls in the file for one matching `verb_from.request_ctor` nested
    inside def #`method_idx`, whose own arg0 is an attribute expression "ENUM.VERB"
    with ENUM == verb_from.enum -- returns VERB upper-cased. None if no such call/arg0
    shape exists anywhere inside the method (see module docstring's "THE NULL-VERB
    DECISION" for what the caller does then -- NOT a verb=None claim).

    M7 T5 (pilot-rerun.md verb_unresolved=15 -- document-management's real
    `ProxyRequest(Request)` subclass): `request_ctor` is "|"-separated alternatives,
    matched via the SAME `_call_alternatives`/`_matches_call_alt` pair the call-site
    step above already uses for `idiom.call` -- reused verbatim, not reimplemented. A
    bare, receiver-less ctor call (`Request(...)`, the overwhelmingly common shape --
    `_full_call_path` contributes just `[callee_name]` when `receiver_text is None`)
    against a single, dot-free alternative degrades to exactly the pre-M7-T5
    `callee_name == request_ctor` comparison, byte-identical.

    First-match semantics (M6 T2 review Minor-4): `ctx.facts.calls` is built in AST
    walk order, so if a method body somehow contains TWO matching `Request(Method.X,
    ...)` ctors with different verbs, the TEXTUALLY FIRST one wins and the second is
    never consulted. The pilot convention this mode models builds exactly one Request
    per method (the whole point of the SDK shape), so this is a documented tiebreak
    for a degenerate input, not a supported pattern."""
    ctor_alternatives = _call_alternatives(verb_from.request_ctor)
    for call in calls:
        if not _matches_call_alt(call, ctor_alternatives):
            continue
        if not _is_within_def(defs_by_index, call, method_idx):
            continue
        arg0 = next((a for a in call.args if a.index == 0), None)
        if arg0 is None or arg0.value_kind != "attr":
            continue
        enum_part, sep, verb_part = arg0.text.rpartition(".")
        if sep and enum_part == verb_from.enum and verb_part:
            return verb_part.upper()
    return None


def _extract_decorator_sdk(
    ctx: FileContext, idiom: HttpClientIdiom, defs_by_index: dict[int, DefFact],
    node_ids: dict[int, str], consts: ConstTable, claimed_starts: set[int],
    claims: list[dict], stats: dict[str, int],
) -> None:
    """One idiom's worth of decorator-SDK candidates -- see module docstring for the
    full 5-step algorithm. Precondition: `idiom.route_from`, `idiom.call` AND
    `idiom.verb_from` are all set -- config/models.py's fail-closed all-or-nothing
    validator (M6 T2 review Important-2) guarantees this for EVERY HttpClientIdiom
    instance (pydantic validators run on construction, not just YAML loading), and the
    caller only dispatches here when `idiom.route_from is not None`."""
    assert idiom.route_from is not None
    assert idiom.call is not None
    assert idiom.verb_from is not None
    alternatives = _call_alternatives(idiom.call)

    for method in ctx.facts.defs:
        if method.kind != "function" or not method.decorators:
            continue
        cls = _enclosing_class(defs_by_index, method)
        if cls is None or not fnmatchcase(cls.name, idiom.class_glob):
            continue
        route = _decorator_route(method, idiom.route_from, consts)
        if route is None:
            continue
        if route == _ROUTE_ARG_UNRESOLVED:
            # Important-3: the route decorator itself matched by name, but its arg
            # couldn't be read -- a real, countable miss (unlike a name mismatch just
            # above, which is the idiom correctly not applying).
            stats["http_route_unresolved"] += 1
            continue
        path, resolution_hint = route

        driver_call = next(
            (
                c for c in ctx.facts.calls
                if c.callee_start_byte not in claimed_starts
                and _is_within_def(defs_by_index, c, method.index)
                and _matches_call_alt(c, alternatives)
            ),
            None,
        )
        if driver_call is None:
            continue
        claimed_starts.add(driver_call.callee_start_byte)

        verb = _find_verb(defs_by_index, ctx.facts.calls, method.index, idiom.verb_from)
        if verb is None:
            stats["http_verb_unresolved"] += 1
            continue

        method_id = node_ids.get(method.index)
        if method_id is None:
            stats["http_call_missing_node_id"] += 1
            continue

        base_url_env = _claim_base_url_env(
            idiom, cls, defs_by_index, ctx.facts.self_attr_assigns, ctx.class_attr_index,
        )
        claims.append({
            "src_id": method_id,
            "verb": verb,
            "path_template": path,
            "base_url_env": base_url_env,
            "resolution_hint": resolution_hint,
            "evidence_line": driver_call.start_line,
        })
        stats["http_calls_resolved"] += 1


def _emit_claim(
    call: CallFact, method: DefFact, node_ids: dict[int, str],
    consts: ConstTable, base_url_env: str | None, claims: list[dict], stats: dict[str, int],
) -> None:
    method_id = node_ids.get(method.index)
    if method_id is None:
        stats["http_call_missing_node_id"] += 1
        return
    arg0 = next((a for a in call.args if a.index == 0), None)
    resolved = resolve_arg(arg0, consts) if arg0 is not None else Resolved(kind="unresolved")
    path, resolution_hint = _resolve_path(resolved)
    if path is None:
        stats["http_url_unresolved"] += 1
        return
    claims.append({
        "src_id": method_id,
        "verb": call.callee_name.upper(),
        "path_template": path,
        "base_url_env": base_url_env,
        "resolution_hint": resolution_hint,
        "evidence_line": call.start_line,
    })
    stats["http_calls_resolved"] += 1


def extract_http_client(
    ctx: FileContext, node_ids: dict[int, str], idioms: ServiceIdioms, consts: ConstTable,
) -> HttpClientResult:
    claims: list[dict] = []
    stats = _stats()
    if not idioms.http_clients:
        return HttpClientResult(claims=claims, stats=stats)

    defs_by_index = {d.index: d for d in ctx.facts.defs}
    claimed_starts: set[int] = set()

    for idiom in idioms.http_clients:
        if not fnmatchcase(ctx.relpath, idiom.file_glob):
            continue
        if idiom.route_from is not None:
            # M6 T2: decorator-SDK mode -- entirely separate candidate discovery (defs,
            # not calls, drive the outer loop); see module docstring. config/models.py
            # fail-closes route_from without call, so idiom.call is never None here.
            _extract_decorator_sdk(
                ctx, idiom, defs_by_index, node_ids, consts, claimed_starts, claims, stats,
            )
            continue
        for call in ctx.facts.calls:
            if call.callee_start_byte in claimed_starts or not _is_candidate_call(call):
                continue
            hit = _enclosing_method_and_class(defs_by_index, call)
            if hit is None:
                continue
            method, cls = hit
            if not fnmatchcase(cls.name, idiom.class_glob):
                continue
            claimed_starts.add(call.callee_start_byte)
            base_url_env = _claim_base_url_env(
                idiom, cls, defs_by_index, ctx.facts.self_attr_assigns, ctx.class_attr_index,
            )
            _emit_claim(call, method, node_ids, consts, base_url_env, claims, stats)

    return HttpClientResult(claims=claims, stats=stats)
