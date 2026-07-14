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
"""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase

from codegraph.config.models import HttpClientIdiom, ServiceIdioms
from codegraph.parsing.consts import ConstTable, Resolved, resolve_arg
from codegraph.parsing.facts import CallFact, DefFact

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


def _emit_claim(
    idiom: HttpClientIdiom, call: CallFact, method: DefFact, node_ids: dict[int, str],
    consts: ConstTable, claims: list[dict], stats: dict[str, int],
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
        "base_url_env": idiom.base_url.env if idiom.base_url else None,
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
            _emit_claim(idiom, call, method, node_ids, consts, claims, stats)

    return HttpClientResult(claims=claims, stats=stats)
