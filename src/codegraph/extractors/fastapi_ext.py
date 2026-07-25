"""fastapi_ext: FastAPI route/dependency extractor (M2 T4; M8 T1 rerun-2 R4).

Function defs decorated with `<recv>.<verb>("path", ...)` (verb in {get, post, put,
delete, patch, head, options}; `<recv>` bound to an APIRouter/FastAPI AssignFact in the
same file, otherwise skipped) become RouteHandlers: a node_props patch ({http_method,
path_template}, from the LOCAL, same-file template) that analyze.py merges into the
handler's own NodeRec before staging.upsert_nodes -- file-local facts, unchanged since
M2 T4 (see analyze.py's S5 wiring).

M8 T1 (rerun-2 R4 -- docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
what CHANGED is the route's full, cross-file-composed identity. The real (and most
common) convention builds a route's prefix across an `include_router` CHAIN spanning
file boundaries (`router = APIRouter()` in file A; `parent.include_router(a.router)` in
file B; `app.include_router(b.router, prefix="/api/v1")` in file C) -- a single-file
extractor pass can never see that whole chain, so this module no longer emits the
Channel(http_route) node or the HANDLES edge itself (both require walking that chain,
workspace-wide, in S7 -- linking/router_prefix.py's job). Instead it emits three
per-file CLAIM kinds (consumed by router_prefix.py, mirroring http_client_ext's own
claims-not-edges precedent):
  - route_decl: {router_symbol, verb, path, handler_node_id, prefix_local,
    evidence_line} -- one per matched route decorator. `path`/`prefix_local` are the
    exact same two pieces the OLD `_template(_route_prefix(assign), path)` call
    combined directly -- kept separate here so router_prefix.py can prepend any
    ADDITIONAL cross-file prefix before prefix_local+path (the trivial, no-chain case
    reproduces today's byte-exact template: see router_prefix.py's own docstring/tests
    for the composition proof). `router_symbol` identifies the SPECIFIC router object
    the route's receiver is bound to (the `assign: AssignFact` `_match_route` already
    locates syntactically, same-file) -- resolved via ctx.def_symbol_lookup on that
    assignment's OWN target token (a DEFINITION occurrence,
    `assign.target_start_byte`), giving a cross-file-stable id any OTHER file's
    `include_router(this_router)` call can independently resolve to the identical
    symbol via a REFERENCE occurrence (proven against real scip-python output: see
    test_pipeline_analyze.py's wiring test docstring). None when unresolvable (no
    lookup wired, a degraded-fallback miss -- fallback.py never lays defs at an
    assignment target at all, only at class/function defs -- or a genuine SCIP miss);
    router_prefix.py then falls back to the LOCAL template alone (prefix_local+path)
    + a route_prefix_unresolved counter, never a guess. `evidence_line` (M8 review
    Important-2) is the handler def's own start_line -- the exact value the pre-M8
    direct-emission HANDLES edge carried, passed through the claim so
    router_prefix.py's HANDLES restores it (evidence_file rides on the claims table's
    own relpath column, injected back by claims_for as `_relpath`).
  - router_decl (M8 review Important-1): {router_symbol, prefix_local} -- one per
    `X = APIRouter(...)`/`X = FastAPI(...)` assignment, REGARDLESS of whether X has
    any routes or include_router calls of its own in this file. This is what lets
    router_prefix.py fold an INTERMEDIATE aggregator router's own declared prefix
    (`B = APIRouter(prefix="/v2")`, no routes, includes A, included by C -- the
    versioned-aggregator-in-__init__.py convention) into the composed template:
    before this claim existed, that prefix was invisible to EVERY claim form, so the
    chain composed "successfully" into a silently-INCOMPLETE confident template with
    no counter -- the funnel-class silent-wrong M7 exists to prevent. FastAPI()
    assignments emit too (prefix_local always "" -- `_route_prefix`'s own FastAPI
    rule): the chain ROOT is itself a hop parent whose own prefix router_prefix.py
    must know, and its missing-hop-decl rule (no router_decl for a hop parent ->
    whole-prefix discard + counter) would otherwise spuriously discard every chain
    ending at an app object. A None router_symbol (same resolution mechanism/misses
    as route_decl's, above) emits NOTHING -- an unkeyable claim is unusable at
    composition (mirrors temporal_start_mark's own "no claim without a dst"
    precedent); the affected chains already discard honestly downstream.
  - router_include: {parent_symbol, child_symbol, prefix} -- one per
    `<parent>.include_router(<child>, prefix=...)` call, ANY receiver/arg0 shape (NOT
    gated on "receiver looks like a known same-file APIRouter" the way route matching
    is -- mirrors temporal_ext's own receiver-agnostic `.signal(...)` sender
    precedent: `include_router` is FastAPI/Starlette-specific enough a vocabulary that
    an unconditional callee-name match carries little false-positive risk, and the
    parent/child object's OWN identity is frequently NOT itself a same-file
    APIRouter(...)-bound name -- it's commonly a parameter, an imported router, or a
    dynamically-built one). `parent_symbol`/`child_symbol` resolve via
    ctx.ref_symbol_lookup on the call's own RECEIVER token (`CallFact.
    receiver_start_byte`, M8 T1 sanctioned facts.py extension) and arg0's own name
    token (`ArgFact.name_start_byte`, the SAME mechanism INVOKES_ACTIVITY already uses)
    respectively -- either (or both) may independently resolve to None; the claim is
    STILL emitted either way (router_prefix.py's own composition is what decides
    whether a None-symbol claim can be used at all -- see its docstring -- never a
    guess made here).

Decorators are never visited as CallFacts by build_file_facts: decorated_definition
unwraps the decorator's text (DefFact.decorators is raw strings), but the decorator
expression itself lives outside `body` and is never walked as a `call` node (M1a
carried-forward limitation, see progress.md M1a Task 8: "вызовы в default-значениях
параметров и class-bases не посещаются" -- the same "outside body" gap also covers
decorators). `_mini_call()` re-parses one decorator's text standalone
(`build_file_facts("<decorator>", text + b"\\n")`) to get a real CallFact with
.args/.receiver_text, reusing T2's own argument parser instead of hand-rolling a second
one. Because that mini-reparse discards the decorator's own absolute byte position,
route_decl's router_symbol resolves via the (real-file-positioned) AssignFact target
instead of the decorator's own receiver token -- see above.

DEPENDS_ON can't ride on facts.calls either, for the identical reason: `Depends(get_db)`
inside a parameter default (or an `Annotated[X, Depends(y)]` annotation) lives inside
`parameters`, not `body` -- never visited, so no CallFact and no ref gets laid down for
it even by the degraded fallback resolver (extractors/../resolvers/fallback.py builds
refs purely from facts.calls). The identifier inside `Depends(...)` is instead found via
regex over ParamFact.default_text/annotation_text, its absolute byte span computed from
default_start_byte/annotation_start_byte, then resolved through ctx.ref_symbol_lookup --
a REF-table lookup (M2 T4 sanctioned FileContext/Staging extension), not the def-table
lookup python_core uses: `Depends(get_db)`'s `get_db` is a *reference* occurrence, not a
definition. A real SCIP run resolves this occurrence like any other identifier
reference; the hand-rolled fallback walker in this repo currently does not (documented
gap, see test_pipeline_analyze.py's wiring test) -- full resolution is proven here via a
stubbed ref_symbol_lookup, matching the brief's own "юнит: стаб; интеграцию покроет T9".
DEPENDS_ON itself is UNCHANGED by M8 T1 -- still file-local, still an edge (not a
claim), still emitted directly here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from codegraph.core.schema import EdgeRec
from codegraph.parsing.facts import ArgFact, AssignFact, CallFact, DefFact, build_file_facts
from codegraph.resolvers.scip.symbols import symbol_to_node_id

from .base import FileContext

_EXTRACTOR = "fastapi"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

_VERBS = frozenset({"get", "post", "put", "delete", "patch", "head", "options"})
_ROUTER_CALLEES = frozenset({"APIRouter", "FastAPI"})
_INCLUDE_ROUTER_CALLEE = "include_router"
# (?<!\w) word-boundary lookbehind (M2 final review fix): without it, re.search matches
# "Depends(" as a bare substring anywhere in the text, so a param default/annotation
# like `x: int = MyDepends(factory)` -- an unrelated custom callable that merely ENDS
# WITH "Depends(...)" -- would be misread as a real FastAPI Depends() call (matching
# starting right after "My"), wrongly resolving `factory` as a DEPENDS_ON target. The
# lookbehind requires "Depends(" to NOT be immediately preceded by a word character, so
# it still matches at the start of text or after whitespace/"["/","/"(" (every real
# shape: bare `Depends(x)`, `Annotated[X, Depends(x)]`, nested calls) but rejects
# "MyDepends(" / "some_depends(" / any other identifier merely suffixed with it.
_DEPENDS_RE = re.compile(r"(?<!\w)Depends\(\s*([A-Za-z_]\w*)\s*[),]")


@dataclass(frozen=True)
class FastapiResult:
    roles: dict[str, set[str]]
    node_props: dict[str, dict]
    edges: list[EdgeRec]
    # M8 T1 (rerun-2 R4): route_decl/router_include/router_decl replace the old direct
    # channels/HANDLES output -- see module docstring for the full claim shapes and
    # why (cross-file `include_router` chains can't be resolved from a single file;
    # router_decl is the M8 review Important-1 addition -- intermediate routers' own
    # declared prefixes).
    route_decl_claims: list[dict]
    router_include_claims: list[dict]
    router_decl_claims: list[dict]
    stats: dict[str, int]


def _mini_call(dec_text: str) -> CallFact | None:
    """Re-parses one decorator's raw text as a standalone snippet to get a real
    CallFact (see module docstring for why decorators aren't already CallFacts). A
    bare/non-call decorator (e.g. "staticmethod") mini-parses to zero calls -> None."""
    mini = build_file_facts("<decorator>", dec_text.encode("utf-8") + b"\n")
    return mini.calls[0] if mini.calls else None


def _route_prefix(assign: AssignFact) -> str:
    """APIRouter(prefix="...") kwarg (ArgFact keyword); FastAPI() itself has no prefix
    concept -- always ""."""
    if assign.callee_name != "APIRouter":
        return ""
    for arg in assign.call_args or ():
        if arg.keyword == "prefix" and arg.value_kind == "string":
            return arg.string_value or ""
    return ""


def _template(prefix: str, path: str) -> str:
    """prefix + path; empty path -> prefix alone; both empty -> "/" (root)."""
    if not path:
        return prefix if prefix else "/"
    return prefix + path


def _byte_offset(text: str, char_offset: int) -> int:
    """Char index within `text` -> byte offset, robust to any multi-byte prefix (a
    Depends(...) expression is always ASCII in practice, but this avoids relying on
    that assumption for the ident's absolute source-byte span)."""
    return len(text[:char_offset].encode("utf-8"))


def _resolve_depends_target(ctx: FileContext, text: str, base_byte: int) -> str | None:
    match = _DEPENDS_RE.search(text)
    if match is None or ctx.ref_symbol_lookup is None:
        return None
    abs_start = base_byte + _byte_offset(text, match.start(1))
    sym = ctx.ref_symbol_lookup(ctx.relpath, abs_start)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _emit_depends(
    ctx: FileContext,
    d: DefFact,
    handler_id: str,
    target_id: str | None,
    via: str,
    edges: list[EdgeRec],
    stats: dict[str, int],
) -> None:
    if target_id is None:
        stats["depends_unresolved"] += 1
        return
    edges.append(EdgeRec(
        src=handler_id, dst=target_id, type="DEPENDS_ON",
        resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
        evidence_file=ctx.relpath, evidence_line=d.start_line,
        props={"via": via},
    ))
    stats["depends_resolved"] += 1


def _collect_depends(
    ctx: FileContext, d: DefFact, handler_id: str, edges: list[EdgeRec], stats: dict[str, int],
) -> None:
    """Once per handler def (not per matched route decorator -- a handler's dependency
    set is a property of its parameters, independent of how many routes point at it)."""
    for p in d.params:
        if p.default_text and p.default_start_byte is not None and "Depends(" in p.default_text:
            target = _resolve_depends_target(ctx, p.default_text, p.default_start_byte)
            _emit_depends(ctx, d, handler_id, target, "depends", edges, stats)
        if (p.annotation_text and p.annotation_start_byte is not None
                and "Depends(" in p.annotation_text):
            target = _resolve_depends_target(ctx, p.annotation_text, p.annotation_start_byte)
            _emit_depends(ctx, d, handler_id, target, "annotated", edges, stats)


def _match_route(
    ctx: FileContext, dec_text: str, assigns_by_target: dict[str, list[AssignFact]],
) -> tuple[str, str, AssignFact] | None:
    """Returns (method, path, assign) if dec_text is a route decorator on a recv bound
    to an APIRouter/FastAPI AssignFact in this file; None otherwise (not a call, not a
    known HTTP verb, receiver not a simple name, or receiver not APIRouter/FastAPI).
    `assign` is the located AssignFact itself (not yet reduced to a prefix/symbol) --
    the caller derives BOTH prefix_local (`_route_prefix(assign)`, unchanged) and
    router_symbol (`_resolve_router_symbol(ctx, assign)`, M8 T1) from it."""
    call = _mini_call(dec_text)
    if call is None or call.callee_name not in _VERBS:
        return None
    receiver = call.receiver_text
    if receiver is None or "." in receiver:
        return None  # not an attribute-call on a simple (dot-free) name
    assign = next(
        (a for a in assigns_by_target.get(receiver, ()) if a.callee_name in _ROUTER_CALLEES),
        None,
    )
    if assign is None:
        return None
    path_arg = next((arg for arg in call.args if arg.index == 0), None)
    if path_arg is None or path_arg.value_kind != "string":
        return None

    method = call.callee_name.upper()
    return method, path_arg.string_value or "", assign


def _resolve_router_symbol(ctx: FileContext, assign: AssignFact) -> str | None:
    """M8 T1: the router object's cross-file-stable identity -- resolved as a
    DEFINITION occurrence at the assignment's OWN target token (`router` in
    `router = APIRouter(...)`), via ctx.def_symbol_lookup (never ref_symbol_lookup:
    this IS the def site, not a reference to one). None when unresolvable (no
    target_start_byte -- shouldn't happen for a real build_file_facts() AssignFact,
    but a hand-built test AssignFact may omit it; the degraded fallback resolver,
    which only ever lays defs at facts.defs -- class/function -- never at an
    assignment target at all; or a genuine SCIP miss)."""
    if assign.target_start_byte is None:
        return None
    sym = ctx.def_symbol_lookup(ctx.relpath, assign.target_start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _resolve_call_receiver_symbol(ctx: FileContext, call: CallFact) -> str | None:
    """M8 T1: router_include's parent_symbol -- a REFERENCE occurrence at the call's
    own receiver token (CallFact.receiver_start_byte, M8 T1 facts.py extension).
    None when the receiver has no resolvable span (a subscript/call-expression
    receiver -- see facts.py's own docstring) or ref_symbol_lookup isn't wired/misses."""
    if call.receiver_start_byte is None or ctx.ref_symbol_lookup is None:
        return None
    sym = ctx.ref_symbol_lookup(ctx.relpath, call.receiver_start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _resolve_arg_symbol(ctx: FileContext, arg: ArgFact | None) -> str | None:
    """M8 T1: router_include's child_symbol -- a REFERENCE occurrence at arg0's own
    name token (ArgFact.name_start_byte, populated for "name"/"attr" value kinds --
    the SAME mechanism INVOKES_ACTIVITY already uses for its own arg0 resolution)."""
    if arg is None or arg.name_start_byte is None or ctx.ref_symbol_lookup is None:
        return None
    sym = ctx.ref_symbol_lookup(ctx.relpath, arg.name_start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _include_prefix(call: CallFact) -> str | None:
    """`include_router(..., prefix="...")` kwarg (ArgFact keyword) -- None when
    absent or non-string (mirrors `_route_prefix`'s own kwarg-extraction shape, but
    returns None rather than "" for "absent": router_prefix.py's own composition
    normalizes None to "" when concatenating, per the claim's documented "prefix may
    be None" contract)."""
    for arg in call.args:
        if arg.keyword == "prefix" and arg.value_kind == "string":
            return arg.string_value
    return None


def _collect_router_includes(ctx: FileContext, claims: list[dict]) -> None:
    """M8 T1: one router_include claim per `X.include_router(Y, prefix=...)` call --
    ANY receiver/arg0 shape (see module docstring for why this is deliberately NOT
    gated on "receiver looks like a known same-file APIRouter" the way route
    decorator matching is). Unresolvable parent/child symbols still produce a claim
    (with None fields) -- no claim is ever silently dropped here; router_prefix.py's
    own composition (S7) is what decides whether/how a None-symbol claim can be used,
    never this per-file pass (see its own docstring for the honesty rule)."""
    for call in ctx.facts.calls:
        if call.callee_name != _INCLUDE_ROUTER_CALLEE:
            continue
        arg0 = next((a for a in call.args if a.index == 0), None)
        claims.append({
            "parent_symbol": _resolve_call_receiver_symbol(ctx, call),
            "child_symbol": _resolve_arg_symbol(ctx, arg0),
            "prefix": _include_prefix(call),
        })


def _collect_router_decls(ctx: FileContext, claims: list[dict]) -> None:
    """M8 review Important-1: one router_decl claim per `X = APIRouter(...)`/
    `X = FastAPI(...)` assignment -- {router_symbol, prefix_local} -- REGARDLESS of
    whether X has any routes or include_router calls of its own in this file (the
    versioned-aggregator-in-__init__.py convention has neither routes NOR its own
    prefix visible any other way -- see module docstring). Symbol resolution is the
    IDENTICAL def-site mechanism route_decl's router_symbol uses
    (`_resolve_router_symbol`); a None symbol emits nothing (an unkeyable claim is
    unusable at composition -- the affected chains already discard honestly
    downstream via router_prefix.py's missing-hop-decl rule)."""
    for a in ctx.facts.assigns:
        if a.callee_name not in _ROUTER_CALLEES:
            continue
        sym = _resolve_router_symbol(ctx, a)
        if sym is None:
            continue
        claims.append({"router_symbol": sym, "prefix_local": _route_prefix(a)})


def extract_fastapi(ctx: FileContext, node_ids: dict[int, str]) -> FastapiResult:
    facts = ctx.facts
    roles: dict[str, set[str]] = {}
    node_props: dict[str, dict] = {}
    edges: list[EdgeRec] = []
    route_decl_claims: list[dict] = []
    router_include_claims: list[dict] = []
    router_decl_claims: list[dict] = []
    stats = {"routes": 0, "depends_resolved": 0, "depends_unresolved": 0}

    assigns_by_target: dict[str, list[AssignFact]] = {}
    for a in facts.assigns:
        assigns_by_target.setdefault(a.target, []).append(a)

    for d in facts.defs:
        if d.kind != "function" or not d.decorators:
            continue
        handler_id = node_ids.get(d.index)
        if handler_id is None:
            continue

        matched_any = False
        for dec_text in d.decorators:
            matched = _match_route(ctx, dec_text, assigns_by_target)
            if matched is None:
                continue
            method, path, assign = matched
            matched_any = True

            prefix_local = _route_prefix(assign)
            # node_props/roles: file-local, unchanged (LOCAL template only -- the
            # cross-file-composed template lives on the Channel node router_prefix.py
            # builds in S7, not here).
            template = _template(prefix_local, path)
            roles.setdefault(handler_id, set()).add("RouteHandler")
            node_props.setdefault(handler_id, {}).update(
                {"http_method": method, "path_template": template}
            )
            route_decl_claims.append({
                "router_symbol": _resolve_router_symbol(ctx, assign),
                "verb": method,
                "path": path,
                "handler_node_id": handler_id,
                "prefix_local": prefix_local,
                # M8 review Important-2: the handler def's own start_line -- the
                # exact evidence_line the pre-M8 direct-emission HANDLES carried,
                # restored onto the S7-built HANDLES by router_prefix.py.
                "evidence_line": d.start_line,
            })
            stats["routes"] += 1

        if matched_any:
            _collect_depends(ctx, d, handler_id, edges, stats)

    _collect_router_includes(ctx, router_include_claims)
    _collect_router_decls(ctx, router_decl_claims)

    return FastapiResult(
        roles=roles, node_props=node_props, edges=edges,
        route_decl_claims=route_decl_claims, router_include_claims=router_include_claims,
        router_decl_claims=router_decl_claims,
        stats=stats,
    )
