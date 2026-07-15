"""fastapi_ext: FastAPI route/dependency extractor (M2 T4).

Function defs decorated with `<recv>.<verb>("path", ...)` (verb in {get, post, put,
delete, patch, head, options}; `<recv>` bound to an APIRouter/FastAPI AssignFact in the
same file, otherwise skipped) become RouteHandlers: an http_route Channel node + a
static HANDLES edge (chan -> handler), plus a node_props patch ({http_method,
path_template}) that analyze.py merges into the handler's own NodeRec before
staging.upsert_nodes (see analyze.py's S5 wiring -- roles/props are NOT routed through
claims here: the route table is fully recoverable later from staged Channel(http_route)
+ HANDLES, so a separate route-claim isn't needed).

Decorators are never visited as CallFacts by build_file_facts: decorated_definition
unwraps the decorator's text (DefFact.decorators is raw strings), but the decorator
expression itself lives outside `body` and is never walked as a `call` node (M1a
carried-forward limitation, see progress.md M1a Task 8: "вызовы в default-значениях
параметров и class-bases не посещаются" -- the same "outside body" gap also covers
decorators). `_mini_call()` re-parses one decorator's text standalone
(`build_file_facts("<decorator>", text + b"\\n")`) to get a real CallFact with
.args/.receiver_text, reusing T2's own argument parser instead of hand-rolling a second
one.

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
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.parsing.facts import AssignFact, CallFact, DefFact, build_file_facts
from codegraph.resolvers.scip.symbols import symbol_to_node_id

from .base import FileContext

_EXTRACTOR = "fastapi"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

_VERBS = frozenset({"get", "post", "put", "delete", "patch", "head", "options"})
_ROUTER_CALLEES = frozenset({"APIRouter", "FastAPI"})
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
    channels: list[NodeRec]
    edges: list[EdgeRec]
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
) -> tuple[str, str] | None:
    """Returns (method, template) if dec_text is a route decorator on a recv bound to
    an APIRouter/FastAPI AssignFact in this file; None otherwise (not a call, not a
    known HTTP verb, receiver not a simple name, or receiver not APIRouter/FastAPI)."""
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
    template = _template(_route_prefix(assign), path_arg.string_value or "")
    return method, template


def extract_fastapi(ctx: FileContext, node_ids: dict[int, str]) -> FastapiResult:
    facts = ctx.facts
    roles: dict[str, set[str]] = {}
    node_props: dict[str, dict] = {}
    channels: list[NodeRec] = []
    edges: list[EdgeRec] = []
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
            method, template = matched
            matched_any = True

            chan = make_channel_node(
                "http_route", owner_service=ctx.service, method=method, template=template,
                http_method=method, path_template=template,
            )
            channels.append(chan)
            edges.append(EdgeRec(
                src=chan.id, dst=handler_id, type="HANDLES",
                resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
                evidence_file=ctx.relpath, evidence_line=d.start_line,
            ))
            roles.setdefault(handler_id, set()).add("RouteHandler")
            node_props.setdefault(handler_id, {}).update(
                {"http_method": method, "path_template": template}
            )
            stats["routes"] += 1

        if matched_any:
            _collect_depends(ctx, d, handler_id, edges, stats)

    return FastapiResult(
        roles=roles, node_props=node_props, channels=channels, edges=edges, stats=stats,
    )
