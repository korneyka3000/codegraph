"""python_core: базовый Python-экстрактор без SCIP — Module/Class/Function узлы и
CONTAINS/IMPORTS рёбра из tree-sitter FileFacts. FileContext.def_symbol_lookup, если
возвращает символ, имеет приоритет над структурным id (см. ids.py: форматы совпадают)."""

from __future__ import annotations

import hashlib

from codegraph.core import ids
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.parsing.facts import DefFact
from codegraph.resolvers.scip.symbols import symbol_to_node_id

from .base import ExtractionResult, FileContext

_EXTRACTOR = "python_core"
_RESOLUTION = "static"
_CONFIDENCE = 1.0


def nesting_chain(defs: list[DefFact], d: DefFact) -> list[tuple[str, str]]:
    chain = []
    cur = d
    while cur is not None:
        chain.append(("class" if cur.kind == "class" else "function", cur.name))
        cur = defs[cur.parent] if cur.parent is not None else None
    return list(reversed(chain))


def _resolve_relative(package: str, target: str) -> str:
    """Резолвинг относительного импорта против СОДЕРЖАЩЕГО ПАКЕТА (семантика Python):
    один лидирующий '.' — сам package, каждая следующая точка — уровень выше.
    package = dotted для __init__.py, parent(dotted) для обычного модуля ("" для
    top-level). Абсолютный target возвращается как есть."""
    if not target.startswith("."):
        return target
    dots = len(target) - len(target.lstrip("."))
    rest = target.lstrip(".")
    base = package.split(".") if package else []
    up = dots - 1  # level 1 = сам package
    base = base[: len(base) - up] if up <= len(base) else []
    return ".".join([*base, rest] if rest else base)


def _module_node_id(service: str, dotted: str) -> str:
    return ids.node_id(service, ids.module_descriptor(dotted))


def _def_id(ctx: FileContext, d: DefFact, dotted: str) -> str:
    """Per-def id: SCIP lookup at the def's name-span wins; otherwise structural,
    rebuilt from the full parent chain (matches SCIP-python's own descriptor format,
    so a resolved ancestor and a structurally-rebuilt descendant always agree)."""
    sym = ctx.def_symbol_lookup(ctx.relpath, d.name_start_byte)
    if sym is not None:
        return symbol_to_node_id(ctx.service, ctx.relpath, sym)
    return ids.node_id(
        ctx.service, ids.structural_descriptor(dotted, nesting_chain(ctx.facts.defs, d))
    )


def _line_count(source: bytes) -> int:
    n = source.count(b"\n")
    return n if source.endswith(b"\n") else n + 1


def extract(ctx: FileContext) -> ExtractionResult:
    service = ctx.service
    facts = ctx.facts
    source = ctx.source
    dotted = ids.relpath_to_module(ctx.relpath)
    is_package = ctx.relpath.endswith("/__init__.py") or ctx.relpath == "__init__.py"
    package = dotted if is_package else dotted.rsplit(".", 1)[0] if "." in dotted else ""

    nodes: list[NodeRec] = []
    edges: list[EdgeRec] = []
    seen_edges: set[tuple[str, str, str]] = set()
    stats = {"nodes": 0, "edges": 0, "imports_external": 0}

    def add_edge(src: str, dst: str, edge_type: str, line: int | None = None) -> None:
        key = (src, dst, edge_type)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append(EdgeRec(
            src=src, dst=dst, type=edge_type,
            resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
            # evidence_file = ctx.relpath UNCONDITIONALLY (M4 T5 fix): before this,
            # evidence_file was tied to `line is not None`, which conflated two
            # unrelated concerns -- CONTAINS edges (no natural line, `line` always
            # None here) ended up with evidence_file=None while IMPORTS (always
            # passes imp.start_line) got ctx.relpath. Staging.delete_file_layer
            # (incremental re-analyze, M4 T5) deletes S5-emitted edges by
            # `origin_service=? AND evidence_file IN (stale-relpaths)` -- with
            # evidence_file=None, a stale file's OLD CONTAINS edge (e.g. parent ->
            # a since-renamed/removed def's old node id) could never be matched for
            # deletion and would survive re-analyze forever, dangling. evidence_line
            # keeps its own, separate meaning (still None for CONTAINS, still a real
            # line for IMPORTS) -- only evidence_file needed decoupling from it.
            evidence_file=ctx.relpath,
            evidence_line=line,
        ))

    # -- module node + svc->module CONTAINS --
    module_id = _module_node_id(service, dotted)
    nodes.append(NodeRec(
        id=module_id, kind="Module", service=service,
        name=dotted.rsplit(".", 1)[-1], qualified_name=dotted,
        relpath=ctx.relpath, start_byte=0, end_byte=len(source),
        start_line=1, end_line=_line_count(source),
        content_hash=hashlib.sha256(source).hexdigest(),
        props={"docstring": facts.module_docstring},
    ))
    add_edge(f"svc:{service}", module_id, "CONTAINS")

    # -- Class/Function nodes + CONTAINS chain --
    # Resolve every def's id up front so the parent id used for a child's CONTAINS
    # edge is the exact same value as that parent's own NodeRec.id (never recomputed).
    #
    # M5 T3 (pilot Bug 7.1): two DIFFERENT defs can legitimately compute the SAME raw
    # id -- a class/function redefined under the same name in mutually-exclusive
    # branches (if/elif feature-flag pattern, e.g. dispatch/config.py's
    # `class Secret` in a metatron/kms/else fork). Two independent causes, both
    # closed by the SAME mechanism below because it only ever looks at _def_id's
    # OUTPUT text, never which branch of it produced that text: (1) SCIP path --
    # pyright/scip-python's symbol table is control-flow-insensitive, so it can
    # resolve BOTH branches' same-named defs to ONE symbol; (2) structural-fallback
    # path -- nesting_chain walks the parent chain by NAME (`cur.name`), not by id,
    # so two branches with identical (kind, name) ancestry rebuild the identical
    # descriptor independently of each other. Left undetected, the second def's
    # NodeRec silently overwrites the first's at `Staging.upsert_nodes`
    # (`INSERT OR REPLACE`, PK == id alone) -- one branch's entire node, and
    # transitively its methods, vanish, and `chunk_embed._symbol_ids_for_file`'s
    # span-match then defensively skips chunking the WHOLE file (its own
    # docstring), not just the colliding pair.
    #
    # Disambiguated in-order below: facts.defs is already appearance order (see
    # parsing.facts.build_file_facts -- a def's `index` is assigned via `len(defs)`
    # BEFORE recursing into its body, so parent.index < child.index always, and
    # siblings are visited left-to-right/top-to-bottom exactly as tree-sitter hands
    # back `node.children`). The FIRST def to produce a given raw id keeps it
    # byte-identical to today (stability constraint, pinned by every pre-existing
    # test -- a file with zero collisions sees zero id changes); every SUBSEQUENT
    # def producing that SAME raw id gets `ids.disambiguate(raw_id, 2)`, then `3`,
    # ... -- stable under line-number shifts (order of appearance, not line
    # numbers). Duplicated class methods (e.g. both branches' `__init__`) are
    # disambiguated by this exact same pass, in the exact same seen-set, since the
    # loop below never special-cases `kind` -- see test_python_core_extractor.py's
    # own "M5 T3" section.
    #
    # Sibling-insert renumbering (INTENDED dynamic, not an accident): because the
    # numbering is appearance-order, inserting a NEW same-named branch ABOVE a
    # previously-solo (or previously-first) def flips THAT def's id (unsuffixed ->
    # ~2, and ~2 -> ~3, ...) even though its own source text never changed -- the
    # new first occurrence takes over the unsuffixed id. This is the deliberate
    # trade for line-shift stability: line-number-keyed ids would churn on EVERY
    # edit anywhere above; appearance-order ids churn only when a same-named
    # sibling is inserted earlier in the file, and only within that one collision
    # family (a def whose raw id never collides is never renumbered by anything).
    # Pinned by test_inserting_same_named_branch_above_renumbers_previously_solo_def.
    #
    # KNOWN LIMITATION (CALLS attribution -- user-facing note in README
    # "Ограничения"): disambiguation exists only HERE, at node emission.
    # extractors/calls.py derives a CALLS edge's dst purely from the call-site's
    # ref SYMBOL text (`symbol_to_node_id`, a pure function over the symbol
    # string -- ids.disambiguate is unreachable from that path), and scip
    # resolves EVERY caller's ref into a collision family to the ONE
    # control-flow-insensitive symbol all branches share -- so every such CALLS
    # edge lands on the FIRST-appearing branch's unsuffixed node. That node is
    # guaranteed staged (the first occurrence keeps exactly the id the symbol
    # maps to -- no dangling edges), but branch-2+ nodes are CALLS-unreachable:
    # fully present as nodes/CONTAINS/chunks, never a CALLS endpoint (calls.py's
    # `_caller_id` recomputes the SRC side from the symbol/structural descriptor
    # the same way, so a call-site INSIDE a branch-2+ def is likewise attributed
    # to the first branch's node). Attributing a call to the branch the runtime
    # would actually pick needs flow-sensitive analysis (which branch is live
    # under which config) -- out of scope here, and inexpressible in scip's own
    # symbol model.
    def_ids: dict[int, str] = {}
    _raw_id_occurrences: dict[str, int] = {}
    for d in facts.defs:
        raw_id = _def_id(ctx, d, dotted)
        occurrence = _raw_id_occurrences.get(raw_id, 0) + 1
        _raw_id_occurrences[raw_id] = occurrence
        def_ids[d.index] = raw_id if occurrence == 1 else ids.disambiguate(raw_id, occurrence)
    for d in facts.defs:
        nid = def_ids[d.index]
        nesting = nesting_chain(facts.defs, d)
        nodes.append(NodeRec(
            id=nid,
            kind="Class" if d.kind == "class" else "Function",
            service=service, name=d.name,
            qualified_name=dotted + "." + ".".join(name for _, name in nesting),
            relpath=ctx.relpath, start_byte=d.start_byte, end_byte=d.end_byte,
            start_line=d.start_line, end_line=d.end_line,
            content_hash=hashlib.sha256(source[d.start_byte:d.end_byte]).hexdigest(),
            props={
                "signature": d.signature,
                "docstring": d.docstring,
                "is_async": d.is_async,
                "decorators": d.decorators,
            },
        ))
        parent_id = def_ids[d.parent] if d.parent is not None else module_id
        add_edge(parent_id, nid, "CONTAINS")

    # -- imports --
    for imp in facts.imports:
        target = _resolve_relative(package, imp.target_module)
        if imp.names:
            for name in imp.names:
                candidate = f"{target}.{name}" if target else name
                if ctx.module_exists(candidate):
                    dst_module = candidate
                elif ctx.module_exists(target):
                    dst_module = target
                else:
                    stats["imports_external"] += 1
                    continue
                add_edge(module_id, _module_node_id(service, dst_module),
                          "IMPORTS", imp.start_line)
        elif ctx.module_exists(target):
            add_edge(module_id, _module_node_id(service, target), "IMPORTS", imp.start_line)
        else:
            stats["imports_external"] += 1

    stats["nodes"] = len(nodes)
    stats["edges"] = len(edges)
    return ExtractionResult(nodes=nodes, edges=edges, stats=stats)
