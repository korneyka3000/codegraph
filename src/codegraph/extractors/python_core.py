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


_nesting = nesting_chain  # alias for internal call sites below; public name is nesting_chain


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
    return ids.node_id(ctx.service, ids.structural_descriptor(dotted, _nesting(ctx.facts.defs, d)))


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
            evidence_file=ctx.relpath if line is not None else None,
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
    def_ids = {d.index: _def_id(ctx, d, dotted) for d in facts.defs}
    for d in facts.defs:
        nid = def_ids[d.index]
        nesting = _nesting(facts.defs, d)
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
