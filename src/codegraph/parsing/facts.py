"""FileFacts: структурные факты одного файла из tree-sitter AST (ручной обход)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DefFact:
    index: int
    kind: str  # "class" | "function"
    name: str
    name_start_byte: int
    name_end_byte: int
    start_byte: int
    end_byte: int
    start_line: int  # 1-based
    end_line: int
    parent: int | None
    is_async: bool
    signature: str
    docstring: str | None
    decorators: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CallFact:
    callee_name: str
    callee_start_byte: int
    callee_end_byte: int
    start_line: int
    enclosing_def: int | None


@dataclass(frozen=True)
class ImportFact:
    target_module: str
    names: list[str]
    start_line: int


@dataclass(frozen=True)
class FileFacts:
    relpath: str
    module_docstring: str | None
    defs: list[DefFact]
    calls: list[CallFact]
    imports: list[ImportFact]


def _strip_string(text: str) -> str:
    t = text.lstrip("rbufRBUF")
    for q in ('"""', "'''", '"', "'"):
        if t.startswith(q) and t.endswith(q) and len(t) >= 2 * len(q):
            return t[len(q):-len(q)]
    return t


def _docstring_of_block(block, source: bytes) -> str | None:
    for child in block.named_children:
        if child.type != "expression_statement":
            return None
        inner = child.named_children[0] if child.named_children else None
        if inner is not None and inner.type == "string":
            return _strip_string(inner.text.decode("utf-8", errors="replace"))
        return None
    return None


def build_file_facts(relpath: str, source: bytes) -> FileFacts:
    from codegraph.parsing.ts import parse

    tree = parse(source)
    root = tree.root_node
    defs: list[DefFact] = []
    calls: list[CallFact] = []
    imports: list[ImportFact] = []

    def visit(node, parent_def: int | None, decorators: list[str]):
        if node.type == "decorated_definition":
            decs = [
                d.text.decode()[1:].strip()
                for d in node.children
                if d.type == "decorator"
            ]
            definition = node.child_by_field_name("definition")
            if definition is not None:
                visit(definition, parent_def, decs)
            return

        if node.type in ("class_definition", "function_definition"):
            name_node = node.child_by_field_name("name")
            body = node.child_by_field_name("body")
            is_async = node.children and node.children[0].type == "async"
            if node.type == "function_definition":
                params = node.child_by_field_name("parameters")
                sig = (
                    "def "
                    + source[name_node.start_byte:params.end_byte].decode(
                        "utf-8", errors="replace"
                    )
                    if params is not None
                    else "def " + name_node.text.decode()
                )
            else:
                sig = "class " + name_node.text.decode()
            idx = len(defs)
            defs.append(DefFact(
                index=idx,
                kind="class" if node.type == "class_definition" else "function",
                name=name_node.text.decode(),
                name_start_byte=name_node.start_byte,
                name_end_byte=name_node.end_byte,
                start_byte=node.start_byte,
                end_byte=node.end_byte,
                start_line=node.start_point[0] + 1,
                end_line=node.end_point[0] + 1,
                parent=parent_def,
                is_async=bool(is_async),
                signature=sig,
                docstring=_docstring_of_block(body, source) if body else None,
                decorators=decorators,
            ))
            if body is not None:
                for ch in body.children:
                    visit(ch, idx, [])
            return

        if node.type == "call":
            fn = node.child_by_field_name("function")
            token = None
            if fn is not None and fn.type == "identifier":
                token = fn
            elif fn is not None and fn.type == "attribute":
                token = fn.child_by_field_name("attribute")
            if token is not None:
                calls.append(CallFact(
                    callee_name=token.text.decode(),
                    callee_start_byte=token.start_byte,
                    callee_end_byte=token.end_byte,
                    start_line=node.start_point[0] + 1,
                    enclosing_def=parent_def,
                ))
            # аргументы могут содержать вложенные вызовы/дефы — обходим дальше

        if node.type == "import_statement":
            for ch in node.named_children:
                if ch.type == "dotted_name":
                    imports.append(ImportFact(ch.text.decode(), [], node.start_point[0] + 1))
                elif ch.type == "aliased_import":
                    dn = ch.child_by_field_name("name")
                    if dn is not None:
                        imports.append(ImportFact(dn.text.decode(), [], node.start_point[0] + 1))
            return

        if node.type == "import_from_statement":
            # Реальная грамматика tree-sitter-python 0.25.0 (проверено node-walk'ом
            # пробных сниппетов, см. отчёт задачи):
            #   from a.b import c, d
            #     import_from_statement[module_name=dotted_name"a.b", name=dotted_name"c",
            #                            name=dotted_name"d"]
            #   from . import x       -> module_name=relative_import[import_prefix"."]
            #   from .sub import y    -> module_name=relative_import[import_prefix".",
            #                            dotted_name"sub"] — .text уже даёт ".sub" целиком
            #   from ..pkg.sub import z -> module_name=relative_import.text == "..pkg.sub"
            #   from a import x as y  -> name=aliased_import[name=dotted_name"x", alias="y"]
            #   from a import (c, d)  -> скобки анонимны, не входят в named_children
            #   from a import *       -> второй элемент wildcard_import (не dotted_name/
            #                            aliased_import) — пропускается
            # Итог: relative_import.text уже содержит ведущие точки + хвост модуля —
            # ручной подсчёт точек (как в эскизе брифа) не нужен и был бы ошибочным
            # (совпадал бы с точками внутри абсолютного dotted-имени).
            module_node = node.child_by_field_name("module_name")
            target = (
                module_node.text.decode("utf-8", errors="replace")
                if module_node is not None
                else ""
            )
            names: list[str] = []
            for ch in node.named_children:
                if ch == module_node:
                    continue
                if ch.type == "dotted_name":
                    names.append(ch.text.decode("utf-8", errors="replace"))
                elif ch.type == "aliased_import":
                    name_node = ch.child_by_field_name("name")
                    if name_node is not None:
                        names.append(name_node.text.decode("utf-8", errors="replace"))
                # wildcard_import ("from a import *"): не даёт конкретного имени — пропускаем
            imports.append(ImportFact(target, names, node.start_point[0] + 1))
            return

        for ch in node.children:
            visit(ch, parent_def, [])

    for top in root.children:
        visit(top, None, [])

    return FileFacts(
        relpath=relpath,
        module_docstring=_docstring_of_block(root, source),
        defs=defs,
        calls=calls,
        imports=imports,
    )
