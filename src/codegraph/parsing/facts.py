"""FileFacts: структурные факты одного файла из tree-sitter AST (ручной обход)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class ArgFact:
    """Один аргумент вызова (позиционный/keyword) либо пара dict-литерала.

    ``text``/``string_value`` — про VALUE-часть (для keyword_argument — без "name=").
    ``string_value`` заполнен только для value_kind == "string" (конкатенация
    string_content, без кавычек/префиксов). ``name_start_byte``/``name_end_byte`` —
    байт-спан идентификатора для value_kind in ("name", "attr") (attr — ПОСЛЕДНИЙ
    сегмент), нужен как scip-lookup join key (как M1a callee-спаны). ``dict_items`` —
    только для value_kind == "dict": список (key ArgFact, value ArgFact), каждый с
    index=None (позиция внутри dict не адресуема).
    """

    index: int | None
    keyword: str | None
    value_kind: Literal["string", "fstring", "name", "attr", "dict", "other"]
    text: str
    string_value: str | None
    name_start_byte: int | None
    name_end_byte: int | None
    dict_items: list[tuple[ArgFact, ArgFact]] | None


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
    params: list[ParamFact] = field(default_factory=list)


@dataclass(frozen=True)
class ParamFact:
    name: str
    annotation_text: str | None
    default_text: str | None
    default_start_byte: int | None
    default_end_byte: int | None


@dataclass(frozen=True)
class CallFact:
    callee_name: str
    callee_start_byte: int
    callee_end_byte: int
    start_line: int
    enclosing_def: int | None
    args: list[ArgFact] = field(default_factory=list)
    receiver_text: str | None = None


@dataclass(frozen=True)
class AssignFact:
    """Только простые `name = Callee(...)` / `name = await Callee(...)` (любой уровень
    вложенности — модуль или функция). callee_name — identifier или последний сегмент
    attribute; сложные цели (attribute/subscript/tuple) и не-call правые части
    пропускаются (не создают AssignFact)."""

    target: str
    callee_name: str | None
    call_args: list[ArgFact] | None
    start_line: int


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
    assigns: list[AssignFact] = field(default_factory=list)


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


# -- ArgFact construction ------------------------------------------------------------
#
# Грамматические факты (tree-sitter-python 0.25, проверено node-walk'ом проб-скриптов,
# см. отчёт задачи):
#   argument_list: позиционные expr — БЕЗ field-имени; keyword_argument(name=, value=);
#     list_splat("*x")/dictionary_splat("**x") — тоже без field-имён, пропускаются
#     (не адресуемы ни по index, ни по keyword; в фикстурах M2 не встречаются в call-сайтах).
#   string: string_start/string_content*/string_end — f-строка распознаётся по 'f'/'F'
#     в тексте string_start (независимо от порядка префиксных букв: rb"..", Rf'..', ...).
#     Пустая строка — без string_content вовсе (не "" ошибка, а отсутствующий child).
#   concatenated_string: смежные строковые литералы ("a" "b") — НЕ "string" узел;
#     считаем value_kind="string" только если НИ ОДНА часть не f-строка (иначе — "other",
#     смешанная f+plain конкатенация не встречается в фикстурах M2 — не резолвим).
#   dictionary: pair(key=, value=); dictionary_splat("**x") пропускается при сборе
#     dict_items (не формирует пару ключ/значение).
#   attribute: object=, attribute= (последний сегмент) — используется и для value_kind
#     "attr" (name-спан = attribute-поле), и для CallFact.receiver_text (= весь текст
#     object-поля, включая вложенные точки: "self._db").

_SPLAT_ARG_TYPES = frozenset({"list_splat", "dictionary_splat"})


def is_fstring_node(node) -> bool:
    """node.type == "string": содержит ли string_start f/F-префикс."""
    for ch in node.children:
        if ch.type == "string_start":
            return "f" in ch.text.decode("utf-8", errors="replace").lower()
    return False


def string_literal_value(node) -> str:
    """Конкатенация string_content дочерних узлов (без кавычек/префиксов); также
    разворачивает concatenated_string (смежные литералы). Не обрабатывает escape-
    последовательности — string_content уже несёт их сырой текст как есть."""
    if node.type == "concatenated_string":
        return "".join(
            string_literal_value(c) for c in node.named_children if c.type == "string"
        )
    if node.type == "string":
        return "".join(
            c.text.decode("utf-8", errors="replace")
            for c in node.children
            if c.type == "string_content"
        )
    return ""


def _build_argfact(node, index: int | None, keyword: str | None) -> ArgFact:
    text = node.text.decode("utf-8", errors="replace")
    if node.type == "string":
        if is_fstring_node(node):
            return ArgFact(index, keyword, "fstring", text, None, None, None, None)
        return ArgFact(index, keyword, "string", text, string_literal_value(node), None, None, None)
    if node.type == "concatenated_string":
        if any(is_fstring_node(c) for c in node.named_children if c.type == "string"):
            return ArgFact(index, keyword, "other", text, None, None, None, None)
        return ArgFact(index, keyword, "string", text, string_literal_value(node), None, None, None)
    if node.type == "identifier":
        return ArgFact(index, keyword, "name", text, None, node.start_byte, node.end_byte, None)
    if node.type == "attribute":
        last = node.child_by_field_name("attribute")
        if last is not None:
            return ArgFact(index, keyword, "attr", text, None, last.start_byte, last.end_byte, None)
        return ArgFact(index, keyword, "attr", text, None, None, None, None)
    if node.type == "dictionary":
        return ArgFact(index, keyword, "dict", text, None, None, None, _build_dict_items(node))
    return ArgFact(index, keyword, "other", text, None, None, None, None)


def _build_dict_items(dict_node) -> list[tuple[ArgFact, ArgFact]]:
    items: list[tuple[ArgFact, ArgFact]] = []
    for child in dict_node.named_children:
        if child.type != "pair":
            continue  # dictionary_splat ("**x") — не формирует key/value пару
        key_node = child.child_by_field_name("key")
        value_node = child.child_by_field_name("value")
        if key_node is None or value_node is None:
            continue
        items.append((_build_argfact(key_node, None, None), _build_argfact(value_node, None, None)))
    return items


def _text_or(node, default: str | None = None) -> str | None:
    return node.text.decode("utf-8", errors="replace") if node is not None else default


def _build_call_args(args_node) -> list[ArgFact]:
    if args_node is None:
        return []
    result: list[ArgFact] = []
    pos_idx = 0
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            name_node = child.child_by_field_name("name")
            value_node = child.child_by_field_name("value")
            if value_node is None:
                continue
            result.append(_build_argfact(value_node, None, _text_or(name_node)))
        elif child.type in _SPLAT_ARG_TYPES:
            continue
        else:
            result.append(_build_argfact(child, pos_idx, None))
            pos_idx += 1
    return result


# -- ParamFact construction ------------------------------------------------------------
#
# Грамматические факты (проверено node-walk'ом): "parameters" может содержать (помимо
# ',', '(', ')'):
#   identifier                    — голый параметр (напр. "self"), без field-имени.
#   typed_parameter(type=)        — ИМЯ БЕЗ field "name" (грамматическая асимметрия
#                                    относительно typed_default_parameter/default_parameter!
#                                    identifier — просто первый именованный child).
#   default_parameter(name=, value=)
#   typed_default_parameter(name=, type=, value=)
#   list_splat_pattern("*args")/dictionary_splat_pattern("**kwargs") — идентификатор
#                                    внутри, тоже без field-имени.
#   positional_separator("/")/keyword_separator("*") — маркеры-разделители, ИМЕНОВАНЫ
#                                    (named=True), но не параметры — пропускаются.

_PARAM_SEPARATOR_TYPES = frozenset({"positional_separator", "keyword_separator"})
_PARAM_SPLAT_TYPES = frozenset({"list_splat_pattern", "dictionary_splat_pattern"})


def _first_identifier_child(node):
    return next((c for c in node.named_children if c.type == "identifier"), None)


def _build_params(params_node) -> list[ParamFact]:
    result: list[ParamFact] = []
    for child in params_node.named_children:
        if child.type in _PARAM_SEPARATOR_TYPES:
            continue
        if child.type == "identifier":
            result.append(ParamFact(
                name=child.text.decode("utf-8", errors="replace"),
                annotation_text=None, default_text=None,
                default_start_byte=None, default_end_byte=None,
            ))
        elif child.type == "typed_parameter":
            name_node = child.child_by_field_name("name") or _first_identifier_child(child)
            type_node = child.child_by_field_name("type")
            result.append(ParamFact(
                name=_text_or(name_node, ""),
                annotation_text=_text_or(type_node),
                default_text=None, default_start_byte=None, default_end_byte=None,
            ))
        elif child.type in ("default_parameter", "typed_default_parameter"):
            name_node = child.child_by_field_name("name")
            type_node = child.child_by_field_name("type")
            value_node = child.child_by_field_name("value")
            result.append(ParamFact(
                name=_text_or(name_node, ""),
                annotation_text=_text_or(type_node),
                default_text=_text_or(value_node),
                default_start_byte=value_node.start_byte if value_node is not None else None,
                default_end_byte=value_node.end_byte if value_node is not None else None,
            ))
        elif child.type in _PARAM_SPLAT_TYPES:
            name_node = _first_identifier_child(child)
            result.append(ParamFact(
                name=_text_or(name_node) or _text_or(child, ""),
                annotation_text=None, default_text=None,
                default_start_byte=None, default_end_byte=None,
            ))
        # иначе (будущие/неизвестные grammar-формы) — пропускаем защитно, не падаем
    return result


def build_file_facts(relpath: str, source: bytes) -> FileFacts:
    from codegraph.parsing.ts import parse

    tree = parse(source)
    root = tree.root_node
    defs: list[DefFact] = []
    calls: list[CallFact] = []
    imports: list[ImportFact] = []
    assigns: list[AssignFact] = []

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
                params = None
                sig = "class " + name_node.text.decode()
            idx = len(defs)
            param_facts = _build_params(params) if params is not None else []
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
                params=param_facts,
            ))
            if body is not None:
                for ch in body.children:
                    visit(ch, idx, [])
            return

        if node.type == "call":
            fn = node.child_by_field_name("function")
            token = None
            receiver_text = None
            if fn is not None and fn.type == "identifier":
                token = fn
            elif fn is not None and fn.type == "attribute":
                token = fn.child_by_field_name("attribute")
                obj = fn.child_by_field_name("object")
                if obj is not None:
                    receiver_text = obj.text.decode("utf-8", errors="replace")
            if token is not None:
                calls.append(CallFact(
                    callee_name=token.text.decode(),
                    callee_start_byte=token.start_byte,
                    callee_end_byte=token.end_byte,
                    start_line=node.start_point[0] + 1,
                    enclosing_def=parent_def,
                    args=_build_call_args(node.child_by_field_name("arguments")),
                    receiver_text=receiver_text,
                ))
            # аргументы могут содержать вложенные вызовы/дефы — обходим дальше

        if node.type == "assignment":
            # Только простые `name = Callee(...)` / `name = await Callee(...)` (любой
            # уровень) — НЕ return: правая часть (в т.ч. вложенный call) обходится ниже
            # обычной рекурсией, как и раньше (нужно для CallFact-сбора того же вызова).
            left = node.child_by_field_name("left")
            right = node.child_by_field_name("right")
            if left is not None and left.type == "identifier" and right is not None:
                call_node = None
                if right.type == "call":
                    call_node = right
                elif right.type == "await" and right.named_children:
                    inner = right.named_children[0]
                    if inner.type == "call":
                        call_node = inner
                if call_node is not None:
                    call_fn = call_node.child_by_field_name("function")
                    callee_name = None
                    if call_fn is not None and call_fn.type == "identifier":
                        callee_name = call_fn.text.decode("utf-8", errors="replace")
                    elif call_fn is not None and call_fn.type == "attribute":
                        attr = call_fn.child_by_field_name("attribute")
                        if attr is not None:
                            callee_name = attr.text.decode("utf-8", errors="replace")
                    if call_fn is not None and call_fn.type in ("identifier", "attribute"):
                        assigns.append(AssignFact(
                            target=left.text.decode("utf-8", errors="replace"),
                            callee_name=callee_name,
                            call_args=_build_call_args(call_node.child_by_field_name("arguments")),
                            start_line=node.start_point[0] + 1,
                        ))

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
        assigns=assigns,
    )
