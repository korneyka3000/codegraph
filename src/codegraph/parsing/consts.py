"""ConstTable + argument-value resolution: частичное статическое вычисление ArgFact-значений.

Не полноценный интерпретатор — резолвит именно те формы, что нужны идиомам M2:
строковые литералы, module-level именованные константы, f-string URL/topic-шаблоны
(с `<base>`-маркером для ведущей интерполяции — типичный паттерн `self._base_url`
HTTP-клиентов) и текстовые обёртки над переменными окружения/настройками
(`os.environ[...]`, `os.getenv(...)`, `settings.X`).

`ConstTable.build` берёт `source` независимо от `facts`: `FileFacts.assigns`
(facts.py) — только call-формы присваиваний (`name = Callee(...)`), а
module-level `NAME = "literal"` там принципиально не хранится, так что здесь
свой отдельный top-level-проход по дереву. `facts` в сигнатуре — по контракту
брифа (задел на будущее cross-referencing); в этой реализации не используется.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

from codegraph.config.models import ValueSpec
from codegraph.parsing.facts import (
    ArgFact,
    CallFact,
    FileFacts,
    is_fstring_node,
    string_literal_value,
)

_ENVIRON_RE = re.compile(r'os\.environ\[\s*["\']([^"\']+)["\']\s*\]')
_GETENV_RE = re.compile(r'os\.getenv\(\s*["\']([^"\']+)["\']')
_SETTINGS_RE = re.compile(r'settings\.([A-Za-z_][A-Za-z0-9_]*)')


@dataclass(frozen=True)
class ConstTable:
    """module-level `NAME = "literal"` (строковый литерал, не f-string) одного файла."""

    values: dict[str, str] = field(default_factory=dict)

    def get(self, name: str) -> str | None:
        return self.values.get(name)

    @staticmethod
    def build(facts: FileFacts, source: bytes) -> ConstTable:
        from codegraph.parsing.ts import parse

        del facts  # см. докстринг модуля — контракт брифа, здесь не требуется
        tree = parse(source)
        values: dict[str, str] = {}
        for top in tree.root_node.children:
            if top.type != "expression_statement" or not top.named_children:
                continue
            inner = top.named_children[0]
            if inner.type != "assignment":
                continue
            left = inner.child_by_field_name("left")
            right = inner.child_by_field_name("right")
            if left is None or left.type != "identifier" or right is None:
                continue
            if right.type != "string" or is_fstring_node(right):
                continue
            values[left.text.decode("utf-8", errors="replace")] = string_literal_value(right)
        return ConstTable(values=values)


@dataclass(frozen=True)
class Resolved:
    kind: Literal["value", "template", "config_ref", "unresolved"]
    value: str | None = None
    config_ref: str | None = None


def resolve_arg(arg: ArgFact, consts: ConstTable) -> Resolved:
    """string → value; name найден в consts → value; fstring → template
    (`<base>`-маркер при ведущей интерполяции); иначе текстовый поиск
    os.environ[...]/os.getenv(...)/settings.X → config_ref; иначе unresolved."""
    if arg.value_kind == "string":
        return Resolved(kind="value", value=arg.string_value)
    if arg.value_kind == "name":
        const_value = consts.get(arg.text)
        if const_value is not None:
            return Resolved(kind="value", value=const_value)
    if arg.value_kind == "fstring":
        return Resolved(kind="template", value=_fstring_template(arg.text))
    config_name = _detect_config_ref(arg.text)
    if config_name is not None:
        return Resolved(kind="config_ref", config_ref=config_name)
    return Resolved(kind="unresolved")


def resolve_value_spec(spec: ValueSpec, call: CallFact, consts: ConstTable) -> Resolved:
    """const → value; arg/kwarg → найти ArgFact на call → resolve_arg; env → config_ref;
    attr — http-идиома receiver-атрибута (T6): здесь заглушка unresolved."""
    if spec.const is not None:
        return Resolved(kind="value", value=spec.const)
    if spec.arg is not None:
        arg = next((a for a in call.args if a.index == spec.arg), None)
        return resolve_arg(arg, consts) if arg is not None else Resolved(kind="unresolved")
    if spec.kwarg is not None:
        arg = next((a for a in call.args if a.keyword == spec.kwarg), None)
        return resolve_arg(arg, consts) if arg is not None else Resolved(kind="unresolved")
    if spec.env is not None:
        return Resolved(kind="config_ref", config_ref=spec.env)
    # spec.attr: ValueSpec._exactly_one гарантирует, что это единственная оставшаяся
    # ветка — receiver-атрибут разрешается только в T6 (http-client extractor).
    return Resolved(kind="unresolved")


def _detect_config_ref(text: str) -> str | None:
    m = _ENVIRON_RE.search(text)
    if m:
        return m.group(1)
    m = _GETENV_RE.search(text)
    if m:
        return m.group(1)
    m = _SETTINGS_RE.search(text)
    if m:
        return m.group(1)
    return None


def _fstring_template(text: str) -> str:
    """Re-parse раз изолированного `text` (полный raw source значения ArgFact,
    включая f/кавычки) — ArgFact контрактно не хранит узел дерева, а текст
    f-строки сам по себе валиден как standalone-выражение (те же грамматические
    формы, что и при первом обходе)."""
    from codegraph.parsing.ts import parse

    tree = parse(text.encode("utf-8"))
    root_children = tree.root_node.named_children
    expr_stmt = root_children[0] if root_children else None
    stmt_children = expr_stmt.named_children if expr_stmt is not None else []
    string_node = stmt_children[0] if stmt_children else None
    if string_node is None or string_node.type != "string":
        return text

    parts: list[str] = []
    seen_content = False
    for ch in string_node.children:
        if ch.type == "string_content":
            parts.append(ch.text.decode("utf-8", errors="replace"))
            seen_content = True
        elif ch.type == "interpolation":
            if not seen_content:
                parts.append("<base>")
            else:
                expr = ch.child_by_field_name("expression")
                parts.append("{" + _interpolation_name(expr) + "}")
            seen_content = True
    return "".join(parts)


def _interpolation_name(expr) -> str:
    if expr is None:
        return ""
    if expr.type == "identifier":
        return expr.text.decode("utf-8", errors="replace")
    if expr.type == "attribute":
        last = expr.child_by_field_name("attribute")
        if last is not None:
            return last.text.decode("utf-8", errors="replace")
    # иначе (subscript/call/binary_operator/...) — сырой текст выражения, без
    # попытки дальнейшей нормализации (не в контракте брифа, ни один фикстурный
    # файл M2 не требует большего, чем identifier/attribute).
    return expr.text.decode("utf-8", errors="replace")
