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
from codegraph.parsing.class_attrs import ClassAttrIndex
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


def resolve_value_spec(
    spec: ValueSpec, call: CallFact, consts: ConstTable,
    class_attr_index: ClassAttrIndex | None = None,
) -> Resolved:
    """const → value; arg/kwarg → найти ArgFact на call → resolve_arg; env → config_ref;
    settings (M7 T2, OPEN R2) → resolve_settings_source (ClassAttrIndex lookup, needs
    NO call-site at all -- see that function's own docstring); enum_ → defensively
    unresolved (NOT a single-value source -- kafka_ext.py fans it out itself, see
    kafka_ext._emit_enum_fanout_produces; reaching here means a caller that should
    have special-cased enum_ FIRST did not, see ValueSpec.enum_'s own docstring);
    attr — http-идиома receiver-атрибута (T6): здесь заглушка unresolved.

    `class_attr_index` (M7 T2): trailing optional param, default None -- every
    pre-existing call site (this module's own non-settings branches never touch it)
    keeps working unchanged; only the settings branch reads it, and even then
    gracefully degrades to unresolved when it's None (no ClassAttrIndex wired at
    all) rather than raising."""
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
    if spec.settings is not None:
        return resolve_settings_source(spec.settings, class_attr_index)
    if spec.enum_ is not None:
        return Resolved(kind="unresolved")
    # spec.attr: ValueSpec._exactly_one гарантирует, что это единственная оставшаяся
    # ветка — receiver-атрибут разрешается только в T6 (http-client extractor).
    return Resolved(kind="unresolved")


def resolve_settings_source(
    settings_ref: str, class_attr_index: ClassAttrIndex | None,
) -> Resolved:
    """M7 T2 (OPEN R2): `settings_ref` is "<ClassFQN>.<field>" -- split on the LAST
    dot (the field name itself can never legitimately contain a dot, so
    `str.rpartition(".")`'s "everything before the last dot" is exactly the class
    FQN even when the FQN itself is deeply dotted, e.g.
    "app.config.kafka.KafkaSettings.step_topic" -> class_fqn=
    "app.config.kafka.KafkaSettings", field="step_topic"). Public (not
    underscore-prefixed) because kafka_ext.py's base_class consumer topic path
    (M6 T3) has no CallFact at all to satisfy resolve_value_spec's signature --
    that call-site-less path calls this directly, exactly the same lookup
    resolve_value_spec's OWN settings branch performs above (single source of
    truth, no logic duplicated between the two call sites).

    Three-way outcome, per ClassAttrIndex.settings_field (M7 T1)'s own
    SettingsField(default, env_name) shape:
      - a literal default is present -> Resolved(kind="value") -- a real static
        literal, exactly as trustworthy as any other code-literal ValueSpec source.
      - no default, but env_name is present (the field carries an env-derived name
        via env_prefix or an explicit alias, T1) -> Resolved(kind="config_ref",
        config_ref=env_name) -- the IDENTICAL shape resolve_value_spec's own `env:`
        source already produces (same downstream channel-name/confidence/props
        handling in kafka_ext.py, no new machinery needed).
      - neither (a genuinely bare field, OR class_attr_index is None, OR the class/
        field isn't found in it at all -- unknown class, unknown field, or an
        ambiguous suffix match, ClassAttrIndex.settings_field's own None contract)
        -> Resolved(kind="unresolved") -- an honest miss, not a crash; callers reuse
        their EXISTING unresolved-value counters for this (kafka_ext.py's
        producer_unresolved_channel/consumer_unresolved_topic), no new counter."""
    if class_attr_index is None:
        return Resolved(kind="unresolved")
    class_fqn, _, field_name = settings_ref.rpartition(".")
    field_obj = class_attr_index.settings_field(class_fqn, field_name)
    if field_obj is None:
        return Resolved(kind="unresolved")
    if field_obj.default is not None:
        return Resolved(kind="value", value=field_obj.default)
    if field_obj.env_name is not None:
        return Resolved(kind="config_ref", config_ref=field_obj.env_name)
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
