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
    # M6 T3 sanctioned extension (GAPS §4/pilot gap 4 pre-step): raw source TEXT of
    # each of a class's base expressions (see module-level `_base_exprs`) -- e.g.
    # ("BaseConsumer[OCRDataEvent]",) for `class C(BaseConsumer[OCRDataEvent])`.
    # Empty tuple for functions (no bases at all) and for a class with no bases
    # (`class C:` / `class C():`). Text only -- no byte spans: the base name token's
    # absolute position (needed by kafka_ext's scip ref-lookup) is recovered by a
    # SEPARATE, narrowly-scoped tree-sitter walk over the file there (mirrors
    # ConstTable.build's own independent pass, see extractors/kafka_ext.py's
    # `_scan_class_bases`) -- kept out of FileFacts on purpose, additive and minimal
    # per this task's brief. New LAST field with a default so every pre-existing
    # positional/keyword DefFact(...) construction still works (same precedent as
    # ParamFact.annotation_start_byte, M2 T4).
    base_exprs: tuple[str, ...] = ()


@dataclass(frozen=True)
class ParamFact:
    name: str
    annotation_text: str | None
    default_text: str | None
    default_start_byte: int | None
    default_end_byte: int | None
    # M2 T4 sanctioned extension: byte span of the annotation expression itself (e.g.
    # `Annotated[Session, Depends(get_db)]`) -- needed for fastapi_ext's DEPENDS_ON
    # lookup on the annotation-only form (no `=` default present). New LAST field with
    # a default so every pre-existing positional ParamFact(...) construction still works.
    annotation_start_byte: int | None = None


@dataclass(frozen=True)
class CallFact:
    callee_name: str
    callee_start_byte: int
    callee_end_byte: int
    start_line: int
    enclosing_def: int | None
    args: list[ArgFact] = field(default_factory=list)
    receiver_text: str | None = None
    # M8 T1 sanctioned additive extension (mirrors ArgFact's own "attr" value_kind
    # convention: a dotted receiver's span is its LAST segment, the actual object
    # being referenced -- "self._db" resolves "_db", not "self"): byte span of the
    # receiver TOKEN itself (identifier, or an attribute chain's last segment).
    # receiver_text alone is raw text with no position, insufficient for
    # ctx.ref_symbol_lookup (linking/router_prefix.py's router_include parent-symbol
    # resolution: "app" in `app.include_router(...)`). None whenever the receiver is
    # neither a bare identifier nor an attribute chain (a subscript/call-expression
    # receiver, e.g. `get_routers()[0].include_router(...)`) -- honestly "not a
    # resolvable shape", never a guess. New LAST fields with defaults so every
    # pre-existing positional/keyword CallFact(...) construction site keeps working
    # unchanged.
    receiver_start_byte: int | None = None
    receiver_end_byte: int | None = None


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
    # M8 T1 sanctioned additive extension (mirrors ParamFact.annotation_start_byte's
    # own M2 T4 precedent): byte span of the LHS target token itself -- e.g. "router"
    # in `router = APIRouter(prefix="/x")` -- needed to resolve this assignment's own
    # DEFINITION occurrence via ctx.def_symbol_lookup (linking/router_prefix.py's
    # router-symbol identity: a cross-file-stable id `include_router(...)` calls
    # elsewhere can point back at via a REFERENCE occurrence -- same descriptor-based
    # id either way, see resolvers/scip/symbols.symbol_to_node_id). New LAST field
    # with a default so every pre-existing positional AssignFact(...) construction
    # site keeps working unchanged.
    target_start_byte: int | None = None


@dataclass(frozen=True)
class SelfAttrFact:
    """`self.<attr> = <expr>` assignments -- M7 T3 sanctioned additive extension
    (mirrors ClassAttrFact's own T1 precedent): http_client_ext.py's auto-anchor
    (OPEN R1) needs to find a client class's ctor-body `self.host = config.services.
    x_url` (an ATTRIBUTE assignment TARGET, RHS an attribute-CHAIN expression),
    which neither AssignFact NOR ClassAttrFact can see -- both gate on `left.type ==
    "identifier"` (a bare name), so an attribute target was invisible to every
    existing fact type before this.

    Scope-blind, same "collection unconditional, caller filters" convention as every
    other Fact type here: populated for `self.<attr> = <expr>` wherever it textually
    occurs (module/function/class body -- a module-level `self.host = x` is
    nonsensical Python but mechanically identical at the tree-sitter level) --
    callers needing "inside THIS class's method" semantics filter by
    `enclosing_def`'s own DefFact.parent chain, same as `_enclosing_method_and_class`
    already does for CallFact in http_client_ext.py.

    Only a SIMPLE target is captured: the assignment's own `left` must be an
    `attribute` node whose `object` is the bare identifier "self" (no deeper chain --
    `self._nested.host = x` is out of scope, same "complex targets skipped"
    restriction AssignFact already applies to its own simple-identifier-target
    contract). `attr` is the single identifier right after `self.` (`self.host` ->
    "host"; ANY name, not just "host" -- `HttpClientIdiom.host_attr` configurability
    is a caller filter, not a collection gate here).

    `rhs_tail` is the LAST identifier segment of the RHS IFF it is itself a plain
    dotted-attribute chain (`config.services.x_url` -> "x_url") or a bare name (`x`
    -> "x") -- exactly what T3's auto-anchor joins through
    `ClassAttrIndex.field_by_name`. `None` for any other RHS shape (call, literal,
    subscript, ...) -- honestly "not this shape", never a guess. `rhs_text` is the
    RHS's raw source text (mirrors every other Fact type's own "keep the raw text
    alongside the decoded value" convention), `None` only when there is no RHS at
    all (never the case for an `assignment` node's own `right` field, but kept
    optional for symmetry with ClassAttrFact's `value_text`)."""

    attr: str
    rhs_tail: str | None
    rhs_text: str | None
    enclosing_def: int | None
    start_line: int


@dataclass(frozen=True)
class ClassAttrFact:
    """One simple `name[: annotation] [= value]` assignment -- ANY scope (module,
    function, or class body; `enclosing_def` is the index into `FileFacts.defs` of the
    immediately-enclosing def, `None` at module level -- exactly the same scope-
    blind-collection/caller-filters convention `AssignFact` already used, just with the
    scope itself now recorded). M7 T1 sanctioned additive extension (class_attrs
    harvesting pre-step, mirrors the M6 T3 `base_exprs` precedent): `AssignFact` turned
    out to carry NEITHER any enclosing-scope field AT ALL, NOR a representation for a
    non-call right-hand side -- a plain string default (`field: str = "x"`, the common
    pydantic-Settings/enum-member shape) or a bare annotation with no value at all
    (`field: str`, "field exists, no default") produced no fact whatsoever before this.
    A brand-new fact type (rather than broadening `AssignFact`'s own call-only
    contract) keeps `AssignFact`'s existing consumers (`idiom_match.py`,
    `fastapi_ext.py`) and its own docstring's documented shape completely untouched.

    Populated for EVERY simple-identifier assignment regardless of scope or RHS shape
    (also module/function-level, also non-string/non-call RHS) -- `class_attrs.py`'s
    own harvest is what filters to class-body-only (`enclosing_def` pointing at a
    `kind == "class"` def) and to the two RHS shapes it actually decodes; keeping
    collection itself unconditional keeps this fact self-contained and reusable for any
    FUTURE class-body-literal consumer, the same reasoning `CallFact`/`AssignFact`
    already apply to their own unconditional, caller-filtered collection.

    `has_value` is False ONLY for a bare annotation with no `=` at all (`field: str`) --
    the "field exists, no default" shape Settings semantics need distinctly from "field
    has some non-string default" (`has_value=True, string_value=None`). `string_value`
    is populated for a plain string literal RHS (never for an f-string or a
    plain+f-string `concatenated_string` mix -- mirrors `_build_argfact`'s own
    fstring-exclusion convention exactly). `call_callee`/`call_args` mirror
    `AssignFact`'s own call-shaped-RHS handling (last attribute segment or bare name;
    `call_args` via the same `_build_call_args`/`ArgFact` machinery) -- populated for
    ANY call-shaped RHS, not just `SettingsConfigDict(...)`: a per-field
    `Field(default=..., alias=...)` call needs its OWN kwargs read the identical way.
    `value_text` is the RHS's raw source text (`None` iff `has_value` is False) --
    kept for the same reason every other Fact type here keeps raw text alongside its
    decoded value (debugging/future consumers, `ArgFact.text`'s own precedent)."""

    name: str
    enclosing_def: int | None
    annotation_text: str | None
    has_value: bool
    value_text: str | None
    string_value: str | None
    call_callee: str | None
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
    # M7 T1 sanctioned additive extension (same precedent as `assigns` above): new LAST
    # field with a default -- every pre-existing positional/keyword FileFacts(...)
    # construction site (grepped before adding this: only build_file_facts' own return,
    # see below) keeps working unchanged.
    class_attrs: list[ClassAttrFact] = field(default_factory=list)
    # M7 T3 sanctioned additive extension (same precedent, SelfAttrFact's own
    # docstring has the full rationale): new LAST field with a default.
    self_attr_assigns: list[SelfAttrFact] = field(default_factory=list)


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


# -- ClassAttrFact construction (M7 T1) -------------------------------------------------
#
# `_call_callee_name` is a pure extraction of AssignFact's own pre-existing callee-name
# logic (identifier as-is; attribute reduced to its LAST segment, mirroring ArgFact's
# "attr" value_kind span convention) -- no behavior change for AssignFact's own
# construction below, just shared with ClassAttrFact's own call-RHS branch.


def _call_callee_name(fn_node) -> str | None:
    if fn_node is None:
        return None
    if fn_node.type == "identifier":
        return fn_node.text.decode("utf-8", errors="replace")
    if fn_node.type == "attribute":
        attr = fn_node.child_by_field_name("attribute")
        if attr is not None:
            return attr.text.decode("utf-8", errors="replace")
    return None


# -- SelfAttrFact construction (M7 T3) ------------------------------------------------
#
# `_dotted_tail` generalizes `_call_callee_name`'s own "attribute reduced to its LAST
# segment" convention to a non-call RHS: an `attribute` node's own "attribute" field
# IS already the rightmost identifier by grammar construction (nested `.object` chains
# of any depth do not need walking), and a bare `identifier` node is a single-segment
# "chain" of its own. Any other node type (call, string, subscript, ...) -> None,
# honestly "not a dotted-chain/bare-name shape" -- see SelfAttrFact's own docstring.


def _dotted_tail(node) -> str | None:
    if node.type == "identifier":
        return node.text.decode("utf-8", errors="replace")
    if node.type == "attribute":
        attr = node.child_by_field_name("attribute")
        return attr.text.decode("utf-8", errors="replace") if attr is not None else None
    return None


def _is_bare_self(node) -> bool:
    return node.type == "identifier" and node.text == b"self"


def _build_class_attr_fact(
    left, type_node, right, parent_def: int | None, start_line: int,
) -> ClassAttrFact:
    """`right` (the assignment's `right` field, possibly None for a bare annotation)
    classified into ClassAttrFact's shape: string-literal (`_strip_string`-free --
    `string_literal_value` already un-quotes) or call-shaped are the only two RHS
    shapes `class_attrs.py`'s harvest actually decodes; every other shape (int/bool/
    name/attr/dict/... RHS) still produces a complete fact (`has_value=True`, every
    value-field None past `value_text`) rather than being skipped, per this fact's own
    docstring."""
    base = {
        "name": left.text.decode("utf-8", errors="replace"),
        "enclosing_def": parent_def,
        "annotation_text": _text_or(type_node),
        "start_line": start_line,
    }
    if right is None:
        return ClassAttrFact(
            **base, has_value=False, value_text=None, string_value=None,
            call_callee=None, call_args=None,
        )
    value_text = right.text.decode("utf-8", errors="replace")
    is_plain_string = (
        (right.type == "string" and not is_fstring_node(right))
        or (
            right.type == "concatenated_string"
            and not any(is_fstring_node(c) for c in right.named_children if c.type == "string")
        )
    )
    if is_plain_string:
        return ClassAttrFact(
            **base, has_value=True, value_text=value_text,
            string_value=string_literal_value(right), call_callee=None, call_args=None,
        )
    if right.type == "call":
        call_fn = right.child_by_field_name("function")
        return ClassAttrFact(
            **base, has_value=True, value_text=value_text, string_value=None,
            call_callee=_call_callee_name(call_fn),
            call_args=_build_call_args(right.child_by_field_name("arguments")),
        )
    return ClassAttrFact(
        **base, has_value=True, value_text=value_text, string_value=None,
        call_callee=None, call_args=None,
    )


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
                annotation_start_byte=type_node.start_byte if type_node is not None else None,
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
                # default_parameter (bare `x=5`) never has a "type" field by grammar --
                # type_node is None there, so this stays None uniformly for that shape.
                annotation_start_byte=type_node.start_byte if type_node is not None else None,
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



# -- DefFact.base_exprs construction (M6 T3) ------------------------------------------
#
# Grammar facts (verified via probe script, tree-sitter-python 0.25): class_definition's
# bases live under its "superclasses" field (an argument_list, ABSENT entirely for a
# bare `class C:`, present-but-empty for `class C():`); each base is one of:
# identifier (bare name), attribute (dotted name), subscript (generic, e.g.
# "Base[Arg]" -- itself has value= the base expr and repeated subscript= fields for
# each bracket item, "Base[A, B]" style), or keyword_argument ("metaclass=X" and
# similar -- NOT a base, excluded). function_definition has no "superclasses" field at
# all, so `child_by_field_name("superclasses")` naturally returns None there --
# `_base_exprs` needs no kind check to stay a no-op for functions.


def _base_exprs(node) -> tuple[str, ...]:
    supers = node.child_by_field_name("superclasses")
    if supers is None:
        return ()
    return tuple(
        ch.text.decode("utf-8", errors="replace")
        for ch in supers.named_children
        if ch.type != "keyword_argument"
    )


def build_file_facts(relpath: str, source: bytes) -> FileFacts:
    from codegraph.parsing.ts import parse

    tree = parse(source)
    root = tree.root_node
    defs: list[DefFact] = []
    calls: list[CallFact] = []
    imports: list[ImportFact] = []
    assigns: list[AssignFact] = []
    class_attrs: list[ClassAttrFact] = []
    self_attr_assigns: list[SelfAttrFact] = []

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
                base_exprs=_base_exprs(node),
            ))
            if body is not None:
                for ch in body.children:
                    visit(ch, idx, [])
            return

        if node.type == "call":
            fn = node.child_by_field_name("function")
            token = None
            receiver_text = None
            receiver_start_byte = None
            receiver_end_byte = None
            if fn is not None and fn.type == "identifier":
                token = fn
            elif fn is not None and fn.type == "attribute":
                token = fn.child_by_field_name("attribute")
                obj = fn.child_by_field_name("object")
                if obj is not None:
                    receiver_text = obj.text.decode("utf-8", errors="replace")
                    # M8 T1: byte span of the receiver TOKEN itself -- a bare
                    # identifier spans whole; a dotted chain spans only its LAST
                    # segment (mirrors ArgFact's own "attr" value_kind convention).
                    # Any other receiver shape (subscript/call-expression result,
                    # e.g. "get_routers()[0]") stays span-less -- not a resolvable
                    # reference occurrence, honestly, not a guess.
                    if obj.type == "identifier":
                        receiver_start_byte, receiver_end_byte = obj.start_byte, obj.end_byte
                    elif obj.type == "attribute":
                        obj_last = obj.child_by_field_name("attribute")
                        if obj_last is not None:
                            receiver_start_byte = obj_last.start_byte
                            receiver_end_byte = obj_last.end_byte
            if token is not None:
                calls.append(CallFact(
                    callee_name=token.text.decode(),
                    callee_start_byte=token.start_byte,
                    callee_end_byte=token.end_byte,
                    start_line=node.start_point[0] + 1,
                    enclosing_def=parent_def,
                    args=_build_call_args(node.child_by_field_name("arguments")),
                    receiver_text=receiver_text,
                    receiver_start_byte=receiver_start_byte,
                    receiver_end_byte=receiver_end_byte,
                ))
            # аргументы могут содержать вложенные вызовы/дефы — обходим дальше

        if node.type == "assignment":
            # Только простые `name = Callee(...)` / `name = await Callee(...)` (любой
            # уровень) — НЕ return: правая часть (в т.ч. вложенный call) обходится ниже
            # обычной рекурсией, как и раньше (нужно для CallFact-сбора того же вызова).
            left = node.child_by_field_name("left")
            right = node.child_by_field_name("right")
            # M7 T1 sanctioned extension (ClassAttrFact's own docstring has the full
            # rationale): EVERY simple `name[: Type] [= value]` assignment, any scope/
            # RHS shape -- unlike the AssignFact branch below, `right` may be None here
            # (a bare `field: str` annotation with no value at all -- the pydantic-
            # Settings "field without a default" shape) and need not be call-shaped (a
            # plain string-literal RHS, the common Settings-default/enum-member case,
            # produces no AssignFact at all).
            if left is not None and left.type == "identifier":
                class_attrs.append(_build_class_attr_fact(
                    left, node.child_by_field_name("type"), right, parent_def,
                    node.start_point[0] + 1,
                ))
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
                    callee_name = _call_callee_name(call_fn)
                    if call_fn is not None and call_fn.type in ("identifier", "attribute"):
                        assigns.append(AssignFact(
                            target=left.text.decode("utf-8", errors="replace"),
                            callee_name=callee_name,
                            call_args=_build_call_args(call_node.child_by_field_name("arguments")),
                            start_line=node.start_point[0] + 1,
                            target_start_byte=left.start_byte,
                        ))
            # M7 T3 sanctioned additive extension: `self.<attr> = <expr>` -- see
            # SelfAttrFact's own docstring for the full rationale (http_client_ext's
            # auto-anchor join surface). Simple target only: `left` is an `attribute`
            # node whose OWN `object` is the bare identifier "self" (a deeper chain
            # like `self._nested.host = x` is out of scope, same restriction
            # AssignFact already applies to ITS OWN simple-identifier-target
            # contract) -- distinct from the identifier-target branches above, so
            # both can never fire on the same assignment.
            if left is not None and left.type == "attribute":
                obj = left.child_by_field_name("object")
                attr_node = left.child_by_field_name("attribute")
                if obj is not None and _is_bare_self(obj) and attr_node is not None:
                    self_attr_assigns.append(SelfAttrFact(
                        attr=attr_node.text.decode("utf-8", errors="replace"),
                        rhs_tail=_dotted_tail(right) if right is not None else None,
                        rhs_text=_text_or(right),
                        enclosing_def=parent_def,
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
        class_attrs=class_attrs,
        self_attr_assigns=self_attr_assigns,
    )
