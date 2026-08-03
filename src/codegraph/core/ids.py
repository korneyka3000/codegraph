"""Стабильные ID узлов: sym:<service>:<scip-descriptors>.

Дескрипторы совпадают с форматом scip-python (без package/version), поэтому
id, построенный экстрактором структурно, равен id, выведенному из SCIP-символа.

M2: Channel/BusinessProcess id-хелперы (chan_kafka/chan_event/chan_http/proc_id) --
единственное место, где строится текст этих id (см. core/schema.py make_channel_node/
make_process_node, единственные вызыватели). Формат зафиксирован планом M2 (Global
Constraints): "chan:kafka_topic:<name>" / "chan:event_type:<name>" /
"chan:http:<owner|?>:<METHOD> <template>"; "proc:<slug>".

M5 T3 (pilot Bug 7.1): `disambiguate` -- ordinal-suffix helper for within-file id
collisions (same-named class/function redefined in mutually-exclusive if/elif
branches). Единственный вызыватель -- extractors/python_core.py::extract, см. её
собственный докстринг/комментарий у `def_ids` за полным разбором первопричины.

M7 T4 (OPEN R3): `chan_temporal_signal` -- same name-only shape as chan_kafka/
chan_event ("chan:temporal_signal:<name>"), added for Temporal signal/update
handler+sender channels (see extractors/temporal_ext.py's module docstring).

M11 T2 (review fix, relative-import provenance): `containing_package` +
`resolve_relative_import` -- PROMOTED verbatim from extractors/python_core.py
(its own module-local `_resolve_relative` + the inline package-derivation in
`extract()`), where they built IMPORTS edges since M1a. Moved here, to the
bottom layer both consumers already import, because parsing/module_singletons.py's
receiver-provenance check needs the IDENTICAL normalization and the codebase's
layering rule forbids a parsing->extractors import (see class_attrs.py's own
`_nesting_chain` docstring for the rule; unlike that 6-line tree walk, this is
subtle dot arithmetic where a drifting duplicate would be a real correctness
risk, so the shared-single-source move is the right trade here). python_core
now calls these through `ids.` -- one formula, two consumers, no drift.
"""

from __future__ import annotations

import re


def relpath_to_module(relpath: str) -> str:
    p = relpath[:-3] if relpath.endswith(".py") else relpath
    if p.endswith("/__init__"):
        p = p[: -len("/__init__")]
    return p.replace("/", ".")


def containing_package(relpath: str) -> str:
    """Dotted CONTAINING PACKAGE of a module, per Python's own relative-import
    semantics: a package's `__init__.py` IS its package (`relpath_to_module`
    already strips the "/__init__" tail, so its dotted form is the package
    itself); a regular module's package is its dotted parent ("" for a top-level
    module). Extracted byte-for-byte from extractors/python_core.py::extract's
    own inline derivation (M11 T2 review fix -- see module docstring)."""
    dotted = relpath_to_module(relpath)
    if relpath.endswith("/__init__.py") or relpath == "__init__.py":
        return dotted
    return dotted.rsplit(".", 1)[0] if "." in dotted else ""


def resolve_relative_import(package: str, target: str) -> str:
    """Резолвинг относительного импорта против СОДЕРЖАЩЕГО ПАКЕТА (семантика Python):
    один лидирующий '.' — сам package, каждая следующая точка — уровень выше.
    package = dotted для __init__.py, parent(dotted) для обычного модуля ("" для
    top-level) — ровно то, что `containing_package` выше выводит из relpath.
    Абсолютный target возвращается как есть. (Перенесено 1:1 из
    extractors/python_core.py::_resolve_relative, M11 T2 review fix — см.
    модульный докстринг.)"""
    if not target.startswith("."):
        return target
    dots = len(target) - len(target.lstrip("."))
    rest = target.lstrip(".")
    base = package.split(".") if package else []
    up = dots - 1  # level 1 = сам package
    base = base[: len(base) - up] if up <= len(base) else []
    return ".".join([*base, rest] if rest else base)


def module_descriptor(module_dotted: str) -> str:
    return f"`{module_dotted}`/"


def structural_descriptor(module_dotted: str, nesting: list[tuple[str, str]]) -> str:
    d = module_descriptor(module_dotted)
    for kind, name in nesting:
        d += f"{name}#" if kind == "class" else f"{name}()."
    return d


def node_id(service: str, descriptors: str) -> str:
    return f"sym:{service}:{descriptors}"


def local_id(service: str, relpath: str, local: str) -> str:
    return f"sym:{service}:{relpath}:{local.replace(' ', '')}"


def disambiguate(base_id: str, ordinal: int) -> str:
    """`ordinal`-th (2, 3, ...) def within one file to independently compute the
    SAME `base_id` -- the first occurrence is returned unsuffixed by the caller and
    never passed through here (see extractors/python_core.py::extract's own
    seen-set loop, the only caller). "~" is safe to append to the END of an
    already-finished id string: it can never appear in a legitimately generated
    (non-disambiguated) id, because every id-construction path above builds its
    descriptors out of either a Python identifier (which cannot contain "~" --
    not a valid token character) or this format's own separator punctuation
    ("`", "/", "#", "().", " " stripped by local_id) -- none of it "~" either.
    That makes this trivially reversible/greppable too (strip a trailing
    "~\\d+$" to recover the base id two-or-more defs collided on)."""
    return f"{base_id}~{ordinal}"


def display_qualified(descriptors: str) -> str:
    s = descriptors.replace("`", "").replace("().", ".").replace("#", ".").replace("/", ".")
    return s.strip(".")


_SLUG_WHITESPACE = re.compile(r"\s+")
_SLUG_INVALID = re.compile(r"[^a-z0-9-]+")
_SLUG_REPEATED_HYPHENS = re.compile(r"-{2,}")


def slugify(name: str) -> str:
    """Лат.-цифры-дефисы, пробелы -> дефис, lower. Прочие символы (включая "_") --
    вырезаны, не заменены на дефис. Повторяющиеся дефисы схлопнуты в один, ведущие/
    хвостовые -- срезаны."""
    s = name.strip().lower()
    s = _SLUG_WHITESPACE.sub("-", s)
    s = _SLUG_INVALID.sub("", s)
    s = _SLUG_REPEATED_HYPHENS.sub("-", s)
    return s.strip("-")


def chan_kafka(name: str) -> str:
    return f"chan:kafka_topic:{name}"


def chan_event(name: str) -> str:
    return f"chan:event_type:{name}"


def chan_temporal_signal(name: str) -> str:
    return f"chan:temporal_signal:{name}"


def chan_http(owner: str | None, method: str, template: str) -> str:
    owner_part = owner if owner is not None else "?"
    return f"chan:http:{owner_part}:{method} {template}"


def proc_id(slug: str) -> str:
    return f"proc:{slug}"
