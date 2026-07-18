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
"""

from __future__ import annotations

import re


def relpath_to_module(relpath: str) -> str:
    p = relpath[:-3] if relpath.endswith(".py") else relpath
    if p.endswith("/__init__"):
        p = p[: -len("/__init__")]
    return p.replace("/", ".")


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


def chan_http(owner: str | None, method: str, template: str) -> str:
    owner_part = owner if owner is not None else "?"
    return f"chan:http:{owner_part}:{method} {template}"


def proc_id(slug: str) -> str:
    return f"proc:{slug}"
