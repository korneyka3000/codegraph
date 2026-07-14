"""Стабильные ID узлов: sym:<service>:<scip-descriptors>.

Дескрипторы совпадают с форматом scip-python (без package/version), поэтому
id, построенный экстрактором структурно, равен id, выведенному из SCIP-символа.

M2: Channel/BusinessProcess id-хелперы (chan_kafka/chan_event/chan_http/proc_id) --
единственное место, где строится текст этих id (см. core/schema.py make_channel_node/
make_process_node, единственные вызыватели). Формат зафиксирован планом M2 (Global
Constraints): "chan:kafka_topic:<name>" / "chan:event_type:<name>" /
"chan:http:<owner|?>:<METHOD> <template>"; "proc:<slug>".
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
