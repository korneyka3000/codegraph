"""Стабильные ID узлов: sym:<service>:<scip-descriptors>.

Дескрипторы совпадают с форматом scip-python (без package/version), поэтому
id, построенный экстрактором структурно, равен id, выведенному из SCIP-символа.
"""

from __future__ import annotations


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
