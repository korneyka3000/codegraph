"""Разбор SCIP-symbol-строк: '<scheme> <manager> <package> <version> <descriptors>' | 'local N'."""

from __future__ import annotations

from dataclasses import dataclass

from codegraph.core import ids


@dataclass(frozen=True)
class ParsedSymbol:
    is_local: bool
    local: str | None = None
    scheme: str | None = None
    manager: str | None = None
    package: str | None = None
    version: str | None = None
    descriptors: str | None = None


def parse_symbol(sym: str) -> ParsedSymbol:
    if sym.startswith("local "):
        return ParsedSymbol(is_local=True, local=sym)
    parts = sym.split(" ", 4)
    if len(parts) != 5:
        raise ValueError(f"malformed SCIP symbol: {sym!r}")
    scheme, manager, package, version, descriptors = parts
    return ParsedSymbol(
        is_local=False, scheme=scheme, manager=manager,
        package=package, version=version, descriptors=descriptors,
    )


def symbol_to_node_id(service: str, relpath: str, sym: str) -> str:
    p = parse_symbol(sym)
    if p.is_local:
        return ids.local_id(service, relpath, p.local)
    return ids.node_id(service, p.descriptors)
