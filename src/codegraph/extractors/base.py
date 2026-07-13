"""FileContext / ExtractionResult: входной контекст и результат per-file экстрактора."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.parsing.facts import FileFacts


@dataclass(frozen=True)
class FileContext:
    service: str
    relpath: str
    source: bytes
    facts: FileFacts
    def_symbol_lookup: Callable[[str, int], str | None]
    module_exists: Callable[[str], bool]


@dataclass(frozen=True)
class ExtractionResult:
    nodes: list[NodeRec]
    edges: list[EdgeRec]
    stats: dict[str, int]
