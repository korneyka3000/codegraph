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
    # M2 T4 sanctioned extension: REF-occurrence lookup at (relpath, start_byte) -- the
    # def-vs-ref split matters because e.g. `Depends(get_db)` inside a parameter default
    # is a *reference* to get_db, not a definition; def_symbol_lookup can't answer that.
    # Default None so every pre-existing FileContext(...) call site (all keyword-based,
    # grepped before adding this field) keeps working unchanged; a None-valued lookup
    # degrades safely (fastapi_ext treats "no lookup wired" same as "lookup found nothing").
    ref_symbol_lookup: Callable[[str, int], str | None] | None = None


@dataclass(frozen=True)
class ExtractionResult:
    nodes: list[NodeRec]
    edges: list[EdgeRec]
    stats: dict[str, int]
