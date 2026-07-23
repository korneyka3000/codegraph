"""FileContext / ExtractionResult: входной контекст и результат per-file экстрактора."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.parsing.class_attrs import ClassAttrIndex
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
    # M7 T1 sanctioned extension: the SERVICE-WIDE class_attrs index (parsing/
    # class_attrs.py -- pydantic-Settings fields + Enum/StrEnum values), built ONCE
    # per analyze_service call (pipeline/analyze.py's own S5 pre-loop pass, from
    # staged "class_attrs" claims) and handed identically to EVERY file's FileContext
    # in that call -- not per-file, so a claim harvested from one file is visible from
    # every other file's ctx in the same service. Default None (same "every
    # pre-existing call site keeps working unchanged" convention as ref_symbol_lookup
    # above) for any FileContext built outside analyze.py's own wiring (e.g. the
    # extractor unit tests' own `_load` helpers) -- this task ships no consumer that
    # reads it yet (T2/T3, later in M7).
    class_attr_index: ClassAttrIndex | None = None


@dataclass(frozen=True)
class ExtractionResult:
    nodes: list[NodeRec]
    edges: list[EdgeRec]
    stats: dict[str, int]
