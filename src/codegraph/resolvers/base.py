"""Типы строк резолвера: то, что кладётся в staging-таблицы scip_defs/scip_refs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DefRow:
    relpath: str
    symbol: str
    start_byte: int
    end_byte: int
    start_line: int  # 1-based


@dataclass(frozen=True)
class RefRow:
    relpath: str
    symbol: str
    start_byte: int
    end_byte: int
    start_line: int  # 1-based
    roles: int
