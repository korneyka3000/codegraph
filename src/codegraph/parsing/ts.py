"""Единая tree-sitter-сессия для Python. Query API не используем (API-стабильность)."""

from __future__ import annotations

import tree_sitter_python as _tspython
from tree_sitter import Language, Parser, Tree

PY_LANGUAGE = Language(_tspython.language())
_parser = Parser(PY_LANGUAGE)


def parse(source: bytes) -> Tree:
    return _parser.parse(source)
