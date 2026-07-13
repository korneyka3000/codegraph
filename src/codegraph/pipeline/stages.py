"""Реестр стадий пайплайна; функции стадий добавятся в M1b."""

STAGES = [
    ("S1", "discover", "конфиг / zero-config, валидация путей"),
    ("S2", "scan", "обход .py, sha256"),
    ("S3", "resolve", "scip-python per service"),
    ("S4", "read-scip", "protobuf → defs/refs"),
    ("S5", "parse+extract", "tree-sitter, идиомы → claims"),
    ("S6", "join", "SCIP refs × call-sites → CALLS"),
    ("S7", "link", "каналы, роуты, NEXT_SEGMENT, процессы"),
    ("S8", "chunk+embed", "AST-чанки + эмбеддинги"),
    ("S9", "load", "UNWIND-батчи → FalkorDB (blue/green)"),
    ("S10", "report", "качество графа"),
]
