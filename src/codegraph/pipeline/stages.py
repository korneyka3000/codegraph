"""Реестр стадий пайплайна.

S1/S2 (discover/scan) реализованы `scan.scan_service`; S3–S6 (resolve/read-scip/
parse+extract/join) оркестрованы вместе в `analyze.analyze_service` (per-service:
begin_service+scan → resolve SCIP или деградированный fallback → facts/extract →
join CALLS). S9/S10 реализованы `load.load_graph` / `report.build_report` +
`write_report`/`print_report` (staging → FalkorDB blue/green, затем агрегированный
отчёт качества графа). Остальные стадии (S7/S8) добавятся дальше в M1b/M3.
"""

STAGES = [
    ("S1", "discover", "конфиг / zero-config, валидация путей"),
    ("S2", "scan", "обход .py, sha256"),  # scan.scan_service
    ("S3", "resolve", "scip-python per service"),  # analyze.analyze_service
    ("S4", "read-scip", "protobuf → defs/refs"),  # analyze.analyze_service
    ("S5", "parse+extract", "tree-sitter, идиомы → claims"),  # analyze.analyze_service
    ("S6", "join", "SCIP refs × call-sites → CALLS"),  # analyze.analyze_service
    ("S7", "link", "каналы, роуты, NEXT_SEGMENT, процессы"),
    ("S8", "chunk+embed", "AST-чанки + эмбеддинги"),
    ("S9", "load", "UNWIND-батчи → FalkorDB (blue/green)"),  # load.load_graph
    ("S10", "report", "качество графа"),  # report.build_report/write_report/print_report
]
