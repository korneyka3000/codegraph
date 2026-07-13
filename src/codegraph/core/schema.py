"""IR узлов/рёбер и константы схемы. Единый словарь для staging, load и eval."""

from __future__ import annotations

from dataclasses import dataclass, field

SCHEMA_VERSION = 1
NODE_KINDS = frozenset({"Service", "Module", "Class", "Function"})
EDGE_TYPES = frozenset({"CONTAINS", "IMPORTS", "CALLS"})
RESOLUTIONS = frozenset({"static", "dynamic", "heuristic", "trace_validated"})


@dataclass(frozen=True)
class NodeRec:
    id: str
    kind: str
    service: str
    name: str
    qualified_name: str
    relpath: str | None = None
    start_byte: int | None = None
    end_byte: int | None = None
    start_line: int | None = None  # 1-based
    end_line: int | None = None
    content_hash: str | None = None
    props: dict = field(default_factory=dict)


@dataclass(frozen=True)
class EdgeRec:
    src: str
    dst: str
    type: str
    resolution: str
    confidence: float
    extractor: str
    evidence_file: str | None = None
    evidence_line: int | None = None  # 1-based
    props: dict = field(default_factory=dict)


def make_service_node(service: str) -> NodeRec:
    return NodeRec(
        id=f"svc:{service}", kind="Service", service=service,
        name=service, qualified_name=service,
    )
