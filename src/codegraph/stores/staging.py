"""SQLite-staging: промежуточное состояние пайплайна. FalkorDB пересоздаваем отсюда."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from codegraph.core import ids
from codegraph.core.errors import InvariantError
from codegraph.core.schema import SCHEMA_VERSION, EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow

_DDL = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS files(
  service TEXT, relpath TEXT, sha256 TEXT, size INTEGER,
  PRIMARY KEY(service, relpath));
CREATE TABLE IF NOT EXISTS scip_defs(
  service TEXT, relpath TEXT, symbol TEXT,
  start_byte INTEGER, end_byte INTEGER, start_line INTEGER,
  PRIMARY KEY(service, relpath, start_byte, symbol));
CREATE TABLE IF NOT EXISTS scip_refs(
  service TEXT, relpath TEXT, symbol TEXT,
  start_byte INTEGER, end_byte INTEGER, start_line INTEGER, roles INTEGER,
  PRIMARY KEY(service, relpath, start_byte, symbol));
CREATE INDEX IF NOT EXISTS idx_refs_file ON scip_refs(service, relpath, start_byte);
CREATE INDEX IF NOT EXISTS idx_defs_at ON scip_defs(service, relpath, start_byte);
CREATE TABLE IF NOT EXISTS nodes(
  id TEXT PRIMARY KEY, kind TEXT, labels TEXT, service TEXT,
  relpath TEXT, start_byte INTEGER, end_byte INTEGER,
  start_line INTEGER, end_line INTEGER,
  name TEXT, qualified_name TEXT, content_hash TEXT, props TEXT);
CREATE INDEX IF NOT EXISTS idx_nodes_service ON nodes(service);
CREATE TABLE IF NOT EXISTS edges(
  src TEXT, dst TEXT, type TEXT, resolution TEXT, confidence REAL,
  extractor TEXT, evidence_file TEXT, evidence_line INTEGER, props TEXT,
  src_service TEXT,
  PRIMARY KEY(src, dst, type));
CREATE INDEX IF NOT EXISTS idx_edges_service ON edges(src_service);
"""


def _id_service(node_id: str) -> str | None:
    if node_id.startswith("sym:"):
        return node_id.split(":", 2)[1]
    if node_id.startswith("svc:"):
        return node_id.split(":", 1)[1]
    return None


class Staging:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self.ensure_schema()
        self._check_schema_version()

    def ensure_schema(self) -> None:
        self._db.executescript(_DDL)
        self._db.commit()

    def _check_schema_version(self) -> None:
        current = self.get_meta("schema_version")
        if current is None:
            self.set_meta("schema_version", str(SCHEMA_VERSION))
        elif current != str(SCHEMA_VERSION):
            raise InvariantError(
                f"schema_version mismatch: staging has {current!r}, expected "
                f"{SCHEMA_VERSION!r} — recreate the staging DB (delete the file and "
                "re-run indexing from scratch)"
            )

    def close(self) -> None:
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- запись --

    def begin_service(self, service: str) -> None:
        cur = self._db
        for t in ("files", "scip_defs", "scip_refs"):
            cur.execute(f"DELETE FROM {t} WHERE service=?", (service,))  # noqa: S608
        cur.execute("DELETE FROM nodes WHERE service=?", (service,))
        cur.execute("DELETE FROM edges WHERE src_service=?", (service,))
        cur.execute("DELETE FROM edges WHERE src_service IS NULL")  # страховка от накопления
        self._db.commit()

    def add_files(self, service: str, rows: list[tuple[str, str, int]]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO files VALUES (?,?,?,?)",
            [(service, r, h, s) for r, h, s in rows],
        )
        self._db.commit()

    def add_defs(self, service: str, rows: list[DefRow]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO scip_defs VALUES (?,?,?,?,?,?)",
            [(service, d.relpath, d.symbol, d.start_byte, d.end_byte, d.start_line)
             for d in rows],
        )
        self._db.commit()

    def add_refs(self, service: str, rows: list[RefRow]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO scip_refs VALUES (?,?,?,?,?,?,?)",
            [(service, r.relpath, r.symbol, r.start_byte, r.end_byte, r.start_line,
              r.roles) for r in rows],
        )
        self._db.commit()

    def upsert_nodes(self, rows: list[NodeRec]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(n.id, n.kind, json.dumps([n.kind]), n.service, n.relpath,
              n.start_byte, n.end_byte, n.start_line, n.end_line,
              n.name, n.qualified_name, n.content_hash, json.dumps(n.props))
             for n in rows],
        )
        self._db.commit()

    def upsert_edges(self, rows: list[EdgeRec]) -> None:
        prepared = []
        for e in rows:
            ss, ds = _id_service(e.src), _id_service(e.dst)
            if ss and ds and ss != ds:
                raise InvariantError(
                    f"cross-service edge forbidden: {e.src} -{e.type}-> {e.dst}"
                )
            prepared.append((e.src, e.dst, e.type, e.resolution, e.confidence,
                             e.extractor, e.evidence_file, e.evidence_line,
                             json.dumps(e.props), ss))
        self._db.executemany(
            "INSERT OR REPLACE INTO edges VALUES (?,?,?,?,?,?,?,?,?,?)", prepared
        )
        self._db.commit()

    # -- чтение --

    def files_for_service(self, service: str) -> list[tuple[str, str]]:
        cur = self._db.execute(
            "SELECT relpath, sha256 FROM files WHERE service=? ORDER BY relpath",
            (service,))
        return list(cur.fetchall())

    def module_set(self, service: str) -> set[str]:
        return {ids.relpath_to_module(r) for r, _ in self.files_for_service(service)}

    def def_symbol_at(self, service: str, relpath: str, start_byte: int) -> str | None:
        cur = self._db.execute(
            "SELECT symbol FROM scip_defs WHERE service=? AND relpath=? AND start_byte=? "
            "ORDER BY symbol LIMIT 1",
            (service, relpath, start_byte))
        row = cur.fetchone()
        return row[0] if row else None

    def local_def_symbols(self, service: str, relpath: str) -> set[str]:
        cur = self._db.execute(
            "SELECT symbol FROM scip_defs WHERE service=? AND relpath=? "
            "AND symbol LIKE 'local %'",
            (service, relpath))
        return {row[0] for row in cur.fetchall()}

    def refs_for_file(self, service: str, relpath: str) -> list[RefRow]:
        cur = self._db.execute(
            "SELECT relpath, symbol, start_byte, end_byte, start_line, roles "
            "FROM scip_refs WHERE service=? AND relpath=? ORDER BY start_byte",
            (service, relpath))
        return [RefRow(*row) for row in cur.fetchall()]

    def counts(self) -> dict:
        out = {}
        for key, table in (("files", "files"), ("defs", "scip_defs"),
                           ("refs", "scip_refs"), ("nodes", "nodes"),
                           ("edges", "edges")):
            out[key] = self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        return out

    def iter_nodes(self) -> Iterator[NodeRec]:
        cur = self._db.execute(
            "SELECT id, kind, service, relpath, start_byte, end_byte, start_line, "
            "end_line, name, qualified_name, content_hash, props FROM nodes")
        for (id_, kind, service, relpath, sb, eb, sl, el, name, qn, ch, props) in cur:
            yield NodeRec(id=id_, kind=kind, service=service, relpath=relpath,
                          start_byte=sb, end_byte=eb, start_line=sl, end_line=el,
                          name=name, qualified_name=qn, content_hash=ch,
                          props=json.loads(props))

    def iter_edges(self) -> Iterator[EdgeRec]:
        cur = self._db.execute(
            "SELECT src, dst, type, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props FROM edges")
        for (src, dst, type_, res, conf, ext, ef, el, props) in cur:
            yield EdgeRec(src=src, dst=dst, type=type_, resolution=res,
                          confidence=conf, extractor=ext, evidence_file=ef,
                          evidence_line=el, props=json.loads(props))

    def set_meta(self, key: str, value: str) -> None:
        self._db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))
        self._db.commit()

    def get_meta(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None
