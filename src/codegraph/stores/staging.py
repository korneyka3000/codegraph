"""SQLite-staging: промежуточное состояние пайплайна. FalkorDB пересоздаваем отсюда."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from codegraph.chunking.splitter import ChunkRec
from codegraph.core import ids
from codegraph.core.errors import InvariantError
from codegraph.core.schema import ROLE_KINDS, SCHEMA_VERSION, EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow


@dataclass(frozen=True)
class ChunkRow:
    """Full on-disk shape of one `chunks` row -- a superset of `ChunkRec` (chunk_id,
    symbol_id, ord, text, start_line, end_line, content_hash) plus the staging-only
    columns `upsert_chunks` injects (service, relpath) and the ones only
    `set_embeddings`/`set_context_headers`/`set_input_hashes` ever populate
    (context_header, embedding, embed_model, embedded_hash, input_hash). Lives here
    rather than in `chunking.splitter` alongside `ChunkRec` on purpose: the splitter is
    storage-agnostic (it has never heard of "service" or "embedding"), this type is
    exactly the staging table's own column list.

    `embedded_hash` (M3 T6 carry, cache hardening; M4 T1 changed its SEMANTICS -- see
    core/schema.py's SCHEMA_VERSION "4 -> 5" history entry): the chunk's `input_hash`
    (NOT `content_hash` any more) AT THE MOMENT it was last embedded, written by
    `set_embeddings` alongside `embedding`/`embed_model`. See
    `chunks_missing_embedding`'s own docstring for the footgun this closes (now also
    covering a HEADER-only change, which content_hash alone could never see) --
    `embedding`/`embedded_hash` are always set together, so `embedded_hash` is None
    exactly when `embedding` is (never independently null, enforced by the `chunks`
    table's own CHECK constraint -- see `_DDL`).

    `input_hash` (M4 T1, persistent cross-run embedding cache): the EXACT embedder
    input's hash (`sha256(augment_text(header, text))` -- `chunking.augment` is the
    single source of truth for that format, staging itself never builds the hash) --
    NULL until `chunking.augment.fill_headers_all` writes it via `set_input_hashes`
    (alongside `context_header`, same call). This is the cache key
    `Staging.embedding_cache_get`/`embedding_cache_put` use (paired with
    `embed_model`), and what `chunks_missing_embedding` compares `embedded_hash`
    against. Populated INDEPENDENTLY of `embedding`/`embed_model`/`embedded_hash` (at
    header-fill time, before any embed call even happens) -- so, unlike that trio, it
    is NOT covered by the CHECK constraint's NULL-together guarantee: a freshly
    chunked, headers-filled-but-not-yet-embedded row legitimately has a non-NULL
    `input_hash` and a still-NULL `embedding` at the same time."""

    chunk_id: str
    symbol_id: str
    service: str
    relpath: str
    ord: int
    text: str
    start_line: int
    end_line: int
    content_hash: str
    context_header: str | None
    embedding: bytes | None
    embed_model: str | None
    embedded_hash: str | None
    input_hash: str | None


# Column list for every `SELECT ... FROM chunks` that feeds a `ChunkRow(*row)` --
# `chunks_for_service`/`chunks_missing_embedding`/`iter_chunks` below all read the
# exact same 14 columns, in `ChunkRow`'s own field order (positional unpacking
# depends on that order matching exactly) -- one constant instead of three
# hand-copied literals that would silently drift apart on a future column change.
_CHUNK_COLUMNS = (
    "chunk_id, symbol_id, service, relpath, ord, text, start_line, end_line, "
    "content_hash, context_header, embedding, embed_model, embedded_hash, input_hash"
)

# M4 T1: `Staging.embedding_cache_get`'s own per-query IN-clause row cap -- keeps a
# single SELECT well under SQLite's own bound-parameter-count ceiling for a whole
# workspace's worth of cache-miss candidates.
_CACHE_LOOKUP_BATCH = 400

# (report key, table) pairs behind `counts()` (workspace-wide) and
# `counts_for_service()` (per-service, M4 T5) -- one constant instead of two
# hand-copied literals that would silently drift apart on a future table addition
# (the exact same reasoning as `_CHUNK_COLUMNS` above). The key is the REPORT-dict
# name (matching analyze_service's own report fields: defs/refs, not
# scip_defs/scip_refs); the value is the actual table name.
_COUNT_TABLES = (
    ("files", "files"),
    ("defs", "scip_defs"),
    ("refs", "scip_refs"),
    ("nodes", "nodes"),
    ("edges", "edges"),
    ("chunks", "chunks"),
)

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
  src TEXT, dst TEXT, type TEXT, via_channel TEXT NOT NULL DEFAULT '',
  resolution TEXT, confidence REAL,
  extractor TEXT, evidence_file TEXT, evidence_line INTEGER, props TEXT,
  origin_service TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(src, dst, type, via_channel, origin_service));
CREATE INDEX IF NOT EXISTS idx_edges_origin ON edges(origin_service);
CREATE TABLE IF NOT EXISTS claims(
  service TEXT, relpath TEXT, kind TEXT, payload_json TEXT,
  PRIMARY KEY(service, relpath, kind, payload_json));
CREATE INDEX IF NOT EXISTS idx_claims_kind ON claims(kind, service);
CREATE TABLE IF NOT EXISTS chunks(
  chunk_id TEXT PRIMARY KEY, symbol_id TEXT, service TEXT, relpath TEXT,
  ord INTEGER, text TEXT, start_line INTEGER, end_line INTEGER, content_hash TEXT,
  context_header TEXT, embedding BLOB, embed_model TEXT, embedded_hash TEXT,
  input_hash TEXT,
  CHECK ((embedding IS NULL) = (embed_model IS NULL)
         AND (embedding IS NULL) = (embedded_hash IS NULL)));
CREATE INDEX IF NOT EXISTS idx_chunks_service ON chunks(service);
CREATE TABLE IF NOT EXISTS embedding_cache(
  input_hash TEXT, embed_model TEXT, dim INTEGER, vec BLOB,
  PRIMARY KEY(input_hash, embed_model));
"""
# M3 T3 (history): `chunks` arrived as a BRAND NEW table -- unlike the 2 -> 3
# edges-PK migration, `CREATE TABLE IF NOT EXISTS` for a name no pre-T3 v3 staging.db
# could already have was purely additive, so SCHEMA_VERSION stayed 3 AT THE TIME (and
# through T4/T5: the table's original T3 DDL already included context_header/
# embedding/embed_model -- T4 only ever wrote INTO context_header, it never changed
# this DDL).
#
# M3 T6: adds `embedded_hash` + the NULL-together CHECK constraint above -- and
# UNLIKE T3's from-scratch creation, this IS a real reshape of a table every v3
# staging.db already has and populated. That is exactly what the SCHEMA_VERSION 3 -> 4
# bump exists for (core/schema.py's "3 -> 4" history entry): reopening a pre-T6 (v3)
# file fails LOUDLY in Staging.__init__ -- `_check_schema_version_before_ddl` reads
# the file's stored schema_version ("3"), sees the mismatch with SCHEMA_VERSION (4),
# and raises InvariantError BEFORE this _DDL (or any embedded_hash-referencing query
# below) ever runs -- never a raw sqlite3.OperationalError. The error message's
# "recreate" instruction is the intended REMEDY (staging.db is a disposable derived
# cache; no data-preserving upgrade path is written for it), not an absence of a
# migration mechanism -- the version check IS the mechanism, and it works (pinned by
# tests/unit/test_staging.py's v2- and v3-shaped-database tests).
#
# M4 T1: same reshape pattern again, one column further -- `chunks` gains
# `input_hash TEXT` (a real column addition to a table every v4 staging.db already
# has), and a brand new `embedding_cache` table appears alongside it. The
# SCHEMA_VERSION 4 -> 5 bump (core/schema.py's "4 -> 5" history entry -- read it for
# the full input_hash/embedding_cache/embedded_hash-semantics story) routes a pre-M4
# (v4) file through the exact same loud InvariantError path, before this DDL or any
# input_hash-referencing query ever runs (pinned by test_v4_pre_m4_database_raises_
# invariant_error_not_operational_error). `embedding_cache` itself needs no such
# guard to worry about on the FRESH-create side: like `chunks` at T3, it is a brand
# new table no pre-M4 staging.db could already have one of to collide with -- `CREATE
# TABLE IF NOT EXISTS` for it is purely additive.


def _id_service(node_id: str) -> str | None:
    if node_id.startswith("sym:"):
        return node_id.split(":", 2)[1]
    if node_id.startswith("svc:"):
        return node_id.split(":", 1)[1]
    return None


class Staging:
    def __init__(self, path: Path):
        """Version-check ordering (M3 T1, mandatory M2-final-review carry-item):
        `_check_schema_version_before_ddl` runs BEFORE `ensure_schema`'s DDL, not
        after -- see that method's own docstring for the raw sqlite3.OperationalError
        this order closes. `ensure_schema` + `_check_schema_version` (unchanged,
        called in that order afterwards) still own the fresh-file path: a brand new
        file has no meta table yet, so the before-DDL check is a deliberate no-op for
        it (see its docstring), and `_check_schema_version` writes schema_version for
        the first time once the DDL has actually created the meta table."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._check_schema_version_before_ddl()
        self.ensure_schema()
        self._check_schema_version()

    def ensure_schema(self) -> None:
        self._db.executescript(_DDL)
        self._db.commit()

    def _meta_table_exists(self) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
        ).fetchone()
        return row is not None

    def _check_schema_version_before_ddl(self) -> None:
        """Guards against `ensure_schema`'s DDL touching a PRE-EXISTING table whose
        on-disk layout doesn't match SCHEMA_VERSION -- e.g. an index or column the
        CURRENT schema references but an OLD file's table lacks would raise a raw
        sqlite3.OperationalError from deep inside `executescript`, instead of the
        actionable InvariantError below (the exact M2-final-review bug this closes:
        "v1→v2 инвалидация падает сырым sqlite3.OperationalError"). Only fires when a
        meta table ALREADY exists -- i.e. this is reopening a pre-existing staging.db,
        not creating a brand new one: a genuinely fresh file has no meta table yet, so
        querying it here (before `ensure_schema` has ever run) would itself raise "no
        such table: meta" -- a naive unconditional version-check-first swap breaks
        exactly this fresh-create path (see the test pinning it). `ensure_schema` +
        `_check_schema_version` below already handle the fresh-file and
        no-schema_version-key-yet cases correctly on their own; this method only ever
        needs to RAISE, never to write -- mismatch here means stop before touching
        anything else, no different DDL path to route to."""
        if not self._meta_table_exists():
            return
        current = self.get_meta("schema_version")
        if current is not None and current != str(SCHEMA_VERSION):
            raise self._version_mismatch_error(current)

    def _check_schema_version(self) -> None:
        current = self.get_meta("schema_version")
        if current is None:
            self.set_meta("schema_version", str(SCHEMA_VERSION))
        elif current != str(SCHEMA_VERSION):
            raise self._version_mismatch_error(current)

    @staticmethod
    def _version_mismatch_error(current: str) -> InvariantError:
        return InvariantError(
            f"schema_version mismatch: staging has {current!r}, expected "
            f"{SCHEMA_VERSION!r} — the on-disk table layout changed (see "
            "core/schema.py SCHEMA_VERSION's history comment for what and why) "
            "and cannot be read forward; staging is a disposable derived cache, "
            "not a source of truth — recreate it (delete the file and re-run "
            "indexing from scratch)"
        )

    def close(self) -> None:
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- запись --

    def begin_service(self, service: str) -> None:
        """Сбрасывает S1-S6 слой ОДНОГО сервиса (files/defs/refs/nodes/edges с
        origin_service=service) + его claims. НЕ трогает workspace-слой (Channel/
        BusinessProcess-узлы, extractor="linking"-рёбра, чужие claims) -- те
        живут в отдельном скоупе, чистятся ТОЛЬКО clear_workspace_layer() (см. её
        докстринг и Global Constraint 2 плана M2). Раньше здесь был ещё глобальный
        `DELETE FROM edges WHERE src_service IS NULL` "страховкой от накопления" --
        убран: он стирал ЛЮБОЙ null-src edge (в т.ч. рёбра с chan:/proc: концом) как
        побочный эффект обработки ОДНОГО сервиса, что ломало персистентность
        workspace-слоя между прогонами S7.

        M2 FINAL REVIEW FIX (origin_service replaces src_service as the deletion key):
        `src_service` was derived from `e.src`'s OWN prefix (`_id_service(e.src)`, used
        for the cross-service INVARIANT check below) -- but that's None for ANY
        chan:/proc:-prefixed src, regardless of which service's analyze emitted the
        edge. HANDLES (src=chan:, dst=handler -- fastapi_ext's own convention) and kafka
        CONTAINS (chan:topic -> chan:event) edges therefore ALWAYS had src_service=NULL,
        so this DELETE could never find them no matter which service originally wrote
        them: they silently survived every re-index. Empirically, that meant a renamed
        route (or renamed kafka topic/event) left its OLD HANDLES/CONTAINS edge -- and
        the now-orphaned old Channel node it pointed at/from -- staged forever,
        poisoning S7's route table with a stale pattern on the SECOND `codegraph index`
        run (false CALLS_HTTP/NEXT_SEGMENT matches against a route that no longer
        exists in source). origin_service is instead an EXPLICIT "who emitted this
        batch" fact supplied by the CALLER of upsert_edges (analyze_service always
        passes svc.name for its own S5/S6 writes; S7/linking-derived batches pass
        None, the default) -- entirely independent of the edge's own endpoint
        prefixes, so it has no such blind spot. See `upsert_edges` and
        `gc_orphan_channels` (the companion fix for the orphaned Channel node itself)."""
        cur = self._db
        # M3 T3: "chunks" joins this same simple "WHERE service=?" family (like
        # files/scip_defs/scip_refs) -- a full per-service wipe, embeddings included.
        #
        # M4 T1 update: wiping a service's `chunks` rows here no longer loses the
        # actual VECTORS -- they live on in the separate, global `embedding_cache`
        # table (keyed by (input_hash, embed_model), see core/schema.py's
        # SCHEMA_VERSION "4 -> 5" history entry), which this loop deliberately does
        # NOT include: embedding_cache has no `service` column at all, and nothing
        # anywhere ever deletes from it. A freshly re-chunked row with the SAME exact
        # embedder input (header + text unchanged since some prior run) rejoins its
        # old vector from that cache at embed time (chunk_embed._embed_missing) at
        # ZERO provider cost, even though its OWN `chunks` row here was just deleted
        # and will be recreated from scratch -- see chunk_embed.py's own module
        # docstring for the full cross-run mechanism this enables (`codegraph index`
        # run twice on an unchanged workspace -> `embedded_fresh == 0`). Within ONE
        # index run, `upsert_chunks`' own ON-CONFLICT-preserving upsert is a SEPARATE,
        # narrower idempotency concern (see that method's own docstring) -- it never
        # even reaches this DELETE, since begin_service always runs before the SAME
        # run's own chunking pass touches this service's rows again.
        for t in ("files", "scip_defs", "scip_refs", "chunks"):
            cur.execute(f"DELETE FROM {t} WHERE service=?", (service,))  # noqa: S608
        cur.execute("DELETE FROM nodes WHERE service=?", (service,))
        cur.execute("DELETE FROM edges WHERE origin_service=?", (service,))
        cur.execute("DELETE FROM claims WHERE service=?", (service,))
        self._db.commit()

    def clear_workspace_layer(self) -> None:
        """Стирает S7-derived-слой ПЕРЕД link_workspace -- вызывается один раз, ПОСЛЕ
        всех analyze_service (см. Global Constraint 2 плана M2: S7 всегда идёт после
        полного прогона; инкрементальность -- M4).

        M2 T7 CONTRACT FIX (сужение T1-контракта, санкционировано контроллером T7):
        Channel-узлы БОЛЬШЕ НЕ удаляются здесь. T1 писал этот метод до того, как T4/T5/
        T6 закрепили, что Channel-узлы создаются ЭКСТРАКТОРАМИ в S5, per-service (fastapi_
        ext/kafka_ext), и живут в staged nodes ТОЧНО так же, как любой код-узел --
        начиная с T7 Channel-узлы ТАКЖЕ создаются самой линковкой (http_routes.link'а
        unresolved-fallback канал). Удаление kind='Channel' здесь стирало бы Channel-узлы
        КАЖДОГО сервиса разом (workspace-wide), хотя begin_service чистит только ОДИН
        сервис за раз -- вызов clear_workspace_layer() перед повторным link_workspace без
        полного re-analyze всех сервисов уничтожил бы каналы сервисов, которые не
        переанализировались. Не страшно для избыточности/дублей: id канала детерминирован
        (ids.chan_kafka/chan_event/chan_http), upsert_nodes -- INSERT OR REPLACE, так что
        повторная эмиссия ТОГО ЖЕ канала -- no-op замена той же строки, а не дубликат;
        явная очистка Channel-узлов здесь была бы избыточной защитой без реальной пользы
        (см. workspace.py модульный докстринг про «unification = no-op при совпадающих
        id» -- то же рассуждение, что закрыло отдельный linking/channels.py).

        Остаётся селективным по двум другим осям: BusinessProcess-узлы -- ВСЕГДА чисто
        S7-derived (материализуются только здесь, processes.materialize), поэтому чистка
        перед пересчётом безопасна и необходима (иначе устаревшие proc:-узлы/слаги
        накапливались бы). Рёбра -- только extractor=="linking" (S7-derived: CALLS_HTTP,
        NEXT_SEGMENT, PART_OF_PROCESS, temporal_start-CALLS созданные S7 "с нуля"); НЕ
        трогает код-рёбра S5/S6 (HANDLES/PRODUCES/CONSUMES/INVOKES_ACTIVITY и т.п. --
        extractor="fastapi"/"kafka"/"temporal"/"calls", чистятся per-service в
        begin_service) и НЕ трогает CALLS-рёбра, которые S7 лишь ПОМЕТИЛ через
        update_edge_props (тот CALLS остаётся с исходным extractor="calls" -- только
        props получают mechanism="temporal_start", extractor не переписывается)."""
        cur = self._db
        cur.execute("DELETE FROM nodes WHERE kind='BusinessProcess'")
        cur.execute("DELETE FROM edges WHERE extractor='linking'")
        self._db.commit()

    # -- M4 T5: incremental per-service layer clearing. Narrower siblings of
    # begin_service -- that method still owns the FULL-reanalyze wipe (files/
    # scip_defs/scip_refs/chunks/nodes/edges/claims, everything, unconditionally);
    # these two exist so an incremental analyze_service can wipe ONLY what it is
    # about to recompute, leaving every other relpath's staged data untouched. See
    # pipeline/analyze.py's module docstring for the exact step order these two
    # slot into (clear_scip_layer first, to make room for a fresh S3/S4 pass;
    # delete_file_layer only once the stale set is actually known, from the fresh
    # refs).

    def clear_scip_layer(self, service: str) -> None:
        """Wipes files/scip_defs/scip_refs for ONE service -- the same three tables
        begin_service's own first loop clears (`for t in ("files", "scip_defs",
        "scip_refs", "chunks")`), minus `chunks` (an S8 concern this S1-S6
        incremental flow never touches -- see delete_file_layer for the narrower,
        relpath-scoped chunk deletion incremental re-analyze uses instead).

        Deliberately leaves nodes/edges/claims (S5/S6) untouched, unlike
        begin_service: the incremental caller still needs the OLD staged nodes/
        edges/claims of every NON-stale relpath to survive this call. Those get
        cleared later, narrowly, by delete_file_layer(stale | dead) -- only once the
        stale set is known, which itself requires the FRESH defs/refs this call
        makes room for (a fresh S3/S4 pass can't write into a table that still
        holds the old service's rows without first clearing it, same reasoning as
        begin_service's own pre-S3 wipe)."""
        for t in ("files", "scip_defs", "scip_refs"):
            self._db.execute(f"DELETE FROM {t} WHERE service=?", (service,))  # noqa: S608
        self._db.commit()

    def delete_file_layer(
        self, service: str, relpaths: set[str], *, drop_calls_evidence: set[str],
    ) -> None:
        """Wipes the S5/S6 layer (nodes/claims/chunks/edges) for a SUBSET of a
        service's relpaths -- the stale∪deleted set an incremental analyze_service
        is about to re-extract (or, for deleted files, never will again). Untouched
        relpaths' nodes/claims/chunks/edges survive completely; that selectivity is
        the entire point of this method existing alongside begin_service's
        unconditional whole-service wipe.

        nodes/claims/chunks each carry a `relpath` column directly, so `relpaths`
        scopes all three with a plain `service=? AND relpath IN (...)`. Channel/
        BusinessProcess nodes (relpath=None, made by make_channel_node/
        make_process_node) can never match an IN-list of real relpath strings --
        SQL's `NULL IN (...)` is never true -- so the workspace layer is safe by
        construction, no extra WHERE clause needed to protect it.

        edges have NO relpath column at all (an edge spans two possibly-different
        files' worth of endpoints) -- `drop_calls_evidence` scopes edge deletion by
        `origin_service=? AND evidence_file IN (...)` instead, mirroring
        begin_service's own origin_service-keyed edge deletion (see its docstring)
        but narrowed to the subset of evidence files whose OWN re-extraction is
        about to re-emit them: a CALLS edge's evidence_file is the call-site's file
        (extractors/calls.py); a python_core CONTAINS/IMPORTS or fastapi/kafka/
        temporal domain edge's is the file the extractor ran over (see
        extractors/python_core.py's evidence_file fix, M4 T5) -- always exactly the
        file whose S5/S6 re-run re-emits that edge. Edges with no origin_service
        (S7/linking-derived: NEXT_SEGMENT, CALLS_HTTP, PART_OF_PROCESS, ...) never
        match `origin_service=?` regardless of evidence_file, so they survive
        untouched -- the same workspace-layer immunity begin_service already has.

        `relpaths` and `drop_calls_evidence` are independent parameters (not one
        merged set) so a caller CAN scope them differently -- analyze.py's own
        incremental branch always passes the same `stale | dead` set for both, but
        this method makes no such assumption. Either (or both) may be empty --
        each guards its own no-op rather than issuing a degenerate `IN ()` query."""
        if relpaths:
            placeholders = ",".join("?" * len(relpaths))
            params = (service, *relpaths)
            for table in ("nodes", "claims", "chunks"):
                self._db.execute(
                    f"DELETE FROM {table} WHERE service=? AND relpath IN ({placeholders})",  # noqa: S608
                    params,
                )
        if drop_calls_evidence:
            placeholders = ",".join("?" * len(drop_calls_evidence))
            self._db.execute(
                "DELETE FROM edges WHERE origin_service=? AND "
                f"evidence_file IN ({placeholders})",  # noqa: S608
                (service, *drop_calls_evidence),
            )
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
        """labels-колонка = json [kind, *roles] (роли -- доп. label'ы поверх kind,
        см. ROLE_KINDS/schema.py докстринг). roles валидируются ⊆ ROLE_KINDS ДО
        любой записи -- один невалидный узел в батче проваливает весь upsert
        (fail-closed, по аналогии с cross-service инвариантом ниже)."""
        for n in rows:
            invalid_roles = [r for r in n.roles if r not in ROLE_KINDS]
            if invalid_roles:
                raise InvariantError(
                    f"invalid role(s) for node {n.id!r}: {invalid_roles!r} "
                    f"(allowed: {sorted(ROLE_KINDS)!r})"
                )
        self._db.executemany(
            "INSERT OR REPLACE INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(n.id, n.kind, json.dumps([n.kind, *n.roles]), n.service, n.relpath,
              n.start_byte, n.end_byte, n.start_line, n.end_line,
              n.name, n.qualified_name, n.content_hash, json.dumps(n.props))
             for n in rows],
        )
        self._db.commit()

    def upsert_edges(self, rows: list[EdgeRec], origin_service: str | None = None) -> None:
        """Cross-service инвариант (Global Constraint 1 плана M2):
          - endpoints, начинающиеся на "chan:"/"proc:" -- без cross-service проверки
            вовсе (каналы/процессы кросс-сервисны по природе, у них нет "своего"
            сервиса).
          - sym→sym (оба конца "sym:") разных сервисов -- разрешено ТОЛЬКО если
            type == "NEXT_SEGMENT" И "via_channel_id" в props (иначе InvariantError).
          - всё прочее (в т.ч. svc:-конец в кросс-сервисной паре) -- как раньше,
            безусловный InvariantError.
        Инвариант по-прежнему считается из `_id_service(e.src/dst)` (ss/ds ниже) --
        это НЕ то же самое, что origin_service (см. следующий абзац); эндпойнт-сервис
        и сервис-эмиттер -- разные оси.

        `origin_service` -- сервис, чей ОДИН analyze_service-прогон эмитнул ВЕСЬ этот
        батч рёбер (М2 final review fix): analyze_service передаёт `svc.name` при
        КАЖДОМ своём вызове upsert_edges (S5 python_core/fastapi/kafka/temporal одним
        батчем + отдельно S6 build_calls -- см. extractors/calls.py); S7/linking-код
        (http_routes.link/segments.derive/processes.materialize/workspace._apply_
        temporal_start_marks) передаёт None (дефолт) -- их рёбра не принадлежат ни
        одному ОДНОМУ сервису, а чистятся целиком через clear_workspace_layer().
        Хранится в edges.origin_service (без валидации/дедукции), независимо от
        e.src/e.dst -- именно поэтому она чинит проблему, которую ss (см. ниже) не
        могла решить: ss/ds выводятся ИЗ endpoint-префикса (None для chan:/proc:), а
        origin_service -- явный факт "кто это записал", который для chan:-src рёбер
        (HANDLES, kafka CONTAINS) настоящий сервис-эмиттер ВСЕГДА имеет, даже когда
        endpoint сам по себе этого не выражает. `begin_service` теперь чистит
        edges.origin_service, а не производный ss (см. её докстринг). `None`
        нормализуется в `''` перед записью (PK-колонка не может быть NULL -- см.
        core/schema.py SCHEMA_VERSION история "5 -> 6"); публичный параметр остаётся
        `str | None` ради обратной совместимости каждого существующего вызывающего.

        M5 T4 (SCHEMA_VERSION 6, closes the M4 T7 residual gap for good):
        `origin_service` now joins the PRIMARY KEY (src, dst, type, via_channel,
        origin_service) -- so this is now an honest, unconditional `INSERT OR
        REPLACE`, exactly like every other upsert method in this class. TWO
        DIFFERENT services asserting the IDENTICAL (src,dst,type,via_channel) edge
        (e.g. kafka_ext's producer branch in one service and its consumer branch's
        dispatch_dict in another, each independently deriving the SAME `CONTAINS
        chan:kafka_topic:X -> chan:event_type:Y` edge from their own idiom config)
        simply get TWO SEPARATE rows now, one per origin -- both coexist,
        unconditionally, regardless of write order or which subset of services runs
        in a given `codegraph index` pass. A service re-upserting its OWN
        previously-written row (identical origin_service too) still gets full
        REPLACE semantics, same as always -- only the PK tuple, now one column
        wider, decides row identity.

        This closes M4 T7's own documented residual gap: that fix made a shared
        PK's "winner" deterministic (first writer, permanently) but explicitly left
        open the case where the CURRENT owner stops emitting the edge while the
        OTHER, still-asserting service is `--incremental`-SKIPPED -- the owner's own
        delete_file_layer/begin_service correctly cleared ITS row, and nothing
        re-inserted it, so the edge wrongly vanished until the sibling's next
        non-skip run. With per-origin rows, deleting one origin's row NEVER touches
        a sibling origin's row for the identical key (see begin_service/
        delete_file_layer's own docstrings -- their origin_service-scoped DELETEs
        needed no change at all, they were already exact) -- a strictly STRONGER
        invariant than "first writer wins", not a weakening of it.

        The graph itself must still end up with exactly ONE edge per shared PK,
        deterministically: that responsibility now lives in `pipeline/load.py`
        (`_dedup_edges`, run over `Staging.iter_edges_with_origin()` before ever
        batching to FalkorDB), not here -- see that function's own docstring for the
        tie-break rules (priority resolution, then confidence, then
        lexicographically-first origin). `Staging.iter_edges()` itself is
        deliberately left returning raw, undeduplicated rows (see
        `iter_edges_with_origin`'s own docstring for why every one of its OTHER
        consumers is unaffected by that)."""
        origin = origin_service or ""
        prepared = []
        for e in rows:
            ss, ds = _id_service(e.src), _id_service(e.dst)
            chan_or_proc_endpoint = (
                e.src.startswith(("chan:", "proc:")) or e.dst.startswith(("chan:", "proc:"))
            )
            if not chan_or_proc_endpoint and ss and ds and ss != ds:
                next_segment_ok = (
                    e.type == "NEXT_SEGMENT"
                    and e.src.startswith("sym:") and e.dst.startswith("sym:")
                    and "via_channel_id" in e.props
                )
                if not next_segment_ok:
                    raise InvariantError(
                        f"cross-service edge forbidden: {e.src} -{e.type}-> {e.dst}"
                    )
            # M3 T1: via_channel -- PK column, not just a props entry (see
            # core/schema.py SCHEMA_VERSION history comment "2 -> 3"). Extracted from
            # props rather than a dedicated EdgeRec field: via_channel_id already lived
            # in props (segments.py's own `_next_segment_edge`), and every OTHER edge
            # type simply has no such prop -- defaulting to '' there reproduces the old
            # (src,dst,type) dedup behavior for the overwhelming majority of edges
            # unchanged.
            via_channel = e.props.get("via_channel_id", "")
            prepared.append((e.src, e.dst, e.type, via_channel, e.resolution, e.confidence,
                             e.extractor, e.evidence_file, e.evidence_line,
                             json.dumps(e.props), origin))
        self._db.executemany(
            "INSERT OR REPLACE INTO edges VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            prepared,
        )
        self._db.commit()

    def gc_orphan_channels(self) -> int:
        """M2 final review fix, companion to the origin_service change above: deletes
        every Channel node (kind='Channel') referenced by ZERO edges (as either src or
        dst). Intended call site is `linking.workspace.link_workspace`, immediately
        AFTER `clear_workspace_layer()` and BEFORE every derivation stage (temporal
        marks, http_routes.link, segments.derive, processes.materialize) -- see that
        function's own docstring for why this exact position is load-bearing, not just
        convenient: by the time clear_workspace_layer() has run, every REAL Channel
        already has its S5-native edge (kafka_ext's PRODUCES/CONSUMES/CONTAINS --
        created in the SAME analyze_service batch as the Channel itself, extractor !=
        "linking", so untouched by clear_workspace_layer), so a Channel with zero
        edges at this exact point can only be leftover data, not legitimately
        in-progress. M8 EXCEPTION -- http_route channels: HANDLES emission moved to S7
        (linking/router_prefix.py, extractor="linking"), so EVERY http_route Channel
        is "orphaned" at this exact point on a re-link run and gets GC'd here, then
        recreated with the identical deterministic id by router_prefix.link right
        after -- routine and harmless (end state unchanged); this is why the
        `channels_gc` report counter reads ~route-count on repeat runs instead of ~0.
        Running GC BEFORE http_routes.link matters: that stage
        rebuilds its route table by scanning EVERY staged Channel(http_route) node,
        stale or not -- if a stale Channel were still present when it scans, a claim
        that happens to still target the old (renamed-away) pattern would silently
        re-match it, minting a brand new CALLS_HTTP edge into a route that no longer
        exists in source and keeping the stale Channel "referenced" (and therefore
        immune to a LATER GC pass) forever. Running GC any later than immediately-after-
        clear would reopen exactly this hole; empirically caught by this fix's own
        double-run regression test (see tests/unit/test_reindex_regression.py) when GC
        was first (wrongly) placed at the end of link_workspace instead.

        A Channel becomes orphaned this way in two cases this fix specifically closes:
        (1) a route/topic/event rename makes the extractor emit a NEW deterministic
        Channel id this run; the OLD id's HANDLES/PRODUCES/CONSUMES/CONTAINS edge is now
        correctly deleted by begin_service's origin_service-scoped DELETE (see its
        docstring), but the OLD Channel NODE itself has no per-service deletion at all
        (Channel.service is always "" -- core/schema.py make_channel_node -- so
        begin_service's "DELETE FROM nodes WHERE service=?" never touches it either);
        (2) an http_call claim that resolved to an "unresolved" synthetic fallback
        Channel in a PRIOR run now resolves to a real route, or simply no longer exists
        -- clear_workspace_layer wipes the old extractor="linking" CALLS_HTTP edge that
        pointed at the fallback channel at the START of this same link_workspace run, so
        the fallback channel has zero edges by the time this method runs, UNLESS some
        other still-unresolved claim needs the exact same (verb, path) fallback again
        (in which case http_routes.link, running right after this method, simply
        recreates the identical deterministic id -- a harmless GC-then-recreate, not
        data loss).

        Returns the number of Channel nodes removed (0 -- the common case -- is a
        cheap no-op: no DELETE statement is even issued)."""
        referenced: set[str] = set()
        for row in self._db.execute("SELECT src, dst FROM edges"):
            referenced.add(row[0])
            referenced.add(row[1])
        orphan_ids = [
            row[0] for row in self._db.execute("SELECT id FROM nodes WHERE kind='Channel'")
            if row[0] not in referenced
        ]
        if orphan_ids:
            self._db.executemany(
                "DELETE FROM nodes WHERE id=?", [(oid,) for oid in orphan_ids]
            )
            self._db.commit()
        return len(orphan_ids)

    def update_edge_props(self, src: str, dst: str, type: str, merge: dict) -> bool:  # noqa: A002
        """Json-merge поверх существующих props КАЖДОЙ строки-ребра (src,dst,type) --
        shallow `{**old, **merge}` НЕЗАВИСИМО на строку, merge побеждает при
        коллизии ключей. No-op (возвращает False), если такого ребра нет вовсе; True
        при успешном обновлении хотя бы одной строки.

        type=="NEXT_SEGMENT" -- InvariantError, не молчаливая порча данных: этот метод
        ключуется по (src,dst,type), a NE via_channel, в отличие от РЕАЛЬНОГО PK рёбер
        (src,dst,type,via_channel,origin_service -- M3 T1 + M5 T4, см. core/schema.py
        SCHEMA_VERSION история "2 -> 3"/"5 -> 6"). Начиная с parallel-channel фикса
        linking/segments.py.derive, у NEXT_SEGMENT легитимно бывает НЕСКОЛЬКО строк с
        одинаковым (src,dst,type), различающихся ТОЛЬКО via_channel -- обновление всех
        строк группы (см. ниже) переписало бы props ОБЕИХ строк одним и тем же merge,
        стерев то, что у них РАЗНОЕ по смыслу (via_channel-специфичные props). Guard
        остаётся ровно тем же, чем был. Единственный реальный вызыватель
        (linking/workspace.py, temporal-start пометка) всегда передаёт type="CALLS",
        так что guard ничего ему не стоит.

        M5 T4 (SCHEMA_VERSION 6): origin_service присоединился к PK рёбер, так что
        голый (src,dst,type)-ключ теперь МОЖЕТ законно матчить БОЛЬШЕ ОДНОЙ строки --
        одну на каждый origin, ровно та же природа неоднозначности, от которой уже
        защищает guard выше, только по другой оси (origin, не via_channel). Для
        CALLS (единственный реальный тип-получатель) via_channel всегда '' (CALLS
        никогда не несёт via_channel_id в props), так что здесь она не даёт НИКАКОЙ
        новой неоднозначности -- различаться между строками группы может ТОЛЬКО
        origin_service. Пометка temporal-start принадлежит паре (src,dst) семантически
        (см. workspace.py._apply_temporal_start_marks), а не тому, чей origin первым
        записал свою CALLS-строку -- поэтому merge применяется К КАЖДОЙ строке группы
        независимо (каждая сохраняет СВОИ собственные исходные props, кроме
        смёрженных ключей), а не только к первой найденной."""
        if type == "NEXT_SEGMENT":
            raise InvariantError(
                "update_edge_props does not support type='NEXT_SEGMENT': this method's "
                "(src,dst,type) key does not distinguish via_channel, but NEXT_SEGMENT's "
                "real primary key is (src,dst,type,via_channel,origin_service) and can "
                "legitimately hold more than one row per (src,dst) pair (see "
                "core/schema.py SCHEMA_VERSION history '2 -> 3') -- updating by "
                "(src,dst,type) alone would silently overwrite every matching row's "
                "props with one arbitrary merge result"
            )
        rows = self._db.execute(
            "SELECT origin_service, props FROM edges WHERE src=? AND dst=? AND type=?",
            (src, dst, type),
        ).fetchall()
        if not rows:
            return False
        for origin_service, props_json in rows:
            props = json.loads(props_json)
            props.update(merge)
            self._db.execute(
                "UPDATE edges SET props=? WHERE src=? AND dst=? AND type=? "
                "AND origin_service=?",
                (json.dumps(props), src, dst, type, origin_service),
            )
        self._db.commit()
        return True

    def update_node_props(self, node_id: str, merge: dict) -> bool:
        """M9 T2: json-merge поверх props ОДНОГО узла (id) -- shallow `{**old,
        **merge}`, merge побеждает при коллизии ключей. No-op (возвращает False),
        если узла с таким id нет вовсе; True при успешном обновлении.

        Mirrors `update_edge_props` above (same shallow-merge-then-UPDATE shape,
        same missing-key False/present-key True contract), but SIMPLER: `nodes.id`
        is a genuine single-column PRIMARY KEY (see `_DDL`'s `CREATE TABLE nodes`),
        unlike edges' composite (src, dst, type, via_channel, origin_service) key --
        a `WHERE id=?` can only ever match zero or one row, so there is no
        via_channel/origin_service-style multi-row ambiguity to guard against here,
        and therefore no NEXT_SEGMENT-like type restriction either (update_edge_props'
        own guard exists ONLY because its bare (src,dst,type) key can legitimately
        span more than one physical row -- see that method's own docstring; that
        premise simply doesn't apply to a PRIMARY KEY lookup).

        Sole real caller (as of M9 T2): `linking/router_prefix.py`'s `link()`,
        compose-back patching a RouteHandler node's own `path_template` prop (staged
        LOCAL-only by `extractors/fastapi_ext.py` in S5) to the S7-composed,
        cross-file template -- see that module's own docstring for the full
        "only if it differs" call-site logic (this method itself has no opinion on
        that; it always writes when the node exists, exactly like `update_edge_props`
        always does)."""
        row = self._db.execute(
            "SELECT props FROM nodes WHERE id=?", (node_id,)
        ).fetchone()
        if row is None:
            return False
        props = json.loads(row[0])
        props.update(merge)
        self._db.execute(
            "UPDATE nodes SET props=? WHERE id=?",
            (json.dumps(props), node_id),
        )
        self._db.commit()
        return True

    def add_claims(self, service: str, relpath: str, kind: str, payloads: list[dict]) -> None:
        """Claims -- staging-only находки экстракторов (M2 S5), ещё не узлы/рёбра
        графа (напр. "этот файл содержит kafka-producer вызов с topic=X") --
        потребляются линковкой (S7) через claims_for(). payload_json сериализуется
        с sort_keys=True, чтобы идентичный по содержимому payload (напр. при
        повторном прогоне на неизменённом файле) давал тот же PRIMARY KEY-ключ и
        не плодил дубликаты строк."""
        self._db.executemany(
            "INSERT OR REPLACE INTO claims VALUES (?,?,?,?)",
            [(service, relpath, kind, json.dumps(p, sort_keys=True)) for p in payloads],
        )
        self._db.commit()

    def claims_for(self, kind: str, service: str | None = None) -> list[dict]:
        """payload dict + инжектированные "_service"/"_relpath" (побеждают при
        коллизии имён с ключами самого payload -- staging-метаданные авторитетнее
        произвольного содержимого claim'а).

        M7 T1 review Minor-2: `payload_json` -- вторичный ключ сортировки (после
        relpath) в ОБЕИХ ветках. До этого несколько claims одного (service, relpath,
        kind) возвращались в неспецифицированном (rowid-случайном, т.е. порядка
        вставки) порядке -- теперь порядок байт-детерминирован независимо от порядка
        add_claims-вызовов. Load-bearing для pipeline/analyze.py::_class_attrs_digest
        (sha256 поверх этого списка: случайная перестановка строк читалась бы как
        фантомное "class_attrs changed" и зря эскалировала бы инкрементальный
        прогон до полного re-extract)."""
        if service is None:
            cur = self._db.execute(
                "SELECT service, relpath, payload_json FROM claims WHERE kind=? "
                "ORDER BY service, relpath, payload_json",
                (kind,),
            )
        else:
            cur = self._db.execute(
                "SELECT service, relpath, payload_json FROM claims "
                "WHERE kind=? AND service=? ORDER BY relpath, payload_json",
                (kind, service),
            )
        out = []
        for svc, relpath, payload_json in cur.fetchall():
            payload = json.loads(payload_json)
            out.append({**payload, "_service": svc, "_relpath": relpath})
        return out

    # -- M3 T3: chunks (chunking.splitter.chunk_file's output, staged for T4's
    # augmentation and T6's embed+load) --

    def upsert_chunks(self, service: str, relpath: str, rows: list[ChunkRec]) -> None:
        """`service`/`relpath` apply to EVERY row in one call -- `chunk_file` runs
        per-file, so a single call always stages one file's worth of chunks.

        Deliberately NOT a blanket `INSERT OR REPLACE`: that would reset
        context_header/embedding/embed_model/embedded_hash/input_hash to NULL on every
        call, even when a chunk_id's content hasn't changed at all. `ON CONFLICT
        (chunk_id) DO UPDATE` instead only ever touches the content-derived columns
        (symbol_id, service, relpath, ord, text, start_line, end_line, content_hash);
        embedding/embed_model/embedded_hash/context_header/input_hash ALL survive a
        re-upsert with the SAME chunk_id untouched -- INCLUDING when the content itself
        changed (a same-chunk_id re-upsert with EDITED text updates content_hash but
        leaves the OLD embedding AND the now-STALE input_hash in place, until the next
        `chunking.augment.fill_headers_all` pass recomputes both the header and
        `input_hash` from the fresh text -- see `chunks_missing_embedding`'s own
        docstring for how that staleness gets detected once it does). This survival is
        what makes `chunks_missing_embedding` a genuine cache check rather than always
        finding everything "missing" -- chunk_embed.run relies on exactly this for its
        own WITHIN-run idempotency (a second call, same files, embeds nothing new).

        That in-place survival only ever matters WITHIN one staging session's chunk
        rows: `begin_service` still deletes chunk rows outright on the NEXT `codegraph
        index` invocation (see its own docstring). Unlike M3, that is no longer the end
        of the embedding-caching story -- M4 T1 adds a SEPARATE, global
        `embedding_cache` table (keyed by `(input_hash, embed_model)`, never touched by
        `begin_service`) that survives exactly this kind of wipe-and-recreate, so a
        cross-run repeat `codegraph index` can still avoid re-embedding unchanged
        chunks even though their `chunks` rows themselves were deleted and recreated
        from scratch -- see that method's own docstring and core/schema.py's
        SCHEMA_VERSION "4 -> 5" history entry for the full mechanism."""
        self._db.executemany(
            "INSERT INTO chunks "
            "(chunk_id, symbol_id, service, relpath, ord, text, start_line, end_line, "
            "content_hash) VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(chunk_id) DO UPDATE SET "
            "symbol_id=excluded.symbol_id, service=excluded.service, "
            "relpath=excluded.relpath, ord=excluded.ord, text=excluded.text, "
            "start_line=excluded.start_line, end_line=excluded.end_line, "
            "content_hash=excluded.content_hash",
            [
                (
                    c.chunk_id,
                    c.symbol_id,
                    service,
                    relpath,
                    c.ord,
                    c.text,
                    c.start_line,
                    c.end_line,
                    c.content_hash,
                )
                for c in rows
            ],
        )
        self._db.commit()

    def chunks_for_service(self, service: str) -> list[ChunkRow]:
        cur = self._db.execute(
            f"SELECT {_CHUNK_COLUMNS} FROM chunks WHERE service=? "  # noqa: S608
            "ORDER BY relpath, symbol_id, ord",
            (service,),
        )
        return [ChunkRow(*row) for row in cur.fetchall()]

    def chunks_missing_embedding(self, model_id: str) -> list[ChunkRow]:
        """Every chunk (any service) that either has no embedding yet, was last
        embedded under a DIFFERENT model id than `model_id` (e.g. the workspace config
        switched embedding models -- everything needs re-embedding, not just new
        chunks), OR (M4 T1 -- was content_hash-keyed through M3 T6, see
        core/schema.py's SCHEMA_VERSION "4 -> 5" history entry for why the swap) has an
        `embedded_hash` that no longer matches its current `input_hash`.

        That third/fourth condition -- `embedded_hash IS NULL OR embedded_hash !=
        input_hash` -- is the real cache-freshness check, and it is keyed on
        `input_hash` (`chunks.input_hash`, written by `chunking.augment.
        fill_headers_all` via `set_input_hashes` -- the EXACT embedder input's hash,
        `sha256(augment_text(header, text))`), not `content_hash` (the chunk's own
        source text alone, blind to its augmentation header). `embedded_hash` (written
        by `set_embeddings` alongside `embedding`/`embed_model`, see its own
        docstring) records exactly which `input_hash` that embedding was actually
        computed FROM.

        Keying on input_hash rather than content_hash is the whole point of the M4 T1
        redesign: content_hash alone can NEVER detect a chunk whose own source text
        never changed but whose HEADER did (e.g. some OTHER symbol elsewhere in the
        graph got renamed, or gained/lost an edge, changing THIS chunk's
        `graph:`/`imports:`/`parent:` line without touching this chunk's own body) --
        embedded_hash == content_hash would never budge, so a stale embedding
        (computed from the OLD header + unchanged text) would silently keep serving
        forever. `input_hash` folds the header into the hash, so a header-only change
        and a text-only change are both visible through this exact same single
        disjunct.

        `embedded_hash IS NULL` is its own explicit disjunct (not folded into
        `embedded_hash != input_hash` alone) because SQL's `!=` against a NULL
        `input_hash` (a chunk whose `fill_headers_all` pass simply hasn't run yet this
        session) evaluates to SQL NULL, not TRUE -- `OR` would silently fail to flag it
        without this. A never-embedded row (`embedding IS NULL`) is caught by the
        first disjunct regardless of either hash's value.

        This whole disjunct-completeness argument (for the embedding/embed_model/
        embedded_hash trio) leans on those three always being NULL together or set
        together (never a PARTIAL NULL). That invariant isn't just a convention
        `set_embeddings` happens to uphold (it's the sole writer of all three columns,
        always together, in one UPDATE) -- it's enforced by the `chunks` table's own
        `CHECK` constraint (_DDL above), so a future write path that ever tried to set
        one without the other two would fail loudly (`sqlite3.IntegrityError`) at
        INSERT/UPDATE time. `input_hash` is deliberately OUTSIDE that CHECK trio (see
        core/schema.py's SCHEMA_VERSION "4 -> 5" history entry): it is set
        independently of embedding, at header-fill time, so a freshly-chunked,
        headers-filled-but-not-yet-embedded row legitimately has non-NULL `input_hash`
        and NULL `embedding` at the same time -- exactly the "never embedded" case the
        first disjunct already catches on its own."""
        cur = self._db.execute(
            f"SELECT {_CHUNK_COLUMNS} FROM chunks WHERE embedding IS NULL "  # noqa: S608
            "OR embed_model != ? OR embedded_hash IS NULL OR embedded_hash != input_hash "
            "ORDER BY chunk_id",
            (model_id,),
        )
        return [ChunkRow(*row) for row in cur.fetchall()]

    def has_live_embeddings(self) -> bool:
        """True iff ANY chunk (any service, workspace-wide) currently carries a real,
        non-NULL `embedding` blob -- `SELECT EXISTS(...)`, not a `COUNT`, so SQLite can
        stop at the first matching row instead of scanning the whole table.

        Deliberately coarse -- unlike `chunks_missing_embedding`, this does NOT check
        `embedded_hash`/`input_hash` freshness or which `embed_model` produced the
        vector: a STALE-but-present embedding (content/header edited since, not yet
        re-embedded) still counts as "live" here, because `pipeline/load.py` loads
        whatever `chunks.embedding` currently holds as-is, regardless of staleness --
        freshness only ever gates the RE-embedding DECISION (`chunks_missing_
        embedding`'s own job), never whether an already-stored value gets loaded into
        the graph at all.

        M5 T7's own consumer: `pipeline.chunk_embed.run`'s embedder-is-None branch
        reads this to decide whether clearing staging's `embed_model`/`embed_dim` Meta
        keys is actually honest for THIS run, rather than assuming a full re-chunk
        alone guarantees no chunk anywhere still has a vector (see that function's own
        docstring -- `upsert_chunks`' ON-CONFLICT contract can leave a prior run's
        embedding sitting untouched even after a full re-chunk of unchanged content)."""
        row = self._db.execute(
            "SELECT EXISTS(SELECT 1 FROM chunks WHERE embedding IS NOT NULL)"
        ).fetchone()
        return bool(row[0])

    def set_embeddings(self, rows: list[tuple[str, bytes, str, str]]) -> None:
        """`rows` -- `(chunk_id, embedding_blob, model_id, input_hash)`; `input_hash`
        (M4 T1 -- was `content_hash` through M3 T6, see core/schema.py's
        SCHEMA_VERSION "4 -> 5" history entry for why the swap) is the chunk's
        `input_hash` AT THE MOMENT it was embedded -- the caller reads it off the SAME
        `ChunkRow` it fed (via `augment_text`) to the embedder, and it's stored as
        `embedded_hash` so a LATER change to this chunk's exact embedder input (its own
        text OR its augmentation header) leaves `embedded_hash` stale and
        `chunks_missing_embedding` correctly flags it (see that method's own docstring
        for the full footgun this closes). No-op for a `chunk_id` that doesn't exist
        (matches `update_edge_props`'s missing-key tolerance)."""
        self._db.executemany(
            "UPDATE chunks SET embedding=?, embed_model=?, embedded_hash=? WHERE chunk_id=?",
            [
                (blob, model_id, input_hash, chunk_id)
                for chunk_id, blob, model_id, input_hash in rows
            ],
        )
        self._db.commit()

    def set_context_headers(self, rows: list[tuple[str, str]]) -> None:
        """`rows` -- `(chunk_id, header)`; T4's augment.build_header output, stored so
        it can be embedded (T6) and fulltext-searched (T7) without being recomputed."""
        self._db.executemany(
            "UPDATE chunks SET context_header=? WHERE chunk_id=?",
            [(header, chunk_id) for chunk_id, header in rows],
        )
        self._db.commit()

    def set_input_hashes(self, rows: list[tuple[str, str]]) -> None:
        """`rows` -- `(chunk_id, input_hash)`; M4 T1's own write-back, called from
        `chunking.augment.fill_headers_all` alongside `set_context_headers` (same
        plain-UPDATE-by-chunk_id shape, same no-op-for-missing-chunk_id tolerance as
        that method and `set_embeddings`). `input_hash` -- the exact embedder input's
        hash (`sha256(augment_text(header, text))`) -- is computed BY THE CALLER
        (`chunking.augment` is its single source of truth, see `augment_text`'s own
        docstring); this method only ever stores whatever string it's handed, same
        division of responsibility as `set_context_headers`/`set_embeddings`."""
        self._db.executemany(
            "UPDATE chunks SET input_hash=? WHERE chunk_id=?",
            [(input_hash, chunk_id) for chunk_id, input_hash in rows],
        )
        self._db.commit()

    # -- M4 T1: embedding_cache -- a GLOBAL, cross-`codegraph index`-run cache, keyed
    # on (input_hash, embed_model) -- see core/schema.py's SCHEMA_VERSION "4 -> 5"
    # history entry for the full design rationale. Never wiped by begin_service or
    # clear_workspace_layer (neither touches it at all -- it has no `service` column
    # to scope a DELETE by in the first place); no GC is implemented (staging.db is
    # itself a disposable, one-shot derived cache per workspace, same reasoning as
    # every other "no migration, just recreate" entry in SCHEMA_VERSION's history).

    def embedding_cache_get(self, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], bytes]:
        """Batched lookup: `pairs` -- every `(input_hash, embed_model)` key
        `chunk_embed._embed_missing` wants a cache hit for, workspace-wide, in ONE
        logical call -- never a per-chunk round trip. Grouped by `embed_model` (in
        practice a single `run()` call only ever asks about ONE model -- the current
        embedder's own `model_id` -- so this rarely does more than one real batch of
        queries), each group's input_hash list chunked to `_CACHE_LOOKUP_BATCH` rows
        per `SELECT ... IN (...)` query to stay well under SQLite's own bound
        parameter-count ceiling for a single statement.

        A miss is simply ABSENT from the returned dict, never a `None` value (mirrors
        this module's existing "absent, not null" convention elsewhere, e.g.
        `claims_for`). `{}` for `pairs == []`, without issuing any query at all."""
        if not pairs:
            return {}
        by_model: dict[str, list[str]] = defaultdict(list)
        for input_hash, embed_model in pairs:
            by_model[embed_model].append(input_hash)
        out: dict[tuple[str, str], bytes] = {}
        for embed_model, hashes in by_model.items():
            for i in range(0, len(hashes), _CACHE_LOOKUP_BATCH):
                batch = hashes[i : i + _CACHE_LOOKUP_BATCH]
                placeholders = ",".join("?" * len(batch))
                cur = self._db.execute(
                    "SELECT input_hash, vec FROM embedding_cache "  # noqa: S608
                    f"WHERE embed_model=? AND input_hash IN ({placeholders})",
                    (embed_model, *batch),
                )
                for input_hash, vec in cur.fetchall():
                    out[(input_hash, embed_model)] = vec
        return out

    def embedding_cache_put(self, rows: list[tuple[str, str, int, bytes]]) -> None:
        """`rows` -- `(input_hash, embed_model, dim, vec)`; INSERT OR REPLACE (a
        re-put of an existing `(input_hash, embed_model)` key overwrites its `dim`/
        `vec` -- matches `set_embeddings`'s own "last write wins" contract, though in
        practice the SAME `(input_hash, embed_model)` pair should always compute to
        the identical vector, since both are pure functions of the -- deterministic --
        embedder and its exact input text). No-op for `rows == []`."""
        if not rows:
            return
        self._db.executemany("INSERT OR REPLACE INTO embedding_cache VALUES (?,?,?,?)", rows)
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

    def ref_symbol_at(self, service: str, relpath: str, start_byte: int) -> str | None:
        """Mirrors def_symbol_at, over scip_refs -- M2 T4's FileContext.ref_symbol_lookup:
        resolves the symbol a REFERENCE occurrence at (relpath, start_byte) points at
        (e.g. the `get_db` identifier inside a `Depends(get_db)` default-value expression,
        which python_core's def-lookup can't answer since that's not a definition site)."""
        cur = self._db.execute(
            "SELECT symbol FROM scip_refs WHERE service=? AND relpath=? AND start_byte=? "
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

    def def_symbols(self, service: str) -> set[str]:
        """M5 Task 1 (pilot Bug B, docs/superpowers/reports/2026-07-18-m4-pilot.md
        §7.2): the FULL, service-wide set of every symbol with a staged def --
        `extractors/calls.py::build_calls`'s new first-party classification source of
        truth, replacing a `parsed.package == service` comparison that `scip-python
        --project-name <service>` makes unreliable (it stamps package=<service> onto
        every symbol it fully resolves, first-party and third-party alike). Unlike
        `local_def_symbols` above (per-relpath, LIKE-filtered to 'local %' rows only),
        this is unscoped by relpath and unfiltered by symbol shape -- every def this
        service's own S3/S4 (scip run + reader, or the degraded fallback resolver)
        ever staged, local and global alike, since a `local N` ref's first-party
        status is decided by the SEPARATE `local_defs_for_file` lookup instead (see
        build_calls' own docstring) and never consults this set.

        Callers hoist this ONCE per service (analyze.py: after S4 rewrites defs, same
        pattern as this file's own module docstring recommends for
        `local_def_symbols`) rather than letting build_calls re-query per file or per
        call-site -- `DISTINCT` because the same symbol can legitimately have more
        than one def occurrence (see test_def_symbols_scoped_to_service_and_deduped),
        and only its PRESENCE, never its count, matters to the caller."""
        cur = self._db.execute(
            "SELECT DISTINCT symbol FROM scip_defs WHERE service=?", (service,))
        return {row[0] for row in cur.fetchall()}

    def refs_for_file(self, service: str, relpath: str) -> list[RefRow]:
        cur = self._db.execute(
            "SELECT relpath, symbol, start_byte, end_byte, start_line, roles "
            "FROM scip_refs WHERE service=? AND relpath=? ORDER BY start_byte",
            (service, relpath))
        return [RefRow(*row) for row in cur.fetchall()]

    def refs_hash_by_file(self, service: str) -> dict[str, str]:
        """M4 T5: per-relpath sha256 over that file's CURRENT scip_refs rows, sorted
        by (symbol, start_byte, end_byte, roles) -- deliberately NOT start_line
        (two occurrences at the same byte span with different reported line numbers
        would be a scip-python encoding artifact, not a real ref change). This is
        incremental analyze_service's own "did this file's refs change" fingerprint
        (see pipeline/analyze.py's module docstring for the full ref_dirty
        mechanism it feeds): a file's OWN defs only change when its OWN content
        changes (already caught by service_delta's added/changed sets), but a
        symbol RENAMED in a DIFFERENT file changes how pyright/scip-python resolves
        every REFERENCING occurrence in THIS file, without this file's bytes moving
        at all -- refs are exactly where that shows up.

        The SQL ORDER BY (relpath, symbol, start_byte, end_byte, roles) does the
        sorting; rows are appended in that fetch order, grouped by relpath, so no
        extra Python-side sort is needed -- the hash is therefore independent of
        `add_refs`' own call/insertion order, only of the row *contents*.

        A relpath with zero staged refs is simply ABSENT from the returned dict
        (mirrors embedding_cache_get's "absent, not null" convention) -- callers
        compare via `.get(relpath)` on both sides so a file that had refs before
        and has none now (or vice versa) still reads as changed (some-hash != None),
        never as a KeyError."""
        cur = self._db.execute(
            "SELECT relpath, symbol, start_byte, end_byte, roles FROM scip_refs "
            "WHERE service=? ORDER BY relpath, symbol, start_byte, end_byte, roles",
            (service,),
        )
        by_file: dict[str, list[str]] = defaultdict(list)
        for relpath, symbol, start_byte, end_byte, roles in cur.fetchall():
            by_file[relpath].append(f"{symbol}\x1f{start_byte}\x1f{end_byte}\x1f{roles}")
        return {
            relpath: hashlib.sha256("\n".join(lines).encode()).hexdigest()
            for relpath, lines in by_file.items()
        }

    def counts(self) -> dict:
        out = {}
        for key, table in _COUNT_TABLES:
            out[key] = self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        return out

    def counts_for_service(self, service: str) -> dict:
        """Per-service sibling of counts() (workspace-wide, no WHERE clause at
        all). M4 T5's skip-mode report needs exactly this ("SQL COUNT по service"
        per the brief): when incremental analyze_service finds nothing to do
        (prior_delta.empty and the config fingerprint still matches), it does ZERO
        staging writes and reports whatever this service's CURRENT staged counts
        already are, instead of the per-run extraction/join counts every other mode
        reports. Same 6 keys as counts() (shared `_COUNT_TABLES` constant);
        `edges` has no `service` column of its own (only `origin_service`, see
        upsert_edges/begin_service) -- scoped by that instead, same key
        begin_service itself deletes by."""
        out = {}
        for key, table in _COUNT_TABLES:
            col = "origin_service" if table == "edges" else "service"
            out[key] = self._db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col}=?", (service,)  # noqa: S608
            ).fetchone()[0]
        return out

    def iter_nodes(self) -> Iterator[NodeRec]:
        """roles реконструируются из labels-колонки (json [kind, *roles]) --
        labels[0] всегда == kind (см. upsert_nodes), roles == остаток списка."""
        cur = self._db.execute(
            "SELECT id, kind, labels, service, relpath, start_byte, end_byte, start_line, "
            "end_line, name, qualified_name, content_hash, props FROM nodes")
        for (id_, kind, labels, service, relpath, sb, eb, sl, el, name, qn, ch, props) in cur:
            roles = tuple(json.loads(labels)[1:])
            yield NodeRec(id=id_, kind=kind, service=service, relpath=relpath,
                          start_byte=sb, end_byte=eb, start_line=sl, end_line=el,
                          name=name, qualified_name=qn, content_hash=ch,
                          props=json.loads(props), roles=roles)

    def iter_edges(self) -> Iterator[EdgeRec]:
        """RAW, undeduplicated rows -- one per (src,dst,type,via_channel,
        origin_service) PK tuple. Since M5 T4 (SCHEMA_VERSION 6), a shared edge
        (today, only a chan:-to-chan: CONTAINS pair -- see `upsert_edges`' own
        docstring) can legitimately surface here as MORE THAN ONE row, one per
        origin that asserts it -- this method deliberately does NOT collapse them
        (see `iter_edges_with_origin`'s own docstring for why that's safe for every
        consumer of THIS method, and for where the real collapsing happens)."""
        cur = self._db.execute(
            "SELECT src, dst, type, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props FROM edges")
        for (src, dst, type_, res, conf, ext, ef, el, props) in cur:
            yield EdgeRec(src=src, dst=dst, type=type_, resolution=res,
                          confidence=conf, extractor=ext, evidence_file=ef,
                          evidence_line=el, props=json.loads(props))

    def iter_edges_with_origin(self) -> Iterator[tuple[EdgeRec, str]]:
        """Like `iter_edges` (same raw, undeduplicated rows), but additionally
        yields each row's `origin_service` alongside its `EdgeRec` -- the ONE piece
        of information `iter_edges` itself never surfaced (see its own tests'
        `# noqa: SLF001` raw-SQL workarounds) and the ONE thing `pipeline/load.py`
        needs that no other `iter_edges` consumer does: its own `_dedup_edges`
        breaks a shared edge's now-possibly-multiple per-origin rows down to one,
        deterministically, and the tie-break's last rule is "lexicographically-first
        origin_service" (see `_dedup_edges`' own docstring) -- there is no way to
        implement that rule without origin_service travelling alongside each row.

        This is a SEPARATE method rather than a change to `iter_edges` itself (which
        would have meant adding an `origin_service` field to `EdgeRec` -- a
        general-purpose IR type constructed at dozens of call sites across every
        extractor, none of which have any use for it) on purpose: every other
        `iter_edges` consumer (`linking/segments.py`'s `derive`, `linking/
        processes.py`, `chunking/augment.py`, `evalx/edges_eval.py`, `evalx/
        calls_eval.py`) has no use for origin_service and is unaffected by a shared
        edge's now-possibly-multiple rows in the first place -- PRODUCES/CONSUMES/
        HANDLES all pin one endpoint to a single sym:-prefixed, single-service id
        (so two different origins can never assert the identical (src,dst,type) for
        those types at all), and CONTAINS specifically -- the one type that CAN be
        shared -- either collapses into an identical result on its own
        (`segments.derive`'s own `derived` dict keys on the pairing OUTCOME, so a
        duplicate `contains_pairs` entry just re-derives the same edge a second
        time) or is never looked up by a chan:-prefixed id at all (`chunking/
        augment.py`'s `contains_children`/`contains_parent` are only ever consulted
        by a code SYMBOL's own id when climbing/aggregating its structural CONTAINS
        neighborhood) or is already folded into a `set` (`evalx`'s comparison
        tuples) -- see this task's own report for the full per-consumer argument.
        `iter_edges()` itself is therefore left completely unchanged."""
        cur = self._db.execute(
            "SELECT src, dst, type, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props, origin_service FROM edges")
        for (src, dst, type_, res, conf, ext, ef, el, props, origin) in cur:
            yield (
                EdgeRec(src=src, dst=dst, type=type_, resolution=res,
                        confidence=conf, extractor=ext, evidence_file=ef,
                        evidence_line=el, props=json.loads(props)),
                origin,
            )

    def iter_chunks(self) -> Iterator[ChunkRow]:
        """For load (T6): every staged chunk, across all services."""
        cur = self._db.execute(
            f"SELECT {_CHUNK_COLUMNS} FROM chunks "  # noqa: S608
            "ORDER BY service, relpath, symbol_id, ord")
        for row in cur:
            yield ChunkRow(*row)

    def set_meta(self, key: str, value: str) -> None:
        self._db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))
        self._db.commit()

    def get_meta(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None
