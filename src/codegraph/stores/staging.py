"""SQLite-staging: промежуточное состояние пайплайна. FalkorDB пересоздаваем отсюда."""

from __future__ import annotations

import json
import sqlite3
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
    `set_embeddings`/`set_context_headers` ever populate (context_header, embedding,
    embed_model, embedded_hash). Lives here rather than in `chunking.splitter` alongside
    `ChunkRec` on purpose: the splitter is storage-agnostic (it has never heard of
    "service" or "embedding"), this type is exactly the staging table's own column list.

    `embedded_hash` (M3 T6 carry, cache hardening): the chunk's `content_hash` AT THE
    MOMENT it was last embedded, written by `set_embeddings` alongside `embedding`/
    `embed_model`. See `chunks_missing_embedding`'s own docstring for the footgun this
    closes -- `embedding`/`embedded_hash` are always set together, so `embedded_hash`
    is None exactly when `embedding` is (never independently null)."""

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


# Column list for every `SELECT ... FROM chunks` that feeds a `ChunkRow(*row)` --
# `chunks_for_service`/`chunks_missing_embedding`/`iter_chunks` below all read the
# exact same 13 columns, in `ChunkRow`'s own field order (positional unpacking
# depends on that order matching exactly) -- one constant instead of three
# hand-copied literals that would silently drift apart on a future column change.
_CHUNK_COLUMNS = (
    "chunk_id, symbol_id, service, relpath, ord, text, start_line, end_line, "
    "content_hash, context_header, embedding, embed_model, embedded_hash"
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
  origin_service TEXT,
  PRIMARY KEY(src, dst, type, via_channel));
CREATE INDEX IF NOT EXISTS idx_edges_origin ON edges(origin_service);
CREATE TABLE IF NOT EXISTS claims(
  service TEXT, relpath TEXT, kind TEXT, payload_json TEXT,
  PRIMARY KEY(service, relpath, kind, payload_json));
CREATE INDEX IF NOT EXISTS idx_claims_kind ON claims(kind, service);
CREATE TABLE IF NOT EXISTS chunks(
  chunk_id TEXT PRIMARY KEY, symbol_id TEXT, service TEXT, relpath TEXT,
  ord INTEGER, text TEXT, start_line INTEGER, end_line INTEGER, content_hash TEXT,
  context_header TEXT, embedding BLOB, embed_model TEXT, embedded_hash TEXT,
  CHECK ((embedding IS NULL) = (embed_model IS NULL)
         AND (embedding IS NULL) = (embedded_hash IS NULL)));
CREATE INDEX IF NOT EXISTS idx_chunks_service ON chunks(service);
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
        # files/scip_defs/scip_refs) -- a full per-service wipe, embeddings included;
        # there's no cross-run embedding cache to preserve here (M3 always re-chunks a
        # freshly begin_service'd file within the SAME index run -- see chunks table's
        # own upsert_chunks docstring for where an ON-CONFLICT-preserving upsert still
        # matters, which is purely a WITHIN-run idempotency concern, not this one).
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
        Хранится СЫРЫМ (без валидации/дедукции) в edges.origin_service, независимо
        от e.src/e.dst -- именно поэтому она чинит проблему, которую ss (см. ниже)
        не могла решить: ss/ds выводятся ИЗ endpoint-префикса (None для chan:/proc:),
        а origin_service -- явный факт "кто это записал", который для chan:-src рёбер
        (HANDLES, kafka CONTAINS) настоящий сервис-эмиттер ВСЕГДА имеет, даже когда
        endpoint сам по себе этого не выражает. `begin_service` теперь чистит
        edges.origin_service, а не производный ss (см. её докстринг)."""
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
                             json.dumps(e.props), origin_service))
        self._db.executemany(
            "INSERT OR REPLACE INTO edges VALUES (?,?,?,?,?,?,?,?,?,?,?)", prepared
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
        already has its S5-native edge (fastapi_ext's HANDLES, kafka_ext's
        PRODUCES/CONSUMES/CONTAINS -- created in the SAME analyze_service batch as the
        Channel itself, extractor != "linking", so untouched by clear_workspace_layer),
        so a Channel with zero edges at this exact point can only be leftover data, not
        legitimately in-progress. Running GC BEFORE http_routes.link matters: that stage
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
        """Json-merge поверх существующих props ребра (src,dst,type) -- shallow
        `{**old, **merge}`, merge побеждает при коллизии ключей. No-op (возвращает
        False), если такого ребра нет; True при успешном обновлении.

        type=="NEXT_SEGMENT" -- InvariantError, не молчаливая порча данных: этот метод
        ключуется по (src,dst,type), a NE via_channel, в отличие от РЕАЛЬНОГО PK рёбер
        (src,dst,type,via_channel -- M3 T1, см. core/schema.py SCHEMA_VERSION история
        "2 -> 3"). Начиная с parallel-channel фикса linking/segments.py.derive, у
        NEXT_SEGMENT легитимно бывает НЕСКОЛЬКО строк с одинаковым (src,dst,type),
        различающихся только via_channel -- SELECT ниже без ORDER BY взял бы props
        ПРОИЗВОЛЬНОЙ из них, а последующий UPDATE (тот же WHERE, без via_channel)
        переписал бы props ОБЕИХ строк идентичным (одним) результатом слияния --
        тихая порча данных, не просто "обновили не ту строку". Единственный реальный
        вызыватель (linking/workspace.py, temporal-start пометка) всегда передаёт
        type="CALLS", так что guard ничего ему не стоит."""
        if type == "NEXT_SEGMENT":
            raise InvariantError(
                "update_edge_props does not support type='NEXT_SEGMENT': this method's "
                "(src,dst,type) key does not distinguish via_channel, but NEXT_SEGMENT's "
                "real primary key is (src,dst,type,via_channel) and can legitimately hold "
                "more than one row per (src,dst) pair (see core/schema.py SCHEMA_VERSION "
                "history '2 -> 3') -- updating by (src,dst,type) alone would silently "
                "overwrite every matching row's props with one arbitrary merge result"
            )
        row = self._db.execute(
            "SELECT props FROM edges WHERE src=? AND dst=? AND type=?",
            (src, dst, type),
        ).fetchone()
        if row is None:
            return False
        props = json.loads(row[0])
        props.update(merge)
        self._db.execute(
            "UPDATE edges SET props=? WHERE src=? AND dst=? AND type=?",
            (json.dumps(props), src, dst, type),
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
        произвольного содержимого claim'а)."""
        if service is None:
            cur = self._db.execute(
                "SELECT service, relpath, payload_json FROM claims WHERE kind=? "
                "ORDER BY service, relpath",
                (kind,),
            )
        else:
            cur = self._db.execute(
                "SELECT service, relpath, payload_json FROM claims "
                "WHERE kind=? AND service=? ORDER BY relpath",
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
        context_header/embedding/embed_model/embedded_hash to NULL on every call, even
        when a chunk_id's content hasn't changed at all. `ON CONFLICT(chunk_id) DO
        UPDATE` instead only ever touches the content-derived columns (symbol_id,
        service, relpath, ord, text, start_line, end_line, content_hash); embedding/
        embed_model/embedded_hash/context_header survive a re-upsert with the SAME
        chunk_id untouched -- INCLUDING when the content itself changed (a same-chunk_id
        re-upsert with EDITED text updates content_hash but leaves the OLD embedding in
        place, which is exactly why `chunks_missing_embedding` also compares
        `embedded_hash` against the fresh `content_hash` rather than trusting
        embedding's mere presence, see that method's own docstring). This survival is
        what makes `chunks_missing_embedding` a genuine cache check rather than always
        finding everything "missing" -- T6's chunk_embed.run relies on exactly this for
        its own idempotency (a second call, same files, embeds nothing new). Note this
        only ever matters WITHIN one index run's staging lifetime: `begin_service`
        still deletes chunk rows outright on the NEXT `codegraph index` invocation (see
        its own docstring) -- there is no cross-run embedding cache in M3, incremental
        indexing is M4 scope."""
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
        chunks), OR (M3 T6 carry, cache hardening) was RE-UPSERTED with different
        content since it was last embedded.

        That third condition -- `embedded_hash != content_hash` -- closes a footgun
        `upsert_chunks`'s own ON-CONFLICT-preserving upsert opens: re-staging the SAME
        chunk_id with EDITED text (same symbol, same ord, different body -- e.g. a file
        changed between two `chunk_embed.run` calls within one staging session, with no
        intervening `begin_service`) updates `content_hash` but leaves the OLD
        `embedding` in place untouched (by design, for the common no-change case). Only
        checking `embedding IS NULL OR embed_model != ?` (the pre-T6 condition) would
        never notice that mismatch -- `embedding` is still non-NULL and `embed_model`
        still matches, so the chunk would silently keep serving a STALE vector forever,
        never flagged for re-embedding. `embedded_hash` (written by `set_embeddings`
        alongside `embedding`/`embed_model`, see its own docstring) records exactly
        which `content_hash` that embedding was actually computed FROM, so a later
        content change is detectable independent of embed_model/NULL-ness. A
        never-embedded row (`embedding IS NULL`) is caught by the first disjunct
        regardless of `embedded_hash`'s value (NULL `!=` comparisons are non-TRUE in
        SQL, but `OR` doesn't need them to be -- the first disjunct alone is enough).

        This whole disjunct-completeness argument leans on `embedding`/`embed_model`/
        `embedded_hash` always being NULL together or set together (never a PARTIAL
        NULL -- e.g. `embedding` set but `embedded_hash` still NULL would slip through
        every disjunct here: `embedding IS NULL` false, `embed_model != ?` false if
        the model matches, `embedded_hash != content_hash` evaluates to SQL NULL --
        not TRUE -- against a NULL `embedded_hash`, so `OR` never catches it). That
        invariant isn't just a convention `set_embeddings` happens to uphold (it's the
        sole writer of all three columns, always together, in one UPDATE) -- it's
        enforced by the `chunks` table's own `CHECK` constraint (_DDL above), so a
        future write path that ever tried to set one without the other two would fail
        loudly (`sqlite3.IntegrityError`) at INSERT/UPDATE time, not silently produce
        an unreachable "missing embedding" row here."""
        cur = self._db.execute(
            f"SELECT {_CHUNK_COLUMNS} FROM chunks WHERE embedding IS NULL "  # noqa: S608
            "OR embed_model != ? OR embedded_hash != content_hash ORDER BY chunk_id",
            (model_id,),
        )
        return [ChunkRow(*row) for row in cur.fetchall()]

    def set_embeddings(self, rows: list[tuple[str, bytes, str, str]]) -> None:
        """`rows` -- `(chunk_id, embedding_blob, model_id, content_hash)`; `content_hash`
        (M3 T6 carry) is the chunk's `content_hash` AT THE MOMENT it was embedded --
        the caller reads it off the SAME `ChunkRow` it fed to the embedder, and it's
        stored as `embedded_hash` so a LATER `upsert_chunks` call that changes this
        chunk_id's content without a matching re-embed leaves `embedded_hash` stale and
        `chunks_missing_embedding` correctly flags it (see that method's own docstring
        for the full footgun this closes). No-op for a `chunk_id` that doesn't exist
        (matches `update_edge_props`'s missing-key tolerance)."""
        self._db.executemany(
            "UPDATE chunks SET embedding=?, embed_model=?, embedded_hash=? WHERE chunk_id=?",
            [
                (blob, model_id, content_hash, chunk_id)
                for chunk_id, blob, model_id, content_hash in rows
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

    def refs_for_file(self, service: str, relpath: str) -> list[RefRow]:
        cur = self._db.execute(
            "SELECT relpath, symbol, start_byte, end_byte, start_line, roles "
            "FROM scip_refs WHERE service=? AND relpath=? ORDER BY start_byte",
            (service, relpath))
        return [RefRow(*row) for row in cur.fetchall()]

    def counts(self) -> dict:
        out = {}
        for key, table in (
            ("files", "files"),
            ("defs", "scip_defs"),
            ("refs", "scip_refs"),
            ("nodes", "nodes"),
            ("edges", "edges"),
            ("chunks", "chunks"),
        ):
            out[key] = self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
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
        cur = self._db.execute(
            "SELECT src, dst, type, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props FROM edges")
        for (src, dst, type_, res, conf, ext, ef, el, props) in cur:
            yield EdgeRec(src=src, dst=dst, type=type_, resolution=res,
                          confidence=conf, extractor=ext, evidence_file=ef,
                          evidence_line=el, props=json.loads(props))

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
