"""chunk_embed.py: M3 T6 S8 stage -- chunk every staged file, augment with graph-aware
headers, and embed any chunk that needs it.

Runs AFTER S7 (`linking.workspace.link_workspace`) and BEFORE S9 (`pipeline.load.
load_graph`) in `cli.index` -- augmentation headers read graph POSITION (CONTAINS/
PRODUCES/CONSUMES/HANDLES/DEPENDS_ON/CALLS) off staged edges, which only exist once
EVERY service has been analyzed (S1-S6) AND link_workspace has derived its own
cross-service edges (NEXT_SEGMENT/CALLS_HTTP/...); reading files/chunking earlier
(interleaved with each service's own analyze_service call) would work for the chunks
themselves but not yet for headers, so this stage -- chunking AND embedding both --
waits for the whole workspace instead of splitting across two different pipeline
points.

Mandatory carries from the T3/T4 review (both closed HERE, in their first real
caller, not in chunking/splitter.py or chunking/augment.py themselves):

  - T3 carry (cache hardening): `Staging.chunks_missing_embedding` also re-flags a
    chunk whose `embedded_hash` disagrees with its CURRENT hash -- closes the
    "silently stale embedding after an in-run content edit" footgun (same chunk_id,
    edited text, same embed_model -- the pre-T6 `chunks_missing_embedding` only
    checked embed_model/NULL-ness, never content freshness). `set_embeddings` takes a
    4th positional field per row, written as `embedded_hash`; see stores/staging.py.
    M4 T1 changed WHAT that 4th field/`embedded_hash` compare against: `content_hash`
    (the chunk's own source text alone) through M3 T6, `input_hash` (the exact
    embedder input's hash -- text AND augmentation header -- see `chunking.augment.
    _input_hash`) from M4 T1 onward, which additionally catches a header-only change
    content_hash alone could never see (see core/schema.py's SCHEMA_VERSION "4 -> 5"
    history entry).
  - T4 carry (one header index per workspace): `chunking.augment.fill_headers_all`
    (new, additive -- `fill_headers` itself is untouched) builds ONE `_GraphIndex`
    snapshot covering every currently-staged chunk across every service, instead of
    this module calling the existing per-service `fill_headers` once per service in a
    loop (which would each independently re-scan the whole nodes/edges/chunks tables --
    O(services x graph) instead of O(graph)).

`run(cfg, staging, embedder, changed_files=None)`, per service in `cfg.services`:
  1. Read every staged file's bytes off disk (`ServiceConfig.path / relpath`, relpaths
     from `staging.files_for_service` -- the SAME set `analyze_service` already scanned
     and staged earlier in this same `codegraph index` run -- unless `changed_files` is
     given, see the M4 T6 section below, in which case relpaths come from THAT instead).
     A file gone by the time THIS read runs (removed/renamed since analyze -- OSError) is
     warned about and skipped, not fatal -- see the loop below.
  2. `build_file_facts` (tree-sitter) -- the same parse `analyze_service` itself ran;
     re-run here rather than cached, since `chunk_embed` has no access to
     `analyze_service`'s ephemeral in-memory `FileFacts` (only their DERIVED staged
     nodes/edges survive into this later stage).
  3. `symbol_ids`/`module_id` for `chunk_file`: matched back to the REAL node ids
     `analyze_service` already staged for this exact (service, relpath) -- by
     `(start_byte, end_byte)` span against already-staged Class/Function nodes (kind
     == "Module" gives `module_id`) -- rather than re-deriving ids independently
     (which would duplicate `extractors.python_core._def_id`'s SCIP-vs-structural
     branching and risk drifting out of sync with it, e.g. after a future change to
     that logic). Every `FileFacts.defs` entry, at every nesting level, has a matching
     staged node (`python_core.extract` emits one `NodeRec` per `defs` entry
     unconditionally, not just top-level ones -- see `_symbol_ids_for_file`), so this
     span-match can never KeyError on a def `chunk_file` actually needs an id for.
  4. `chunk_file` -> `staging.upsert_chunks` (per-file, matches `upsert_chunks`' own
     one-file-per-call contract).

The node index (`(service, relpath) -> [NodeRec, ...]`) is built ONCE, from a single
`staging.iter_nodes()` pass, up front -- reused across every file of every service in
step 3 above, the same "read once, reuse many" discipline as the T4 carry.

Then, ONCE for the whole workspace (not per service): `fill_headers_all` (see above --
M4 T1 addition: this ALSO (re)computes every chunk's `input_hash`, unconditionally),
then the embed pass -- SKIPPED (`skipped_no_embedder` counted instead) when `embedder`
is None, e.g. `cli.index`'s graceful-degradation path for a missing local-emb/openai/
voyage dependency or API key, or its explicit `--no-embed` flag. Otherwise:
`chunks_missing_embedding(embedder.model_id)` (workspace-wide, so a chunk from ANY
service that needs (re-)embedding is found, not just the ones this exact call happened
to re-chunk) -- EVERY missing row is first checked against the persistent, cross-run
`Staging.embedding_cache` table (M4 T1, keyed on `(input_hash, embed_model)` -- see
`_embed_missing`'s own docstring): a cache HIT reuses the stored vector via
`set_embeddings` alone, at zero provider cost; only a genuine cache MISS is batched
<=64 rows at a time, run through `augment_text` (header + blank line + code) ->
`embedder.embed_batch` -> `set_embeddings` (`embedding.codec.pack_vector` -- the shared
float32 little-endian wire format FalkorDB's `vecf32()` expects on read, see that
module for the write/read pairing with `pipeline/load.py`'s decode side) ->
`embedding_cache_put` (so a LATER run reuses it too, even across a full
`begin_service` wipe-and-recreate of this exact chunk_id). `run`'s own report splits
the combined `embedded` count into `embedded_fresh` (genuine provider calls) and
`embedded_from_cache` (free reuses) -- `cli.index`'s paid-provider warning keys off
`embedded_fresh` alone, not the combined total (see `pipeline/report.py`/`cli.py`).

M4 T6 (`changed_files: dict[str, set[str]] | None = None`, new trailing parameter):
`None` (the default) reproduces every byte of the behavior described above, unchanged
-- every existing caller (`cli.py`'s own `run_chunk_embed(cfg, staging, embedder)`
call, every pre-M4 test) stays byte-identical, since step 1's per-service relpath list
is computed exactly as it always was. A non-None dict instead SCOPES step 1's per-file
loop to `changed_files.get(svc.name, set())` -- a service with no key in the dict has
its ENTIRE chunk loop skipped (`.get(..., set())` defaults to an empty set, so the
inner per-file loop simply never runs for it), not merely left with a coincidentally
empty relpath list. Chunks already staged for a relpath NOT named in
`changed_files[svc.name]` are left completely alone by this loop -- they simply
persist in `chunks` exactly as some EARLIER run (or, within this same run, an earlier
`analyze_service` call that never had its own `begin_service` invoked -- T5's
"skipped"/"incremental" modes) last left them.

`fill_headers_all` and the embed pass (both described just above) are DELIBERATELY
left workspace-wide and unconditional in BOTH the None and non-None cases -- this is
the one part of S8 that must NOT be `changed_files`-scoped. A header renders a
chunk's GRAPH POSITION (imports/produces/consumes/handles/depends_on/calls -- see
`chunking.augment`'s own module docstring), and an edit confined to service A's own
files can change a CALLS/DEPENDS_ON/... edge whose OTHER endpoint sits in service B's
completely untouched chunk (e.g. renaming a function in A changes the `calls:` clause
of every OTHER chunk, anywhere in the workspace, that calls it). Recomputing every
header is an in-memory, staging-only pass (one `_GraphIndex` snapshot, see the T4
carry above) -- cheap regardless of how many files were actually re-chunked this run
-- and its own `input_hash` write-back is what keeps the embed pass HONEST despite
staying workspace-wide: a chunk whose header text didn't actually change is
recomputed to the exact SAME `input_hash` it already had, so `chunks_missing_
embedding` (itself completely unchanged by this task) leaves it alone (free
`reused`); a chunk whose header DID change gets a genuinely NEW `input_hash`, so that
SAME workspace-wide embed pass picks it up for a real re-embed -- through the T1
persistent cache when that exact input was embedded before (e.g. a rename-and-back),
or a real provider call otherwise. Scoping the embed pass to `changed_files` too
(instead of leaving it workspace-wide) would MISS exactly this cross-service
header-drift case -- a chunk whose OWN file was never touched this run would keep
serving a stale vector for its now-wrong header forever.

Caller precondition this parameter leans on (verified against `upsert_chunks`' actual
behavior, not just assumed -- see the M4 T6 task brief's own "IMPORTANT deletion
nuance"): `upsert_chunks` (see its own docstring, `stores/staging.py`) is a
per-`chunk_id` UPSERT, never a per-file REPLACE -- it has no way to notice that a
re-chunked file now produces FEWER pieces than it used to (e.g. a function that
shrank from a 2-piece line-split, ord 0/1, down to a single ord-0 piece) and delete
the now-surplus OLD chunk_id on its own. Calling this function with a relpath in
`changed_files[svc.name]` is therefore only SAFE when the caller has already cleared
that relpath's entire OLD chunk layer first: `Staging.delete_file_layer(service,
relpaths, ...)` deletes chunks (alongside nodes/claims) `WHERE service=? AND relpath
IN (...)`, so a relpath re-chunked immediately after a `delete_file_layer` call that
included it starts from a genuinely EMPTY set of rows -- the following `upsert_chunks`
call is then a plain from-scratch insert, and no surplus ordinal can survive something
that was never there to begin with. This precondition holds by construction for
`changed_files`' one intended real producer: T5's `_analyze_incremental` calls
`staging.delete_file_layer(svc.name, stale | dead, ...)` for its entire stale set
BEFORE its own S5/S6 re-run (`pipeline/analyze.py` step 7) -- and `chunk_embed.run`
(S8) only ever executes AFTER S1-S7 has fully completed for the whole workspace (see
this module's own opening paragraph), so by the time this function's changed_files-
scoped loop reaches any relpath T7 threads through from that same stale set, its
chunk layer has already been sitting empty since T5's own pass. `chunk_embed.run`
does NOT re-verify or re-enforce this itself (no redundant delete-then-insert here)
-- it is a documented CONTRACT of the parameter, not a defensive runtime check,
matching this module's existing trust-the-caller posture elsewhere (`chunk_file`'s
own `symbol_ids` KeyError-trusts-caller contract, see its docstring). A caller that
violates this precondition (passes a relpath whose old chunk layer was never
cleared) observes exactly the surplus-row symptom described above, not a crash --
both halves (orphan-without-precondition, clean-with-precondition) are pinned
directly, at the `Staging` level, by `tests/unit/test_staging.py::
test_upsert_chunks_after_delete_file_layer_leaves_no_orphaned_chunk_id`."""

from __future__ import annotations

import logging
from collections import defaultdict

from codegraph.chunking import augment
from codegraph.chunking.splitter import chunk_file
from codegraph.config.models import WorkspaceConfig
from codegraph.core.schema import NodeRec
from codegraph.embedding.base import Embedder
from codegraph.embedding.codec import pack_vector
from codegraph.parsing.facts import FileFacts, build_file_facts
from codegraph.stores.staging import ChunkRow, Staging

logger = logging.getLogger(__name__)

_EMBED_BATCH_SIZE = 64


def _symbol_ids_for_file(
    file_nodes: list[NodeRec], facts: FileFacts
) -> tuple[dict[int, str], str] | None:
    """`chunk_file`'s own `(symbol_ids, module_id)` inputs, rebuilt from ALREADY-staged
    nodes for one (service, relpath) -- see module docstring point 3. `file_nodes` is
    every node `analyze_service` staged for this exact file (one Module + one Class/
    Function per `FileFacts.defs` entry, at every nesting level -- `python_core.
    extract`'s own unconditional per-def loop); span-matching `(start_byte, end_byte)`
    against `facts.defs` finds the REAL id `analyze_service` assigned each one,
    whichever branch (SCIP-resolved or structural-fallback) actually produced it.

    Returns None (sweep-review fix -- callers skip the file with a warning) when the
    fresh parse's spans DON'T all match staged nodes, or no Module node is staged for
    the file at all: `run` re-reads file bytes off disk independently of the read
    `analyze_service` did earlier in the same `codegraph index` invocation, with no
    lock between them -- a file modified in that window (autosave, formatter, a
    concurrent build step touching sources mid-index) in a way that SHIFTS def byte
    spans would otherwise KeyError here, crashing the whole invocation and discarding
    S1-S7's already-completed work over one racing file. (A same-length edit that
    leaves spans in place still matches -- intentionally: the staged node ids are
    still span-correct, and the hash-aware re-embed path handles the changed text,
    see Staging.chunks_missing_embedding.) The skipped file's chunks are simply
    whatever staging already had for it -- consistent with the staged NODES, which
    are equally from the analyze-time content."""
    by_span = {
        (n.start_byte, n.end_byte): n.id
        for n in file_nodes
        if n.kind in ("Class", "Function")
    }
    module_id = next((n.id for n in file_nodes if n.kind == "Module"), None)
    if module_id is None:
        return None
    symbol_ids: dict[int, str] = {}
    for d in facts.defs:
        node_id = by_span.get((d.start_byte, d.end_byte))
        if node_id is None:
            return None
        symbol_ids[d.index] = node_id
    return symbol_ids, module_id


def _embed_missing(staging: Staging, embedder: Embedder) -> tuple[int, int]:
    """Embeds every chunk `Staging.chunks_missing_embedding` currently flags
    (workspace-wide -- see module docstring), first partitioned against the
    persistent, cross-run `Staging.embedding_cache` table (M4 T1):

      - cache HIT: a missing chunk whose exact embedder input (its own `input_hash`
        -- already fresh, since `run()` always calls `augment.fill_headers_all`
        immediately before this function, see that function's own M4 T1 addition)
        already has a vector cached under this exact `embedder.model_id` reuses it
        via `set_embeddings` alone, at ZERO provider cost. Additionally gated on the
        cached vector's DECODED dimension (`len(blob) // 4`, the same convention
        `embedding.codec.unpack_vector` uses) matching `embedder.dim` -- a mismatch
        (e.g. a corrupt/unexpected blob length, or two genuinely different-dimension
        embedders that happen to share a `model_id` string) is treated as a MISS, not
        a crash: it falls through to a real `embed_batch` call and the cache entry is
        overwritten with a fresh, correctly-sized vector.
      - cache MISS: batched <=64 rows per `embed_batch` call (unchanged batch size),
        `set_embeddings` writes the fresh vector into `chunks` AND
        `embedding_cache_put` writes it into the persistent cache too, so a LATER run
        (even one that wipes and recreates this exact chunk_id via `begin_service`)
        can reuse it.

    Both partitions preserve `missing`'s own order (`chunks_missing_embedding`'s own
    `ORDER BY chunk_id`) -- cache hits are written in one `set_embeddings` call up
    front, then cache misses are walked in that SAME relative order for batching, so
    which rows land in which `embed_batch` call is fully deterministic for a given
    staging state.

    Returns `(embedded_fresh, embedded_from_cache)` -- `run`'s own report counters
    (see its docstring)."""
    missing = staging.chunks_missing_embedding(embedder.model_id)
    if not missing:
        return 0, 0

    # `row.input_hash` is expected to already be fresh here (see docstring above) --
    # a defensive `is not None` guard routes a chunk with no input_hash yet (not
    # reachable via `run()`'s own call order, but this function has no other way to
    # enforce that a caller behaves) straight to a cache miss, rather than passing
    # `None` into `embedding_cache_get`'s `list[tuple[str, str]]` contract.
    cache_pairs = [
        (row.input_hash, embedder.model_id) for row in missing if row.input_hash is not None
    ]
    cached = staging.embedding_cache_get(cache_pairs) if cache_pairs else {}

    cache_hits: list[tuple[str, bytes, str, str]] = []
    cache_misses: list[ChunkRow] = []
    for row in missing:
        vec_blob = (
            cached.get((row.input_hash, embedder.model_id))
            if row.input_hash is not None else None
        )
        if vec_blob is not None and len(vec_blob) // 4 == embedder.dim:
            cache_hits.append((row.chunk_id, vec_blob, embedder.model_id, row.input_hash))
        else:
            cache_misses.append(row)

    if cache_hits:
        staging.set_embeddings(cache_hits)

    for i in range(0, len(cache_misses), _EMBED_BATCH_SIZE):
        batch = cache_misses[i : i + _EMBED_BATCH_SIZE]
        texts = [augment.augment_text(row.context_header or "", row.text) for row in batch]
        vectors = embedder.embed_batch(texts)
        packed = [pack_vector(vec) for vec in vectors]
        staging.set_embeddings([
            (row.chunk_id, blob, embedder.model_id, row.input_hash)
            for row, blob in zip(batch, packed, strict=True)
        ])
        staging.embedding_cache_put([
            (row.input_hash, embedder.model_id, embedder.dim, blob)
            for row, blob in zip(batch, packed, strict=True)
        ])

    return len(cache_misses), len(cache_hits)


def run(
    cfg: WorkspaceConfig,
    staging: Staging,
    embedder: Embedder | None,
    changed_files: dict[str, set[str]] | None = None,
) -> dict:
    """S8: chunk + augment + (maybe) embed every service in `cfg.services`. See module
    docstring for the full per-file/per-workspace breakdown. Returns
    `{chunks_total, embedded, embedded_fresh, embedded_from_cache, reused,
    skipped_no_embedder}` (`pipeline.report.build_report`'s own per-run S8 line) --
    `embedded_fresh`/`embedded_from_cache` are M4 T1's own addition (persistent
    embedding cache, see `_embed_missing`'s docstring): `embedded` remains their SUM,
    unchanged in meaning from every pre-M4 consumer's point of view (total chunks
    that got a usable vector THIS call, whether via a real provider call or a cache
    reuse).

    `changed_files` (M4 T6, `{service: {relpath, ...}}`) -- `None` (every pre-M4
    caller, unchanged) chunks every currently-staged file of every configured
    service, exactly as before; given, the chunk loop below is scoped to
    `changed_files.get(svc.name, set())` per service instead -- see the module
    docstring's own "M4 T6" section for the full contract (scope, the deliberately
    still-workspace-wide header/embed phases, and the caller-side deletion
    precondition this parameter leans on)."""
    nodes_by_file: dict[tuple[str, str], list[NodeRec]] = defaultdict(list)
    for n in staging.iter_nodes():
        if n.relpath is not None:
            nodes_by_file[(n.service, n.relpath)].append(n)

    for svc in cfg.services:
        if changed_files is None:
            relpaths = [rp for rp, _ in staging.files_for_service(svc.name)]
        else:
            # sorted(): deterministic iteration order over a caller-supplied `set`
            # (matches this codebase's existing "sorted files for determinism"
            # convention elsewhere, e.g. pipeline/analyze.py's own `stale_sorted`) --
            # correctness doesn't depend on it (each file's chunking is independent),
            # but log/warning order and test assertions do.
            relpaths = sorted(changed_files.get(svc.name, set()))
        for rp in relpaths:
            try:
                source = (svc.path / rp).read_bytes()
            except OSError as e:
                # Same race class as the span-shift skip just below (staged-vs-disk
                # drift within one `codegraph index` invocation), just the more
                # extreme case: the file isn't merely edited, it's GONE (removed/
                # renamed since `analyze_service` scanned it earlier in this same
                # run). One missing file must not crash the whole invocation and
                # discard every other file/service's already-completed work.
                logger.warning(
                    "%s/%s: could not read file off disk (removed since analyze ran "
                    "earlier in this same index invocation?) -- skipping chunking "
                    "for this file: %s",
                    svc.name, rp, e,
                )
                continue
            facts = build_file_facts(rp, source)
            resolved = _symbol_ids_for_file(nodes_by_file.get((svc.name, rp), []), facts)
            if resolved is None:
                logger.warning(
                    "%s/%s: staged node spans no longer match the file's current "
                    "on-disk content (file changed since analyze ran earlier in this "
                    "same index invocation?) -- skipping chunking for this file",
                    svc.name, rp,
                )
                continue
            symbol_ids, module_id = resolved
            chunks = chunk_file(rp, source, facts, symbol_ids, module_id)
            staging.upsert_chunks(svc.name, rp, chunks)

    augment.fill_headers_all(staging)

    # Workspace-wide (staging.counts()["chunks"]) -- NOT "however many chunks THIS
    # run's cfg.services loop just produced". This keeps `reused = chunks_total -
    # embedded` non-negative by construction: `embedded` comes from `_embed_missing`'s
    # own workspace-wide `chunks_missing_embedding` scan, which can never return more
    # rows than exist in the whole `chunks` table -- so it can never exceed a
    # workspace-wide `chunks_total` either. A `cfg.services`-scoped count could NOT
    # make that same guarantee: a service removed from `codegraph.yaml` without
    # deleting `.codegraph/staging.db` leaves its stale, never-re-chunked-this-run
    # rows sitting in the table, where `_embed_missing`'s workspace-wide scan (by
    # design -- see that function's own docstring) would still pick them up and embed
    # them, making `embedded` exceed a services-scoped `chunks_total` and driving
    # `reused` negative (a real, since-fixed bug -- caught in code review).
    chunks_total = staging.counts()["chunks"]

    if embedder is None:
        embedded_fresh, embedded_from_cache, reused, skipped_no_embedder = 0, 0, 0, chunks_total
        model_meta, dim_meta = "", ""
    else:
        embedded_fresh, embedded_from_cache = _embed_missing(staging, embedder)
        embedded = embedded_fresh + embedded_from_cache
        reused, skipped_no_embedder = chunks_total - embedded, 0
        model_meta, dim_meta = embedder.model_id, str(embedder.dim)

    # "" (not a missing key) for the no-embedder case -- pipeline.load._embed_meta
    # reads "" back as None, same as absent, but writing it explicitly here CLEARS any
    # embed_model/dim a PRIOR run left behind. Every `codegraph index` run re-chunks
    # EVERY configured service from scratch (this run's chunking loop above just did
    # exactly that), so if THIS run has no embedder, no chunk in the whole workspace
    # has a live embedding right now -- Meta must not keep advertising a stale model/
    # dim that no longer matches any actual Chunk.embedding in the graph about to be
    # loaded.
    staging.set_meta("embed_model", model_meta)
    staging.set_meta("embed_dim", dim_meta)
    return {
        "chunks_total": chunks_total,
        "embedded": embedded_fresh + embedded_from_cache,
        "embedded_fresh": embedded_fresh,
        "embedded_from_cache": embedded_from_cache,
        "reused": reused,
        "skipped_no_embedder": skipped_no_embedder,
    }
