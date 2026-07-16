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

  - T3 carry (cache hardening): `Staging.chunks_missing_embedding` now also re-flags a
    chunk whose `embedded_hash` (the content_hash AT THE TIME it was last embedded)
    disagrees with its CURRENT `content_hash` -- closes the "silently stale embedding
    after an in-run content edit" footgun (same chunk_id, edited text, same
    embed_model -- the pre-T6 `chunks_missing_embedding` only checked embed_model/
    NULL-ness, never content freshness). `set_embeddings` now takes a 4th positional
    field (`content_hash`) per row, written as `embedded_hash`; see stores/staging.py.
  - T4 carry (one header index per workspace): `chunking.augment.fill_headers_all`
    (new, additive -- `fill_headers` itself is untouched) builds ONE `_GraphIndex`
    snapshot covering every currently-staged chunk across every service, instead of
    this module calling the existing per-service `fill_headers` once per service in a
    loop (which would each independently re-scan the whole nodes/edges/chunks tables --
    O(services x graph) instead of O(graph)).

`run(cfg, staging, embedder)`, per service in `cfg.services`:
  1. Read every staged file's bytes off disk (`ServiceConfig.path / relpath`, relpaths
     from `staging.files_for_service` -- the SAME set `analyze_service` already scanned
     and staged earlier in this same `codegraph index` run). A file gone by the time
     THIS read runs (removed/renamed since analyze -- OSError) is warned about and
     skipped, not fatal -- see the loop below.
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

Then, ONCE for the whole workspace (not per service): `fill_headers_all` (see above),
then the embed pass -- SKIPPED (`skipped_no_embedder` counted instead) when `embedder`
is None, e.g. `cli.index`'s graceful-degradation path for a missing local-emb/openai/
voyage dependency or API key, or its explicit `--no-embed` flag. Otherwise:
`chunks_missing_embedding(embedder.model_id)` (workspace-wide, so a chunk from ANY
service that needs (re-)embedding is found, not just the ones this exact call happened
to re-chunk) -- batched <=64 rows at a time, `augment_text` (header + blank line +
code) -> `embedder.embed_batch` -> `set_embeddings` (`embedding.codec.pack_vector` --
the shared float32 little-endian wire format FalkorDB's `vecf32()` expects on read,
see that module for the write/read pairing with `pipeline/load.py`'s decode side)."""

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
from codegraph.stores.staging import Staging

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


def _embed_missing(staging: Staging, embedder: Embedder) -> int:
    """Embeds every chunk `Staging.chunks_missing_embedding` currently flags
    (workspace-wide -- see module docstring), batched <=64 rows per `embed_batch`
    call. Returns the count actually embedded."""
    missing = staging.chunks_missing_embedding(embedder.model_id)
    for i in range(0, len(missing), _EMBED_BATCH_SIZE):
        batch = missing[i : i + _EMBED_BATCH_SIZE]
        texts = [augment.augment_text(row.context_header or "", row.text) for row in batch]
        vectors = embedder.embed_batch(texts)
        staging.set_embeddings([
            (row.chunk_id, pack_vector(vec), embedder.model_id, row.content_hash)
            for row, vec in zip(batch, vectors, strict=True)
        ])
    return len(missing)


def run(cfg: WorkspaceConfig, staging: Staging, embedder: Embedder | None) -> dict:
    """S8: chunk + augment + (maybe) embed every service in `cfg.services`. See module
    docstring for the full per-file/per-workspace breakdown. Returns
    `{chunks_total, embedded, reused, skipped_no_embedder}` (`pipeline.report.
    build_report`'s own per-run S8 line)."""
    nodes_by_file: dict[tuple[str, str], list[NodeRec]] = defaultdict(list)
    for n in staging.iter_nodes():
        if n.relpath is not None:
            nodes_by_file[(n.service, n.relpath)].append(n)

    for svc in cfg.services:
        relpaths = [rp for rp, _ in staging.files_for_service(svc.name)]
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
        embedded, reused, skipped_no_embedder = 0, 0, chunks_total
        model_meta, dim_meta = "", ""
    else:
        embedded = _embed_missing(staging, embedder)
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
        "chunks_total": chunks_total, "embedded": embedded,
        "reused": reused, "skipped_no_embedder": skipped_no_embedder,
    }
