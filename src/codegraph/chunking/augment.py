"""augment.py: M3 T4 graph-aware contextual augmentation headers.

`build_header(staging, chunk)` renders a compact, human-legible "graph position in
words" prefix for one staged chunk -- the header is what the embedder and the fulltext
index actually see (via `augment_text`), while `chunk.text` itself stays pure source
code (the augmented string is never written back into `chunks.text`, only into the
`chunks.context_header` column T3 already added to the DDL -- no DDL change needed
here, see `stores/staging.py`'s `_DDL`).

Header shape (only non-empty lines are emitted, "\\n"-joined; brief's own template):

    file: <relpath> · service: <service>
    symbol: <qualified_name> (<kind>[, roles]) [· parent: <сигнатура родителя>]
    [imports: <up to 8 dotted module names>]
    [doc: <docstring's first line, <=120 chars>]
    [graph: produces <...> · consumes <...> · calls_http <...> · handles <...> ·
     depends_on <...> · calls <...>]

Every piece is sourced STRICTLY from already-staged nodes/edges (`staging.iter_nodes`/
`iter_edges`/`iter_chunks`) -- never a resolver call or a fresh graph traversal -- so
this module has zero SCIP/idiom-matching dependency of its own; whatever the degraded
fallback path (or a real SCIP run) already managed to stage is what shows up here,
nothing more, nothing re-derived.

## Children aggregation (the load-bearing design decision here)

T3's chunker gives a symbol EITHER one whole-body chunk (fits under max_chars -- e.g. a
class with all its methods INSIDE that one chunk, no separate chunk for any method) OR
splits it into a family of per-child chunks (an oversized class: header/gap/tail chunks
under the class's OWN symbol_id, PLUS one independent chunk family per direct method
under EACH method's own symbol_id -- see splitter.py rule 3). PRODUCES/CONSUMES/
CALLS_HTTP/HANDLES/DEPENDS_ON/CALLS edges, however, always attach to the exact def that
contains the call/decorator/dict-entry -- e.g. `OrderService.place`'s PRODUCES edge sits
on `place`, never on the class `OrderService`. In the whole-class-fits case that would
otherwise leave the class's OWN chunk showing an empty graph: block despite visibly
containing `place`'s body -- exactly the information the header exists to surface.

The fix: `build_header` aggregates graph edges over the chunk's OWN symbol_id PLUS every
CONTAINS-descendant of that symbol which has NO CHUNK ROW OF ITS OWN in `chunks`
(`_aggregate_symbols`, recursive) -- stopping at (excluding, not recursing past) any
descendant that DOES own a chunk, since that descendant's own future `build_header` call
is responsible for its own subtree. One rule self-selects the right behavior for every
chunk shape without the caller ever distinguishing them:

  - Whole-class-fits chunk: none of the class's direct methods have their own chunk row
    (T3 never emits one when the class fits as a whole) -- aggregation walks INTO every
    method (and, recursively, any of ITS un-chunked nested closures), pulling `place`'s
    PRODUCES edge into the class chunk's header. This is the case every REAL fixture in
    this repo currently exercises (OrderService, DocumentManagementClient -- T3's own
    report: none of the 29 fixture files split, every class/function chunks whole).
  - Oversized/per-method-split chunk: every direct method DOES have its own chunk row --
    aggregation stops immediately at each one (excluded, never recursed into), so the
    class's header/gap/tail chunk pieces see an EMPTY child set and never duplicate a
    method's edges into both the method's OWN header and the class's header/gap chunks.
    No real fixture is large enough to trigger this path -- covered here only by a
    synthetic oversized-class test.
  - Module chunk: a module's CONTAINS children are exactly its top-level defs, and EVERY
    top-level def always gets its own chunk family unconditionally (`chunk_file`'s loop
    over `top_level`, no size gate at all unlike the in-class-body case) -- so a module
    chunk's aggregation always stops immediately too, and since Module nodes never carry
    PRODUCES/CONSUMES/CALLS_HTTP/HANDLES/DEPENDS_ON/CALLS edges themselves either, the
    graph: line ends up entirely absent for a module chunk. No special-casing needed for
    "is this a module chunk" -- it falls out of the very same rule.

## Edge directions consulted (verified against the actual extractors + a live probe
## against the real fixtures/workspace.yaml pipeline, not assumed)

  - PRODUCES / CONSUMES / CALLS_HTTP / DEPENDS_ON: `src` = the symbol, `dst` = the
    channel/dependency target.
  - HANDLES: `src` = the Channel, `dst` = the handler symbol (fastapi_ext's own
    convention) -- the INVERSE of the other four, called out explicitly in the T4 brief
    ("символ — dst HANDLES").
  - CALLS: `src` = caller, `dst` = callee (extractors/calls.py).
  - CONTAINS: `src` = parent, `dst` = child (python_core.py) -- used here both for
    children-aggregation and for climbing to a method's owning class (`parent:` line)
    and to a symbol's owning module (`imports:` line's module-of-symbol climb).

A channel node's own `.name` (`core/schema.py make_channel_node`) is ALREADY the exact
human-readable string the brief asks for -- `name` verbatim for kafka_topic/event_type,
`"<METHOD> <template>"` for http_route -- so produces/consumes/calls_http/handles all
just read `NodeRec.name` off the channel endpoint with no per-kind branching needed.
Same story for depends_on/calls targets: `NodeRec.name` is already a def's bare
(unqualified) name (`python_core.py`'s `name=d.name`), i.e. already "the last dotted
segment" the brief asks for.

## Deviation from the brief's literal `chunk: ChunkRec` annotation

The brief's interface line reads `build_header(staging, chunk: ChunkRec) -> str`, but
the header's own first line needs `relpath`/`service`, which only exist on
`stores.staging.ChunkRow` (`ChunkRec` is the storage-agnostic splitter output --
service/relpath are staging-only columns `upsert_chunks` injects; see `ChunkRow`'s own
docstring in `stores/staging.py`). Every real call site (`fill_headers`, fed rows from
`staging.chunks_for_service`) hands this function a `ChunkRow` in practice, so that is
the type used here -- `ChunkRec`'s fields are a strict subset of `ChunkRow`'s, so this
only WIDENS what the brief's literal annotation would accept, never narrows it.
"""

from __future__ import annotations

from dataclasses import dataclass

from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.stores.staging import ChunkRow, Staging

_MAX_IMPORTS = 8
_MAX_CALLS = 5
_MAX_DOC_CHARS = 120
_SEP = " · "

# (graph: sub-clause label, edge type, which endpoint holds the aggregated symbol) --
# "src" covers produces/consumes/calls_http/depends_on/calls (symbol -> channel/target);
# "dst" is HANDLES alone, the one inverted direction (channel -> symbol), see module
# docstring's "Edge directions consulted" section. Order here IS the brief's own
# produces/consumes/calls_http/handles/depends_on/calls line order.
_GRAPH_CLAUSES: tuple[tuple[str, str, str], ...] = (
    ("produces", "PRODUCES", "src"),
    ("consumes", "CONSUMES", "src"),
    ("calls_http", "CALLS_HTTP", "src"),
    ("handles", "HANDLES", "dst"),
    ("depends_on", "DEPENDS_ON", "src"),
    ("calls", "CALLS", "src"),
)


@dataclass(frozen=True)
class _GraphIndex:
    """One snapshot of `staging.iter_nodes()`/`iter_edges()`/`iter_chunks()`, built once
    and reused across every chunk of a `fill_headers` batch (`build_header` builds its
    own single-use index each call, so it stays usable standalone) -- avoids an
    O(chunks x edges) full-table rescan when rendering a whole service's worth of
    headers."""

    nodes_by_id: dict[str, NodeRec]
    contains_parent: dict[str, str]
    contains_children: dict[str, list[str]]
    edges_by_src: dict[str, list[EdgeRec]]
    edges_by_dst: dict[str, list[EdgeRec]]
    chunked_symbols: set[str]


def _build_index(staging: Staging, chunks: list[ChunkRow] | None = None) -> _GraphIndex:
    """`chunks`, if given, is used directly for `chunked_symbols` instead of this
    function issuing its OWN `staging.iter_chunks()` call -- an efficiency-only
    parameter (code review finding) for a caller that ALREADY has the full,
    workspace-wide chunk list in hand (`fill_headers_all`, the only caller that passes
    it) and would otherwise cause the exact same unfiltered query to run twice back to
    back. `fill_headers`'s own call (its `chunks` variable is SERVICE-scoped, a
    different, narrower query -- not the same data this function needs for
    `chunked_symbols`, which must see every service's chunks regardless of which one's
    headers are being rendered right now) deliberately leaves this at its default and
    keeps issuing its own workspace-wide `iter_chunks()` call, unchanged."""
    nodes_by_id = {n.id: n for n in staging.iter_nodes()}
    contains_parent: dict[str, str] = {}
    contains_children: dict[str, list[str]] = {}
    edges_by_src: dict[str, list[EdgeRec]] = {}
    edges_by_dst: dict[str, list[EdgeRec]] = {}
    for e in staging.iter_edges():
        if e.type == "CONTAINS":
            contains_parent[e.dst] = e.src
            contains_children.setdefault(e.src, []).append(e.dst)
        edges_by_src.setdefault(e.src, []).append(e)
        edges_by_dst.setdefault(e.dst, []).append(e)
    chunked_symbols = {
        row.symbol_id for row in (chunks if chunks is not None else staging.iter_chunks())
    }
    return _GraphIndex(
        nodes_by_id=nodes_by_id,
        contains_parent=contains_parent,
        contains_children=contains_children,
        edges_by_src=edges_by_src,
        edges_by_dst=edges_by_dst,
        chunked_symbols=chunked_symbols,
    )


def _aggregate_symbols(
    symbol_id: str,
    index: _GraphIndex,
    _seen: set[str] | None = None,
) -> list[str]:
    """`symbol_id` + every CONTAINS-descendant with no chunk row of its own -- see
    module docstring's "Children aggregation" section. `symbol_id` itself is ALWAYS
    included (it's the root of the walk, not a candidate for the own-chunk exclusion --
    that only ever applies to descendants). `_seen` is an internal recursion guard
    against a malformed/cyclic CONTAINS graph (never expected in practice -- CONTAINS is
    a strict tree by construction -- but cheap to guard against, mirrors `_module_of`'s
    own defensive cycle check below)."""
    seen = _seen if _seen is not None else set()
    if symbol_id in seen:
        return []
    seen.add(symbol_id)
    out = [symbol_id]
    for child_id in index.contains_children.get(symbol_id, ()):
        if child_id in index.chunked_symbols:
            continue  # owns its own chunk -- its subtree is its own header's concern
        out.extend(_aggregate_symbols(child_id, index, seen))
    return out


def _normalize_line(text: str) -> str:
    """Collapses any internal whitespace run (including embedded newlines -- e.g. a
    multi-line def signature with a long parameter list) into single spaces, so a
    header line never itself spans multiple physical lines."""
    return " ".join(text.split())


def _first_line(text: str, limit: int) -> str:
    stripped = text.strip()
    first = stripped.split("\n", 1)[0].strip()
    return first[:limit]


def _node_name(node_id: str, index: _GraphIndex) -> str | None:
    """`NodeRec.name` off any endpoint -- already the exact human-readable string
    needed whether `node_id` is a Channel (kafka/event: bare name; http: "METHOD
    template", per `make_channel_node`) or a Function/Class (bare unqualified name,
    "the last dotted segment"). None if the id isn't staged at all (defensive)."""
    node = index.nodes_by_id.get(node_id)
    return node.name if node is not None else None


def _module_of(symbol_id: str, index: _GraphIndex) -> NodeRec | None:
    """Climbs the CONTAINS-parent chain from `symbol_id` up to (and including) the
    first `kind == "Module"` ancestor -- `symbol_id`'s own node if it's already a
    Module (zero climbs). None if the chain runs out, or cycles, before reaching one
    (missing/malformed staging data)."""
    node = index.nodes_by_id.get(symbol_id)
    seen: set[str] = set()
    while node is not None and node.kind != "Module":
        if node.id in seen:
            return None  # defensive cycle guard -- CONTAINS is a tree in practice
        seen.add(node.id)
        parent_id = index.contains_parent.get(node.id)
        node = index.nodes_by_id.get(parent_id) if parent_id is not None else None
    return node


def _imports_line(symbol_id: str, index: _GraphIndex) -> str | None:
    module = _module_of(symbol_id, index)
    if module is None:
        return None
    names = sorted(
        {
            index.nodes_by_id[e.dst].qualified_name
            for e in index.edges_by_src.get(module.id, ())
            if e.type == "IMPORTS" and e.dst in index.nodes_by_id
        }
    )
    if not names:
        return None
    return "imports: " + ", ".join(names[:_MAX_IMPORTS])


def _doc_line(node: NodeRec) -> str | None:
    doc = node.props.get("docstring")
    if not doc:
        return None
    first = _first_line(doc, _MAX_DOC_CHARS)
    return f"doc: {first}" if first else None


def _parent_line(symbol_id: str, index: _GraphIndex) -> str | None:
    """`parent: <class signature>` -- only when `symbol_id`'s OWN CONTAINS-parent is a
    Class (i.e. this chunk's symbol is a method of a class). Uses CONTAINS rather than a
    qualified_name string-prefix check, per the brief's own stated preference ("CONTAINS
    надёжнее")."""
    parent_id = index.contains_parent.get(symbol_id)
    if parent_id is None:
        return None
    parent = index.nodes_by_id.get(parent_id)
    if parent is None or parent.kind != "Class":
        return None
    sig = parent.props.get("signature")
    if not sig:
        return None
    return f"parent: {_normalize_line(sig)}"


def _symbol_line(node: NodeRec, index: _GraphIndex) -> str:
    kind_and_roles = node.kind
    if node.roles:
        kind_and_roles += ", " + ", ".join(sorted(node.roles))
    line = f"symbol: {_normalize_line(node.qualified_name)} ({kind_and_roles})"
    parent = _parent_line(node.id, index)
    if parent:
        line += _SEP + parent
    return line


def _graph_line(symbol_id: str, index: _GraphIndex) -> str | None:
    aggregated = _aggregate_symbols(symbol_id, index)
    clauses: list[str] = []
    for label, edge_type, symbol_side in _GRAPH_CLAUSES:
        names: set[str] = set()
        for sid in aggregated:
            edges = (
                index.edges_by_src.get(sid, ())
                if symbol_side == "src"
                else index.edges_by_dst.get(sid, ())
            )
            for e in edges:
                if e.type != edge_type:
                    continue
                other_id = e.dst if symbol_side == "src" else e.src
                name = _node_name(other_id, index)
                if name:
                    names.add(name)
        if not names:
            continue
        ordered = sorted(names)
        if label == "calls":
            ordered = ordered[:_MAX_CALLS]
        clauses.append(f"{label} {', '.join(ordered)}")
    if not clauses:
        return None
    return "graph: " + _SEP.join(clauses)


def _render_header(index: _GraphIndex, chunk: ChunkRow) -> str:
    lines = [f"file: {chunk.relpath}{_SEP}service: {chunk.service}"]
    node = index.nodes_by_id.get(chunk.symbol_id)
    if node is None:
        # Defensive fallback for out-of-sync staging (a chunk referencing a symbol_id
        # with no corresponding staged node) -- not expected on the real pipeline
        # (chunk_file's symbol_ids always mirror analyze_service's own node_ids map),
        # but rendering SOMETHING beats crashing a whole fill_headers batch on one row.
        lines.append(f"symbol: {chunk.symbol_id} (unknown)")
        return "\n".join(lines)

    lines.append(_symbol_line(node, index))
    imports_line = _imports_line(chunk.symbol_id, index)
    if imports_line:
        lines.append(imports_line)
    doc_line = _doc_line(node)
    if doc_line:
        lines.append(doc_line)
    graph_line = _graph_line(chunk.symbol_id, index)
    if graph_line:
        lines.append(graph_line)
    return "\n".join(lines)


def build_header(staging: Staging, chunk: ChunkRow) -> str:
    """Renders one chunk's header -- see module docstring for the full shape/rules.
    Builds its own `_GraphIndex` snapshot (a fresh `iter_nodes`/`iter_edges`/
    `iter_chunks` pass) each call, so this stays usable standalone (tests, one-off
    calls); see `fill_headers` for the batch path that builds the index once and reuses
    it across a whole service's chunks."""
    return _render_header(_build_index(staging), chunk)


def augment_text(header: str, chunk_text: str) -> str:
    """header + a blank line + the chunk's own (unmodified) code text -- what actually
    goes to the embedder/fulltext index. `chunk.text` in staging itself stays pure
    source (see module docstring's opening paragraph) -- this function's RETURN value
    is never written back into `chunks.text`."""
    return header + "\n\n" + chunk_text


def fill_headers(staging: Staging, service: str) -> int:
    """Orchestration, scoped to ONE service: `build_header` for every chunk currently
    staged for `service`, batched into ONE `set_context_headers` call. Idempotent --
    header content is a pure function of currently-staged nodes/edges/chunks, and
    `set_context_headers` is a plain UPDATE-by-chunk_id, so re-running this against
    unchanged staging state re-writes the exact same header strings. Returns the number
    of chunks updated (0 for a service with no staged chunks yet -- skips building the
    graph index entirely in that case, not just the no-op `set_context_headers` call).

    NOT the function `chunk_embed.run` (S8/T6) actually calls -- looping THIS function
    once per service would rebuild `_GraphIndex` (a full nodes/edges/chunks scan) once
    per service too (O(services x graph), the exact anti-pattern the T4 review flagged
    as a mandatory M3 T6 carry). See `fill_headers_all` below for the workspace-wide
    sibling T6 actually uses; this per-service version stays available (and tested)
    standalone since it's still the more convenient shape for a single-service caller
    (e.g. a future incremental-reindex path, M4)."""
    chunks = staging.chunks_for_service(service)
    if not chunks:
        return 0
    index = _build_index(staging)
    rows = [(c.chunk_id, _render_header(index, c)) for c in chunks]
    staging.set_context_headers(rows)
    return len(rows)


def fill_headers_all(staging: Staging) -> int:
    """Workspace-wide sibling of `fill_headers` (M3 T6 carry, mandatory per the T4
    review): renders headers for EVERY currently-staged chunk, across ALL services, in
    ONE call -- building exactly ONE `_GraphIndex` snapshot for the whole call, instead
    of `chunk_embed.run` looping `fill_headers(staging, svc.name)` once per service
    (each of which would independently re-scan the full nodes/edges/chunks tables --
    O(services x graph) instead of O(graph); see `_GraphIndex`'s own docstring for why
    that per-call rescan exists at all). Semantics are otherwise IDENTICAL to
    `fill_headers` run once per service -- same header content per chunk (both go
    through the same `_render_header`), same idempotency (a pure function of currently-
    staged nodes/edges/chunks), same one-batched-`set_context_headers`-call shape.
    Returns the total number of chunks updated (0 -- without ever building the graph
    index -- if nothing is staged yet anywhere, mirroring `fill_headers`'s own
    no-chunks-yet shortcut)."""
    chunks = list(staging.iter_chunks())
    if not chunks:
        return 0
    # Passes the already-fetched `chunks` straight into `_build_index` (its own
    # `chunked_symbols` computation would otherwise issue the EXACT SAME unfiltered
    # `staging.iter_chunks()` query a second time back to back -- code review finding;
    # see `_build_index`'s own docstring for why `fill_headers` itself doesn't do this).
    index = _build_index(staging, chunks=chunks)
    rows = [(c.chunk_id, _render_header(index, c)) for c in chunks]
    staging.set_context_headers(rows)
    return len(rows)
