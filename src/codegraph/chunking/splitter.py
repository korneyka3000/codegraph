"""splitter.py: symbol-aligned, size-bounded AST chunking (M3 T3).

`chunk_file` splits one file's source into `ChunkRec` pieces, each attached to exactly
ONE symbol_id (a node id keyed off `FileFacts.defs[i].index`, or the file's own module
id for module-level content). `chunk_id` (`f"{symbol_id}#c{ord}"`) is the join key back
to the graph -- the "bridge" between retrieval (chunk hit) and the graph (symbol node)
that M3 is building. `ord` is a per-`symbol_id` sequence (0..N for a symbol split across
several pieces), never per-file -- see ChunkRec's own docstring.

Rules (byte length -- `end_byte - start_byte`, or a line-split piece's byte span -- is
what's compared against `max_chars` throughout, not decoded character count; for
plain-ASCII source the two coincide, and this mirrors the brief's own "end-start <=
max_chars" wording, which already conflates the two):

  1. Module preamble: bytes from 0 to the start_byte of the FIRST top-level def/class
     (the whole file if there are no defs at all) becomes ONE chunk under `module_id`,
     ord 0 -- IF non-empty after `.strip()`. Unlike every other rule below, this single
     chunk is never further size-split, even if it exceeds `max_chars` on its own (the
     brief specifies exactly one chunk, ord 0, for the preamble; splitting it would be a
     departure from that, so it's deliberately out of scope here). Code that lives
     AFTER the first top-level def but isn't itself part of any def's own byte span
     (e.g. a decorator line on the second-or-later top-level def, or a bare module-level
     statement between two defs) is likewise out of scope for T3 -- see rule 3 for why
     the analogous gap WITHIN an oversized class needs special handling but this
     module-level one doesn't: splitting a class into header+methods carves new seams
     out of what was previously one contiguous, fully-covered chunk, so preserving its
     in-between content is what keeps that split lossless; top-level defs were already
     discontiguous islands in the source (separated by imports/blank lines/decorators)
     before chunking ever entered the picture, so chunking them doesn't newly lose
     anything a whole-file single chunk wouldn't already have had to special-case.
  2. Each top-level def/class that fits within `max_chars` bytes becomes exactly one
     chunk under its own symbol_id, ord 0 (methods are NOT split out -- the class's
     bytes, methods included, are one chunk).
  3. A top-level CLASS bigger than `max_chars`: every DIRECT child def (a DefFact whose
     `parent` is the class's own index -- "method" includes a nested class, not just
     `kind == "function"`) gets its own chunk family under ITS OWN symbol_id (further
     line-split via rule 4 if the method itself is still bigger than `max_chars`).
     Everything else that belongs to the class body -- the "header" before the first
     method, any code between two methods, any trailing code after the last method
     (docstring, class-level assignments, decorators on a non-first method, comments,
     blank lines) -- is glued onto the CLASS's own symbol_id as ADDITIONAL chunks,
     continuing ONE shared ord sequence in source order: ord 0 is always the true
     header (class start .. first method start -- never whitespace-only in valid
     Python, since it always contains at least the `class ...:` line), and each
     subsequent non-whitespace gap continues that same sequence (ord 1, 2, ...); a gap
     that is only whitespace is silently dropped instead of emitting an empty chunk
     (mirrors rule 1's "non-empty after strip" gate). Any of these header/gap pieces
     that is itself bigger than `max_chars` is further line-split exactly like rule 4,
     still continuing the same ord sequence. A class with NO direct methods at all
     (rare -- e.g. a huge class made entirely of attribute assignments) falls back to
     rule 4's line-split applied to the whole class body under the class's own
     symbol_id, ord 0..N (there's no method boundary to split on, so this degrades to
     "oversized def, not a class" handling).
  4. Any function/method (top-level, or a class method under rule 3) bigger than
     `max_chars` bytes is split on LINE boundaries into consecutive pieces of at most
     `max_chars` bytes each (ord 0..N under that same symbol_id): lines are accumulated
     greedily into a piece until the NEXT line would push it over `max_chars`, at which
     point the piece is closed and a new one started -- never cutting a line in the
     middle. A single line that is by itself already longer than `max_chars` is the one
     documented exception to the size bound: it is emitted whole as its own piece
     rather than being split mid-line.
  5. Nested functions/classes (definitions nested two or more levels below a top-level
     def -- e.g. a helper closure inside a top-level function, or a method's own local
     def) are NEVER chunked separately: their bytes are simply part of whichever
     enclosing piece covers that byte range. The only definitions that ever get their
     own symbol_id chunks are top-level defs (rule 2/4) and, under rule 3, the DIRECT
     methods of an oversized top-level class -- a method nested inside ANOTHER method of
     that same class does not get pulled out again.
  6. `text` is always `source[start:end].decode("utf-8", errors="replace")` for a
     contiguous byte range (never a concatenation of non-adjacent ranges -- every
     ChunkRec's text is one plain slice); `start_line`/`end_line` are the 1-based line
     numbers of the first and last byte actually included in that range.

Chunks never overlap in bytes (every byte range handed to `_make_chunk`/`_split_lines`
comes from a disjoint partition of a def's own span, or of the whole file for the
preamble); every top-level def is covered by exactly one symbol_id family of chunks
(itself, or -- rule 3 -- the class + its direct methods) modulo whitespace-only gaps.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass

from codegraph.parsing.facts import DefFact, FileFacts


@dataclass(frozen=True)
class ChunkRec:
    """One retrieval unit. `chunk_id` (`f"{symbol_id}#c{ord}"`, built by `_make_chunk`)
    is the join key back to the graph. `ord` is a per-`symbol_id` sequence (0, 1, 2, ...
    for a symbol split into several pieces) -- SEQUENTIAL WITHIN one symbol_id, not a
    file-wide counter, so two different symbols' chunks both legitimately start at
    ord 0. `content_hash` is `sha256(text.encode("utf-8")).hexdigest()` -- hashed off
    the DECODED text (what actually gets embedded/stored), not the raw source bytes, so
    it stays meaningful even for the errors="replace" edge case (rule 6)."""

    chunk_id: str
    symbol_id: str
    ord: int
    text: str
    start_line: int  # 1-based, inclusive
    end_line: int  # 1-based, inclusive
    content_hash: str


def chunk_file(
    relpath: str,
    source: bytes,
    facts: FileFacts,
    symbol_ids: dict[int, str],
    module_id: str,
    max_chars: int = 2000,
) -> list[ChunkRec]:
    """See the module docstring for the full rule set (1-6). `symbol_ids` maps a
    `DefFact.index` to that def's node id (built by the caller the same way
    `pipeline.analyze`'s own per-file `node_ids` map is -- one entry per `facts.defs`
    element); every top-level def and, for an oversized class, every direct method MUST
    have an entry here (a missing one raises a plain KeyError -- chunk_file trusts its
    caller the same way python_core.extract trusts its own def_ids map, no defensive
    re-validation). `relpath` is accepted but unused by the pure byte-range logic below
    -- kept purely so this call's signature stays self-describing at call sites (mirrors
    `extractors.base.FileContext`'s own `(service, relpath, ...)` shape); see rule 1's
    note on why content between/after top-level defs at the module level is out of
    scope for this function regardless of relpath.
    """
    line_starts = _line_starts(source)
    children_of: dict[int, list[DefFact]] = {}
    top_level: list[DefFact] = []
    for d in facts.defs:
        (top_level if d.parent is None else children_of.setdefault(d.parent, [])).append(d)
    top_level.sort(key=lambda d: d.start_byte)
    for kids in children_of.values():
        kids.sort(key=lambda k: k.start_byte)

    chunks: list[ChunkRec] = []
    preamble_end = top_level[0].start_byte if top_level else len(source)
    if source[:preamble_end].strip():
        chunks.append(_make_chunk(module_id, 0, 0, preamble_end, source, line_starts))

    for d in top_level:
        chunks.extend(
            _chunk_top_level_def(d, children_of, symbol_ids, source, line_starts, max_chars)
        )

    return chunks


def _chunk_top_level_def(
    d: DefFact,
    children_of: dict[int, list[DefFact]],
    symbol_ids: dict[int, str],
    source: bytes,
    line_starts: list[int],
    max_chars: int,
) -> list[ChunkRec]:
    symbol_id = symbol_ids[d.index]
    if d.kind != "class" or d.end_byte - d.start_byte <= max_chars:
        # Rule 2 (fits, either kind) or rule 4 (oversized function/method, not a class):
        # both are just "one symbol_id, one byte span, split on lines if it doesn't fit".
        return _bounded_chunks(
            symbol_id, d.start_byte, d.end_byte, source, line_starts, max_chars, 0
        )
    return _chunk_large_class(
        d, children_of.get(d.index, []), symbol_ids, source, line_starts, max_chars
    )


def _chunk_large_class(
    d: DefFact,
    methods: list[DefFact],
    symbol_ids: dict[int, str],
    source: bytes,
    line_starts: list[int],
    max_chars: int,
) -> list[ChunkRec]:
    """Rule 3. `methods` is the DIRECT children of `d` (already sorted by start_byte by
    the caller). Header/gap pieces (before the first method, between two methods, after
    the last one) share ONE ord sequence under the class's own symbol_id; each method
    gets its own independent ord sequence under its own symbol_id."""
    class_id = symbol_ids[d.index]
    if not methods:
        return _split_lines(class_id, d.start_byte, d.end_byte, source, line_starts, max_chars, 0)

    chunks: list[ChunkRec] = []
    header_ord = 0
    cursor = d.start_byte
    for m in methods:
        # `m.start_byte` excludes ONLY m's own leading indentation on ITS line (tree-
        # sitter's node span starts at "def"/"class", not column 0) -- using it
        # directly as the gap's end would silently pull that indentation INTO the
        # gap's byte range as trailing filler, which then makes `_line_span`'s
        # end_line bleed onto the method's own start_line (a same-line "collision"
        # at the line-number level even though the byte ranges themselves never
        # overlap). Trim it back to the real end of gap content first.
        gap_end = _trim_trailing_indent(source, cursor, m.start_byte)
        if gap_end > cursor and source[cursor:gap_end].strip():
            gap_chunks = _bounded_chunks(
                class_id, cursor, gap_end, source, line_starts, max_chars, header_ord
            )
            chunks.extend(gap_chunks)
            header_ord += len(gap_chunks)
        method_id = symbol_ids[m.index]
        chunks.extend(
            _bounded_chunks(method_id, m.start_byte, m.end_byte, source, line_starts, max_chars, 0)
        )
        # Mirror image of the trim above: `m.end_byte` is tree-sitter's own node end,
        # right after the method's last real content byte -- NOT after its trailing
        # newline. Left as-is, that '\n' would become the NEXT gap's own first
        # included byte, landing ITS start_line back on the method's own last line
        # (the same same-line "collision", now from the other direction).
        cursor = _skip_leading_newline(source, m.end_byte, d.end_byte)
    if d.end_byte > cursor and source[cursor : d.end_byte].strip():
        chunks.extend(
            _bounded_chunks(
                class_id, cursor, d.end_byte, source, line_starts, max_chars, header_ord
            )
        )
    return chunks


def _bounded_chunks(
    symbol_id: str,
    start: int,
    end: int,
    source: bytes,
    line_starts: list[int],
    max_chars: int,
    ord_start: int,
) -> list[ChunkRec]:
    """One chunk if `[start, end)` already fits `max_chars`; otherwise rule 4's
    line-bounded split. Shared by the "fits" branch of rule 2/4 and by every
    header/gap/method piece of rule 3."""
    if end - start <= max_chars:
        return [_make_chunk(symbol_id, ord_start, start, end, source, line_starts)]
    return _split_lines(symbol_id, start, end, source, line_starts, max_chars, ord_start)


def _split_lines(
    symbol_id: str,
    start: int,
    end: int,
    source: bytes,
    line_starts: list[int],
    max_chars: int,
    ord_start: int,
) -> list[ChunkRec]:
    """Rule 4: greedy line-bounded split of `[start, end)` into pieces of at most
    `max_chars` bytes each. A line is added to the current (possibly empty) piece
    unconditionally if the piece is still empty -- this is what makes a single
    oversized line the one allowed violation of the size bound, emitted whole as its
    own piece -- otherwise only if doing so would keep the piece within `max_chars`;
    once it wouldn't, the current piece is closed and a new one started with that line."""
    chunks: list[ChunkRec] = []
    ord_ = ord_start
    piece_start: int | None = None
    piece_end = start
    for line_start, line_end in _line_ranges(source, start, end):
        if piece_start is None:
            piece_start, piece_end = line_start, line_end
            continue
        if line_end - piece_start > max_chars:
            chunks.append(_make_chunk(symbol_id, ord_, piece_start, piece_end, source, line_starts))
            ord_ += 1
            piece_start, piece_end = line_start, line_end
        else:
            piece_end = line_end
    if piece_start is not None:
        chunks.append(_make_chunk(symbol_id, ord_, piece_start, piece_end, source, line_starts))
    return chunks


def _trim_trailing_indent(source: bytes, start: int, end: int) -> int:
    """Backs `end` up past a trailing run of pure horizontal whitespace (space/tab)
    immediately before it, never crossing a newline or going below `start`. Used by
    `_chunk_large_class` when a gap's computed end boundary is really another
    (indented) DefFact's own `start_byte` -- e.g. the 4 spaces of `    def method_b`
    -- so that indentation (which belongs to THAT node's line, not the gap) doesn't
    get pulled into the gap chunk as trailing filler. See its call site for why this
    matters beyond cosmetics: left untrimmed, that whitespace would be the gap
    chunk's own "last included byte", and since it sits on the SAME physical line as
    the method's first byte, `_line_span` would report both chunks touching that
    line -- an apparent collision at line-number granularity despite the two chunks'
    byte ranges never actually overlapping."""
    e = end
    while e > start and source[e - 1] in (0x20, 0x09):
        e -= 1
    return e


def _skip_leading_newline(source: bytes, start: int, end: int) -> int:
    """Advances `start` forward past a single leading newline, if the byte right
    there is one (never past `end`). Mirrors `_trim_trailing_indent` from the other
    side: used by `_chunk_large_class` when a gap's computed start boundary is
    really the PRECEDING method's own `end_byte` -- tree-sitter places that right
    after the method's last real content byte, NOT after its trailing newline. Left
    unskipped, that '\\n' would be the gap chunk's own first included byte, on the
    SAME physical line as the method's own last byte -- the same line-number
    "collision" `_trim_trailing_indent` prevents, mirrored to the start side."""
    if start < end and source[start] == 0x0A:
        return start + 1
    return start


def _line_ranges(source: bytes, start: int, end: int) -> list[tuple[int, int]]:
    """Absolute byte ranges of each line within `[start, end)`, each including its
    trailing `\\n` (the last one won't have one if `end` isn't itself right after a
    newline -- e.g. end-of-file with no trailing newline, or a def span that -- like
    every DefFact -- ends exactly at its last real byte, never at a line boundary)."""
    ranges = []
    pos = start
    while pos < end:
        nl = source.find(b"\n", pos, end)
        line_end = nl + 1 if nl != -1 else end
        ranges.append((pos, line_end))
        pos = line_end
    return ranges


def _make_chunk(
    symbol_id: str,
    ord_: int,
    start: int,
    end: int,
    source: bytes,
    line_starts: list[int],
) -> ChunkRec:
    text = source[start:end].decode("utf-8", errors="replace")
    start_line, end_line = _line_span(line_starts, start, end)
    return ChunkRec(
        chunk_id=f"{symbol_id}#c{ord_}",
        symbol_id=symbol_id,
        ord=ord_,
        text=text,
        start_line=start_line,
        end_line=end_line,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


def _line_starts(source: bytes) -> list[int]:
    """0-based byte offset where each line begins (`_line_starts(...)[0] == 0` always).
    Used as a sorted array for `_line_at`'s bisect -- same technique as
    `core.spans.LineIndex`, but keyed purely on raw byte offsets (no SCIP-style
    line/column input to convert), so it's simpler to keep local here rather than
    reusing that class."""
    starts = [0]
    for i, b in enumerate(source):
        if b == 0x0A:
            starts.append(i + 1)
    return starts


def _line_at(line_starts: list[int], byte_offset: int) -> int:
    """1-based line number containing `byte_offset`."""
    return bisect.bisect_right(line_starts, byte_offset)


def _line_span(line_starts: list[int], start: int, end: int) -> tuple[int, int]:
    """1-based (start_line, end_line) of the byte range `[start, end)` -- end_line is
    the line of the LAST byte actually included (`end` itself is exclusive)."""
    end_incl = max(start, end - 1)
    return _line_at(line_starts, start), _line_at(line_starts, end_incl)
