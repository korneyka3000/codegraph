"""M3 T3: chunk_file rule coverage (splitter.py's module docstring, rules 1-6) +
cross-fixture invariant sweep (no byte/line overlap, full top-level-def coverage)."""

import hashlib
from pathlib import Path

import pytest

from codegraph.chunking.splitter import chunk_file
from codegraph.parsing.facts import build_file_facts

FIXTURES_ROOT = Path(__file__).parents[2] / "fixtures" / "services"


def _facts_and_symbol_ids(path: Path, service: str = "test"):
    relpath = str(path.relative_to(FIXTURES_ROOT))
    source = path.read_bytes()
    facts = build_file_facts(relpath, source)
    symbol_ids = {d.index: f"sym:{service}:{d.index}:{d.name}" for d in facts.defs}
    module_id = f"sym:{service}:module"
    return relpath, source, facts, symbol_ids, module_id


# -- real fixtures named in the brief --


def test_order_py_module_preamble_and_single_class_chunk():
    """order.py: OrderService fits under the default max_chars (2000) -- rule 2, one
    chunk for the whole class (methods included) -- plus one module-preamble chunk
    (the import block). Exactly 2 chunks total, per the brief."""
    path = FIXTURES_ROOT / "orders_api/app/services/order.py"
    relpath, source, facts, symbol_ids, module_id = _facts_and_symbol_ids(path)
    top_level = [d for d in facts.defs if d.parent is None]
    assert len(top_level) == 1
    cls = top_level[0]
    assert cls.kind == "class" and cls.name == "OrderService"
    assert cls.end_byte - cls.start_byte <= 2000

    chunks = chunk_file(relpath, source, facts, symbol_ids, module_id)
    assert len(chunks) == 2

    module_chunks = [c for c in chunks if c.symbol_id == module_id]
    assert len(module_chunks) == 1
    assert module_chunks[0].ord == 0
    assert module_chunks[0].chunk_id == f"{module_id}#c0"
    assert "import uuid" in module_chunks[0].text
    assert "class OrderService" not in module_chunks[0].text

    class_symbol = symbol_ids[cls.index]
    class_chunks = [c for c in chunks if c.symbol_id == class_symbol]
    assert len(class_chunks) == 1
    assert class_chunks[0].ord == 0
    assert class_chunks[0].start_line == cls.start_line
    assert class_chunks[0].end_line == cls.end_line
    assert "class OrderService" in class_chunks[0].text
    assert "async def place" in class_chunks[0].text
    assert "async def get" in class_chunks[0].text  # whole class, methods included

    # methods are NOT separately chunked -- the class fits within max_chars (rule 2)
    method_symbols = {symbol_ids[d.index] for d in facts.defs if d.parent == cls.index}
    assert method_symbols and not (method_symbols & {c.symbol_id for c in chunks})

    for c in chunks:
        assert c.content_hash == hashlib.sha256(c.text.encode("utf-8")).hexdigest()


def test_consumer_main_py_module_preamble_and_function_chunk():
    path = FIXTURES_ROOT / "kyc_worker/app/consumer_main.py"
    relpath, source, facts, symbol_ids, module_id = _facts_and_symbol_ids(path)
    top_level = [d for d in facts.defs if d.parent is None]
    assert len(top_level) == 1
    fn = top_level[0]
    assert fn.kind == "function" and fn.name == "run_consumer"

    chunks = chunk_file(relpath, source, facts, symbol_ids, module_id)
    assert len(chunks) == 2

    module_chunks = [c for c in chunks if c.symbol_id == module_id]
    assert len(module_chunks) == 1 and "import json" in module_chunks[0].text

    fn_chunks = [c for c in chunks if c.symbol_id == symbol_ids[fn.index]]
    assert len(fn_chunks) == 1 and fn_chunks[0].ord == 0
    assert "async def run_consumer" in fn_chunks[0].text


# -- rule 1: module preamble edge cases --


def test_file_without_defs_becomes_single_module_chunk():
    source = b"import os\nX = 1\n"
    facts = build_file_facts("consts.py", source)
    assert facts.defs == []
    chunks = chunk_file("consts.py", source, facts, {}, "sym:test:module")
    assert len(chunks) == 1
    c = chunks[0]
    assert c.symbol_id == "sym:test:module" and c.ord == 0
    assert c.chunk_id == "sym:test:module#c0"
    assert c.text == source.decode("utf-8")
    assert (c.start_line, c.end_line) == (1, 2)
    assert c.content_hash == hashlib.sha256(c.text.encode("utf-8")).hexdigest()


def test_empty_preamble_produces_no_module_chunk():
    """No import/docstring/const before the first (and only) top-level def -- the
    preamble slice is empty, so rule 1's non-empty-after-strip gate suppresses it."""
    source = b"def f():\n    return 1\n"
    facts = build_file_facts("f.py", source)
    d = facts.defs[0]
    symbol_ids = {d.index: "sym:test:f"}
    chunks = chunk_file("f.py", source, facts, symbol_ids, "sym:test:module")
    assert len(chunks) == 1
    assert chunks[0].symbol_id == "sym:test:f"


def test_whitespace_only_preamble_produces_no_module_chunk():
    source = b"\n\n   \ndef f():\n    return 1\n"
    facts = build_file_facts("f.py", source)
    d = facts.defs[0]
    symbol_ids = {d.index: "sym:test:f"}
    chunks = chunk_file("f.py", source, facts, symbol_ids, "sym:test:module")
    assert all(c.symbol_id != "sym:test:module" for c in chunks)


def test_size_exactly_max_chars_is_not_split():
    """Boundary: max_chars is measured with <=, not <."""
    source = b"def f():\n    return 1\n"
    facts = build_file_facts("f.py", source)
    d = facts.defs[0]
    exact = d.end_byte - d.start_byte  # whatever it measures out to, used verbatim
    symbol_ids = {d.index: "sym:test:f"}
    chunks = chunk_file("f.py", source, facts, symbol_ids, "sym:test:module", max_chars=exact)
    assert len(chunks) == 1 and chunks[0].ord == 0


# -- rule 4: oversized top-level function, line-bounded split --


def test_large_function_splits_on_line_boundaries_with_reconstruction_and_ord_sequence():
    body = "".join(f"    v{i:04d} = {i:04d}\n" for i in range(300))
    source_str = "def big():\n" + body + "    return None\n"
    source = source_str.encode("utf-8")
    facts = build_file_facts("big.py", source)
    assert len(facts.defs) == 1
    d = facts.defs[0]
    assert d.end_byte - d.start_byte > 2000  # comfortably exceeds the default max_chars

    symbol_ids = {d.index: "sym:test:big"}
    chunks = chunk_file("big.py", source, facts, symbol_ids, "sym:test:module")

    assert all(c.symbol_id == "sym:test:big" for c in chunks)
    assert len(chunks) > 1

    chunks_sorted = sorted(chunks, key=lambda c: c.ord)
    assert [c.ord for c in chunks_sorted] == list(range(len(chunks_sorted)))
    assert [c.chunk_id for c in chunks_sorted] == [
        f"sym:test:big#c{i}" for i in range(len(chunks_sorted))
    ]
    for c in chunks_sorted:
        assert len(c.text.encode("utf-8")) <= 2000

    # consecutive pieces are exactly line-adjacent -- no gap, no overlap, no mid-line cut
    assert chunks_sorted[0].start_line == d.start_line
    assert chunks_sorted[-1].end_line == d.end_line
    # intentionally uneven zip (pairwise-adjacent scan) -- strict=False, not True
    for prev, nxt in zip(chunks_sorted, chunks_sorted[1:], strict=False):
        assert nxt.start_line == prev.end_line + 1

    expected_text = source[d.start_byte : d.end_byte].decode("utf-8")
    assert "".join(c.text for c in chunks_sorted) == expected_text


def test_single_oversized_line_is_emitted_whole_as_its_own_piece():
    huge_line = '    x = "' + ("A" * 3000) + '"\n'
    source_str = "def f():\n    a = 1\n" + huge_line + "    b = 2\n"
    source = source_str.encode("utf-8")
    facts = build_file_facts("f.py", source)
    d = facts.defs[0]
    symbol_ids = {d.index: "sym:test:f"}
    chunks = sorted(
        chunk_file("f.py", source, facts, symbol_ids, "sym:test:module"),
        key=lambda c: c.ord,
    )

    oversized = [c for c in chunks if len(c.text.encode("utf-8")) > 2000]
    assert len(oversized) == 1
    huge = oversized[0]
    assert huge.start_line == huge.end_line  # exactly the one line, not cut mid-line
    assert "A" * 3000 in huge.text

    for c in chunks:
        if c is not huge:
            assert len(c.text.encode("utf-8")) <= 2000

    expected_text = source[d.start_byte : d.end_byte].decode("utf-8")
    assert "".join(c.text for c in chunks) == expected_text


# -- rule 3: oversized top-level class --


def test_large_class_splits_header_methods_and_glues_nonwhitespace_gaps():
    max_chars = 300
    header = (
        "class Big:\n"
        '    """Header docstring padding so the header alone is a nontrivial chunk."""\n'
    )

    def method_src(name: str) -> str:
        return (
            f"    def {name}(self):\n        value = 1\n        value += 2\n        return value\n"
        )

    gap = '    BETWEEN = "gap between method_a and method_b"\n'
    tail = '    TAIL = "gap after the last method"\n'
    class_body = header + method_src("method_a") + gap + method_src("method_b") + tail

    # every individual piece fits max_chars on its own; only the WHOLE class doesn't --
    # that's what forces rule 3's header/method split instead of a plain rule-4 line-split.
    assert len(header.encode()) <= max_chars
    assert len(method_src("method_a").encode()) <= max_chars
    assert len(gap.encode()) <= max_chars
    assert len(tail.encode()) <= max_chars
    assert len(class_body.encode()) > max_chars

    source = class_body.encode("utf-8")
    facts = build_file_facts("big.py", source)
    d = next(x for x in facts.defs if x.parent is None)
    assert d.kind == "class"
    method_a = next(x for x in facts.defs if x.parent == d.index and x.name == "method_a")
    method_b = next(x for x in facts.defs if x.parent == d.index and x.name == "method_b")

    symbol_ids = {
        d.index: "sym:test:Big",
        method_a.index: "sym:test:Big.method_a",
        method_b.index: "sym:test:Big.method_b",
    }
    chunks = chunk_file("big.py", source, facts, symbol_ids, "sym:test:module", max_chars=max_chars)

    by_symbol: dict[str, list] = {}
    for c in chunks:
        by_symbol.setdefault(c.symbol_id, []).append(c)

    class_chunks = sorted(by_symbol["sym:test:Big"], key=lambda c: c.ord)
    assert [c.ord for c in class_chunks] == [0, 1, 2]
    assert "class Big" in class_chunks[0].text and "Header docstring" in class_chunks[0].text
    assert "BETWEEN" in class_chunks[1].text
    assert "TAIL" in class_chunks[2].text

    assert len(by_symbol["sym:test:Big.method_a"]) == 1
    assert by_symbol["sym:test:Big.method_a"][0].ord == 0
    assert "value += 2" in by_symbol["sym:test:Big.method_a"][0].text

    assert len(by_symbol["sym:test:Big.method_b"]) == 1
    assert by_symbol["sym:test:Big.method_b"][0].ord == 0

    # no two chunks (any symbol) claim the same source line
    seen: set[int] = set()
    for c in chunks:
        rng = range(c.start_line, c.end_line + 1)
        assert seen.isdisjoint(rng), c.chunk_id
        seen.update(rng)


def test_large_class_with_no_direct_methods_falls_back_to_line_split():
    """A class with no direct def/class children at all has no method boundary to
    split on -- degrades to rule 4's plain line-split under the class's own symbol_id."""
    max_chars = 200
    lines = "".join(f"    ATTR{i} = {i}\n" for i in range(30))
    source_str = "class NoMethods:\n" + lines
    source = source_str.encode("utf-8")
    facts = build_file_facts("nomethods.py", source)
    d = facts.defs[0]
    assert d.end_byte - d.start_byte > max_chars

    symbol_ids = {d.index: "sym:test:NoMethods"}
    chunks = sorted(
        chunk_file(
            "nomethods.py", source, facts, symbol_ids, "sym:test:module", max_chars=max_chars
        ),
        key=lambda c: c.ord,
    )
    assert len(chunks) > 1
    assert all(c.symbol_id == "sym:test:NoMethods" for c in chunks)
    assert [c.ord for c in chunks] == list(range(len(chunks)))

    expected_text = source[d.start_byte : d.end_byte].decode("utf-8")
    assert "".join(c.text for c in chunks) == expected_text


def test_method_inside_large_class_that_is_itself_oversized_also_splits_on_lines():
    """Rule 3 explicitly calls out this combination: a method of an oversized class
    that is ITSELF bigger than max_chars gets rule 4's line-split too (ord 0..N under
    the method's own symbol_id), independent of the class header's own ord sequence."""
    max_chars = 150
    header = "class Big:\n"
    small_method = "    def small(self):\n        return 1\n"
    big_method_lines = "".join(f"        v{i} = {i}\n" for i in range(20))
    big_method = f"    def big(self):\n{big_method_lines}        return None\n"
    source_str = header + small_method + big_method
    source = source_str.encode("utf-8")

    assert len(small_method.encode()) <= max_chars
    assert len(big_method.encode()) > max_chars

    facts = build_file_facts("bigmethod.py", source)
    d = next(x for x in facts.defs if x.parent is None)
    small = next(x for x in facts.defs if x.parent == d.index and x.name == "small")
    big = next(x for x in facts.defs if x.parent == d.index and x.name == "big")

    symbol_ids = {
        d.index: "sym:test:Big",
        small.index: "sym:test:Big.small",
        big.index: "sym:test:Big.big",
    }
    chunks = chunk_file(
        "bigmethod.py", source, facts, symbol_ids, "sym:test:module", max_chars=max_chars
    )

    small_chunks = [c for c in chunks if c.symbol_id == "sym:test:Big.small"]
    assert len(small_chunks) == 1 and small_chunks[0].ord == 0

    big_chunks = sorted(
        (c for c in chunks if c.symbol_id == "sym:test:Big.big"), key=lambda c: c.ord
    )
    assert len(big_chunks) > 1
    assert [c.ord for c in big_chunks] == list(range(len(big_chunks)))
    assert [c.chunk_id for c in big_chunks] == [
        f"sym:test:Big.big#c{i}" for i in range(len(big_chunks))
    ]
    for c in big_chunks:
        assert len(c.text.encode("utf-8")) <= max_chars
    expected_big_text = source[big.start_byte : big.end_byte].decode("utf-8")
    assert "".join(c.text for c in big_chunks) == expected_big_text

    # class header (ord 0, its own family) still present and unaffected
    header_chunks = [c for c in chunks if c.symbol_id == "sym:test:Big"]
    assert len(header_chunks) == 1 and "class Big" in header_chunks[0].text


# -- rule 5: nested defs are never chunked separately --


def test_nested_function_not_chunked_separately():
    source_str = "def outer():\n    def inner():\n        return 1\n    return inner()\n"
    source = source_str.encode("utf-8")
    facts = build_file_facts("nested.py", source)
    outer = next(x for x in facts.defs if x.parent is None)
    inner = next(x for x in facts.defs if x.parent == outer.index)
    symbol_ids = {outer.index: "sym:test:outer", inner.index: "sym:test:inner"}

    chunks = chunk_file("nested.py", source, facts, symbol_ids, "sym:test:module")
    assert len(chunks) == 1
    assert chunks[0].symbol_id == "sym:test:outer"
    assert "def inner" in chunks[0].text and "return inner()" in chunks[0].text
    assert "sym:test:inner" not in {c.symbol_id for c in chunks}


def test_grandchild_def_inside_large_class_method_not_chunked_separately():
    """Rule 3 pulls out DIRECT methods of an oversized class only -- a def nested a
    level deeper (a helper closure inside one of those methods) stays part of the
    method's own chunk, exactly like rule 5's general nested-def treatment."""
    max_chars = 200
    source_str = (
        "class Big:\n"
        "    def method_a(self):\n"
        "        def helper():\n"
        "            return 1\n"
        "        return helper()\n"
        "    def method_b(self):\n"
        + "".join(f"        pad{i} = {i}\n" for i in range(20))
        + "        return None\n"
    )
    source = source_str.encode("utf-8")
    facts = build_file_facts("big2.py", source)
    d = next(x for x in facts.defs if x.parent is None)
    assert d.end_byte - d.start_byte > max_chars
    method_a = next(x for x in facts.defs if x.parent == d.index and x.name == "method_a")
    method_b = next(x for x in facts.defs if x.parent == d.index and x.name == "method_b")
    helper = next(x for x in facts.defs if x.parent == method_a.index)

    symbol_ids = {
        d.index: "sym:test:Big",
        method_a.index: "sym:test:Big.method_a",
        method_b.index: "sym:test:Big.method_b",
        helper.index: "sym:test:helper",
    }
    chunks = chunk_file(
        "big2.py", source, facts, symbol_ids, "sym:test:module", max_chars=max_chars
    )
    assert "sym:test:helper" not in {c.symbol_id for c in chunks}
    method_a_chunks = [c for c in chunks if c.symbol_id == "sym:test:Big.method_a"]
    assert len(method_a_chunks) == 1
    assert "def helper" in method_a_chunks[0].text


# -- rule 6 / decode resilience --


def test_malformed_utf8_in_source_does_not_raise():
    source = b"# bad bytes: \xff\xfe end\ndef f():\n    return 1\n"
    facts = build_file_facts("bad.py", source)
    assert len(facts.defs) == 1  # tree-sitter still finds the def past the bad comment
    d = facts.defs[0]
    symbol_ids = {d.index: "sym:test:f"}

    chunks = chunk_file("bad.py", source, facts, symbol_ids, "sym:test:module")
    assert len(chunks) == 2
    module_chunk = next(c for c in chunks if c.symbol_id == "sym:test:module")
    assert "�" in module_chunk.text


# -- cross-fixture property sweep: non-overlap + top-level coverage on all 29 files --


def _all_fixture_files() -> list[Path]:
    return sorted(FIXTURES_ROOT.rglob("*.py"))


@pytest.mark.parametrize(
    "path", _all_fixture_files(), ids=lambda p: str(p.relative_to(FIXTURES_ROOT))
)
def test_fixture_chunks_do_not_overlap_and_cover_every_top_level_def(path):
    relpath, source, facts, symbol_ids, module_id = _facts_and_symbol_ids(path)
    chunks = chunk_file(relpath, source, facts, symbol_ids, module_id)

    # chunk_id / content_hash / ord sanity on every chunk
    chunk_ids = [c.chunk_id for c in chunks]
    assert len(chunk_ids) == len(set(chunk_ids)), path
    for c in chunks:
        assert c.chunk_id == f"{c.symbol_id}#c{c.ord}", (path, c)
        assert c.content_hash == hashlib.sha256(c.text.encode("utf-8")).hexdigest(), (path, c)
        assert c.start_line <= c.end_line, (path, c)

    # per-symbol ord sequence is exactly 0..N-1
    by_symbol: dict[str, list[int]] = {}
    for c in chunks:
        by_symbol.setdefault(c.symbol_id, []).append(c.ord)
    for sym, ords in by_symbol.items():
        assert sorted(ords) == list(range(len(ords))), (path, sym, ords)

    # no two chunks (any symbol) claim the same source line
    line_owner: dict[int, str] = {}
    for c in chunks:
        for line in range(c.start_line, c.end_line + 1):
            assert line not in line_owner, (path, line, line_owner[line], c.chunk_id)
            line_owner[line] = c.symbol_id

    # every top-level def's own line range is covered by its symbol family (itself, or
    # -- oversized class -- itself + its direct methods), except whitespace-only lines
    src_lines = source.decode("utf-8", errors="replace").splitlines()
    for d in facts.defs:
        if d.parent is not None:
            continue
        family = {symbol_ids[d.index]}
        if d.kind == "class":
            family |= {symbol_ids[m.index] for m in facts.defs if m.parent == d.index}
        for line in range(d.start_line, d.end_line + 1):
            owner = line_owner.get(line)
            if owner is None:
                text = src_lines[line - 1] if 1 <= line <= len(src_lines) else ""
                assert text.strip() == "", (path, d.name, line, "unclaimed non-blank line")
            else:
                assert owner in family, (path, d.name, line, owner, family)
