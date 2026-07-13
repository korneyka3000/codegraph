from codegraph.parsing.facts import build_file_facts
from codegraph.resolvers.fallback import resolve_service

A = b"""from b import helper


def local():
    pass


def caller():
    local()
    helper()
    mystery()
"""
B = b"""def helper():
    pass
"""


def test_fallback_defs_and_refs():
    files = {"a.py": A, "b.py": B}
    facts = {rp: build_file_facts(rp, src) for rp, src in files.items()}
    defs, refs = resolve_service("svc", files, facts)
    def_syms = {d.symbol for d in defs}
    assert any("`a`/caller()." in s for s in def_syms)
    assert any("`b`/helper()." in s for s in def_syms)
    ref_syms = [r.symbol for r in refs]
    assert any("`a`/local()." in s for s in ref_syms)      # same-file
    assert any("`b`/helper()." in s for s in ref_syms)     # via from-import
    assert not any("mystery" in s for s in ref_syms)       # нерезолвлено — пропущено
    helper_ref = next(r for r in refs if "`b`/helper()." in r.symbol)
    assert A[helper_ref.start_byte:helper_ref.end_byte] == b"helper"
