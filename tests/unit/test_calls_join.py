from codegraph.extractors.base import FileContext
from codegraph.extractors.calls import build_calls
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.facts import build_file_facts
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.resolvers.fallback import resolve_service
from codegraph.stores.staging import Staging

SRC = b"""def g():
    pass


def f():
    g()
    g()
    unknown_dyn()
    import os
    os.getpid()
"""

SYM_G = "scip-python python svc 0.1 `m`/g()."
SYM_F = "scip-python python svc 0.1 `m`/f()."
SYM_OS_GETPID = "scip-python python cpython 3.12 `os`/getpid()."


def _prepare(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    facts = build_file_facts("m.py", SRC)
    g_def = next(d for d in facts.defs if d.name == "g")
    f_def = next(d for d in facts.defs if d.name == "f")
    st.add_defs("svc", [
        DefRow("m.py", SYM_G, g_def.name_start_byte, g_def.name_end_byte, 1),
        DefRow("m.py", SYM_F, f_def.name_start_byte, f_def.name_end_byte, 5),
    ])
    g_calls = [c for c in facts.calls if c.callee_name == "g"]
    getpid_call = next(c for c in facts.calls if c.callee_name == "getpid")
    st.add_refs("svc", [
        RefRow("m.py", SYM_G, g_calls[0].callee_start_byte, g_calls[0].callee_end_byte, 6, 0),
        RefRow("m.py", SYM_G, g_calls[1].callee_start_byte, g_calls[1].callee_end_byte, 7, 0),
        RefRow("m.py", SYM_OS_GETPID, getpid_call.callee_start_byte,
               getpid_call.callee_end_byte, 10, 0),
    ])

    def lookup(rp, sb):
        return {g_def.name_start_byte: SYM_G, f_def.name_start_byte: SYM_F}.get(sb)

    return st, {"m.py": facts}, lookup


def test_join_aggregates_and_classifies(tmp_path):
    st, facts_by_file, lookup = _prepare(tmp_path)
    stats = build_calls("svc", st, facts_by_file, lookup, def_symbols=st.def_symbols("svc"))
    assert stats.calls_joined == 2       # два вызова g() слились в одно ребро
    assert stats.calls_external == 1     # os.getpid -- нет staged def (M5: def-existence)
    assert stats.calls_unresolved == 1   # unknown_dyn
    edges = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(edges) == 1
    e = edges[0]
    assert e.src == "sym:svc:`m`/f()." and e.dst == "sym:svc:`m`/g()."
    assert e.props["callsite_count"] == 2
    assert e.resolution == "static" and e.confidence == 1.0
    assert e.evidence_file == "m.py" and e.evidence_line == 6


def test_module_level_call_attributed_to_module(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def g():\n    pass\n\n\ng()\n"
    facts = build_file_facts("m.py", src)
    g_def = next(d for d in facts.defs if d.name == "g")
    call = facts.calls[0]
    # M5: a staged def for SYM_G is now required for the join (def-existence, not
    # package-tag, decides first-party) -- absent before this task's fix, since the
    # old package==service check needed no def lookup at all.
    st.add_defs("svc", [DefRow("m.py", SYM_G, g_def.name_start_byte, g_def.name_end_byte, 1)])
    st.add_refs("svc", [RefRow("m.py", SYM_G, call.callee_start_byte,
                               call.callee_end_byte, 5, 0)])
    build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None,
                def_symbols=st.def_symbols("svc"))
    e = next(e for e in st.iter_edges() if e.type == "CALLS")
    assert e.src == "sym:svc:`m`/"


def test_containment_fallback_when_exact_miss(tmp_path):
    st, facts_by_file, lookup = _prepare(tmp_path)
    # captured BEFORE the begin_service() below wipes scip_defs too (it clears
    # files/scip_defs/scip_refs/chunks/nodes/edges/claims, not just refs) -- def_symbols
    # is a plain materialized set by this point, unaffected by the later DB wipe.
    def_symbols = st.def_symbols("svc")
    # сдвинем все ref-спаны на -1 (утрируем расхождение конвертации на 1 байт)
    refs = st.refs_for_file("svc", "m.py")
    st.begin_service("svc")
    st.add_refs("svc", [RefRow(r.relpath, r.symbol, r.start_byte - 1,
                               r.end_byte, r.start_line, r.roles) for r in refs])
    stats = build_calls("svc", st, facts_by_file, lookup, def_symbols=def_symbols)
    assert stats.calls_joined >= 2  # containment всё ещё сшивает


def test_local_ref_without_local_def_is_unresolved(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def f():\n    ghost()\n"
    facts = build_file_facts("m.py", src)
    call = facts.calls[0]
    st.add_refs("svc", [RefRow("m.py", "local 5", call.callee_start_byte,
                               call.callee_end_byte, 2, 0)])  # def-а local 5 в файле НЕТ
    stats = build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None,
                        def_symbols=set(),
                        local_defs_for_file=lambda rp: st.local_def_symbols("svc", rp))
    assert stats.calls_unresolved == 1 and stats.calls_joined == 0


def test_local_ref_with_local_def_is_joined(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def f():\n    ghost()\n"
    facts = build_file_facts("m.py", src)
    call = facts.calls[0]
    st.add_defs("svc", [DefRow("m.py", "local 5", 0, 1, 1)])  # def-а local 5 в файле ЕСТЬ
    st.add_refs("svc", [RefRow("m.py", "local 5", call.callee_start_byte,
                               call.callee_end_byte, 2, 0)])
    stats = build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None,
                        def_symbols=set(),
                        local_defs_for_file=lambda rp: st.local_def_symbols("svc", rp))
    assert stats.calls_joined == 1 and stats.calls_unresolved == 0


# -- M5 Task 1 (pilot Bug B, docs/superpowers/reports/2026-07-18-m4-pilot.md §7.2):
# first-party is now decided by EXISTENCE OF A STAGED DEF for the callee symbol
# (`def_symbols`), not by `parsed.package == service`. `scip-python --project-name
# <service>` stamps package=<service> on EVERY symbol it fully resolves -- first-party
# AND third-party alike, as long as the service's own venv makes the callee resolvable
# at all -- so a bare package-tag comparison can no longer tell first-party from
# third-party once a real venv is in play. Measured on a real repo during the M4
# pilot: of 5345 staged CALLS edges, only 2916 (54.6%) had a valid dst at load; the
# other 2429 (45.4%) were third-party calls masquerading as first-party "joined"
# edges (94% of those pointing at obviously third-party prefixes -- sqlalchemy/
# pydantic/fastapi/blockkit/slack_sdk/etc), silently dropped at S9 load since their
# dst node is never staged (defs only ever exist for a service's own scanned files).


def test_resolved_call_with_service_tagged_package_but_no_staged_def_is_external(tmp_path):
    """Bug B repro: a non-local, fully-resolved symbol whose SCIP package equals the
    service name (exactly what --project-name stamps onto a resolved third-party
    call) but which has NO staged def anywhere in the service (nothing in this
    service's own source defines sqlalchemy.orm.session.Session.query) must classify
    as external/not-joined. The OLD package==service criterion would have wrongly
    joined it -- that silent misclassification is Bug B."""
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def f():\n    q.query()\n"
    facts = build_file_facts("m.py", src)
    call = facts.calls[0]
    masked_symbol = "scip-python python svc deadbeef `sqlalchemy.orm.session`/Session#query()."
    st.add_refs("svc", [RefRow("m.py", masked_symbol, call.callee_start_byte,
                               call.callee_end_byte, 2, 0)])
    # NB: no add_defs for masked_symbol -- it's genuinely third-party, never staged.
    stats = build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None, def_symbols=set())
    assert stats.calls_external == 1
    assert stats.calls_joined == 0
    assert list(st.iter_edges()) == []


def test_resolved_call_with_staged_def_still_joins_regardless_of_package(tmp_path):
    """Mirror of the above: a non-local, fully-resolved symbol whose package is
    something else entirely (not the service, not even a real package -- the package
    tag is no longer consulted at all) still joins as first-party as long as a def
    for it is staged. Def-existence is the WHOLE story now; package is irrelevant
    either way (not just when it happens to equal the service)."""
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def f():\n    g()\n"
    facts = build_file_facts("m.py", src)
    call = facts.calls[0]
    odd_package_symbol = "scip-python python totally-unrelated-package 9.9 `m`/g()."
    st.add_defs("svc", [DefRow("m.py", odd_package_symbol, 0, 1, 1)])
    st.add_refs("svc", [RefRow("m.py", odd_package_symbol, call.callee_start_byte,
                               call.callee_end_byte, 2, 0)])
    stats = build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None,
                        def_symbols=st.def_symbols("svc"))
    assert stats.calls_joined == 1
    assert stats.calls_external == 0


def test_degraded_fallback_path_all_refs_have_defs_join_unchanged(tmp_path):
    """(d) degraded fallback path (resolvers.fallback.resolve_service's structural
    symbols): every ref it emits targets a def it ALSO staged in the same call (see
    that module's own docstring -- refs only ever point at a top-level def already
    present in facts_by_file, built through the identical `_symbol` format) -- so
    switching build_calls' first-party criterion from package-tag to def-existence
    changes nothing on this path: every resolved ref already satisfies "has a staged
    def" by construction, exactly as it always satisfied "package == service"
    (fallback's own `_symbol` helper always stamps package=service too)."""
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def g():\n    pass\n\n\ndef f():\n    g()\n    unknown_dyn()\n"
    facts = build_file_facts("m.py", src)
    def_rows, ref_rows = resolve_service("svc", {"m.py": src}, {"m.py": facts})
    st.add_defs("svc", def_rows)
    st.add_refs("svc", ref_rows)
    stats = build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None,
                        def_symbols=st.def_symbols("svc"),
                        resolution="heuristic", confidence=0.6)
    assert stats.calls_joined == 1
    assert stats.calls_external == 0
    assert stats.calls_unresolved == 1  # unknown_dyn -- unresolved даже эвристикой
    edges = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(edges) == 1 and edges[0].resolution == "heuristic"


# ============================================================================
# M9 T4 (backlog M5-carry, progress.md ledger M5-T3 entry: "ревьюер live-walked
# CALLS-interplay [no dangling; dst→первая ветка]"): T1×T3 seam -- a CALLS edge
# resolving into an M5 T3 id-collision family (see test_python_core_extractor.py's
# own "M5 T3" section and extractors/python_core.py::extract's KNOWN LIMITATION
# comment on `def_ids`, also README "Ограничения") must land on the FIRST
# (unsuffixed) branch's node id and never dangle.
#
# Root cause, walked here end-to-end at the CALLS-join layer rather than just the
# node-emission layer those other two places already pin: build_calls' own dst
# computation (`symbol_to_node_id` applied directly to the matched ref's symbol,
# see calls.py) NEVER consults `ids.disambiguate` -- that ordinal-suffixing only
# ever happens inside python_core.extract()'s own per-file seen-set loop. So when
# scip's control-flow-insensitive symbol table resolves BOTH branches of a
# same-named if/else def to ONE symbol (mirrors
# test_colliding_def_ids_get_ordinal_suffix_scip_path's own scenario, one layer
# up), every caller's CALLS edge computes that SAME raw (pre-suffix) id as its
# dst -- which is, by construction, exactly the id extract() assigns to the FIRST
# occurrence, unsuffixed (occurrence 1 is never passed through disambiguate; only
# occurrence 2+ is) -- so the edge is guaranteed to land on a REAL staged node,
# never dangling, but always the first branch specifically (branch-2+ is
# CALLS-unreachable, per the documented limitation).
# ============================================================================

_COLLISION_CALLS_RELPATH = "app/flags.py"
_COLLISION_CALLS_SRC = b'''FLAG = "a"

if FLAG == "a":
    def handler():
        pass
else:
    def handler():
        pass


def caller():
    handler()
'''
_COLLISION_CALLS_SYM = "scip-python python svc 0.1 `app.flags`/handler()."


def test_calls_edge_into_id_collision_family_lands_on_first_branch_never_dangles(tmp_path):
    """Both if/else `handler` defs resolve, via def_symbol_lookup, to the SAME scip
    symbol (scip's own control-flow-insensitive table) -- python_core.extract()
    disambiguates them into TWO distinct node ids (unsuffixed for the 1st
    occurrence, `~2` for the 2nd, in file-appearance order), but a real caller
    elsewhere's CALLS edge must resolve its dst to the FIRST branch's id only, and
    that id must be a node extract() actually staged -- never a dangling
    reference."""
    facts = build_file_facts(_COLLISION_CALLS_RELPATH, _COLLISION_CALLS_SRC)
    handler_defs = [d for d in facts.defs if d.name == "handler"]
    assert len(handler_defs) == 2
    handler_spans = {d.name_start_byte for d in handler_defs}

    def def_symbol_lookup(rp, sb):
        return _COLLISION_CALLS_SYM if sb in handler_spans else None

    core_ctx = FileContext(
        service="svc", relpath=_COLLISION_CALLS_RELPATH, source=_COLLISION_CALLS_SRC,
        facts=facts, def_symbol_lookup=def_symbol_lookup, module_exists=lambda d: False,
    )
    core_res = extract_python_core(core_ctx)
    first_branch_id = "sym:svc:`app.flags`/handler()."
    second_branch_id = "sym:svc:`app.flags`/handler().~2"
    staged_ids = {n.id for n in core_res.nodes}
    # sanity: extract() really did stage BOTH (disambiguated) branches as real
    # nodes -- this test would be meaningless against a fixture that silently
    # dropped one of them (the exact M5 T3 bug this mechanism prevents).
    assert {first_branch_id, second_branch_id} <= staged_ids

    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    st.upsert_nodes(core_res.nodes)
    st.add_defs("svc", [
        DefRow(_COLLISION_CALLS_RELPATH, _COLLISION_CALLS_SYM,
               d.name_start_byte, d.name_end_byte, d.start_line)
        for d in handler_defs
    ])
    call = next(c for c in facts.calls if c.callee_name == "handler")
    st.add_refs("svc", [RefRow(
        _COLLISION_CALLS_RELPATH, _COLLISION_CALLS_SYM,
        call.callee_start_byte, call.callee_end_byte, call.start_line, 0,
    )])

    stats = build_calls(
        "svc", st, {_COLLISION_CALLS_RELPATH: facts}, def_symbol_lookup,
        def_symbols=st.def_symbols("svc"),
    )
    assert stats.calls_joined == 1
    edges = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(edges) == 1
    e = edges[0]
    assert e.src == "sym:svc:`app.flags`/caller()."
    assert e.dst == first_branch_id
    assert e.dst != second_branch_id
    # never dangling: the edge's dst is a node id that was ACTUALLY staged -- not
    # just a member of def_symbols (a symbol-string set); this checks the disjoint
    # NODE id-space extract() itself populated above.
    assert e.dst in staged_ids
