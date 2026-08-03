"""M10 T1 realstack leg (task-5; docs/superpowers/reports/2026-08-03-mcp-pilot.md
§5): module-level singleton method-call resolution -- mirrors the pilot's own
`registry = _DBRegistry(config.database.dsn)` pattern (a module-level INSTANCE
binding whose attribute access, from ANOTHER file, scip-python cannot trace back
to the class's own method through the module-level binding -- see
parsing/module_singletons.py's own module docstring for the full mechanism this
leg proves end-to-end against REAL scip-python; Task 1 unit-tested it
synthetically only, real-corpus verification is explicitly this task's job).

`store`, bound here at true module level (ctor-form RHS, `DocStore` capitalized),
is imported and called from app/routes/admin.py -- the exact dropped-CALLS shape
that was 79 of 149 (53%) on the pilot's real corpus, all this ONE pattern.

READ-FIRST FINDING (task-5, empirical, live scip-python 0.6.6 -- see
task-5-report.md's own "mechanism bugs found" section for the full writeup):
the pilot's OWN literal shape (a BARE, unannotated `name = ClassName(...)`,
same-file or cross-file, with or without an unresolved base class, with or
without an `@asynccontextmanager`-style method) does NOT reproduce the drop
against a minimal two-file fixture -- this scip-python version's real type
inference is good enough to trace a SIMPLE, self-contained class through the
module-level binding directly, landing on a properly callable-shaped ref
(`DocStore#persist().`) with no redirect needed at all (confirmed via a raw
scip occurrence dump, not guessed). The explicit `: Any` annotation below is
the one variation empirically confirmed (same raw-occurrence method) to
reproduce the pilot's OWN diagnosed dst shape: it suppresses scip's own
occurrence for the `.persist` callee token entirely (no ref at that byte
range at all -- `build_calls`' `ref is None` branch), while the CTOR
callee's OWN ref (`DocStore` on the line below) is COMPLETELY UNAFFECTED and
still resolves to the real class symbol (confirmed via the same dump) -- so
the static tier is still earned honestly. A real, common source of this exact
shape in production code: gradual-typing escape hatches / legacy `Any`
annotations on values a checker (or an author) couldn't or didn't fully type
-- functionally the same "scip cannot trace the binding through" outcome the
pilot observed, reached by a different, real, and honestly-documented route
rather than this fixture's own literal (harder to force cheaply) one."""


from typing import Any


class DocStore:
    """Persists finalized document records (fixture stub -- no real I/O; only the
    shape scip resolves against matters here, not the behavior)."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def persist(self, doc_uid: str, payload: dict) -> None:
        raise NotImplementedError


store: Any = DocStore("postgresql://worker-db/docs")
