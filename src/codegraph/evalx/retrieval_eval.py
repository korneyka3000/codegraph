"""M3 T8 eval: hit@k over `query.retrieval.search_code`'s hybrid mode against
`fixtures/golden/questions.yaml`-shaped golden questions. Mirrors M1/M2's evalx
modules (`calls_eval.py`/`edges_eval.py`) in spirit -- pure functions over plain
data, no Cypher/store dependency of its own -- but the input this time is a live
search function, not a `Staging` snapshot: retrieval quality is a property of the
whole stack (graph + real embedder + FalkorDB's fulltext/vector indexes), not
something a staging-only comparison could meaningfully approximate the way
`calls_eval`/`edges_eval` compare staged EDGES against golden structurally.

`run_questions` takes a plain `search_fn(query: str, k: int) -> dict` callable --
NOT a `GraphQuery` instance directly -- so it stays testable with a bare fake (no
store/embedder/model needed, see tests/unit/test_retrieval_eval.py) while the real
caller (tests/eval/test_m3_gate.py's gate, cli.py's `eval retrieval` command) passes
`lambda q, k: gq.search_code(q, k=k, mode="hybrid", exact=exact)` (`exact` -- M5 T2:
cli.py's `--exact` flag, routing the vector leg through the deterministic full-scan
store method for CI-reproducible hit@k; False by default, which keeps ANN -- the m3
gate's own closure omits it entirely): hybrid -- text (fulltext) +
vector (real embedder) fused via RRF -- is the intended real-world usage of this
eval (graph+embedder together, exactly as an agent's own `search_code` MCP calls
work), not baked into this module as a hardcoded dependency on `GraphQuery` itself.
`search_fn`'s return is trusted to be `search_code`-shaped: `{"items": [...]}` on
success (each item carrying `symbol_id`/`qualified_name`/`service`/`score`, see
`query.retrieval._chunk_item`) or `{"error": ...}` (e.g. mode="vector" with no
usable embedder) -- a missing `"items"` key degrades to zero items rather than
raising, so one degraded/errored question never aborts the whole batch.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import yaml


def load_questions(path: Path) -> list[dict]:
    """Golden questions from a YAML file shaped like `fixtures/golden/questions.yaml`:
    `{version: 1, questions: [{question, accept: [{service, symbol}], k}, ...]}`.
    Returns the raw `questions` list of dicts as-is (no dataclass wrapping -- every
    consumer here, `run_questions` and `cli.py`'s `eval retrieval`, only ever needs
    plain dict/key access, and staying on plain dicts keeps this module free of a
    schema class that would just duplicate the YAML shape). An empty/missing file
    (`yaml.safe_load` returning `None`) -> `[]`, same "no golden -> no work" convention
    `edges_eval.load_golden_edges` already uses for its own `data.get(..., []) or {}`
    guard."""
    data = yaml.safe_load(path.read_text()) or {}
    return data.get("questions", [])


def run_questions(
    search_fn: Callable[[str, int], dict],
    questions: list[dict],
) -> list[dict]:
    """One `search_fn(question["question"], question["k"])` call per question;
    returns `[{question, hit, rank, top}, ...]`, same order as `questions`.

    `top`: every returned item (defensively capped to this question's own `k`, in
    case `search_fn` ever hands back more than asked -- keeps "top" a true top-k
    regardless of what the callable underneath actually does), reduced to
    `{symbol_id, qualified_name, score}` -- `symbol_id` is ALWAYS included, even
    when `qualified_name` is `None` (a chunk whose owning symbol wasn't in staged
    nodes, see `query.retrieval._chunk_item`'s own docstring for when that
    happens): a human reading a miss's diagnostic `top` list can still see WHAT was
    actually retrieved instead of a row of bare `None`s.

    `hit`/`rank`: `accept` (`question["accept"]`, `[{service, symbol}, ...]`) is an
    OR-set of `(service, symbol)` pairs -- `hit` is `True` iff ANY top-k item counts
    as a hit for ANY accept pair; `rank` is the 0-based index of the FIRST such item,
    `None` if `hit` is `False`.

    R7 (docs/superpowers/reports/2026-08-05-pilot-rerun-7-open-gaps.md): an item
    counts as a hit for accept pair `(service, symbol)` iff `item.service == service`
    AND ANY of --
      1. `item.qualified_name == symbol` -- original exact match (pre-R7 behaviour,
         unchanged); the ONLY rule that ever applies when `item.chunk_kind` is
         missing/`None` (pre-M10 graph, no `chunk_kind` denormalized onto the Chunk
         node yet) or `"Module"` (a whole-file chunk crediting ANY accept anywhere in
         that file would be far too weak a match -- deliberately not bridged);
      2. `item.chunk_kind == "Class"` and `symbol` is a dotted-prefix child of
         `item.qualified_name` (`symbol.startswith(item.qualified_name + ".")`) --
         M12's symbol-aggregation (see `query.retrieval._aggregate_by_symbol`)
         collapses a "fat" class's sibling chunks -- including its own separately
         chunked methods, `chunking/splitter.py` rule 3 -- into ONE class-
         representative item, so that item stands in for an accept naming one of
         ITS OWN methods too;
      3. `item.chunk_kind == "Function"` and `item.qualified_name` is a dotted-prefix
         child of `symbol` (`item.qualified_name.startswith(symbol + ".")`) -- the
         mirror case: a method-chunk item credits an accept naming its OWN enclosing
         class.
    The mandatory `"."` boundary in 2/3 is load-bearing: `Cls` must NOT match an item
    qualified_name of `ClsOther.method` (a bare, non-dotted prefix check would
    false-positive on an unrelated, merely similarly-named class) -- and `service`
    must match regardless of which rule fires. `enclosing_symbol`
    (`query.retrieval._chunk_item`) is deliberately NOT used here -- it is
    denormalized to the SAME value as `qualified_name` (see that function's own M10
    comment), so keying off it would be a no-op; dotted-prefix + `chunk_kind` is the
    real signal. `qualified_name is None` (owning symbol wasn't in staged nodes) can
    never satisfy any rule, same as before R7.

    A `search_fn` result with no `"items"` key (e.g. `{"error": ...}`, mode="vector"
    with no usable embedder -- see `query.retrieval.search_code`'s own docstring)
    degrades to zero items for that question (`hit=False`, `rank=None`, `top=[]`),
    not a `KeyError` -- one degraded/errored question shouldn't crash the whole
    batch any more than one broken row would in `calls_eval`/`edges_eval`."""

    def accepts(item: dict, service: str, symbol: str) -> bool:
        """R7 hit predicate for one (item, accept-pair) -- see this function's own
        docstring above for the full rule table; kept as a nested closure (rather
        than a module-level helper) since it exists purely to serve this loop."""
        qn = item.get("qualified_name")
        if qn is None or item.get("service") != service:
            return False
        if qn == symbol:
            return True
        kind = item.get("chunk_kind")
        if kind == "Class":
            return symbol.startswith(qn + ".")
        if kind == "Function":
            return qn.startswith(symbol + ".")
        return False

    results: list[dict] = []
    for q in questions:
        k = q["k"]
        raw = search_fn(q["question"], k)
        items = (raw.get("items") or [])[:k]
        accept = {(a["service"], a["symbol"]) for a in q["accept"]}

        top = [
            {
                "symbol_id": it.get("symbol_id"),
                "qualified_name": it.get("qualified_name"),
                "score": it.get("score"),
            }
            for it in items
        ]

        hit = False
        rank: int | None = None
        for i, it in enumerate(items):
            if any(accepts(it, service, symbol) for service, symbol in accept):
                hit = True
                rank = i
                break

        results.append({"question": q["question"], "hit": hit, "rank": rank, "top": top})
    return results
