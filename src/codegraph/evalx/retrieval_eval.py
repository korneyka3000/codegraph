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
    OR-set of `(service, qualified_name)` pairs -- `hit` is `True` iff ANY top-k
    item's `(service, qualified_name)` is in that set (an item with `qualified_name
    is None` can never match -- `accept` only ever names real qualified names, so
    this falls out of plain tuple equality with no special-casing needed); `rank`
    is the 0-based index of the FIRST such matching item, `None` if `hit` is
    `False`.

    A `search_fn` result with no `"items"` key (e.g. `{"error": ...}`, mode="vector"
    with no usable embedder -- see `query.retrieval.search_code`'s own docstring)
    degrades to zero items for that question (`hit=False`, `rank=None`, `top=[]`),
    not a `KeyError` -- one degraded/errored question shouldn't crash the whole
    batch any more than one broken row would in `calls_eval`/`edges_eval`."""
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
            if (it.get("service"), it.get("qualified_name")) in accept:
                hit = True
                rank = i
                break

        results.append({"question": q["question"], "hit": hit, "rank": rank, "top": top})
    return results
