"""Shared selector grammar (M3 T2): "<service>:<METHOD> <path>" (http-route form) or
"<service>:<dotted.qualified.name>" (qualified form) -- the CLI/MCP-facing way a human
names a graph entrypoint without knowing its raw node id.

Two independent resolvers consume this SAME grammar, against two DIFFERENT data
sources:
  - `linking.processes._resolve_entrypoint` (S7 staging-side, SQLite `Staging`) --
    used by `processes.materialize` (cfg.processes anchors) and its own
    `processes.resolve_selector` public wrapper, still relied on by the M2 gate
    (tests/eval/test_m2_gate.py) to resolve an entrypoint straight out of a staging.db
    without needing a live FalkorDB.
  - `query.api.GraphQuery.resolve_selector` (M3 T2, graph-side, live FalkorDB) --
    used by `cli.py`'s `trace` command so a `codegraph trace` invocation no longer
    needs `.codegraph/staging.db` to exist at all (M2 final review carry-item: trace
    used to hard-require a staging.db from a prior `codegraph index` run purely to
    resolve the selector string, even though the actual trace walk was always
    graph-only).

Parsing lives in exactly ONE place (here) so the grammar can't silently drift between
the two resolvers -- both import `parse_selector`/`RouteSelector`/`QualifiedSelector`
from this module rather than re-implementing the "does the first token after the
colon look like an HTTP verb" check independently.
"""

from __future__ import annotations

from dataclasses import dataclass

_HTTP_VERBS = frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"})


@dataclass(frozen=True)
class RouteSelector:
    """"<service>:<METHOD> <path>", e.g. "orders-api:POST /orders" -- resolves via a
    Channel(http_route) matching (owner_service=service, http_method=method,
    path_template=path) EXACTLY, then that channel's HANDLES edge."""

    service: str
    method: str
    path: str


@dataclass(frozen=True)
class QualifiedSelector:
    """"<service>:<dotted.qualified.name>", e.g.
    "kyc-worker:app.workflows.kyc.KycWorkflow" -- resolves via a direct
    (service, qualified_name) lookup, no channel/edge hop involved."""

    service: str
    qualified: str


Selector = RouteSelector | QualifiedSelector


def parse_selector(selector: str) -> Selector | None:
    """Splits on the FIRST ":" into (service, rest); `rest` is the route form iff it
    splits on its FIRST space into an uppercase HTTP verb + a (possibly empty)
    template -- e.g. "POST /orders" -- otherwise the whole of `rest` is a qualified
    name. No ":" at all (malformed selector, e.g. a bare string with no service
    prefix) -> None; callers treat that identically to "selector well-formed but
    resolves to nothing" (see both resolvers' own not-found handling)."""
    service, sep, rest = selector.partition(":")
    if not sep:
        return None
    verb, space, template = rest.partition(" ")
    if space and verb.upper() in _HTTP_VERBS:
        return RouteSelector(service=service, method=verb.upper(), path=template)
    return QualifiedSelector(service=service, qualified=rest)
