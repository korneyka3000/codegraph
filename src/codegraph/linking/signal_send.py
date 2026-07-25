"""M8 T2 (rerun-2 R5, docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
links `temporal_signal_send` claims (temporal_ext.py's own typed-sender arg0
resolution -- `handle.signal(Cls.method, payload)` / a bare-name imported method,
resolved via ctx.ref_symbol_lookup to a METHOD SYMBOL node id, see that module's
own docstring for the full extraction-side design) onto the SAME temporal_signal
Channel a handler's own CONSUMES edge (M7 T4, `_extract_signal_kind_roles`) already
targets -- PRODUCES src -> that channel, static/1.0. A resolved symbol reference IS
the full ground truth here, the exact same argument INVOKES_ACTIVITY's own
static/1.0 already makes (a decorator match on the OTHER end is equally full ground
truth, mirrors fastapi_ext.py's own HANDLES edge) -- there is no weaker-confidence
tier for this path the way the sender's OLD string/const-literal path still carries
(heuristic/0.6, unchanged, untouched by this module).

ALGORITHM (`link`): scan every currently-staged CONSUMES edge whose dst is a
temporal_signal channel (`chan:temporal_signal:...`) into a `sym_id -> [chan_id,
...]` map (staged workspace-wide by temporal_ext's OWN S5 handler-side pass,
per-service -- see that module's docstring's "Handlers" section; still present at
this point in link_workspace, since clear_workspace_layer only ever wipes
extractor="linking" edges and this CONSUMES edge's own extractor is "temporal").
This module never touches a SCIP symbol STRING itself, only the already-
materialized node-id space -- temporal_ext.py resolved the claim's own
`method_symbol` to a real node id at EXTRACTION time (via ctx.ref_symbol_lookup +
resolvers.scip.symbols.symbol_to_node_id, the identical path INVOKES_ACTIVITY
uses for its own dst), so no `symbol_to_node_id` call belongs here at all.

For each `temporal_signal_send` claim: look up its own `method_symbol` (a node id,
despite the field's name -- see temporal_ext.py's own claim-shape comment) in that
map. Found (1+ channel(s)) -> PRODUCES(claim's own src_id -> each channel),
static/1.0, extractor="linking", props={"mechanism": "temporal_signal"} (prop-
parity with the sender's OWN old direct-emission heuristic/0.6 edge, so a graph
consumer filtering by mechanism sees the SAME tag regardless of which resolution
path produced the edge). A list, not a single id, because nothing structurally
forbids one method_symbol from carrying more than one CONSUMES edge into a
temporal_signal channel -- this module's own job is only to report what it finds,
never to silently pick one (see test_linking_signal_send.py's own belt-and-braces
pin). NOT found (the symbol resolved at extraction time, but no CONSUMES edge into
a temporal_signal channel exists for it -- a foreign scope, an ordinary non-handler
method, an out-of-workspace symbol) -> counter `signal_send_unlinked`, no edge --
the SAME "no guessing, ever" honesty rule every other linking module in this
codebase already follows (http_routes.py/router_prefix.py's own binding
constraint).

CROSS-SERVICE sends (sender in service A, handler in service B --
`get_external_workflow_handle(...).signal(Cls.m, ...)` legitimately crosses
services): the CONSUMES lookup above is workspace-wide staged state, not scoped to
any one service, so this needs no special-casing at all -- and the resulting
PRODUCES edge crosses services ONLY through the channel endpoint
(`chan:temporal_signal:...`), which `Staging.upsert_edges`'s own cross-service
invariant already exempts unconditionally (a chan:-prefixed endpoint on either side
skips the same-service check entirely, see that method's own docstring) -- the
exact same "channels are the legal bridge" property kafka/http_route channels
already rely on. Verified by this module's own cross-service test, no staging
change needed.

DEDUP: PRODUCES' real primary key is (src, dst, type, via_channel, origin_service)
-- see core/schema.py's SCHEMA_VERSION history "5 -> 6". Two DIFFERENT sender call
sites sharing the identical (src_id, method_symbol) pair (e.g. two
`.signal(WF.go, ...)` calls inside the SAME enclosing method) collapse onto ONE
edge row here (last-claim-processed evidence wins) -- a documented, ACCEPTED
evidence-choice, not a correctness bug: the EDGE (a PRODUCES relationship exists
between this method and this channel) is unaffected by which one of N identical
call sites' own evidence_line survives, mirroring temporal_ext.py's OWN
pre-existing "no cross-request dedup" property for its old direct-emission sender
path (see that module's own docstring). Two DIFFERENT sender methods targeting the
SAME handler (the realistic multi-caller case) each get their own row (different
src) -- never collapsed.

Wired into `linking.workspace.link_workspace`, right after `_apply_temporal_start_
marks` (the OTHER temporal-claim consumer -- grouped here purely for prose
locality; this step has no POSITIONAL dependency on router_prefix.link/
http_routes.link, a disjoint Channel kind built directly by temporal_ext in S5) and
BEFORE `segments.derive` (that stage's own exact-channel PRODUCES/CONSUMES pairing
rule needs THIS step's freshly-linked PRODUCES edges to exist -- the same "must run
before segments.derive" constraint router_prefix.link/http_routes.link already
document for their own edge types)."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"
_RESOLUTION = "static"
_CONFIDENCE = 1.0
_SIGNAL_CHANNEL_PREFIX = "chan:temporal_signal:"


def _signal_consumers(staging: Staging) -> dict[str, list[str]]:
    """sym-node-id -> [temporal_signal channel id, ...] it CONSUMES from -- built
    from every currently-staged CONSUMES edge into a temporal_signal channel
    (temporal_ext.py's OWN S5 handler-side emission, `_extract_signal_kind_roles`).
    A list, not a single id -- see module docstring."""
    consumers: dict[str, list[str]] = {}
    for e in staging.iter_edges():
        if e.type == "CONSUMES" and e.dst.startswith(_SIGNAL_CHANNEL_PREFIX):
            consumers.setdefault(e.src, []).append(e.dst)
    return consumers


def link(staging: Staging) -> dict:
    """S7 entry point (called from linking.workspace.link_workspace, see module
    docstring for exact placement). staging-only (no FalkorDB access). Returns
    {"signal_send_unlinked": <count>} -- the number of temporal_signal_send claims
    whose own method_symbol resolved (at EXTRACTION time, temporal_ext.py) but
    names no method with a live CONSUMES edge into a temporal_signal channel at
    LINK time -- see module docstring's honesty rule."""
    consumers = _signal_consumers(staging)
    edges: list[EdgeRec] = []
    unlinked = 0

    for claim in staging.claims_for("temporal_signal_send"):
        src_id = claim["src_id"]
        method_symbol = claim["method_symbol"]
        channel_ids = consumers.get(method_symbol)
        if not channel_ids:
            unlinked += 1
            continue
        for chan_id in channel_ids:
            edges.append(EdgeRec(
                src=src_id, dst=chan_id, type="PRODUCES",
                resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
                evidence_file=claim.get("_relpath"), evidence_line=claim.get("evidence_line"),
                props={"mechanism": "temporal_signal"},
            ))

    if edges:
        staging.upsert_edges(edges)

    return {"signal_send_unlinked": unlinked}
