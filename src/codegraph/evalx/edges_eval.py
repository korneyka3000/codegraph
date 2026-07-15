"""M2 eval: generalized typed-edge staging vs golden comparison (fixtures/golden/
edges.yaml), for any edge type carrying golden labels -- HANDLES, DEPENDS_ON,
PRODUCES, CONSUMES, INVOKES_ACTIVITY, CALLS_HTTP (and, generically, CALLS too, though
the M2 gate itself keeps CALLS on M1's calls_eval -- see below). Generalizes M1's
calls_eval (CALLS-only, fixed 4-tuple shape) to any golden edge type; calls_eval.py
itself is UNTOUCHED (M1 gate imports it as-is and stays green) -- this is a new,
separate module, not a rewrite.

Two dst shapes, mirroring fixtures/golden/edges.yaml's own `dst: symbol XOR channel`
policy:
  - dst.symbol (CALLS, DEPENDS_ON, INVOKES_ACTIVITY -- both ends are code): 5-tuple
    (type, src_service, src_qualified, dst_service, dst_qualified).
  - dst.channel (PRODUCES, CONSUMES, CALLS_HTTP, HANDLES -- one end is a channel):
    4-tuple (type, src_service, src_qualified, dst_channel_id).

HANDLES is the one type whose STAGED direction is reversed relative to golden: golden
records it "code(handler) -- channel" (src=handler, dst=channel -- see edges.yaml's own
policy comment), which already falls naturally into the dst.channel 4-tuple shape above
via load_golden_edges' generic branching (no special-casing needed there). But the
STAGED graph direction is Channel -HANDLES-> RouteHandler (src=channel, dst=handler --
fastapi_ext.py's own convention, "код -- канал" inverted), so found_edges normalizes it
explicitly: swap ends so the comparable tuple is (HANDLES, handler_service,
handler_qualified, chan_id) -- the SAME shape golden already produces, symmetric with
every other channel-dst type.

mechanism-tagged golden records (e.g. `handle_order_created -> KycWorkflow.run` via
Temporal, mechanism: temporal_start) are excluded ONLY when type == "CALLS" -- mirrors
calls_eval.load_golden_calls exactly (mechanism is a CALLS-specific annotation in
today's golden; scoping the filter to CALLS, rather than applying it unconditionally to
any type, keeps this function honest about what it actually knows rather than
guessing at a policy for hypothetical future mechanism-tagged non-CALLS records).

precision_recall is REUSED from calls_eval (re-exported here, not reimplemented): its
body is pure set arithmetic over arbitrary hashable tuples -- nothing about it is
CALLS-specific or fixed-arity, so forking it here would just be duplication with two
places to keep in sync. A single source of truth stays in calls_eval.py."""

from __future__ import annotations

from pathlib import Path

import yaml

from codegraph.evalx.calls_eval import precision_recall
from codegraph.stores.staging import Staging

__all__ = ["found_edges", "load_golden_edges", "precision_recall"]

# (type, src_service, src_qualified, dst_service, dst_qualified) -- both ends code.
SymEdgeTuple = tuple[str, str, str, str, str]
# (type, src_service, src_qualified, dst_channel_id) -- dst (or, for HANDLES, the
# normalized "consumer side") is a channel.
ChanEdgeTuple = tuple[str, str, str, str]
EdgeTuple = SymEdgeTuple | ChanEdgeTuple

_CHANNEL_PREFIX = "chan:"


def load_golden_edges(path: Path, types: set[str]) -> set[EdgeTuple]:
    """Golden typed edges from fixtures/golden/edges.yaml, restricted to `types`.

    Per-record shape follows the record's own `dst` key (see module docstring):
    `dst.symbol` -> 5-tuple, `dst.channel` -> 4-tuple. `types` is a hard filter --
    a record whose `type` isn't in it is skipped before any other check, so calling
    this with `types={"CALLS"}` reproduces calls_eval.load_golden_calls' own record
    selection exactly (mechanism-filter included), just returning CALLS' native
    4-tuple shape (src_service, src_qualified, dst_service, dst_qualified) instead of
    a distinct CallTuple type alias -- same values, same set semantics.
    """
    data = yaml.safe_load(path.read_text()) or {}
    out: set[EdgeTuple] = set()
    for e in data.get("edges", []):
        edge_type = e.get("type")
        if edge_type not in types:
            continue
        if edge_type == "CALLS" and "mechanism" in e:
            continue
        src = e["src"]
        dst = e["dst"]
        if "channel" in dst:
            out.add((edge_type, src["service"], src["symbol"], dst["channel"]))
        else:
            out.add((edge_type, src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def found_edges(staging: Staging, types: set[str]) -> tuple[set[EdgeTuple], int]:
    """Staged typed edges restricted to `types`, normalized to the same tuple shapes
    `load_golden_edges` produces.

    sym-ends resolve through an in-memory id -> (service, qualified_name) join over
    staging.iter_nodes() (mirrors calls_eval.found_calls' own JOIN -- Staging exposes
    no raw SQL join, so it's done here in Python); a sym-end that fails to resolve (id
    missing from nodes, or present with an empty qualified_name) increments `dangling`
    and drops the whole edge, same convention as found_calls.

    chan-ends are used AS THE RAW ID, never looked up -- a channel node id is already
    the comparable value (golden stores channel ids as opaque strings too), and
    requiring a matching Channel NodeRec to exist would be an unrelated, stricter
    check this function has no reason to make (see test_found_edges_channel_dst_
    uses_id_directly_without_node_join). Consequently a chan-end can never contribute
    to `dangling` -- only sym-ends can.

    HANDLES is special-cased BEFORE the generic dst-is-channel check: its staged
    direction is src=channel, dst=handler (see module docstring), the reverse of every
    other channel-dst type here -- checking `e.dst.startswith("chan:")` for HANDLES
    would be False (dst is the sym handler), and falling through to the generic
    "both ends are symbols" branch would silently mis-treat the channel src as a sym id
    (staging.iter_nodes() DOES include Channel NodeRecs, each with a non-empty
    qualified_name == its own id -- see core/schema.py make_channel_node -- so that
    wrong branch wouldn't even crash, it would just produce a wrong tuple with an empty
    service). The explicit type check avoids that trap.
    """
    node_lookup: dict[str, tuple[str, str]] = {
        n.id: (n.service, n.qualified_name) for n in staging.iter_nodes() if n.qualified_name
    }

    edges: set[EdgeTuple] = set()
    dangling = 0
    for e in staging.iter_edges():
        if e.type not in types:
            continue
        if e.type == "CALLS" and "mechanism" in e.props:
            # Symmetric with load_golden_edges' own CALLS mechanism-filter (mirrors
            # calls_eval.load_golden_calls -- see this module's docstring): a
            # mechanism-tagged CALLS edge (e.g. temporal_start, see
            # linking/workspace.py's _apply_temporal_start_marks) is not a direct
            # Python call golden ever records under plain CALLS, so the staged side
            # must drop it too -- otherwise it would surface in `found` with nothing on
            # the golden side to match, corrupting precision for no real reason.
            continue

        if e.type == "HANDLES":
            handler = node_lookup.get(e.dst)
            if handler is None:
                dangling += 1
                continue
            edges.add((e.type, handler[0], handler[1], e.src))
            continue

        if e.dst.startswith(_CHANNEL_PREFIX):
            src = node_lookup.get(e.src)
            if src is None:
                dangling += 1
                continue
            edges.add((e.type, src[0], src[1], e.dst))
            continue

        src = node_lookup.get(e.src)
        dst = node_lookup.get(e.dst)
        if src is None or dst is None:
            dangling += 1
            continue
        edges.add((e.type, src[0], src[1], dst[0], dst[1]))

    return edges, dangling
