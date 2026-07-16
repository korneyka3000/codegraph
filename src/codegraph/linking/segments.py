"""M2 T7: derives NEXT_SEGMENT edges from staged channel-boundary edges (PRODUCES,
CONSUMES, CALLS_HTTP, HANDLES, CONTAINS) -- the cross-service edges that make the graph
a graph of business processes, not just N separate per-service call graphs.

Two independently-triggered pairings (see the master plan's Global Constraints + this
task's own T7 interfaces section):

  1. exact-channel: for a single channel C, every (X -PRODUCES-> C) or (X -CALLS_HTTP-> C)
     edge pairs with every (Y -CONSUMES-> C) or (C -HANDLES-> Y) edge -> NEXT_SEGMENT
     X->Y. HANDLES is the only edge type in this module whose "consumer side" node sits
     on the DST (channel -> handler, per fastapi_ext's own convention), everything else's
     consumer side is the edge's SRC (code -> channel).
  2. containment: a CONTAINS(topic -> event) edge additionally pairs every producer INTO
     the event with every consumer OF THE TOPIC (a topic-level consumer receives every
     event_type published under it, so a producer that targets the specific event
     satisfies a topic-level subscriber too). The REVERSE is deliberately not derived: a
     producer that only targets the topic (no event_type) gives no guarantee about which
     specific event_type an event-level consumer is waiting for -- see the negative test
     in test_linking_segments.py pinning this asymmetry.

Confidence/resolution for a derived edge come ONLY from the two boundary edges that
discovered the pair (the "производитель"/"consumer" edge) -- the CONTAINS edge's own
confidence is intentionally NOT folded into the product for the containment pairing
(the plan's own wording, "confidence = произведение конфиденсов пары", refers to the
PAIR of boundary edges, matching the exact-channel case's shape exactly): confidence =
producer_edge.confidence * consumer_edge.confidence; resolution = "static" iff BOTH
are "static", else "heuristic" (the plan's own literal binary rule -- no current
extractor ever puts "dynamic"/"trace_validated" on a PRODUCES/CONSUMES/CALLS_HTTP/
HANDLES edge, so a richer weakest-of-N-resolutions ranking would be untested dead code).

via_channel_id always names the channel the PRODUCER side has a direct edge into (for
the exact-channel pairing that's the only channel involved; for containment it's the
EVENT channel, never the topic -- X has no direct producer edge into the topic at all
in that pairing, so via_channel_id must be the event to stay "reconstructable": the
invariant that via_channel_id always names a channel X has a real boundary edge to).

Two DIFFERENT discovery pairs can derive the SAME (X, Y) NEXT_SEGMENT via DIFFERENT
channels (e.g. X reaches Y both via a direct HTTP call AND via a Kafka event X also
produces, or Y consumes an event directly AND consumes its containing topic) -- this is
the normal, intended parallel-channel case, not an edge case to collapse: `derived`
below is keyed on the full (x_id, y_id, via_channel_id) triple, and staging's own
(src, dst, type, via_channel) primary key (core/schema.py's SCHEMA_VERSION "2 -> 3"
history) plus the FalkorDB-side key_props MERGE key (pipeline/load.py's
_KEY_PROPS_BY_TYPE) both exist specifically so every such edge survives staging and
load intact, instead of one via_channel_id silently overwriting another. Only a true
duplicate -- the SAME (x_id, y_id, via_channel_id) triple discovered twice (e.g. both
pairing rules independently reach identical endpoints via the identical channel) --
collapses to one row, which is correct: it is the same edge, not two.
"""

from __future__ import annotations

from codegraph.core.schema import EdgeRec
from codegraph.stores.staging import Staging

_EXTRACTOR = "linking"


def _weakest_resolution(a: str, b: str) -> str:
    return "static" if a == "static" and b == "static" else "heuristic"


def _next_segment_edge(x_id: str, x_edge: EdgeRec, y_id: str, y_edge: EdgeRec,
                        via_channel_id: str) -> EdgeRec:
    return EdgeRec(
        src=x_id, dst=y_id, type="NEXT_SEGMENT",
        resolution=_weakest_resolution(x_edge.resolution, y_edge.resolution),
        confidence=x_edge.confidence * y_edge.confidence,
        extractor=_EXTRACTOR,
        props={"via_channel_id": via_channel_id, "derived": True},
    )


def derive(staging: Staging) -> dict:
    producers: dict[str, list[tuple[str, EdgeRec]]] = {}  # channel_id -> [(node_id, edge)]
    consumers: dict[str, list[tuple[str, EdgeRec]]] = {}  # channel_id -> [(node_id, edge)]
    contains_pairs: list[tuple[str, str]] = []  # (topic_id, event_id)

    for e in staging.iter_edges():
        if e.type in ("PRODUCES", "CALLS_HTTP"):
            producers.setdefault(e.dst, []).append((e.src, e))
        elif e.type == "CONSUMES":
            consumers.setdefault(e.dst, []).append((e.src, e))
        elif e.type == "HANDLES":
            consumers.setdefault(e.src, []).append((e.dst, e))
        elif e.type == "CONTAINS":
            contains_pairs.append((e.src, e.dst))

    for lst in (*producers.values(), *consumers.values()):
        lst.sort(key=lambda p: p[0])

    derived: dict[tuple[str, str, str], EdgeRec] = {}

    def emit(x_id: str, x_edge: EdgeRec, y_id: str, y_edge: EdgeRec, via_channel_id: str) -> None:
        if x_id == y_id:
            return
        # Keyed on the FULL (x_id, y_id, via_channel_id) triple -- NOT just (x_id, y_id)
        # -- so two parallel channels between the same (X, Y) pair both survive (see
        # module docstring). A bare (x_id, y_id) key would let a second channel's edge
        # silently overwrite the first here, before staging.upsert_edges is ever
        # called -- staging's own widened PK can't save data this loop never emits.
        derived[(x_id, y_id, via_channel_id)] = _next_segment_edge(
            x_id, x_edge, y_id, y_edge, via_channel_id
        )

    # 1. exact-channel pairing.
    for channel_id in sorted(producers):
        for x_id, x_edge in producers[channel_id]:
            for y_id, y_edge in consumers.get(channel_id, []):
                emit(x_id, x_edge, y_id, y_edge, channel_id)

    # 2. containment pairing: producer->event x consumer->topic, ONE direction only.
    for topic_id, event_id in sorted(contains_pairs):
        for x_id, x_edge in producers.get(event_id, []):
            for y_id, y_edge in consumers.get(topic_id, []):
                emit(x_id, x_edge, y_id, y_edge, event_id)

    edges_out = list(derived.values())
    if edges_out:
        staging.upsert_edges(edges_out)
    return {"next_segments": len(edges_out)}
