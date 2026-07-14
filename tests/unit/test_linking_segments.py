"""M2 T7: linking.segments.derive -- NEXT_SEGMENT derivation over synthetic staging.

Two pair shapes (see segments.py module docstring for the full contract):
  1. exact-channel: (X -PRODUCES/CALLS_HTTP-> C) x (Y -CONSUMES-> C or C -HANDLES-> Y)
  2. containment: (X -PRODUCES/CALLS_HTTP-> event) x (Y -CONSUMES-> topic), topic
     CONTAINS event -- ONE direction only (a topic-level producer does NOT guarantee
     the specific event an event-level consumer wants; see negative test below).

confidence = product of the two boundary edges' confidence; resolution = "static" iff
BOTH boundary edges are "static", else "heuristic"; via_channel_id/derived always set;
self-pairs (X==Y) are skipped.
"""

from __future__ import annotations

from codegraph.core.schema import EdgeRec
from codegraph.linking import segments
from codegraph.stores.staging import Staging


def _edge(src, dst, type_, resolution="static", confidence=1.0, **props) -> EdgeRec:
    return EdgeRec(src=src, dst=dst, type=type_, resolution=resolution,
                   confidence=confidence, extractor="test", props=props)


# -- exact-channel pairing: PRODUCES/CONSUMES --


def test_produces_consumes_exact_pair_derives_next_segment(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES"),
        _edge("sym:b:y", "chan:event_type:E", "CONSUMES"),
    ])

    stats = segments.derive(st)

    ns = [e for e in st.iter_edges() if e.type == "NEXT_SEGMENT"]
    assert len(ns) == 1
    e = ns[0]
    assert (e.src, e.dst) == ("sym:a:x", "sym:b:y")
    assert e.props == {"via_channel_id": "chan:event_type:E", "derived": True}
    assert e.extractor == "linking"
    assert stats == {"next_segments": 1}


def test_produces_consumes_confidence_is_product_and_resolution_static_when_both_static(
    tmp_path,
):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES", resolution="static", confidence=0.9),
        _edge("sym:b:y", "chan:event_type:E", "CONSUMES", resolution="static", confidence=0.8),
    ])
    segments.derive(st)
    e = next(e for e in st.iter_edges() if e.type == "NEXT_SEGMENT")
    assert e.resolution == "static"
    assert abs(e.confidence - 0.72) < 1e-9


def test_produces_consumes_resolution_heuristic_when_either_side_not_static(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES", resolution="heuristic", confidence=0.6),
        _edge("sym:b:y", "chan:event_type:E", "CONSUMES", resolution="static", confidence=1.0),
    ])
    segments.derive(st)
    e = next(e for e in st.iter_edges() if e.type == "NEXT_SEGMENT")
    assert e.resolution == "heuristic"
    assert abs(e.confidence - 0.6) < 1e-9


# -- exact-channel pairing: CALLS_HTTP/HANDLES --


def test_calls_http_handles_exact_pair_derives_next_segment(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:client", "chan:http:b:GET /x", "CALLS_HTTP", resolution="static",
              confidence=1.0),
        _edge("chan:http:b:GET /x", "sym:b:handler", "HANDLES", resolution="static",
              confidence=1.0),
    ])

    segments.derive(st)

    ns = [e for e in st.iter_edges() if e.type == "NEXT_SEGMENT"]
    assert len(ns) == 1
    e = ns[0]
    assert (e.src, e.dst) == ("sym:a:client", "sym:b:handler")
    assert e.props["via_channel_id"] == "chan:http:b:GET /x"


# -- cross-service is expected and must pass the staging invariant --


def test_derived_edge_crosses_services_without_raising(tmp_path):
    """Positive proof that segments.py's own EdgeRec construction always satisfies
    Staging.upsert_edges' NEXT_SEGMENT cross-service invariant (via_channel_id is always
    set) -- the negative half (no via_channel_id -> InvariantError) is already pinned at
    the staging layer (test_staging.py); this proves the PRODUCER side of that contract."""
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:svc-a:x", "chan:event_type:E", "PRODUCES"),
        _edge("sym:svc-b:y", "chan:event_type:E", "CONSUMES"),
    ])
    segments.derive(st)  # must not raise InvariantError
    assert any(e.type == "NEXT_SEGMENT" for e in st.iter_edges())


# -- containment pairing: producer->event, consumer->topic (ONE direction only) --


def test_containment_event_producer_to_topic_consumer_derives(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("chan:kafka_topic:T", "chan:event_type:E", "CONTAINS"),
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES", resolution="static", confidence=1.0),
        _edge("sym:b:y", "chan:kafka_topic:T", "CONSUMES", resolution="heuristic",
              confidence=0.6),
    ])

    segments.derive(st)

    ns = [e for e in st.iter_edges() if e.type == "NEXT_SEGMENT"]
    assert len(ns) == 1
    e = ns[0]
    assert (e.src, e.dst) == ("sym:a:x", "sym:b:y")
    # via_channel_id points at the SPECIFIC channel X has a direct producer edge into
    # (the event), not the topic -- X never produces directly into the topic itself.
    assert e.props["via_channel_id"] == "chan:event_type:E"
    assert e.resolution == "heuristic"
    assert abs(e.confidence - 0.6) < 1e-9


def test_containment_topic_producer_to_event_consumer_is_not_derived(tmp_path):
    """Negative direction, explicitly excluded by the master plan: a bare topic-level
    producer does not guarantee the specific event an event-level consumer wants."""
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("chan:kafka_topic:T", "chan:event_type:E", "CONTAINS"),
        _edge("sym:a:x", "chan:kafka_topic:T", "PRODUCES"),
        _edge("sym:b:y", "chan:event_type:E", "CONSUMES"),
    ])

    stats = segments.derive(st)

    assert stats == {"next_segments": 0}
    assert not any(e.type == "NEXT_SEGMENT" for e in st.iter_edges())


def test_containment_without_matching_producer_or_consumer_derives_nothing(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([_edge("chan:kafka_topic:T", "chan:event_type:E", "CONTAINS")])
    stats = segments.derive(st)
    assert stats == {"next_segments": 0}


# -- self-pair skip --


def test_self_pair_is_skipped(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES"),
        _edge("sym:a:x", "chan:event_type:E", "CONSUMES"),
    ])
    stats = segments.derive(st)
    assert stats == {"next_segments": 0}
    assert not any(e.type == "NEXT_SEGMENT" for e in st.iter_edges())


def test_self_pair_skipped_but_other_consumers_still_derived(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x", "chan:event_type:E", "PRODUCES"),
        _edge("sym:a:x", "chan:event_type:E", "CONSUMES"),  # self -- skipped
        _edge("sym:b:y", "chan:event_type:E", "CONSUMES"),  # not self -- kept
    ])
    stats = segments.derive(st)
    assert stats == {"next_segments": 1}
    ns = next(e for e in st.iter_edges() if e.type == "NEXT_SEGMENT")
    assert (ns.src, ns.dst) == ("sym:a:x", "sym:b:y")


# -- fan-out / fan-in --


def test_multiple_producers_and_consumers_cross_product(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([
        _edge("sym:a:x1", "chan:event_type:E", "PRODUCES"),
        _edge("sym:a:x2", "chan:event_type:E", "PRODUCES"),
        _edge("sym:b:y1", "chan:event_type:E", "CONSUMES"),
        _edge("sym:b:y2", "chan:event_type:E", "CONSUMES"),
    ])
    stats = segments.derive(st)
    assert stats == {"next_segments": 4}
    pairs = {(e.src, e.dst) for e in st.iter_edges() if e.type == "NEXT_SEGMENT"}
    assert pairs == {
        ("sym:a:x1", "sym:b:y1"), ("sym:a:x1", "sym:b:y2"),
        ("sym:a:x2", "sym:b:y1"), ("sym:a:x2", "sym:b:y2"),
    }


# -- no-op --


def test_no_producer_consumer_edges_derives_nothing(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_edges([_edge("sym:a:x", "sym:a:y", "CALLS")])
    stats = segments.derive(st)
    assert stats == {"next_segments": 0}
    remaining = list(st.iter_edges())
    assert len(remaining) == 1 and remaining[0].type == "CALLS"


def test_empty_staging_derives_nothing(tmp_path):
    st = Staging(tmp_path / "s.db")
    assert segments.derive(st) == {"next_segments": 0}
