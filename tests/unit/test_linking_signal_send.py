"""M8 T2 (rerun-2 R5): linking.signal_send -- links `temporal_signal_send` claims
(temporal_ext.py's own typed-sender arg0 resolution, `handle.signal(Cls.method,
payload)` / a bare-name imported method) onto the SAME temporal_signal Channel a
handler's own CONSUMES edge (M7 T4) already targets. See linking/signal_send.py's
own module docstring for the full algorithm/honesty-rule/dedup argument this test
file exercises."""

from __future__ import annotations

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking import signal_send
from codegraph.stores.staging import Staging

HANDLER_SYMBOL = "sym:gateway:`app.workflows.survey`/SurveyWorkflow#complete_survey()."
SENDER_SYMBOL = "sym:worker:`app.consumers.doc`/DocConsumer.notify()."


def _fn(id_: str, service: str = "svc") -> NodeRec:
    return NodeRec(id=id_, kind="Function", service=service, name="h", qualified_name="q.h")


def _consumes(handler_id: str, chan_id: str) -> EdgeRec:
    """Mirrors temporal_ext.py's OWN handler-side CONSUMES emission
    (_extract_signal_kind_roles) -- extractor="temporal", static/1.0."""
    return EdgeRec(
        src=handler_id, dst=chan_id, type="CONSUMES",
        resolution="static", confidence=1.0, extractor="temporal",
        props={"signal_kind": "signal"},
    )


def _claim(
    staging: Staging, service: str, relpath: str, *,
    src_id: str, method_symbol: str, evidence_line: int | None = None,
) -> None:
    staging.add_claims(service, relpath, "temporal_signal_send", [{
        "src_id": src_id, "method_symbol": method_symbol, "evidence_line": evidence_line,
    }])


# -- link(): return shape / no-op on empty staging --


def test_link_returns_signal_send_unlinked_key(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = signal_send.link(st)
    assert report == {"signal_send_unlinked": 0}


def test_link_no_claims_is_a_pure_noop(tmp_path):
    st = Staging(tmp_path / "s.db")
    report = signal_send.link(st)
    assert report == {"signal_send_unlinked": 0}
    assert not list(st.iter_edges())


# -- resolved symbol + existing CONSUMES -> PRODUCES static/1.0 into the SAME channel --


def test_claim_with_existing_consumes_produces_static_edge_into_same_channel(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("temporal_signal", name="complete-survey")
    sender = _fn(SENDER_SYMBOL, "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    st.upsert_nodes([chan, sender, handler])
    st.upsert_edges([_consumes(handler.id, chan.id)], origin_service="gateway")
    _claim(
        st, "worker", "app/consumers/doc.py",
        src_id=sender.id, method_symbol=handler.id, evidence_line=42,
    )

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 0}
    produces = [e for e in st.iter_edges() if e.type == "PRODUCES"]
    assert len(produces) == 1
    e = produces[0]
    assert (e.src, e.dst) == (sender.id, chan.id)
    assert e.resolution == "static" and e.confidence == 1.0
    assert e.extractor == "linking"
    assert e.props == {"mechanism": "temporal_signal"}
    assert e.evidence_file == "app/consumers/doc.py"
    assert e.evidence_line == 42


# -- resolved symbol, NO matching CONSUMES edge -> signal_send_unlinked, no edge --


def test_claim_without_consumes_bumps_unlinked_counter_no_edge(tmp_path):
    st = Staging(tmp_path / "s.db")
    sender = _fn(SENDER_SYMBOL, "worker")
    st.upsert_nodes([sender])
    _claim(
        st, "worker", "app/consumers/doc.py",
        src_id=sender.id, method_symbol="sym:gateway:`app.x`/NotAHandler#m().",
        evidence_line=1,
    )

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 1}
    assert not any(e.type == "PRODUCES" for e in st.iter_edges())


def test_consumes_edge_into_non_temporal_signal_channel_does_not_satisfy_the_claim(tmp_path):
    """The CONSUMES scan is scoped to `chan:temporal_signal:` dsts only -- an
    UNRELATED CONSUMES edge from the identical symbol into a DIFFERENT channel kind
    (e.g. a kafka topic -- structurally impossible in practice for one symbol to
    carry both roles, but nothing stops staging from holding it) must never be
    treated as satisfying a temporal_signal_send claim."""
    st = Staging(tmp_path / "s.db")
    sender = _fn(SENDER_SYMBOL, "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    kafka_chan = make_channel_node("kafka_topic", name="unrelated.topic")
    st.upsert_nodes([sender, handler, kafka_chan])
    st.upsert_edges(
        [EdgeRec(src=handler.id, dst=kafka_chan.id, type="CONSUMES",
                  resolution="static", confidence=1.0, extractor="kafka")],
        origin_service="gateway",
    )
    _claim(
        st, "worker", "app/consumers/doc.py",
        src_id=sender.id, method_symbol=handler.id, evidence_line=1,
    )

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 1}
    assert not any(e.type == "PRODUCES" for e in st.iter_edges())


# -- linking-layer bridge property (NOT a reachable cross-service extraction state) --


def test_linking_layer_bridges_channel_regardless_of_services(tmp_path):
    """Pins the LINKING layer's own service-agnosticism, in isolation: the CONSUMES
    map is workspace-wide, and the created PRODUCES' chan:-prefixed dst passes
    upsert_edges' cross-service invariant unconditionally (sym-to-sym directly
    would be forbidden; the channel is the legal bridge). The foreign-service
    method id below is HAND-CRAFTED -- real extraction can never produce it:
    temporal_ext._resolve_ref stamps the SENDER's own service into method_symbol
    (symbol_to_node_id(ctx.service, ...)), so a REAL cross-service typed send
    resolves to a sym:<sender-svc>:... id, matches nothing here, and lands in
    signal_send_unlinked -- the TRACKED LIMITATION documented in both
    temporal_ext.py and linking/signal_send.py (M8 T2 review Important-1). This
    pin therefore proves the linking-side bridge ALONE: if the extraction-side
    limitation is ever lifted (a package-aware symbol->service mapping), no
    linking change will be needed."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("temporal_signal", name="doc-approved")
    sender = _fn(SENDER_SYMBOL, "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    st.upsert_nodes([chan, sender, handler])
    st.upsert_edges([_consumes(handler.id, chan.id)], origin_service="gateway")
    _claim(
        st, "worker", "app/consumers/doc.py",
        src_id=sender.id, method_symbol=handler.id, evidence_line=7,
    )

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 0}
    produces = next(e for e in st.iter_edges() if e.type == "PRODUCES")
    assert (produces.src, produces.dst) == (sender.id, chan.id)


# -- dedup: two DIFFERENT sender methods -> two edges; two claims from the SAME --
# -- sender method -> one edge (documented evidence-choice, mirrors the extractor's --
# -- own pre-existing "architecturally shared, accepted" dedup property) --


def test_two_different_senders_to_same_handler_produce_two_edges(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("temporal_signal", name="complete-survey")
    sender_a = _fn("sym:worker:a", "worker")
    sender_b = _fn("sym:worker:b", "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    st.upsert_nodes([chan, sender_a, sender_b, handler])
    st.upsert_edges([_consumes(handler.id, chan.id)], origin_service="gateway")
    _claim(st, "worker", "a.py", src_id=sender_a.id, method_symbol=handler.id, evidence_line=1)
    _claim(st, "worker", "b.py", src_id=sender_b.id, method_symbol=handler.id, evidence_line=2)

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 0}
    produces = [e for e in st.iter_edges() if e.type == "PRODUCES"]
    assert {e.src for e in produces} == {sender_a.id, sender_b.id}
    assert all(e.dst == chan.id for e in produces)


def test_two_call_sites_in_same_sender_method_collapse_to_one_edge(tmp_path):
    """Two `.signal(WF.go, ...)` call sites inside the SAME enclosing method emit
    TWO temporal_signal_send claims sharing the identical (src_id, method_symbol)
    pair -- both compose the SAME (src, dst) PRODUCES edge, which staging's own PK
    (src, dst, type, via_channel, origin_service) collapses to one row. The
    surviving evidence is the lexicographically-LAST payload_json's (claims_for
    orders rows by (service, relpath, payload_json) -- byte-deterministic across
    runs, NOT source-order): here `{"evidence_line": 9, ...}` sorts after
    `{"evidence_line": 3, ...}`, so line 9 survives. Documented, accepted property
    (mirrors temporal_ext.py's OWN pre-existing multi-call-site dedup, see its
    module docstring's "No cross-request dedup" paragraph) -- the EDGE (a PRODUCES
    relationship exists) is what matters, not which one of N identical call sites'
    own evidence_line survives."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("temporal_signal", name="complete-survey")
    sender = _fn(SENDER_SYMBOL, "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    st.upsert_nodes([chan, sender, handler])
    st.upsert_edges([_consumes(handler.id, chan.id)], origin_service="gateway")
    _claim(st, "worker", "a.py", src_id=sender.id, method_symbol=handler.id, evidence_line=3)
    _claim(st, "worker", "a.py", src_id=sender.id, method_symbol=handler.id, evidence_line=9)

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 0}
    produces = [e for e in st.iter_edges() if e.type == "PRODUCES"]
    assert len(produces) == 1
    # lexicographically-last payload_json wins (claims_for ORDER BY) via staging PK dedup
    assert produces[0].evidence_line == 9


def test_resolved_symbol_with_two_distinct_consumes_channels_produces_edge_per_channel(tmp_path):
    """Belt-and-braces: nothing structurally forbids one method_symbol from
    carrying more than one CONSUMES edge into a temporal_signal channel -- this
    module never picks one arbitrarily (see linking/signal_send.py's own
    docstring), it reports every channel it finds."""
    st = Staging(tmp_path / "s.db")
    chan_a = make_channel_node("temporal_signal", name="a")
    chan_b = make_channel_node("temporal_signal", name="b")
    sender = _fn(SENDER_SYMBOL, "worker")
    handler = _fn(HANDLER_SYMBOL, "gateway")
    st.upsert_nodes([chan_a, chan_b, sender, handler])
    st.upsert_edges(
        [_consumes(handler.id, chan_a.id), _consumes(handler.id, chan_b.id)],
        origin_service="gateway",
    )
    _claim(st, "worker", "a.py", src_id=sender.id, method_symbol=handler.id, evidence_line=1)

    report = signal_send.link(st)

    assert report == {"signal_send_unlinked": 0}
    produces_dst = {e.dst for e in st.iter_edges() if e.type == "PRODUCES"}
    assert produces_dst == {chan_a.id, chan_b.id}
