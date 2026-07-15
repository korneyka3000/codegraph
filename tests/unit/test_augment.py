"""M3 T4: augment.build_header/augment_text/fill_headers.

Two halves:
  - Synthetic/unit tests (hand-built `Staging` via public upsert_nodes/upsert_edges/
    upsert_chunks) -- pin each header component (file/service, symbol+roles+parent,
    imports cap/sort, doc truncation, each graph: sub-clause, children-aggregation's
    "no duplication for a per-method split" rule) in isolation, with full control over
    the data so exact-string assertions are possible.
  - Real-fixture tests (mirrors test_linking_smoke.py's degraded-pipeline-plus-link
    harness) -- the brief's own 5 required scenarios, run against genuinely staged data
    from `fixtures/workspace.yaml`, chunked via the real `chunk_file` with symbol_ids
    span-matched back to the ids `analyze_service` actually assigned (verified live
    against the real fixtures before writing these assertions -- see augment.py's own
    module docstring "Edge directions consulted" section for what that probe found).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from codegraph.chunking import augment
from codegraph.chunking.splitter import ChunkRec, chunk_file
from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.core import ids
from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.linking.workspace import link_workspace
from codegraph.parsing.facts import build_file_facts
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.staging import Staging

# ======================================================================================
# -- synthetic helpers (mirror test_staging.py's own _node/_chunk conventions) --
# ======================================================================================


def _node(id_, kind, service="svc", name="n", qualified_name="q", props=None, roles=()):
    return NodeRec(
        id=id_,
        kind=kind,
        service=service,
        name=name,
        qualified_name=qualified_name,
        props=props or {},
        roles=roles,
    )


def _edge(src, dst, type_, resolution="static", confidence=1.0, extractor="test", props=None):
    return EdgeRec(
        src=src,
        dst=dst,
        type=type_,
        resolution=resolution,
        confidence=confidence,
        extractor=extractor,
        props=props or {},
    )


def _chunk(chunk_id, symbol_id, ord_=0, text="x", start_line=1, end_line=1):
    return ChunkRec(
        chunk_id=chunk_id,
        symbol_id=symbol_id,
        ord=ord_,
        text=text,
        start_line=start_line,
        end_line=end_line,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


def _chunk_row(staging, service, symbol_id, ord_=0):
    return next(
        r for r in staging.chunks_for_service(service) if r.symbol_id == symbol_id and r.ord == ord_
    )


def _graph_line_of(header: str) -> str | None:
    return next((line for line in header.splitlines() if line.startswith("graph: ")), None)


# ======================================================================================
# -- file/service + symbol line --
# ======================================================================================


def test_file_service_and_bare_module_symbol_line(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("sym:svc:`app.m`/", "Module", qualified_name="app.m")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("sym:svc:`app.m`/#c0", "sym:svc:`app.m`/")])

    header = augment.build_header(st, _chunk_row(st, "svc", "sym:svc:`app.m`/"))
    assert header == "file: app/m.py · service: svc\nsymbol: app.m (Module)"


def test_symbol_line_single_role(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("f1", "Function", qualified_name="app.m.f", roles=("RouteHandler",))])
    st.upsert_chunks("svc", "app/m.py", [_chunk("f1#c0", "f1")])

    header = augment.build_header(st, _chunk_row(st, "svc", "f1"))
    assert "symbol: app.m.f (Function, RouteHandler)" in header


def test_symbol_line_multiple_roles_sorted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node(
                "f1",
                "Function",
                qualified_name="app.m.f",
                roles=("RouteHandler", "MessageProducer"),
            )
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("f1#c0", "f1")])

    header = augment.build_header(st, _chunk_row(st, "svc", "f1"))
    # sorted alphabetically -- MessageProducer before RouteHandler ('M' < 'R')
    assert "(Function, MessageProducer, RouteHandler)" in header


def test_unknown_symbol_id_falls_back_without_crashing(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_chunks("svc", "app/m.py", [_chunk("ghost#c0", "sym:svc:ghost")])

    header = augment.build_header(st, _chunk_row(st, "svc", "sym:svc:ghost"))
    assert header == "file: app/m.py · service: svc\nsymbol: sym:svc:ghost (unknown)"


# ======================================================================================
# -- parent line (method-of-a-class only) --
# ======================================================================================


def test_parent_line_present_for_method_of_a_class(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("cls", "Class", qualified_name="app.m.Big", props={"signature": "class Big"}),
            _node(
                "meth",
                "Function",
                qualified_name="app.m.Big.meth",
                props={"signature": "def meth(self)"},
            ),
        ]
    )
    st.upsert_edges([_edge("mod", "cls", "CONTAINS"), _edge("cls", "meth", "CONTAINS")])
    # `meth` owns its OWN chunk (simulates a rule-3 per-method split) -- the case where
    # the brief's "parent:" line actually matters (the method's chunk text alone has no
    # visible class context).
    st.upsert_chunks("svc", "app/m.py", [_chunk("meth#c0", "meth")])

    header = augment.build_header(st, _chunk_row(st, "svc", "meth"))
    assert "· parent: class Big" in header
    assert "symbol: app.m.Big.meth (Function) · parent: class Big" in header


def test_parent_line_absent_for_top_level_function(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("fn", "Function", qualified_name="app.m.f"),
        ]
    )
    st.upsert_edges([_edge("mod", "fn", "CONTAINS")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    assert "parent:" not in header


def test_parent_line_absent_for_the_class_itself(tmp_path):
    """A class's own CONTAINS-parent is a Module, not a Class -- no parent: line for
    the class-chunk itself (only for one of ITS methods, when that method has its own
    chunk)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("cls", "Class", qualified_name="app.m.Big", props={"signature": "class Big"}),
        ]
    )
    st.upsert_edges([_edge("mod", "cls", "CONTAINS")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("cls#c0", "cls")])

    header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    assert "parent:" not in header


def test_parent_line_omitted_when_parent_class_has_no_signature_prop(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("cls", "Class", qualified_name="app.m.Big"),  # no props at all
            _node("meth", "Function", qualified_name="app.m.Big.meth"),
        ]
    )
    st.upsert_edges([_edge("cls", "meth", "CONTAINS")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("meth#c0", "meth")])

    header = augment.build_header(st, _chunk_row(st, "svc", "meth"))
    assert "parent:" not in header


# ======================================================================================
# -- imports line --
# ======================================================================================


def test_imports_line_sorted_and_capped_at_eight(tmp_path):
    st = Staging(tmp_path / "s.db")
    targets = [f"app.pkg{i}" for i in range(10)]  # 10 > the 8-cap
    nodes = [_node("mod", "Module", qualified_name="app.m")]
    edges = []
    for i, dotted in enumerate(targets):
        tid = f"tmod{i}"
        nodes.append(_node(tid, "Module", qualified_name=dotted))
        edges.append(_edge("mod", tid, "IMPORTS"))
    st.upsert_nodes(nodes)
    st.upsert_edges(edges)
    st.upsert_chunks("svc", "app/m.py", [_chunk("mod#c0", "mod")])

    header = augment.build_header(st, _chunk_row(st, "svc", "mod"))
    imports_line = next(line for line in header.splitlines() if line.startswith("imports:"))
    shown = imports_line[len("imports: ") :].split(", ")
    assert shown == sorted(targets)[:8]
    assert len(shown) == 8


def test_imports_line_uses_owning_modules_imports_for_a_class_symbol(tmp_path):
    """A class-chunk's `imports:` line reflects its OWN FILE's imports (climbed via
    CONTAINS up to the nearest Module ancestor), not some per-class notion."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("cls", "Class", qualified_name="app.m.C"),
            _node("tgt", "Module", qualified_name="app.other"),
        ]
    )
    st.upsert_edges([_edge("mod", "cls", "CONTAINS"), _edge("mod", "tgt", "IMPORTS")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("cls#c0", "cls")])

    header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    assert "imports: app.other" in header


def test_imports_line_absent_when_module_has_no_imports(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("mod", "Module", qualified_name="app.m")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("mod#c0", "mod")])

    header = augment.build_header(st, _chunk_row(st, "svc", "mod"))
    assert "imports:" not in header


# ======================================================================================
# -- doc line --
# ======================================================================================


def test_doc_line_first_line_only(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node(
                "fn",
                "Function",
                qualified_name="app.m.f",
                props={"docstring": "First line here.\nSecond line ignored entirely."},
            )
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    assert "doc: First line here." in header
    assert "Second line" not in header


def test_doc_line_truncated_to_120_chars(tmp_path):
    long_line = "x" * 200
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [_node("fn", "Function", qualified_name="app.m.f", props={"docstring": long_line})]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    doc_line = next(line for line in header.splitlines() if line.startswith("doc:"))
    assert doc_line == "doc: " + "x" * 120


def test_doc_line_absent_when_no_docstring(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("fn", "Function", qualified_name="app.m.f")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    assert "doc:" not in header


# ======================================================================================
# -- graph: sub-clauses (direction, human names, dedup+sort, calls cap) --
# ======================================================================================


def test_graph_line_all_six_clauses_combined_exact_format(tmp_path):
    st = Staging(tmp_path / "s.db")
    sym = "fn"
    nodes = [_node(sym, "Function", qualified_name="app.m.f")]
    edges = []

    # produces (2 channels, different kinds -- sorted by human name)
    chan_topic = make_channel_node("kafka_topic", name="topicA")
    chan_event = make_channel_node("event_type", name="EventA")
    nodes += [chan_topic, chan_event]
    edges += [_edge(sym, chan_topic.id, "PRODUCES"), _edge(sym, chan_event.id, "PRODUCES")]

    # consumes
    chan_topic_b = make_channel_node("kafka_topic", name="topicB")
    nodes.append(chan_topic_b)
    edges.append(_edge(sym, chan_topic_b.id, "CONSUMES"))

    # calls_http
    chan_http_get = make_channel_node("http_route", method="GET", template="/x")
    nodes.append(chan_http_get)
    edges.append(_edge(sym, chan_http_get.id, "CALLS_HTTP"))

    # handles -- INVERTED direction: channel -> symbol
    chan_http_post = make_channel_node("http_route", method="POST", template="/y")
    nodes.append(chan_http_post)
    edges.append(_edge(chan_http_post.id, sym, "HANDLES"))

    # depends_on
    dep = _node("dep", "Function", qualified_name="app.db.get_db", name="get_db")
    nodes.append(dep)
    edges.append(_edge(sym, "dep", "DEPENDS_ON"))

    # calls -- 7 targets, cap at 5, sorted
    for i in range(1, 8):
        cid = f"c{i}"
        nodes.append(_node(cid, "Function", qualified_name=f"app.m.{cid}", name=cid))
        edges.append(_edge(sym, cid, "CALLS"))

    st.upsert_nodes(nodes)
    st.upsert_edges(edges)
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", sym)])

    header = augment.build_header(st, _chunk_row(st, "svc", sym))
    graph_line = _graph_line_of(header)
    assert graph_line == (
        "graph: produces EventA, topicA · consumes topicB · calls_http GET /x · "
        "handles POST /y · depends_on get_db · calls c1, c2, c3, c4, c5"
    )


def test_graph_line_absent_when_symbol_has_no_relevant_edges(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("fn", "Function", qualified_name="app.m.f")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    assert "graph:" not in header


def test_produces_dedups_same_channel_reached_twice(tmp_path):
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("event_type", name="E")
    st.upsert_nodes([_node("fn", "Function", qualified_name="app.m.f"), chan])
    st.upsert_edges(
        [
            _edge("fn", chan.id, "PRODUCES", extractor="a"),
            _edge("fn", chan.id, "PRODUCES", extractor="b", resolution="heuristic", confidence=0.6),
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    header = augment.build_header(st, _chunk_row(st, "svc", "fn"))
    assert _graph_line_of(header) == "graph: produces E"


# ======================================================================================
# -- children aggregation --
# ======================================================================================


def test_children_aggregation_pulls_method_edges_into_whole_class_chunk(tmp_path):
    """The OrderService shape in miniature: class C fits whole, methods m1/m2 have NO
    chunk of their own -- C's header must still surface their graph edges."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("event_type", name="Evt")
    target = _node("tgt", "Function", qualified_name="app.m.tgt", name="tgt")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("cls", "Class", qualified_name="app.m.C"),
            _node("m1", "Function", qualified_name="app.m.C.m1"),
            _node("m2", "Function", qualified_name="app.m.C.m2"),
            chan,
            target,
        ]
    )
    st.upsert_edges(
        [
            _edge("mod", "cls", "CONTAINS"),
            _edge("cls", "m1", "CONTAINS"),
            _edge("cls", "m2", "CONTAINS"),
            _edge("m1", chan.id, "PRODUCES"),
            _edge("m2", "tgt", "CALLS"),
        ]
    )
    # ONLY the class owns a chunk row -- m1/m2 have none (whole-class-fits shape).
    st.upsert_chunks("svc", "app/m.py", [_chunk("cls#c0", "cls")])

    header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    graph_line = _graph_line_of(header)
    assert graph_line == "graph: produces Evt · calls tgt"


def test_children_aggregation_does_not_duplicate_for_per_method_split_class(tmp_path):
    """Same edges as above, but this time BOTH methods own their OWN chunk (the
    oversized-class/rule-3 split shape) -- the class's own chunk must show NO graph
    block at all (methods excluded from aggregation), while each method's OWN header
    independently shows its own edge. This is the exact "у сплита детская агрегация
    НЕ дублирует" behavior the task calls out."""
    st = Staging(tmp_path / "s.db")
    chan = make_channel_node("event_type", name="Evt")
    target = _node("tgt", "Function", qualified_name="app.m.tgt", name="tgt")
    st.upsert_nodes(
        [
            _node("mod", "Module", qualified_name="app.m"),
            _node("cls", "Class", qualified_name="app.m.C", props={"signature": "class C"}),
            _node(
                "m1", "Function", qualified_name="app.m.C.m1", props={"signature": "def m1(self)"}
            ),
            _node(
                "m2", "Function", qualified_name="app.m.C.m2", props={"signature": "def m2(self)"}
            ),
            chan,
            target,
        ]
    )
    st.upsert_edges(
        [
            _edge("mod", "cls", "CONTAINS"),
            _edge("cls", "m1", "CONTAINS"),
            _edge("cls", "m2", "CONTAINS"),
            _edge("m1", chan.id, "PRODUCES"),
            _edge("m2", "tgt", "CALLS"),
        ]
    )
    # class header/gap chunk AND both methods each own a chunk row (per-method split).
    st.upsert_chunks(
        "svc",
        "app/m.py",
        [
            _chunk("cls#c0", "cls"),
            _chunk("m1#c0", "m1"),
            _chunk("m2#c0", "m2"),
        ],
    )

    class_header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    assert "graph:" not in class_header  # no duplication -- methods own their own edges

    m1_header = augment.build_header(st, _chunk_row(st, "svc", "m1"))
    assert _graph_line_of(m1_header) == "graph: produces Evt"
    assert "· parent: class C" in m1_header

    m2_header = augment.build_header(st, _chunk_row(st, "svc", "m2"))
    assert _graph_line_of(m2_header) == "graph: calls tgt"


def test_children_aggregation_recurses_two_levels_when_neither_owns_a_chunk(tmp_path):
    """class -> method (no chunk) -> nested closure (no chunk, rule 5's "never chunked
    separately") -- the class chunk must still reach the closure's own edge two levels
    down."""
    st = Staging(tmp_path / "s.db")
    target = _node("tgt", "Function", qualified_name="app.m.tgt", name="tgt")
    st.upsert_nodes(
        [
            _node("cls", "Class", qualified_name="app.m.C"),
            _node("meth", "Function", qualified_name="app.m.C.meth"),
            _node("closure", "Function", qualified_name="app.m.C.meth.closure"),
            target,
        ]
    )
    st.upsert_edges(
        [
            _edge("cls", "meth", "CONTAINS"),
            _edge("meth", "closure", "CONTAINS"),
            _edge("closure", "tgt", "CALLS"),
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("cls#c0", "cls")])  # only the class chunks

    header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    assert _graph_line_of(header) == "graph: calls tgt"


def test_children_aggregation_stops_at_a_chunked_descendant_without_recursing_past_it(tmp_path):
    """Mirror image of the previous test: `meth` DOES own its own chunk this time --
    aggregation must stop AT `meth` (excluded) and never reach `closure`'s edge, even
    though `closure` itself has no chunk of its own. `closure`'s edge is `meth`'s own
    header's concern, not the class's."""
    st = Staging(tmp_path / "s.db")
    target = _node("tgt", "Function", qualified_name="app.m.tgt", name="tgt")
    st.upsert_nodes(
        [
            _node("cls", "Class", qualified_name="app.m.C"),
            _node("meth", "Function", qualified_name="app.m.C.meth"),
            _node("closure", "Function", qualified_name="app.m.C.meth.closure"),
            target,
        ]
    )
    st.upsert_edges(
        [
            _edge("cls", "meth", "CONTAINS"),
            _edge("meth", "closure", "CONTAINS"),
            _edge("closure", "tgt", "CALLS"),
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("cls#c0", "cls"), _chunk("meth#c0", "meth")])

    class_header = augment.build_header(st, _chunk_row(st, "svc", "cls"))
    assert "graph:" not in class_header

    # `meth`'s own header DOES reach `closure` (its own un-chunked child).
    meth_header = augment.build_header(st, _chunk_row(st, "svc", "meth"))
    assert _graph_line_of(meth_header) == "graph: calls tgt"


# ======================================================================================
# -- augment_text --
# ======================================================================================


def test_augment_text_joins_header_and_code_with_blank_line():
    assert augment.augment_text("HEADER", "CODE") == "HEADER\n\nCODE"


def test_augment_text_does_not_mutate_staged_chunk_text(tmp_path):
    """augment_text's return value is a NEW string -- staging chunk text is never
    rewritten by anything in this module (build_header/fill_headers only ever write
    to context_header, per set_context_headers)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("fn", "Function", qualified_name="app.m.f")])
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn", text="ORIGINAL CODE")])

    row = _chunk_row(st, "svc", "fn")
    header = augment.build_header(st, row)
    augmented = augment.augment_text(header, row.text)
    assert augmented.endswith("ORIGINAL CODE")

    row_after = _chunk_row(st, "svc", "fn")
    assert row_after.text == "ORIGINAL CODE"  # untouched


# ======================================================================================
# -- fill_headers orchestration --
# ======================================================================================


def test_fill_headers_batches_all_chunks_of_one_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("fn1", "Function", qualified_name="app.m.f1"),
            _node("fn2", "Function", qualified_name="app.m.f2"),
        ]
    )
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn1#c0", "fn1"), _chunk("fn2#c0", "fn2")])

    updated = augment.fill_headers(st, "svc")
    assert updated == 2

    rows = {r.chunk_id: r for r in st.chunks_for_service("svc")}
    assert rows["fn1#c0"].context_header == augment.build_header(st, rows["fn1#c0"])
    assert rows["fn2#c0"].context_header == augment.build_header(st, rows["fn2#c0"])
    assert all(r.context_header is not None for r in rows.values())


def test_fill_headers_calls_set_context_headers_exactly_once(tmp_path):
    """ "Batched" means ONE `set_context_headers` call carrying every row -- not N
    individual calls that happen to converge to the same end state (which the
    end-state-only assertions above couldn't tell apart from a real batch)."""
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("fn1", "Function", qualified_name="app.m.f1"),
            _node("fn2", "Function", qualified_name="app.m.f2"),
            _node("fn3", "Function", qualified_name="app.m.f3"),
        ]
    )
    st.upsert_chunks(
        "svc",
        "app/m.py",
        [_chunk("fn1#c0", "fn1"), _chunk("fn2#c0", "fn2"), _chunk("fn3#c0", "fn3")],
    )

    calls: list[list[tuple[str, str]]] = []
    original = st.set_context_headers

    def spy(rows):
        calls.append(list(rows))
        return original(rows)

    st.set_context_headers = spy
    augment.fill_headers(st, "svc")

    assert len(calls) == 1
    assert {chunk_id for chunk_id, _ in calls[0]} == {"fn1#c0", "fn2#c0", "fn3#c0"}


def test_fill_headers_is_idempotent(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes([_node("fn", "Function", qualified_name="app.m.f", roles=("RouteHandler",))])
    st.upsert_chunks("svc", "app/m.py", [_chunk("fn#c0", "fn")])

    augment.fill_headers(st, "svc")
    first = _chunk_row(st, "svc", "fn").context_header
    augment.fill_headers(st, "svc")
    second = _chunk_row(st, "svc", "fn").context_header
    assert first == second


def test_fill_headers_scoped_to_one_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.upsert_nodes(
        [
            _node("a1", "Function", qualified_name="app.a.f", service="a"),
            _node("b1", "Function", qualified_name="app.b.f", service="b"),
        ]
    )
    st.upsert_chunks("a", "app/a.py", [_chunk("a1#c0", "a1")])
    st.upsert_chunks("b", "app/b.py", [_chunk("b1#c0", "b1")])

    augment.fill_headers(st, "a")
    assert _chunk_row(st, "a", "a1").context_header is not None
    assert _chunk_row(st, "b", "b1").context_header is None


def test_fill_headers_returns_zero_for_service_with_no_chunks(tmp_path):
    st = Staging(tmp_path / "s.db")
    assert augment.fill_headers(st, "nonexistent") == 0


# ======================================================================================
# -- real fixtures: degraded pipeline + link (mirrors test_linking_smoke.py) --
# ======================================================================================

WORKSPACE = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"
FIXTURES_ROOT = Path(__file__).parents[2] / "fixtures" / "services"
_SERVICE_DIR = {
    "orders-api": "orders_api",
    "kyc-worker": "kyc_worker",
    "document-management": "document_management",
}

ORDER_SERVICE = "sym:orders-api:`app.services.order`/OrderService#"
ORDER_MODULE = "sym:orders-api:`app.services.order`/"
CREATE_ORDER = "sym:orders-api:`app.routes.orders`/create_order()."
DOC_MGMT_CLIENT = (
    "sym:kyc-worker:`app.clients.document_management_client`/DocumentManagementClient#"
)
HANDLE_ORDER_CREATED = "sym:kyc-worker:`app.consumers.orders`/handle_order_created()."


class _AlwaysFailRunner:
    """Same technique as test_pipeline_analyze.py/test_linking_smoke.py -- forces the
    degraded fallback path without a real scip-python subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def _chunk_and_stage(staging: Staging, service: str, relpath: str) -> None:
    """Chunks one real fixture file with the REAL `chunk_file`, span-matching each
    DefFact back to the node id `analyze_service` actually staged for it (rather than
    re-deriving ids independently) -- T3's chunker isn't wired into analyze_service yet
    (that's T6/S8's job), so tests have to do this bridging themselves."""
    svc_dir = _SERVICE_DIR[service]
    source = (FIXTURES_ROOT / svc_dir / relpath).read_bytes()
    facts = build_file_facts(relpath, source)
    file_defs = [
        n
        for n in staging.iter_nodes()
        if n.service == service and n.relpath == relpath and n.kind in ("Class", "Function")
    ]
    by_span = {(n.start_byte, n.end_byte): n.id for n in file_defs}
    symbol_ids = {d.index: by_span[(d.start_byte, d.end_byte)] for d in facts.defs}
    module_node = next(
        n
        for n in staging.iter_nodes()
        if n.service == service and n.relpath == relpath and n.kind == "Module"
    )
    chunks = chunk_file(relpath, source, facts, symbol_ids, module_node.id)
    staging.upsert_chunks(service, relpath, chunks)


def _index_and_chunk_workspace(tmp_path) -> Staging:
    cfg = load_workspace(WORKSPACE)
    staging = Staging(tmp_path / "s.db")
    active_idioms = frozenset(cfg.builtin_idioms)
    for svc in cfg.services:
        analyze_service(
            svc,
            staging,
            tmp_path / "cache",
            runner=_AlwaysFailRunner(),
            active_idioms=active_idioms,
            idioms=effective_idioms(cfg, svc),
        )
    link_workspace(cfg, staging)

    for service, relpath in [
        ("orders-api", "app/services/order.py"),
        ("orders-api", "app/routes/orders.py"),
        ("kyc-worker", "app/clients/document_management_client.py"),
        ("kyc-worker", "app/consumers/orders.py"),
    ]:
        _chunk_and_stage(staging, service, relpath)
    return staging


def test_order_service_class_chunk_header_shows_produces_via_children_aggregation(tmp_path):
    """Brief's own required scenario #1 (T3-carry corrected): `order.py` chunks whole
    -- ONE chunk for the OrderService class (methods included, `place` has no chunk of
    its own) -- so `place`'s PRODUCES edge only surfaces via children-aggregation."""
    staging = _index_and_chunk_workspace(tmp_path)
    row = _chunk_row(staging, "orders-api", ORDER_SERVICE)

    header = augment.build_header(staging, row)
    assert "produces" in header
    assert "OrderCreated" in header
    graph_line = _graph_line_of(header)
    assert graph_line == "graph: produces OrderCreated · calls Order, OutboxRepository"


def test_create_order_chunk_header_shows_role_and_handles_route(tmp_path):
    """Brief's own required scenario #2: create_order is a top-level def, own chunk;
    RouteHandler role + HANDLES(chan -> create_order) sit directly on it, no
    aggregation needed."""
    staging = _index_and_chunk_workspace(tmp_path)
    row = _chunk_row(staging, "orders-api", CREATE_ORDER)

    header = augment.build_header(staging, row)
    assert "RouteHandler" in header
    assert "POST /orders" in header
    assert _graph_line_of(header) == "graph: handles POST /orders · calls OrderService"


def test_document_management_client_chunk_header_shows_calls_http_via_children_aggregation(
    tmp_path,
):
    """Brief's own required scenario #4: DocumentManagementClient chunks whole (like
    OrderService) -- get_document's CALLS_HTTP edge only surfaces via children
    aggregation. Bonus coverage: the class's own docstring shows up on the SAME chunk
    (doc: line is a property of the chunk's own symbol, unaffected by aggregation)."""
    staging = _index_and_chunk_workspace(tmp_path)
    row = _chunk_row(staging, "kyc-worker", DOC_MGMT_CLIENT)

    header = augment.build_header(staging, row)
    assert "calls_http" in header
    assert "GET /documents/{doc_id}" in header
    assert "doc: Рукописный SDK сервиса document-management." in header


def test_module_chunk_header_has_file_service_imports_but_no_graph_block(tmp_path):
    """Brief's own required scenario #5: a module-level chunk never carries a graph:
    block (Module nodes never own PRODUCES/CONSUMES/etc, and every top-level def always
    owns its own chunk -- see augment.py's own "Children aggregation" docstring section
    for why no special-casing is needed for this)."""
    staging = _index_and_chunk_workspace(tmp_path)
    row = _chunk_row(staging, "orders-api", ORDER_MODULE)
    assert row.ord == 0

    header = augment.build_header(staging, row)
    assert header.startswith("file: app/services/order.py · service: orders-api\n")
    assert "symbol: app.services.order (Module)" in header
    assert "graph:" not in header
    # bonus: imports line IS populated for a module chunk (only the graph: block is
    # module-exempt) -- order.py imports outbox/session/models.
    imports_line = next(line for line in header.splitlines() if line.startswith("imports:"))
    assert imports_line == "imports: app.db.outbox, app.db.session, app.models"


def test_handle_order_created_chunk_header_shows_synthetic_consumes_edge(tmp_path):
    """Brief's own required scenario #3 (T3-carry MANDATORY test): kyc-worker's
    dispatch_dict CONSUMES edge for `handle_order_created` needs a value-span SCIP ref
    the degraded fallback resolver never lays down (confirmed live -- no CONSUMES edge
    exists after `_index_and_chunk_workspace` alone; see also test_linking_smoke.py's
    own documented gap for the identical fixture). The header CONTRACT is "renders
    whatever is staged", independent of whether the resolver could reach it -- so the
    edge is inserted straight into staging here, exactly as it would be by a resolver
    capable of reaching it (e.g. a real scip-python run)."""
    staging = _index_and_chunk_workspace(tmp_path)

    # Sanity: confirm the documented gap is real BEFORE compensating for it, so this
    # test can't silently pass for the wrong reason if a future resolver fix makes it
    # unnecessary.
    pre_existing = [
        e for e in staging.iter_edges() if e.type == "CONSUMES" and e.src == HANDLE_ORDER_CREATED
    ]
    assert pre_existing == []

    staging.upsert_edges(
        [
            EdgeRec(
                src=HANDLE_ORDER_CREATED,
                dst=ids.chan_event("OrderCreated"),
                type="CONSUMES",
                resolution="heuristic",
                confidence=0.6,
                extractor="test-synthetic",
                props={"dispatch": "event_type"},
            )
        ]
    )

    row = _chunk_row(staging, "kyc-worker", HANDLE_ORDER_CREATED)
    header = augment.build_header(staging, row)
    assert "consumes" in header
    assert "OrderCreated" in header
    assert _graph_line_of(header) == "graph: consumes OrderCreated · calls _temporal"
