"""Юнит-тест pipeline.load._labels_for_kind/_node_props (M2): чистые функции
kind/roles -> labels-кортеж и NodeRec -> props-dict, без живого FalkorDB (тот
сценарий -- tests/integration/test_pipeline_load.py, marker falkordb, реальный
MERGE с multi-label)."""

from __future__ import annotations

import hashlib
import struct

import pytest

from codegraph.chunking.splitter import ChunkRec
from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.pipeline.load import (
    _chunk_node_batches,
    _chunk_props,
    _edge_row,
    _key_props_for,
    _labels_for_kind,
    _node_props,
)
from codegraph.stores.staging import ChunkRow, Staging


def test_code_kind_without_roles():
    assert _labels_for_kind("Function") == ("Sym", "Function")
    assert _labels_for_kind("Class") == ("Sym", "Class")
    assert _labels_for_kind("Module") == ("Sym", "Module")


def test_code_kind_with_roles_appended_in_order():
    assert _labels_for_kind("Function", ("RouteHandler",)) == ("Sym", "Function", "RouteHandler")
    assert _labels_for_kind("Function", ("MessageConsumer", "TemporalActivity")) == (
        "Sym", "Function", "MessageConsumer", "TemporalActivity",
    )


def test_service_kind_ignores_roles():
    assert _labels_for_kind("Service") == ("Service",)
    assert _labels_for_kind("Service", ("RouteHandler",)) == ("Service",)


def test_channel_kind():
    assert _labels_for_kind("Channel") == ("Channel",)
    assert _labels_for_kind("Channel", ("RouteHandler",)) == ("Channel",)


def test_business_process_kind():
    assert _labels_for_kind("BusinessProcess") == ("BusinessProcess",)


def test_unknown_kind_raises_invariant_error():
    with pytest.raises(InvariantError):
        _labels_for_kind("Nope")


# -- _node_props: roles mirrored into props (M2 T8, traverse.py needs them --
# store.get_nodes()/neighbors() only ever return n.properties, never labels(n),
# so a role-carrying node's roles must ALSO live as an explicit prop, redundant
# with the graph labels _labels_for_kind produces) --


def test_node_props_includes_roles_list_when_present():
    n = NodeRec(
        id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f",
        roles=("RouteHandler",),
    )
    assert _node_props(n)["roles"] == ["RouteHandler"]


def test_node_props_preserves_role_order_for_multiple_roles():
    n = NodeRec(
        id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f",
        roles=("MessageConsumer", "TemporalActivity"),
    )
    assert _node_props(n)["roles"] == ["MessageConsumer", "TemporalActivity"]


def test_node_props_omits_roles_key_when_no_roles():
    n = NodeRec(id="sym:a:f", kind="Function", service="a", name="f", qualified_name="m.f")
    assert "roles" not in _node_props(n)


def test_node_props_omits_roles_key_for_channel_and_service_kinds():
    # Channel/Service/BusinessProcess NodeRecs never carry roles (roles are only
    # meaningful for code kinds) -- default roles=() -- same omission as above,
    # exercised on the non-code kinds specifically since those are the ones
    # _labels_for_kind ignores roles for entirely.
    from codegraph.core.schema import make_service_node

    assert "roles" not in _node_props(make_service_node("svc"))


# -- M3 T1: _edge_row/_key_props_for -- NEXT_SEGMENT's via_channel_id promoted to the
# row's TOP level (alongside src/dst), since batch.upsert_edges' MERGE-key Cypher reads
# key_props as r.<k>, not r.props.<k> (see stores/falkordb/batch.py's upsert_edges
# docstring and core/schema.py's SCHEMA_VERSION "2 -> 3" history comment) --


def test_key_props_for_next_segment_is_via_channel_id():
    assert _key_props_for("NEXT_SEGMENT") == ("via_channel_id",)


def test_key_props_for_other_types_is_empty():
    assert _key_props_for("CALLS") == ()
    assert _key_props_for("PRODUCES") == ()


def test_edge_row_promotes_via_channel_id_to_top_level_for_next_segment():
    e = EdgeRec(src="a", dst="b", type="NEXT_SEGMENT", resolution="derived", confidence=0.9,
                extractor="linking",
                props={"via_channel_id": "chan:kafka_topic:orders", "derived": True})
    row = _edge_row(e)
    assert row["src"] == "a" and row["dst"] == "b"
    assert row["via_channel_id"] == "chan:kafka_topic:orders"
    # still inside props too -- that's what SET e += r.props actually persists as a
    # real graph edge property (the top-level copy only feeds the MERGE key).
    assert row["props"]["via_channel_id"] == "chan:kafka_topic:orders"


def test_edge_row_normalizes_missing_via_channel_id_to_empty_string():
    e = EdgeRec(src="a", dst="b", type="NEXT_SEGMENT", resolution="derived", confidence=0.9,
                extractor="linking")
    row = _edge_row(e)
    assert row["via_channel_id"] == ""


def test_edge_row_other_types_get_no_via_channel_id_key_at_all():
    e = EdgeRec(src="a", dst="b", type="CALLS", resolution="static", confidence=1.0,
                extractor="calls")
    row = _edge_row(e)
    assert "via_channel_id" not in row
    assert set(row) == {"src", "dst", "props"}


# -- M3 T6: _chunk_props -- pure ChunkRow -> Chunk-node-props helper (no Staging/live
# FalkorDB needed; see tests/unit/test_embedding_codec.py for the pack_vector/
# unpack_vector round-trip and tests/integration/test_pipeline_load.py for the live
# end-to-end Chunk/Meta node + vecf32 sanity check) --


def _row(
    chunk_id="c#c0", symbol_id="sym:a:f", service="a", relpath="m.py", ord_=0,
    text="hello", start_line=1, end_line=1, content_hash="hash1",
    context_header="file: m.py", embedding=None, embed_model=None, embedded_hash=None,
    input_hash=None,
):
    return ChunkRow(
        chunk_id=chunk_id, symbol_id=symbol_id, service=service, relpath=relpath,
        ord=ord_, text=text, start_line=start_line, end_line=end_line,
        content_hash=content_hash, context_header=context_header, embedding=embedding,
        embed_model=embed_model, embedded_hash=embedded_hash, input_hash=input_hash,
    )


def test_chunk_props_maps_every_documented_field():
    row = _row(embed_model="fake-8d")
    props = _chunk_props(row)
    assert props == {
        "id": "c#c0", "kind": "Chunk", "symbol_id": "sym:a:f", "service": "a",
        "relpath": "m.py", "ord": 0, "start_line": 1, "end_line": 1,
        "content_hash": "hash1", "text": "hello", "context_header": "file: m.py",
        "embed_model": "fake-8d",
    }


def test_chunk_props_always_includes_kind_chunk():
    """FalkorStore.stats()/get_nodes_by_kind group/filter by the `kind` PROPERTY
    (Cypher labels can't be parameterized) -- a Chunk node missing it would collapse
    into a `None` bucket in stats(), breaking cli.py's `stats` command's `sorted()`
    call the moment a graph has both a kind-bearing node and a Chunk node."""
    assert _chunk_props(_row())["kind"] == "Chunk"


def test_chunk_props_omits_none_embed_model_and_none_context_header():
    row = _row(context_header=None, embed_model=None)
    props = _chunk_props(row)
    assert "embed_model" not in props
    assert "context_header" not in props
    assert props["id"] == "c#c0"  # unaffected core fields still present


def test_chunk_props_never_includes_embedding_or_embedded_hash():
    """The vector value travels via a SEPARATE top-level row field ("embedding"), not
    inside props at all -- see batch.upsert_nodes' own vector_props docstring for why
    (a plain `SET n += {embedding: [...]}"` would store an ordinary array property, not
    the vecf32-encoded type the vector index needs). embedded_hash AND input_hash (M4
    T1's own new ChunkRow column) are both staging-only bookkeeping columns, never a
    graph-visible property -- input_hash in particular is staging-internal cache-key
    plumbing the graph has no use for at all (see core/schema.py's SCHEMA_VERSION
    "4 -> 5" history entry)."""
    row = _row(
        embedding=b"\x00\x00\x80?", embed_model="m", embedded_hash="hash1",
        input_hash="ih-1",
    )
    props = _chunk_props(row)
    assert "embedding" not in props
    assert "embedded_hash" not in props
    assert "input_hash" not in props


# -- M3 T7 review fix: qualified_name denormalized onto the Chunk node at load time
# (the owning symbol's qualified_name, joined via row.symbol_id -- see _chunk_props'
# own docstring for why load-time denormalization beat a per-search get_nodes
# backfill) --


def test_chunk_props_joins_qualified_name_from_map():
    props = _chunk_props(_row(symbol_id="sym:a:f"), {"sym:a:f": "app.mod.f"})
    assert props["qualified_name"] == "app.mod.f"


def test_chunk_props_omits_qualified_name_when_symbol_not_in_map():
    # Defensive edge case: a chunk whose symbol_id has no staged node -- the property
    # is simply absent (same _omit_none convention), never a None value in the graph.
    props = _chunk_props(_row(symbol_id="sym:a:ghost"), {"sym:a:f": "app.mod.f"})
    assert "qualified_name" not in props


def test_chunk_props_omits_qualified_name_without_a_map_at_all():
    # Direct callers passing no map (pre-T7 signature) keep the exact pre-T7 shape.
    assert "qualified_name" not in _chunk_props(_row())


def test_chunk_node_batches_passes_qualified_names_through(tmp_path):
    st = _staged_chunk_with_embedding(tmp_path, [1.0, 2.0, 3.0])
    with_vector, without_vector = _chunk_node_batches(
        st, dim=3, qualified_names={"c": "app.mod.c"}  # symbol_id "c", see helper
    )
    assert without_vector == []
    assert with_vector[0]["props"]["qualified_name"] == "app.mod.c"


# -- M3 T6 code-review fix: _chunk_node_batches routes a dimension-mismatched
# embedding to the without-vector batch instead of vector_props, rather than letting
# batch.upsert_nodes' own bisection silently drop that one row later with no context
# tying it back to the real cause --


def _staged_chunk_with_embedding(tmp_path, vector: list[float]) -> Staging:
    st = Staging(tmp_path / "s.db")
    text = "hello"
    chunk = ChunkRec(
        chunk_id="c#c0", symbol_id="c", ord=0, text=text, start_line=1, end_line=1,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )
    st.upsert_chunks("a", "m.py", [chunk])
    blob = struct.pack(f"<{len(vector)}f", *vector)
    st.set_embeddings([("c#c0", blob, "model-a", chunk.content_hash)])
    return st


def test_chunk_node_batches_routes_dim_mismatch_to_without_vector(tmp_path):
    st = _staged_chunk_with_embedding(tmp_path, [1.0, 2.0, 3.0])  # 3-dim

    with_vector, without_vector = _chunk_node_batches(st, dim=8)  # index is 8-dim

    assert with_vector == []
    assert len(without_vector) == 1
    assert without_vector[0]["id"] == "c#c0"
    assert "embedding" not in without_vector[0]
    # a Chunk advertising an embed_model whose embedding it doesn't carry is the same
    # inconsistency at property granularity -- stripped along with the vector.
    assert "embed_model" not in without_vector[0]["props"]


def test_chunk_node_batches_matching_dim_goes_to_with_vector(tmp_path):
    st = _staged_chunk_with_embedding(tmp_path, [1.0, 2.0, 3.0])

    with_vector, without_vector = _chunk_node_batches(st, dim=3)  # matches

    assert without_vector == []
    assert len(with_vector) == 1
    assert with_vector[0]["embedding"] == pytest.approx([1.0, 2.0, 3.0])
    assert with_vector[0]["props"]["embed_model"] == "model-a"  # kept when vector is kept


def test_chunk_node_batches_dim_none_routes_stale_embedding_to_without_vector(tmp_path):
    """Coordinator fix (reviewer-reproduced leak): dim=None means this run had NO
    working embedder -- ensure_schema created NO vector index and Meta carries NO
    embed_model -- so a STALE embedding blob surviving in staging from a PRIOR run
    (upsert_chunks' ON-CONFLICT contract deliberately preserves it) must NOT leak into
    the vecf32 batch. The pre-fix guard (`dim is not None and len != dim`) waved it
    straight through, producing a Chunk carrying embedding+embed_model in a graph
    whose Meta advertises no model and which has no vector index at all."""
    st = _staged_chunk_with_embedding(tmp_path, [1.0, 2.0, 3.0])

    with_vector, without_vector = _chunk_node_batches(st)  # dim defaults to None

    assert with_vector == []
    assert len(without_vector) == 1
    assert without_vector[0]["id"] == "c#c0"
    assert "embedding" not in without_vector[0]
    assert "embed_model" not in without_vector[0]["props"]
