"""M3 T6: embedding.codec.pack_vector/unpack_vector -- the shared float32-LE wire
format between chunk_embed (writer) and pipeline.load (reader). Extracted out of both
call sites into one place (code-review finding: two independent struct.pack/unpack
format strings, linked only by prose comments, could silently drift apart)."""

from __future__ import annotations

import struct

import pytest

from codegraph.embedding.codec import pack_vector, unpack_vector


def test_pack_then_unpack_round_trips():
    vector = [0.5, -1.0, 2.25, 0.0]
    assert unpack_vector(pack_vector(vector)) == pytest.approx(vector)


def test_pack_vector_uses_float32_little_endian():
    vector = [1.0, -2.5]
    assert pack_vector(vector) == struct.pack("<2f", 1.0, -2.5)


def test_unpack_vector_matches_struct_unpack():
    blob = struct.pack("<3f", 0.1, 0.2, 0.3)
    assert unpack_vector(blob) == pytest.approx([0.1, 0.2, 0.3])


def test_empty_vector_round_trips_to_empty_list():
    assert unpack_vector(pack_vector([])) == []


def test_pack_vector_length_matches_dimension():
    assert len(pack_vector([1.0] * 8)) == 8 * 4
