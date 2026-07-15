"""M3 T6: the wire format for an embedding vector as it travels through staging's
`chunks.embedding` BLOB column (SQLite) on its way into a FalkorDB `Chunk.embedding`
vector property: float32, little-endian, one 4-byte float per vector dimension, no
length/header prefix at all (the BLOB's own byte length divided by 4 gives the
dimension back on read -- see `unpack_vector`).

`pipeline.chunk_embed._embed_missing` is the only writer (`pack_vector`, feeding
`Staging.set_embeddings`); `pipeline.load._chunk_node_batches` is the only reader
(`unpack_vector`, feeding `FalkorStore.upsert_nodes`'s `vector_props` path, which
wraps the resulting `list[float]` in Cypher's `vecf32(...)` at write time -- see
`stores/falkordb/batch.py`). Both directions live HERE, in one place, rather than each
being reimplemented ad hoc in its own module (as an earlier draft of this task did) --
two independent `struct.pack`/`struct.unpack` format strings, linked only by matching
prose comments across two files, could drift out of sync silently (a future change to
one side without mirroring the other wouldn't raise -- `struct.unpack` against
mismatched-but-length-compatible bytes just returns wrong floats)."""

from __future__ import annotations

import struct


def pack_vector(vector: list[float]) -> bytes:
    """`list[float]` -> float32 little-endian BLOB, for `Staging.set_embeddings`."""
    return struct.pack(f"<{len(vector)}f", *vector)


def unpack_vector(blob: bytes) -> list[float]:
    """float32 little-endian BLOB -> `list[float]`, for `FalkorStore.upsert_nodes`'s
    `vector_props` path (via `pipeline.load._chunk_node_batches`)."""
    count = len(blob) // 4
    return list(struct.unpack(f"<{count}f", blob))
