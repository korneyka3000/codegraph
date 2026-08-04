"""Живой FalkorDB: query.retrieval.search_code/find_entrypoint (M3 T7, Step 2 брифа).
T6-паттерн (Staging + hand-crafted ChunkRec/embeddings + load_graph, БЕЗ полного
analyze_service/scip прогона -- см. tests/integration/test_pipeline_load.py's
собственный `_chunk_staging`) для мини-графов с Chunk-узлами.

Векторные сценарии используют ДВЕ разные техники контроля, обе намеренно
детерминированные (не полагаются на "похожесть по смыслу" FakeEmbedder'а, которой
у него нет -- см. embedding/fake.py собственный докстринг):
  - "top-1 sanity" (vector-mode/find_entrypoint): запрос эмбедится ТЕМ ЖЕ текстом,
    что и целевой чанк -- бит-в-бит идентичный вектор, дистанция 0, первое место
    гарантировано КОНСТРУКЦИЕЙ (T6-приём, см. test_chunk_embed_load.py).
  - "hybrid mixing" (RRF реально смешивает): query_vec вычисляется РЕАЛЬНЫМ
    FakeEmbedder'ом один раз, а вектора остальных чанков строятся АРИФМЕТИЧЕСКИ ИЗ
    query_vec (тождественная копия / поворот-и-смешивание / отрицание) -- так
    порядок по косинусной дистанции гарантирован для ЛЮБОГО фактического значения
    query_vec, не только для конкретного, которое сейчас выдаёт FakeEmbedder.
"""

from __future__ import annotations

import hashlib
import math

import pytest

from codegraph.chunking.splitter import ChunkRec
from codegraph.config.models import FalkorDBConfig
from codegraph.core.schema import NodeRec
from codegraph.embedding.codec import pack_vector
from codegraph.embedding.fake import FakeEmbedder
from codegraph.pipeline.load import load_graph
from codegraph.query import retrieval
from codegraph.stores.falkordb.connection import connect
from codegraph.stores.falkordb.store import FalkorStore
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.falkordb


def _cleanup(cfg: FalkorDBConfig, *graph_names: str) -> None:
    db = connect(cfg)
    for name in graph_names:
        try:
            db.select_graph(name).delete()
        except Exception:
            pass  # swap_in may have already consumed the build-key; final may not exist


def _chunk(chunk_id: str, symbol_id: str, text: str, ord_: int = 0) -> ChunkRec:
    return ChunkRec(
        chunk_id=chunk_id, symbol_id=symbol_id, ord=ord_, text=text,
        start_line=1, end_line=1,
        content_hash=hashlib.sha256(text.encode()).hexdigest(),
    )


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    return [v / norm for v in vec]


def _build_chunk_graph(
    falkordb_cfg: FalkorDBConfig,
    graph_name: str,
    chunks: list[ChunkRec],
    tmp_path,
    embeddings: dict[str, list[float]] | None = None,
    embed_model: str | None = None,
    symbols: list[NodeRec] | None = None,
    service: str = "svc",
) -> FalkorStore:
    """T6-паттерн: hand-built Staging (ChunkRec + опционально Sym NodeRec) ->
    load_graph. `embeddings`/`embed_model` отсутствуют (None) -> граф деградирован
    (ensure_schema(dim=None) внутри load_graph -- никакого векторного индекса,
    Meta.embed_model отсутствует), ровно сценарий "граф ни разу не эмбеден"."""
    st = Staging(tmp_path / "s.db")
    if symbols:
        st.upsert_nodes(symbols)
    st.upsert_chunks(service, "mod.py", chunks)
    if embeddings:
        st.set_embeddings([
            (
                cid, pack_vector(vec), embed_model,
                next(c.content_hash for c in chunks if c.chunk_id == cid),
            )
            for cid, vec in embeddings.items()
        ])
        st.set_meta("embed_model", embed_model)
        st.set_meta("embed_dim", str(len(next(iter(embeddings.values())))))
    load_graph(st, lambda name: FalkorStore(falkordb_cfg, name), graph_name)
    return FalkorStore(falkordb_cfg, graph_name)


VECTOR_TOP1_GRAPH = "__t7_retrieval_vector_top1__"


def test_search_code_vector_mode_top1_sanity(falkordb_cfg, tmp_path):
    """Идентичный текст -> идентичный вектор -> дистанция 0 -> топ-1 гарантирован
    конструкцией (T6-приём), не везением."""
    target_text = "TARGET_QUERY_MARKER_STRING"
    embedder = FakeEmbedder(dim=8)
    target_vec = embedder.embed_query(target_text)
    other_vec = embedder.embed_query("something else entirely unrelated")

    chunks = [
        _chunk("c-target", "sym:svc:target", "def create_order(): return 1"),
        _chunk("c-other", "sym:svc:other", "def unrelated(): return 2"),
    ]
    try:
        store = _build_chunk_graph(
            falkordb_cfg, VECTOR_TOP1_GRAPH, chunks, tmp_path,
            embeddings={"c-target": target_vec, "c-other": other_vec},
            embed_model=embedder.model_id,
        )
        result = retrieval.search_code(store, embedder, target_text, k=2, mode="vector")
        assert result["mode_used"] == "vector"
        assert result["items"][0]["chunk_id"] == "c-target"
    finally:
        _cleanup(falkordb_cfg, f"{VECTOR_TOP1_GRAPH}__build", VECTOR_TOP1_GRAPH)


TEXT_MODE_GRAPH = "__t7_retrieval_text_mode__"


def test_search_code_text_mode_finds_matching_chunk_no_embedder_needed(falkordb_cfg, tmp_path):
    match_sym = NodeRec(
        id="sym:svc:a", kind="Function", service="svc", name="create_order",
        qualified_name="app.orders.create_order",
    )
    chunks = [
        _chunk("c-match", "sym:svc:a", "def create_order(): handle the widget order"),
        _chunk("c-nomatch", "sym:svc:b", "def totally_different(): pass"),
    ]
    try:
        store = _build_chunk_graph(
            falkordb_cfg, TEXT_MODE_GRAPH, chunks, tmp_path, symbols=[match_sym]
        )
        result = retrieval.search_code(store, None, "widget", k=5, mode="text")
        assert result["mode_used"] == "text"
        assert [i["chunk_id"] for i in result["items"]] == ["c-match"]
        assert "widget" in result["items"][0]["snippet"]
        # qualified_name denormalized onto the Chunk node at load (T7 review fix) --
        # the staged symbol's own qualified_name, read straight off the live graph.
        assert result["items"][0]["qualified_name"] == "app.orders.create_order"
        # M10 T3 (pilot §4.1): enclosing_symbol/chunk_kind, same load-time join,
        # read straight off the live graph -- proves the FULL pipeline (load_graph's
        # staging.iter_nodes() pass -> Chunk node props -> retrieval._chunk_item),
        # not just the pure _chunk_props helper unit tests already cover.
        assert result["items"][0]["enclosing_symbol"] == "app.orders.create_order"
        assert result["items"][0]["chunk_kind"] == "Function"  # match_sym.kind above
    finally:
        _cleanup(falkordb_cfg, f"{TEXT_MODE_GRAPH}__build", TEXT_MODE_GRAPH)


HYBRID_MIX_GRAPH = "__t7_retrieval_hybrid_mix__"


def test_search_code_hybrid_mode_rrf_actually_mixes_both_rankings(falkordb_cfg, tmp_path):
    """3 "real" chunks: doc-a matches the TEXT query only (doc-b/doc-c don't contain
    the keyword); doc-b is the closest VECTOR match only (identical to the query
    vector); doc-c is a plausible-but-not-top vector match (rank1, NOT rank0) that
    ALSO matches the text query -- i.e. doc-c is the only one appearing in BOTH
    rankings.

    RRF(text=[doc-a, doc-c], vector=[doc-b, doc-c, ...]): doc-c is rank<=1 in BOTH ->
    score >= 2/62; doc-a is rank0 in text but NOT rank0 in vector -> its score stays
    below that even if it also appears in the vector ranking somewhere. doc-c must
    win even though it is never rank0 anywhere.

    M12 T1: search_code now over-fetches each leg to `k * _SEARCH_POOL_FACTOR`
    chunks (symbol-aggregation headroom, see query/retrieval.py's own docstring) --
    with only 3 real chunks in the graph, that pool (k=2 -> 8) swallows doc-a into
    the vector ranking too (previously, pre-T1, a plain k=2 vector fetch excluded it
    outright). FOUR filler chunks (vector-closer to the query than doc-a, but
    text-silent and each its own distinct symbol -- no aggregation collapsing) are
    added so doc-a still lands at vector-rank6, not rank2: its dampened
    1/(60+6+1)=1/67 vector contribution is too small to lift doc-a's total
    (1/61 + 1/67 ~= 0.0313) past doc-c's dual-list total (2/62 ~= 0.0323) -- the
    same "doc-c wins despite never being rank0" property the original (pre-overfetch)
    version of this test demonstrated via outright exclusion instead of damping."""
    query = "widget"
    embedder = FakeEmbedder(dim=8)
    query_vec = embedder.embed_query(query)
    # doc-c / fillers: blend query_vec with a rotated copy of itself -- some OTHER
    # direction, not proportional to query_vec for any real (hash-derived)
    # query_vec -- giving vectors strictly between "identical" and "opposite",
    # ordered by their own blend weight (higher weight -> closer to query_vec).
    rotated = query_vec[1:] + query_vec[:1]

    def _blend(weight: float) -> list[float]:
        return _normalize(
            [weight * q + (1 - weight) * p for q, p in zip(query_vec, rotated, strict=True)]
        )

    doc_c_vec = _blend(0.7)
    doc_a_vec = _normalize([-q for q in query_vec])  # exactly opposite -- maximally far
    # Weights all < doc-c's 0.7 (so doc-c stays vector-rank1, ahead of every filler)
    # and > -1/"opposite" (so every filler stays closer to query_vec than doc-a).
    filler_weights = [0.6, 0.45, 0.3, 0.15]
    filler_vecs = {f"filler-{i}": _blend(w) for i, w in enumerate(filler_weights)}

    chunks = [
        _chunk("doc-a", "sym:svc:a", "gizmo widget special-marker-alpha"),
        _chunk("doc-b", "sym:svc:b", "totally unrelated content, no keyword here"),
        _chunk("doc-c", "sym:svc:c", "gizmo widget appears in this one too"),
    ] + [
        _chunk(f"filler-{i}", f"sym:svc:filler{i}", f"irrelevant filler content marker {i}")
        for i in range(len(filler_weights))
    ]
    try:
        store = _build_chunk_graph(
            falkordb_cfg, HYBRID_MIX_GRAPH, chunks, tmp_path,
            embeddings={
                "doc-a": doc_a_vec, "doc-b": query_vec, "doc-c": doc_c_vec, **filler_vecs,
            },
            embed_model=embedder.model_id,
        )
        # Sanity on the constructed geometry itself, so a failure below points at
        # retrieval.py's fusion logic, not at a broken test-vector assumption:
        # doc-b closest, doc-c next, then the 4 fillers (relative order among
        # themselves doesn't matter), doc-a last of all 7.
        vector_only = store.search_vector_chunks(query_vec, k=7)
        ids = [props["id"] for props, _score in vector_only]
        assert len(ids) == 7
        assert ids[0] == "doc-b"
        assert ids[1] == "doc-c"
        assert ids[-1] == "doc-a"

        result = retrieval.search_code(store, embedder, query, k=2, mode="hybrid")
        assert result["mode_used"] == "hybrid"
        assert result["items"][0]["chunk_id"] == "doc-c"
    finally:
        _cleanup(falkordb_cfg, f"{HYBRID_MIX_GRAPH}__build", HYBRID_MIX_GRAPH)


DEGRADED_GRAPH = "__t7_retrieval_degraded__"


def test_search_code_degraded_graph_no_vector_index_hybrid_falls_back_to_text(
    falkordb_cfg, tmp_path,
):
    """Граф ни разу не эмбеден (embeddings=None -> ensure_schema(dim=None) -> нет
    векторного индекса, Meta без embed_model) -- mode="vector" явный error dict,
    mode="hybrid" молчаливая деградация в text (см. query.retrieval докстринг)."""
    chunks = [_chunk("c1", "sym:svc:a", "def create_order(): handle widget")]
    try:
        store = _build_chunk_graph(falkordb_cfg, DEGRADED_GRAPH, chunks, tmp_path)
        embedder = FakeEmbedder(dim=8)

        vector_result = retrieval.search_code(store, embedder, "widget", mode="vector")
        assert "error" in vector_result
        assert "reindex" in vector_result["error"].lower()

        hybrid_result = retrieval.search_code(store, embedder, "widget", mode="hybrid")
        assert "error" not in hybrid_result
        assert hybrid_result["mode_used"] == "text"
        assert [i["chunk_id"] for i in hybrid_result["items"]] == ["c1"]

        # find_entrypoint v2 degrades the SAME way (no error, mode_used="text").
        fe_result = retrieval.find_entrypoint(store, embedder, "widget")
        assert fe_result["mode_used"] == "text"
    finally:
        _cleanup(falkordb_cfg, f"{DEGRADED_GRAPH}__build", DEGRADED_GRAPH)


MISMATCH_GRAPH = "__t7_retrieval_mismatch__"


def test_search_code_meta_mismatch_error_for_vector_degrade_for_hybrid(falkordb_cfg, tmp_path):
    """Граф эмбеден МОДЕЛЬЮ A, вызывающий передаёт embedder МОДЕЛИ B -- тот же класс
    деградации, что "нет индекса вовсе" (см. query.retrieval._vector_unusable_reason),
    но с actionable "reindex needed" сообщением вместо тихого []."""
    embed_a = FakeEmbedder(dim=8, model_id="model-a")
    chunks = [_chunk("c1", "sym:svc:a", "def create_order(): handle widget")]
    try:
        store = _build_chunk_graph(
            falkordb_cfg, MISMATCH_GRAPH, chunks, tmp_path,
            embeddings={"c1": embed_a.embed_query("anything")}, embed_model="model-a",
        )
        embed_b = FakeEmbedder(dim=8, model_id="model-b")

        vector_result = retrieval.search_code(store, embed_b, "widget", mode="vector")
        assert "error" in vector_result
        assert "model-a" in vector_result["error"]
        assert "model-b" in vector_result["error"]

        hybrid_result = retrieval.search_code(store, embed_b, "widget", mode="hybrid")
        assert "error" not in hybrid_result
        assert hybrid_result["mode_used"] == "text"
    finally:
        _cleanup(falkordb_cfg, f"{MISMATCH_GRAPH}__build", MISMATCH_GRAPH)


FIND_ENTRYPOINT_GRAPH = "__t7_retrieval_find_entrypoint__"


def test_find_entrypoint_v2_hybrid_live_agrees_across_both_modalities(falkordb_cfg, tmp_path):
    """Sym-fulltext (по name/qualified_name) И chunk-vector (агрегированный до
    symbol_id) сходятся на одном и том же символе -- живая санити для v2's основного
    RRF-пути (полное "документ в обоих списках выигрывает" покрыто юнитами
    tests/unit/test_retrieval.py + search_code's собственный live-тест выше; здесь --
    что весь стек, вплоть до реального FalkorDB fulltext+vector индексов и
    get_nodes-резолюции symbol_id, реально работает вместе)."""
    target = NodeRec(
        id="sym:svc:target_func", kind="Function", service="svc", name="target_func",
        qualified_name="app.target_func",
    )
    other = NodeRec(
        id="sym:svc:other_func", kind="Function", service="svc", name="other_func",
        qualified_name="app.other_func",
    )
    query = "target_func"
    embedder = FakeEmbedder(dim=8)
    target_vec = embedder.embed_query(query)  # identical -- guarantees vector top-1
    other_vec = embedder.embed_query("something else")

    chunks = [
        _chunk("c-target", target.id, "def target_func(): pass"),
        _chunk("c-other", other.id, "def other_func(): pass"),
    ]
    try:
        store = _build_chunk_graph(
            falkordb_cfg, FIND_ENTRYPOINT_GRAPH, chunks, tmp_path,
            embeddings={"c-target": target_vec, "c-other": other_vec},
            embed_model=embedder.model_id, symbols=[target, other],
        )
        result = retrieval.find_entrypoint(store, embedder, query, k=5)
        assert result["mode_used"] == "hybrid"
        assert result["results"][0]["id"] == target.id

        # kinds filter applied AFTER fusion still narrows correctly.
        filtered = retrieval.find_entrypoint(store, embedder, query, k=5, kinds=["Class"])
        assert filtered["results"] == []  # both symbols are Function, not Class
    finally:
        _cleanup(falkordb_cfg, f"{FIND_ENTRYPOINT_GRAPH}__build", FIND_ENTRYPOINT_GRAPH)
