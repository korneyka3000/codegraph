"""GraphStore Protocol: контракт serving-слоя графа. FalkorStore — единственная
реализация в M1 (src/codegraph/stores/falkordb/store.py).

Hop — единственный примитив обхода, который отдаёт neighbors(): многошаговые обходы
(depth>1) собираются в Python поверх повторных вызовов neighbors() выше по стеку
(query/api.py), а не Cypher-паттернами произвольной длины (см. Global Constraints
m1b-serving.md: "Многошаговость -- Python BFS").

M2: Hop -- 4-кортеж (было 3 в M1, добавлено direction). direction ∈ {"out","in"} --
ИСТИННОЕ направление ЭТОГО конкретного перехода относительно запрошенного node_id,
а не эхо параметра direction, переданного в neighbors(): при direction="both"
neighbors() сливает out- и in-обходы в один список, и каждый hop в результате несёт
СВОЁ направление (какая часть слияния его породила), иначе после слияния "out" и
"in" стали бы неразличимы (см. FalkorStore.neighbors/._one_way).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, Protocol

Hop = tuple[str, dict, dict, str]  # (edge_type, edge_props, node_dict, direction)


class GraphStore(Protocol):
    def ensure_schema(self, dim: int | None = None) -> None:
        """`dim` (M3 T6): dimension of the Chunk.embedding vector index -- only
        created when given (a workspace with no chunk carrying a live embedding this
        run has nothing meaningful to size a vector index with); see
        `stores/falkordb/ddl.py`'s `ensure_schema` for the concrete Cypher shape."""
        ...

    def upsert_nodes(
        self, labels: tuple[str, ...], rows: list[dict], vector_props: tuple[str, ...] = ()
    ) -> int:
        """`vector_props` (M3 T6, e.g. `("embedding",)` for Chunk nodes): each name is
        read off the row's TOP level (a plain `list[float]`, not inside `props`) and
        written via `vecf32(...)` rather than a plain property `SET` -- see
        `stores/falkordb/batch.py`'s `upsert_nodes` for the full rationale/Cypher
        shape. Default `()` -- identical to the pre-M3-T6 behavior."""
        ...

    def upsert_edges(
        self,
        edge_type: str,
        rows: list[dict],
        known_ids: set[str],
        key_props: tuple[str, ...] = (),
    ) -> tuple[int, int]: ...

    def get_nodes(self, ids: Sequence[str]) -> list[dict]: ...

    def get_nodes_by_kind(self, kind: str) -> list[dict]: ...  # every node with n.kind == kind

    def find_by_qualified(self, service: str, qualified: str) -> dict | None:
        """MATCH by (service, qualified_name), ORDER BY id LIMIT 1 -- M3 T2, the
        qualified-selector-form lookup for query.api.GraphQuery.resolve_selector.
        None if no node matches."""
        ...

    def search_fulltext(
        self, query: str, k: int, kinds: Sequence[str] | None = None
    ) -> list[dict]:
        """Fulltext over Sym(name, qualified_name, docstring); query pre-sanitized
        by the implementation, empty-after-sanitize -> [] without a store round
        trip. Each result: node properties + "score" (float)."""
        ...

    def search_vector_chunks(
        self, vec: list[float], k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """M3 T7: `db.idx.vector.queryNodes('Chunk', 'embedding', k, vecf32(vec))` --
        nearest chunks by cosine DISTANCE (lower score = more similar; NOT the same
        sign convention as search_fulltext's relevance score, see the implementation's
        own docstring). Returns `[(chunk_props, score), ...]` -- a tuple, deliberately
        NOT dict-with-"score"-key like search_fulltext (retrieval.py builds RRF
        rankings straight off these pairs). No vector index on this graph (degraded --
        no embedder has ever run) -> `[]`, never an exception (confirmed live: FalkorDB
        raises a ResponseError querying an absent index; the implementation catches
        that specific case)."""
        ...

    def search_vector_chunks_exact(
        self, vec: list[float], k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """M5 T2 (pilot Bug A fix): deterministic full-scan twin of
        search_vector_chunks -- plain Cypher `vec.cosineDistance` over every Chunk
        with a non-null embedding, no ANN index involved (`ORDER BY dist ASC, c.id
        ASC` -- the id tiebreak makes this method byte-reproducible across repeated
        identical calls, unlike the ANN version's unseeded HNSW rebuild-per-load).
        Same `[(chunk_props, score), ...]` shape AND score semantics as
        search_vector_chunks (cosine distance, lower = more similar -- live-verified
        identical scale/values against FalkorDB's own ANN score, see the
        implementation's own docstring) -- callers (query.retrieval) can route
        through either method and stay agnostic to which one produced a given (id,
        score) pair. Slower than search_vector_chunks on a large graph (no index) --
        intended for eval/CI determinism (`codegraph eval retrieval --exact`), NOT
        production/MCP search, which stays ANN-only via search_vector_chunks."""
        ...

    def search_text_chunks(
        self, query: str, k: int, service: str | None = None
    ) -> list[tuple[dict, float]]:
        """M3 T7: fulltext over Chunk(text, context_header) -- same sanitize/
        empty-short-circuit contract as search_fulltext, but scoped to Chunk nodes
        and returning `[(chunk_props, score)]` tuples (see search_vector_chunks for
        why -- shared with it, not search_fulltext's dict+"score"-key shape)."""
        ...

    def neighbors(
        self,
        node_id: str,
        edge_types: Sequence[str] | None,
        direction: Literal["out", "in", "both"],
        limit: int,
    ) -> list[Hop]: ...

    def stats(self) -> dict: ...

    def graph_exists(self) -> bool: ...  # граф-ключ существует; read-only (без auto-vivify)

    def swap_in(self, build_name: str) -> None: ...  # blue/green: RENAME build_name -> self

    def delete_graph(self) -> None: ...  # удалить СВОЙ граф-ключ, если есть (идемпотентно)

    def raw(self, cypher: str, params: dict | None = None) -> Any:
        """Internal-only: тонкий проброс в g.query. НЕ экспонируется в MCP (см.
        m1b-serving.md Global Constraints: "GraphStore.raw() — internal-only, в MCP
        не экспонируется")."""
        ...
