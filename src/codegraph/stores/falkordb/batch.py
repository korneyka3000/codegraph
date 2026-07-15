"""Батчевые upsert'ы узлов/рёбер в FalkorDB: MERGE-семантика, бисекция при ошибке батча.

Весь Cypher этого пакета живёт только здесь (stores/falkordb/) — единственное место,
где строятся f-string запросы; labels/edge_type валидируются ДО интерполяции в Cypher
(защита от инъекции — оба параметра проверяются против schema.NODE_KINDS/EDGE_TYPES
до того, как попадут в f-string).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable

from codegraph.core.errors import InvariantError
from codegraph.core.schema import EDGE_TYPES, NODE_KINDS, ROLE_KINDS

logger = logging.getLogger(__name__)

# NODE_KINDS уже содержит Service/Channel/BusinessProcess; "Sym" -- структурный
# маркер-label для кодовых узлов (не NodeRec.kind сам по себе, см. pipeline/load.
# _labels_for_kind), ROLE_KINDS -- доп. label'ы поверх kind (multi-label, напр.
# :Sym:Function:RouteHandler). Растёт автоматически вместе со schema.py -- schema.py
# остаётся единственным источником истины (fail-closed расширение, Global
# Constraint 5 плана M2).
_ALLOWED_NODE_LABELS = frozenset(NODE_KINDS | ROLE_KINDS | {"Sym"})

# M3 T1: key_props (upsert_edges) -- prop NAMES only ever come from trusted callers
# (pipeline/load.py's own constant table, not user/config input), but they still get
# interpolated straight into the Cypher f-string below (same "validate before
# interpolation" discipline as _validate_labels/_validate_edge_type -- fail-closed,
# not "we happen to control the only caller today"). Letters/underscore only --
# comfortably covers every real prop name (e.g. "via_channel_id") while staying a
# trivially safe allowlist.
_KEY_PROP_NAME_RE = re.compile(r"^[A-Za-z_]+$")


def upsert_nodes(
    g, labels: tuple[str, ...], rows: list[dict], batch_size: int = 1000
) -> int:
    """MERGE-upsert узлов по id: `MERGE (n:<L1:L2> {id: r.id}) SET n += r.props`.

    rows: [{"id": ..., "props": {...}}, ...]. При ошибке батча — рекурсивная
    бисекция (см. _bisecting_upsert). Возвращает фактически записанное количество.
    """
    _validate_labels(labels)
    if not rows:
        return 0
    label_expr = ":".join(labels)
    cypher = f"UNWIND $rows AS r MERGE (n:{label_expr} {{id: r.id}}) SET n += r.props"
    written = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        written += _bisecting_upsert(g, cypher, chunk, _describe_node)
    return written


def upsert_edges(
    g,
    edge_type: str,
    rows: list[dict],
    known_ids: set[str],
    batch_size: int = 1000,
    key_props: tuple[str, ...] = (),
) -> tuple[int, int]:
    """MERGE-upsert рёбер по (src,dst) id (+ опционально key_props), с предфильтром по
    known_ids на обоих концах.

    rows: [{"src": ..., "dst": ..., "props": {...}, **{k: row[k] for k in key_props}}, ...]
    -- key_props values live at the TOP level of each row (alongside src/dst), not only
    inside props, because the Cypher MERGE pattern below reads them as `r.<k>`; the
    caller (pipeline/load.py) is expected to also keep the same value inside props if it
    needs to persist as a real edge property (MERGE's own pattern-matching props are NOT
    automatically written -- only `SET e += r.props` writes properties). Строки, у
    которых src или dst отсутствует в known_ids, отбрасываются ДО построения params
    (никогда не попадают в Cypher-запрос) и учитываются в dropped.

    Cypher (key_props=(), the default -- unchanged from M1/M2): `UNWIND $rows AS r
    MATCH (a {id: r.src}) MATCH (b {id: r.dst}) MERGE (a)-[e:<TYPE>]->(b)
    SET e += r.props` — MATCH label-agnostic по id (индекс есть только на Sym.id;
    Service и прочие узлы без метки Sym матчатся по свойству — приемлемо на объёмах
    M1). With key_props (M3 T1, e.g. `("via_channel_id",)` for NEXT_SEGMENT -- see
    core/schema.py's SCHEMA_VERSION "2 -> 3" history comment): the MERGE relationship
    pattern grows extra key: value pairs, `MERGE (a)-[e:<TYPE> {k1: r.k1}]->(b)`, so two
    rows sharing (src,dst,type) but differing in a key_prop value MERGE onto two
    DISTINCT edges instead of one — mirrors staging's own PK widening, so a
    parallel-channel NEXT_SEGMENT pair from staging round-trips as two edges here too,
    not one silently overwritten. key_props names are validated against a
    letters/underscore allowlist before being interpolated into the Cypher f-string
    (same fail-closed discipline as edge_type/labels below — see _KEY_PROP_NAME_RE).
    Возвращает (written, dropped).
    """
    _validate_edge_type(edge_type)
    _validate_key_props(key_props)
    filtered = [r for r in rows if r["src"] in known_ids and r["dst"] in known_ids]
    dropped = len(rows) - len(filtered)
    if not filtered:
        return 0, dropped
    cypher = (
        f"UNWIND $rows AS r MATCH (a {{id: r.src}}) MATCH (b {{id: r.dst}}) "
        f"MERGE (a)-[e:{edge_type}{_merge_key_expr(key_props)}]->(b) SET e += r.props"
    )
    written = 0
    for i in range(0, len(filtered), batch_size):
        chunk = filtered[i : i + batch_size]
        written += _bisecting_upsert(g, cypher, chunk, _describe_edge)
    return written, dropped


def _merge_key_expr(key_props: tuple[str, ...]) -> str:
    if not key_props:
        return ""
    inner = ", ".join(f"{k}: r.{k}" for k in key_props)
    return f" {{{inner}}}"


def _validate_labels(labels: tuple[str, ...]) -> None:
    invalid = [label for label in labels if label not in _ALLOWED_NODE_LABELS]
    if invalid:
        raise InvariantError(f"invalid node label(s): {invalid!r}")


def _validate_edge_type(edge_type: str) -> None:
    if edge_type not in EDGE_TYPES:
        raise InvariantError(f"invalid edge type: {edge_type!r}")


def _validate_key_props(key_props: tuple[str, ...]) -> None:
    invalid = [k for k in key_props if not _KEY_PROP_NAME_RE.fullmatch(k)]
    if invalid:
        raise InvariantError(f"invalid key_props name(s): {invalid!r}")


def _describe_node(row: dict) -> str:
    return f"id={row.get('id')!r}"


def _describe_edge(row: dict) -> str:
    return f"src={row.get('src')!r} dst={row.get('dst')!r}"


def _bisecting_upsert(
    g, cypher: str, rows: list[dict], describe: Callable[[dict], str]
) -> int:
    """Выполняет батч; при ошибке — рекурсивная бисекция пополам до одиночной строки.

    Одиночная строка, вызывающая ошибку, — warning в лог и skip (без raise: одна
    плохая строка не должна проваливать весь upsert). Возвращает фактически
    записанное количество строк для данного (под)батча.
    """
    try:
        g.query(cypher, {"rows": rows})
        return len(rows)
    except Exception as e:
        if len(rows) == 1:
            logger.warning("skipping bad row (%s): %s", describe(rows[0]), e)
            return 0
        mid = len(rows) // 2
        return (
            _bisecting_upsert(g, cypher, rows[:mid], describe)
            + _bisecting_upsert(g, cypher, rows[mid:], describe)
        )
