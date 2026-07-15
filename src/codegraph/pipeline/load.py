"""S9 load: staging (SQLite) -> FalkorDB, blue/green.

Композиция store_factory (закреплена живым тестом Task 3, tests/integration/
test_falkordb_store.py): build_store = store_factory(f"{graph_name}__build") --
это store, В КОТОРЫЙ мы пишем узлы/рёбра; final_store = store_factory(graph_name) --
отдельный store с ЦЕЛЕВЫМ именем, и именно НА НЁМ вызывается final_store.swap_in(
build_name), потому что FalkorStore.swap_in(build_name) переименовывает build_name
в self.graph_name (см. store.py: `RENAME build_name self.graph_name`) -- self
здесь обязан УЖЕ быть final-именем, иначе получим RENAME в неверную сторону.

Labels staging не хранит как готовый набор для serving-графа -- реконструируем по
(kind, roles) (Staging.iter_nodes() отдаёт оба, roles восстановлены из labels-json,
см. staging.py): {Module,Class,Function} (кодовые) -> ("Sym", kind, *roles) --
roles добавляют multi-label поверх kind (M2, см. core/schema.py ROLE_KINDS);
Service -> ("Service",); Channel -> ("Channel",); BusinessProcess ->
("BusinessProcess",) -- эти три игнорируют roles (см. _labels_for_kind). Ребро ->
группировка по type (единственный дискриминатор, который есть у EdgeRec и который
batch.py принимает как edge_type).

known_ids собирается ПОКА обходим все узлы (один проход iter_nodes(), до единой
записи ребра) -- это обязательное условие корректности endpoint-policy рёбер:
`batch.upsert_edges` дропает ребро, если src/dst нет в known_ids, а known_ids
должен отражать ПОЛНЫЙ набор узлов графа, не только уже записанную лейбл-группу
(иначе ребро между двумя ещё не сгруппированными узлами дропалось бы ложно).

Свойства узлов/рёбер: None-значения ВЫРЕЗАНЫ из props целиком, не переданы как
null. Живой пробой (см. отчёт m1b-task-5) подтверждено: FalkorDB `SET n += {k:
null}` для НИКОГДА не существовавшего свойства -- no-op (ключ не появляется);
для УЖЕ существующего -- СТИРАЕТ его (открытая семантика Cypher `+=`). Обе ветки
безопасны сами по себе, но проще и однозначнее просто не посылать null. Списки
строк (decorators) и bool (is_async) отправляются как есть -- живой пробой
подтверждено, что FalkorDB хранит и возвращает python list/bool без искажений
через UNWIND/SET += (json-string fallback из плана не понадобился).

Crash-recovery: build-граф сбрасывается (delete_graph) ПЕРВЫМ действием каждого
прогона. Успешный прогон и так потребляет build-ключ через RENAME, но прогон,
упавший ПОСЛЕ частичной записи и ДО swap_in, оставляет ключ жить -- без сброса
этот мусор протёк бы в финальный граф при следующем успешном прогоне (живьём
воспроизведено в первичном ревью T5; регрессия -- test_load_graph_resets_stale_
build_graph_from_crashed_run). Заодно сброс снимает и след-риск уровня свойств:
None-omission (выше) не стирает устаревшее значение на переиспользуемом узле,
но переиспользуемых узлов теперь не бывает -- build всегда стартует пустым.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.stores.graph import GraphStore
from codegraph.stores.staging import Staging

_CODE_KINDS = frozenset({"Module", "Class", "Function"})

_NODE_CORE_FIELDS = (
    "id", "kind", "service", "name", "qualified_name",
    "relpath", "start_line", "end_line", "start_byte", "end_byte", "content_hash",
)
_EDGE_CORE_FIELDS = ("resolution", "confidence", "extractor", "evidence_file", "evidence_line")


def _labels_for_kind(kind: str, roles: tuple[str, ...] = ()) -> tuple[str, ...]:
    """Кодовые kinds (Module/Class/Function) -> ("Sym", kind, *roles) -- roles
    добавляют доп. label'ы поверх kind (multi-label, см. core/schema.py ROLE_KINDS).
    Service/Channel/BusinessProcess -- фиксированный однословный label, roles
    игнорируются (роли осмысленны только для кодовых узлов)."""
    if kind in _CODE_KINDS:
        return ("Sym", kind, *roles)
    if kind == "Service":
        return ("Service",)
    if kind == "Channel":
        return ("Channel",)
    if kind == "BusinessProcess":
        return ("BusinessProcess",)
    raise InvariantError(f"unknown node kind for graph load: {kind!r}")


def _omit_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def _node_props(n: NodeRec) -> dict:
    core = {field: getattr(n, field) for field in _NODE_CORE_FIELDS}
    props = _omit_none({**core, **n.props})
    # M2 T8: roles live as graph LABELS via _labels_for_kind (multi-label, e.g.
    # :Sym:Function:RouteHandler) -- but store.get_nodes()/neighbors() only ever
    # return n.properties (see stores/falkordb/store.py: `RETURN n`/`RETURN e, m`
    # decode to .properties, never labels(n)), so query/traverse.py (which walks
    # role-gated transitions -- MessageConsumer/RouteHandler/TemporalWorkflow) has
    # no way to see a node's roles from a plain node dict without this explicit
    # mirror. Omitted entirely (not even []) when a node carries no roles, same
    # spirit as _omit_none: don't store a not-applicable field on kinds that never
    # have roles (Channel/BusinessProcess/Service) or role-less code nodes.
    if n.roles:
        props["roles"] = list(n.roles)
    return props


def _edge_props(e: EdgeRec) -> dict:
    core = {field: getattr(e, field) for field in _EDGE_CORE_FIELDS}
    return _omit_none({**core, **e.props})


# M3 T1: key_props per edge type -- passed through to store.upsert_edges (batch.py)
# so its MERGE pattern includes these props in the relationship key, not just (src,dst).
# NEXT_SEGMENT is the only type that currently needs one: linking/segments.py can derive
# TWO NEXT_SEGMENT edges between the SAME (src,dst) pair when a producer reaches the same
# downstream node via two DIFFERENT channels (see core/schema.py's SCHEMA_VERSION
# "2 -> 3" history comment for the staging-side half of this same fix) -- without
# via_channel_id in the graph MERGE key too, the second edge would silently overwrite the
# first in FalkorDB even after staging correctly kept both rows. Every other edge type is
# untouched (empty tuple, i.e. today's (src,dst)-only MERGE key, unchanged).
_KEY_PROPS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "NEXT_SEGMENT": ("via_channel_id",),
}


def _key_props_for(edge_type: str) -> tuple[str, ...]:
    return _KEY_PROPS_BY_TYPE.get(edge_type, ())


def _edge_row(e: EdgeRec) -> dict:
    """Row dict for store.upsert_edges: src/dst always, plus -- for a type listed in
    _KEY_PROPS_BY_TYPE -- that type's key_props promoted to the row's TOP level (read
    from e.props, defaulting absence to '' the same way staging.upsert_edges normalizes
    the via_channel PK column, so a row's key_props value is always present, never
    sometimes-missing). Top level, not just inside props, because batch.upsert_edges'
    MERGE pattern reads key_props as `r.<k>` -- a plain dict key lookup, not a nested
    r.props.<k> (see its own docstring). The value ALSO stays inside props (_edge_props
    already puts it there via **e.props) completely unchanged -- `SET e += r.props` is
    what actually persists it as a real, queryable graph edge property (e.g.
    query/traverse.py's `_resolve_exits` reads via_channel_id off the edge's own
    properties); the top-level copy exists ONLY to feed the MERGE key and is otherwise
    redundant with what's already in props."""
    row: dict = {"src": e.src, "dst": e.dst, "props": _edge_props(e)}
    for key in _key_props_for(e.type):
        row[key] = e.props.get(key, "")
    return row


def load_graph(
    staging: Staging,
    store_factory: Callable[[str], GraphStore],
    graph_name: str,
) -> dict:
    """staging -> `<graph_name>__build` (предварительно сброшенный) -> ensure_schema ->
    upsert (nodes then edges, grouped) -> swap_in в graph_name. Возврат -- счётчики
    для report.build_report."""
    build_name = f"{graph_name}__build"
    build_store = store_factory(build_name)
    # crash-recovery: снести возможный мусор от прогона, упавшего до swap_in
    # (см. модульный докстринг) -- ДО ensure_schema, чтобы схема легла на пустой граф
    build_store.delete_graph()
    build_store.ensure_schema()

    # -- 1. nodes: сгруппировать по labels-набору, попутно собрать known_ids --
    nodes_by_labels: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    known_ids: set[str] = set()
    for n in staging.iter_nodes():
        labels = _labels_for_kind(n.kind, n.roles)
        nodes_by_labels[labels].append({"id": n.id, "props": _node_props(n)})
        known_ids.add(n.id)

    nodes_written = 0
    nodes_written_by_label: dict[str, int] = {}
    for labels, rows in nodes_by_labels.items():
        written = build_store.upsert_nodes(labels, rows)
        nodes_written += written
        nodes_written_by_label[":".join(labels)] = written

    # -- 2. edges: сгруппировать по type; known_ids уже ПОЛНЫЙ (весь проход nodes
    # выше завершён до этой точки) --
    edges_by_type: dict[str, list[dict]] = defaultdict(list)
    for e in staging.iter_edges():
        edges_by_type[e.type].append(_edge_row(e))

    edges_written = 0
    edges_written_by_type: dict[str, int] = {}
    edges_dropped_by_type: dict[str, int] = {}
    for edge_type, rows in edges_by_type.items():
        written, dropped = build_store.upsert_edges(
            edge_type, rows, known_ids, key_props=_key_props_for(edge_type)
        )
        edges_written += written
        edges_written_by_type[edge_type] = written
        edges_dropped_by_type[edge_type] = dropped

    # -- 3. blue/green: final_store -- ОТДЕЛЬНЫЙ store с целевым именем; swap_in
    # вызывается НА НЁМ (см. модульный докстринг) --
    final_store = store_factory(graph_name)
    final_store.swap_in(build_name)

    return {
        "nodes_written": nodes_written,
        "nodes_written_by_label": nodes_written_by_label,
        "edges_written": edges_written,
        "edges_written_by_type": edges_written_by_type,
        "edges_dropped_missing_endpoint": sum(edges_dropped_by_type.values()),
        "edges_dropped_by_type": edges_dropped_by_type,
    }
