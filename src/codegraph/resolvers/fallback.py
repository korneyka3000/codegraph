"""fallback.py: эвристический резолвер для degraded-сервисов (ScipRunError у scip-python).

Строит defs/refs БЕЗ SCIP, чисто структурно из tree-sitter `FileFacts`:

- Defs: КАЖДЫЙ `DefFact` (включая вложенные классы/функции) получает синтетический символ
  `f"scip-python python {service} 0.0 {структурный дескриптор}"`, где дескриптор строится
  через `ids.structural_descriptor` по полной цепочке родителей (`_nesting` из
  `python_core` — тот же алгоритм, что и у static-резолвера, поэтому формат совпадает с
  parse_symbol/symbol_to_node_id и с id, которые построил бы python_core extractor).
  Байтовый span — name-token (`name_start_byte:name_end_byte`), не весь def/class.

- Refs — только к TOP-LEVEL def'ам (эвристика намеренно не лезет во вложенные скоупы):
  (a) вызов имени, которое является top-level def'ом В ЭТОМ ЖЕ файле;
  (b) вызов имени, импортированного `from X import name`, где X после normalize
      (`ids.relpath_to_module`) есть ключ в `facts_by_file` и содержит top-level def
      с этим именем.
  Приоритет (a) над (b) при коллизии имён (shadowing).
  Всё остальное — включая относительные импорты (`target_module` начинается с ".": пакет
  сервиса тут не участвует, надёжно резолвить их на этом уровне нельзя — вне эвристического
  scope) и вызовы, для которых имя не нашлось нигде, — НЕ резолвится: ref не создаётся,
  ничего не угадывается.

Чистая функция, без зависимости от staging: вызывающая сторона (M1b) сама кладёт
def-/ref-строки в staging и запускает `extractors.calls.build_calls` с
resolution="heuristic", confidence=0.6.
"""

from __future__ import annotations

from codegraph.core import ids
from codegraph.extractors.python_core import _nesting
from codegraph.parsing.facts import DefFact, FileFacts
from codegraph.resolvers.base import DefRow, RefRow


def _symbol(service: str, module_dotted: str, nesting: list[tuple[str, str]]) -> str:
    descriptors = ids.structural_descriptor(module_dotted, nesting)
    return f"scip-python python {service} 0.0 {descriptors}"


def resolve_service(
    service: str,
    files: dict[str, bytes],
    facts_by_file: dict[str, FileFacts],
) -> tuple[list[DefRow], list[RefRow]]:
    del files  # байты не нужны: все спаны/имена уже есть в facts_by_file

    # module_dotted -> relpath, для резолвинга target_module из from-импортов (b).
    module_to_relpath = {ids.relpath_to_module(rp): rp for rp in facts_by_file}

    defs: list[DefRow] = []
    # per-file индексы, нужны на втором проходе (refs могут ссылаться на defs другого файла).
    top_level_by_file: dict[str, dict[str, DefFact]] = {}
    symbol_by_def_index: dict[str, dict[int, str]] = {}

    for relpath, facts in facts_by_file.items():
        module_dotted = ids.relpath_to_module(relpath)
        top_level: dict[str, DefFact] = {}
        sym_by_index: dict[int, str] = {}
        for d in facts.defs:
            sym = _symbol(service, module_dotted, _nesting(facts.defs, d))
            sym_by_index[d.index] = sym
            defs.append(DefRow(relpath, sym, d.name_start_byte, d.name_end_byte, d.start_line))
            if d.parent is None:
                top_level[d.name] = d
        top_level_by_file[relpath] = top_level
        symbol_by_def_index[relpath] = sym_by_index

    refs: list[RefRow] = []
    for relpath, facts in facts_by_file.items():
        top_level = top_level_by_file[relpath]

        # imported name -> target module (только from-импорты; имена — pre-alias, см. Task 8).
        imported_from: dict[str, str] = {}
        for imp in facts.imports:
            if not imp.names or imp.target_module.startswith("."):
                continue  # относительный импорт — вне эвристического scope
            for name in imp.names:
                imported_from[name] = imp.target_module

        for call in facts.calls:
            name = call.callee_name

            target_relpath = relpath
            target_def = top_level.get(name)  # (a) same-file top-level def

            if target_def is None:  # (b) from-import -> top-level def в другом файле
                target_module = imported_from.get(name)
                if target_module is not None:
                    target_relpath = module_to_relpath.get(target_module)
                    if target_relpath is not None:
                        target_def = top_level_by_file[target_relpath].get(name)

            if target_def is None:
                continue  # нерезолвлено — пропускаем, не гадаем

            sym = symbol_by_def_index[target_relpath][target_def.index]
            refs.append(RefRow(
                relpath, sym, call.callee_start_byte, call.callee_end_byte,
                call.start_line, 0,
            ))

    return defs, refs
