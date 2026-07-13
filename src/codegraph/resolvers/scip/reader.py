"""SCIP protobuf → staging: occurrences документа конвертируются в DefRow/RefRow.

Document.relative_path резолвится относительно service_root и читается с диска (не
Document.text — оно почти всегда пустое, индексеры его не заполняют). Позиции
occurrence заданы в 0-based code units кодировки документа (position_encoding);
конвертация в байтовый оффсет utf-8-исходника делегирована LineIndex.to_byte.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from codegraph.core.spans import LineIndex
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.resolvers.scip import scip_pb2
from codegraph.stores.staging import Staging

# PositionEncoding=0 (UnspecifiedPositionEncoding): scip.proto комментирует его как
# "Default value. This value should not be used by new SCIP indexers so that a
# consumer can process the SCIP index without ambiguity" — спецификация сознательно
# НЕ определяет, что "unspecified" означает для консьюмера; она лишь просит
# производителей не эмитить 0. Изначальная гипотеза (utf-8) была эмпирически
# ОПРОВЕРГНУТА: реальный пиннутый scip-python 0.6.6 на фикстуре document_management
# оставляет position_encoding=Unspecified на КАЖДОМ документе (8/8), но фактические
# character-офсеты occurrences — utf-16 code units, не utf-8 байты (провалено ручным
# non-ASCII пробником: символ после кириллицы на той же строке резолвился по
# utf-16-семантике). Это согласуется с рекомендацией самого scip.proto для
# Document.position_encoding: "For an indexer implemented in ... JavaScript/
# TypeScript, use UTF16CodeUnitOffsetFromLineStart" — scip-python обёрнут вокруг
# pyright (TS), унаследовавшего LSP-конвенцию UTF-16, но, судя по всему, не
# проставляет сам enum. Поэтому Unspecified маппится на "utf-16" (подробности и
# воспроизведение — в отчёте задачи), а не на "utf-8".
_ENC = {
    scip_pb2.PositionEncoding.UnspecifiedPositionEncoding: "utf-16",
    scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart: "utf-8",
    scip_pb2.PositionEncoding.UTF16CodeUnitOffsetFromLineStart: "utf-16",
    scip_pb2.PositionEncoding.UTF32CodeUnitOffsetFromLineStart: "utf-32",
}


@dataclass(frozen=True)
class ReaderStats:
    documents: int
    defs: int
    refs: int
    skipped_documents: int


def _clamp_line(line0: int, li: LineIndex) -> int:
    """LineIndex не валидирует границы (plan-inherited gap) — SCIP это внешние
    данные, malformed occurrence не должна ронять reader."""
    return max(0, min(line0, li.line_count - 1))


def _normalize_range(occ: scip_pb2.Occurrence) -> tuple[int, int, int, int]:
    r = list(occ.range)
    if len(r) == 3:
        return r[0], r[1], r[0], r[2]
    return r[0], r[1], r[2], r[3]


def read_scip_into_staging(
    scip_path: Path, service: str, service_root: Path, staging: Staging
) -> ReaderStats:
    idx = scip_pb2.Index()
    idx.ParseFromString(scip_path.read_bytes())

    documents = 0
    total_defs = 0
    total_refs = 0
    skipped_documents = 0

    for doc in idx.documents:
        documents += 1
        try:
            data = (service_root / doc.relative_path).read_bytes()
        except OSError:
            skipped_documents += 1
            continue

        li = LineIndex(data)
        encoding = _ENC.get(doc.position_encoding, "utf-8")

        defs: list[DefRow] = []
        refs: list[RefRow] = []
        for occ in doc.occurrences:
            sl, sc, el, ec = _normalize_range(occ)
            sl_clamped = _clamp_line(sl, li)
            start_byte = li.to_byte(sl_clamped, sc, encoding)
            end_byte = li.to_byte(_clamp_line(el, li), ec, encoding)
            start_line = sl_clamped + 1
            roles = occ.symbol_roles

            if roles & scip_pb2.SymbolRole.Definition:
                defs.append(DefRow(doc.relative_path, occ.symbol, start_byte,
                                    end_byte, start_line))
            else:
                refs.append(RefRow(doc.relative_path, occ.symbol, start_byte,
                                    end_byte, start_line, roles))

        if defs:
            staging.add_defs(service, defs)
        if refs:
            staging.add_refs(service, refs)
        total_defs += len(defs)
        total_refs += len(refs)

    return ReaderStats(documents=documents, defs=total_defs, refs=total_refs,
                        skipped_documents=skipped_documents)
