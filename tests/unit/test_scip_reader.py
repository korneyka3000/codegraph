from codegraph.resolvers.scip import scip_pb2
from codegraph.resolvers.scip.reader import read_scip_into_staging
from codegraph.stores.staging import Staging


def _occ(doc, symbol, rng, roles=0):
    o = doc.occurrences.add()
    o.symbol = symbol
    o.range.extend(rng)
    o.symbol_roles = roles


def _write_index(path, docs):
    idx = scip_pb2.Index()
    for d in docs:
        idx.documents.append(d)
    path.write_bytes(idx.SerializeToString())


def test_reader_utf8_and_ranges(tmp_path):
    (tmp_path / "m.py").write_bytes(b"def f():\n    g()\n")
    doc = scip_pb2.Document()
    doc.relative_path = "m.py"
    doc.position_encoding = scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart
    _occ(doc, "S_DEF_F", [0, 4, 5], roles=scip_pb2.SymbolRole.Definition)  # 'f'
    _occ(doc, "S_REF_G", [1, 4, 1, 5])  # 'g'
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])

    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(scip, "svc", tmp_path, st)
    assert (stats.documents, stats.defs, stats.refs) == (1, 1, 1)
    assert st.def_symbol_at("svc", "m.py", 4) == "S_DEF_F"
    ref = st.refs_for_file("svc", "m.py")[0]
    assert (ref.start_byte, ref.end_byte, ref.start_line) == (13, 14, 2)


def test_reader_utf16_cyrillic(tmp_path):
    src = '# привет\ndef ф():\n    pass\n'.encode()
    (tmp_path / "c.py").write_bytes(src)
    doc = scip_pb2.Document()
    doc.relative_path = "c.py"
    doc.position_encoding = scip_pb2.PositionEncoding.UTF16CodeUnitOffsetFromLineStart
    _occ(doc, "S_DEF_CYR", [1, 4, 5], roles=scip_pb2.SymbolRole.Definition)  # 'ф'
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])

    st = Staging(tmp_path / "s.db")
    read_scip_into_staging(scip, "svc", tmp_path, st)
    line1_start = src.index(b"def")
    assert st.def_symbol_at("svc", "c.py", line1_start + 4) == "S_DEF_CYR"


def test_reader_skips_missing_file(tmp_path):
    doc = scip_pb2.Document()
    doc.relative_path = "gone.py"
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])
    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(scip, "svc", tmp_path, st)
    assert stats.skipped_documents == 1


def test_reader_clamps_out_of_range_occurrence_line(tmp_path):
    """Controller amendment 1: LineIndex не валидирует границы line0 (plan-inherited
    gap) — reader обязан клампить line0 occurrence-а в [0, line_count) перед вызовом
    to_byte. SCIP — внешние данные; некорректная строка не должна ронять reader."""
    (tmp_path / "m.py").write_bytes(b"def f():\n    pass\n")  # ровно 2 строки
    doc = scip_pb2.Document()
    doc.relative_path = "m.py"
    doc.position_encoding = scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart
    _occ(doc, "S_OOB", [999, 0, 3])  # line далеко за пределами файла (line_count=2)
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])

    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(scip, "svc", tmp_path, st)  # не должно бросить исключение

    assert stats.refs == 1
    row = st.refs_for_file("svc", "m.py")[0]
    # клампленная 1-based строка: min(line, line_count-1)+1 = min(999, 2-1)+1 = 2
    assert row.start_line == 2


def test_reader_unspecified_encoding_defaults_to_utf16(tmp_path):
    """Amendment 2: scip.proto не предписывает семантику PositionEncoding=0
    (Unspecified) — комментарий лишь запрещает новым индексерам её эмитить "so that a
    consumer can process the SCIP index without ambiguity", т.е. сам протокол
    сознательно оставляет это на усмотрение консьюмера. Эмпирически провалено против
    реального пиннутого scip-python 0.6.6 (см. отчёт задачи): он всегда оставляет
    position_encoding=Unspecified, но фактически считает character в utf-16
    code units (ожидаемо: индексер на TS поверх pyright, а .proto прямо рекомендует
    UTF16CodeUnitOffsetFromLineStart для JS/TS-индексеров). Поэтому Unspecified
    маппится на "utf-16", а не "utf-8". Строка ниже не-ASCII специально ПОСЛЕ
    кириллицы на той же строке, чтобы utf-8-байтовый и utf-16-code-unit офсеты
    расходились (14 code units vs 20 байт) — тест ловит regression на "utf-8"-дефолт.
    """
    src = 'x = "привет"; y = x\n'.encode()
    (tmp_path / "m.py").write_bytes(src)
    doc = scip_pb2.Document()
    doc.relative_path = "m.py"
    # doc.position_encoding не выставляется -> остаётся UnspecifiedPositionEncoding (0)
    _occ(doc, "S_DEF_Y", [0, 14, 15], roles=scip_pb2.SymbolRole.Definition)  # 'y'
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])

    st = Staging(tmp_path / "s.db")
    read_scip_into_staging(scip, "svc", tmp_path, st)
    # occ character=14 (utf-16 units); correctly converted to byte offset 20 (verified
    # directly via LineIndex.to_byte). A buggy utf-8 default would instead give 14.
    assert st.def_symbol_at("svc", "m.py", 20) == "S_DEF_Y"


def test_reader_skips_malformed_range(tmp_path):
    (tmp_path / "m.py").write_bytes(b"x = 1\n")
    doc = scip_pb2.Document()
    doc.relative_path = "m.py"
    _occ(doc, "S_BAD", [5])  # длина 1 — малформ
    _occ(doc, "S_OK", [0, 0, 1])
    scip = tmp_path / "x.scip"
    _write_index(scip, [doc])
    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(scip, "svc", tmp_path, st)
    assert stats.malformed_ranges == 1 and stats.refs == 1
