from codegraph.core.spans import LineIndex

SRC = 'x = 1\nname = "привет"\nz = "🌍ok"\n'.encode()


def test_ascii_line_utf8():
    li = LineIndex(SRC)
    assert li.to_byte(0, 4) == 4  # '1' в первой строке


def test_cyrillic_utf16_vs_utf8():
    li = LineIndex(SRC)
    line1_start = li.line_span(1)[0]
    # 'привет' начинается после 'name = "' (8 ascii-символов)
    assert li.to_byte(1, 8, "utf-16") == line1_start + 8
    # конец 'привет' (6 кириллических букв = 6 utf-16 units = 12 байт utf-8)
    assert li.to_byte(1, 14, "utf-16") == line1_start + 8 + 12
    assert li.to_byte(1, 20, "utf-8") == line1_start + 20  # utf-8 col = байты


def test_emoji_utf16_surrogate_pair():
    li = LineIndex(SRC)
    line2_start = li.line_span(2)[0]
    # '🌍' = 2 utf-16 units = 4 байта utf-8; 'z = "' = 5 ascii
    assert li.to_byte(2, 5 + 2, "utf-16") == line2_start + 5 + 4
    assert li.to_byte(2, 5 + 1, "utf-32") == line2_start + 5 + 4  # 1 кодпоинт


def test_clamp_beyond_line_end():
    li = LineIndex(SRC)
    s, e = li.line_span(0)
    assert li.to_byte(0, 999) == e


def test_line_count_and_last_line_without_newline():
    li = LineIndex(b"a\nbb")
    assert li.line_count == 2
    assert li.line_span(1) == (2, 4)
    assert li.to_byte(1, 2) == 4
