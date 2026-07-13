"""Конверсия позиций: SCIP (строка, колонка в code units кодировки) → байтовый оффсет.

SCIP-позиции 0-based; колонка считается в code units кодировки документа
(pyright обычно UTF-16 — критично для кириллицы/эмодзи). tree-sitter и staging
работают в байтах UTF-8 исходника.
"""

from __future__ import annotations


class LineIndex:
    def __init__(self, data: bytes):
        self._data = data
        self._starts = [0]
        for i, b in enumerate(data):
            if b == 0x0A:
                self._starts.append(i + 1)
        # если файл заканчивается \n, последний "старт" указывает за конец —
        # это пустая строка нулевой длины, line_span вернёт (len, len)

    @property
    def line_count(self) -> int:
        if self._data.endswith(b"\n"):
            return len(self._starts) - 1
        return len(self._starts)

    def line_span(self, line0: int) -> tuple[int, int]:
        start = self._starts[line0]
        if line0 + 1 < len(self._starts):
            end = self._starts[line0 + 1] - 1  # без \n
        else:
            end = len(self._data)
        return start, end

    def to_byte(self, line0: int, col: int, encoding: str = "utf-8") -> int:
        start, end = self.line_span(line0)
        if encoding == "utf-8":
            return min(start + col, end)
        text = self._data[start:end].decode("utf-8", errors="replace")
        units = 0
        byte_off = 0
        for ch in text:
            if units >= col:
                break
            units += 1 if encoding == "utf-32" else len(ch.encode("utf-16-le")) // 2
            byte_off += len(ch.encode("utf-8"))
        return min(start + byte_off, end)
