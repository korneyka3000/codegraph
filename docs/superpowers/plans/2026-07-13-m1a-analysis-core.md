# M1a: Analysis Core (SCIP + tree-sitter + staging + CALLS join) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) или superpowers:executing-plans. Часть 1 вехи M1; часть 2 (M1b: FalkorDB store, pipeline+CLI, MCP v0, eval-гейт) — отдельный план.

**Goal:** Аналитическое ядро `codegraph`: скан сервиса → scip-python → чтение SCIP в staging (SQLite) → tree-sitter FileFacts → узлы Module/Class/Function + CONTAINS/IMPORTS → span-join SCIP-ссылок с call-sites → CALLS-рёбра в staging. Плюс долги финального ревью M0.

**Architecture:** Все стадии — чистые функции над `Staging` (SQLite в `.codegraph/`). ID узлов сходятся между экстрактором и join'ом через политику «scip-def-lookup first, structural fallback». Позиции: SCIP (line, col в code units кодировки) конвертируются в байтовые оффсеты tree-sitter через `LineIndex`. FalkorDB в M1a НЕ трогаем.

**Tech Stack:** + tree-sitter (>=0.23), tree-sitter-python (>=0.23), pathspec (>=0.12). Без Query API tree-sitter — только ручной обход узлов (стабильность между версиями биндингов).

## Global Constraints

- Коммиты: `feat(m1a): …` / `test(m1a): …` / `chore(m1a): …`; коммит после каждой задачи.
- Все `*_line` в staging и IR — **1-based**; все `*_byte` — байтовые оффсеты от начала файла.
- ID кодового узла: `sym:<service>:<scip-descriptors>` (дескрипторы БЕЗ package/version); SCIP `local N` → `sym:<service>:<relpath>:local<N>`. Узел и ребро обязаны получать ОДИН и тот же id для одного символа: политика — сначала lookup scip-def по span имени, потом структурный фоллбек.
- На каждом ребре: `resolution ∈ {static,dynamic,heuristic,trace_validated}` + `confidence`. S6-CALLS от scip: static/1.0; от fallback-резолвера: heuristic/0.6.
- Кросс-сервисные code→code рёбра запрещены — `Staging.upsert_edges` валидирует и кидает `InvariantError`.
- tree-sitter: НИКАКИХ `Query` — только `node.type` / `node.children` / `node.child_by_field_name` / `node.text`.
- pytest-сводки на этой машине искажает rtk-хук: читать через `uv run python -m pytest … > /tmp/x.txt 2>&1; cat /tmp/x.txt`.
- Интеграционные тесты, требующие node/npx + сети (реальный scip-python): маркер `scip` (зарегистрировать). FalkorDB-маркер не нужен в M1a.
- YAGNI: ничего для M1b/M2 сверх заявленных заготовок (декораторы и строковые аргументы в FileFacts — собираем, но не потребляем).

---

### Task 1: Долги M0 + зависимости M1a

**Files:**
- Create: `src/codegraph/constants.py`, `src/codegraph/pipeline/__init__.py`, `src/codegraph/pipeline/stages.py`
- Modify: `pyproject.toml` (deps + marker), `src/codegraph/doctor.py` (импорт константы), `src/codegraph/cli.py` (ConfigError-boundary, STAGES из pipeline)
- Test: `tests/unit/test_cli_errors.py`

**Interfaces:**
- Produces: `codegraph.constants.SCIP_PYTHON_VERSION = "0.6.6"`; `codegraph.pipeline.stages.STAGES: list[tuple[str, str, str]]` (перенос из cli.py, содержимое неизменно); `codegraph.cli._load(target: Path) -> WorkspaceConfig` — оборачивает `load_workspace`, при `ConfigError` печатает `[red]config error:[/] <msg>` и `raise typer.Exit(1)`.

- [ ] **Step 1: Падающий тест**

`tests/unit/test_cli_errors.py`:
```python
from typer.testing import CliRunner

from codegraph.cli import app

runner = CliRunner()


def test_index_bad_target_prints_one_liner_not_traceback(tmp_path):
    cfg = tmp_path / "codegraph.yaml"
    cfg.write_text("version: 1\ngraph_name: x\nservices:\n  - name: a\n    path: ./missing\n")
    result = runner.invoke(app, ["index", str(cfg), "--dry-run"])
    assert result.exit_code == 1
    assert "config error" in result.output
    assert "Traceback" not in result.output


def test_doctor_bad_config_prints_one_liner(tmp_path):
    cfg = tmp_path / "codegraph.yaml"
    cfg.write_text("version: 1\ngraph_name: x\nservices:\n  - name: a\n    path: ./missing\n")
    result = runner.invoke(app, ["doctor", "--config", str(cfg), "--skip-store"])
    assert result.exit_code == 1
    assert "config error" in result.output


def test_stages_moved_to_pipeline():
    from codegraph.pipeline.stages import STAGES

    assert STAGES[0][0] == "S1" and STAGES[-1][0] == "S10"


def test_scip_version_single_source():
    from codegraph import constants, doctor

    assert doctor.SCIP_PYTHON_VERSION == constants.SCIP_PYTHON_VERSION
```

- [ ] **Step 2: RED** — `uv run python -m pytest tests/unit/test_cli_errors.py > /tmp/t1.txt 2>&1; cat /tmp/t1.txt` → ImportError/FAIL.

- [ ] **Step 3: Реализация**

`src/codegraph/constants.py`:
```python
"""Общие пины версий (единственный источник — импортировать отсюда)."""

SCIP_PYTHON_VERSION = "0.6.6"  # npm @sourcegraph/scip-python; двигать вместе с doctor --probe-scip
```

`src/codegraph/pipeline/__init__.py` — пустой. `src/codegraph/pipeline/stages.py` — перенести список STAGES из cli.py без изменений содержимого, с докстрингом «Реестр стадий пайплайна; функции стадий добавятся в M1b».

`src/codegraph/doctor.py`: заменить локальную константу на `from codegraph.constants import SCIP_PYTHON_VERSION` (строку `SCIP_PYTHON_VERSION = "0.6.6"` удалить; сохранить публичное имя `doctor.SCIP_PYTHON_VERSION` через импорт).

`src/codegraph/cli.py`:
```python
from codegraph.config.loader import ConfigError, load_workspace
from codegraph.pipeline.stages import STAGES


def _load(target: Path) -> "WorkspaceConfig":
    try:
        return load_workspace(target)
    except ConfigError as e:
        console.print(f"[red]config error:[/] {e}")
        raise typer.Exit(1) from e
```
Использовать `_load` в `doctor` и `index` вместо прямого `load_workspace`; локальное определение STAGES удалить.

`pyproject.toml`: в `dependencies` добавить `"tree-sitter>=0.23"`, `"tree-sitter-python>=0.23"`, `"pathspec>=0.12"`; в `[tool.pytest.ini_options] markers` добавить `"scip: needs node/npx and network (runs real scip-python)"`.

- [ ] **Step 4: GREEN + полный прогон** — `uv sync`, затем `uv run python -m pytest > /tmp/t1full.txt 2>&1; cat /tmp/t1full.txt` (все + 4 новых), `uv run ruff check .`.

- [ ] **Step 5: Commit** — `chore(m1a): m0 review debts (ConfigError boundary, shared constants, stages module) + m1a deps`

---

### Task 2: core/schema.py + core/errors.py + core/ids.py

**Files:**
- Create: `src/codegraph/core/__init__.py`, `src/codegraph/core/schema.py`, `src/codegraph/core/errors.py`, `src/codegraph/core/ids.py`
- Test: `tests/unit/test_core_schema.py`, `tests/unit/test_core_ids.py`

**Interfaces (контракты для ВСЕХ последующих задач):**
- `schema.NodeRec(id, kind, service, name, qualified_name, relpath=None, start_byte=None, end_byte=None, start_line=None, end_line=None, content_hash=None, props={})` — frozen dataclass; `schema.EdgeRec(src, dst, type, resolution, confidence, extractor, evidence_file=None, evidence_line=None, props={})` — frozen dataclass.
- `schema.NODE_KINDS = {"Service","Module","Class","Function"}`, `schema.EDGE_TYPES = {"CONTAINS","IMPORTS","CALLS"}`, `schema.RESOLUTIONS = {...4}`, `schema.SCHEMA_VERSION = 1`.
- `schema.make_service_node(service) -> NodeRec` (id=`svc:<service>`, kind="Service").
- `errors.CodegraphError(Exception)`; `errors.InvariantError(CodegraphError)`; `errors.ServiceDegraded(CodegraphError)` (поля service, reason).
- `ids.relpath_to_module("app/routes/orders.py") == "app.routes.orders"`; `"app/__init__.py" → "app"`.
- `ids.module_descriptor("app.db") == "`app.db`/"`; `ids.structural_descriptor(module, nesting)` где nesting=[("class","OrderService"),("function","place")] → `` `app.services.order`/OrderService#place(). `` (class → `Name#`, function → `name().`).
- `ids.node_id(service, descriptors) -> f"sym:{service}:{descriptors}"`; `ids.local_id(service, relpath, local) -> f"sym:{service}:{relpath}:{local}"` (local вида "local 3" нормализуется в "local3").
- `ids.display_qualified(descriptors) -> str` — `` `app.mod`/Cls#meth(). `` → `app.mod.Cls.meth` (backticks убрать, `/`→`.`, `#`→`.`, `().`→пусто, финальные точки убрать).

- [ ] **Step 1: Падающие тесты**

`tests/unit/test_core_ids.py`:
```python
from codegraph.core import ids


def test_relpath_to_module():
    assert ids.relpath_to_module("app/routes/orders.py") == "app.routes.orders"
    assert ids.relpath_to_module("app/__init__.py") == "app"
    assert ids.relpath_to_module("main.py") == "main"


def test_structural_descriptor_and_id():
    d = ids.structural_descriptor(
        "app.services.order", [("class", "OrderService"), ("function", "place")]
    )
    assert d == "`app.services.order`/OrderService#place()."
    assert ids.node_id("orders-api", d) == "sym:orders-api:`app.services.order`/OrderService#place()."


def test_module_descriptor_matches_structural_with_empty_nesting():
    assert ids.module_descriptor("app.db") == ids.structural_descriptor("app.db", [])


def test_local_id_normalized():
    assert ids.local_id("svc", "app/x.py", "local 3") == "sym:svc:app/x.py:local3"


def test_display_qualified():
    assert ids.display_qualified("`app.mod`/Cls#meth().") == "app.mod.Cls.meth"
    assert ids.display_qualified("`app.mod`/fn().") == "app.mod.fn"
    assert ids.display_qualified("`app.mod`/") == "app.mod"
```

`tests/unit/test_core_schema.py`:
```python
import dataclasses

import pytest

from codegraph.core.schema import EdgeRec, NodeRec, make_service_node


def test_node_rec_frozen():
    n = NodeRec(id="x", kind="Function", service="s", name="f", qualified_name="m.f")
    with pytest.raises(dataclasses.FrozenInstanceError):
        n.id = "y"


def test_make_service_node():
    n = make_service_node("orders-api")
    assert n.id == "svc:orders-api" and n.kind == "Service"


def test_edge_defaults():
    e = EdgeRec(src="a", dst="b", type="CALLS", resolution="static",
                confidence=1.0, extractor="calls")
    assert e.props == {} and e.evidence_line is None
```

- [ ] **Step 2: RED** (ModuleNotFoundError)

- [ ] **Step 3: Реализация**

`core/errors.py`:
```python
class CodegraphError(Exception):
    pass


class InvariantError(CodegraphError):
    pass


class ServiceDegraded(CodegraphError):
    def __init__(self, service: str, reason: str):
        self.service = service
        self.reason = reason
        super().__init__(f"service {service!r} degraded: {reason}")
```

`core/schema.py`:
```python
"""IR узлов/рёбер и константы схемы. Единый словарь для staging, load и eval."""

from __future__ import annotations

from dataclasses import dataclass, field

SCHEMA_VERSION = 1
NODE_KINDS = frozenset({"Service", "Module", "Class", "Function"})
EDGE_TYPES = frozenset({"CONTAINS", "IMPORTS", "CALLS"})
RESOLUTIONS = frozenset({"static", "dynamic", "heuristic", "trace_validated"})


@dataclass(frozen=True)
class NodeRec:
    id: str
    kind: str
    service: str
    name: str
    qualified_name: str
    relpath: str | None = None
    start_byte: int | None = None
    end_byte: int | None = None
    start_line: int | None = None  # 1-based
    end_line: int | None = None
    content_hash: str | None = None
    props: dict = field(default_factory=dict)


@dataclass(frozen=True)
class EdgeRec:
    src: str
    dst: str
    type: str
    resolution: str
    confidence: float
    extractor: str
    evidence_file: str | None = None
    evidence_line: int | None = None  # 1-based
    props: dict = field(default_factory=dict)


def make_service_node(service: str) -> NodeRec:
    return NodeRec(
        id=f"svc:{service}", kind="Service", service=service,
        name=service, qualified_name=service,
    )
```

`core/ids.py`:
```python
"""Стабильные ID узлов: sym:<service>:<scip-descriptors>.

Дескрипторы совпадают с форматом scip-python (без package/version), поэтому
id, построенный экстрактором структурно, равен id, выведенному из SCIP-символа.
"""

from __future__ import annotations


def relpath_to_module(relpath: str) -> str:
    p = relpath[:-3] if relpath.endswith(".py") else relpath
    if p.endswith("/__init__"):
        p = p[: -len("/__init__")]
    return p.replace("/", ".")


def module_descriptor(module_dotted: str) -> str:
    return f"`{module_dotted}`/"


def structural_descriptor(module_dotted: str, nesting: list[tuple[str, str]]) -> str:
    d = module_descriptor(module_dotted)
    for kind, name in nesting:
        d += f"{name}#" if kind == "class" else f"{name}()."
    return d


def node_id(service: str, descriptors: str) -> str:
    return f"sym:{service}:{descriptors}"


def local_id(service: str, relpath: str, local: str) -> str:
    return f"sym:{service}:{relpath}:{local.replace(' ', '')}"


def display_qualified(descriptors: str) -> str:
    s = descriptors.replace("`", "").replace("().", ".").replace("#", ".").replace("/", ".")
    return s.strip(".")
```

- [ ] **Step 4: GREEN + ruff** — обе группы тестов, полный прогон через редирект.

- [ ] **Step 5: Commit** — `feat(m1a): core IR (NodeRec/EdgeRec), errors, stable id builders`

---

### Task 3: core/spans.py — конверсия позиций SCIP ↔ байты

**Files:**
- Create: `src/codegraph/core/spans.py`
- Test: `tests/unit/test_core_spans.py`

**Interfaces:**
- `spans.LineIndex(data: bytes)`; `.to_byte(line0: int, col: int, encoding: str = "utf-8") -> int` — line0 **0-based** (как в SCIP), col в code units кодировки (`"utf-8"`=байты, `"utf-16"`=UTF-16 code units, `"utf-32"`=кодпоинты); возвращает байтовый оффсет, клампится в границы строки; `.line_count`; `.line_span(line0) -> tuple[int, int]` (байтовые границы строки без учёта \n... включая содержимое до \n).

- [ ] **Step 1: Падающие тесты**

`tests/unit/test_core_spans.py`:
```python
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
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация**

`core/spans.py`:
```python
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
```

- [ ] **Step 4: GREEN + ruff**

- [ ] **Step 5: Commit** — `feat(m1a): LineIndex position conversion (utf-8/16/32 code units to bytes)`

---

### Task 4: resolvers/base.py + SCIP symbol parser

**Files:**
- Create: `src/codegraph/resolvers/base.py`, `src/codegraph/resolvers/scip/symbols.py`
- Test: `tests/unit/test_scip_symbols.py`

**Interfaces:**
- `base.DefRow(relpath, symbol, start_byte, end_byte, start_line)`, `base.RefRow(relpath, symbol, start_byte, end_byte, start_line, roles)` — frozen dataclasses (staging-строки).
- `symbols.ParsedSymbol` — frozen: `is_local: bool`, `local: str | None` (вида "local 3"), `scheme/manager/package/version: str | None`, `descriptors: str | None`.
- `symbols.parse_symbol(sym: str) -> ParsedSymbol`: `"local N"` → local; иначе `sym.split(" ", 4)` → 5 полей; меньше 5 полей → `ValueError`.
- `symbols.symbol_to_node_id(service: str, relpath: str, sym: str) -> str` — local → `ids.local_id`, иначе `ids.node_id(service, descriptors)`.

- [ ] **Step 1: Падающие тесты**

`tests/unit/test_scip_symbols.py`:
```python
import pytest

from codegraph.resolvers.scip.symbols import parse_symbol, symbol_to_node_id

SYM = "scip-python python orders-api 0.1 `app.services.order`/OrderService#place()."


def test_parse_global_symbol():
    p = parse_symbol(SYM)
    assert not p.is_local
    assert p.scheme == "scip-python" and p.manager == "python"
    assert p.package == "orders-api" and p.version == "0.1"
    assert p.descriptors == "`app.services.order`/OrderService#place()."


def test_parse_local_symbol():
    p = parse_symbol("local 42")
    assert p.is_local and p.local == "local 42" and p.descriptors is None


def test_symbol_to_node_id_global_and_local():
    assert (
        symbol_to_node_id("orders-api", "app/services/order.py", SYM)
        == "sym:orders-api:`app.services.order`/OrderService#place()."
    )
    assert (
        symbol_to_node_id("orders-api", "app/x.py", "local 7")
        == "sym:orders-api:app/x.py:local7"
    )


def test_malformed_symbol_raises():
    with pytest.raises(ValueError):
        parse_symbol("garbage without enough fields")
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация**

`resolvers/base.py`:
```python
"""Типы строк резолвера: то, что кладётся в staging-таблицы scip_defs/scip_refs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DefRow:
    relpath: str
    symbol: str
    start_byte: int
    end_byte: int
    start_line: int  # 1-based


@dataclass(frozen=True)
class RefRow:
    relpath: str
    symbol: str
    start_byte: int
    end_byte: int
    start_line: int  # 1-based
    roles: int
```

`resolvers/scip/symbols.py`:
```python
"""Разбор SCIP-symbol-строк: '<scheme> <manager> <package> <version> <descriptors>' | 'local N'."""

from __future__ import annotations

from dataclasses import dataclass

from codegraph.core import ids


@dataclass(frozen=True)
class ParsedSymbol:
    is_local: bool
    local: str | None = None
    scheme: str | None = None
    manager: str | None = None
    package: str | None = None
    version: str | None = None
    descriptors: str | None = None


def parse_symbol(sym: str) -> ParsedSymbol:
    if sym.startswith("local "):
        return ParsedSymbol(is_local=True, local=sym)
    parts = sym.split(" ", 4)
    if len(parts) != 5:
        raise ValueError(f"malformed SCIP symbol: {sym!r}")
    scheme, manager, package, version, descriptors = parts
    return ParsedSymbol(
        is_local=False, scheme=scheme, manager=manager,
        package=package, version=version, descriptors=descriptors,
    )


def symbol_to_node_id(service: str, relpath: str, sym: str) -> str:
    p = parse_symbol(sym)
    if p.is_local:
        return ids.local_id(service, relpath, p.local)
    return ids.node_id(service, p.descriptors)
```

- [ ] **Step 4: GREEN + ruff**

- [ ] **Step 5: Commit** — `feat(m1a): resolver row types and SCIP symbol parser`

---

### Task 5: stores/staging.py — SQLite staging

**Files:**
- Create: `src/codegraph/stores/staging.py`
- Test: `tests/unit/test_staging.py`

**Interfaces:**
- `Staging(path: Path)` — открывает/создаёт SQLite (`ensure_schema()` идемпотентен; WAL). Контекст-менеджер (`close()`).
- `begin_service(service)` — удаляет ВСЕ строки этого сервиса из files/scip_defs/scip_refs/nodes/edges (идемпотентная переиндексация).
- `add_files(service, rows: list[tuple[relpath, sha256, size]])`.
- `add_defs(service, rows: list[DefRow])`, `add_refs(service, rows: list[RefRow])`.
- `upsert_nodes(rows: list[NodeRec])` (INSERT OR REPLACE; labels: json-список [kind] — роли добавятся в M2; props — json).
- `upsert_edges(rows: list[EdgeRec])` — определяет сервис src/dst по префиксу id (`sym:<svc>:`/`svc:<svc>`); если оба кодовые (`sym:`) и сервисы разные → `InvariantError`. PK (src,dst,type) REPLACE.
- `def_symbol_at(service, relpath, start_byte) -> str | None` — символ def-occurrence, чей span начинается ровно в start_byte.
- `refs_for_file(service, relpath) -> list[RefRow]` (отсортированы по start_byte).
- `files_for_service(service) -> list[tuple[relpath, sha256]]`; `module_set(service) -> set[str]` (dotted-модули из files через `ids.relpath_to_module`).
- `counts() -> dict` (files/defs/refs/nodes/edges); `iter_nodes() / iter_edges()` — для M1b-загрузки; `set_meta(k, v) / get_meta(k)`.

- [ ] **Step 1: Падающие тесты**

`tests/unit/test_staging.py`:
```python
import pytest

from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.stores.staging import Staging


def _node(id_, svc, kind="Function"):
    return NodeRec(id=id_, kind=kind, service=svc, name="n", qualified_name="q")


def test_roundtrip_and_counts(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_files("a", [("app/x.py", "abc", 10)])
    st.add_defs("a", [DefRow("app/x.py", "local 1", 5, 8, 1)])
    st.add_refs("a", [RefRow("app/x.py", "local 1", 20, 23, 2, 0)])
    st.upsert_nodes([_node("sym:a:`app.x`/f().", "a")])
    st.upsert_edges([EdgeRec("sym:a:`app.x`/f().", "sym:a:`app.x`/g().", "CALLS",
                             "static", 1.0, "calls")])
    c = st.counts()
    assert (c["files"], c["defs"], c["refs"], c["nodes"], c["edges"]) == (1, 1, 1, 1, 1)


def test_begin_service_wipes_only_that_service(tmp_path):
    st = Staging(tmp_path / "s.db")
    for svc in ("a", "b"):
        st.begin_service(svc)
        st.add_files(svc, [("m.py", "h", 1)])
    st.begin_service("a")
    assert st.files_for_service("a") == []
    assert st.files_for_service("b") == [("m.py", "h")]


def test_def_symbol_at_and_refs_sorted(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_defs("a", [DefRow("m.py", "SYM_F", 100, 103, 5)])
    st.add_refs("a", [RefRow("m.py", "R2", 50, 52, 3, 0), RefRow("m.py", "R1", 10, 12, 1, 0)])
    assert st.def_symbol_at("a", "m.py", 100) == "SYM_F"
    assert st.def_symbol_at("a", "m.py", 99) is None
    assert [r.symbol for r in st.refs_for_file("a", "m.py")] == ["R1", "R2"]


def test_cross_service_code_edge_forbidden(tmp_path):
    st = Staging(tmp_path / "s.db")
    with pytest.raises(InvariantError):
        st.upsert_edges([EdgeRec("sym:a:`m`/f().", "sym:b:`m`/g().", "CALLS",
                                 "static", 1.0, "calls")])


def test_edge_replace_on_pk(tmp_path):
    st = Staging(tmp_path / "s.db")
    e1 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0,
                 "calls", props={"callsite_count": 1})
    e2 = EdgeRec("sym:a:`m`/f().", "sym:a:`m`/g().", "CALLS", "static", 1.0,
                 "calls", props={"callsite_count": 3})
    st.upsert_edges([e1])
    st.upsert_edges([e2])
    edges = list(st.iter_edges())
    assert len(edges) == 1 and edges[0].props["callsite_count"] == 3


def test_module_set(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("a")
    st.add_files("a", [("app/__init__.py", "h", 1), ("app/db/outbox.py", "h", 1)])
    assert st.module_set("a") == {"app", "app.db.outbox"}


def test_meta(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.set_meta("schema_version", "1")
    assert st.get_meta("schema_version") == "1"
    assert st.get_meta("nope") is None
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** (`stores/staging.py`, sqlite3 из stdlib)

```python
"""SQLite-staging: промежуточное состояние пайплайна. FalkorDB пересоздаваем отсюда."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from codegraph.core import ids
from codegraph.core.errors import InvariantError
from codegraph.core.schema import EdgeRec, NodeRec
from codegraph.resolvers.base import DefRow, RefRow

_DDL = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS files(
  service TEXT, relpath TEXT, sha256 TEXT, size INTEGER,
  PRIMARY KEY(service, relpath));
CREATE TABLE IF NOT EXISTS scip_defs(
  service TEXT, relpath TEXT, symbol TEXT,
  start_byte INTEGER, end_byte INTEGER, start_line INTEGER,
  PRIMARY KEY(service, relpath, start_byte, symbol));
CREATE TABLE IF NOT EXISTS scip_refs(
  service TEXT, relpath TEXT, symbol TEXT,
  start_byte INTEGER, end_byte INTEGER, start_line INTEGER, roles INTEGER,
  PRIMARY KEY(service, relpath, start_byte, symbol));
CREATE INDEX IF NOT EXISTS idx_refs_file ON scip_refs(service, relpath, start_byte);
CREATE INDEX IF NOT EXISTS idx_defs_at ON scip_defs(service, relpath, start_byte);
CREATE TABLE IF NOT EXISTS nodes(
  id TEXT PRIMARY KEY, kind TEXT, labels TEXT, service TEXT,
  relpath TEXT, start_byte INTEGER, end_byte INTEGER,
  start_line INTEGER, end_line INTEGER,
  name TEXT, qualified_name TEXT, content_hash TEXT, props TEXT);
CREATE INDEX IF NOT EXISTS idx_nodes_service ON nodes(service);
CREATE TABLE IF NOT EXISTS edges(
  src TEXT, dst TEXT, type TEXT, resolution TEXT, confidence REAL,
  extractor TEXT, evidence_file TEXT, evidence_line INTEGER, props TEXT,
  src_service TEXT,
  PRIMARY KEY(src, dst, type));
CREATE INDEX IF NOT EXISTS idx_edges_service ON edges(src_service);
"""


def _id_service(node_id: str) -> str | None:
    if node_id.startswith("sym:"):
        return node_id.split(":", 2)[1]
    if node_id.startswith("svc:"):
        return node_id.split(":", 1)[1]
    return None


class Staging:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self._db.executescript(_DDL)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- запись --

    def begin_service(self, service: str) -> None:
        cur = self._db
        for t in ("files", "scip_defs", "scip_refs"):
            cur.execute(f"DELETE FROM {t} WHERE service=?", (service,))  # noqa: S608
        cur.execute("DELETE FROM nodes WHERE service=?", (service,))
        cur.execute("DELETE FROM edges WHERE src_service=?", (service,))
        self._db.commit()

    def add_files(self, service: str, rows: list[tuple[str, str, int]]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO files VALUES (?,?,?,?)",
            [(service, r, h, s) for r, h, s in rows],
        )
        self._db.commit()

    def add_defs(self, service: str, rows: list[DefRow]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO scip_defs VALUES (?,?,?,?,?,?)",
            [(service, d.relpath, d.symbol, d.start_byte, d.end_byte, d.start_line)
             for d in rows],
        )
        self._db.commit()

    def add_refs(self, service: str, rows: list[RefRow]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO scip_refs VALUES (?,?,?,?,?,?,?)",
            [(service, r.relpath, r.symbol, r.start_byte, r.end_byte, r.start_line,
              r.roles) for r in rows],
        )
        self._db.commit()

    def upsert_nodes(self, rows: list[NodeRec]) -> None:
        self._db.executemany(
            "INSERT OR REPLACE INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(n.id, n.kind, json.dumps([n.kind]), n.service, n.relpath,
              n.start_byte, n.end_byte, n.start_line, n.end_line,
              n.name, n.qualified_name, n.content_hash, json.dumps(n.props))
             for n in rows],
        )
        self._db.commit()

    def upsert_edges(self, rows: list[EdgeRec]) -> None:
        prepared = []
        for e in rows:
            ss, ds = _id_service(e.src), _id_service(e.dst)
            if (e.src.startswith("sym:") and e.dst.startswith("sym:")
                    and ss and ds and ss != ds):
                raise InvariantError(
                    f"cross-service code edge forbidden: {e.src} -{e.type}-> {e.dst}"
                )
            prepared.append((e.src, e.dst, e.type, e.resolution, e.confidence,
                             e.extractor, e.evidence_file, e.evidence_line,
                             json.dumps(e.props), ss))
        self._db.executemany(
            "INSERT OR REPLACE INTO edges VALUES (?,?,?,?,?,?,?,?,?,?)", prepared
        )
        self._db.commit()

    # -- чтение --

    def files_for_service(self, service: str) -> list[tuple[str, str]]:
        cur = self._db.execute(
            "SELECT relpath, sha256 FROM files WHERE service=? ORDER BY relpath",
            (service,))
        return list(cur.fetchall())

    def module_set(self, service: str) -> set[str]:
        return {ids.relpath_to_module(r) for r, _ in self.files_for_service(service)}

    def def_symbol_at(self, service: str, relpath: str, start_byte: int) -> str | None:
        cur = self._db.execute(
            "SELECT symbol FROM scip_defs WHERE service=? AND relpath=? AND start_byte=?",
            (service, relpath, start_byte))
        row = cur.fetchone()
        return row[0] if row else None

    def refs_for_file(self, service: str, relpath: str) -> list[RefRow]:
        cur = self._db.execute(
            "SELECT relpath, symbol, start_byte, end_byte, start_line, roles "
            "FROM scip_refs WHERE service=? AND relpath=? ORDER BY start_byte",
            (service, relpath))
        return [RefRow(*row) for row in cur.fetchall()]

    def counts(self) -> dict:
        out = {}
        for key, table in (("files", "files"), ("defs", "scip_defs"),
                           ("refs", "scip_refs"), ("nodes", "nodes"),
                           ("edges", "edges")):
            out[key] = self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        return out

    def iter_nodes(self) -> Iterator[NodeRec]:
        cur = self._db.execute(
            "SELECT id, kind, service, relpath, start_byte, end_byte, start_line, "
            "end_line, name, qualified_name, content_hash, props FROM nodes")
        for (id_, kind, service, relpath, sb, eb, sl, el, name, qn, ch, props) in cur:
            yield NodeRec(id=id_, kind=kind, service=service, relpath=relpath,
                          start_byte=sb, end_byte=eb, start_line=sl, end_line=el,
                          name=name, qualified_name=qn, content_hash=ch,
                          props=json.loads(props))

    def iter_edges(self) -> Iterator[EdgeRec]:
        cur = self._db.execute(
            "SELECT src, dst, type, resolution, confidence, extractor, "
            "evidence_file, evidence_line, props FROM edges")
        for (src, dst, type_, res, conf, ext, ef, el, props) in cur:
            yield EdgeRec(src=src, dst=dst, type=type_, resolution=res,
                          confidence=conf, extractor=ext, evidence_file=ef,
                          evidence_line=el, props=json.loads(props))

    def set_meta(self, key: str, value: str) -> None:
        self._db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))
        self._db.commit()

    def get_meta(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None
```

- [ ] **Step 4: GREEN + ruff** (примечание: если ruff ругается S608 — в select-наборе его нет, noqa-комментарии из листинга можно убрать)

- [ ] **Step 5: Commit** — `feat(m1a): sqlite staging store with invariant-checked edges`

---

### Task 6: resolvers/scip/runner.py — запуск scip-python

**Files:**
- Create: `src/codegraph/resolvers/scip/runner.py`
- Test: `tests/unit/test_scip_runner.py`, `tests/integration/test_scip_real.py`

**Interfaces:**
- `ScipRunError(CodegraphError)`.
- `ScipRunner(version: str = SCIP_PYTHON_VERSION, timeout_s: int = 1200, node_options: str = "--max-old-space-size=8192", npx: str = "npx")`.
- `.run(service_name, service_path: Path, venv: Path | None, cache_dir: Path, tree_hash: str) -> ScipRunResult(scip_path: Path, from_cache: bool)` — кэш-файл `cache_dir/<service_name>-<tree_hash>.scip`; при существовании — `from_cache=True` без запуска. Команда: `[npx, "--yes", f"@sourcegraph/scip-python@{version}", "index", ".", "--project-name", service_name, "--output", <abs scip_path>]`, `cwd=service_path`, `start_new_session=True`; env: копия + `NODE_OPTIONS`; при venv: `VIRTUAL_ENV` + `PATH=<venv>/bin:...`. Таймаут → `os.killpg(os.getpgid(pid), SIGKILL)` + `ScipRunError`. Ненулевой код или отсутствие файла → `ScipRunError` с хвостом вывода (последние ~2000 символов).

- [ ] **Step 1: Падающие тесты** (fake npx — python-скрипт, создаваемый фикстурой)

`tests/unit/test_scip_runner.py`:
```python
import os
import stat
import time

import pytest

from codegraph.resolvers.scip.runner import ScipRunError, ScipRunner

FAKE_OK = """#!/usr/bin/env python3
import sys, pathlib
args = sys.argv[1:]
out = args[args.index("--output") + 1]
pathlib.Path(out).write_bytes(b"FAKE-SCIP")
marker = pathlib.Path(__file__).parent / "invocations.log"
marker.open("a").write("run\\n")
"""

FAKE_SLEEP = """#!/usr/bin/env python3
import time
time.sleep(60)
"""

FAKE_FAIL = """#!/usr/bin/env python3
import sys
print("boom: cannot resolve environment")
sys.exit(3)
"""


def _mk_fake(tmp_path, body, name="fake-npx"):
    p = tmp_path / name
    p.write_text(body)
    p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return p


def test_run_creates_scip_and_caches(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_OK)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake))
    res1 = r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert res1.scip_path.read_bytes() == b"FAKE-SCIP" and not res1.from_cache
    res2 = r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert res2.from_cache
    assert (tmp_path / "invocations.log").read_text().count("run") == 1


def test_new_tree_hash_reruns(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_OK)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake))
    r.run("svc", svc, None, tmp_path / "cache", "h1")
    r.run("svc", svc, None, tmp_path / "cache", "h2")
    assert (tmp_path / "invocations.log").read_text().count("run") == 2


def test_timeout_kills_process_group(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_SLEEP)
    svc = tmp_path / "svc"
    svc.mkdir()
    r = ScipRunner(npx=str(fake), timeout_s=1)
    t0 = time.monotonic()
    with pytest.raises(ScipRunError, match="timeout"):
        r.run("svc", svc, None, tmp_path / "cache", "h1")
    assert time.monotonic() - t0 < 10


def test_nonzero_exit_raises_with_output_tail(tmp_path):
    fake = _mk_fake(tmp_path, FAKE_FAIL)
    svc = tmp_path / "svc"
    svc.mkdir()
    with pytest.raises(ScipRunError, match="cannot resolve environment"):
        ScipRunner(npx=str(fake)).run("svc", svc, None, tmp_path / "cache", "h1")


def test_venv_env_injected(tmp_path):
    probe = tmp_path / "probe-npx"
    probe.write_text(
        """#!/usr/bin/env python3
import os, sys, pathlib
args = sys.argv[1:]
out = args[args.index("--output") + 1]
pathlib.Path(out).write_text(os.environ.get("VIRTUAL_ENV", "") + "|" +
                             os.environ["PATH"].split(os.pathsep)[0])
"""
    )
    probe.chmod(probe.stat().st_mode | stat.S_IEXEC)
    svc = tmp_path / "svc"
    svc.mkdir()
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    res = ScipRunner(npx=str(probe)).run("svc", svc, venv, tmp_path / "c", "h")
    env_dump = res.scip_path.read_text()
    assert str(venv) in env_dump and str(venv / "bin") in env_dump
```

`tests/integration/test_scip_real.py`:
```python
"""Реальный scip-python на фикстурном сервисе. Медленно при первом запуске (npx скачивает пакет)."""

import shutil
from pathlib import Path

import pytest

from codegraph.resolvers.scip.runner import ScipRunner

pytestmark = pytest.mark.scip

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_scip_python_on_fixture(tmp_path):
    res = ScipRunner(timeout_s=600).run(
        "document-management", FIXTURE, None, tmp_path, "real"
    )
    assert res.scip_path.stat().st_size > 0
    from codegraph.resolvers.scip import scip_pb2

    idx = scip_pb2.Index()
    idx.ParseFromString(res.scip_path.read_bytes())
    assert len(idx.documents) >= 8  # 8 .py-файлов в фикстуре
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** (`resolvers/scip/runner.py`)

```python
"""Запуск scip-python через npx: pinned-версия, venv-окружение, таймаут, кэш по tree-hash."""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path

from codegraph.constants import SCIP_PYTHON_VERSION
from codegraph.core.errors import CodegraphError


class ScipRunError(CodegraphError):
    pass


@dataclass(frozen=True)
class ScipRunResult:
    scip_path: Path
    from_cache: bool


class ScipRunner:
    def __init__(self, version: str = SCIP_PYTHON_VERSION, timeout_s: int = 1200,
                 node_options: str = "--max-old-space-size=8192", npx: str = "npx"):
        self.version = version
        self.timeout_s = timeout_s
        self.node_options = node_options
        self.npx = npx

    def run(self, service_name: str, service_path: Path, venv: Path | None,
            cache_dir: Path, tree_hash: str) -> ScipRunResult:
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / f"{service_name}-{tree_hash}.scip"
        if out.exists():
            return ScipRunResult(scip_path=out, from_cache=True)

        cmd = [self.npx, "--yes", f"@sourcegraph/scip-python@{self.version}",
               "index", ".", "--project-name", service_name, "--output", str(out)]
        env = os.environ.copy()
        env["NODE_OPTIONS"] = self.node_options
        if venv is not None:
            env["VIRTUAL_ENV"] = str(venv)
            env["PATH"] = f"{venv / 'bin'}{os.pathsep}{env.get('PATH', '')}"

        proc = subprocess.Popen(
            cmd, cwd=service_path, env=env, start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            output, _ = proc.communicate(timeout=self.timeout_s)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
            raise ScipRunError(
                f"scip-python timeout after {self.timeout_s}s for {service_name!r}"
            ) from None
        if proc.returncode != 0 or not out.exists():
            tail = (output or "")[-2000:]
            raise ScipRunError(
                f"scip-python failed for {service_name!r} "
                f"(exit {proc.returncode}):\n{tail}"
            )
        return ScipRunResult(scip_path=out, from_cache=False)
```

- [ ] **Step 4: GREEN юниты + ruff**; затем интеграция: `uv run python -m pytest tests/integration/test_scip_real.py -m scip > /tmp/t6i.txt 2>&1; cat /tmp/t6i.txt` (реальный запуск, первый — минуты).

- [ ] **Step 5: Commit** — `feat(m1a): scip-python runner (pinned npx, venv env, timeout, tree-hash cache)`

---

### Task 7: resolvers/scip/reader.py — SCIP → staging

**Files:**
- Create: `src/codegraph/resolvers/scip/reader.py`
- Test: `tests/unit/test_scip_reader.py`, дополнение в `tests/integration/test_scip_real.py`

**Interfaces:**
- `ReaderStats(documents: int, defs: int, refs: int, skipped_documents: int)` — frozen dataclass.
- `read_scip_into_staging(scip_path: Path, service: str, service_root: Path, staging: Staging) -> ReaderStats`: для каждого `Index.documents[]` читает файл `service_root/relative_path` с диска (нет файла → skipped_documents+1), строит `LineIndex`, кодировку берёт из `doc.position_encoding` (маппинг enum→"utf-8"/"utf-16"/"utf-32"; **сверить имена enum-значений и семантику unspecified по vendored scip.proto** — в тестах покрыть utf-8 и utf-16 явно), нормализует `occ.range` (3 int → [l, sc, l, ec]; 4 int → как есть; 0-based, конец эксклюзивен), конвертирует в байты, `roles & 1` → DefRow, иначе RefRow (roles сохраняются). `start_line` = line0+1. Пишет `add_defs`/`add_refs` батчем на документ.

- [ ] **Step 1: Падающие тесты** (синтетический Index; кириллица в utf-16)

`tests/unit/test_scip_reader.py`:
```python
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
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** — по Interfaces; маппинг кодировок:
```python
_ENC = {
    scip_pb2.PositionEncoding.UTF8CodeUnitOffsetFromLineStart: "utf-8",
    scip_pb2.PositionEncoding.UTF16CodeUnitOffsetFromLineStart: "utf-16",
    scip_pb2.PositionEncoding.UTF32CodeUnitOffsetFromLineStart: "utf-32",
}
# unspecified (0): свериться с комментарием в vendored scip.proto; если спецификация
# говорит "по умолчанию UTF-8" — дефолт "utf-8", иначе следовать спецификации.
```
Нормализация range: `r = list(occ.range); sl, sc, el, ec = (r[0], r[1], r[0], r[2]) if len(r) == 3 else r`. Батчи: копить DefRow/RefRow на документ, один вызов add_defs/add_refs на файл.

- [ ] **Step 4: GREEN + ruff.** Дополнить `tests/integration/test_scip_real.py` вторым тестом (тот же маркер):
```python
@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_scip_reader_fills_staging(tmp_path):
    from codegraph.resolvers.scip.reader import read_scip_into_staging
    from codegraph.stores.staging import Staging

    res = ScipRunner(timeout_s=600).run("document-management", FIXTURE, None, tmp_path, "real2")
    st = Staging(tmp_path / "s.db")
    stats = read_scip_into_staging(res.scip_path, "document-management", FIXTURE, st)
    assert stats.defs > 10 and stats.refs > 10 and stats.skipped_documents == 0
```

- [ ] **Step 5: Commit** — `feat(m1a): SCIP reader (protobuf occurrences to staging with encoding-aware spans)`

---

### Task 8: parsing/ts.py + parsing/facts.py — tree-sitter FileFacts

**Files:**
- Create: `src/codegraph/parsing/__init__.py`, `src/codegraph/parsing/ts.py`, `src/codegraph/parsing/facts.py`
- Test: `tests/unit/test_parsing_facts.py`

**Interfaces:**
- `ts.parse(source: bytes) -> tree_sitter.Tree` (модульный singleton Parser; `ts.PY_LANGUAGE`).
- `facts.DefFact(index, kind: "class"|"function", name, name_start_byte, name_end_byte, start_byte, end_byte, start_line, end_line, parent: int | None, is_async: bool, signature: str, docstring: str | None, decorators: list[str])` — start/end_line 1-based; parent — индекс в списке defs.
- `facts.CallFact(callee_name, callee_start_byte, callee_end_byte, start_line, enclosing_def: int | None)` — для `call` с function: identifier → сам identifier; attribute → его поле attribute (последний сегмент); иные типы callee пропускаются.
- `facts.ImportFact(target_module: str, names: list[str], start_line: int)` — `import a.b` → target "a.b", names []; `from a.b import c, d` → target "a.b", names ["c","d"]; относительные (`from . import x`, `from .sub import y`) → target с ведущими точками ('.'*level + module) — резолв точек делает экстрактор.
- `facts.FileFacts(relpath, module_docstring: str | None, defs: list[DefFact], calls: list[CallFact], imports: list[ImportFact])`.
- `facts.build_file_facts(relpath: str, source: bytes) -> FileFacts` — ручной рекурсивный обход (без Query). `decorated_definition` разворачивается (декораторы → текст без `@`). Docstring: первый statement блока — string → текст без кавычек (обрезать `'''`/`"""`/`'`/`"` и префиксы r/b/f). Signature: `def name(params)` — срез исходника от `def`-токена до конца parameters. is_async: первый ребёнок function_definition имеет type "async".

- [ ] **Step 1: Падающие тесты**

`tests/unit/test_parsing_facts.py`:
```python
from codegraph.parsing.facts import build_file_facts

SRC = b'''"""Module doc."""
import os
from app.db.session import Session, get_db
from . import sibling


class OrderService:
    """Svc doc."""

    def __init__(self, db):
        self._db = db

    async def place(self, req):
        order = self._build(req)
        await self._persist(order)
        outbox = OutboxRepository(self._db)
        await outbox.add_event("OrderCreated", {})
        return order

    def _build(self, req):
        return req


def helper():
    print(os.getpid())
'''


def _facts():
    return build_file_facts("app/services/order.py", SRC)


def test_module_docstring_and_imports():
    f = _facts()
    assert f.module_docstring == "Module doc."
    targets = [(i.target_module, i.names) for i in f.imports]
    assert ("os", []) in targets
    assert ("app.db.session", ["Session", "get_db"]) in targets
    assert (".", ["sibling"]) in targets


def test_def_hierarchy_and_flags():
    f = _facts()
    by_name = {d.name: d for d in f.defs}
    cls = by_name["OrderService"]
    place = by_name["place"]
    assert cls.kind == "class" and cls.parent is None
    assert place.kind == "function" and f.defs[place.parent] is cls
    assert place.is_async and not by_name["helper"].is_async
    assert by_name["helper"].parent is None
    assert cls.docstring == "Svc doc."
    assert place.signature.startswith("def place(") or "place(self, req)" in place.signature


def test_name_token_span_points_at_name():
    f = _facts()
    place = next(d for d in f.defs if d.name == "place")
    assert SRC[place.name_start_byte:place.name_end_byte] == b"place"


def test_calls_with_enclosing():
    f = _facts()
    calls = {(c.callee_name, c.enclosing_def) for c in f.calls}
    place_i = next(d.index for d in f.defs if d.name == "place")
    helper_i = next(d.index for d in f.defs if d.name == "helper")
    assert ("_build", place_i) in calls
    assert ("_persist", place_i) in calls
    assert ("OutboxRepository", place_i) in calls
    assert ("add_event", place_i) in calls
    assert ("print", helper_i) in calls
    assert ("getpid", helper_i) in calls


def test_callee_token_span_is_last_segment():
    f = _facts()
    add_event = next(c for c in f.calls if c.callee_name == "add_event")
    assert SRC[add_event.callee_start_byte:add_event.callee_end_byte] == b"add_event"
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация**

`parsing/ts.py`:
```python
"""Единая tree-sitter-сессия для Python. Query API не используем (API-стабильность)."""

from __future__ import annotations

import tree_sitter_python as _tspython
from tree_sitter import Language, Parser, Tree

PY_LANGUAGE = Language(_tspython.language())
_parser = Parser(PY_LANGUAGE)


def parse(source: bytes) -> Tree:
    return _parser.parse(source)
```
(Если установленная версия биндингов требует `Parser(); parser.language = PY_LANGUAGE` — адаптировать конструктор, это единственная разрешённая правка.)

`parsing/facts.py` — датаклассы по Interfaces + обход:
```python
"""FileFacts: структурные факты одного файла из tree-sitter AST (ручной обход)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DefFact:
    index: int
    kind: str  # "class" | "function"
    name: str
    name_start_byte: int
    name_end_byte: int
    start_byte: int
    end_byte: int
    start_line: int  # 1-based
    end_line: int
    parent: int | None
    is_async: bool
    signature: str
    docstring: str | None
    decorators: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CallFact:
    callee_name: str
    callee_start_byte: int
    callee_end_byte: int
    start_line: int
    enclosing_def: int | None


@dataclass(frozen=True)
class ImportFact:
    target_module: str
    names: list[str]
    start_line: int


@dataclass(frozen=True)
class FileFacts:
    relpath: str
    module_docstring: str | None
    defs: list[DefFact]
    calls: list[CallFact]
    imports: list[ImportFact]


def _strip_string(text: str) -> str:
    t = text.lstrip("rbufRBUF")
    for q in ('"""', "'''", '"', "'"):
        if t.startswith(q) and t.endswith(q) and len(t) >= 2 * len(q):
            return t[len(q):-len(q)]
    return t


def _docstring_of_block(block, source: bytes) -> str | None:
    for child in block.named_children:
        if child.type != "expression_statement":
            return None
        inner = child.named_children[0] if child.named_children else None
        if inner is not None and inner.type == "string":
            return _strip_string(inner.text.decode("utf-8", errors="replace"))
        return None
    return None


def build_file_facts(relpath: str, source: bytes) -> FileFacts:
    from codegraph.parsing.ts import parse

    tree = parse(source)
    root = tree.root_node
    defs: list[DefFact] = []
    calls: list[CallFact] = []
    imports: list[ImportFact] = []

    def visit(node, parent_def: int | None, decorators: list[str]):
        if node.type == "decorated_definition":
            decs = [
                d.text.decode()[1:].strip()
                for d in node.children
                if d.type == "decorator"
            ]
            definition = node.child_by_field_name("definition")
            if definition is not None:
                visit(definition, parent_def, decs)
            return

        if node.type in ("class_definition", "function_definition"):
            name_node = node.child_by_field_name("name")
            body = node.child_by_field_name("body")
            is_async = node.children and node.children[0].type == "async"
            if node.type == "function_definition":
                params = node.child_by_field_name("parameters")
                sig = (
                    "def "
                    + source[name_node.start_byte:params.end_byte].decode(
                        "utf-8", errors="replace"
                    )
                    if params is not None
                    else "def " + name_node.text.decode()
                )
            else:
                sig = "class " + name_node.text.decode()
            idx = len(defs)
            defs.append(DefFact(
                index=idx,
                kind="class" if node.type == "class_definition" else "function",
                name=name_node.text.decode(),
                name_start_byte=name_node.start_byte,
                name_end_byte=name_node.end_byte,
                start_byte=node.start_byte,
                end_byte=node.end_byte,
                start_line=node.start_point[0] + 1,
                end_line=node.end_point[0] + 1,
                parent=parent_def,
                is_async=bool(is_async),
                signature=sig,
                docstring=_docstring_of_block(body, source) if body else None,
                decorators=decorators,
            ))
            if body is not None:
                for ch in body.children:
                    visit(ch, idx, [])
            return

        if node.type == "call":
            fn = node.child_by_field_name("function")
            token = None
            if fn is not None and fn.type == "identifier":
                token = fn
            elif fn is not None and fn.type == "attribute":
                token = fn.child_by_field_name("attribute")
            if token is not None:
                calls.append(CallFact(
                    callee_name=token.text.decode(),
                    callee_start_byte=token.start_byte,
                    callee_end_byte=token.end_byte,
                    start_line=node.start_point[0] + 1,
                    enclosing_def=parent_def,
                ))
            # аргументы могут содержать вложенные вызовы/дефы — обходим дальше

        if node.type == "import_statement":
            for ch in node.named_children:
                if ch.type == "dotted_name":
                    imports.append(ImportFact(ch.text.decode(), [], node.start_point[0] + 1))
                elif ch.type == "aliased_import":
                    dn = ch.child_by_field_name("name")
                    if dn is not None:
                        imports.append(ImportFact(dn.text.decode(), [], node.start_point[0] + 1))
            return

        if node.type == "import_from_statement":
            module_node = node.child_by_field_name("module_name")
            level = node.text.decode().split("import")[0].count(".")
            base = module_node.text.decode() if module_node is not None and module_node.type != "relative_import" else ""
            if module_node is not None and module_node.type == "relative_import":
                base = module_node.text.decode()
            target = base if base.startswith(".") else ("." * 0 + base)
            names = [
                ch.text.decode()
                for ch in node.named_children
                if ch.type == "dotted_name" and ch is not module_node
            ] + [
                ch.child_by_field_name("name").text.decode()
                for ch in node.named_children
                if ch.type == "aliased_import" and ch.child_by_field_name("name") is not None
            ]
            _ = level
            imports.append(ImportFact(target, names, node.start_point[0] + 1))
            return

        for ch in node.children:
            visit(ch, parent_def, [])

    for top in root.children:
        visit(top, None, [])

    return FileFacts(
        relpath=relpath,
        module_docstring=_docstring_of_block(root, source),
        defs=defs,
        calls=calls,
        imports=imports,
    )
```
**Примечание исполнителю (обязательное):** блок `import_from_statement` в листинге — ЭСКИЗ: точные типы дочерних узлов (`relative_import`, позиция module_name, aliased_import) проверить на реальной грамматике установленного tree-sitter-python (напечатать `node.sexp()` для тестовых импортов) и добиться прохождения теста `test_module_docstring_and_imports`, включая случай `from . import sibling` → target ".". Остальные ветви обхода менять только при фактическом расхождении грамматики.

- [ ] **Step 4: GREEN + ruff; дополнительно прогнать build_file_facts на всех 29 фикстурных файлах** (быстрый smoke прямо в тесте):
```python
def test_smoke_all_fixture_files_parse():
    from pathlib import Path

    fixtures = Path(__file__).parents[2] / "fixtures" / "services"
    for f in fixtures.rglob("*.py"):
        facts = build_file_facts(str(f), f.read_bytes())
        assert facts is not None
```

- [ ] **Step 5: Commit** — `feat(m1a): tree-sitter FileFacts (defs hierarchy, calls, imports, docstrings)`

---

### Task 9: extractors/base.py + extractors/python_core.py

**Files:**
- Create: `src/codegraph/extractors/__init__.py`, `src/codegraph/extractors/base.py`, `src/codegraph/extractors/python_core.py`
- Test: `tests/unit/test_python_core_extractor.py`

**Interfaces:**
- `base.FileContext(service, relpath, source: bytes, facts: FileFacts, def_symbol_lookup: Callable[[str, int], str | None], module_exists: Callable[[str], bool])` — lookup(relpath, name_start_byte) → SCIP-символ def'а или None; module_exists(dotted) — есть ли такой модуль среди файлов сервиса.
- `base.ExtractionResult(nodes: list[NodeRec], edges: list[EdgeRec], stats: dict[str, int])`.
- `python_core.extract(ctx) -> ExtractionResult`:
  - Module NodeRec: id = `ids.node_id(svc, ids.module_descriptor(dotted))`; kind Module; name = последний сегмент; qualified_name = dotted; span = весь файл (1..последняя строка); props: docstring; content_hash = sha256(source).
  - CONTAINS `svc:<service>` → module (extractor "python_core", static, 1.0).
  - Для каждого DefFact: nesting из цепочки parent'ов → структурный дескриптор; id = lookup(name-span) → `symbols.symbol_to_node_id` ИЛИ структурный `ids.node_id`; kind Class|Function; qualified_name = `dotted + "." + ".".join(имена цепочки)`; props: signature, docstring, is_async, decorators; content_hash = sha256(среза source по span). CONTAINS parent→child (parent = module или родительский def, id по той же политике).
  - Импорты: target с ведущими точками резолвится относительно dotted текущего модуля (`.` → пакет текущего модуля; `..x` → на уровень выше; правило: base = dotted.split('.'), отрезать по числу точек, добавить остаток); если итоговый модуль ∈ module_exists → IMPORTS module→target; для `from X import c`: если `X.c` ∈ module_exists — ребро в `X.c` (импорт подмодуля), иначе в `X`. Не найден в сервисе → stats["imports_external"] += 1.
  - stats: nodes, edges, imports_external.

- [ ] **Step 1: Падающие тесты** (без scip: lookup всегда None → структурные id)

`tests/unit/test_python_core_extractor.py`:
```python
from pathlib import Path

from codegraph.extractors.base import FileContext
from codegraph.extractors.python_core import extract
from codegraph.parsing.facts import build_file_facts

FIXTURE = (Path(__file__).parents[2] / "fixtures" / "services" / "orders_api"
           / "app" / "services" / "order.py")


def _ctx(module_set=frozenset()):
    source = FIXTURE.read_bytes()
    relpath = "app/services/order.py"
    return FileContext(
        service="orders-api", relpath=relpath, source=source,
        facts=build_file_facts(relpath, source),
        def_symbol_lookup=lambda rp, sb: None,
        module_exists=lambda dotted: dotted in module_set,
    )


def test_module_and_defs_nodes():
    res = extract(_ctx())
    by_qn = {n.qualified_name: n for n in res.nodes}
    assert by_qn["app.services.order"].kind == "Module"
    assert by_qn["app.services.order.OrderService"].kind == "Class"
    place = by_qn["app.services.order.OrderService.place"]
    assert place.kind == "Function" and place.props["is_async"] is True
    assert place.id == "sym:orders-api:`app.services.order`/OrderService#place()."
    assert place.content_hash and place.start_line > 1


def test_contains_chain():
    res = extract(_ctx())
    contains = {(e.src, e.dst) for e in res.edges if e.type == "CONTAINS"}
    mod = "sym:orders-api:`app.services.order`/"
    cls = "sym:orders-api:`app.services.order`/OrderService#"
    assert ("svc:orders-api", mod) in contains
    assert (mod, cls) in contains
    assert (cls, "sym:orders-api:`app.services.order`/OrderService#place().") in contains


def test_imports_internal_vs_external():
    res = extract(_ctx(module_set={"app.db.outbox", "app.db.session", "app.models"}))
    imports = {e.dst for e in res.edges if e.type == "IMPORTS"}
    assert "sym:orders-api:`app.db.outbox`/" in imports
    assert "sym:orders-api:`app.models`/" in imports
    assert res.stats["imports_external"] >= 1  # uuid


def test_scip_lookup_takes_precedence():
    ctx = _ctx()
    sym = "scip-python python orders-api 0.1 `app.services.order`/OrderService#"
    ctx2 = FileContext(
        service=ctx.service, relpath=ctx.relpath, source=ctx.source, facts=ctx.facts,
        def_symbol_lookup=lambda rp, sb: sym if ctx.source[sb:sb + 12] == b"OrderService" else None,
        module_exists=lambda d: False,
    )
    res = extract(ctx2)
    cls = next(n for n in res.nodes if n.kind == "Class")
    assert cls.id == "sym:orders-api:`app.services.order`/OrderService#"
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** — по Interfaces. `base.py`: два frozen dataclass'а (FileContext c Callable-полями — не frozen обязателен, обычный dataclass допустим). Ключевые куски python_core:
```python
def _nesting(defs, d):
    chain = []
    cur = d
    while cur is not None:
        chain.append(("class" if cur.kind == "class" else "function", cur.name))
        cur = defs[cur.parent] if cur.parent is not None else None
    return list(reversed(chain))


def _resolve_relative(current_module: str, target: str) -> str:
    if not target.startswith("."):
        return target
    dots = len(target) - len(target.lstrip("."))
    rest = target.lstrip(".")
    base = current_module.split(".")
    base = base[: len(base) - dots] if dots <= len(base) else []
    return ".".join([*base, rest] if rest else base)
```
Ребро IMPORTS: evidence_file=relpath, evidence_line=строка импорта. Дедуп рёбер по (src,dst,type) внутри результата (set).

- [ ] **Step 4: GREEN + ruff**

- [ ] **Step 5: Commit** — `feat(m1a): python_core extractor (Module/Class/Function, CONTAINS, IMPORTS)`

---

### Task 10: S6 join — CALLS из SCIP-refs × call-sites

**Files:**
- Create: `src/codegraph/extractors/calls.py`
- Test: `tests/unit/test_calls_join.py`, дополнение в `tests/integration/test_scip_real.py`

**Interfaces:**
- `calls.JoinStats(calls_joined: int, calls_unresolved: int, calls_external: int)`.
- `calls.build_calls(service: str, staging: Staging, facts_by_file: dict[str, FileFacts], def_symbol_lookup: Callable[[str, int], str | None], resolution: str = "static", confidence: float = 1.0) -> JoinStats`:
  - Для каждого файла: refs = `staging.refs_for_file` → точный словарь `{start_byte: RefRow}` + отсортированный список для containment-фоллбека (bisect: последний ref с start_byte ≤ callee_start < end_byte).
  - Для CallFact: ref по `callee_start_byte` (точно, иначе containment). Нет → calls_unresolved+1.
  - `parse_symbol(ref.symbol)`: global и `package != service` → calls_external+1, пропустить. Иначе dst id = `symbol_to_node_id(service, relpath, ref.symbol)`.
  - Caller: enclosing_def → его name-span → def_symbol_lookup → id (или структурный через facts-цепочку); enclosing None → id модуля.
  - Агрегация по (src, dst): callsite_count; evidence = первый call (file, line). EdgeRec CALLS, extractor="calls", resolution/confidence из параметров → `staging.upsert_edges`.
- Политика: вызов конструктора (`Klass()`) даёт CALLS → узел класса (`…#`) — это осознанно разрешено.

- [ ] **Step 1: Падающие тесты** (синтетика: staging с рукамиположенными refs/defs + facts от build_file_facts)

`tests/unit/test_calls_join.py`:
```python
from pathlib import Path

from codegraph.extractors.calls import build_calls
from codegraph.parsing.facts import build_file_facts
from codegraph.resolvers.base import DefRow, RefRow
from codegraph.stores.staging import Staging

SRC = b"""def g():
    pass


def f():
    g()
    g()
    unknown_dyn()
    import os
    os.getpid()
"""

SYM_G = "scip-python python svc 0.1 `m`/g()."
SYM_F = "scip-python python svc 0.1 `m`/f()."
SYM_OS_GETPID = "scip-python python cpython 3.12 `os`/getpid()."


def _prepare(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    facts = build_file_facts("m.py", SRC)
    g_def = next(d for d in facts.defs if d.name == "g")
    f_def = next(d for d in facts.defs if d.name == "f")
    st.add_defs("svc", [
        DefRow("m.py", SYM_G, g_def.name_start_byte, g_def.name_end_byte, 1),
        DefRow("m.py", SYM_F, f_def.name_start_byte, f_def.name_end_byte, 5),
    ])
    g_calls = [c for c in facts.calls if c.callee_name == "g"]
    getpid_call = next(c for c in facts.calls if c.callee_name == "getpid")
    st.add_refs("svc", [
        RefRow("m.py", SYM_G, g_calls[0].callee_start_byte, g_calls[0].callee_end_byte, 6, 0),
        RefRow("m.py", SYM_G, g_calls[1].callee_start_byte, g_calls[1].callee_end_byte, 7, 0),
        RefRow("m.py", SYM_OS_GETPID, getpid_call.callee_start_byte,
               getpid_call.callee_end_byte, 10, 0),
    ])

    def lookup(rp, sb):
        return {g_def.name_start_byte: SYM_G, f_def.name_start_byte: SYM_F}.get(sb)

    return st, {"m.py": facts}, lookup


def test_join_aggregates_and_classifies(tmp_path):
    st, facts_by_file, lookup = _prepare(tmp_path)
    stats = build_calls("svc", st, facts_by_file, lookup)
    assert stats.calls_joined == 2       # два вызова g() слились в одно ребро
    assert stats.calls_external == 1     # os.getpid
    assert stats.calls_unresolved == 1   # unknown_dyn
    edges = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(edges) == 1
    e = edges[0]
    assert e.src == "sym:svc:`m`/f()." and e.dst == "sym:svc:`m`/g()."
    assert e.props["callsite_count"] == 2
    assert e.resolution == "static" and e.confidence == 1.0


def test_module_level_call_attributed_to_module(tmp_path):
    st = Staging(tmp_path / "s.db")
    st.begin_service("svc")
    src = b"def g():\n    pass\n\n\ng()\n"
    facts = build_file_facts("m.py", src)
    call = facts.calls[0]
    st.add_refs("svc", [RefRow("m.py", SYM_G, call.callee_start_byte,
                               call.callee_end_byte, 5, 0)])
    build_calls("svc", st, {"m.py": facts}, lambda rp, sb: None)
    e = next(e for e in st.iter_edges() if e.type == "CALLS")
    assert e.src == "sym:svc:`m`/"


def test_containment_fallback_when_exact_miss(tmp_path):
    st, facts_by_file, lookup = _prepare(tmp_path)
    # сдвинем все ref-спаны на -1 (утрируем расхождение конвертации на 1 байт)
    refs = st.refs_for_file("svc", "m.py")
    st.begin_service("svc")
    st.add_refs("svc", [RefRow(r.relpath, r.symbol, r.start_byte - 1,
                               r.end_byte, r.start_line, r.roles) for r in refs])
    stats = build_calls("svc", st, facts_by_file, lookup)
    assert stats.calls_joined >= 2  # containment всё ещё сшивает
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** — по Interfaces; caller-id хелпер:
```python
def _caller_id(service, relpath, facts, enclosing, lookup):
    if enclosing is None:
        return ids.node_id(service, ids.module_descriptor(ids.relpath_to_module(relpath)))
    d = facts.defs[enclosing]
    sym = lookup(relpath, d.name_start_byte)
    if sym:
        return symbol_to_node_id(service, relpath, sym)
    return ids.node_id(
        service,
        ids.structural_descriptor(ids.relpath_to_module(relpath), _nesting(facts.defs, d)),
    )
```
(вынести `_nesting` в общий модуль `extractors/python_core.py` → импортировать, НЕ дублировать).

- [ ] **Step 4: GREEN + ruff.** Интеграция (дополнить test_scip_real.py, маркер scip):
```python
@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_real_calls_join_document_management(tmp_path):
    from codegraph.extractors.calls import build_calls
    from codegraph.parsing.facts import build_file_facts
    from codegraph.resolvers.scip.reader import read_scip_into_staging
    from codegraph.stores.staging import Staging

    svc = "document-management"
    res = ScipRunner(timeout_s=600).run(svc, FIXTURE, None, tmp_path, "real3")
    st = Staging(tmp_path / "s.db")
    st.begin_service(svc)
    read_scip_into_staging(res.scip_path, svc, FIXTURE, st)
    facts_by_file = {}
    for f in FIXTURE.rglob("*.py"):
        rel = str(f.relative_to(FIXTURE))
        facts_by_file[rel] = build_file_facts(rel, f.read_bytes())
    stats = build_calls(svc, st, facts_by_file, st_lookup(st, svc))
    calls = {(e.src, e.dst) for e in st.iter_edges() if e.type == "CALLS"}
    assert stats.calls_joined >= 5
    assert any("create_document" in s and "emit_document_indexed" in d
               for s, d in calls), calls


def st_lookup(st, svc):
    def lookup(relpath, start_byte):
        return st.def_symbol_at(svc, relpath, start_byte)
    return lookup
```
(точная форма id в реальном SCIP может отличаться деталями дескрипторов — asserts по подстрокам имён; расхождения формата дескрипторов с ids.py зафиксировать в отчёте — это вход для M1b.)

- [ ] **Step 5: Commit** — `feat(m1a): CALLS join (scip refs x call-sites, aggregation, external/unresolved counters)`

---

### Task 11: resolvers/fallback.py — эвристический резолвер

**Files:**
- Create: `src/codegraph/resolvers/fallback.py`
- Test: `tests/unit/test_fallback_resolver.py`

**Interfaces:**
- `fallback.resolve_service(service: str, files: dict[str, bytes], facts_by_file: dict[str, FileFacts]) -> tuple[list[DefRow], list[RefRow]]`:
  - Defs: для каждого DefFact — синтетический символ `f"scip-python python {service} 0.0 {структурный дескриптор}"` на name-span (совместим с parse_symbol/ids).
  - Refs: (а) вызов имени, определённого В ЭТОМ файле (по имени DefFact верхнего уровня) → ref на его символ; (б) вызов имени, импортированного `from X import name`, где модуль X есть в facts_by_file и имеет top-level def `name` → ref на символ этого def'а. Прочее — пропустить (нерезолвлено).
- Использование (M1b): при ScipRunError сервис уходит в degraded → эти defs/refs кладутся в staging, join зовётся с resolution="heuristic", confidence=0.6. В M1a — только юнит-тест функции.

- [ ] **Step 1: Падающий тест**

`tests/unit/test_fallback_resolver.py`:
```python
from codegraph.parsing.facts import build_file_facts
from codegraph.resolvers.fallback import resolve_service

A = b"""from b import helper


def local():
    pass


def caller():
    local()
    helper()
    mystery()
"""
B = b"""def helper():
    pass
"""


def test_fallback_defs_and_refs():
    files = {"a.py": A, "b.py": B}
    facts = {rp: build_file_facts(rp, src) for rp, src in files.items()}
    defs, refs = resolve_service("svc", files, facts)
    def_syms = {d.symbol for d in defs}
    assert any("`a`/caller()." in s for s in def_syms)
    assert any("`b`/helper()." in s for s in def_syms)
    ref_syms = [r.symbol for r in refs]
    assert any("`a`/local()." in s for s in ref_syms)      # same-file
    assert any("`b`/helper()." in s for s in ref_syms)     # via from-import
    assert not any("mystery" in s for s in ref_syms)       # нерезолвлено — пропущено
    helper_ref = next(r for r in refs if "`b`/helper()." in r.symbol)
    assert A[helper_ref.start_byte:helper_ref.end_byte] == b"helper"
```

- [ ] **Step 2: RED**

- [ ] **Step 3: Реализация** — прямолинейно по Interfaces (символ синтезировать через `ids.structural_descriptor`; индексы: по файлу — {имя top-level def → DefFact}; по импортам файла — {imported name → target module}).

- [ ] **Step 4: GREEN + ruff + полный прогон** через редирект.

- [ ] **Step 5: Commit** — `feat(m1a): heuristic fallback resolver (same-file defs + from-imports)`

---

## Верификация M1a (гейт перед M1b)

1. `uv run python -m pytest > /tmp/m1a.txt 2>&1; cat /tmp/m1a.txt` — все юниты зелёные.
2. `uv run python -m pytest -m scip > /tmp/m1a_scip.txt 2>&1; cat /tmp/m1a_scip.txt` — реальный scip-python: runner → reader → join на document_management, включая ребро create_document→emit_document_indexed.
3. `uv run ruff check .` — чисто.
4. В отчёте последней задачи зафиксировать фактический формат SCIP-дескрипторов scip-python 0.6.6 (совпадение/расхождения с ids.py) — вход для M1b.

