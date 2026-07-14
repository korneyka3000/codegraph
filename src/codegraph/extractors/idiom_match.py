"""idiom_match: ярусный матчер вызовов/декораторов на структурные факты T2 (FileFacts),
без обязательного участия SCIP. См. §M2 T3 брифа (.superpowers/sdd/m2-task-3-brief.md)
для полной спецификации.

Три яруса доказательности для CallFact — match_calls проверяет их СТРОГО по приоритету,
первый сработавший ярус побеждает; один call даёт максимум один CallMatch (elif-цепочка
ниже физически не может породить два матча на один call — естественный дедуп по
callee_start_byte, т.к. каждый CallFact в facts.calls уникален по этому полю):

  1. STATIC (resolution="static", confidence=1.0) — qualified_of(call) (обычно
     scip-lookup по callee-спану → display_qualified вызываемого символа) сравнивается
     с pattern через fnmatchcase; паттерн без модульного префикса тоже допускается
     (доп. попытка "*."+pattern).
  2. RECEIVER (resolution="heuristic", confidence=0.8) — attribute-вызов вида
     "receiver.method(...)", где receiver — ПРОСТОЕ имя (без точек в тексте), и в этом
     же файле есть присваивание `receiver = Класс(...)` (AssignFact). класс/метод берутся
     из паттерна: класс = предпоследний сегмент, метод = последний.
  3. IMPORT_NAME (resolution="heuristic", confidence=0.6) — самый слабый ярус,
     файловый (НЕ привязан к конкретному receiver): файл импортирует модуль/класс из
     паттерна, а имя вызова совпадает с последним сегментом паттерна (методом) либо,
     для ctor-паттернов (см. _is_ctor_pattern), с самим классом.

match_decorators — отдельная (не ярусная) сверка текста декораторов с паттерном.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatchcase

from codegraph.parsing.facts import CallFact, DefFact, FileFacts, ImportFact


class MatchTier(Enum):
    """resolution/confidence — готовые значения для EdgeRec (core/schema.py), чтобы
    вызывающему (T4/T5) не пришлось повторно маппить tier → resolution/confidence."""

    STATIC = ("static", 1.0)
    RECEIVER = ("heuristic", 0.8)
    IMPORT_NAME = ("heuristic", 0.6)

    def __init__(self, resolution: str, confidence: float) -> None:
        self.resolution = resolution
        self.confidence = confidence


@dataclass(frozen=True)
class CallMatch:
    call: CallFact
    tier: MatchTier
    resolution: str
    confidence: float


def _is_ctor_pattern(segments: list[str]) -> bool:
    """Эвристика брифа: паттерн без метода (голый конструктор) — последний сегмент
    начинается с заглавной буквы (CamelCase-класс), напр. "aiokafka.AIOKafkaConsumer"
    (в отличие от метод-формы "aiokafka.AIOKafkaProducer.send", где последний сегмент —
    snake_case-имя метода)."""
    last = segments[-1]
    return bool(last) and last[0].isupper()


def _match_static(
    call: CallFact, pattern: str, qualified_of: Callable[[CallFact], str | None]
) -> bool:
    qualified = qualified_of(call)
    if not qualified:
        return False
    return fnmatchcase(qualified, pattern) or fnmatchcase(qualified, "*." + pattern)


def _match_receiver(call: CallFact, facts: FileFacts, segments: list[str]) -> bool:
    if len(segments) < 2:
        return False  # нет предпоследнего сегмента — класс не определён, ярус неприменим
    class_seg, method = segments[-2], segments[-1]
    receiver = call.receiver_text
    if receiver is None or "." in receiver:
        return False  # не attribute-вызов ПРОСТОГО имени (без точек)
    if call.callee_name != method:
        return False
    return any(a.target == receiver and a.callee_name == class_seg for a in facts.assigns)


def _imports_module(imports: list[ImportFact], first_segment: str) -> bool:
    """Файл импортирует first_segment как модуль: `import first_segment[.sub]` или
    `from first_segment[.sub] import ...` — в обоих случаях ImportFact.target_module
    равен first_segment либо является его под-модулем (начинается с "first_segment.")."""
    return any(
        imp.target_module == first_segment or imp.target_module.startswith(first_segment + ".")
        for imp in imports
    )


def _match_import_name_method_form(call: CallFact, facts: FileFacts, segments: list[str]) -> bool:
    method = segments[-1]
    if call.callee_name != method:
        return False
    if _imports_module(facts.imports, segments[0]):
        return True
    if len(segments) < 2:
        return False
    class_seg = segments[-2]
    prefix = ".".join(segments[:-2])  # всё "до класса"; "" если паттерн — Класс.метод (2 сегмента)
    if not prefix:
        return False
    return any(class_seg in imp.names and imp.target_module == prefix for imp in facts.imports)


def _match_import_name_ctor_form(call: CallFact, facts: FileFacts, segments: list[str]) -> bool:
    class_seg = segments[-1]
    if call.callee_name != class_seg:
        return False
    if any(class_seg in imp.names for imp in facts.imports):
        return True  # from-import: "from <модуль> import Класс"
    # "import модуль" + "модуль.Класс(...)" (attr-вызов); receiver не обязан текстуально
    # совпадать с именем модуля — ярус и так самый слабый по доказательности.
    return call.receiver_text is not None and _imports_module(facts.imports, segments[0])


def _match_import_name(call: CallFact, facts: FileFacts, segments: list[str]) -> bool:
    if _is_ctor_pattern(segments):
        return _match_import_name_ctor_form(call, facts, segments)
    return _match_import_name_method_form(call, facts, segments)


def match_calls(
    pattern: str,
    facts: FileFacts,
    qualified_of: Callable[[CallFact], str | None],
) -> list[CallMatch]:
    """Каждый call проверяется по ярусам СТРОГО по приоритету (STATIC → RECEIVER →
    IMPORT_NAME); первый сработавший — финальный."""
    segments = pattern.split(".")
    matches: list[CallMatch] = []
    for call in facts.calls:
        if _match_static(call, pattern, qualified_of):
            tier = MatchTier.STATIC
        elif _match_receiver(call, facts, segments):
            tier = MatchTier.RECEIVER
        elif _match_import_name(call, facts, segments):
            tier = MatchTier.IMPORT_NAME
        else:
            continue
        matches.append(CallMatch(
            call=call, tier=tier, resolution=tier.resolution, confidence=tier.confidence,
        ))
    return matches


def match_decorators(pattern: str, defs: list[DefFact]) -> list[tuple[DefFact, str]]:
    """decorator-текст (DefFact.decorators, уже без "@") матчится, если: точное
    равенство pattern; либо call-форма — префикс "pattern(" (быстрый путь для
    паттернов без своих glob-символов) или fnmatchcase(dec, pattern + "(*") (даёт
    самому pattern право содержать glob-символы, напр. "router.*" матчит и
    "router.get(...)", и "router.post(...)"). Возвращает (def, полный текст декоратора)
    — один def с несколькими декораторами может дать несколько независимых записей."""
    call_prefix = pattern + "("
    glob_pattern = pattern + "(*"
    results: list[tuple[DefFact, str]] = []
    for d in defs:
        for dec in d.decorators:
            if dec == pattern or dec.startswith(call_prefix) or fnmatchcase(dec, glob_pattern):
                results.append((d, dec))
    return results
