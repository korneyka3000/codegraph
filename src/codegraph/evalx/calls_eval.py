"""M1 eval: CALLS-рёбра staging против вручную размеченного golden
(fixtures/golden/edges.yaml). Только Staging (SQLite) на входе — никаких
Cypher/FalkorDB-зависимостей, эта проверка гоняется до и независимо от загрузки в
граф-стор (см. m1b-task-9-brief.md self-review: "evalx без Cypher/store-зависимостей").
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from codegraph.stores.staging import Staging

# (src_service, src_qualified, dst_service, dst_qualified)
CallTuple = tuple[str, str, str, str]


@dataclass(frozen=True)
class FoundCalls:
    """Результат found_calls(): резолвленные CALLS-рёбра + счётчик отброшенных.

    edges: множество (src_service, src_qualified, dst_service, dst_qualified) — для
        каждого CALLS-ребра staging ОБА конца успешно резолвлены INNER JOIN'ом по id
        в nodes-таблице.
    skipped_dangling: число CALLS-рёбер, исключённых из edges, потому что хотя бы
        один конец (src ИЛИ dst) не резолвится в узел с непустым qualified_name.
        Две причины делят один счётчик (обе означают "ребро нельзя выразить как
        (service, qualified) пару, сравнимую с golden"):
          1. id отсутствует в nodes вовсе — истинный JOIN-промах. Единственный
             известный случай на фикстурах: kyc-worker `run_consumer` дергает
             динамический dispatch `handler(event)`, где `handler` — локальная
             переменная; SCIP резолвит ref в тот же локальный символ, что и её
             def в этом же файле, поэтому build_calls классифицирует вызов как
             первопартийный и создаёт CALLS-ребро — но python_core.extract строит
             Node только для Module/Class/Function (tree-sitter defs), не для
             произвольных локальных переменных, так что у этого dst никогда не
             появляется NodeRec. См. m1b-task-8-report.md, раздел
             "Third discrepancy".
          2. id найден, но у узла пустой/отсутствующий qualified_name — не может
             быть выражен как (service, qualified) пара. На текущей схеме
             (NodeRec.qualified_name — обязательное str-поле) это не встречается
             в реальном пайплайне; проверка защитная.
    """

    edges: set[CallTuple]
    skipped_dangling: int


def found_calls(staging: Staging) -> FoundCalls:
    """CALLS-рёбра staging: оба конца — id → (service, qualified_name) через JOIN по
    id в nodes-таблице. Staging не отдаёт сырой SQL JOIN (только iter_nodes/
    iter_edges), поэтому JOIN делается в памяти — держит evalx независимым от
    деталей SQL staging и от Cypher/store. Ребро с ЛЮБЫМ висячим концом в edges не
    попадает, инкрементит skipped_dangling (см. FoundCalls.__doc__)."""
    node_lookup: dict[str, tuple[str, str]] = {
        n.id: (n.service, n.qualified_name) for n in staging.iter_nodes() if n.qualified_name
    }

    edges: set[CallTuple] = set()
    skipped_dangling = 0
    for e in staging.iter_edges():
        if e.type != "CALLS":
            continue
        src = node_lookup.get(e.src)
        dst = node_lookup.get(e.dst)
        if src is None or dst is None:
            skipped_dangling += 1
            continue
        edges.add((src[0], src[1], dst[0], dst[1]))

    return FoundCalls(edges=edges, skipped_dangling=skipped_dangling)


def load_golden_calls(path: Path) -> set[CallTuple]:
    """Golden CALLS-рёбра из fixtures/golden/edges.yaml (формат M1b Task 8).

    Фильтры:
      - только записи с type == "CALLS" (golden несёт и другие типы: DEPENDS_ON,
        PRODUCES, CONSUMES, INVOKES_ACTIVITY, CALLS_HTTP, HANDLES — вне M1 eval);
      - записи с ключом `mechanism` (например temporal_start) — исключены: это не
        прямой Python-вызов, а отдельный механизм (см. edges.yaml policy-комментарий
        и m1b-task-8-report.md); M1 eval их не учитывает;
      - записи, где dst задан через `channel` (а не `service`+`symbol`), — пропуск;
        по схеме edges.yaml CALLS всегда таргетит code-символ (dst.channel
        встречается только у PRODUCES/CONSUMES/CALLS_HTTP), но фильтр защитный —
        неважно, что появится в golden в будущем, precision_recall не сможет
        сравнить dst без qualified-имени с found_calls, поэтому такая запись
        отбрасывается тут же, а не падает ниже по пайплайну.
    """
    data = yaml.safe_load(path.read_text()) or {}
    out: set[CallTuple] = set()
    for e in data.get("edges", []):
        if e.get("type") != "CALLS":
            continue
        if "mechanism" in e:
            continue
        dst = e["dst"]
        if "channel" in dst:
            continue
        src = e["src"]
        out.add((src["service"], src["symbol"], dst["service"], dst["symbol"]))
    return out


def precision_recall(found: set[CallTuple], golden: set[CallTuple]) -> dict:
    """Precision/recall found против golden.

    precision = |found ∩ golden| / |found|; recall = |found ∩ golden| / |golden|.

    Конвенция для 0/0 (математически не определено): 1.0 — vacuous truth. Пустой
    found => ноль предсказаний => ноль ЛОЖНЫХ предсказаний => precision 1.0 по
    определению "доли верных среди сделанных предсказаний" (сделанных предсказаний
    нет, значит и неверных нет). Симметрично пустой golden => нечего было искать
    => ничего не пропущено => recall 1.0. Каждая доля считается независимо от
    другой: подмена этой конвенции на 0.0 не изменила бы гейт (при непустом golden
    recall из пустого found будет честным 0.0 и провалит гейт сам по себе) — выбор
    касается только синтетических edge-case юнитов, не реального прогона (found и
    golden на фикстурах непустые).

    Возвращает dict с ровно пятью ключами: precision (float), recall (float),
    tp (int, |found ∩ golden|), fp_list (sorted list, found − golden),
    fn_list (sorted list, golden − found).
    """
    tp = found & golden
    fp = found - golden
    fn = golden - found
    precision = (len(tp) / len(found)) if found else 1.0
    recall = (len(tp) / len(golden)) if golden else 1.0
    return {
        "precision": precision,
        "recall": recall,
        "tp": len(tp),
        "fp_list": sorted(fp),
        "fn_list": sorted(fn),
    }
