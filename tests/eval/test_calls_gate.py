"""M1 gate: CALLS precision/recall пайплайна (analyze_service, реальный scip-python)
против golden (fixtures/golden/edges.yaml) — финальная проверка milestone M1.

Медленно при первом запуске (npx скачивает scip-python; три сервиса). Гейт НЕ
ослабляется при провале и golden НЕ подгоняется под находки пайплайна — см.
m1b-task-9-report.md "Self-review"/"Concerns" по требованию task-брифа.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from codegraph.config.loader import load_workspace
from codegraph.evalx.calls_eval import found_calls, load_golden_calls, precision_recall
from codegraph.pipeline.analyze import analyze_service
from codegraph.stores.staging import Staging

pytestmark = pytest.mark.scip

FIXTURES = Path(__file__).parents[2] / "fixtures"
GOLDEN_EDGES = FIXTURES / "golden" / "edges.yaml"

PRECISION_GATE = 0.95
RECALL_GATE = 0.85

# Известный случай (M1b Task 8-отчёт, "Third discrepancy"): kyc-worker
# app.consumer_main.run_consumer вызывает динамический dispatch handler(event), где
# handler -- локальная переменная; SCIP резолвит ref в тот же локальный символ, что
# и её def в этом файле, поэтому build_calls создаёт CALLS-ребро на него, но
# python_core.extract не строит Node для произвольных локальных переменных (только
# Module/Class/Function) -- JOIN-промах в found_calls. Единственный ожидаемый
# dangling на фикстурах; если число изменилось -- расследовать, НЕ подгонять
# константу вслепую (это может значить как новую находку, так и регрессию).
EXPECTED_SKIPPED_DANGLING = 1


@pytest.mark.skipif(shutil.which("npx") is None, reason="npx not available")
def test_calls_precision_recall_gate(tmp_path):
    cfg = load_workspace(FIXTURES / "workspace.yaml")
    staging = Staging(tmp_path / "staging.db")
    cache_dir = tmp_path / "scip-cache"

    reports = [
        analyze_service(svc, staging, cache_dir, runner=None) for svc in cfg.services
    ]

    degraded = [(r["service"], r["reason"]) for r in reports if r["degraded"]]
    assert not degraded, (
        f"real scip expected for all fixture services, got degraded: {degraded}"
    )

    result = found_calls(staging)
    golden = load_golden_calls(GOLDEN_EDGES)
    pr = precision_recall(result.edges, golden)

    print(
        f"\n[M1 gate] precision={pr['precision']:.4f} recall={pr['recall']:.4f} "
        f"tp={pr['tp']} fp={len(pr['fp_list'])} fn={len(pr['fn_list'])} "
        f"found={len(result.edges)} golden={len(golden)} "
        f"skipped_dangling={result.skipped_dangling}"
    )

    assert pr["precision"] >= PRECISION_GATE, (
        f"precision {pr['precision']:.4f} < {PRECISION_GATE}; "
        f"fp ({len(pr['fp_list'])}): {pr['fp_list']}"
    )
    assert pr["recall"] >= RECALL_GATE, (
        f"recall {pr['recall']:.4f} < {RECALL_GATE}; "
        f"fn ({len(pr['fn_list'])}): {pr['fn_list']}"
    )

    # Документирующий ассерт (не гейт precision/recall): подтверждает, что
    # dangling-счётчик found_calls ловит ровно известный случай, не больше и не
    # меньше. Провал здесь -- сигнал расследовать, не подгонять число.
    assert result.skipped_dangling == EXPECTED_SKIPPED_DANGLING, (
        f"skipped_dangling {result.skipped_dangling} != {EXPECTED_SKIPPED_DANGLING} "
        "(known case: kyc-worker dynamic handler(event) dispatch -> local variable, "
        "no Node created; see m1b-task-8-report.md 'Third discrepancy'). Investigate "
        "before touching this constant -- it may be a real regression or a new case."
    )
