"""S1–S6 analyze_service orchestration: деградированный (fallback) путь через ФЕЙКОВЫЙ
runner, отчёт-словарь, venv-автодетект. Не-деградированный путь юнит-тестируется через
фейковый runner, отдающий валидный (но пустой) SCIP-индекс — без реального scip-python
(это делает интеграционный tests/integration/test_pipeline_real.py, marker scip)."""

from __future__ import annotations

from pathlib import Path

from codegraph.config.models import (
    ChannelSpec,
    ConsumerIdiom,
    ProducerIdiom,
    ServiceConfig,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip import scip_pb2
from codegraph.resolvers.scip.runner import ScipRunError, ScipRunResult
from codegraph.stores.staging import Staging

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "document_management"


class _AlwaysFailRunner:
    """Симулирует ScipRunError у scip-python (сеть/npx/venv недоступны) — форсирует
    degraded-путь без реального subprocess."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("simulated scip-python failure")


def test_analyze_degraded_path_falls_back_to_heuristic_calls(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert report["degraded"] is True

    nodes = list(st.iter_nodes())
    assert len(nodes) > 0
    assert any(n.id == "svc:document-management" and n.kind == "Service" for n in nodes)

    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert len(calls) > 0
    assert all(e.resolution == "heuristic" and e.confidence == 0.6 for e in calls)


def test_analyze_degraded_report_dict_fields(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    expected_keys = {
        "service", "files", "defs", "refs", "malformed_ranges", "nodes", "edges",
        "imports_external", "calls_joined", "calls_unresolved", "calls_external",
        "degraded", "reason", "from_cache",
    }
    assert expected_keys <= report.keys()
    assert report["service"] == "document-management"
    assert report["files"] == 8
    assert report["degraded"] is True
    assert report["reason"]  # непустая строка-причина
    assert report["from_cache"] is False
    assert report["nodes"] > 0
    assert report["calls_joined"] > 0


def _empty_scip_bytes() -> bytes:
    return scip_pb2.Index().SerializeToString()


class _FakeSuccessRunner:
    """Пишет валидный, но пустой SCIP-индекс: проверяет НЕ-деградированную ветку
    оркестрации (from_cache passthrough, defs/refs/malformed_ranges из ReaderStats,
    degraded=False/reason=None) без сборки настоящего дерева occurrences."""

    def __init__(self, from_cache: bool):
        self.from_cache = from_cache
        self.calls: list[dict] = []

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        self.calls.append({
            "service_name": service_name, "service_path": service_path,
            "venv": venv, "cache_dir": cache_dir, "tree_hash": tree_hash,
        })
        cache_dir.mkdir(parents=True, exist_ok=True)
        out = cache_dir / f"{service_name}-{tree_hash}.scip"
        out.write_bytes(_empty_scip_bytes())
        return ScipRunResult(scip_path=out, from_cache=self.from_cache)


def test_analyze_static_path_not_degraded(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    runner = _FakeSuccessRunner(from_cache=True)
    report = analyze_service(svc, st, tmp_path / "cache", runner=runner)

    assert report["degraded"] is False
    assert report["reason"] is None
    assert report["from_cache"] is True
    assert report["defs"] == 0 and report["refs"] == 0 and report["malformed_ranges"] == 0
    assert report["files"] == 8


def _minimal_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "main.py").write_text("def f():\n    pass\n")
    return svc_root


class _SpyDegradedRunner:
    """Пишет call-лог, затем всегда кидает ScipRunError — форсирует degraded-путь,
    не требуя валидного SCIP-протобафа; достаточно для проверки venv-автодетекта."""

    def __init__(self):
        self.calls: list[dict] = []

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        self.calls.append({"service_name": service_name, "venv": venv})
        raise ScipRunError("forced for venv-autodetect test")


def test_venv_autodetect_explicit_python(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    (svc_root / "myenv").mkdir()
    svc = ServiceConfig(name="svc", path=svc_root, python="myenv")
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] == svc_root / "myenv"


def test_venv_autodetect_dot_venv_fallback_when_exists(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    (svc_root / ".venv").mkdir()
    svc = ServiceConfig(name="svc", path=svc_root)
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] == svc_root / ".venv"


def test_venv_autodetect_none_when_absent(tmp_path):
    svc_root = _minimal_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    spy = _SpyDegradedRunner()
    analyze_service(svc, Staging(tmp_path / "s.db"), tmp_path / "cache", runner=spy)
    assert spy.calls[0]["venv"] is None


class _FakeDefaultScipRunnerClass:
    """Заглушка на месте класса ScipRunner: конструктор без аргументов (как ScipRunner()),
    .run() сразу кидает ScipRunError — доказывает, что runner=None доходит до конструирования
    default ScipRunner() (см. codegraph.pipeline.analyze.ScipRunner, монки-патченный ниже),
    без реального subprocess/сети."""

    def __init__(self):
        pass

    def run(self, *a, **kw):
        raise ScipRunError("fake default runner")


def test_default_runner_is_constructed_when_none(tmp_path, monkeypatch):
    svc_root = _minimal_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    monkeypatch.setattr("codegraph.pipeline.analyze.ScipRunner", _FakeDefaultScipRunnerClass)
    report = analyze_service(svc, st, tmp_path / "cache", runner=None)
    assert report["degraded"] is True


# -- M2 T4: fastapi extractor wiring (active_idioms), degraded/fallback path --
#
# Exercised through the SAME degraded fallback path as the tests above (no real
# scip-python): def_symbol_lookup is covered end-to-end (fallback.resolve_service lays
# down a synthetic def for every DefFact, including first-party module-level
# functions), so route/HANDLES/role wiring is provable here. DEPENDS_ON is NOT provable
# here (see test below and fastapi_ext.py's module docstring for why) -- its full
# resolution is proven at the extract_fastapi unit level instead
# (test_fastapi_extractor.py, stubbed ref_symbol_lookup), matching the brief's own
# "юнит: стаб; интеграцию покроет T9".

ORDERS_FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "orders_api"


def test_analyze_fastapi_inactive_by_default_no_route_roles_or_channels(tmp_path):
    """active_idioms defaults to frozenset() -- opt-in, so every pre-existing caller
    (incl. every other test in this file, and cli.py as of this task) is unaffected."""
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    nodes = list(st.iter_nodes())
    assert nodes  # sanity: the service was actually analyzed
    assert not any(n.roles for n in nodes)
    assert not any(n.kind == "Channel" for n in nodes)
    assert not any(e.type in ("HANDLES", "DEPENDS_ON") for e in st.iter_edges())


def test_analyze_fastapi_active_wires_route_roles_channels_and_handles(tmp_path):
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"fastapi"}),
    )

    by_name = {n.name: n for n in st.iter_nodes() if n.kind == "Function"}
    create_order, get_order = by_name["create_order"], by_name["get_order"]

    assert create_order.roles == ("RouteHandler",)
    assert create_order.props["http_method"] == "POST"
    assert create_order.props["path_template"] == "/orders"
    assert get_order.roles == ("RouteHandler",)
    assert get_order.props["http_method"] == "GET"
    assert get_order.props["path_template"] == "/orders/{order_id}"

    channel_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert channel_ids == {
        "chan:http:orders-api:POST /orders",
        "chan:http:orders-api:GET /orders/{order_id}",
    }

    handles = {e.src: e.dst for e in st.iter_edges() if e.type == "HANDLES"}
    assert handles["chan:http:orders-api:POST /orders"] == create_order.id
    assert handles["chan:http:orders-api:GET /orders/{order_id}"] == get_order.id


def test_analyze_fastapi_active_degraded_fallback_cannot_resolve_depends_on(tmp_path):
    """Documented gap: the degraded fallback resolver builds refs purely from
    facts.calls (extractors/calls.py's join source), which never visits parameter
    default/annotation expressions (M1a carried-forward: "calls in default values ...
    are not visited") -- Depends(get_db)'s `get_db` identifier never gets a ref laid
    down at its span, so ref_symbol_lookup finds nothing and DEPENDS_ON stays
    unresolved through this specific path, by design of the fallback resolver (not a
    fastapi_ext bug -- see test_fastapi_extractor.py for proof it resolves correctly
    given a real/stubbed ref-lookup)."""
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"fastapi"}),
    )
    assert not any(e.type == "DEPENDS_ON" for e in st.iter_edges())


def test_analyze_non_fastapi_active_idiom_is_a_noop_for_fastapi_wiring(tmp_path):
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"aiokafka"}),
    )
    assert not any(n.roles for n in st.iter_nodes())
    assert not any(n.kind == "Channel" for n in st.iter_nodes())


# -- M2 T5: kafka_ext/temporal_ext wiring (idioms param, active_idioms), degraded path --
#
# kafka is DATA-driven: activation depends on the `idioms` PARAMETER (effective
# ServiceIdioms), NOT active_idioms set membership -- proven below by never putting
# "aiokafka"/etc in active_idioms while still exercising kafka wiring. The outbox
# producer's own call-site (`outbox.add_event(...)`, receiver a same-scope AssignFact)
# resolves at RECEIVER tier through the degraded fallback path (fallback.resolve_service
# only lays refs at CallFact callee spans of TOP-LEVEL defs -- `add_event` is a method,
# never top-level, so STATIC never fires here; RECEIVER needs no SCIP at all -- full
# STATIC-tier proof lives in test_kafka_extractor.py's stubbed-lookup unit test).
#
# temporal's @workflow.defn/@activity.defn roles need no SCIP either (pure decorator-text
# matching) and DO wire through the degraded path; INVOKES_ACTIVITY/start_workflow need a
# ref at an ARGUMENT's name span, which the degraded fallback never lays down (same
# documented gap as fastapi_ext's DEPENDS_ON) -- proven unresolved below, full resolution
# proven at the extract_temporal unit level (test_temporal_extractor.py, stubbed
# ref_symbol_lookup), matching the brief's own "юнит: стаб; интеграцию покроет T9".

KYC_FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "kyc_worker"

OUTBOX_IDIOM = ProducerIdiom(
    name="outbox",
    call="app.db.outbox.OutboxRepository.add_event",
    channel=ChannelSpec(
        kind="event_type", event_type_from=ValueSpec(arg=0), topic=ValueSpec(const="orders.events"),
    ),
)


def test_analyze_kafka_inactive_by_default_no_roles_or_channels(tmp_path):
    """idioms defaults to None -- opt-in, so every pre-existing caller (incl. every
    other test in this file, and cli.py before this task) is unaffected."""
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert not any(n.roles for n in st.iter_nodes())
    assert not any(n.kind == "Channel" for n in st.iter_nodes())
    assert not any(e.type in ("PRODUCES", "CONSUMES") for e in st.iter_edges())


def test_analyze_kafka_active_wires_producer_role_channel_and_produces_edge(tmp_path):
    """Activation is idioms-driven, NOT active_idioms-driven: active_idioms stays
    empty here on purpose."""
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(producers=[OUTBOX_IDIOM]),
    )

    place = next(n for n in st.iter_nodes() if n.kind == "Function" and n.name == "place")
    assert place.roles == ("MessageProducer",)

    channel_ids = {n.id for n in st.iter_nodes() if n.kind == "Channel"}
    assert channel_ids == {"chan:event_type:OrderCreated", "chan:kafka_topic:orders.events"}

    produces = [e for e in st.iter_edges() if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].src == place.id
    assert produces[0].dst == "chan:event_type:OrderCreated"
    assert produces[0].resolution == "heuristic" and produces[0].confidence == 0.8

    assert any(
        e.type == "CONTAINS" and e.src == "chan:kafka_topic:orders.events"
        and e.dst == "chan:event_type:OrderCreated"
        for e in st.iter_edges()
    )


def test_analyze_kafka_active_with_no_producers_or_consumers_is_a_noop(tmp_path):
    svc = ServiceConfig(name="orders-api", path=ORDERS_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(),
    )
    assert not any(n.roles for n in st.iter_nodes())
    assert not any(n.kind == "Channel" for n in st.iter_nodes())


def test_analyze_temporal_inactive_by_default_no_roles(tmp_path):
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert not any(n.roles for n in st.iter_nodes())
    assert not any(e.type == "INVOKES_ACTIVITY" for e in st.iter_edges())


def test_analyze_temporal_active_wires_workflow_and_activity_roles(tmp_path):
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"temporal"}),
    )

    workflow = next(n for n in st.iter_nodes() if n.kind == "Class" and n.name == "KycWorkflow")
    assert workflow.roles == ("TemporalWorkflow",)
    assert workflow.props["workflow_name"] == "KycWorkflow"

    activity = next(
        n for n in st.iter_nodes() if n.kind == "Function" and n.name == "verify_documents"
    )
    assert activity.roles == ("TemporalActivity",)


def test_analyze_temporal_active_degraded_fallback_cannot_resolve_invokes_activity(tmp_path):
    """Documented gap (mirrors fastapi_ext's own DEPENDS_ON note): the degraded fallback
    resolver only lays refs at CallFact callee spans -- `verify_documents` here is an
    ARGUMENT reference (execute_activity's arg0), never itself a callee span, so no ref
    ever lands there and ref_symbol_lookup finds nothing through this path. Not a
    temporal_ext bug -- see test_temporal_extractor.py for proof it resolves correctly
    given a real/stubbed ref-lookup."""
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"temporal"}),
    )
    assert not any(e.type == "INVOKES_ACTIVITY" for e in st.iter_edges())
    assert st.claims_for("temporal_start_mark") == []


def test_analyze_non_temporal_active_idiom_is_a_noop_for_temporal_wiring(tmp_path):
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"fastapi"}),
    )
    assert not any(n.roles for n in st.iter_nodes())


def test_analyze_kafka_and_temporal_can_both_be_active_together(tmp_path):
    """Sanity: the two extractors don't clobber each other's roles/edges/claims when
    both fire in the same analyze_service call (as cli.py's real wiring always does)."""
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    dispatch_idiom = ConsumerIdiom(
        name="dispatch-map", kind="dispatch_dict",
        registrar_call="app.consumers.base.register_handlers",
        topic=ValueSpec(const="orders.events"), event_type_from="dict_key",
    )
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"temporal"}),
        idioms=ServiceIdioms(consumers=[dispatch_idiom]),
    )

    workflow = next(n for n in st.iter_nodes() if n.kind == "Class" and n.name == "KycWorkflow")
    assert workflow.roles == ("TemporalWorkflow",)
    # dispatch_dict's registrar_call resolves at IMPORT_NAME tier through the degraded
    # path (confirmed structurally in this task's report) -- handler ref resolution
    # itself needs a value-arg SCIP ref, which the fallback never lays down either.
    assert st.claims_for("temporal_start_mark", service="kyc-worker") == []
