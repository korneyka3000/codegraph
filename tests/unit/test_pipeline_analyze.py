"""S1–S6 analyze_service orchestration: деградированный (fallback) путь через ФЕЙКОВЫЙ
runner, отчёт-словарь, venv-автодетект. Не-деградированный путь юнит-тестируется через
фейковый runner, отдающий валидный (но пустой) SCIP-индекс — без реального scip-python
(это делает интеграционный tests/integration/test_pipeline_real.py, marker scip)."""

from __future__ import annotations

from pathlib import Path

from codegraph.config.models import (
    BaseUrlSpec,
    ChannelSpec,
    ConsumerIdiom,
    GenericArgSpec,
    HttpClientIdiom,
    HttpRouteFromSpec,
    HttpVerbFromSpec,
    ProducerIdiom,
    ServiceConfig,
    ServiceIdioms,
    ValueSpec,
)
from codegraph.pipeline.analyze import analyze_service
from codegraph.pipeline.diff import ServiceDelta
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
        # M6 T2 review Important-1: http idiom failure counters are part of every
        # full/incremental report shape (0 when the http_client extractor never ran).
        "http_url_unresolved", "http_verb_unresolved", "http_route_unresolved",
        # M6 T3: same precedent, kafka's own base_class honest-miss counter (0 when
        # kafka is inactive or no base_class idiom is configured).
        "consumer_base_class_no_generic",
        # M6 T4: same precedent again, kafka's own producer_unresolved_channel
        # honest-miss counter (0 when kafka is inactive or every producer resolved).
        "producer_unresolved_channel",
        # M7 T4 (OPEN R3): same precedent again, temporal's own signal-sender
        # honest-miss counter (0 when temporal is inactive or every `.signal(...)`
        # call resolved/was silently skipped as non-signal-shaped noise).
        "signal_name_unresolved",
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


# -- M2 T6: http_client_ext wiring (idioms param), degraded path --
#
# Unlike fastapi/kafka/temporal, http_client_ext needs NO ref_symbol_lookup at all (see
# its own module docstring: pure structural facts + consts.resolve_arg, zero SCIP
# dependency) -- so, unlike every other T4-T6 domain extractor's wiring test, there is
# no "cannot resolve under the degraded fallback" gap to document here: claims come out
# fully resolved below using ONLY the degraded _AlwaysFailRunner path.

DEFAULT_SDK_IDIOM = HttpClientIdiom(
    name="default-sdk", file_glob="**/clients/*_client.py", class_glob="*Client",
    base_url=BaseUrlSpec(attr="self._base_url", env="DOCUMENT_MANAGEMENT_URL"),
)


def test_analyze_http_client_inactive_by_default_no_claims(tmp_path):
    """idioms defaults to None -- opt-in, so every pre-existing caller is unaffected,
    same convention as kafka's own "inactive by default" test."""
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert st.claims_for("http_call") == []


def test_analyze_http_client_active_with_no_http_clients_is_a_noop(tmp_path):
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(),
    )
    assert st.claims_for("http_call") == []


def test_analyze_http_client_active_wires_http_call_claims(tmp_path):
    """Activation is idioms-driven (idioms.http_clients non-empty), NOT
    active_idioms-driven -- active_idioms stays empty here on purpose, mirroring kafka's
    own wiring test. Claims land in staging via add_claims, readable back through
    claims_for exactly like temporal_start_mark (kind passed separately, "_service"/
    "_relpath" injected by claims_for itself)."""
    svc = ServiceConfig(name="kyc-worker", path=KYC_FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(http_clients=[DEFAULT_SDK_IDIOM]),
    )

    claims = st.claims_for("http_call", service="kyc-worker")
    assert len(claims) == 2
    verbs_paths = {(c["verb"], c["path_template"]) for c in claims}
    assert verbs_paths == {("GET", "/documents/{doc_id}"), ("POST", "/documents")}
    assert all(c["base_url_env"] == "DOCUMENT_MANAGEMENT_URL" for c in claims)
    assert all(c["resolution_hint"] == "static" for c in claims)
    assert all(c["_relpath"] == "app/clients/document_management_client.py" for c in claims)

    # http_client_ext emits no roles/edges/channels/node_props at all (master plan:
    # "роли для клиентов не вводим; CALLS_HTTP делает S7") -- mirrors kafka's own
    # "structurally empty besides claims" shape check.
    assert not any(n.roles for n in st.iter_nodes())
    assert not any(e.type == "CALLS_HTTP" for e in st.iter_edges())


# -- M6 T2 review Important-1: http idiom failure counters flow into the per-service
# report dict (hr.stats was dead-on-arrival before: analyze consumed only hr.claims).
# Aggregated across files inside _extract_join_and_stage (imports_external precedent),
# then copied into BOTH the full and incremental report shapes.

DECORATOR_SDK_IDIOM = HttpClientIdiom(
    name="decorator-sdk",
    file_glob="**/clients/*.py",
    class_glob="*Client",
    route_from=HttpRouteFromSpec(decorator="path_template", arg=0),
    call="driver.fetch_content|driver.fetch",
    verb_from=HttpVerbFromSpec(request_ctor="Request", enum="Method"),
)

# One resolvable claim (get_ok) + one verb-unresolvable method (get_noverb: decorator
# and driver call present, NO Request(Method.X, ...) anywhere in the body).
_NOVERB_CLIENT_SRC = (
    "from some_sdk import BaseClient, Method, Request, path_template\n"
    "\n"
    "\n"
    "class DMoutClient(BaseClient):\n"
    '    @path_template("/v1/ok/{id}")\n'
    "    async def get_ok(self, id):\n"
    "        request = Request(Method.GET, self.host)\n"
    "        return await self.driver.fetch_content(request)\n"
    "\n"
    '    @path_template("/v1/noverb/{id}")\n'
    "    async def get_noverb(self, id):\n"
    "        return await self.driver.fetch_content(self.host)\n"
)


def _decorator_sdk_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app" / "clients").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "clients" / "__init__.py").write_text("")
    (svc_root / "app" / "clients" / "dmout_client.py").write_text(_NOVERB_CLIENT_SRC)
    return svc_root


def test_analyze_full_report_carries_http_idiom_failure_counters(tmp_path):
    svc = ServiceConfig(name="svc", path=_decorator_sdk_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]),
    )

    assert report["http_verb_unresolved"] == 1
    assert report["http_url_unresolved"] == 0
    assert report["http_route_unresolved"] == 0
    # sanity: the resolvable sibling method still produced its claim -- the counter
    # counts the miss, it does not swallow the file's other hits.
    claims = st.claims_for("http_call", service="svc")
    assert len(claims) == 1
    assert claims[0]["path_template"] == "/v1/ok/{id}"


def test_analyze_incremental_report_carries_http_idiom_failure_counters(tmp_path):
    """Same counters on the INCREMENTAL report shape: first-run incremental (no prior
    staged files) makes every scanned file `added` -> stale -> extracted, so the same
    verb-unresolvable method is seen by the same aggregation."""
    svc = ServiceConfig(name="svc", path=_decorator_sdk_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        idioms=ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["http_verb_unresolved"] == 1
    assert report["http_url_unresolved"] == 0
    assert report["http_route_unresolved"] == 0


# -- M6 T3: kafka's own base_class honest-miss counter flows into the per-service
# report dict, same precedent as the http_* counters just above (kr.stats was
# discarded entirely before this -- only kr.roles/channels/edges were consumed).

BASE_CLASS_NO_GENERIC_IDIOM = ConsumerIdiom(
    name="base-consumer-subclass", kind="base_class",
    base_class="kyc_base_consumer.base.BaseConsumer",
    handler_method="process_event",
    event_type_from=GenericArgSpec(generic_arg=0),
)

# Bare (non-generic) subclass -- base_class idiom's target base matches textually
# (the from-import line is the required corroboration, M6 T3 review Important-1:
# bare name equality with zero import evidence no longer matches at all), but there
# is no subscript, so extract_kafka bumps consumer_base_class_no_generic instead of
# producing a claim (see test_kafka_extractor.py's own unit-level proof).
_NO_GENERIC_CONSUMER_SRC = (
    "from kyc_base_consumer.base import BaseConsumer\n"
    "\n"
    "\n"
    "class OCRDataConsumer(BaseConsumer):\n"
    "    async def process_event(self, event) -> bool:\n"
    "        return True\n"
)


def _base_class_no_generic_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app" / "consumers").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "consumers" / "__init__.py").write_text("")
    (svc_root / "app" / "consumers" / "ocr.py").write_text(_NO_GENERIC_CONSUMER_SRC)
    return svc_root


def test_analyze_full_report_carries_kafka_base_class_no_generic_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_base_class_no_generic_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(consumers=[BASE_CLASS_NO_GENERIC_IDIOM]),
    )

    assert report["consumer_base_class_no_generic"] == 1
    assert not any(n.roles for n in st.iter_nodes())
    assert not any(e.type == "CONSUMES" for e in st.iter_edges())


def test_analyze_incremental_report_carries_kafka_base_class_no_generic_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_base_class_no_generic_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        idioms=ServiceIdioms(consumers=[BASE_CLASS_NO_GENERIC_IDIOM]),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["consumer_base_class_no_generic"] == 1


# -- M6 T4 (GAPS §6/pilot gap 5): kafka's producer_unresolved_channel counter flows
# into the per-service report dict too, same precedent as base_class_no_generic just
# above. kafka_ext.py's _Sink.stats already counted producer_unresolved_channel
# (kafka_topic/event_type producer paths bump it on any unresolved value -- missing
# kwarg, a dynamic attribute expression like `payload.topic_name`, ...) well before
# this task -- but analyze.py's own kafka_stats aggregator only ever picked up
# consumer_base_class_no_generic, so a would-be PRODUCES claim that died on an
# unresolved topic/event_type value vanished from the report without a trace.

WRAPPER_PRODUCER_IDIOM = ProducerIdiom(
    name="kyc-event-publisher-wrapper",
    call="app.services.producer.KYCEventPublisher.publish",
    channel=ChannelSpec(kind="kafka_topic", name_from=ValueSpec(arg=1)),
)

# Dynamic topic argument (`payload.topic_name`, an attribute expression) -- the
# exact pilot gap 5 shape: resolve_arg can't statically know an arbitrary
# attribute's runtime value, so the topic never resolves.
_DYNAMIC_TOPIC_WRAPPER_SRC = (
    "from app.services.producer import KYCEventPublisher\n"
    "\n"
    "\n"
    "async def use(publisher: KYCEventPublisher, payload):\n"
    "    await publisher.publish(payload.body, payload.topic_name, payload.customer_uid)\n"
)


def _wrapper_producer_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app" / "activities").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "activities" / "__init__.py").write_text("")
    (svc_root / "app" / "activities" / "kafka_events.py").write_text(_DYNAMIC_TOPIC_WRAPPER_SRC)
    return svc_root


def test_analyze_full_report_carries_producer_unresolved_channel_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_wrapper_producer_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        idioms=ServiceIdioms(producers=[WRAPPER_PRODUCER_IDIOM]),
    )

    assert report["producer_unresolved_channel"] == 1
    assert not any(n.roles for n in st.iter_nodes())
    assert not any(e.type == "PRODUCES" for e in st.iter_edges())


def test_analyze_incremental_report_carries_producer_unresolved_channel_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_wrapper_producer_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        idioms=ServiceIdioms(producers=[WRAPPER_PRODUCER_IDIOM]),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["producer_unresolved_channel"] == 1


# -- M7 T4 (OPEN R3): temporal's own signal_name_unresolved counter flows into the
# per-service report dict too, same precedent as producer_unresolved_channel just
# above. temporal is active_idioms-gated (unlike kafka's idioms-object gating --
# see analyze.py's own module docstring), so active_idioms=frozenset({"temporal"})
# is what turns the extractor on here, not an `idioms=ServiceIdioms(...)` kwarg.
#
# The RESOLVED path gets its own end-to-end wiring test too (M7 T4 review: the
# `domain_channels.extend(tr.channels)` glue is temporal's FIRST-ever channels
# output, and the extractor's own unit tests bypass analyze.py's node_ids/consts/
# upsert wiring entirely) -- same template as
# test_analyze_kafka_active_wires_producer_role_channel_and_produces_edge above.

_SIGNAL_HANDLER_AND_SENDER_SRC = (
    "class SurveyWorkflow:\n"
    '    @workflow.signal(name="complete-survey")\n'
    "    async def complete_survey(self, payload):\n"
    "        pass\n"
    "\n"
    "\n"
    "async def notify(handle, payload):\n"
    '    await handle.signal("complete-survey", payload)\n'
)


def _signal_handler_and_sender_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app" / "workflows").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "workflows" / "__init__.py").write_text("")
    (svc_root / "app" / "workflows" / "survey.py").write_text(_SIGNAL_HANDLER_AND_SENDER_SRC)
    return svc_root


def test_analyze_temporal_active_wires_signal_handler_channel_and_both_edges(tmp_path):
    svc = ServiceConfig(name="svc", path=_signal_handler_and_sender_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"temporal"}),
    )

    handler = next(
        n for n in st.iter_nodes() if n.kind == "Function" and n.name == "complete_survey"
    )
    assert handler.roles == ("TemporalSignalHandler",)
    assert handler.props["signal_kind"] == "signal"

    channels = [n for n in st.iter_nodes() if n.kind == "Channel"]
    assert [c.id for c in channels] == ["chan:temporal_signal:complete-survey"]
    assert channels[0].props["channel_kind"] == "temporal_signal"

    consumes = [e for e in st.iter_edges() if e.type == "CONSUMES"]
    assert len(consumes) == 1
    assert consumes[0].src == handler.id
    assert consumes[0].dst == "chan:temporal_signal:complete-survey"
    assert consumes[0].resolution == "static" and consumes[0].confidence == 1.0

    notify = next(n for n in st.iter_nodes() if n.kind == "Function" and n.name == "notify")
    produces = [e for e in st.iter_edges() if e.type == "PRODUCES"]
    assert len(produces) == 1
    assert produces[0].src == notify.id
    assert produces[0].dst == "chan:temporal_signal:complete-survey"
    assert produces[0].resolution == "heuristic" and produces[0].confidence == 0.6
    assert produces[0].props["mechanism"] == "temporal_signal"

    assert report["signal_name_unresolved"] == 0


_UNRESOLVABLE_SIGNAL_SRC = (
    "async def notify(handle, signal_name, payload):\n"
    "    await handle.signal(signal_name, payload)\n"
)


def _signal_sender_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app" / "workflows").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "workflows" / "__init__.py").write_text("")
    (svc_root / "app" / "workflows" / "notify.py").write_text(_UNRESOLVABLE_SIGNAL_SRC)
    return svc_root


def test_analyze_full_report_carries_signal_name_unresolved_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_signal_sender_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        active_idioms=frozenset({"temporal"}),
    )

    assert report["signal_name_unresolved"] == 1
    assert not any(e.type == "PRODUCES" for e in st.iter_edges())


def test_analyze_incremental_report_carries_signal_name_unresolved_counter(tmp_path):
    svc = ServiceConfig(name="svc", path=_signal_sender_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        active_idioms=frozenset({"temporal"}),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["signal_name_unresolved"] == 1


def test_analyze_skip_report_has_signal_name_unresolved_key_defaulted_to_zero(tmp_path):
    """_skip_report's own hardcoded-zeros convention (analyze.py: "keeping all three
    report shapes (full/incremental/skipped) key-uniform") -- a skipped service ran
    no extractors, so signal_name_unresolved is definitionally 0, same as every
    other M6/M7 honest-miss counter."""
    svc = ServiceConfig(name="svc", path=_signal_sender_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        active_idioms=frozenset({"temporal"}),
    )

    empty_delta = ServiceDelta(
        added=(), changed=(), deleted=(), unchanged=("app/workflows/notify.py",),
    )
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FailIfCalledRunner(),
        active_idioms=frozenset({"temporal"}), incremental=True,
        prior_delta=empty_delta, fingerprint_ok=True,
    )

    assert report["mode"] == "skipped"
    assert report["signal_name_unresolved"] == 0


# -- M4 T5: incremental analyze_service -- mode tagging, skip precondition,
# degraded-in-incremental fallback. These are fake-runner (fast, no real scip-python)
# tests covering the ORCHESTRATION contract: mode dispatch, the skip precondition
# (prior_delta.empty AND fingerprint_ok), ScipRunError-during-incremental falling back
# to the full path, and stale-set detection driven by genuine on-disk content changes
# (service_delta needs no SCIP at all). The one thing NO fake runner can prove --
# cross-file ref_dirty detection, which depends on pyright/scip-python actually
# re-resolving a renamed symbol's references -- is covered separately by the
# scip-marked tests/integration/test_analyze_incremental.py.


class _FailIfCalledRunner:
    """Proves skip mode does ZERO scip work -- any call to .run() is itself a test
    failure, not just an unwanted side effect."""

    def run(self, *a, **kw):
        raise AssertionError("skip mode must not invoke the scip runner at all")


def test_analyze_full_path_default_reports_mode_full(tmp_path):
    """Regression pin: the existing degraded-report-fields test only asserts
    expected_keys <= report.keys() (subset check), which would NOT catch a missing
    "mode" key on its own -- this test pins it directly. The plain default call
    (incremental=False) is exactly today's pre-M4-T5 full path; "mode" is the only
    new key it should ever gain."""
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())
    assert report["mode"] == "full"
    assert "stale_files" not in report


def test_analyze_incremental_skip_when_delta_empty_and_fingerprint_ok(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())
    counts_before = st.counts_for_service(svc.name)

    empty_delta = ServiceDelta(added=(), changed=(), deleted=(), unchanged=("app/main.py",))
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FailIfCalledRunner(),
        incremental=True, prior_delta=empty_delta, fingerprint_ok=True,
    )

    assert report["mode"] == "skipped"
    assert "stale_files" not in report
    # zero staging writes: current per-service counts are UNCHANGED by the skip call.
    assert st.counts_for_service(svc.name) == counts_before
    assert report["files"] == counts_before["files"]
    assert report["defs"] == counts_before["defs"]
    assert report["refs"] == counts_before["refs"]
    assert report["nodes"] == counts_before["nodes"]
    assert report["edges"] == counts_before["edges"]
    assert report["degraded"] is False
    assert report["reason"] is None
    assert report["from_cache"] is False


def test_analyze_incremental_not_skipped_when_fingerprint_not_ok(tmp_path):
    """prior_delta.empty alone is not enough -- a config-fingerprint mismatch
    (fingerprint_ok=False) must force the real incremental branch to run, even
    though nothing changed on disk."""
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    empty_delta = ServiceDelta(added=(), changed=(), deleted=(), unchanged=("x",))
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True, prior_delta=empty_delta, fingerprint_ok=False,
    )
    assert report["mode"] == "incremental"
    # nothing on disk changed and the fake scip index is empty (no refs anywhere)
    # -> no file is content-changed, added, or ref-dirty.
    assert report["stale_files"] == 0


def test_analyze_incremental_not_skipped_when_prior_delta_non_empty(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    non_empty_delta = ServiceDelta(added=("new.py",), changed=(), deleted=(), unchanged=())
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True, prior_delta=non_empty_delta, fingerprint_ok=True,
    )
    assert report["mode"] == "incremental"


def test_analyze_incremental_without_prior_delta_never_skips(tmp_path):
    """prior_delta defaults to None -- with nothing to check .empty against, skip
    eligibility can never be evaluated, so the incremental branch always runs for
    real instead."""
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )
    assert report["mode"] == "incremental"


def test_analyze_incremental_degraded_scip_falls_back_to_full_path(tmp_path):
    """ScipRunError raised DURING the incremental branch's own scip attempt must
    abandon incremental entirely and re-run as an ordinary full analyze (which
    hits the SAME ScipRunError again and degrades exactly as it always has) --
    never a half-incremental "degraded" hybrid, since the heuristic fallback
    resolver gives no stable refs-diff to compute ref_dirty from."""
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_AlwaysFailRunner(),
        incremental=True, fingerprint_ok=False,
    )

    assert report["mode"] == "full"
    assert report["degraded"] is True
    assert report["reason"]
    assert "stale_files" not in report
    # the fallback is a REAL full re-analyze -- nodes/edges/calls still land.
    assert report["nodes"] > 0
    calls = [e for e in st.iter_edges() if e.type == "CALLS"]
    assert calls and all(e.resolution == "heuristic" and e.confidence == 0.6 for e in calls)


def test_analyze_incremental_stale_files_reflects_changed_file_on_disk(tmp_path):
    """Fake-runner-only proof that a content change drives stale_files through
    service_delta (needs no SCIP at all) -- and that a NON-stale file's staged node
    survives the incremental call completely untouched. The genuine cross-file
    ref_dirty mechanism needs real scip-python; proven by the scip-marked
    integration test instead."""
    svc_root = tmp_path / "svc"
    (svc_root / "app").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "main.py").write_text("def f():\n    pass\n")
    (svc_root / "app" / "other.py").write_text("def g():\n    pass\n")
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    other_before = next(
        n for n in st.iter_nodes() if n.relpath == "app/other.py" and n.kind == "Module"
    )
    main_before = next(
        n for n in st.iter_nodes() if n.relpath == "app/main.py" and n.kind == "Module"
    )

    (svc_root / "app" / "main.py").write_text("def f():\n    return 1\n")

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["stale_files"] == 1

    other_after = next(
        n for n in st.iter_nodes() if n.relpath == "app/other.py" and n.kind == "Module"
    )
    main_after = next(
        n for n in st.iter_nodes() if n.relpath == "app/main.py" and n.kind == "Module"
    )
    assert other_after == other_before  # untouched: not in the stale set
    assert main_after.content_hash != main_before.content_hash  # re-extracted


def test_analyze_incremental_report_exposes_stale_relpaths_for_cli_changed_files(tmp_path):
    """T7 (CLI --incremental) needs the ACTUAL stale relpath set, not just its count,
    to scope S8 chunk_embed's own changed_files parameter -- stale_relpaths is exactly
    the `stale` set computed internally (changed | added | ref_dirty, sorted), a
    sibling of the pre-existing stale_files COUNT, never present outside mode=
    "incremental" (skipped/full reports have no stale set to report at all)."""
    svc_root = tmp_path / "svc"
    (svc_root / "app").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "main.py").write_text("def f():\n    pass\n")
    (svc_root / "app" / "other.py").write_text("def g():\n    pass\n")
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    (svc_root / "app" / "main.py").write_text("def f():\n    return 1\n")

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )
    assert report["mode"] == "incremental"
    assert report["stale_relpaths"] == ("app/main.py",)
    assert len(report["stale_relpaths"]) == report["stale_files"]


def test_analyze_full_and_skipped_reports_have_no_stale_relpaths_key(tmp_path):
    svc = ServiceConfig(name="document-management", path=FIXTURE)
    st = Staging(tmp_path / "s.db")
    full_report = analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())
    assert "stale_relpaths" not in full_report
    assert "stale_escalation" not in full_report  # M7 T1: same incremental-only family

    empty_delta = ServiceDelta(added=(), changed=(), deleted=(), unchanged=("app/main.py",))
    skip_report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FailIfCalledRunner(),
        incremental=True, prior_delta=empty_delta, fingerprint_ok=True,
    )
    assert skip_report["mode"] == "skipped"
    assert "stale_relpaths" not in skip_report
    assert "stale_escalation" not in skip_report


# -- M7 T1: class_attrs harvesting -- claims written in S5 (kind="class_attrs"),
# service-wide ClassAttrIndex assembled from those claims BEFORE the per-file
# extractor loop and exposed via FileContext.class_attr_index. Unconditional (not
# gated behind active_idioms/idioms -- no fixture here activates fastapi/kafka/
# temporal/http_client at all), same "cheap, always-on structural harvest" status as
# python_core's own CONTAINS/IMPORTS edges. Degraded (_AlwaysFailRunner) path used
# throughout except where the scenario itself needs mode="incremental" (which,
# per this file's own established convention above, needs a runner that actually
# succeeds -- _FakeSuccessRunner).

_SETTINGS_SRC = (
    "from pydantic_settings import BaseSettings, SettingsConfigDict\n"
    "\n"
    "\n"
    "class ServiceSettings(BaseSettings):\n"
    '    model_config = SettingsConfigDict(env_prefix="service_")\n'
    "\n"
    '    verification_requests_url: str = "http://localhost:8000"\n'
)


def _class_attrs_service_tree(tmp_path):
    svc_root = tmp_path / "svc"
    (svc_root / "app").mkdir(parents=True)
    (svc_root / "app" / "__init__.py").write_text("")
    (svc_root / "app" / "config.py").write_text(_SETTINGS_SRC)
    (svc_root / "app" / "other.py").write_text("class Other:\n    def f(self):\n        pass\n")
    return svc_root


def test_analyze_full_writes_class_attrs_claims_unconditionally(tmp_path):
    """No idioms/active_idioms passed at all -- proves the harvest pass is NOT gated
    behind any domain-extractor activation flag."""
    svc = ServiceConfig(name="svc", path=_class_attrs_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    claims = st.claims_for("class_attrs", "svc")
    by_class = {c["class_fqn"]: c for c in claims}
    assert "app.config.ServiceSettings" in by_class
    assert by_class["app.config.ServiceSettings"]["fields"]["verification_requests_url"] == {
        "default": "http://localhost:8000",
        "env_name": "SERVICE_VERIFICATION_REQUESTS_URL",
    }
    # a class-body-literal-free file (app/other.py) contributes nothing -- no claim
    # emitted for it at all (harvest_class_attrs' own "nothing to index" skip).
    assert all(rp != "app/other.py" for rp in (c["_relpath"] for c in claims))


def test_analyze_wires_service_wide_class_attr_index_into_every_file_context(
    tmp_path, monkeypatch,
):
    """The index handed to EACH file's FileContext must already be the FULLY
    assembled, service-wide index -- not accumulated incrementally as the per-file
    loop progresses. Proven by checking it from app/__init__.py's own ctx too: that
    file sorts (and is therefore processed) BEFORE app/config.py alphabetically, so
    an incorrectly-incremental implementation would hand it an index that doesn't
    know about ServiceSettings yet."""
    from codegraph.extractors.python_core import extract as real_extract_python_core
    from codegraph.parsing.class_attrs import ClassAttrIndex

    captured: dict[str, ClassAttrIndex | None] = {}

    def spy(ctx):
        captured[ctx.relpath] = ctx.class_attr_index
        return real_extract_python_core(ctx)

    monkeypatch.setattr("codegraph.pipeline.analyze.extract_python_core", spy)

    svc = ServiceConfig(name="svc", path=_class_attrs_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_AlwaysFailRunner())

    assert set(captured) == {"app/__init__.py", "app/config.py", "app/other.py"}
    for relpath, idx in captured.items():
        assert isinstance(idx, ClassAttrIndex), relpath
        sf = idx.settings_field("ServiceSettings", "verification_requests_url")
        assert sf is not None, relpath
        assert sf.default == "http://localhost:8000"
        assert sf.env_name == "SERVICE_VERIFICATION_REQUESTS_URL"


def test_analyze_incremental_first_run_writes_class_attrs_claims_for_all_files(tmp_path):
    """Same 'first-run incremental, no prior state, every file added->stale' shape
    as this file's own pre-existing http/kafka counter tests above."""
    svc = ServiceConfig(name="svc", path=_class_attrs_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        incremental=True,
    )
    assert report["mode"] == "incremental"
    claims = st.claims_for("class_attrs", "svc")
    assert any(c["class_fqn"] == "app.config.ServiceSettings" for c in claims)


def test_analyze_incremental_class_attr_index_sees_unchanged_file_via_persisted_claims(
    tmp_path, monkeypatch,
):
    """Incremental coherence (task brief scenario): a FULL run stages both files'
    claims; a LATER incremental run that only re-harvests the STALE file (app/
    other.py -- app/config.py's own content never changes) must still see
    ServiceSettings in the assembled index, because claims_for reads the WHOLE
    service, not just what THIS call's harvest pass touched -- exactly mirroring
    delete_file_layer's own relpath-scoped claims deletion (staging.py) on the
    write side."""
    from codegraph.extractors.python_core import extract as real_extract_python_core

    svc_root = _class_attrs_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    (svc_root / "app" / "other.py").write_text(
        "class Other:\n    def f(self):\n        return 2\n",
    )

    captured = {}

    def spy(ctx):
        captured[ctx.relpath] = ctx.class_attr_index
        return real_extract_python_core(ctx)

    monkeypatch.setattr("codegraph.pipeline.analyze.extract_python_core", spy)

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["stale_relpaths"] == ("app/other.py",)  # only the changed file
    assert set(captured) == {"app/other.py"}

    idx = captured["app/other.py"]
    sf = idx.settings_field("ServiceSettings", "verification_requests_url")
    assert sf is not None
    assert sf.default == "http://localhost:8000"
    assert sf.env_name == "SERVICE_VERIFICATION_REQUESTS_URL"


# -- M7 T1 review Important-1 (settings-staleness hole, empirically proven by the
# reviewer on real scip): a Settings-default edit under --incremental made only the
# Settings FILE itself stale -- refs_hash is blind to attribute VALUES, so consumer
# files (whose T2/T3-derived edges will bake in the OLD default/env) were never
# re-extracted, and their derived edges would have stayed stale forever. Fix: the
# incremental path snapshots the service's class_attrs claims digest BEFORE any wipe,
# re-harvests the stale files' claims, and -- if the service-wide digest changed --
# escalates THIS run's stale set to ALL scanned files (reported as
# stale_escalation="class_attrs_changed"). Self-contained in analyze.py; no
# config_fingerprint plumbing.


def test_analyze_incremental_settings_change_escalates_stale_to_all_files(tmp_path):
    svc_root = _class_attrs_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    # ONLY the Settings file changes -- and only a class-attr VALUE at that.
    (svc_root / "app" / "config.py").write_text(
        _SETTINGS_SRC.replace("http://localhost:8000", "http://localhost:9000"),
    )

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["stale_escalation"] == "class_attrs_changed"
    # ALL scanned files re-extracted, not just the settings file itself.
    assert report["stale_files"] == report["files"] == 3
    assert report["stale_relpaths"] == (
        "app/__init__.py", "app/config.py", "app/other.py",
    )
    # and the escalated run's claims carry the NEW default.
    by_class = {c["class_fqn"]: c for c in st.claims_for("class_attrs", "svc")}
    assert by_class["app.config.ServiceSettings"]["fields"][
        "verification_requests_url"
    ]["default"] == "http://localhost:9000"


def test_analyze_incremental_non_class_attr_change_does_not_escalate(tmp_path):
    """The negative: an edit that does NOT move any class-body literal (app/other.py
    has none at all) keeps the narrow stale set and reports stale_escalation=None --
    escalation keys off the class_attrs claims DIGEST, not off 'some file changed'."""
    svc_root = _class_attrs_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    (svc_root / "app" / "other.py").write_text(
        "class Other:\n    def f(self):\n        return 3\n",
    )

    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )

    assert report["mode"] == "incremental"
    assert report["stale_escalation"] is None
    assert report["stale_relpaths"] == ("app/other.py",)


def test_analyze_incremental_first_run_all_stale_reports_no_escalation(tmp_path):
    """First-run incremental (no prior state): every file is added->stale already, so
    even though the class_attrs digest necessarily changes (empty -> populated),
    there is nothing to WIDEN -- stale_escalation stays None (the marker means 'this
    run re-extracted more than the file delta demanded', not 'the digest moved')."""
    svc = ServiceConfig(name="svc", path=_class_attrs_service_tree(tmp_path))
    st = Staging(tmp_path / "s.db")
    report = analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=False),
        incremental=True,
    )
    assert report["mode"] == "incremental"
    assert report["stale_files"] == report["files"]
    assert report["stale_escalation"] is None


# -- M7 T1 review Important-2 (delete-before-harvest ordering pin, through the REAL
# analyze_service path): the claims PK is (service, relpath, kind, payload_json) --
# a v1->v2 Settings edit produces a DIFFERENT payload_json, so if the incremental
# path ever harvested BEFORE delete_file_layer wiped the stale file's old rows,
# BOTH versions' rows would coexist and the assembled index would see phantom v1
# data. Reviewer's probe scenario, committed as a regression test.


def test_analyze_incremental_stale_settings_file_claims_replaced_not_duplicated(tmp_path):
    svc_root = _class_attrs_service_tree(tmp_path)
    svc = ServiceConfig(name="svc", path=svc_root)
    st = Staging(tmp_path / "s.db")
    analyze_service(svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True))

    (svc_root / "app" / "config.py").write_text(
        _SETTINGS_SRC.replace("http://localhost:8000", "http://localhost:9000"),
    )

    analyze_service(
        svc, st, tmp_path / "cache", runner=_FakeSuccessRunner(from_cache=True),
        incremental=True,
    )

    config_claims = [
        c for c in st.claims_for("class_attrs", "svc")
        if c["_relpath"] == "app/config.py"
    ]
    assert len(config_claims) == 1  # exactly ONE row -- v1's row is GONE, not beside v2
    assert config_claims[0]["fields"]["verification_requests_url"]["default"] == (
        "http://localhost:9000"
    )
