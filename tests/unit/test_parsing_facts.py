from codegraph.parsing.facts import ArgFact, AssignFact, ParamFact, build_file_facts

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


def test_smoke_all_fixture_files_parse():
    from pathlib import Path

    fixtures = Path(__file__).parents[2] / "fixtures" / "services"
    for f in fixtures.rglob("*.py"):
        facts = build_file_facts(str(f), f.read_bytes())
        assert facts is not None


# --- M2 Task 2: ArgFact / CallFact.args+receiver_text / AssignFact / ParamFact ------------


def _fixture_facts(relpath: str):
    from pathlib import Path

    path = Path(__file__).parents[2] / "fixtures" / "services" / relpath
    source = path.read_bytes()
    return build_file_facts(str(path), source), source


# -- contract shape (verbatim field order per brief) --


def test_arg_fact_field_order_matches_contract():
    arg = ArgFact(0, None, "string", '"hi"', "hi", None, None, None)
    assert arg.index == 0
    assert arg.keyword is None
    assert arg.value_kind == "string"
    assert arg.text == '"hi"'
    assert arg.string_value == "hi"
    assert arg.name_start_byte is None
    assert arg.name_end_byte is None
    assert arg.dict_items is None


def test_assign_fact_field_order_matches_contract():
    a = AssignFact("x", "Callee", [], 3)
    assert a.target == "x"
    assert a.callee_name == "Callee"
    assert a.call_args == []
    assert a.start_line == 3


def test_param_fact_field_order_matches_contract():
    p = ParamFact("db", "Session", "Depends(get_db)", 10, 26)
    assert p.name == "db"
    assert p.annotation_text == "Session"
    assert p.default_text == "Depends(get_db)"
    assert p.default_start_byte == 10
    assert p.default_end_byte == 26


def test_param_fact_annotation_start_byte_appended_field_defaults_to_none():
    """M2 T4 sanctioned extension (brief-authorized): annotation_start_byte is a new
    LAST field with a default -- old 5-positional-arg construction sites (incl. the
    contract test above) keep working unchanged."""
    p = ParamFact("db", "Session", "Depends(get_db)", 10, 26)
    assert p.annotation_start_byte is None
    p2 = ParamFact("db", "Session", "Depends(get_db)", 10, 26, 3)
    assert p2.annotation_start_byte == 3


# -- CallFact.args: positional + keyword, string_value --


def test_call_args_positional_and_keyword_with_string_value():
    facts, _ = _fixture_facts("kyc_worker/app/consumer_main.py")
    call = next(c for c in facts.calls if c.callee_name == "AIOKafkaConsumer")
    assert len(call.args) == 2
    topic_arg, bootstrap_arg = call.args
    assert topic_arg.index == 0 and topic_arg.keyword is None
    assert topic_arg.value_kind == "string"
    assert topic_arg.string_value == "orders.events"
    assert bootstrap_arg.index is None and bootstrap_arg.keyword == "bootstrap_servers"
    assert bootstrap_arg.value_kind == "string"
    assert bootstrap_arg.string_value == "kafka:9092"
    assert bootstrap_arg.text == '"kafka:9092"'


def test_call_with_no_args_has_empty_args_list():
    facts, _ = _fixture_facts("document_management/app/events/producer.py")
    call = next(c for c in facts.calls if c.callee_name == "start")
    assert call.args == []


def test_call_arg_name_span_and_other_kind_from_workflow_fixture():
    facts, source = _fixture_facts("kyc_worker/app/workflows/kyc.py")
    call = next(c for c in facts.calls if c.callee_name == "execute_activity")
    assert len(call.args) == 3
    name_arg, subscript_arg, kw_arg = call.args
    assert name_arg.index == 0 and name_arg.value_kind == "name"
    assert source[name_arg.name_start_byte:name_arg.name_end_byte] == b"verify_documents"
    assert subscript_arg.index == 1 and subscript_arg.value_kind == "other"
    assert kw_arg.keyword == "start_to_close_timeout" and kw_arg.value_kind == "other"


def test_call_args_skips_splat_arguments_entirely():
    """M6 T4 (GAPS §6, elaborated scenario list): `*args`/`**kwargs` splats AT A
    CALL SITE (contrast test_param_fact_star_args_and_kwargs_and_bare_separators
    below, which is about a function's own PARAMETER list -- a different grammar
    production entirely) carry no field name at all in the tree-sitter grammar --
    not addressable by index OR keyword -- so _build_call_args skips them outright
    (module docstring's own documented convention, `_SPLAT_ARG_TYPES`) rather than
    inventing a placeholder ArgFact neither an `arg` nor a `kwarg` ValueSpec source
    could ever correctly address. Load-bearing honesty for kafka_ext's kwarg
    producer-topic source: a call that forwards its topic through a splat (`producer
    .send_and_wait(**opts)`) must not be silently misattributed to some OTHER
    positional/keyword slot -- pinned here as the same honest "no matching ArgFact"
    shape as a keyword that's simply absent (see test_kafka_extractor.py's
    test_producer_kwarg_missing_from_call_is_unresolved_with_counter_not_crash)."""
    src = b'''def use(a, b, **extra):
    pass


x = [1, 2]
y = {"c": 3}
use(*x, **y, d=4)
'''
    facts = build_file_facts("m.py", src)
    call = next(c for c in facts.calls if c.callee_name == "use")
    assert len(call.args) == 1
    assert call.args[0].keyword == "d" and call.args[0].index is None
    assert call.args[0].value_kind == "other" and call.args[0].text == "4"


# -- CallFact.receiver_text --


def test_receiver_text_multi_segment_attribute():
    facts, _ = _fixture_facts("orders_api/app/db/outbox.py")
    call = next(c for c in facts.calls if c.callee_name == "execute")
    assert call.receiver_text == "self._db"


def test_receiver_text_none_for_plain_identifier_call():
    facts, _ = _fixture_facts("document_management/app/events/producer.py")
    call = next(c for c in facts.calls if c.callee_name == "AIOKafkaProducer")
    assert call.receiver_text is None


def test_receiver_text_single_identifier():
    facts, _ = _fixture_facts("document_management/app/events/producer.py")
    call = next(c for c in facts.calls if c.callee_name == "send")
    assert call.receiver_text == "producer"


# -- dict-literal ArgFact (key/value spans) --


def test_dict_arg_single_pair_string_key_name_value_register_handlers():
    facts, source = _fixture_facts("kyc_worker/app/consumers/orders.py")
    call = next(c for c in facts.calls if c.callee_name == "register_handlers")
    assert len(call.args) == 1
    dict_arg = call.args[0]
    assert dict_arg.value_kind == "dict"
    assert dict_arg.index == 0
    assert dict_arg.dict_items is not None and len(dict_arg.dict_items) == 1
    key, value = dict_arg.dict_items[0]
    assert key.value_kind == "string" and key.string_value == "OrderCreated"
    assert key.index is None and key.keyword is None
    assert value.value_kind == "name"
    assert source[value.name_start_byte:value.name_end_byte] == b"handle_order_created"


def test_dict_arg_two_pairs_attribute_values_add_event():
    facts, source = _fixture_facts("orders_api/app/services/order.py")
    place = next(d for d in facts.defs if d.name == "place")
    call = next(
        c for c in facts.calls if c.callee_name == "add_event" and c.enclosing_def == place.index
    )
    assert len(call.args) == 2
    assert call.args[0].value_kind == "string" and call.args[0].string_value == "OrderCreated"
    dict_arg = call.args[1]
    assert dict_arg.value_kind == "dict"
    assert dict_arg.dict_items is not None and len(dict_arg.dict_items) == 2
    keys = [k.string_value for k, _ in dict_arg.dict_items]
    assert keys == ["order_id", "customer_id"]
    values = [v for _, v in dict_arg.dict_items]
    assert values[0].value_kind == "attr"
    assert source[values[0].name_start_byte:values[0].name_end_byte] == b"id"
    assert values[1].value_kind == "attr"
    assert source[values[1].name_start_byte:values[1].name_end_byte] == b"customer_id"


# -- fstring ArgFact classification (template resolution itself lives in consts.py) --


def test_fstring_arg_value_kind_and_text():
    facts, _ = _fixture_facts("kyc_worker/app/clients/document_management_client.py")
    call = next(c for c in facts.calls if c.callee_name == "get")
    assert len(call.args) == 1
    arg = call.args[0]
    assert arg.value_kind == "fstring"
    assert arg.text == 'f"{self._base_url}/documents/{doc_id}"'
    assert arg.string_value is None


# -- AssignFact: simple name = Callee(...) / name = await Callee(...) --


def test_assign_fact_identifier_and_await_identifier_from_producer_fixture():
    facts, _ = _fixture_facts("document_management/app/events/producer.py")
    by_target = {a.target: a for a in facts.assigns}
    producer_ctor = by_target["_producer"]
    assert producer_ctor.callee_name == "AIOKafkaProducer"
    assert len(producer_ctor.call_args) == 1
    assert producer_ctor.call_args[0].keyword == "bootstrap_servers"
    assert producer_ctor.call_args[0].string_value == "kafka:9092"

    awaited = by_target["producer"]
    assert awaited.callee_name == "get_producer"
    assert awaited.call_args == []
    assert awaited.start_line > 0


def test_assign_fact_await_attribute_call_from_workflow_fixture():
    facts, _ = _fixture_facts("kyc_worker/app/workflows/kyc.py")
    status_assign = next(a for a in facts.assigns if a.target == "status")
    assert status_assign.callee_name == "execute_activity"
    assert len(status_assign.call_args) == 3


def test_assign_fact_all_kwargs_and_positional_attr_from_order_service_fixture():
    facts, _ = _fixture_facts("orders_api/app/services/order.py")
    by_target = {a.target: a for a in facts.assigns}

    order_assign = by_target["order"]
    assert order_assign.callee_name == "Order"
    kw = {a.keyword: a for a in order_assign.call_args}
    assert kw["id"].value_kind == "other"
    assert kw["customer_id"].value_kind == "attr"
    assert kw["amount"].value_kind == "attr"
    assert kw["status"].value_kind == "string" and kw["status"].string_value == "pending_kyc"

    outbox_assign = by_target["outbox"]
    assert outbox_assign.callee_name == "OutboxRepository"
    assert len(outbox_assign.call_args) == 1
    assert outbox_assign.call_args[0].value_kind == "attr"
    assert outbox_assign.call_args[0].text == "self._db"


def test_assign_fact_not_created_for_attribute_target():
    facts, _ = _fixture_facts("orders_api/app/services/order.py")
    assert all(a.target != "self._db" for a in facts.assigns)
    assert "_db" not in {a.target for a in facts.assigns}


def test_assigns_collected_at_module_and_function_level():
    src = b'''client = make_client()


def handler():
    result = process(client)
    return result
'''
    facts = build_file_facts("x.py", src)
    targets = {a.target: a.callee_name for a in facts.assigns}
    assert targets == {"client": "make_client", "result": "process"}


# -- ParamFact: typed_parameter / typed_default_parameter (Depends) with spans --


def test_param_fact_depends_from_orders_route_fixture():
    facts, source = _fixture_facts("orders_api/app/routes/orders.py")
    create_order = next(d for d in facts.defs if d.name == "create_order")
    by_name = {p.name: p for p in create_order.params}

    req_param = by_name["req"]
    assert req_param.annotation_text == "OrderCreate"
    assert req_param.default_text is None
    assert req_param.default_start_byte is None
    assert req_param.default_end_byte is None

    db_param = by_name["db"]
    assert db_param.annotation_text == "Session"
    assert db_param.default_text == "Depends(get_db)"
    assert source[db_param.default_start_byte:db_param.default_end_byte] == b"Depends(get_db)"


def test_param_fact_annotation_start_byte_from_orders_route_fixture():
    """M2 T4 sanctioned extension: annotation_start_byte -- byte-span anchor the
    fastapi extractor needs for the `Annotated[X, Depends(y)]` form (default_start_byte
    alone only covers the `= Depends(y)` default-value form)."""
    facts, source = _fixture_facts("orders_api/app/routes/orders.py")
    create_order = next(d for d in facts.defs if d.name == "create_order")
    by_name = {p.name: p for p in create_order.params}

    db_param = by_name["db"]
    assert db_param.annotation_start_byte is not None
    end = db_param.annotation_start_byte + len("Session")
    assert source[db_param.annotation_start_byte:end] == b"Session"

    req_param = by_name["req"]
    assert req_param.annotation_start_byte is not None
    end = req_param.annotation_start_byte + len("OrderCreate")
    assert source[req_param.annotation_start_byte:end] == b"OrderCreate"


def test_param_fact_annotation_start_byte_none_when_no_annotation():
    src = b'''def f(x=5):
    pass
'''
    facts = build_file_facts("x.py", src)
    f = next(d for d in facts.defs if d.name == "f")
    assert f.params[0].annotation_start_byte is None


def test_param_fact_annotation_start_byte_typed_parameter_annotated_depends():
    """typed_parameter (annotation only, no default) -- the realistic modern-FastAPI
    `Annotated[Session, Depends(get_db)]` shape; no M2 fixture uses Annotated (grep-
    confirmed), so synthetic, matching the M2 T2 precedent for uncovered grammar shapes."""
    src = b'''def f(db: Annotated[Session, Depends(get_db)]):
    pass
'''
    facts = build_file_facts("x.py", src)
    f = next(d for d in facts.defs if d.name == "f")
    db_param = f.params[0]
    assert db_param.default_text is None
    assert db_param.annotation_text == "Annotated[Session, Depends(get_db)]"
    assert db_param.annotation_start_byte is not None
    end = db_param.annotation_start_byte + len(db_param.annotation_text)
    assert src[db_param.annotation_start_byte:end] == b"Annotated[Session, Depends(get_db)]"


def test_param_fact_bare_identifier_no_annotation_no_default():
    f = _facts()
    place = next(d for d in f.defs if d.name == "place")
    assert [p.name for p in place.params] == ["self", "req"]
    self_param, req_param = place.params
    assert self_param.annotation_text is None and self_param.default_text is None
    assert req_param.annotation_text is None and req_param.default_text is None


def test_class_def_has_empty_params():
    f = _facts()
    cls = next(d for d in f.defs if d.kind == "class")
    assert cls.params == []


def test_param_fact_bare_default_without_annotation():
    src = b'''def f(x=5):
    pass
'''
    facts = build_file_facts("x.py", src)
    f = next(d for d in facts.defs if d.name == "f")
    x_param = f.params[0]
    assert x_param.name == "x"
    assert x_param.annotation_text is None
    assert x_param.default_text == "5"


def test_param_fact_star_args_and_kwargs_and_bare_separators():
    src = b'''def f(a, /, b, *args, c, **kwargs):
    pass
'''
    facts = build_file_facts("x.py", src)
    f = next(d for d in facts.defs if d.name == "f")
    names = [p.name for p in f.params]
    assert names == ["a", "b", "args", "c", "kwargs"]


# -- smoke: all 29 fixture files, args/params/assigns actually populated (not just present) --


def test_smoke_all_fixture_files_have_populated_args_params_assigns():
    from pathlib import Path

    fixtures = Path(__file__).parents[2] / "fixtures" / "services"
    calls_with_args = 0
    params_with_annotation = 0
    assigns_total = 0
    dict_args_seen = 0
    fstring_args_seen = 0
    for f in fixtures.rglob("*.py"):
        facts = build_file_facts(str(f), f.read_bytes())
        for c in facts.calls:
            assert isinstance(c.args, list)
            if c.args:
                calls_with_args += 1
            for a in c.args:
                if a.value_kind == "dict":
                    dict_args_seen += 1
                if a.value_kind == "fstring":
                    fstring_args_seen += 1
        for d in facts.defs:
            assert isinstance(d.params, list)
            for p in d.params:
                if p.annotation_text is not None:
                    params_with_annotation += 1
        for a in facts.assigns:
            assert isinstance(a.call_args, list)
        assigns_total += len(facts.assigns)

    # known, real call-sites across the fixture set (see the dedicated per-fixture
    # tests above for exact shapes) — a nonzero floor here would catch a regression
    # where args/params/assigns silently stayed empty across the whole walker.
    assert calls_with_args >= 10
    assert params_with_annotation >= 10
    assert assigns_total >= 5
    assert dict_args_seen >= 2  # register_handlers (1 pair) + add_event (2 pairs)
    assert fstring_args_seen >= 2  # get_document + create_document base_url interpolation


# -- M6 T3: DefFact.base_exprs (class bases, pilot gap 4 pre-step) --
#
# GAPS §4 pre-step: build_file_facts did not carry class bases at all before this --
# kafka_ext's new base_class consumer idiom needs to know (a) whether a class has any
# bases at all worth checking and (b) each base's raw text (a subscript base like
# "BaseConsumer[OCRDataEvent]" carries the generic-arg text a base_class idiom resolves
# event_type from). base_exprs is deliberately TEXT ONLY -- no byte spans -- see
# extractors/kafka_ext.py's own `_scan_class_bases` for why the scip-lookup byte
# position is recovered via a separate, narrowly-scoped walk instead.


def test_base_exprs_empty_for_class_with_no_bases():
    facts = build_file_facts("x.py", b"class Plain:\n    pass\n")
    cls = next(d for d in facts.defs if d.name == "Plain")
    assert cls.base_exprs == ()


def test_base_exprs_empty_for_class_with_empty_parens():
    facts = build_file_facts("x.py", b"class Plain():\n    pass\n")
    cls = next(d for d in facts.defs if d.name == "Plain")
    assert cls.base_exprs == ()


def test_base_exprs_single_generic_subscript_base():
    src = b"class OCRDataConsumer(BaseConsumer[OCRDataEvent]):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "OCRDataConsumer")
    assert cls.base_exprs == ("BaseConsumer[OCRDataEvent]",)


def test_base_exprs_bare_non_generic_base():
    src = b"class OCRDataConsumer(BaseConsumer):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "OCRDataConsumer")
    assert cls.base_exprs == ("BaseConsumer",)


def test_base_exprs_unrelated_base_untouched():
    src = b"class Other(SomeOtherBase):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "Other")
    assert cls.base_exprs == ("SomeOtherBase",)


def test_base_exprs_multi_inherit_mixin_plus_generic_base():
    src = b"class C(Mixin, BaseConsumer[FooEvent]):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "C")
    assert cls.base_exprs == ("Mixin", "BaseConsumer[FooEvent]")


def test_base_exprs_attribute_chain_base_and_generic_arg():
    src = b"class AttrBase(pkgmod.BaseConsumer[evtmod.OCRDataEvent]):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "AttrBase")
    assert cls.base_exprs == ("pkgmod.BaseConsumer[evtmod.OCRDataEvent]",)


def test_base_exprs_multi_generic_args():
    src = b"class MultiGeneric(Base[A, B]):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "MultiGeneric")
    assert cls.base_exprs == ("Base[A, B]",)


def test_base_exprs_keyword_argument_metaclass_excluded():
    src = b"class WithMeta(Base, metaclass=ABCMeta):\n    pass\n"
    facts = build_file_facts("x.py", src)
    cls = next(d for d in facts.defs if d.name == "WithMeta")
    assert cls.base_exprs == ("Base",)


def test_base_exprs_empty_for_function():
    facts = build_file_facts("x.py", b"def f():\n    pass\n")
    fn = next(d for d in facts.defs if d.name == "f")
    assert fn.base_exprs == ()


def test_def_fact_base_exprs_appended_field_defaults_to_empty_tuple():
    """Sanctioned additive extension (M6 T3, same precedent as
    ParamFact.annotation_start_byte, M2 T4): base_exprs is a new LAST field with a
    default -- construction sites that don't pass it keep working unchanged."""
    from codegraph.parsing.facts import DefFact

    d = DefFact(
        index=0, kind="class", name="C", name_start_byte=0, name_end_byte=1,
        start_byte=0, end_byte=10, start_line=1, end_line=2, parent=None,
        is_async=False, signature="class C", docstring=None,
    )
    assert d.base_exprs == ()
    d2 = DefFact(
        index=0, kind="class", name="C", name_start_byte=0, name_end_byte=1,
        start_byte=0, end_byte=10, start_line=1, end_line=2, parent=None,
        is_async=False, signature="class C", docstring=None,
        base_exprs=("Base[X]",),
    )
    assert d2.base_exprs == ("Base[X]",)


# -- M7 T1: ClassAttrFact (class-body literal assignments, class_attrs harvesting
# pre-step) -----------------------------------------------------------------------
#
# Pre-step finding (brief Step 1): AssignFact carries NEITHER any scope/enclosing-def
# field AT ALL, NOR non-call right-hand sides (a plain string default like
# `field: str = "x"`, or a bare `field: str` annotation with no value, produces no
# AssignFact today -- only `name = Callee(...)`/`name = await Callee(...)` does).
# Settings-field harvesting needs both: which CLASS a class-body attribute belongs to,
# and its literal (non-call) value. ClassAttrFact is a brand-new fact type (rather than
# broadening AssignFact's own call-only contract) -- AssignFact's existing consumers
# (idiom_match.py, fastapi_ext.py) and its own docstring stay completely untouched.


def test_class_attr_fact_annotated_string_default():
    facts = build_file_facts(
        "x.py", b'class C:\n    x: str = "http://localhost:8000"\n',
    )
    cls = next(d for d in facts.defs if d.name == "C")
    attrs = [a for a in facts.class_attrs if a.enclosing_def == cls.index]
    assert len(attrs) == 1
    a = attrs[0]
    assert a.name == "x"
    assert a.annotation_text == "str"
    assert a.has_value is True
    assert a.value_text == '"http://localhost:8000"'
    assert a.string_value == "http://localhost:8000"
    assert a.call_callee is None
    assert a.call_args is None
    assert a.start_line == 2


def test_class_attr_fact_bare_annotation_no_value():
    facts = build_file_facts("x.py", b"class C:\n    x: str\n")
    cls = next(d for d in facts.defs if d.name == "C")
    a = next(a for a in facts.class_attrs if a.enclosing_def == cls.index)
    assert a.name == "x"
    assert a.annotation_text == "str"
    assert a.has_value is False
    assert a.value_text is None
    assert a.string_value is None


def test_class_attr_fact_plain_assignment_no_annotation():
    facts = build_file_facts("x.py", b'class C:\n    x = "v"\n')
    cls = next(d for d in facts.defs if d.name == "C")
    a = next(a for a in facts.class_attrs if a.enclosing_def == cls.index)
    assert a.annotation_text is None
    assert a.has_value is True
    assert a.string_value == "v"


def test_class_attr_fact_call_rhs_keeps_callee_and_kwargs():
    facts = build_file_facts(
        "x.py",
        b'class C:\n    model_config = SettingsConfigDict(env_prefix="service_")\n',
    )
    cls = next(d for d in facts.defs if d.name == "C")
    a = next(a for a in facts.class_attrs if a.enclosing_def == cls.index)
    assert a.name == "model_config"
    assert a.has_value is True
    assert a.string_value is None
    assert a.call_callee == "SettingsConfigDict"
    assert a.call_args is not None and len(a.call_args) == 1
    assert a.call_args[0].keyword == "env_prefix"
    assert a.call_args[0].value_kind == "string"
    assert a.call_args[0].string_value == "service_"


def test_class_attr_fact_fstring_rhs_not_treated_as_string():
    facts = build_file_facts("x.py", b'class C:\n    x: str = f"{a}"\n')
    cls = next(d for d in facts.defs if d.name == "C")
    a = next(a for a in facts.class_attrs if a.enclosing_def == cls.index)
    assert a.has_value is True
    assert a.string_value is None
    assert a.call_callee is None


def test_class_attr_fact_non_string_literal_rhs():
    facts = build_file_facts("x.py", b"class C:\n    x: int = 5\n")
    cls = next(d for d in facts.defs if d.name == "C")
    a = next(a for a in facts.class_attrs if a.enclosing_def == cls.index)
    assert a.has_value is True
    assert a.value_text == "5"
    assert a.string_value is None
    assert a.call_callee is None


def test_class_attr_fact_enclosing_def_none_at_module_level():
    facts = build_file_facts("x.py", b'x = "v"\n')
    a = next(a for a in facts.class_attrs if a.name == "x")
    assert a.enclosing_def is None


def test_class_attr_fact_enclosing_def_points_to_function_not_class():
    """A local variable assignment inside a METHOD body is still captured (AssignFact-
    style scope-blindness, unchanged) -- class_attrs.py's own harvester is what filters
    by `defs[enclosing_def].kind == "class"`, not this fact-collection layer."""
    facts = build_file_facts(
        "x.py", b'class C:\n    def f(self):\n        y = "local"\n',
    )
    fn = next(d for d in facts.defs if d.name == "f")
    a = next(a for a in facts.class_attrs if a.name == "y")
    assert a.enclosing_def == fn.index
    assert facts.defs[a.enclosing_def].kind == "function"


def test_class_attr_fact_multiple_members_preserve_declaration_order():
    facts = build_file_facts(
        "x.py",
        b'class C:\n    A = "a"\n    B = "b"\n    C_ = "c"\n',
    )
    cls = next(d for d in facts.defs if d.name == "C")
    names = [a.name for a in facts.class_attrs if a.enclosing_def == cls.index]
    assert names == ["A", "B", "C_"]


def test_file_facts_class_attrs_defaults_to_empty_list_when_omitted():
    """Same additive-field precedent as `assigns`/`base_exprs`: a construction site
    that doesn't pass `class_attrs` keeps working, defaulting to an empty list."""
    from codegraph.parsing.facts import FileFacts

    f = FileFacts(relpath="x.py", module_docstring=None, defs=[], calls=[], imports=[])
    assert f.class_attrs == []


def test_smoke_all_fixture_files_have_class_attrs_list_populated_where_expected():
    from pathlib import Path

    fixtures = Path(__file__).parents[2] / "fixtures" / "services"
    for f in fixtures.rglob("*.py"):
        facts = build_file_facts(str(f), f.read_bytes())
        assert isinstance(facts.class_attrs, list)
        for a in facts.class_attrs:
            assert isinstance(a.name, str)
            assert a.call_args is None or isinstance(a.call_args, list)
