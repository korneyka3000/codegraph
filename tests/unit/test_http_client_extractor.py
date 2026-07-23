"""M2 T6: extract_http_client (http_call claims over HttpClientIdiom).

Real-fixture tests exercise kyc_worker/app/clients/document_management_client.py -- the
ONLY *_client.py fixture in the repo, matched by BOTH the workspace.yaml custom
"default-sdk" idiom (file_glob "**/clients/*_client.py", base_url_env=
DOCUMENT_MANAGEMENT_URL) and the builtin "aiohttp-client-convention" idiom (all
defaults, no base_url) -- each exercised SEPARATELY below (its own single-idiom
ServiceIdioms), matching the task's own "два claim'а ... base_url_env=...; и
builtin-вариант без env" as two independent scenarios. The combined case is pinned
twice: synthetic dedup tests in both explicit list orders, plus a regression test on
the REAL `effective_idioms(cfg, kyc-worker)` merge, whose order is load-bearing (T6
review fix: service idioms come FIRST and shadow builtins, so the custom idiom's
base_url_env survives first-idiom-wins dedup).

No ref_symbol_lookup stubbing anywhere in this file (unlike every other T4-T6 extractor
test module) -- extract_http_client never calls it at all, see
extractors/http_client_ext.py's own module docstring: everything resolves from
structural facts (DefFact parent-chain, CallFact args) + consts.resolve_arg's partial
constant evaluation, with zero SCIP dependency. `consts` is built by the `_load` helper
exactly as analyze.py's S5 wiring does (once per file, shared) and passed as the 4th
argument (T6 review fix -- was built internally by the extractor before).
"""

from __future__ import annotations

from pathlib import Path

from codegraph.config.loader import effective_idioms, load_workspace
from codegraph.config.models import (
    BaseUrlSpec,
    HttpClientIdiom,
    HttpRouteFromSpec,
    HttpVerbFromSpec,
    ServiceIdioms,
)
from codegraph.extractors.base import FileContext
from codegraph.extractors.http_client_ext import HttpClientResult, extract_http_client
from codegraph.extractors.python_core import extract as extract_python_core
from codegraph.parsing.consts import ConstTable
from codegraph.parsing.facts import build_file_facts

FIXTURES = Path(__file__).parents[2] / "fixtures" / "services"
WORKSPACE_YAML = Path(__file__).parents[2] / "fixtures" / "workspace.yaml"


def _fixture_bytes(relpath: str) -> bytes:
    return (FIXTURES / relpath).read_bytes()


def _load(relpath: str, service: str, source: bytes):
    """Builds (ctx, node_ids, consts) exactly as analyze.py's S5 wiring will: node_ids
    is def-index -> resolved node id, derived from python_core's OWN per-file output
    (Module node first, then exactly one node per facts.defs entry, same order), plus
    the None -> Module-node-id fallback entry T5 introduced (unused by this extractor's
    real fixture/tests -- every call here sits inside a method -- but kept for parity
    with node_ids' documented full shape); consts is the same once-per-file ConstTable
    analyze.py now builds and shares between kafka_ext and this extractor."""
    facts = build_file_facts(relpath, source)
    core_ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    core_res = extract_python_core(core_ctx)
    node_ids = {
        d.index: n.id
        for d, n in zip(facts.defs, core_res.nodes[1:], strict=True)
    }
    node_ids[None] = core_res.nodes[0].id
    ctx = FileContext(
        service=service, relpath=relpath, source=source, facts=facts,
        def_symbol_lookup=lambda rp, sb: None, module_exists=lambda d: False,
    )
    consts = ConstTable.build(facts, source)
    return ctx, node_ids, consts


def _client_ctx():
    relpath = "app/clients/document_management_client.py"
    return _load(relpath, "kyc-worker", _fixture_bytes(f"kyc_worker/{relpath}"))


def _def(ctx: FileContext, name: str):
    return next(d for d in ctx.facts.defs if d.name == name)


def _call(ctx: FileContext, callee_name: str):
    return next(c for c in ctx.facts.calls if c.callee_name == callee_name)


DEFAULT_SDK_IDIOM = HttpClientIdiom(
    name="default-sdk",
    file_glob="**/clients/*_client.py",
    class_glob="*Client",
    base_url=BaseUrlSpec(attr="self._base_url", env="DOCUMENT_MANAGEMENT_URL"),
)

AIOHTTP_CLIENT_BUILTIN_IDIOM = HttpClientIdiom(name="aiohttp-client-convention")

ANY_CLIENT_IDIOM = HttpClientIdiom(name="any", file_glob="**/*_client.py", class_glob="*Client")


# -- HttpClientResult: contract shape --


def test_http_client_result_field_shape():
    r = HttpClientResult(claims=[], stats={})
    assert r.claims == []
    assert r.stats == {}


def test_no_http_clients_idiom_is_a_noop():
    ctx, node_ids, consts = _client_ctx()
    result = extract_http_client(ctx, node_ids, ServiceIdioms(), consts)
    assert result == HttpClientResult(claims=[], stats=result.stats)
    assert result.stats["http_calls_resolved"] == 0


# -- real fixture: two claims, each idiom variant exercised separately --


def test_custom_default_sdk_idiom_two_claims_with_base_url_env():
    ctx, node_ids, consts = _client_ctx()
    get_call, post_call = _call(ctx, "get"), _call(ctx, "post")
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DEFAULT_SDK_IDIOM]), consts,
    )

    get_id = node_ids[_def(ctx, "get_document").index]
    create_id = node_ids[_def(ctx, "create_document").index]

    assert len(result.claims) == 2
    by_src = {c["src_id"]: c for c in result.claims}
    assert by_src[get_id] == {
        "src_id": get_id, "verb": "GET", "path_template": "/documents/{doc_id}",
        "base_url_env": "DOCUMENT_MANAGEMENT_URL", "resolution_hint": "static",
        "evidence_line": get_call.start_line,
    }
    assert by_src[create_id] == {
        "src_id": create_id, "verb": "POST", "path_template": "/documents",
        "base_url_env": "DOCUMENT_MANAGEMENT_URL", "resolution_hint": "static",
        "evidence_line": post_call.start_line,
    }
    assert result.stats["http_calls_resolved"] == 2
    assert result.stats["http_url_unresolved"] == 0


def test_builtin_aiohttp_client_idiom_two_claims_no_base_url_env():
    """Same fixture, same 2 call-sites -- just the builtin idiom (default globs, no
    base_url) applied ALONE, proving the file_glob/class_glob defaults on
    HttpClientIdiom itself are enough to match this fixture without a custom idiom at
    all, and that base_url_env correctly comes out None when idiom.base_url is None."""
    ctx, node_ids, consts = _client_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[AIOHTTP_CLIENT_BUILTIN_IDIOM]), consts,
    )
    assert len(result.claims) == 2
    assert all(c["base_url_env"] is None for c in result.claims)
    assert all(c["resolution_hint"] == "static" for c in result.claims)
    assert {c["verb"] for c in result.claims} == {"GET", "POST"}
    assert {c["path_template"] for c in result.claims} == {"/documents/{doc_id}", "/documents"}


# -- negatives: scope gates --


def test_file_outside_file_glob_is_empty():
    ctx, node_ids, consts = _client_ctx()  # relpath: app/clients/document_management_client.py
    idiom = HttpClientIdiom(
        name="only-sdk-dir", file_glob="**/sdk/*_client.py", class_glob="*Client",
    )
    result = extract_http_client(ctx, node_ids, ServiceIdioms(http_clients=[idiom]), consts)
    assert result.claims == []
    assert result.stats["http_calls_resolved"] == 0


def test_class_outside_class_glob_is_empty():
    ctx, node_ids, consts = _client_ctx()
    idiom = HttpClientIdiom(
        name="wrong-class", file_glob="**/clients/*_client.py", class_glob="*Gateway",
    )
    result = extract_http_client(ctx, node_ids, ServiceIdioms(http_clients=[idiom]), consts)
    assert result.claims == []


CALL_OUTSIDE_CLASS_SRC = b'''import aiohttp


async def fetch(base):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base}/documents") as resp:
            return await resp.json()
'''


def test_call_outside_any_class_is_empty():
    """A plain top-level function (no enclosing class at all) -- resolve_arg would
    happily resolve this call's arg0 to a <base>-prefixed template, proving the empty
    result here comes from the scope gate, not from URL-resolution failure."""
    ctx, node_ids, consts = _load(
        "app/clients/standalone_client.py", "svc", CALL_OUTSIDE_CLASS_SRC,
    )
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert result.claims == []


def test_call_directly_in_class_body_not_inside_any_method_is_empty():
    """A call in the class BODY itself (not inside a method) -- call.enclosing_def IS
    the class's own DefFact index, kind="class" -- must NOT be treated as "inside a
    method of the class"."""
    src = (
        b"import aiohttp\n\n\n"
        b"class BodyLevelClient:\n"
        b'    _default = aiohttp.ClientSession().get("/x")\n'
    )
    ctx, node_ids, consts = _load("app/clients/bodylevel_client.py", "svc", src)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert result.claims == []


BARE_CALL_SRC = b'''from some_lib import get


class BareCallClient:
    async def fetch(self, url):
        return get(url)
'''


def test_bare_non_attribute_call_is_not_a_candidate():
    """`get(url)` (no receiver at all) must not be confused with an attribute-call
    verb -- receiver_text stays None for a bare identifier callee."""
    ctx, node_ids, consts = _load("app/clients/barecall_client.py", "svc", BARE_CALL_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert result.claims == []


SHORT_RECEIVER_SRC = b'''import aiohttp


class ShortNameClient:
    def __init__(self, base_url):
        self._base_url = base_url

    async def fetch(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{self._base_url}/x") as resp:
                return await resp.json()
'''


def test_any_receiver_name_matches_not_just_session():
    """receiver_text любой (see module docstring's self-correction note): "s" doesn't
    even contain the substring "session", proving there is no receiver-text-content
    filter at all, only "is this an attribute-call"."""
    ctx, node_ids, consts = _load("app/clients/shortname_client.py", "svc", SHORT_RECEIVER_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert len(result.claims) == 1
    assert result.claims[0]["verb"] == "GET"


# -- negatives / branches: arg0 resolution --


RESOLUTION_BRANCHES_SRC = b'''import aiohttp


class BranchClient:
    def __init__(self, base_url):
        self._base_url = base_url

    async def plain_literal(self):
        async with aiohttp.ClientSession() as session:
            async with session.get("/documents/static") as resp:
                return await resp.json()

    async def fstring_no_base(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"/documents/{doc_id}") as resp:
                return await resp.json()

    async def relative_literal(self):
        async with aiohttp.ClientSession() as session:
            async with session.get("documents/relative") as resp:
                return await resp.json()

    async def no_arg(self):
        async with aiohttp.ClientSession() as session:
            async with session.get() as resp:
                return await resp.json()
'''


def _branch_ctx():
    return _load("app/clients/branch_client.py", "svc", RESOLUTION_BRANCHES_SRC)


def test_plain_string_literal_leading_slash_is_static():
    ctx, node_ids, consts = _branch_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    method_id = node_ids[_def(ctx, "plain_literal").index]
    claim = next(c for c in result.claims if c["src_id"] == method_id)
    assert claim["path_template"] == "/documents/static"
    assert claim["resolution_hint"] == "static"


def test_fstring_without_base_marker_but_leading_slash_is_heuristic():
    ctx, node_ids, consts = _branch_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    method_id = node_ids[_def(ctx, "fstring_no_base").index]
    claim = next(c for c in result.claims if c["src_id"] == method_id)
    assert claim["path_template"] == "/documents/{doc_id}"
    assert claim["resolution_hint"] == "heuristic"


def test_relative_string_literal_no_leading_slash_is_unresolved():
    ctx, node_ids, consts = _branch_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    claimed_ids = {c["src_id"] for c in result.claims}
    assert node_ids[_def(ctx, "relative_literal").index] not in claimed_ids


def test_missing_arg0_is_unresolved():
    ctx, node_ids, consts = _branch_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    claimed_ids = {c["src_id"] for c in result.claims}
    assert node_ids[_def(ctx, "no_arg").index] not in claimed_ids
    # relative_literal (no leading "/") + no_arg (arg0 missing entirely) both unresolved;
    # plain_literal + fstring_no_base both resolve fine.
    assert result.stats["http_url_unresolved"] == 2
    assert result.stats["http_calls_resolved"] == 2


CONST_NAME_ARG0_SRC = b'''import aiohttp

DOCS_PATH = "/documents/by-const"


class ConstArgClient:
    def __init__(self, base_url):
        self._base_url = base_url

    async def fetch(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(DOCS_PATH) as resp:
                return await resp.json()
'''


def test_module_const_name_arg0_resolves_through_shared_const_table():
    """arg0 is a NAME referring to a module-level string constant -- resolvable only
    through the ConstTable parameter (T6 review fix: built once per file in analyze.py
    and passed in), proving the passed-in table is actually consulted, not rebuilt
    or ignored."""
    ctx, node_ids, consts = _load("app/clients/constarg_client.py", "svc", CONST_NAME_ARG0_SRC)
    assert consts.get("DOCS_PATH") == "/documents/by-const"  # sanity: table saw the const
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert len(result.claims) == 1
    assert result.claims[0]["path_template"] == "/documents/by-const"
    assert result.claims[0]["resolution_hint"] == "static"


NON_URL_ARG0_SRC = b'''import aiohttp
import os


class WeirdClient:
    def __init__(self, base_url):
        self._base_url = base_url

    async def ping(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(os.environ["PING_URL"]) as resp:
                return await resp.json()
'''


def test_non_url_arg0_config_ref_increments_unresolved_stat_no_claim():
    ctx, node_ids, consts = _load("app/clients/weird_client.py", "svc", NON_URL_ARG0_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    assert result.claims == []
    assert result.stats["http_url_unresolved"] == 1


# -- verbs --


OTHER_VERBS_SRC = b'''import aiohttp


class VerbClient:
    def __init__(self, base_url):
        self._base_url = base_url

    async def do_put(self):
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{self._base_url}/x") as resp:
                return await resp.json()

    async def do_delete(self):
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self._base_url}/x") as resp:
                return await resp.json()

    async def do_patch(self):
        async with aiohttp.ClientSession() as session:
            async with session.patch(f"{self._base_url}/x") as resp:
                return await resp.json()

    async def do_head(self):
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{self._base_url}/x") as resp:
                return await resp.json()
'''


def test_put_delete_patch_verbs_recognized_head_is_not():
    ctx, node_ids, consts = _load("app/clients/verb_client.py", "svc", OTHER_VERBS_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[ANY_CLIENT_IDIOM]), consts,
    )
    verbs = {c["verb"] for c in result.claims}
    assert verbs == {"PUT", "DELETE", "PATCH"}
    head_id = node_ids[_def(ctx, "do_head").index]
    assert not any(c["src_id"] == head_id for c in result.claims)


# -- defensive: missing node id --


def test_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _client_ctx()
    result = extract_http_client(ctx, {}, ServiceIdioms(http_clients=[DEFAULT_SDK_IDIOM]), consts)
    assert result.claims == []
    assert result.stats["http_call_missing_node_id"] == 2


# -- cross-idiom dedup (mirrors kafka_ext's own producer dedup precedent) --


def test_cross_idiom_dedup_first_idiom_in_list_order_wins():
    """Both idioms match the SAME fixture class -- first idiom in list order claims
    both calls; the second idiom's own matches are silently skipped. (Synthetic
    builtin-first order here -- the REAL effective_idioms merge is custom-first since
    the T6 review fix, pinned by the dedicated regression test below.)"""
    ctx, node_ids, consts = _client_ctx()
    idioms = ServiceIdioms(http_clients=[AIOHTTP_CLIENT_BUILTIN_IDIOM, DEFAULT_SDK_IDIOM])
    result = extract_http_client(ctx, node_ids, idioms, consts)
    assert len(result.claims) == 2
    assert all(c["base_url_env"] is None for c in result.claims)


def test_cross_idiom_dedup_reversed_order_flips_winner():
    ctx, node_ids, consts = _client_ctx()
    idioms = ServiceIdioms(http_clients=[DEFAULT_SDK_IDIOM, AIOHTTP_CLIENT_BUILTIN_IDIOM])
    result = extract_http_client(ctx, node_ids, idioms, consts)
    assert len(result.claims) == 2
    assert all(c["base_url_env"] == "DOCUMENT_MANAGEMENT_URL" for c in result.claims)


# -- M6 T2: decorator-SDK mode (route_from/call/verb_from) -- pilot GAPS §2 gap 1 --
#
# Real convention (camunda-gateway app/clients/*.py, class *Client(BaseClient)):
#   @path_template("/v1/dmout/user_hv/uuid/{customer_uid}")     <- route lives HERE
#   async def get_client_hv_sign(self, customer_uid, **kwargs):
#       request = Request(Method.GET, self.host, ...)            <- verb lives HERE
#       return await self.driver.fetch_content(request, ...)     <- the CALL is HERE
# Old verb-mode never matches this shape at all (callee_name="fetch_content" not in
# _VERBS, arg0=`request` is not a URL). route_from switches extract_http_client to this
# second mode entirely, per-idiom.

DECORATOR_SDK_IDIOM = HttpClientIdiom(
    name="decorator-sdk",
    file_glob="**/clients/*.py",
    class_glob="*Client",
    route_from=HttpRouteFromSpec(decorator="path_template", arg=0),
    call="driver.fetch_content|driver.fetch",
    verb_from=HttpVerbFromSpec(request_ctor="Request", enum="Method"),
)


DMOUT_CLIENT_SRC = b'''from some_sdk import BaseClient, Method, Request, path_template


class DMoutClient(BaseClient):
    @path_template("/v1/dmout/user_hv/uuid/{customer_uid}")
    async def get_client_hv_sign(self, customer_uid, **kwargs):
        request = Request(Method.GET, self.host, customer_uid=customer_uid)
        return await self.driver.fetch_content(request)

    async def get_no_decorator(self, customer_uid):
        request = Request(Method.GET, self.host)
        return await self.driver.fetch_content(request)
'''


def _dmout_ctx():
    return _load("app/clients/dmout_client.py", "svc", DMOUT_CLIENT_SRC)


def test_decorator_sdk_single_claim_correct_path_and_verb():
    """GAPS §2 synthetic client: exactly one http_call claim, decorator path template
    (with its {customer_uid} placeholder untouched) and GET verb from Method.GET."""
    ctx, node_ids, consts = _dmout_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    method_id = node_ids[_def(ctx, "get_client_hv_sign").index]
    assert len(result.claims) == 1
    claim = result.claims[0]
    assert claim["src_id"] == method_id
    assert claim["path_template"] == "/v1/dmout/user_hv/uuid/{customer_uid}"
    assert claim["verb"] == "GET"
    assert claim["resolution_hint"] == "static"
    assert result.stats["http_calls_resolved"] == 1


def test_decorator_sdk_method_without_decorator_produces_no_claim():
    """Same file, second method calls the same driver indirection but carries no
    @path_template decorator at all -- must not produce a claim."""
    ctx, node_ids, consts = _dmout_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    no_dec_id = node_ids[_def(ctx, "get_no_decorator").index]
    assert not any(c["src_id"] == no_dec_id for c in result.claims)


FETCH_ALT_SRC = b'''from some_sdk import BaseClient, Method, Request, path_template


class AltClient(BaseClient):
    @path_template("/v1/alt/{id}")
    async def get_alt(self, id):
        request = Request(Method.POST, self.host)
        return await self.driver.fetch(request)
'''


def test_decorator_sdk_driver_fetch_alternative_matches():
    """`call` DSL is "|"-separated alternatives -- the SECOND alt ("driver.fetch", no
    "_content" suffix) must match too, proving alternation is actually tried in full,
    not just the first entry."""
    ctx, node_ids, consts = _load("app/clients/alt_client.py", "svc", FETCH_ALT_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert len(result.claims) == 1
    assert result.claims[0]["verb"] == "POST"
    assert result.claims[0]["path_template"] == "/v1/alt/{id}"


WRONG_RECEIVER_SRC = b'''from some_sdk import BaseClient, Method, Request, path_template


class WrongReceiverClient(BaseClient):
    @path_template("/v1/wrong/{id}")
    async def get_wrong(self, id):
        request = Request(Method.GET, self.host)
        return await self.other.fetch_content(request)
'''


def test_decorator_sdk_wrong_receiver_tail_produces_no_claim():
    """receiver-tail match is exact on dotted segments -- "self.other.fetch_content"
    must NOT match the "driver.fetch_content" alternative (only the callee name
    coincides; the receiver segment right before it differs: other != driver)."""
    ctx, node_ids, consts = _load(
        "app/clients/wrongrecv_client.py", "svc", WRONG_RECEIVER_SRC,
    )
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert result.claims == []


NO_VERB_SRC = b'''from some_sdk import BaseClient, path_template


class NoVerbClient(BaseClient):
    @path_template("/v1/noverb/{id}")
    async def get_noverb(self, id):
        return await self.driver.fetch_content(self.host)
'''


def test_decorator_sdk_verb_not_found_produces_no_claim_not_null_verb():
    """M6 T2 null-verb decision (see task-2-report.md): linking/http_routes.py's
    make_channel_node(kind="http_route") raises ValueError on a falsy method, and a
    None-verb claim can never match a real route (every staged route carries a real
    string http_method) -- it would ALWAYS fall to the unresolved-channel branch and
    crash there. So: no `Request(Method.X, ...)` found in the method body -> emit NO
    claim at all (never verb=None), bump a dedicated stat instead."""
    ctx, node_ids, consts = _load("app/clients/noverb_client.py", "svc", NO_VERB_SRC)
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert result.claims == []
    assert result.stats["http_verb_unresolved"] == 1


NO_DRIVER_CALL_SRC = b'''from some_sdk import BaseClient, Method, Request, path_template


class NoDriverCallClient(BaseClient):
    @path_template("/v1/nodrivercall/{id}")
    async def get_x(self, id):
        request = Request(Method.GET, self.host)
        return request
'''


def test_decorator_sdk_no_matching_call_site_produces_no_claim():
    """Decorator present, verb-ctor present -- but NEITHER `call` alternative is
    actually invoked anywhere in the method body -- still no claim (the driver
    call-site is mandatory, not just the decorator + Request ctor)."""
    ctx, node_ids, consts = _load(
        "app/clients/nodrivercall_client.py", "svc", NO_DRIVER_CALL_SRC,
    )
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert result.claims == []


WRONG_DECORATOR_NAME_SRC = b'''from some_sdk import BaseClient, Method, Request, other_decorator


class WrongDecoratorClient(BaseClient):
    @other_decorator("/v1/wrongdec/{id}")
    async def get_y(self, id):
        request = Request(Method.GET, self.host)
        return await self.driver.fetch_content(request)
'''


def test_decorator_sdk_non_matching_decorator_name_produces_no_claim():
    """Decorated, but with a DIFFERENT decorator than route_from.decorator names -- must
    not be mistaken for a route (distinct from the "zero decorators" case above: this
    one actually exercises the callee-name comparison inside _decorator_route)."""
    ctx, node_ids, consts = _load(
        "app/clients/wrongdec_client.py", "svc", WRONG_DECORATOR_NAME_SRC,
    )
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert result.claims == []


def test_decorator_sdk_class_outside_class_glob_is_empty():
    ctx, node_ids, consts = _dmout_ctx()
    idiom = HttpClientIdiom(
        name="wrong-class", file_glob="**/clients/*.py", class_glob="*Gateway",
        route_from=HttpRouteFromSpec(decorator="path_template", arg=0),
        call="driver.fetch_content|driver.fetch",
        verb_from=HttpVerbFromSpec(request_ctor="Request", enum="Method"),
    )
    result = extract_http_client(ctx, node_ids, ServiceIdioms(http_clients=[idiom]), consts)
    assert result.claims == []


def test_decorator_sdk_missing_node_id_skips_gracefully():
    ctx, _real_node_ids, consts = _dmout_ctx()
    result = extract_http_client(
        ctx, {}, ServiceIdioms(http_clients=[DECORATOR_SDK_IDIOM]), consts,
    )
    assert result.claims == []
    assert result.stats["http_call_missing_node_id"] == 1


def test_verb_mode_idiom_without_route_from_is_unaffected_byte_identical():
    """route_from absent -> existing verb-mode path is completely untouched by the new
    branch (regression guard alongside every pre-existing test in this module, which
    stays green unmodified)."""
    ctx, node_ids, consts = _client_ctx()
    result = extract_http_client(
        ctx, node_ids, ServiceIdioms(http_clients=[DEFAULT_SDK_IDIOM]), consts,
    )
    assert len(result.claims) == 2
    assert result.stats["http_verb_unresolved"] == 0


def test_real_effective_idioms_custom_sdk_shadows_builtin_base_url_env():
    """T6 review fix regression, on the REAL fixtures/workspace.yaml: kyc-worker's
    effective idioms contain BOTH the env-less builtin aiohttp-client-convention AND
    the custom default-sdk idiom (base_url_env=DOCUMENT_MANAGEMENT_URL), and both
    globs match this fixture class. `effective_idioms` merges service idioms FIRST
    (they shadow builtins), so first-idiom-wins dedup resolves every call through the
    custom idiom -- both claims must carry the env. Was RED before the loader fix:
    builtin-first ordering made base_url_env come out None on a real merged run."""
    cfg = load_workspace(WORKSPACE_YAML)
    kyc = next(s for s in cfg.services if s.name == "kyc-worker")
    idioms = effective_idioms(cfg, kyc)
    ctx, node_ids, consts = _client_ctx()
    result = extract_http_client(ctx, node_ids, idioms, consts)
    assert len(result.claims) == 2
    assert all(c["base_url_env"] == "DOCUMENT_MANAGEMENT_URL" for c in result.claims)
    assert all(c["resolution_hint"] == "static" for c in result.claims)
