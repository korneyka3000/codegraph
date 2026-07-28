"""temporal_ext: Temporal workflow/activity structural extractor (M2 T5).

Like fastapi_ext, this is a STRUCTURAL extractor -- its patterns (`@workflow.defn`,
`@activity.defn`, `workflow.execute_activity(...)`, `*.start_workflow(...)`) are
hardcoded, not data-driven through the idiom DSL (ServiceIdioms), because Temporal's
own decorator/call vocabulary IS the idiom: a service either uses the temporalio SDK
this way or it doesn't. `builtin_idioms.py`'s "temporal" entry is (deliberately) an
empty ServiceIdioms; activation is purely "temporal" ∈ active_idioms (see analyze.py).

  - `@workflow.defn` / `@activity.defn`: `idiom_match.match_decorators` (both are bare,
    non-call decorators -- no CallFact needed, unlike fastapi's `router.get("/x")`)
    grants TemporalWorkflow/TemporalActivity roles; workflow.defn additionally patches
    `workflow_name` onto the class's own node_props (the class's DISPLAY name, not a
    qualified/resolved value -- matches the brief's "имя класса" wording exactly).
  - `workflow.execute_activity(fn_ref, ...)`: matched structurally (callee_name IN
    _ACTIVITY_INVOKE_CALLEES AND receiver_text == "workflow" -- confirmed against the
    real kyc_worker/app/workflows/kyc.py fixture, see this module's test file; NOT a
    glob on receiver, unlike start_workflow below). arg0's own name/attr byte span
    (already exactly ArgFact.name_start_byte/end_byte -- no span math, ArgFact already
    carries "attr" as the LAST segment per T2's contract) resolves via
    ctx.ref_symbol_lookup to the activity's node id. src is the enclosing DEF id as-is
    (the method, e.g. `KycWorkflow.run` -- the brief's own initial "raise to the
    workflow class" idea is explicitly overridden by checking the golden fixture:
    golden src IS the method).
  - `*.start_workflow(fn_ref, ...)`: glob on receiver (ANY receiver -- Temporal client
    handles are typically obtained dynamically, e.g. `client = await Client.connect(...)`,
    so there's no fixed name to check the way execute_activity has "workflow"; see
    test_start_workflow_matches_any_receiver_not_just_client). callee_name IN
    _START_WORKFLOW_CALLEES (M6 T1, below). Produces a "temporal_start_mark" CLAIM,
    not a direct edge -- see design-decision note below.

M6 T1 (pilot report docs/superpowers/reports/2026-07-23-pilot-real-services-gaps.md
§3/§4): a real-stack pilot on camunda-gateway found BOTH matchers above were strict
`==` comparisons against a single literal name, so `INVOKES_ACTIVITY` came out at
**zero** edges despite 43 activities and 80 real call sites -- the code there calls
`workflow.execute_activity_method(SomeActivity.some_method, ...)` (a bound-method
ref), never bare `execute_activity`; and `start_workflow`'s claim only resolved 4/7
real child-workflow starts, missing `start_child_workflow`/`execute_child_workflow`.
Fix is deliberately minimal (GAPS §3: "резолвится одинаково" -- arg0 resolution is
byte-identical regardless of which name matched): each `==` became an `in` against a
frozenset of Temporal's own SDK vocabulary for that operation --
`_ACTIVITY_INVOKE_CALLEES` (receiver check unchanged, still exactly "workflow") and
`_START_WORKFLOW_CALLEES` (receiver check unchanged too -- still no check at all, any
receiver). No other line in either matcher function changed.

M9 T4 (backlog M6-carry, progress.md ledger M6-T1 entry: "client.execute_workflow
вне пилотных находок -- сознательно вне скоупа"): `client.execute_workflow(fn_ref,
...)` -- the CLIENT-side spelling that starts a workflow and synchronously awaits
its result (Temporal's own sugar for `start_workflow` + `handle.result()`) -- is
added to `_START_WORKFLOW_CALLEES` here, closing the one name M6 T1 deliberately
left out of its own pilot-scoped widening (that pilot's evidence never exercised
it). Same any-receiver, claim-only (never a direct edge) contract as every other
member of the set -- no new branch/guard needed, since the set was already
receiver-agnostic by design.

Design decision on the temporal_start_mark claim (brief's explicit "реши и
задокументируй"): `dst_id` is resolved NOW, at extraction time, via
ctx.ref_symbol_lookup on arg0's own last-segment span (`KycWorkflow.run` -> the "run"
token's span -- ArgFact.name_start_byte already IS that span for both "attr" and "name"
value_kinds per T2's contract, so no extra span arithmetic is needed here either) --
NOT a deferred (relpath, dst_start_byte) pair for S7 to re-resolve later. Why this is
"чище" (cleaner), as the brief invites: S7 (linking) runs workspace-wide AFTER every
service's own analyze_service call, by which point per-service SCIP ref-lookup closures
are gone; resolving here, while ctx.ref_symbol_lookup is still in scope, means S7's own
job (per the brief: "S7 просто update_edge_props") stays a pure "does a (src_id, dst_id,
CALLS) edge already exist -> tag it {mechanism: temporal_start, ...}" lookup with no
per-service SCIP access of its own to wire up. Concretely, S7 needs BOTH ids to find the
CALLS edge in the first place (`update_edge_props(src, dst, "CALLS", ...)` takes the
full (src, dst, type) key) -- carrying a raw span instead of dst_id would just move the
identical ref-lookup into S7 for no benefit, at the cost of S7 needing per-service
Staging.ref_symbol_at access it doesn't otherwise need. If the ref can't be resolved (no
ref_symbol_lookup wired at all, or the lookup misses -- e.g. degraded/heuristic fallback,
which never lays refs inside call arguments any more than it does for fastapi's
DEPENDS_ON), no claim is emitted: there is nothing for S7 to mark without a dst.

Note on WHY a claim (deferred write) is needed at all, rather than temporal_ext just
emitting a CALLS edge itself: S6's build_calls (extractors/calls.py) joins purely on
each CallFact's OWN callee span -- for `client.start_workflow(KycWorkflow.run, ...)`
that span is "start_workflow" itself, which resolves to the external temporalio SDK
(or nothing, in degraded mode), never to `KycWorkflow.run` -- and `KycWorkflow.run`
passed bare (no call parens) is never itself a CallFact either. So S6 can never produce
the `handle_order_created -> KycWorkflow.run` CALLS edge the golden fixture expects
(see fixtures/golden/edges.yaml's own policy note on this exact case) -- it has to be
synthesized. Whether the eventual write is a fresh CALLS edge or a props-patch onto one
is S7's call (T7, not in scope here); this extractor's job is only to surface the fact
with everything S7 needs to act on it.

TemporalResult's field list is `(roles, node_props, edges, claims, stats)` --
deliberately not the plan doc's abbreviated top-line `TemporalResult(roles, edges,
claims)`: node_props is required (nowhere else to deliver workflow_name -- this module
only ever sees `node_ids: dict[int, str]`, never NodeRec objects, so a props-patch dict
is the only channel analyze.py's `_apply_role_props_patch` can consume), and `stats` is
added for parity with kafka_ext/fastapi_ext (not read downstream yet, same as
fastapi_ext.stats today). This mirrors T4's own documented precedent (progress.md:
"FastapiResult без claims, с node_props -- прозой плана суперсидится top-line
сигнатура") of the plan's prose description winning over its own abbreviated signature
line. (M7 T4, below, adds a `channels` field -- the "temporal never creates Channel
nodes" claim this paragraph originally made no longer holds; see that section.)

M7 T4 (OPEN R3, docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md): Temporal
signals as first-class channels -- 34 real `@workflow.signal|query|update` handlers +
45 `.signal(` sender call-sites + 3 `get_external_workflow_handle` uses, all invisible
before this task (grep against the extractor turned up zero matches). Binding design
(M7 plan): REUSE PRODUCES/CONSUMES over a new Channel kind "temporal_signal" -- no new
edge types at all (EDGE_TYPES/core/schema.py is untouched); trace_process/
linking.segments.derive group PRODUCES/CONSUMES purely by edge type + channel id,
never by Channel.props["channel_kind"] (verified by reading that module), so the
signal hop is picked up by the EXISTING grouping for free, no linking-layer change
needed. `TemporalResult` gains a `channels: list[NodeRec]` field (placed right after
`node_props`, mirroring FastapiResult's own `roles, node_props, channels, edges, ...`
order) -- both handlers and senders create `Channel(kind="temporal_signal")` nodes,
unlike every OTHER edge this module emits.

  - Handlers: `@workflow.signal`/`@workflow.update`-decorated methods share ONE role,
    TemporalSignalHandler (mirrors the activity-role precedent) -- `_extract_signal_
    kind_roles` is one shared helper parameterized by (decorator pattern, signal_kind
    string), called once per decorator kind so the two stay byte-identical in shape.
    `@workflow.query` is role-only (`_extract_query_roles`, its own smaller function --
    NO channel/edge at all, since a query is a synchronous read, not an async
    boundary a sender ever "produces" into). All three write `node_props["signal_kind"]`
    (mirrors `workflow_name`'s own node_props-patch convention) -- this is the ONLY
    place `signal_kind` lives when the decorator is `query` (no edge exists to also
    carry it), so it is deliberately a node prop first and foremost; signal/update
    additionally mirror it onto their own CONSUMES edge's props for query-ability
    without a node join.

    Channel identity: the decorator's own `name=` kwarg (a STRING LITERAL only --
    decorators are never visited as CallFacts by build_file_facts, since a decorated
    def's decorator expression lives outside `body` and is never walked as a `call`
    node, M1a's own carried-forward "outside body" limitation; `_mini_decorator_call`
    re-parses one decorator's raw text standalone to recover a real CallFact, the
    EXACT mirror of fastapi_ext.py's own `_mini_call` -- see that module's docstring
    for the full "why" this is needed at all) falling back to the method's OWN name
    when name= is absent (bare `@workflow.signal`), a call-form decorator with no
    name= kwarg at all (`@workflow.signal()`), or name= present but non-string/empty.
    Deliberately NOT const-resolved against the file's ConstTable (unlike the sender
    side below) -- mirrors fastapi_ext._route_prefix's own `APIRouter(prefix=...)`
    kwarg convention (string-literal only), and every real fixture this task's own
    pilot evidence quotes (`@workflow.signal(name="complete-survey")`, `"survey-ready"`,
    ...) is already a literal. A `name=SOME_CONST` kwarg is a documented, deliberate
    scope boundary, not a silent gap: falling back to the method name (same as no
    name= at all) is a strictly SAFER default than emitting a wrong/absent channel,
    and no real fixture exercises this shape.

  - Senders: `<handle>.signal("<name>", ...)` and `get_external_workflow_handle(...).
    signal(...)` share ONE matcher, `_extract_signal_senders` -- callee_name == "signal"
    EXACTLY, receiver checked NOT AT ALL (mirrors `_extract_start_workflow_claims`'s
    own no-receiver-check precedent for "ANY receiver", one level more permissive than
    `execute_activity`'s fixed `receiver_text == "workflow"` check above). Chained-call
    receivers (`get_external_workflow_handle(wf_id).signal(...)`) need NO special
    casing at all: CallFact.receiver_text is the raw source TEXT of the call's own
    `object` field regardless of that field's node type (parsing/facts.py's own
    contract comment: "receiver_text = весь текст object-поля, включая вложенные
    точки" -- text, not a structural walk), so for a chained call it comes back as
    the literal string "get_external_workflow_handle(wf_id)", simply never inspected
    by a matcher that ignores receiver entirely -- pinned by
    test_signal_sender_via_external_workflow_handle_chained_call_produces in this
    module's own test file, which asserts the exact receiver_text value AND that
    extraction succeeds through the identical code path as a plain
    `handle.signal(...)` call.

    arg0 resolution (`_resolve_signal_arg0`) is a three-way split, chosen over a
    plain binary resolved/unresolved because a receiver-agnostic, callee-name-only
    match (the weakest-evidence pattern in this entire codebase -- even weaker than
    idiom_match's own IMPORT_NAME tier, which at least requires an import statement
    corroborating the callee) needs to separate "doesn't even look like a signal
    call" from "looks like one but we honestly can't resolve it", rather than
    bumping a miss counter for every non-Temporal `.signal(...)` call in a codebase
    (an honest, but noisy, per-call-site classification cost this task's brief
    explicitly asked to avoid via "noise guard"):
      1. arg0 is a STRING literal (`value_kind == "string"`), or a bare NAME
         resolving through `consts.resolve_arg` to a real value (a module-level
         `NAME = "literal"` constant, per this task's brief: "Consumes: consts
         (arg0-литерал имени)"; an ATTR-shaped arg0 goes through the same
         resolve_arg call for uniformity but can never come back kind="value"
         today -- consts.py's attr paths only ever yield config_ref/unresolved --
         so every attr lands in bucket 2 in practice) -> MATCH: PRODUCES into
         Channel(temporal_signal,
         name). An empty string ("" literal, or a const resolving to "") is treated
         as an honest miss (bucket 2), not a match -- same "M2 final review" empty-
         name crash guard every make_channel_node call site elsewhere in this
         codebase already carries (kafka_ext.py), just applied a half-step earlier
         here (before ever calling make_channel_node) since the empty-vs-populated
         distinction also decides which stat bucket this call falls into.
      2. arg0 IS name/attr-shaped (`value_kind in ("name", "attr")`) but does NOT
         resolve to a usable value (a runtime variable, an attribute chain neither a
         module const nor recognized by consts.py's os.environ/getenv/settings.X
         textual fallback) -> counts as an honest miss, `signal_name_unresolved`
         (this task's brief-mandated counter, wired into the per-service report --
         see pipeline/analyze.py and pipeline/report.py). This is deliberately the
         SAME bucket a `settings.X`-shaped attr expression falls into (resolves to
         `Resolved(kind="config_ref")`, not "value") -- still name-like, still
         honestly unresolvable to a concrete identity, still worth counting.
      3. Anything else (a numeric/bool/None/dict/list literal, an f-string, no arg0
         at all) -> SILENTLY skipped, no counter at all: "not a signal-looking call"
         (brief's own framing) -- these shapes don't read as a signal-NAME reference
         in the first place, so treating every one of them as a miss would just
         manufacture noise for the overwhelming majority of unrelated `.signal(...)`
         methods (Qt-style signal objects, etc.) any receiver-agnostic matcher will
         inevitably sweep in. An f-string arg0 (`.signal(f"signal-{x}", ...)`) is a
         genuinely plausible real Temporal call this bucket still discards -- a
         documented, deliberate scope boundary (consts.resolve_arg WOULD resolve it
         to a "template" Resolved, but this function never asks past the value_kind
         gate above), not an oversight; see this module's own test file for the pin.

    Documented, ACCEPTED false-positive risk (brief: "document the FP-risk
    honestly"): because there is no receiver-TYPE check, any unrelated `.signal(...)`
    method (Qt-style signal objects, etc.) structurally collides -- a string arg0
    would even produce a (mechanism-tagged) edge. This is accepted noise, not a bug:
    a real receiver-type check is impossible here (real Temporal handles are obtained
    too dynamically, e.g. `handle = await client.get_workflow_handle(...)`, to check
    by name the way `execute_activity`'s fixed "workflow" receiver can).
    `props={"mechanism": "temporal_signal"}` on every emitted PRODUCES edge marks
    this provenance explicitly, so a graph consumer can identify (and, if ever
    needed, filter or deprioritize) edges from this specific, weaker-evidence
    matcher.

    ONE exact-name carve-out (M7 T4 review follow-up) narrows the loudest instance:
    a receiver literally spelled `signal` -- Python's OWN stdlib
    `signal.signal(signal.SIGTERM, handler)`, whose arg0 is attr-shaped/unresolvable
    and previously landed in bucket 2, bumping `signal_name_unresolved` on virtually
    every service (SIGTERM/SIGINT installation is that common) -- is dropped before
    arg0 classification entirely: neither edge nor counter. This is an EXCLUSION of
    one specific non-Temporal spelling (no real Temporal handle variable is named
    `signal`), not a positive receiver filter -- the design above stands. Known
    limit: an aliased `import signal as sig; sig.signal(...)` is NOT filtered and
    still lands in the honest-miss bucket like any other name-like unresolvable
    arg0. Pinned by
    test_stdlib_signal_signal_receiver_filtered_no_edge_no_counter in this module's
    own test file (which also documents the alias limit), so a future change cannot
    silently regress either direction -- start counting the stdlib idiom again, or
    start emitting a bogus PRODUCES edge for it.

    Resolution/confidence: the handler-side CONSUMES edge uses this module's
    existing `_RESOLUTION`/`_CONFIDENCE` ("static", 1.0) constants, same as
    INVOKES_ACTIVITY above -- a decorator match IS the full, unambiguous ground
    truth here (mirrors fastapi_ext.py's own HANDLES edge, also "static"/1.0 from a
    decorator match alone). The sender-side PRODUCES edge instead uses a fixed
    ("heuristic", 0.6) -- NOT derived from any idiom_match MatchTier (there is no
    tiered matching here at all, just a bare callee-name check), chosen because 0.6
    is this codebase's own established floor for "a weak, textually-matched
    reference with no receiver/import corroboration" (idiom_match.MatchTier.
    IMPORT_NAME) -- and this matcher has LESS corroboration than even that tier (no
    import-statement check either), so reusing the existing floor value keeps the
    number inside this codebase's own established resolution/confidence vocabulary
    rather than inventing a new, one-off magic constant.

    No cross-request dedup at PRODUCES-edge-emission time (unlike kafka_ext.py's own
    enum-fanout path, `_emit_enum_fanout_produces`): two `.signal("x")` call sites
    sharing one enclosing method both independently append a fresh Channel(...) +
    EdgeRec to this extractor's own output lists; staging's (src, dst, type,
    via_channel, origin_service) PRIMARY KEY then collapses them to one row, last
    write's evidence winning. This is the SAME "architecturally shared, pre-existing,
    accepted" property kafka_ext.py's OWN single-channel emit paths
    (`_emit_kafka_topic_produces`/`_emit_event_type_produces`/the call/dispatch_dict
    consumer paths) already have -- see that module's own `_emit_enum_fanout_produces`
    docstring for the full "only fan-out (N edges per call site, not 1) needs its own
    dedup" argument, which does not apply here (one edge per matched call site).

    Cross-service edge invariant (staging.upsert_edges, verified by reading its own
    logic directly, brief: "no invariant change needed; verify"): PRODUCES/CONSUMES
    into a `chan:temporal_signal:...` id already passes today, unconditionally --
    `chan_or_proc_endpoint` short-circuits the same-service check whenever EITHER
    endpoint starts with "chan:"/"proc:", and every edge this section emits has a
    chan:-prefixed dst. No schema/staging change was needed for signal channels to
    legitimately bridge two different services' own sym: ids (e.g. a consumer in
    service A signaling a workflow that also lives in service A is the common case,
    but `get_external_workflow_handle` legitimately crosses services too) -- exactly
    the same "channels are the legal bridge" property kafka/http_route channels
    already rely on.

    Handler-side identity comes from decorator TEXT (a literal or the method's own
    name) -- unaffected by M8 T2 below, still no ref_symbol_lookup involved, still
    resolves identically under real SCIP and the degraded heuristic fallback.
    Sender-side identity, THROUGH M7, came ONLY from `consts.resolve_arg` (module-
    level literals only), the same real-SCIP/degraded-fallback-identical property.
    M8 T2 (rerun-2 R5, below) adds a SECOND, `ctx.ref_symbol_lookup`-based resolution
    path for the typed-method-reference shape -- so this paragraph's claim no longer
    holds for the sender side as originally stated: a typed
    `handle.signal(Cls.method, ...)` call now resolves under real SCIP (via
    ref_symbol_lookup, exactly like INVOKES_ACTIVITY) but NOT under the degraded
    fallback (which never lays a ref at an ARGUMENT span -- the same documented gap
    INVOKES_ACTIVITY/start_workflow already carry, see
    test_pipeline_analyze.py::test_analyze_temporal_active_degraded_fallback_cannot_
    resolve_invokes_activity for the parallel case). The STRING-literal/module-const
    sender path is completely unaffected and still resolves identically either way.

M8 T2 (rerun-2 R5, docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
real Temporal code overwhelmingly uses the canonical TYPED sender API -- a bound-
method/function REFERENCE as arg0, not a string -- e.g.
`handle.signal(PartnerProfileWorkflow.complete_survey, payload)` (attr-shaped) or a
bare-name imported method, `handle.signal(step_other_evidence_update, payload)`
(name-shaped) -- which `_resolve_signal_arg0`'s PRE-EXISTING `consts.resolve_arg`
call can never resolve (consts.py only ever knows MODULE-LEVEL STRING LITERALS; a
class-method attr chain or an imported function name is neither): every real M7 T4
sender site landed in `signal_name_unresolved` before this task (rerun-2's own
evidence: 18 of 18 real send-sites, PRODUCES into temporal_signal channels == 0,
even though 25 handler channels already existed to pair them with).

Why this is solvable WITHOUT any receiver-type check (this matcher still has none --
see the FP-risk section above, unchanged by this task): the target CHANNEL NAME is
already fully known from the OTHER side. The referenced method itself carries
`@workflow.signal(name=...)` (or falls back to its own method name), and the
handler-side path above (`_extract_signal_kind_roles`) already turns that into a
`chan:temporal_signal:<name>` CONSUMES edge. This task only needs to connect arg0 to
THAT method's IDENTITY -- the channel name itself is never re-derived here, and
never will be re-derived in this file at all (see below).

Design (the brief's fix direction (a), "claim + линковка" -- explicitly preferred
over its own alternative (b), an in-file fast path keying off a same-file decorator:
a typed reference is routinely CROSS-FILE -- `PartnerProfileWorkflow` imported from
another module entirely -- so an in-file-only fast path would silently miss the
overwhelming majority of real sites; cross-file within ONE service is the case that
matters here, see the cross-SERVICE TRACKED LIMITATION below).
`_resolve_signal_arg0`'s own three-way split is UNCHANGED, byte-for-byte: the
string-literal branch and the "a bare NAME
resolves through the file's own ConstTable to a module-level string constant"
branch (`SIGNAL_NAME = "x"; handle.signal(SIGNAL_NAME, ...)`) both keep the EXACT
SAME heuristic/0.6 direct-PRODUCES-edge behavior as before this task. ONLY when
THAT resolution fails (arg0 IS name/attr-shaped -- so it still LOOKS like a
signal-name reference -- but names no known constant) does `_extract_signal_senders`
now attempt a SECOND resolution: `_resolve_ref` on arg0's own span
(`arg0.name_start_byte` -- already exactly the right span for both "attr" and
"name" value_kinds, per T2's contract -- no new span arithmetic needed) -- the
IDENTICAL helper/span convention `_extract_invokes_activity` already uses for
`execute_activity(fn_ref, ...)`'s own arg0.

Resolved -> a `sym:`-id method node. UNLIKE invokes_activity/start_workflow, though,
this file does NOT itself know whether that method is even a signal handler, let
alone which channel (or service) it answers to -- that fact lives in a DIFFERENT
file's decorator, possibly a different service entirely -- so no edge is ever
emitted here. Instead, a `temporal_signal_send` CLAIM is appended: `{src_id:
<the sender's own enclosing-method node id>, method_symbol: <the resolved node id>,
evidence_line}` (staged per-file via `staging.add_claims`, the SAME deferred-write
pattern `temporal_start_mark` already established two sections above -- see that
paragraph for why a claim, not a direct edge, is the right shape whenever one
extractor pass can't see everything a fact requires). `TemporalResult` gains a
SEPARATE `signal_send_claims` field for this -- kept apart from the pre-existing
`claims` field (which stays temporal_start_mark-only, untouched) rather than merged
into one flat list conflating two unrelated claim shapes -- mirrors fastapi_ext.py's
own multi-typed-claim-list precedent (`route_decl_claims`/`router_include_claims`/
`router_decl_claims`), each claim kind staged under its own `kind` string by
analyze.py.

TRACKED LIMITATION (M8 T2 review Important-1) -- cross-SERVICE typed sends do NOT
resolve through this mechanism: `_resolve_ref` stamps ctx.service -- the SENDER's
own service -- into the resolved node id (`symbol_to_node_id(ctx.service, ...)`,
package-blind by design, see resolvers/scip/symbols.py), so a typed reference to
ANOTHER service's method -- expressible at all only when that service's code is
importable from this one (a monorepo layout; separate-repo services, the pilot's
own layout, cannot even write the shape) -- resolves to a `sym:<sender-service>:...`
id that linking/signal_send.py's CONSUMES map (keyed by the HANDLER service's own
`sym:<handler-service>:...` id) can never match. Such a claim lands honestly in
`signal_send_unlinked` at link time -- never a wrong edge (fails safe). Real
cross-service signaling instead uses
`get_external_workflow_handle(wf_id).signal("<name>", ...)` with a STRING --
precisely because the foreign class isn't importable -- which the unchanged
string-literal path above already fully covers (heuristic/0.6, the channel as the
legal cross-service bridge; the pilot's only cross-service signal is exactly this
shape, fixtures/realstack's worker->gateway "doc-approved"). Lifting this would
need a package-aware symbol->service mapping inside `_resolve_ref` -- deliberately
out of scope, the same honesty-over-guessing reasoning as http_client_ext.py's own
inherited-self.host TRACKED LIMITATION (M7 T3 review Important-2).

Still genuinely unresolvable (arg0 IS name/attr-shaped, but NEITHER consts NOR
ref_symbol_lookup can name anything -- a true runtime variable, or a real SCIP miss)
-> the PRE-EXISTING `signal_name_unresolved` counter, unchanged semantics: this task
narrows WHEN that bucket is reached (a real typed method reference no longer falls
into it), it does not touch what falls into it once every resolution avenue is
exhausted. A new stats key, `signal_sender_symbol_resolved` (mirrors
`invokes_activity_resolved`/`start_workflow_resolved`'s own "a claim was
successfully emitted" naming precedent -- NOT promoted to the per-service report,
same as those two: see pipeline/analyze.py's own `temporal_stats` dict, which has
only ever surfaced `signal_name_unresolved`), counts a successful claim emission
SEPARATELY from `signal_sender_resolved` (which keeps its PRE-EXISTING meaning -- a
literal/const NAME resolved straight to a direct edge -- completely untouched).

The other half of this fix -- turning a `temporal_signal_send` claim into a real
PRODUCES edge -- is deliberately NOT this file's job: see `linking/signal_send.py`
(S7, wired into `linking.workspace.link_workspace` before `segments.derive`) for the
full algorithm (a workspace-wide CONSUMES lookup keyed by the claim's own
`method_symbol`; the honesty rule for a resolved-but-not-a-handler symbol --
`signal_send_unlinked`, no edge, no guessing; the dedup argument for repeat call
sites). This split mirrors `temporal_start_mark`'s own S5/S7 division of labor
exactly: extraction resolves everything IT can see (here: the sender's OWN identity
and the target method's SYMBOL) and stops there -- cross-file channel membership is
a workspace-wide fact no single-file S5 pass can ever answer honestly.
"""

from __future__ import annotations

from dataclasses import dataclass

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.extractors.idiom_match import match_decorators
from codegraph.parsing.consts import ConstTable, resolve_arg
from codegraph.parsing.facts import ArgFact, CallFact, build_file_facts
from codegraph.resolvers.scip.symbols import symbol_to_node_id

from .base import FileContext

_EXTRACTOR = "temporal"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

# M7 T4 (OPEN R3): decorator patterns for the shared signal/update channel-emitting
# path -- (idiom_match.match_decorators pattern, node_props["signal_kind"] value).
# workflow.query is handled separately (_extract_query_roles): role only, no channel.
_SIGNAL_KIND_DECORATORS = (
    ("workflow.signal", "signal"),
    ("workflow.update", "update"),
)
_QUERY_DECORATOR_PATTERN = "workflow.query"

# M6 T1 (pilot GAPS §3): every Temporal SDK spelling of "invoke an activity from a
# workflow" -- receiver is still required to be exactly "workflow" (unchanged).
_ACTIVITY_INVOKE_CALLEES = frozenset({
    "execute_activity",
    "execute_activity_method",
    "execute_local_activity",
    "execute_local_activity_method",
    "start_activity",
    "start_local_activity",
})

# M6 T1 (pilot GAPS §4): every Temporal SDK spelling of "start a (child) workflow" --
# receiver stays unchecked (ANY receiver, unchanged -- see module docstring).
# M9 T4 (backlog M6-carry): "execute_workflow" added -- the CLIENT-side synchronous
# spelling, consciously deferred by M6 T1's own pilot-scoped widening (see module
# docstring's M9 T4 note) -- same any-receiver/claim-only contract as every other
# member here.
_START_WORKFLOW_CALLEES = frozenset({
    "start_workflow",
    "start_child_workflow",
    "execute_child_workflow",
    "execute_workflow",
})


@dataclass(frozen=True)
class TemporalResult:
    roles: dict[str, set[str]]
    node_props: dict[str, dict]
    # M7 T4 (OPEN R3): signal/update/query handlers and their `.signal(...)` senders
    # both create Channel(kind="temporal_signal") nodes -- placed right after
    # node_props, mirroring FastapiResult's own field order.
    channels: list[NodeRec]
    edges: list[EdgeRec]
    claims: list[dict]
    # M8 T2 (rerun-2 R5): `temporal_signal_send` claims -- typed-sender arg0
    # (attr/name-shaped, resolved via ctx.ref_symbol_lookup to a METHOD SYMBOL node
    # id) {src_id, method_symbol, evidence_line}, consumed by linking/signal_send.py
    # (S7) to pair with the handler's own CONSUMES edge. Kept SEPARATE from `claims`
    # above (which stays temporal_start_mark-only, unchanged) rather than merged into
    # one flat list -- mirrors fastapi_ext's own multi-typed-claim-list precedent
    # (route_decl_claims/router_include_claims/router_decl_claims), each staged under
    # its own claim `kind` by analyze.py. See module docstring's M8 T2 section.
    signal_send_claims: list[dict]
    stats: dict[str, int]


def _stats() -> dict[str, int]:
    return {
        "defn_missing_node_id": 0,
        "invokes_activity_resolved": 0,
        "invokes_activity_unresolved": 0,
        "invokes_activity_missing_node_id": 0,
        "start_workflow_resolved": 0,
        "start_workflow_unresolved": 0,
        "start_workflow_missing_node_id": 0,
        # M7 T4 (OPEN R3): signal/update/query handler roles (+ CONSUMES channel for
        # signal/update) -- see module docstring for the full design.
        "signal_handler_missing_node_id": 0,
        # M7 T4: `.signal(...)` sender call-sites -- PRODUCES into the SAME
        # temporal_signal channel a handler CONSUMES from.
        "signal_sender_missing_node_id": 0,
        "signal_sender_resolved": 0,
        # M8 T2 (rerun-2 R5): arg0 resolved via ctx.ref_symbol_lookup to a METHOD
        # SYMBOL (a typed `Cls.method`/bare-name reference) -- a `temporal_signal_send`
        # claim was emitted, NOT a direct edge (see module docstring). Deliberately
        # separate from `signal_sender_resolved` above, which keeps its PRE-EXISTING
        # meaning (a literal/const NAME resolved straight to a direct edge).
        "signal_sender_symbol_resolved": 0,
        "signal_name_unresolved": 0,
    }


def _resolve_ref(ctx: FileContext, start_byte: int | None) -> str | None:
    if start_byte is None or ctx.ref_symbol_lookup is None:
        return None
    sym = ctx.ref_symbol_lookup(ctx.relpath, start_byte)
    if sym is None:
        return None
    return symbol_to_node_id(ctx.service, ctx.relpath, sym)


def _arg0(call: CallFact) -> ArgFact | None:
    """First POSITIONAL argument of `call`, or None -- shared by all three
    arg0-consuming matchers below (invokes_activity/start_workflow/signal senders;
    M7 T4 review: the identical one-liner had accreted three copies)."""
    return next((a for a in call.args if a.index == 0), None)


def _extract_defn_roles(
    ctx: FileContext, node_ids: dict[int, str],
    roles: dict[str, set[str]], node_props: dict[str, dict], stats: dict[str, int],
) -> None:
    for d, _text in match_decorators("workflow.defn", ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["defn_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalWorkflow")
        node_props.setdefault(node_id, {})["workflow_name"] = d.name

    for d, _text in match_decorators("activity.defn", ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["defn_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalActivity")


def _extract_invokes_activity(
    ctx: FileContext, node_ids: dict[int, str], edges: list[EdgeRec], stats: dict[str, int],
) -> None:
    for call in ctx.facts.calls:
        if call.callee_name not in _ACTIVITY_INVOKE_CALLEES or call.receiver_text != "workflow":
            continue
        enclosing_id = node_ids.get(call.enclosing_def)
        if enclosing_id is None:
            stats["invokes_activity_missing_node_id"] += 1
            continue
        arg0 = _arg0(call)
        activity_id = _resolve_ref(ctx, arg0.name_start_byte) if arg0 is not None else None
        if activity_id is None:
            stats["invokes_activity_unresolved"] += 1
            continue
        edges.append(EdgeRec(
            src=enclosing_id, dst=activity_id, type="INVOKES_ACTIVITY",
            resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
            evidence_file=ctx.relpath, evidence_line=call.start_line,
            props={"by": "ref"},
        ))
        stats["invokes_activity_resolved"] += 1


def _extract_start_workflow_claims(
    ctx: FileContext, node_ids: dict[int, str], claims: list[dict], stats: dict[str, int],
) -> None:
    for call in ctx.facts.calls:
        if call.callee_name not in _START_WORKFLOW_CALLEES:
            continue
        enclosing_id = node_ids.get(call.enclosing_def)
        if enclosing_id is None:
            stats["start_workflow_missing_node_id"] += 1
            continue
        arg0 = _arg0(call)
        dst_id = _resolve_ref(ctx, arg0.name_start_byte) if arg0 is not None else None
        if dst_id is None:
            stats["start_workflow_unresolved"] += 1
            continue
        claims.append({
            "src_id": enclosing_id,
            "dst_id": dst_id,
            "evidence_line": call.start_line,
        })
        stats["start_workflow_resolved"] += 1


def _mini_decorator_call(dec_text: str) -> CallFact | None:
    """Re-parses one decorator's raw text as a standalone snippet to get a real
    CallFact -- exact mirror of fastapi_ext.py's own `_mini_call` (see this module's
    own docstring, M7 T4 section, for why decorators aren't already CallFacts). A
    bare/non-call decorator (e.g. "workflow.signal") mini-parses to zero calls ->
    None, same as a call-form decorator with no usable arguments at all never would
    -- both are handled identically by the caller (fall back to the method name).

    Deliberately its OWN 2-line copy, not a cross-module import of fastapi_ext's
    `_mini_call`: unlike kafka_ext.py's reuse of idiom_match._imports_module (two
    independent implementations of one matching RULE that must stay check-for-check
    in sync -- a real drift risk, see that module's own docstring), there is no
    algorithm here to drift out of sync -- both are a bare call-through to
    build_file_facts plus "first call or None". Keeping temporal_ext.py
    self-contained for a wrapper this trivial outweighs coupling two otherwise-
    unrelated peer domain extractors over it (M7 T4 review weighed the hoist and
    kept the copy; the return annotation below matches fastapi_ext's exactly)."""
    mini = build_file_facts("<decorator>", dec_text.encode("utf-8") + b"\n")
    return mini.calls[0] if mini.calls else None


def _signal_channel_name(dec_text: str, method_name: str) -> str:
    """`name=`-kwarg string literal wins; a bare decorator, a call-form decorator
    with no name= kwarg at all, or name= present but non-string/empty all fall back
    to the METHOD's own name -- brief: "(или имя метода при отсутствии)". See module
    docstring (M7 T4) for why this is deliberately NOT const-resolved against the
    file's ConstTable, unlike the sender-side arg0 below."""
    call = _mini_decorator_call(dec_text)
    if call is not None:
        name_arg = next((a for a in call.args if a.keyword == "name"), None)
        if name_arg is not None and name_arg.value_kind == "string" and name_arg.string_value:
            return name_arg.string_value
    return method_name


def _extract_signal_kind_roles(
    ctx: FileContext, node_ids: dict[int, str], pattern: str, signal_kind: str,
    roles: dict[str, set[str]], node_props: dict[str, dict], channels: list[NodeRec],
    edges: list[EdgeRec], stats: dict[str, int],
) -> None:
    """Shared by BOTH `workflow.signal` and `workflow.update` (see
    _SIGNAL_KIND_DECORATORS) -- role TemporalSignalHandler + node_props signal_kind +
    CONSUMES into Channel(temporal_signal, name). `workflow.query` is intentionally
    NOT handled here -- see `_extract_query_roles` (role only, no channel/edge)."""
    for d, dec_text in match_decorators(pattern, ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["signal_handler_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalSignalHandler")
        node_props.setdefault(node_id, {})["signal_kind"] = signal_kind

        name = _signal_channel_name(dec_text, d.name)
        chan = make_channel_node("temporal_signal", name=name)
        channels.append(chan)
        edges.append(EdgeRec(
            src=node_id, dst=chan.id, type="CONSUMES",
            resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
            evidence_file=ctx.relpath, evidence_line=d.start_line,
            props={"signal_kind": signal_kind},
        ))


def _extract_query_roles(
    ctx: FileContext, node_ids: dict[int, str],
    roles: dict[str, set[str]], node_props: dict[str, dict], stats: dict[str, int],
) -> None:
    """`workflow.query` -- role ONLY (props signal_kind="query"), NO channel/edge:
    a query is a synchronous read of workflow state, not an async boundary any
    sender ever "produces" into (module docstring, M7 T4)."""
    for d, _dec_text in match_decorators(_QUERY_DECORATOR_PATTERN, ctx.facts.defs):
        node_id = node_ids.get(d.index)
        if node_id is None:
            stats["signal_handler_missing_node_id"] += 1
            continue
        roles.setdefault(node_id, set()).add("TemporalSignalHandler")
        node_props.setdefault(node_id, {})["signal_kind"] = "query"


def _resolve_signal_arg0(arg0: ArgFact | None, consts: ConstTable) -> tuple[str | None, bool]:
    """Three-way classification of a `.signal(...)` call's arg0 -- see module
    docstring (M7 T4) for the full rationale. Returns (channel_name,
    counts_as_unresolved_miss):
      - (name, False): a concrete, non-empty channel name -- MATCH.
      - (None, True): arg0 LOOKS like a signal-name reference (a string that
        resolved empty, or a name/attr that didn't resolve to a usable value) --
        an honest miss, caller bumps signal_name_unresolved.
      - (None, False): arg0 doesn't look like a signal-name reference at all (no
        arg0, or a non-string/non-name/non-attr shape -- numeric/bool/dict/fstring/
        ...) -- silent skip, noise guard, no counter.
    """
    if arg0 is None:
        return None, False
    if arg0.value_kind == "string":
        name = arg0.string_value
        return (name, False) if name else (None, True)
    if arg0.value_kind in ("name", "attr"):
        resolved = resolve_arg(arg0, consts)
        if resolved.kind == "value" and resolved.value:
            return resolved.value, False
        return None, True
    return None, False


def _extract_signal_senders(
    ctx: FileContext, node_ids: dict[int, str], consts: ConstTable,
    channels: list[NodeRec], edges: list[EdgeRec], claims: list[dict],
    stats: dict[str, int],
) -> None:
    """`<handle>.signal("<name>", ...)` / `get_external_workflow_handle(...).
    signal(...)` -- ANY receiver except the ONE exact-name exclusion below (mirrors
    `_extract_start_workflow_claims`'s own no-positive-receiver-check precedent),
    callee_name == "signal" exactly -> PRODUCES into the SAME temporal_signal
    channel a handler CONSUMES from. See module docstring (M7 T4) for the residual
    FP-risk this deliberately accepts.

    M8 T2 (rerun-2 R5): arg0 resolution is now a TWO-STAGE attempt, not one.
    `_resolve_signal_arg0` (UNCHANGED) still owns the string-literal and
    module-const-name paths -- when it returns a usable `name`, this function takes
    the SAME direct-PRODUCES-edge branch as before this task, byte-identical. Only
    when it does NOT (arg0 is name/attr-shaped but names no known constant) does
    this function now try a SECOND resolution, `_resolve_ref` on arg0's own span --
    the identical helper/span convention `_extract_invokes_activity` above uses --
    to catch a TYPED method reference (`handle.signal(Cls.method, ...)`, or a
    bare-name imported method). Resolved -> a `temporal_signal_send` claim (NEVER a
    direct edge -- this file can't see the handler side); still unresolved -> the
    PRE-EXISTING `signal_name_unresolved` counter, same bucket as before this task.
    See module docstring's M8 T2 section for the full design."""
    for call in ctx.facts.calls:
        if call.callee_name != "signal":
            continue
        if call.receiver_text == "signal":
            # M7 T4 review follow-up: Python's own stdlib `signal.signal(SIGTERM,
            # handler)` -- the one receiver spelling that is reliably NEVER a
            # Temporal handle (no real handle variable is named `signal`; the
            # attribute-call form `signal.signal(...)` on that name is the stdlib
            # module in practice), and SIGTERM/SIGINT installation is common
            # enough that counting it would add signal_name_unresolved noise on
            # virtually every service. An exact-name EXCLUSION dropped BEFORE
            # arg0 classification (neither edge nor counter), not a positive
            # receiver filter -- every other receiver stays matched blindly; an
            # aliased `import signal as sig` is a documented filter limit (still
            # lands in the honest-miss bucket). See the module docstring's
            # FP-risk section.
            continue
        enclosing_id = node_ids.get(call.enclosing_def)
        if enclosing_id is None:
            stats["signal_sender_missing_node_id"] += 1
            continue
        arg0 = _arg0(call)
        name, counts_as_miss = _resolve_signal_arg0(arg0, consts)
        if name is not None:
            chan = make_channel_node("temporal_signal", name=name)
            channels.append(chan)
            edges.append(EdgeRec(
                src=enclosing_id, dst=chan.id, type="PRODUCES",
                resolution="heuristic", confidence=0.6, extractor=_EXTRACTOR,
                evidence_file=ctx.relpath, evidence_line=call.start_line,
                props={"mechanism": "temporal_signal"},
            ))
            stats["signal_sender_resolved"] += 1
            continue
        # M8 T2 (rerun-2 R5): a second resolution attempt, ONLY for name/attr-shaped
        # arg0 that _resolve_signal_arg0 just failed to resolve to a literal/const
        # NAME above -- see this function's own docstring and the module docstring's
        # M8 T2 section for the full rationale.
        if arg0 is not None and arg0.value_kind in ("name", "attr"):
            method_symbol = _resolve_ref(ctx, arg0.name_start_byte)
            if method_symbol is not None:
                claims.append({
                    "src_id": enclosing_id,
                    "method_symbol": method_symbol,
                    "evidence_line": call.start_line,
                })
                stats["signal_sender_symbol_resolved"] += 1
                continue
        if counts_as_miss:
            stats["signal_name_unresolved"] += 1


def extract_temporal(
    ctx: FileContext, node_ids: dict[int, str], consts: ConstTable,
) -> TemporalResult:
    roles: dict[str, set[str]] = {}
    node_props: dict[str, dict] = {}
    channels: list[NodeRec] = []
    edges: list[EdgeRec] = []
    claims: list[dict] = []
    signal_send_claims: list[dict] = []
    stats = _stats()

    _extract_defn_roles(ctx, node_ids, roles, node_props, stats)
    _extract_invokes_activity(ctx, node_ids, edges, stats)
    _extract_start_workflow_claims(ctx, node_ids, claims, stats)
    for pattern, signal_kind in _SIGNAL_KIND_DECORATORS:
        _extract_signal_kind_roles(
            ctx, node_ids, pattern, signal_kind, roles, node_props, channels, edges, stats,
        )
    _extract_query_roles(ctx, node_ids, roles, node_props, stats)
    _extract_signal_senders(
        ctx, node_ids, consts, channels, edges, signal_send_claims, stats,
    )

    return TemporalResult(
        roles=roles, node_props=node_props, channels=channels, edges=edges,
        claims=claims, signal_send_claims=signal_send_claims, stats=stats,
    )
