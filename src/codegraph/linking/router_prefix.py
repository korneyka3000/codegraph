"""M8 T1 (rerun-2 R4 -- docs/superpowers/reports/2026-07-24-pilot-rerun-2-open-gaps.md):
composes FastAPI route path templates across `include_router` chains that span file
(and, structurally, service) boundaries -- the identity a route's Channel(http_route)
node and HANDLES edge are built from can no longer be computed inside fastapi_ext.py's
own single-file pass (see that module's own docstring for the full "why").

Consumes the three per-file claim kinds fastapi_ext.py now emits instead of a direct
Channel/HANDLES:
  - route_decl: {router_symbol, verb, path, handler_node_id, prefix_local,
    evidence_line} -- one per matched route decorator.
  - router_include: {parent_symbol, child_symbol, prefix} -- one per
    `<parent>.include_router(<child>, prefix=...)` call.
  - router_decl (M8 review Important-1): {router_symbol, prefix_local} -- one per
    `X = APIRouter(...)`/`X = FastAPI(...)` assignment (routes or not) -- each
    router's OWN declared prefix, the piece neither of the other two claim kinds
    carries for an INTERMEDIATE chain hop.

FASTAPI COMPOSITION ORDER (M8 review Important-1 -- empirically verified against real
FastAPI 0.140.0 via its own OpenAPI schema, this task's report has the raw probe):
`APIRouter.add_api_route` registers a route at `self.prefix + path`, and
`include_router(child, prefix=ip)` re-registers each of child's routes at
`self.prefix + ip + child_route.path` -- the child's own declared prefix is already
baked into `child_route.path` by ITS OWN registration/include time. Flattened
root-to-leaf, the served path is therefore, per mount: [mounting include-kwarg
prefix] + [mounted router's own declared prefix], ..., ending with [leaf's own
prefix_local] + [decorator path]. Probe: `app.include_router(B, prefix="/ia")`,
`B = APIRouter(prefix="/pb")`, `B.include_router(A, prefix="/ib")`,
`A = APIRouter(prefix="/pa")`, route "/x" -> served at `/ia/pb/ib/pa/x`; the review's
own versioned-aggregator shape (bare A, `B = APIRouter(prefix="/v2")` with no routes,
`app.include_router(B, prefix="/api")`) -> `/api/v2/x`.

ALGORITHM (`link`): build a `child_symbol -> list[(parent_symbol, prefix)]` graph
from every staged `router_include` claim (M9 T3: a LIST, one entry per DISTINCT
mount -- see `_build_include_graph`; M8 kept at most one, discarding on any second
claim) plus a `router_symbol -> own declared prefix` map from every `router_decl`
claim (workspace-wide, no per-service scoping needed -- every symbol already bakes
`service` into its own id, see resolvers/scip/symbols.symbol_to_node_id, so two
different services' routers can never collide or cross-link by construction). For
each `route_decl` claim, `_resolve_prefixes` walks UP the graph from its own
`router_symbol` (child -> parent, repeatedly) to every root it can reach (nobody's
`include_router` call ever named that symbol as arg0 -- a bare `FastAPI()` app
object, or a router that simply isn't nested inside anything else), composing via
the recurrence `alts(X) = UNION over X's own mounts of { pp + parent_own_prefix +
mount.prefix : pp in alts(parent) }`, `alts(root) = [""]` -- each individual
summand flattens to exactly the verified per-mount order above, and the union
across mounts (plus, transitively, across any ANCESTOR's own multiple mounts) is
the cross-product multi-mount composition M9 T3 adds (see that section below) --
then `templates = sorted({g + prefix_local + path : g in alts(router_symbol)})`
(deduped and sorted; M8-era single-mount chains always produce exactly one
alternative here, so `templates` degenerates to the old scalar `[template]`
byte-for-byte). The leaf's own prefix comes from route_decl.prefix_local and is
NEVER double-counted (each summand only ever adds PARENT hops' own prefixes; the
leaf's own router_decl claim exists but is only consulted when the leaf serves as a
parent for an even deeper router). `prefix + path` itself is computed identically
to fastapi_ext.py's pre-M8 direct-template behavior, see `_template` below, a
deliberate byte-for-byte copy of the function this module's own logic superseded.

HONESTY RULE (mirrors linking/http_routes.py's own binding "NO static/1.0 without
anchor, ever" constraint -- no guessing, ever): THREE distinct per-MOUNT failure
shapes below can each cause ONE mount alternative to contribute nothing (see
`_resolve_prefixes`); a route is only counted in `route_prefix_unresolved` -- and
its composed prefix DISCARDED ENTIRELY, falling back to `prefix_local + path` alone
-- when its OWN router_symbol ends up with ZERO surviving alternatives (every mount
it has, transitively, failed) or is None to begin with. Never a partial pick, and
never a partial-but-plausible template built from an incomplete chain:
  1. `router_symbol` itself is None (unresolvable at extraction time -- no SCIP wired,
     a degraded/heuristic-fallback run -- resolvers/fallback.py never lays a def at an
     assignment target at all, only at class/function defs -- or a genuine SCIP miss).
     Route-level, not graph-level: no mount resolution is even attempted.
  2. A CYCLE along one mount's own root-to-symbol walk (A includes B, B includes A --
     never structurally valid FastAPI, but claims are per-file and blind to the
     workspace-wide graph shape, so this module must guard against it explicitly
     rather than recursing forever). M9 T3: a cyclic mount contributes nothing from
     ITSELF alone -- a symbol with one cyclic mount and one genuinely-resolvable
     OTHER mount still composes the valid one (see `_resolve_prefixes`'s own
     docstring); the cycle only reaches the whole-route discard above if it is the
     ONLY mount (directly or transitively) that symbol has.
  3. (M9 T3 -- RETIRED as a failure shape, see the module's own dedicated "M9 T3"
     section below) Before this task, ANY second `router_include` claim naming the
     identical child_symbol -- not just "two DISTINCT parents", even byte-identical
     includes from two files -- immediately discarded the whole chain (M8 final
     review, finding 3: "multi-mount support would need one composed template PER
     mount; until then every such router falls to discard+counter"). That
     under-approximation is exactly what M9 T3 lifts: distinct mounts now compose
     independently (one template each), byte-identical duplicates dedup to one
     mount, and neither case reaches this honesty rule at all anymore -- see "M9
     T3" below for the replacement mechanism and its own (different) failure mode
     (the combination-count cap, documented in the paragraph right after this list).
  4. (M8 review Important-1) A hop PARENT with NO router_decl claim for its symbol
     (its own declared prefix is simply unknown -- e.g. a factory-built router,
     `router = create_router()`, whose assignment matches no APIRouter/FastAPI
     callee), or with CONFLICTING router_decl claims (two different prefix_local
     values for one symbol -- a same-symbol re-declaration ambiguity). Composing
     while silently ASSUMING an unknown parent's own prefix is empty is precisely
     the incomplete-confident-template bug the review caught -- the versioned
     aggregator (`B = APIRouter(prefix="/v2")`, no routes of its own) is an ordinary
     FastAPI convention, and its /v2 was invisible to every claim form before
     router_decl existed. M9 T3: also now per-mount -- a hop parent with an unknown
     own prefix poisons only the ONE mount that goes through it, not every mount a
     child might have.

A FOURTH, structurally different guard -- not a per-mount honesty-rule failure, a
pure safety valve (M9 T3): a route_decl's own router_symbol resolving to MORE than
`_MAX_TEMPLATES` (16) live alternatives is treated as a runaway/malformed include
graph, not a legitimate scenario -- "a legit app won't 16-mount a router". The
WHOLE composed prefix is discarded (never a truncated, silently-partial first-16
subset -- that would just relocate the exact dishonesty this module exists to
avoid one level up the chain), counted in `route_prefix_unresolved` same as any
other discard, and ADDITIONALLY logged at WARNING (`logger.warning`, this module's
own logger -- unlike the ordinary honesty-rule shapes above, which are
unremarkable/expected in real codebases, a 17+-way fan-out is unusual enough to be
worth a human's attention). See `_resolve_prefixes`'s own docstring for how the cap
is enforced DURING resolution, not just checked once after the fact, so a deeply or
widely fanned include graph can't blow up memory/runtime before any per-route check
even runs.

The TRIVIAL case -- no `router_include` claim anywhere names this router_symbol as a
child at all (a genuine root: same-file `APIRouter(prefix=...)`, zero cross-file
`include_router` involvement) -- is NOT a failure: `alts(router_symbol) = [""]`
(exactly one alternative, the empty string), giving a single `templates ==
[prefix_local + path]` again, but WITHOUT bumping the counter. THIS is the CRITICAL
CONSTRAINT case: every M2/M6/M7 golden fixture route composes through the
""-ancestor-prefix path today (proven empirically by decoding fixtures/.codegraph/
scip's own orders-api index -- `app.include_router(orders_router)` carries no `prefix=`
kwarg and `app = FastAPI(...)` has no prefix concept, so even orders-api's own real
cross-file chain composes an empty ancestor prefix, now THROUGH the app's own
router_decl claim rather than around it) -- golden HANDLES/CALLS_HTTP tuples must not
shift by one byte. M9 T3 preserves this exactly: every M2/M6/M7 fixture route is a
single-mount chain top to bottom, so `_resolve_prefixes` produces a one-element list
at every hop and `templates` degenerates to the identical single-element list the
old scalar code produced -- no gate fixture double-mounts a router (that is a LATER
milestone task's own realstack leg, deliberately out of this task's scope).

Channel/HANDLES creation, ONE pair per live template (M9 T3: `templates` can now
hold more than one -- see that section below -- so a single route_decl claim can
stage several Channel/HANDLES pairs, all sharing the same handler_node_id, method,
and evidence; the M8-era single-template case is just the `len(templates) == 1`
special case), mirrors fastapi_ext.py's OLD direct-emission shape exactly per pair
(`make_channel_node("http_route", ...)` + HANDLES chan->handler, evidence_file/
evidence_line restored from the claim's own _relpath/evidence_line -- M8 review
Important-2, the identical claim-evidence pass-through linking/http_routes.py's
CALLS_HTTP edges already do) -- just relocated here, `extractor="linking"`/
`origin=None` instead of "fastapi" (cleared by `clear_workspace_layer`, rebuilt fresh
every S7 run, same as CALLS_HTTP/NEXT_SEGMENT already are -- Channel-GC continues to
work exactly as documented, just now doing a "GC-then-recreate" pass over EVERY
http_route channel each run instead of only the rare unresolved-fallback one; see
`stores.staging.Staging.gc_orphan_channels`'s own docstring for why that pattern is
harmless, not data loss). `evalx.edges_eval` does not compare extractor/resolution at
all for its golden-tuple gate (verified by reading `found_edges`/`load_golden_edges`
directly, per this task's own Step 1) -- only `(type, src_service, src_qualified,
dst_channel_id)` for HANDLES -- so this relocation alone cannot shift any golden
HANDLES/CALLS_HTTP tuple.

Wired into `linking.workspace.link_workspace`, BEFORE `http_routes.link` (that stage's
own `_route_table` scan reads whatever Channel(http_route) nodes are ALREADY staged --
this module is what stages them now, where fastapi_ext.py used to).

M9 T2 (rerun-3 backlog): COMPOSE-BACK -- the handler's OWN node also gets patched, not
just the Channel. Before this task, a handler's `path_template`/`http_method` node
props (staged LOCAL-only by fastapi_ext.py in S5, see that module's own docstring) were
NEVER updated once the composed, cross-file identity became known here in S7 -- a route
handler's own card/`get_source`/any other direct consumer of the handler NODE (as
opposed to its Channel) kept showing the local-only fragment (e.g. `/steps/{id}`) even
though the REAL served path (`/api/v1/steps/{id}`) had been sitting on the Channel the
whole time. `link()` now calls `staging.update_node_props(handler_node_id,
{"path_template": template})` for every `route_decl` claim whose composed `template`
differs from the LOCAL-only one (`_template(prefix_local, path)` -- the exact value
fastapi_ext.py already staged the node with) -- comparing against a FRESH recomputation
of the local template from the claim's own `prefix_local`/`path` fields, never a read of
the node's CURRENTLY staged props (`link()` stays a pure claims-in transformation, no
extra node read added). This one comparison naturally covers both zero-patch cases at
once, with no separate branch: the TRIVIAL case (`chain_prefix == ""`) and the
UNRESOLVED/honesty-rule-failure case (`chain_prefix is None`) both set
`template = _template(prefix_local, path)` verbatim -- i.e. exactly `local_template` --
so the comparison is trivially false and no write is even attempted (the brief's own
"avoid no-op writes" requirement; every M2/M6/M7 fixture route takes this no-patch path).
M9 T3 generalizes this scalar `template`/`local_template` comparison to `templates`/
`[local_template]` (a list) -- see the dedicated "M9 T3" section below for the full
generalization, including the new `path_templates` prop.

Idempotent by construction (`staging.update_node_props`'s own shallow-merge-then-UPDATE,
mirroring `update_edge_props`'s INSERT-OR-REPLACE-adjacent semantics): a second `link()`
call over unchanged claims recomputes the identical `template`/`local_template` pair and
either re-applies the same value (chain case) or again skips the write (trivial/
unresolved case) -- the end state never drifts. Incremental coherence: S7 (this module)
always runs in FULL on every `codegraph index` invocation, full or `--incremental` alike
(see `linking/workspace.py`'s own docstring) -- but the handler NODE itself belongs to
its origin service's S5/S6 layer, so whenever that service re-analyzes its OWN stale
file, `pipeline/analyze.py`'s `upsert_nodes` (INSERT OR REPLACE, keyed on node id)
re-stages the handler with the LOCAL-only value again, wholesale, wiping any earlier S7
patch's props entirely; the very next `link_workspace` call (which always follows a
stale re-analyze in the SAME `codegraph index` run) re-composes and re-patches right
after, so the node's props are never observably out of sync with the Channel's across
one full pipeline run.

Retrieval headers (`chunking/augment.py`) are UNAFFECTED by this patch: verified by
reading `_render_header`/`_symbol_line`/`_graph_line` -- none of them ever consult a
RouteHandler node's OWN `path_template`/`http_method` props (only `docstring`/
`signature`, for the doc/parent lines). The header's `graph:` line's own `handles`
clause already reads the COMPOSED path today, independently of this patch -- via the
Channel node's `.name` (`"<METHOD> <template>"`, `make_channel_node`), which `link()`
has built from the fully-composed `template` since M8 T1, long before this task. So this
patch changes NO chunk's `context_header`/`input_hash` -- no spurious re-embed.

M9 T3 (multi-mount router support): lifts the M8 under-approximation documented in
the OLD "HONESTY RULE" shape 3 above (see that slot's own replacement text) -- a
router legitimately mounted more than once (`app.include_router(r, prefix="/v1")` +
`app.include_router(r, prefix="/legacy")`, or two structurally distinct parent
objects both including the same child) now composes ONE template PER mount instead
of discarding the whole chain. Real FastAPI serves every such mount live (a common
API-versioning idiom: the same router reachable at both a current and a legacy
prefix) -- silently picking one, or discarding both, would both be worse than the
honest plural answer.

Mechanism: `_build_include_graph` now maps each child_symbol to a LIST of
`_IncludeEntry` mounts (byte-identical (parent, child, prefix) triples dedup to
one, across files -- see that function's own docstring) instead of at most one
entry (M8: any second claim collapsed straight to `_AMBIGUOUS`). `_resolve_prefixes`
(the direct generalization of M8's scalar `_resolve_prefix`) returns every
surviving composed prefix alternative for a symbol, computed as the union, across
that symbol's own mounts, of each mount's parent's own alternative set combined
with that mount's own contribution -- see that function's own docstring for the
full recurrence, the per-mount-independent failure handling (one bad mount no
longer poisons a sibling good one -- a deliberate strengthening beyond the literal
M8 behavior for cycles/shape-4 hops too, not just the new multi-mount case), and
the `_MAX_TEMPLATES`/`_OVERFLOW` cap guard against runaway/malformed graphs.
`link()` composes each surviving alternative with the route's own `prefix_local +
path`, dedups and sorts the result into `templates`, and builds one Channel + one
HANDLES edge PER entry in `templates` -- all sharing the SAME `handler_node_id`
(same underlying Python function), differing only by the composed template baked
into each Channel's own id (`ids.chan_http` -- naturally distinct ids, no collision
handling needed). `route_prefix_unresolved` (the counter `link` returns) is
UNCHANGED in spirit but different in practice: a route resolving to 2+ templates is
fully resolved (just plural) and does NOT count here anymore -- only a route whose
OWN router_symbol ends up with zero surviving alternatives (or is None, or
overflows the cap) still counts, exactly the honesty-rule discipline this module
has always applied, now evaluated over a set instead of a scalar.

TRACKED ASSUMPTION (M9 T3 review item 3 -- honest caveat, not a fix): the
cross-product treats every mount of a router as serving that router's FULL route
set -- i.e. it assumes all mounts of a common ancestor see the IDENTICAL
descendant-route snapshot. Real FastAPI's `include_router` is an EAGER SNAPSHOT:
it copies the child's routes at the moment of the call, so a route registered (or
a deeper include performed) AFTER an earlier mount is NOT served under that
earlier mount -- and claims, being per-file and execution-order-blind, cannot see
registration/include interleaving at all. Under such interleaving this module can
compose a template real FastAPI never serves -- the FIRST place in this module
where a FALSE POSITIVE (a confidently-wrong path, not just a miss) is structurally
possible. Single-mount composition shares the same order-blindness in principle
(a decorator AFTER the include), but multi-mount is where two LIVE mounts can
legitimately hold two DIFFERENT snapshots of one router, so the divergence becomes
observable as an extra, unserved template rather than degenerate code. Accepted
rather than guarded: idiomatic FastAPI builds a router completely before mounting
it (module-level decorators run at import time; includes run during app assembly,
strictly after), so interleaving is vanishingly rare, and modeling it would
require execution-ORDER claims -- a different claim mechanism entirely, not an
incremental fix here. Tracked in the same honesty spirit as the former shape-3
under-approximation note this task replaced.

Compose-back (M9 T2) generalizes from a single `path_template` overwrite to: the
FIRST template by lexicographic sort becomes `path_template` (a card/get_source/
any other single-value consumer needs SOME canonical value, and lexicographic order
is a cheap, fully deterministic, content-derived tiebreak -- no claim-order or
arbitrary-first-seen dependency), plus a `path_templates` key holding the FULL
sorted list -- but the second key is added ONLY when there is more than one
template (`len(templates) > 1`); a single-mount route's props stay byte-identical
to the pre-T3 (T2-only) shape, no `path_templates` key at all, ever. T2/T3-era: the
SAME `templates != [local_template]` comparison T2 already used to decide whether
to write anything at all continued to cover every zero-write case uniformly
(trivial root, unresolved router_symbol, total per-mount failure, AND cap overflow
alike -- all four set `templates = [local_template]` verbatim) -- no new branching
needed, consistent with T2's own "avoid no-op writes" requirement. M10 T4
REPLACES the RIGHT-hand side of that comparison (see that section below) -- the
LEFT-hand side (`templates`) and the write shape itself are unchanged by this
paragraph's own T2/T3 design.

Stale-key removal (M9 T3 review item 1): the single-template write passes
`remove=("path_templates",)` to `staging.update_node_props` -- when a double-mount
collapses back to ONE mount in source, the very next link re-patches
`path_template` by shallow merge, but merge alone can never DELETE the now-dead
`path_templates` list, and the handler's OWN file never went stale in that edit,
so S5 never re-stages the node clean either: without the remove, the dead path
survived on the node forever, flatly contradicting this module's own no-drift
claims (the review reproduced it; pinned by the module's remount-removal repro
test). Removing an absent key is a documented silent no-op
(`Staging.update_node_props`'s own semantics: remove applies to the OLD props
first, merge lands after and wins on overlap), so the remove is passed
unconditionally on every single-template patch, including a first-ever one.
RESOLVED (M10 T4) -- HONESTY UPGRADE + FIX (M9 final review Important-2,
.superpowers/sdd/m9-final-fix-report.md): the note historically here (through the
M9 final review) undersold the gap by saying composed values merely "persist" --
the review restated it EXPLICITLY as a violation of this project's own M4
"supreme" dump-equivalence invariant (an `--incremental` run must reach the
byte-identical staging/graph state a fresh FULL reindex of the identical final
tree would) for one specific edit shape: full-chain DISSOLUTION under
`--incremental` -- every mount of a router removed (both `include_router` calls
deleted from main.py, say) so the router reverts to a trivial root, while the
HANDLER's own file is untouched (never goes stale, so S5 never re-stages it
LOCAL-only) -- left the handler node's `path_template`/`path_templates` props
stuck at their last-composed, now-INCORRECT value, because `templates ==
[local_template]` put this route back on the NO-write path above even though the
composed value that used to be correct no longer applied. Reproduced directly by
the M9 final review's probe1 (dissolve every mount via `staging.delete_file_layer`
on the router-owning file alone, re-stage its surviving `router_decl`, re-link --
the handler node's props then diverged from a FRESH second `Staging` built from
the identical final claim set, which never composed a prefix in the first place
and so never had one to leave behind) -- now pinned directly in
`tests/unit/test_router_prefix.py`'s own
`test_probe1_full_chain_dissolution_converges_to_fresh_reindex`.

FIX: `link()` now reads the handler node's CURRENTLY STAGED `path_template`/
`path_templates` (`staging.get_node_props`, one call per route_decl claim) and
compares `templates` against THAT instead of against a fresh recomputation of
`local_template` -- direction (b) of the two the M9 final review's own carry
named (a node-props read, not an unconditional write; see that review's report
for why (a) was rejected: it pays a real write for every trivial-root/unresolved
route in a workspace, empirically most real routes). This closes the gap for
every fallback shape uniformly (trivial root, unresolved router_symbol, per-mount
failure, cap overflow alike) with no new branching -- the write condition is
simply `templates != staged_templates` now, where `staged_templates` is
reconstructed from the node's OWN `path_templates` (if present) or singleton
`[path_template]` (if not), `None` if the node carries neither (or doesn't exist
-- `update_node_props` itself already no-ops safely on a missing id). `link()` is
consequently no longer a pure claims-in transformation (T2's own original design
constraint) -- an ACKNOWLEDGED, INTENDED trade this fix makes, not an oversight.

The two pins T2's "avoid no-op writes" design left behind --
`test_trivial_chain_does_not_patch_handler_node_props` and
`test_unresolvable_router_symbol_does_not_patch_handler_node_props`
(tests/unit/test_router_prefix.py) -- are INVERTED per the review's own sanction
(names/history kept deliberately, see each test's own updated docstring): they now
stage a DELIBERATELY STALE value and assert the write DOES happen, correcting it.
A THIRD test, `test_no_patch_attempted_when_staged_value_already_matches_composed`,
takes over the original "avoid no-op writes" intent, correctly re-targeted at an
ACTUAL no-op (staged value already equals composed) rather than a structural shape
that merely usually implied one. Idempotency is unaffected: a second `link()` call
over an unchanged claim set recomputes the identical `templates` a second time,
which now already matches what the first call just staged, so the write is
skipped exactly as before (`test_double_link_is_idempotent_for_handler_node_props`,
`test_probe1_dissolution_then_second_link_is_idempotent`).

Cross-product explosion guard (`_MAX_TEMPLATES = 16`, `_OVERFLOW` sentinel): a
route_decl's own live-template count is bounded above by its router_symbol's own
alternative count, which can in principle grow exponentially with chain depth (a
router double-mounted N hops up a chain of routers EACH double-mounted again
multiplies out to 2**N alternatives for every route underneath). A real
application's include graph is essentially always a tree or a very shallow DAG --
"a legit app won't 16-mount a router" -- so a count this large signals a malformed
or adversarial include graph, not a legitimate scenario, and the guard exists
purely so such a graph fails FAST and SAFE (discard + counter + a WARNING log line
naming the offending router_symbol/route -- see the HONESTY RULE section above)
rather than exhausting memory/CPU inside `_resolve_prefixes`'s own recursion. The
cap is enforced DURING resolution, not just checked once at the top-level route --
`_resolve_prefixes` stops growing (and memoizes as `_OVERFLOW`) any symbol's own
alternative list the moment it would exceed the cap, and `_OVERFLOW` is CONTAGIOUS
(a symbol that fans out through an `_OVERFLOW` parent is itself `_OVERFLOW`, never
a truncated-but-seemingly-complete subset) -- both because per-symbol memoization
means an unbounded symbol would otherwise stay unboundedly expensive for EVERY
route that references it or any of its descendants (not just the one route that
happens to trip the top-level check), and because truncating-and-pretending-
complete would silently under-count a downstream route's true alternative set,
exactly the kind of confident-but-incomplete answer this module's honesty rule
exists to prevent.

Idempotent by construction, same as T2: a second `link()` call over an unchanged
multi-mount include graph recomputes the identical `templates` list (deterministic
sort) and either re-applies the same `path_template`/`path_templates` values or
again skips the write -- no drift. `staging.upsert_edges`' own INSERT-OR-REPLACE PK
semantics (src, dst, type, via_channel, origin_service) mean re-emitting the same N
HANDLES edges a second time converges to the identical N rows, never N*2
duplicates. Incremental coherence is unaffected by the fan-out: S7 still always
runs in FULL (see `linking/workspace.py`'s own docstring), so the same full-graph
recomputation this module has always done on every run now simply considers every
mount alternative instead of at most one."""

from __future__ import annotations

import logging

from codegraph.core.schema import EdgeRec, NodeRec, make_channel_node
from codegraph.stores.staging import Staging

logger = logging.getLogger(__name__)

_EXTRACTOR = "linking"
_RESOLUTION = "static"
_CONFIDENCE = 1.0

# Sentinel: 2+ router_decl claims carry DIFFERENT prefix_local values for one
# symbol (own-prefix ambiguity, honesty-rule failure shape 4) -- never silently
# resolved either way. M9 T3: no longer used by the include graph itself (see
# _build_include_graph's own docstring) -- multiple router_include claims for one
# child_symbol are now legitimate multi-mount alternatives, not an ambiguity;
# _AMBIGUOUS survives ONLY for this one remaining true-ambiguity case
# (_build_own_prefix_map).
_AMBIGUOUS = object()

# M9 T3: sane cap on the number of composed template ALTERNATIVES a single symbol
# (and therefore a single route_decl, since a route's own alternative count is
# exactly its router_symbol's) may carry -- a runaway/malformed include-graph
# guard, not a legitimate-use limit (see module docstring's own "M9 T3" section:
# "a legit app won't 16-mount a router"). Exceeding it discards the WHOLE route's
# composed prefix (never a truncated, silently-partial subset) and logs a
# warning -- see _resolve_prefixes and link's own cap handling.
_MAX_TEMPLATES = 16

# M9 T3 sentinel: symbol has MORE than _MAX_TEMPLATES alternatives -- the exact
# count beyond the cap is never tracked (would defeat the point of the guard);
# distinct from an empty list (genuinely zero alternatives -- every mount
# failed) -- see _resolve_prefixes's own docstring.
_OVERFLOW = object()


def _template(prefix: str, path: str) -> str:
    """Byte-for-byte copy of fastapi_ext.py's OLD (pre-M8) `_template` -- prefix +
    path; empty path -> prefix alone; both empty -> "/" (root). Duplicated rather
    than imported: this two-line pure function is not real logic to keep in sync (no
    fastapi_ext.py import here keeps this module's own dependency surface limited to
    core.schema/stores.staging, matching linking/http_routes.py's own precedent of
    never importing a domain extractor module)."""
    if not path:
        return prefix if prefix else "/"
    return prefix + path


class _IncludeEntry:
    """One resolved `router_include` claim -- ONE mount alternative for its own
    child_symbol (M9 T3: a child can now carry several of these, see
    `_build_include_graph`; before this task at most one survived per child, any
    second claim collapsing straight to `_AMBIGUOUS`)."""

    __slots__ = ("parent", "prefix")

    def __init__(self, parent: str | None, prefix: str) -> None:
        self.parent = parent
        self.prefix = prefix


def _build_include_graph(staging: Staging) -> dict[str, list[_IncludeEntry]]:
    """child_symbol -> list of _IncludeEntry(parent_symbol, prefix), one per
    DISTINCT (parent_symbol, prefix) mount claimed for that child anywhere in the
    workspace -- M9 T3: a child legitimately mounted more than once (the same
    parent under different include-kwarg prefixes, e.g. a `/v1` + `/legacy`
    double-mount, OR two structurally distinct parents both including it) now
    keeps EVERY distinct mount as its own alternative, composed independently by
    `_resolve_prefixes` (see that function and the module docstring's own "M9 T3"
    section) -- replacing the M8 under-approximation that collapsed any second
    claim for one child straight to `_AMBIGUOUS`/discard.

    Byte-identical duplicate claims -- the SAME (parent_symbol, child_symbol,
    prefix) triple, potentially staged from two DIFFERENT files (e.g. a
    re-exported include, or two copies of the same mount call) -- dedup to ONE
    mount, never counted twice: tracked via the local `seen` set, keyed on the
    full triple. This is a STRONGER dedup than the claims table's own PK
    (service, relpath, kind, payload_json), which only collapses duplicates
    WITHIN one file; two distinct files independently emitting the identical
    mount claim persist as two distinct claims rows, and would double the
    composed-template count without this extra graph-level pass.

    Claims with child_symbol=None carry no identity to graph anything under at all
    -- dropped outright (not an error at THIS stage: an unrelated route_decl
    elsewhere is never affected by a claim that names no child, see the module's
    own unusable-claim test). A parent_symbol=None entry IS kept, graphed under
    its own child_symbol -- `_resolve_prefixes` below treats a None parent as a
    resolution failure for THAT mount alone (not as "no entry at all", which would
    wrongly read as "this is a root", and not as poisoning any OTHER mount the
    same child might have)."""
    graph: dict[str, list[_IncludeEntry]] = {}
    seen: set[tuple[str | None, str, str]] = set()
    for claim in staging.claims_for("router_include"):
        child = claim.get("child_symbol")
        if child is None:
            continue
        parent = claim.get("parent_symbol")
        prefix = claim.get("prefix") or ""
        key = (parent, child, prefix)
        if key in seen:
            continue
        seen.add(key)
        graph.setdefault(child, []).append(_IncludeEntry(parent, prefix))
    return graph


def _build_own_prefix_map(staging: Staging) -> dict[str, str | object]:
    """M8 review Important-1: router_symbol -> its OWN declared prefix (router_decl
    claims) | _AMBIGUOUS (conflicting prefix_local values for one symbol). A symbol
    entirely ABSENT from this map means "own prefix unknown" -- `_resolve_prefixes`
    treats that as a hop failure whenever the symbol serves as a PARENT (honesty-rule
    failure shape 4), never as an implicit ''. Duplicate claims with the IDENTICAL
    prefix are naturally idempotent (the claims-table PK already collapses
    byte-identical payloads per (service, relpath); cross-file re-declarations of the
    same symbol with the same prefix are also fine -- same value, no conflict).

    Unaffected by M9 T3: a router's own declared prefix is a property of the SYMBOL,
    not of any one mount -- it has exactly one true value (or is genuinely
    ambiguous), never "one per mount"."""
    own: dict[str, str | object] = {}
    for claim in staging.claims_for("router_decl"):
        sym = claim.get("router_symbol")
        if sym is None:
            continue
        prefix = claim.get("prefix_local") or ""
        if sym not in own:
            own[sym] = prefix
        elif own[sym] != prefix:
            own[sym] = _AMBIGUOUS
    return own


def _resolve_prefixes(
    symbol: str,
    graph: dict[str, list[_IncludeEntry]],
    own_prefix: dict[str, str | object],
    memo: dict[str, list[str] | object],
    in_progress: set[str],
) -> list[str] | object:
    """M9 T3: EVERY distinct accumulated prefix `symbol` can be reached under,
    root-first, one entry per surviving root-to-symbol walk -- the generalization
    of the old (M8) scalar `_resolve_prefix` to a LIST, driven by
    `_build_include_graph` now carrying a LIST of mount alternatives per child
    instead of at most one. Returns:
      - a (possibly single-element) list of composed prefix strings -- one per
        alternative mount chain that fully resolved; `[""]` if `symbol` is itself
        a root (nobody includes it);
      - `[]` (empty list) if EVERY mount alternative failed to resolve (the
        generalization of the old scalar `None` -- see below);
      - `_OVERFLOW` if the alternative count would exceed `_MAX_TEMPLATES`
        (module-level cap; see the module docstring's own "M9 T3" section for the
        OOM-guard rationale) -- contagious: any symbol that fans out through an
        `_OVERFLOW` parent is ALSO `_OVERFLOW`, never a silently truncated-but-
        seemingly-complete list (that would just relocate the dishonesty one hop
        up the chain).

    Per-mount independence (the actual mechanism lifting the M8 under-
    approximation): `symbol`'s own alternative set is the UNION, across every one
    of its OWN mount entries, of (that mount's parent's own alternative set, each
    combined with that mount's own include/own-prefix contribution) --
    `alts(X) = Sum_mount (parent_own(mount) known) { pp + parent_own(mount) +
    mount.prefix : pp in alts(mount.parent) }`, `alts(root) = [""]`. A mount whose
    parent is unresolvable (None), or whose parent's own declared prefix is
    unknown/conflicting (missing/`_AMBIGUOUS` in `own_prefix` -- honesty-rule
    shape 4), contributes NOTHING from THAT mount alone -- it does not poison
    sibling mounts of the SAME child (see
    `test_one_hop_failure_mount_does_not_poison_sibling_valid_mount`). A chain
    with a double-mounted ANCESTOR multiplies through: if some symbol N hops up
    has 2 alternatives, every mount that (transitively) depends on N fans out
    over both, so a route several hops below a doubly-fanned ancestor can end up
    with a genuine cross-product (`test_triple_nested_cross_product_of_two_
    double_mounts`: 2 mounts at one hop x 2 mounts at another = 4).

    Cycle handling generalizes the same way: `in_progress` still guards against
    infinite recursion (a symbol currently being resolved on THIS root-to-symbol
    walk), but re-entering an in-progress symbol now returns `[]` for just that
    ONE recursive call -- i.e. the cyclic mount contributes zero alternatives --
    rather than poisoning the symbol's ENTIRE resolution the way the old scalar
    `None` return did; a symbol with one cyclic mount and one genuinely-valid
    mount still resolves to the valid mount's alternative(s), not a blanket
    failure (a deliberate strengthening beyond the literal M8 behavior, keeping
    the SAME "never guess, never discard real information just because SOME
    other path failed" honesty spirit the module has always claimed).

    Memoized across the whole `link()` call, keyed by symbol (not by
    symbol+in_progress-path) -- a cycle or cap-overflow discovered from one
    route's own walk is remembered identically for every OTHER route that shares
    any node of that same walk, the same caching contract as the old scalar
    version."""
    if symbol in memo:
        return memo[symbol]
    if symbol in in_progress:
        return []  # cycle: THIS walk contributes no alternative (memo untouched)
    in_progress.add(symbol)

    mounts = graph.get(symbol)
    if not mounts:
        result: list[str] | object = [""]  # root -- nobody includes this router
    else:
        alts: list[str] = []
        overflowed = False
        for entry in mounts:
            parent = entry.parent
            parent_own = own_prefix.get(parent) if parent is not None else None
            if parent is None or parent_own is None or parent_own is _AMBIGUOUS:
                continue  # this mount alone contributes nothing (hop failure)
            assert isinstance(parent_own, str)
            parent_alts = _resolve_prefixes(parent, graph, own_prefix, memo, in_progress)
            if parent_alts is _OVERFLOW:
                overflowed = True
                break
            assert isinstance(parent_alts, list)
            for pp in parent_alts:
                alts.append(pp + parent_own + entry.prefix)
                if len(alts) > _MAX_TEMPLATES:
                    overflowed = True
                    break
            if overflowed:
                break
        result = _OVERFLOW if overflowed else alts

    in_progress.discard(symbol)
    memo[symbol] = result
    return result


def link(staging: Staging) -> dict:
    """S7 entry point (called from linking.workspace.link_workspace, BEFORE
    http_routes.link). Mirrors http_routes.link's own signature shape minus the
    (unneeded here) WorkspaceConfig parameter -- claims -> graph -> Channel/HANDLES
    composition, PLUS (M9 T2/T3, M10 T4) a compose-back patch onto each handler
    node's own path_template/path_templates props when the composed template(s)
    differ from the node's CURRENTLY STAGED value (see module docstring's own
    "M9 T2"/"M9 T3"/"M10 T4" sections for the full design/idempotency/incremental
    -coherence argument). M10 T4: no longer staging-only in the "never reads node
    state" sense T2 originally shipped with -- one `staging.get_node_props` read
    per route_decl claim is the acknowledged, intentional trade the read-compare
    fix makes (still no FalkorDB access; staging.db itself is the only thing read).
    Returns {"route_prefix_unresolved": <count>} -- the number of route_decl claims
    whose composition fell back to the local-only template (see module docstring's
    honesty rule for the failure shapes this counts; M9 T3: a route composing to
    2+ live templates via multi-mount is NOT counted here -- it resolved, just
    plurally)."""
    graph = _build_include_graph(staging)
    own_prefix = _build_own_prefix_map(staging)
    memo: dict[str, list[str] | object] = {}

    channels: dict[str, NodeRec] = {}
    edges: list[EdgeRec] = []
    unresolved = 0

    for claim in staging.claims_for("route_decl"):
        prefix_local = claim["prefix_local"]
        path = claim["path"]
        router_symbol = claim.get("router_symbol")
        handler_node_id = claim["handler_node_id"]
        method = claim["verb"]

        local_template = _template(prefix_local, path)

        if router_symbol is None:
            templates = [local_template]
            unresolved += 1
        else:
            prefixes = _resolve_prefixes(router_symbol, graph, own_prefix, memo, set())
            if prefixes is _OVERFLOW:
                templates = [local_template]
                unresolved += 1
                logger.warning(
                    "router_prefix: router_symbol=%s (route %s %s) composes to more "
                    "than %d template alternatives -- discarding the composed prefix "
                    "entirely and falling back to the local template %r; this is "
                    "almost always a malformed/runaway include graph, not a "
                    "legitimate multi-mount",
                    router_symbol, method, path, _MAX_TEMPLATES, local_template,
                )
            elif not prefixes:
                templates = [local_template]
                unresolved += 1
            else:
                assert isinstance(prefixes, list)
                templates = sorted({_template(p + prefix_local, path) for p in prefixes})

        # M9 T2/T3 + M10 T4 (read-compare -- closes the M9 final review's own
        # Important-2 "RESIDUAL, tracked" gap; see module docstring's "M10 T4"
        # section for the full before/after argument): patch the HANDLER node's
        # own path_template (+ path_templates, when there is more than one live
        # template) props to match `templates`, but only when it would actually
        # change something -- compared against the node's CURRENTLY STAGED value
        # (one `staging.get_node_props` read per claim), NOT a fresh
        # recomputation of the local-only template the way this comparison used
        # to work pre-M10. The old `templates != [local_template]` comparison
        # degenerated to comparing `local_template` against itself in EVERY
        # fallback branch (trivial root, unresolved router_symbol, total
        # per-mount failure, cap overflow alike) -- structurally blind to a node
        # whose staged value had drifted from what THIS run's claims currently
        # compose (exactly the "every mount dissolved, handler file untouched"
        # --incremental shape the M9 final review's probe1 caught). Comparing
        # against the real staged value instead catches every such case
        # uniformly, including writing the local template BACK when a chain
        # fully dissolves, while still skipping a redundant write whenever the
        # staged value already matches (the original "avoid no-op writes" goal,
        # now correctly re-targeted at ACTUAL no-ops instead of a structural
        # shape that only usually implied one). The single-template write still
        # actively REMOVES any stale path_templates key unconditionally (M9 T3
        # review item 1 -- removing an absent key is a documented silent no-op,
        # so this costs nothing on a first-ever patch).
        staged_props = staging.get_node_props(handler_node_id)
        staged_path_templates = (staged_props or {}).get("path_templates")
        if staged_path_templates is None:
            staged_single = (staged_props or {}).get("path_template")
            staged_templates = [staged_single] if staged_single is not None else None
        else:
            staged_templates = staged_path_templates

        if templates != staged_templates:
            if len(templates) > 1:
                staging.update_node_props(handler_node_id, {
                    "path_template": templates[0], "path_templates": templates,
                })
            else:
                staging.update_node_props(
                    handler_node_id, {"path_template": templates[0]},
                    remove=("path_templates",),
                )

        for template in templates:
            chan = make_channel_node(
                "http_route", owner_service=claim["_service"], method=method, template=template,
                http_method=method, path_template=template,
            )
            channels[chan.id] = chan
            edges.append(EdgeRec(
                src=chan.id, dst=handler_node_id, type="HANDLES",
                resolution=_RESOLUTION, confidence=_CONFIDENCE, extractor=_EXTRACTOR,
                # M8 review Important-2: evidence restored from the claim itself --
                # evidence_file from claims_for's injected _relpath, evidence_line from
                # route_decl's own field (the handler def's start_line, the exact value
                # the pre-M8 direct-emission HANDLES carried) -- mirrors
                # http_routes.py's own CALLS_HTTP claim-evidence pass-through. Every
                # HANDLES for the SAME route_decl (M9 T3: one per live template) shares
                # the identical evidence -- they are all the same decorator site.
                evidence_file=claim.get("_relpath"),
                evidence_line=claim.get("evidence_line"),
            ))

    if channels:
        staging.upsert_nodes(list(channels.values()))
    if edges:
        staging.upsert_edges(edges)

    return {"route_prefix_unresolved": unresolved}
