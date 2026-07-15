"""M2 final whole-milestone review, CRITICAL regression: stale S5 layer surviving
re-index.

Empirically proven bug: `Staging.begin_service(service)` used to delete staged edges
via `WHERE src_service=?`, where src_service was derived from an edge's OWN src prefix
(`_id_service`) -- None for any chan:/proc:-prefixed src, regardless of which service's
analyze emitted the edge. HANDLES edges (src=chan:, fastapi_ext's own convention) and
kafka CONTAINS edges (chan:topic -> chan:event) therefore always had src_service=NULL,
so begin_service(service) could never find and delete them on re-index: they silently
survived forever. A renamed route (or renamed kafka topic/event) left its OLD Channel
node and OLD HANDLES/CONTAINS edge staged, poisoning S7's route table with a pattern
that no longer exists in source -- an UNCHANGED client claim (from a service whose own
source never touched the rename) could then silently resolve a FALSE CALLS_HTTP/
NEXT_SEGMENT against that stale route on the SECOND `codegraph index` run, instead of
correctly falling back to unresolved.

The fix: `origin_service` (an explicit "which service's analyze emitted this edge"
fact, supplied by the CALLER of `upsert_edges` -- see its own docstring) replaces
`src_service` as `begin_service`'s deletion key, closing the chan:-src blind spot; a
companion `Staging.gc_orphan_channels()` sweep (run at the end of `link_workspace`)
removes the now-edgeless old Channel node itself, which has no per-service home to be
cleaned up by begin_service at all (Channel.service is always "").

This test reproduces the bug's own repro recipe end to end, entirely in DEGRADED mode
(no real scip-python needed -- the bug and its fix live in staging/linking, not symbol
resolution; forcing `ScipRunError` exercises the exact same heuristic-fallback path
`analyze_service` always takes when scip-python is genuinely unavailable, sanctioned by
this fix task's own verification note: "degraded-режим достаточен -- стейл-слой
воспроизводится и без scip"): analyze+link a COPY of orders_api with its shipped
`POST ""` route (under `APIRouter(prefix="/orders")`, i.e. `POST /orders`), rename it to
`POST "/v2"` in that SAME copy on disk, then begin_service+analyze+link AGAIN into the
SAME staging DB -- mirroring two consecutive `codegraph index` runs over one workspace.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from codegraph.config.models import ServiceConfig, WorkspaceConfig
from codegraph.linking.workspace import link_workspace
from codegraph.pipeline.analyze import analyze_service
from codegraph.resolvers.scip.runner import ScipRunError
from codegraph.stores.staging import Staging

FIXTURE = Path(__file__).parents[2] / "fixtures" / "services" / "orders_api"

OLD_CHANNEL_ID = "chan:http:orders-api:POST /orders"
NEW_CHANNEL_ID = "chan:http:orders-api:POST /orders/v2"

# "caller"'s own source never changes across the two runs -- only orders-api's route
# does -- so it re-emits this SAME claim, unchanged, both times (exactly the scenario
# that lets a surviving stale route silently resolve a false match on the second run).
STALE_CLAIM = {
    "src_id": "sym:caller:`app.client`/stale_client().", "verb": "POST",
    "path_template": "/orders", "base_url_env": None, "resolution_hint": "static",
    "evidence_line": 5,
}
# A second, brand-new caller claim added only in run 2, targeting the NEW path --
# proves the new route resolves correctly, not just that the old one is gone.
FRESH_CLAIM = {
    "src_id": "sym:caller:`app.client_v2`/fresh_client().", "verb": "POST",
    "path_template": "/orders/v2", "base_url_env": None, "resolution_hint": "static",
    "evidence_line": 9,
}


class _AlwaysFailRunner:
    """Forces analyze_service's degraded/heuristic fallback path -- see module
    docstring for why no real scip-python is needed for this regression."""

    def run(self, service_name, service_path, venv, cache_dir, tree_hash):
        raise ScipRunError("forced degraded path for this staging-only regression")


def _cfg(svc_path: Path) -> WorkspaceConfig:
    return WorkspaceConfig(
        graph_name="g", services=[ServiceConfig(name="orders-api", path=svc_path)],
    )


def _next_segment_channels(staging: Staging) -> set[str]:
    return {
        e.props["via_channel_id"] for e in staging.iter_edges() if e.type == "NEXT_SEGMENT"
    }


def _route_channel_props(staging: Staging) -> set[tuple[str, str, str]]:
    return {
        (n.props["owner_service"], n.props["http_method"], n.props["path_template"])
        for n in staging.iter_nodes()
        if n.kind == "Channel" and n.props.get("channel_kind") == "http_route"
        and n.props.get("owner_service") is not None
    }


def test_route_rename_double_run_does_not_leave_stale_handles_channel_or_route(tmp_path):
    target = tmp_path / "orders_api"
    shutil.copytree(FIXTURE, target)
    staging = Staging(tmp_path / "staging.db")
    cache = tmp_path / "cache"
    svc = ServiceConfig(name="orders-api", path=target)
    cfg = _cfg(target)

    # -- run 1: source as shipped (POST "" under APIRouter(prefix="/orders")) --
    analyze_service(svc, staging, cache, runner=_AlwaysFailRunner(),
                     active_idioms=frozenset({"fastapi"}))
    staging.begin_service("caller")
    staging.add_claims("caller", "app/client.py", "http_call", [STALE_CLAIM])
    report1 = link_workspace(cfg, staging)

    assert any(n.id == OLD_CHANNEL_ID for n in staging.iter_nodes())
    assert any(e.type == "HANDLES" and e.src == OLD_CHANNEL_ID for e in staging.iter_edges())
    assert ("orders-api", "POST", "/orders") in _route_channel_props(staging)
    assert report1["calls_http"] == 1 and report1["calls_http_unresolved"] == 0
    assert OLD_CHANNEL_ID in _next_segment_channels(staging)

    # -- rename the route in the COPY only; fixtures/ itself is never touched --
    routes_path = target / "app" / "routes" / "orders.py"
    source = routes_path.read_text()
    assert '@router.post("")' in source
    routes_path.write_text(source.replace('@router.post("")', '@router.post("/v2")'))

    # -- run 2: SAME staging DB, full re-index (begin_service happens INSIDE
    # analyze_service -- see its own module docstring, step 1). "caller" re-emits the
    # IDENTICAL stale claim (its own source never changed) plus a fresh one for the new
    # path, mirroring a real second `codegraph index` run over the whole workspace. --
    analyze_service(svc, staging, cache, runner=_AlwaysFailRunner(),
                     active_idioms=frozenset({"fastapi"}))
    staging.begin_service("caller")
    staging.add_claims("caller", "app/client.py", "http_call", [STALE_CLAIM])
    staging.add_claims("caller", "app/client_v2.py", "http_call", [FRESH_CLAIM])
    report2 = link_workspace(cfg, staging)

    node_ids = {n.id for n in staging.iter_nodes()}
    edges2 = list(staging.iter_edges())

    # the OLD route/channel/edge is GONE -- not merely superseded, not just "unused".
    assert OLD_CHANNEL_ID not in node_ids
    assert not any(e.src == OLD_CHANNEL_ID or e.dst == OLD_CHANNEL_ID for e in edges2)
    assert ("orders-api", "POST", "/orders") not in _route_channel_props(staging)

    # the NEW route/channel/edge is present and correctly wired.
    assert NEW_CHANNEL_ID in node_ids
    assert any(e.type == "HANDLES" and e.src == NEW_CHANNEL_ID for e in edges2)
    assert ("orders-api", "POST", "/orders/v2") in _route_channel_props(staging)

    # THE discriminating assertion: the stale claim (unchanged, still targeting the now
    # -nonexistent "/orders" POST route) must fall back to unresolved, not silently
    # match the (correctly-deleted) stale route -- this is exactly the "false
    # CALLS_HTTP/NEXT_SEGMENT on the SECOND index run" danger the bug caused. Under the
    # pre-fix code the stale route/HANDLES survive begin_service untouched, so this
    # claim would incorrectly resolve (calls_http_unresolved == 0, not 1) -- this
    # assertion fails on the old code and passes on the fix.
    assert report2["calls_http"] == 2
    assert report2["calls_http_unresolved"] == 1

    next_segment_channels = _next_segment_channels(staging)
    assert OLD_CHANNEL_ID not in next_segment_channels
    assert NEW_CHANNEL_ID in next_segment_channels

    # the Channel-GC companion fix actually swept the orphaned old Channel node.
    assert report2["channels_gc"] >= 1

    # regression guard: the real fixtures/ tree must stay untouched by this test.
    assert '@router.post("")' in (FIXTURE / "app" / "routes" / "orders.py").read_text()
