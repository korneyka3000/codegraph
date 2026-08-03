"""M9 T3 realstack leg (docs/superpowers/sdd/progress.md M9-БЕКЛОГ, multi-mount
routers): `admin_router` is legitimately double-mounted from app/main.py --
`app.include_router(admin_router, prefix="/v1")` AND `app.include_router(
admin_router, prefix="/legacy")` -- the brief's own literal double-mount scenario
(same parent object, two distinct include-kwarg prefixes). Real FastAPI serves
BOTH mounts live (a common API-versioning idiom: a current + a legacy prefix for
the identical router) -- proves linking/router_prefix.py's per-mount composition
end-to-end against real scip: TWO Channels (chan:http:worker:GET /v1/ping,
chan:http:worker:GET /legacy/ping) + TWO HANDLES onto the SAME handler
(admin_ping), and the handler's own compose-back props carry
path_template=<first, lexicographic> + path_templates=<both, sorted> (see that
module's own "M9 T3" docstring section). Deliberately isolated from every other
route in the fixture (no client anywhere calls it) -- purely additive, touches
no existing golden/trace pin.

M10 T1 realstack leg (task-5): `admin_ping` ALSO calls the module-level DocStore
singleton (app/services/doc_store.py's `store = DocStore(...)`) -- deliberately
reusing THIS already-isolated route (no client anywhere calls it, see above) so
the new singleton-dispatch CALLS edge disturbs no existing golden/trace pin,
exactly the same reasoning this file's own M9 T3 isolation already relies on."""

from fastapi import APIRouter

from app.services.doc_store import store

router = APIRouter()


@router.get("/ping")
async def admin_ping() -> dict:
    # M10 T1 (task-5): module-level singleton method-call resolution -- see
    # app/services/doc_store.py's own docstring for the mechanism this proves.
    # The literal probe text below is also this leg's own search_code (T3) target.
    store.persist("admin-ping-probe", {"source": "admin_ping"})
    return {"status": "ok"}
