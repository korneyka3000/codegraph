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
no existing golden/trace pin."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/ping")
async def admin_ping() -> dict:
    return {"status": "ok"}
