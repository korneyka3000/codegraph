"""M8 T3 (realstack leg, rerun-2 R4 proof): the versioned-aggregator-in-__init__.py
convention `linking/router_prefix.py`'s own module docstring names explicitly as the
motivating real-world shape for the M8 review Important-1 fix. `api` owns ITS OWN
declared prefix ("/v1", a `router_decl` claim) and includes both leaf routers (neither
carries a local prefix, nor an include-kwarg prefix here) BEFORE main.py mounts `api`
a SECOND time under the include-kwarg prefix "/api". Every route declared in
app.routes.submit/app.routes.ops therefore composes to "/api" + "/v1" + <local path> --
exercising BOTH halves of the R4 fix in one real, cross-file chain (the include-kwarg
prefix main.py supplies AND this aggregator's own separately-declared prefix, the
specific gap M8 review Important-1 closed) -- proven against real scip via
fixtures/realstack/golden/edges.yaml's HANDLES section.

Worker's own routing (app/routes/documents.py + app/main.py) is DELIBERATELY left as
the pre-existing trivial single-file, no-prefix, no-chain case -- the regression pin
that composition still reproduces today's byte-identical (empty ancestor prefix)
template for the common case every M2/M6/M7 fixture route already exercises."""

from fastapi import APIRouter

from app.routes.ops import router as ops_router
from app.routes.submit import router as submit_router

api = APIRouter(prefix="/v1")
api.include_router(submit_router)
api.include_router(ops_router)
