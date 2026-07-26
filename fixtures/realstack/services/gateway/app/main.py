from fastapi import FastAPI

from app.routes import api

# M8 T3 (realstack leg, rerun-2 R4 proof): `api` (app/routes/__init__.py) is a
# cross-file aggregator carrying its OWN declared prefix ("/v1") -- mounted here a
# SECOND time under the include-kwarg prefix "/api", so every route inside
# app.routes.submit/app.routes.ops composes to "/api/v1/<local path>" via a real,
# multi-hop include_router chain (router_prefix.py's own module docstring names this
# exact "versioned aggregator" shape).
app = FastAPI(title="gateway")
app.include_router(api, prefix="/api")
