from fastapi import FastAPI

from app.routes.admin import router as admin_router
from app.routes.documents import router as documents_router

app = FastAPI(title="worker")
app.include_router(documents_router)
# M9 T3 realstack leg: legitimate double-mount -- admin_router reachable at BOTH
# /v1/ping and /legacy/ping (real FastAPI serves both prefixes live). See
# app/routes/admin.py's own docstring.
app.include_router(admin_router, prefix="/v1")
app.include_router(admin_router, prefix="/legacy")
