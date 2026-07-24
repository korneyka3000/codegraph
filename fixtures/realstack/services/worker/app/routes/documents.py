from fastapi import APIRouter

router = APIRouter()


@router.get("/documents/{doc_uid}")
async def get_document(doc_uid: str) -> dict:
    return {"doc_uid": doc_uid, "status": "available"}


# M7 T3: the route gateway's StatusClient legitimately calls (auto-anchored via
# SERVICE_WORKER_URL -> env_values.yaml -> this service) -- CALLS_HTTP static/1.0.
@router.get("/api/v1/status/{doc_uid}")
async def get_document_status(doc_uid: str) -> dict:
    return {"doc_uid": doc_uid, "status": "processed"}


# M7 T3 funnel NEGATIVE (OPEN R1's own bug shape, pinned): an all-params route with
# a static tail. Under the OLD bidirectional wildcard rule, StatusClient's
# /api/v1/status/{doc_uid} claim would ALSO match here (its {doc_uid} tail
# absorbing the static "misc" segment) -- the exact mechanism that funneled three
# unrelated pilot paths onto /{a}/{b}/{c}/parsed-data. Strict route-side-only
# matching forbids it; the M7 gate asserts ZERO CALLS_HTTP into this channel.
@router.get("/{a}/{b}/{c}/misc")
async def misc_catch_all(a: str, b: str, c: str) -> dict:
    return {"a": a, "b": b, "c": c}
