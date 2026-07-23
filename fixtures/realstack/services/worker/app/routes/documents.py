from fastapi import APIRouter

router = APIRouter()


@router.get("/documents/{doc_uid}")
async def get_document(doc_uid: str) -> dict:
    return {"doc_uid": doc_uid, "status": "available"}
