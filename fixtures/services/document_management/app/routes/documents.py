from fastapi import APIRouter

from app.events.producer import emit_document_indexed
from app.services.documents import DocumentService

router = APIRouter(prefix="/documents")


@router.get("/{doc_id}")
async def get_document(doc_id: str) -> dict:
    service = DocumentService()
    return await service.fetch(doc_id)


@router.post("")
async def create_document(payload: dict) -> dict:
    service = DocumentService()
    doc = await service.store(payload)
    await emit_document_indexed(doc["id"])
    return doc
