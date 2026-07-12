class DocumentService:
    async def fetch(self, doc_id: str) -> dict:
        return {"id": doc_id, "status": "verified"}

    async def store(self, payload: dict) -> dict:
        return {"id": "new-doc", **payload}
