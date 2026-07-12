import aiohttp


class DocumentManagementClient:
    """Рукописный SDK сервиса document-management."""

    def __init__(self, base_url: str):
        self._base_url = base_url

    async def get_document(self, doc_id: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._base_url}/documents/{doc_id}") as resp:
                return await resp.json()

    async def create_document(self, payload: dict) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._base_url}/documents", json=payload) as resp:
                return await resp.json()
