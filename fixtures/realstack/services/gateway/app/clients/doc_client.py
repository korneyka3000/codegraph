from app.clients.sdk import BaseClient, Method, Request, path_template


class DocClient(BaseClient):
    """Real convention: camunda-gateway's `app/clients/*.py`, class `*Client(BaseClient)`."""

    @path_template("/documents/{doc_uid}")
    async def fetch_document(self, doc_uid: str, **kwargs) -> dict:
        request = Request(Method.GET, self.host, doc_uid)
        return await self.driver.fetch_content(request)
