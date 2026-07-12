class Session:
    async def execute(self, query: str, params: dict | None = None) -> None:
        pass


async def get_db():
    session = Session()
    yield session
