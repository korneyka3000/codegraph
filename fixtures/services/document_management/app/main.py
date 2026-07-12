from fastapi import FastAPI

from app.routes.documents import router as documents_router

app = FastAPI(title="document-management")
app.include_router(documents_router)
