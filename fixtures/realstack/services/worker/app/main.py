from fastapi import FastAPI

from app.routes.documents import router as documents_router

app = FastAPI(title="worker")
app.include_router(documents_router)
