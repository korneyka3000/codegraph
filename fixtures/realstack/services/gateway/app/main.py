from fastapi import FastAPI

from app.routes.submit import router as submit_router

app = FastAPI(title="gateway")
app.include_router(submit_router)
