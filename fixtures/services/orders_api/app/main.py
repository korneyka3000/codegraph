from fastapi import FastAPI

from app.routes.orders import router as orders_router

app = FastAPI(title="orders-api")
app.include_router(orders_router)
