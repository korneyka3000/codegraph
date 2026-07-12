from fastapi import APIRouter, Depends

from app.db.session import Session, get_db
from app.models import Order, OrderCreate
from app.services.order import OrderService

router = APIRouter(prefix="/orders")


@router.post("")
async def create_order(req: OrderCreate, db: Session = Depends(get_db)) -> Order:
    service = OrderService(db)
    return await service.place(req)


@router.get("/{order_id}")
async def get_order(order_id: str, db: Session = Depends(get_db)) -> Order:
    service = OrderService(db)
    return await service.get(order_id)
