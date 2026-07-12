import uuid

from app.db.outbox import OutboxRepository
from app.db.session import Session
from app.models import Order, OrderCreate


class OrderService:
    def __init__(self, db: Session):
        self._db = db

    async def place(self, req: OrderCreate) -> Order:
        order = Order(
            id=str(uuid.uuid4()),
            customer_id=req.customer_id,
            amount=req.amount,
            status="pending_kyc",
        )
        await self._persist(order)
        outbox = OutboxRepository(self._db)
        await outbox.add_event(
            "OrderCreated",
            {"order_id": order.id, "customer_id": order.customer_id},
        )
        return order

    async def _persist(self, order: Order) -> None:
        await self._db.execute("INSERT INTO orders ...", order.model_dump())

    async def get(self, order_id: str) -> Order:
        await self._db.execute("SELECT ...", {"id": order_id})
        return Order(id=order_id, customer_id="", amount=0.0, status="unknown")
