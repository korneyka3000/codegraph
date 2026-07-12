from pydantic import BaseModel


class OrderCreate(BaseModel):
    customer_id: str
    amount: float


class Order(BaseModel):
    id: str
    customer_id: str
    amount: float
    status: str
