from app.db.session import Session


class OutboxRepository:
    """Транзакционный outbox: события уходят в Kafka отдельным relay-подом."""

    def __init__(self, db: Session):
        self._db = db

    async def add_event(self, event_type: str, payload: dict) -> None:
        await self._db.execute(
            "INSERT INTO outbox (event_type, payload) VALUES (:t, :p)",
            {"t": event_type, "p": payload},
        )
