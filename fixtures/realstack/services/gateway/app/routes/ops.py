"""Operational routes (M7): three independent entrypoints, deliberately OUTSIDE the
POST /submit trace (golden/traces.yaml) so the M6 trace only grows by the signal
hop, never by these legs.

  - POST /documents/{doc_uid}/decision -> OutboxRepository.add_document_event: the
    settings-source producer leg (M7 T2) -- PRODUCES lands on add_document_event
    (the Event ctor's enclosing def), not on this handler.
  - POST /documents/{doc_uid}/replay -> TopicMirror.replicate x2: the enum fan-out
    leg (M7 T2/R2a). TWO textual call-sites in ONE def (requested replay + the
    unconditional audit mirror) pin kafka_ext's per-(src, dst) emission dedup:
    each fanned-out edge carries callsite_count=2 instead of a PK-colliding
    duplicate row.
  - GET /status/{doc_uid} -> StatusClient.fetch_status: drives the auto-anchor +
    ProxyRequest client (M7 T3/T5) -- the CALLS_HTTP claim itself is method-driven
    (decorator-SDK mode), but a real caller keeps the fixture honest."""

from fastapi import APIRouter

from app.clients.status_client import StatusClient
from app.config import GatewaySettings
from app.services.mirror import TopicMirror
from app.services.outbox_repo import OutboxRepository
from app.topics import DocTopicName

router = APIRouter()


@router.post("/documents/{doc_uid}/decision")
async def submit_decision(doc_uid: str) -> dict:
    repo = OutboxRepository(session=None)
    repo.add_document_event(doc_uid)
    return {"doc_uid": doc_uid, "status": "decided"}


@router.post("/documents/{doc_uid}/replay")
async def replay_document(doc_uid: str, topic_name: str) -> dict:
    mirror = TopicMirror()
    await mirror.replicate(doc_uid, topic_name)
    await mirror.replicate(doc_uid, DocTopicName.AUDIT.value)
    return {"doc_uid": doc_uid, "status": "replayed"}


@router.get("/status/{doc_uid}")
async def document_status(doc_uid: str) -> dict:
    client = StatusClient(GatewaySettings())
    return await client.fetch_status(doc_uid)
