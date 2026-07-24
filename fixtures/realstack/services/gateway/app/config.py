"""Pydantic-settings service config (M7 T1 foundation; OPEN R1/R2 -- see
docs/superpowers/reports/2026-07-23-pilot-rerun-open-gaps.md). Real convention:
`class ServiceSettings(BaseSettings)` with `model_config = SettingsConfigDict(
env_prefix=...)` -- ClassAttrIndex harvests every field's string-literal default
AND its derived env name ((prefix + field).upper()), so:

  - `doc_events_topic` -> default "kyc.document.events" is the M7 T2 settings-source
    producer leg's channel identity (workspace.yaml's outbox-doc-events idiom names
    this field via `name_from: {settings: "app.config.GatewaySettings.
    doc_events_topic"}`);
  - `worker_url` -> env SERVICE_WORKER_URL is the M7 T3 auto-anchor leg's join
    target: StatusClient's own `self.host = config.worker_url` assignment joins
    "worker_url" through ClassAttrIndex.field_by_name to THIS field's env name,
    which env_values.yaml (workspace.yaml `env_sources:`) then maps to the worker
    service's cluster hostname.

pydantic_settings is deliberately NOT installed for this fixture (no venv, same as
every other third-party import here) -- harvesting is purely structural (tree-
sitter), it never imports this module."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="service_")

    doc_events_topic: str = "kyc.document.events"
    worker_url: str = "http://localhost:9000"
