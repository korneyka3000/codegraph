"""Shared consumer base (mirrors the real pilot's `kyc_base_consumer` library --
GAPS §5/pilot gap 4): a path-dependency of the worker service, installed into NO
venv here (no lockfile/site-packages for this synthetic fixture) -- scip-python's
first-party-only resolution therefore CANNOT see across this package boundary,
same as the real pilot. See fixtures/realstack/workspace.yaml's own comment on
the `base_class` consumer idiom for what this means for CONSUMES resolution."""

from typing import Generic, TypeVar

T = TypeVar("T")


class BaseConsumer(Generic[T]):
    """Base class for event consumers; subclasses override `process_event` with
    the concrete event type as the generic argument (`BaseConsumer[FooEvent]`)."""

    async def process_event(self, event: T) -> bool:
        raise NotImplementedError
