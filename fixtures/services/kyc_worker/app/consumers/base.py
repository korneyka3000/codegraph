from collections.abc import Awaitable, Callable

Handler = Callable[[dict], Awaitable[None]]

EVENT_HANDLERS: dict[str, Handler] = {}


def register_handlers(mapping: dict[str, Handler]) -> None:
    EVENT_HANDLERS.update(mapping)
