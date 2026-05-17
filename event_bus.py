import asyncio
from collections import defaultdict
from typing import Callable, Any


class EventBus:
    def __init__(self):
        self._listeners: dict[str, list[Callable]] = defaultdict(list)

    def subscribe(self, event: str, handler: Callable):
        self._listeners[event].append(handler)

    async def publish(self, event: str, data: Any = None):
        for handler in self._listeners[event]:
            if asyncio.iscoroutinefunction(handler):
                await handler(data)
            else:
                handler(data)


bus = EventBus()
