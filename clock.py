import asyncio
import time
from typing import Callable


class SimClock:
    """
    Main loop: executes once every TICK_MS milliseconds.
    Each agent registers its update function and frequency.
    Execution order is fixed by registration priority.
    """
    TICK_MS = 50

    def __init__(self):
        self._agents: list[tuple[int, Callable, str]] = []
        self._tick = 0

    @property
    def tick_count(self) -> int:
        return self._tick

    def register(self, every_n_ticks: int, handler: Callable, name: str):
        self._agents.append((every_n_ticks, handler, name))

    async def run(self):
        while True:
            start = time.monotonic()
            self._tick += 1

            for n, handler, name in self._agents:
                if self._tick % n == 0:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler()
                        else:
                            handler()
                    except Exception as e:
                        print(f"[Clock] {name} error: {e}")

            elapsed = (time.monotonic() - start) * 1000
            sleep_ms = max(0, self.TICK_MS - elapsed)
            await asyncio.sleep(sleep_ms / 1000)
