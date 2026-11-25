"""Backpressure at the gateway (HU-8, decision D13).

- Gate: bounded number of in-flight operations; when full, reject IMMEDIATELY with `overloaded`
  instead of queueing (an unbounded queue only postpones the collapse).
- RateLimiter: token bucket per client (REQ-8.2).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from dvs.core.errors import Overloaded


class Gate:
    def __init__(self, name: str, limit: int) -> None:
        self.name = name
        self.limit = limit
        self.in_flight = 0

    @contextmanager
    def enter(self) -> Iterator[None]:
        if self.in_flight >= self.limit:
            raise Overloaded(f"too many concurrent {self.name} operations", limit=self.limit)
        self.in_flight += 1
        try:
            yield
        finally:
            self.in_flight -= 1


class RateLimiter:
    def __init__(
        self, rate_per_s: float, burst: int, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.rate = rate_per_s
        self.burst = burst
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}  # client → (tokens, last refill)

    def allow(self, client: str, cost: float = 1.0) -> bool:
        now = self._clock()
        tokens, last = self._buckets.get(client, (float(self.burst), now))
        tokens = min(float(self.burst), tokens + (now - last) * self.rate)
        if tokens < cost:
            self._buckets[client] = (tokens, now)
            return False
        self._buckets[client] = (tokens - cost, now)
        return True
