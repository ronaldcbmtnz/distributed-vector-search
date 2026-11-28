"""Replica choice per consistency level, local suspicion and the cached shard map.

Suspicion is LOCAL to a gateway and only reorders replicas (a timeout is suspicion, not
certainty): it never changes roles, the map or any state.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections.abc import Callable

from dvs.coordinator.transport import ShardTransport
from dvs.core.model import Consistency, ShardInfo, ShardMap

Clock = Callable[[], float]

log = logging.getLogger("dvs.coordinator")


class SuspicionTracker:
    def __init__(self, ttl_s: float, clock: Clock = time.monotonic) -> None:
        self.ttl_s = ttl_s
        self._clock = clock
        self._until: dict[str, float] = {}

    def suspect(self, replica_id: str) -> None:
        self._until[replica_id] = self._clock() + self.ttl_s

    def clear(self, replica_id: str) -> None:
        self._until.pop(replica_id, None)

    def is_suspect(self, replica_id: str) -> bool:
        until = self._until.get(replica_id)
        if until is None:
            return False
        if self._clock() >= until:
            del self._until[replica_id]
            return False
        return True


class ReplicaRouter:
    """Order in which to try the replicas of a shard for a read."""

    def __init__(self, suspicion: SuspicionTracker) -> None:
        self.suspicion = suspicion
        self._rr: dict[int, itertools.count[int]] = {}

    def read_order(self, info: ShardInfo, consistency: Consistency) -> list[str]:
        if consistency == "strong":
            return [info.primary]
        counter = self._rr.setdefault(info.shard, itertools.count())
        n = len(info.replicas)
        start = next(counter) % n if n else 0
        rotated = list(info.replicas[start:]) + list(info.replicas[:start])
        healthy = [r for r in rotated if not self.suspicion.is_suspect(r)]
        suspect = [r for r in rotated if self.suspicion.is_suspect(r)]
        return healthy + suspect


class ShardMapCache:
    """Shard map with a TTL. Refreshed on expiry or explicit invalidation (e.g. NotPrimary).

    If a refresh fails (the map's owner is unreachable) the last known map keeps being served
    and the refresh is retried after another TTL: losing the map's owner must not take down a
    gateway that already knows the shards (T121b: the Catalog died with its node and every
    request, health included, returned 500). Only the very first fetch propagates the error.
    """

    def __init__(self, transport: ShardTransport, ttl_s: float, clock: Clock = time.monotonic):
        self._transport = transport
        self._ttl_s = ttl_s
        self._clock = clock
        self._map: ShardMap | None = None
        self._fetched_at = 0.0
        self._fresh = False
        self._lock = asyncio.Lock()

    def invalidate(self) -> None:
        self._fresh = False

    def _valid(self) -> bool:
        return (
            self._map is not None and self._fresh and self._clock() - self._fetched_at < self._ttl_s
        )

    async def get(self) -> ShardMap:
        if self._valid():
            assert self._map is not None
            return self._map
        async with self._lock:  # one refresh at a time (no stampede)
            if not self._valid():
                try:
                    self._map = await self._transport.shard_map()
                except Exception as e:
                    if self._map is None:
                        raise
                    log.warning(
                        "shard map refresh failed; serving the last known map",
                        extra={
                            "event": "map.refresh_failed",
                            "error": type(e).__name__,
                            "map_version": self._map.version,
                        },
                    )
                self._fetched_at = self._clock()
                self._fresh = True
            assert self._map is not None
            return self._map
