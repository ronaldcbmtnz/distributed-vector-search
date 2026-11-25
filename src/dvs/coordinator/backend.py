"""Backend facade used by the API layer. Identical in phase 1 and phase 2.

`CoordinatorBackend` composes routing, the shard-map cache, scatter-gather and the write path on
top of any ShardTransport. The API never sees replicas, actors or RPCs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import Protocol

from dvs.coordinator.routing import Clock, ReplicaRouter, ShardMapCache, SuspicionTracker
from dvs.coordinator.scatter_gather import ScatterGather, read_one_shard
from dvs.coordinator.transport import ShardTransport, TransportError, WriteOutcome
from dvs.coordinator.types import SearchRequest, SearchResponse
from dvs.coordinator.writes import WriteCoordinator
from dvs.core.errors import Unavailable
from dvs.core.hashing import shard_for
from dvs.core.model import Consistency, GetResult, ReplicaStatus, ShardMap, WriteOp


@dataclass(frozen=True, slots=True)
class ReplicaHealth:
    """Status of one replica as seen by the gateway (None status = could not be reached)."""

    replica_id: str
    shard: int
    status: ReplicaStatus | None
    error: str | None = None


class Backend(Protocol):
    async def write(self, ops: Sequence[WriteOp]) -> list[WriteOutcome]: ...

    async def get(
        self, collection: str, vid: str, consistency: Consistency, min_seq: int, deadline_s: float
    ) -> GetResult: ...

    async def search(self, req: SearchRequest) -> SearchResponse: ...

    async def status(self) -> list[ReplicaHealth]: ...

    async def shard_map(self) -> ShardMap: ...


class CoordinatorBackend:
    def __init__(
        self,
        transport: ShardTransport,
        *,
        attempt_s: float,
        write_timeout_s: float,
        suspect_ttl_s: float,
        map_ttl_s: float,
        status_timeout_s: float = 1.0,
        max_hedges: int = 1,
        clock: Clock = time.monotonic,
    ) -> None:
        self.transport = transport
        self.maps = ShardMapCache(transport, map_ttl_s, clock)
        self.router = ReplicaRouter(SuspicionTracker(suspect_ttl_s, clock))
        self.attempt_s = attempt_s
        self.status_timeout_s = status_timeout_s
        self._search = ScatterGather(transport, self.router, attempt_s, max_hedges)
        self._writes = WriteCoordinator(transport, self.maps, write_timeout_s)

    async def shard_map(self) -> ShardMap:
        return await self.maps.get()

    async def write(self, ops: Sequence[WriteOp]) -> list[WriteOutcome]:
        return await self._writes.write(ops)

    async def search(self, req: SearchRequest) -> SearchResponse:
        return await self._search.search(await self.maps.get(), req)

    async def get(
        self, collection: str, vid: str, consistency: Consistency, min_seq: int, deadline_s: float
    ) -> GetResult:
        """Read one id. Returns status ok / not_found; raises Unavailable if no replica answers."""
        smap = await self.maps.get()
        info = smap.shard(shard_for(collection, vid, smap.num_shards))
        need = min_seq if consistency == "read_your_writes" else 0
        loop = asyncio.get_running_loop()

        def call(rid: str, timeout_s: float) -> Awaitable[GetResult]:
            return self.transport.get(rid, info.shard, collection, vid, need, timeout_s)

        read = await read_one_shard(
            info,
            consistency,
            loop.time() + deadline_s,
            call,
            self.router,
            self.attempt_s,
            terminal=frozenset({"ok", "not_found"}),
            context=f"get:{vid}",
        )
        if read.result is None:
            raise Unavailable(
                "no replica could serve the read", shard=info.shard, reason=read.reason
            )
        return read.result

    async def status(self) -> list[ReplicaHealth]:
        smap = await self.maps.get()
        pairs = [(info.shard, rid) for info in smap.shards for rid in info.replicas]

        async def one(shard: int, rid: str) -> ReplicaHealth:
            try:
                return ReplicaHealth(
                    rid, shard, await self.transport.status(rid, self.status_timeout_s)
                )
            except TransportError as e:
                return ReplicaHealth(rid, shard, None, type(e).__name__)

        return list(await asyncio.gather(*(one(s, r) for s, r in pairs)))
