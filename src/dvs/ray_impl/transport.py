"""RayShardTransport: the ShardTransport contract implemented with Ray actor calls.

Normalizes Ray's runtime failures (observed in the T110a probe) into transport errors:
- ActorDiedError / ActorUnavailableError / RayActorError / unknown name → ReplicaUnreachable
- PendingCallsLimitExceeded (max_pending_calls, raised on submit)          → ReplicaOverloaded
- timeout                                                                 → TransportTimeout
A local timeout does NOT stop the remote call in Ray, so on timeout or cancellation of the
awaiting task this transport calls `ray.cancel` explicitly.

`ray.get_actor` is a blocking call (a round trip to the GCS, through the head's proxy with Ray
Client). It runs in a thread with the call's timeout: run inline, it froze the gateway's event
loop for over a second while a killed actor was restarting, and every concurrent search missed
its deadline (E1, T121b). A handle survives actor restarts, so it is only dropped when the actor
is dead for good.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import ray
from ray.exceptions import ActorDiedError, PendingCallsLimitExceeded, RayActorError

from dvs.coordinator.transport import (
    NotPrimary,
    ReplicaOverloaded,
    ReplicaUnreachable,
    TransportTimeout,
    WriteOutcome,
)
from dvs.core.model import (
    GetResult,
    ReplicaStatus,
    ShardMap,
    ShardSearchRequest,
    ShardSearchResult,
    WriteOp,
)
from dvs.ray_impl.catalog_actor import replica_actor_name
from dvs.ray_impl.shard_actor import WriteBatchReply


class RayShardTransport:
    def __init__(self, catalog: Any, cluster: str = "dvs", namespace: str | None = None) -> None:
        self.catalog = catalog
        self.cluster = cluster
        self.namespace = namespace
        self._handles: dict[str, Any] = {}

    async def _handle(self, replica_id: str, timeout_s: float) -> Any:
        h = self._handles.get(replica_id)
        if h is None:
            name = replica_actor_name(self.cluster, replica_id)
            try:
                h = await asyncio.wait_for(
                    asyncio.to_thread(ray.get_actor, name, self.namespace), timeout=timeout_s
                )
            except ValueError as e:  # name not registered (dead detached actor, wrong cluster)
                raise ReplicaUnreachable(replica_id, "no such actor") from e
            except TimeoutError as e:
                raise TransportTimeout(replica_id) from e
            self._handles[replica_id] = h
        return h

    async def _call(self, replica_id: str, method: str, timeout_s: float, *args: Any) -> Any:
        loop = asyncio.get_running_loop()
        started = loop.time()
        handle = await self._handle(replica_id, timeout_s)
        timeout_s = max(0.0, timeout_s - (loop.time() - started))
        try:
            ref = getattr(handle, method).remote(*args)
        except PendingCallsLimitExceeded as e:
            raise ReplicaOverloaded(replica_id, "max_pending_calls reached") from e
        try:
            return await asyncio.wait_for(_await_ref(ref), timeout=timeout_s)
        except TimeoutError as e:
            ray.cancel(ref)
            raise TransportTimeout(replica_id) from e
        except asyncio.CancelledError:
            ray.cancel(ref)  # hedge lost or global deadline expired: stop the remote work
            raise
        except RayActorError as e:  # died, unavailable (restarting), or unreachable
            if isinstance(e, ActorDiedError):  # restarting actors keep the same handle
                self._handles.pop(replica_id, None)
            raise ReplicaUnreachable(replica_id, type(e).__name__) from e

    async def shard_map(self) -> ShardMap:
        result: ShardMap = await self.catalog.shard_map.remote()
        return result

    async def search(
        self, replica_id: str, req: ShardSearchRequest, timeout_s: float
    ) -> ShardSearchResult:
        result: ShardSearchResult = await self._call(replica_id, "search", timeout_s, req)
        return result

    async def get(
        self, replica_id: str, shard: int, collection: str, vid: str, min_seq: int, timeout_s: float
    ) -> GetResult:
        result: GetResult = await self._call(replica_id, "get", timeout_s, collection, vid, min_seq)
        return result

    async def write(
        self, replica_id: str, ops: Sequence[WriteOp], timeout_s: float
    ) -> list[WriteOutcome]:
        reply: WriteBatchReply = await self._call(replica_id, "write", timeout_s, list(ops))
        if reply.status == "not_primary":
            raise NotPrimary(replica_id)
        if reply.status == "not_ready":
            raise ReplicaUnreachable(replica_id, "primary not ready (rebuilt with empty state)")
        return reply.outcomes

    async def status(self, replica_id: str, timeout_s: float) -> ReplicaStatus:
        result: ReplicaStatus = await self._call(replica_id, "status", timeout_s)
        return result


async def _await_ref(ref: Any) -> Any:
    return await ref
