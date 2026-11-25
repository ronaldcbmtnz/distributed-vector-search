"""Write path: group ops by shard, send each group to its primary, map failures (HU-1, HU-5).

Failure mapping (outcome per op, returned in the caller's order):
- NotPrimary            → refresh the shard map and retry ONCE (stale routing)
- ReplicaUnreachable    → Unavailable       (retry with the same request_id is safe)
- ReplicaOverloaded     → Overloaded
- TransportTimeout      → DeadlineExceeded  (outcome UNKNOWN: it may have been applied)
Retrying is safe only because every write carries a request_id deduplicated by the shard.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from dvs.coordinator.routing import ShardMapCache
from dvs.coordinator.transport import (
    NotPrimary,
    ReplicaOverloaded,
    ReplicaUnreachable,
    ShardTransport,
    TransportError,
    TransportTimeout,
    WriteOutcome,
)
from dvs.core.errors import DeadlineExceeded, DVSError, Overloaded, Unavailable
from dvs.core.hashing import shard_for
from dvs.core.model import WriteOp


def _error_for(e: TransportError) -> DVSError:
    if isinstance(e, ReplicaOverloaded):
        return Overloaded("shard primary is overloaded", replica=e.replica_id)
    if isinstance(e, TransportTimeout):
        return DeadlineExceeded(
            "write not confirmed in time; outcome unknown", replica=e.replica_id
        )
    if isinstance(e, ReplicaUnreachable | NotPrimary):
        return Unavailable("shard primary unavailable", replica=e.replica_id)
    return Unavailable(str(e))


class WriteCoordinator:
    def __init__(
        self, transport: ShardTransport, maps: ShardMapCache, write_timeout_s: float
    ) -> None:
        self.transport = transport
        self.maps = maps
        # The primary enforces `write_timeout_s` for its quorum; the transport gets a margin so a
        # live-but-slow primary answers DeadlineExceeded itself instead of a transport timeout.
        self.call_timeout_s = write_timeout_s + max(0.5, 0.25 * write_timeout_s)

    async def write(self, ops: Sequence[WriteOp]) -> list[WriteOutcome]:
        if not ops:
            return []
        smap = await self.maps.get()
        groups: dict[int, list[int]] = {}
        for i, op in enumerate(ops):
            groups.setdefault(shard_for(op.collection, op.id, smap.num_shards), []).append(i)

        async def run(shard: int, idxs: list[int]) -> tuple[list[int], list[WriteOutcome]]:
            return idxs, await self._write_shard(shard, [ops[i] for i in idxs])

        out: list[WriteOutcome | None] = [None] * len(ops)
        for idxs, outcomes in await asyncio.gather(*(run(s, ix) for s, ix in groups.items())):
            for i, o in zip(idxs, outcomes, strict=True):
                out[i] = o
        assert all(o is not None for o in out)
        return out  # type: ignore[return-value]

    async def _write_shard(self, shard: int, ops: list[WriteOp]) -> list[WriteOutcome]:
        for attempt in range(2):
            smap = await self.maps.get()
            primary = smap.shard(shard).primary
            try:
                outcomes = await self.transport.write(primary, ops, self.call_timeout_s)
            except NotPrimary as e:
                self.maps.invalidate()
                if attempt == 0:
                    continue
                return [_error_for(e)] * len(ops)
            except TransportError as e:
                return [_error_for(e)] * len(ops)
            if len(outcomes) != len(ops):
                raise RuntimeError("transport returned a different number of write outcomes")
            return outcomes
        raise AssertionError("unreachable")  # pragma: no cover
