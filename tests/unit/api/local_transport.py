"""Transporte en memoria (RF = 1 por shard, sin red ni Ray) para probar la API de punta a punta
sobre el CoordinatorBackend real y el ShardCore real. Permite "tirar" un shard (`down`)."""

from __future__ import annotations

from collections.abc import Sequence

from dvs.coordinator.transport import ReplicaUnreachable, WriteOutcome
from dvs.core.errors import IdempotencyConflict
from dvs.core.model import (
    CollectionSpec,
    GetResult,
    ReplicaStatus,
    ShardInfo,
    ShardMap,
    ShardSearchRequest,
    ShardSearchResult,
    WriteOp,
)
from dvs.core.replication import PrimaryLog
from dvs.core.shard_core import ShardCore


class LocalTransport:
    def __init__(self, specs: Sequence[CollectionSpec], num_shards: int = 2) -> None:
        self.cores = {s: ShardCore(s, specs) for s in range(num_shards)}
        self.logs = {s: PrimaryLog(f"s{s}/r0", 1) for s in range(num_shards)}
        self.down: set[int] = set()
        self.smap = ShardMap(
            1, tuple(ShardInfo(s, f"s{s}/r0", (f"s{s}/r0",)) for s in range(num_shards))
        )

    def _shard(self, rid: str) -> int:
        s = int(rid[1 : rid.index("/")])
        if s in self.down:
            raise ReplicaUnreachable(rid)
        return s

    async def shard_map(self) -> ShardMap:
        return self.smap

    async def write(
        self, replica_id: str, ops: Sequence[WriteOp], timeout_s: float
    ) -> list[WriteOutcome]:
        s = self._shard(replica_id)
        core, log = self.cores[s], self.logs[s]
        out: list[WriteOutcome] = []
        for op in ops:
            try:
                prev = core.lookup_dedup(op.request_id, op.fingerprint)
            except IdempotencyConflict as e:
                out.append(e)
                continue
            if prev is not None:
                out.append(prev)
                continue
            log.append(op)
            (entry,) = log.advance()
            out.append(core.apply(entry))
        return out

    async def search(
        self, replica_id: str, req: ShardSearchRequest, timeout_s: float
    ) -> ShardSearchResult:
        core = self.cores[self._shard(replica_id)]
        hits = core.search(req.collection, req.vector, req.k, req.filter)  # type: ignore[arg-type]
        return ShardSearchResult("ok", core.shard, replica_id, core.applied_seq, tuple(hits), 0.1)

    async def get(
        self, replica_id: str, shard: int, collection: str, vid: str, min_seq: int, timeout_s: float
    ) -> GetResult:
        core = self.cores[self._shard(replica_id)]
        item = core.get(collection, vid)
        status = "ok" if item is not None else "not_found"
        return GetResult(status, shard, replica_id, core.applied_seq, item)

    async def status(self, replica_id: str, timeout_s: float) -> ReplicaStatus:
        s = self._shard(replica_id)
        c = self.cores[s]
        return ReplicaStatus(
            replica_id, s, "primary", True, "local", 1, "inc", c.applied_seq, c.applied_seq
        )
