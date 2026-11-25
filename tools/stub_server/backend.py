"""Single-process in-memory backend used by the contract stub."""

from __future__ import annotations

import os
import time
from collections.abc import Sequence

from dvs.coordinator.backend import ReplicaHealth
from dvs.coordinator.transport import WriteOutcome
from dvs.coordinator.types import SearchRequest, SearchResponse, ShardReport
from dvs.core.errors import IdempotencyConflict
from dvs.core.model import (
    CollectionSpec,
    Consistency,
    GetResult,
    ReplicaStatus,
    ShardInfo,
    ShardMap,
    WriteOp,
)
from dvs.core.replication import PrimaryLog
from dvs.core.shard_core import ShardCore


class StubBackend:
    """Adapt one in-memory state machine to the API backend protocol.

    The stub has no network transport, replicas, or fault behavior. Calls mutate state
    synchronously within one process; searches use the core's exact brute-force index.
    """

    _REPLICA_ID = "stub/r0"

    def __init__(self, collections: Sequence[CollectionSpec]) -> None:
        self._core = ShardCore(0, collections)
        self._log = PrimaryLog(self._REPLICA_ID, quorum=1)
        self._shard_map = ShardMap(
            version=1,
            shards=(ShardInfo(0, self._REPLICA_ID, (self._REPLICA_ID,), ("stub",)),),
        )

    async def write(self, ops: Sequence[WriteOp]) -> list[WriteOutcome]:
        outcomes: list[WriteOutcome] = []
        for op in ops:
            try:
                previous = self._core.lookup_dedup(op.request_id, op.fingerprint)
            except IdempotencyConflict as exc:
                outcomes.append(exc)
                continue
            if previous is not None:
                outcomes.append(previous)
                continue

            self._log.append(op)
            (committed,) = self._log.advance()
            outcomes.append(self._core.apply(committed))
        return outcomes

    async def get(
        self,
        collection: str,
        vid: str,
        consistency: Consistency,
        min_seq: int,
        deadline_s: float,
    ) -> GetResult:
        """Read the sole local state; this backend has no stale replicas or pending writes."""
        item = self._core.get(collection, vid)
        return GetResult(
            "ok" if item is not None else "not_found",
            0,
            self._REPLICA_ID,
            self._core.applied_seq,
            item,
        )

    async def search(self, req: SearchRequest) -> SearchResponse:
        started = time.perf_counter()
        hits = self._core.search(req.collection, req.vector, req.k, req.filter)
        latency_ms = (time.perf_counter() - started) * 1000
        return SearchResponse(
            query_id=req.query_id,
            hits=tuple(hits),
            partial=False,
            coverage=1.0,
            missing=(),
            consistency=req.consistency,
            shards={
                0: ShardReport(
                    shard=0,
                    replica=self._REPLICA_ID,
                    status="ok",
                    latency_ms=latency_ms,
                    applied_seq=self._core.applied_seq,
                )
            },
        )

    async def status(self) -> list[ReplicaHealth]:
        core = self._core
        status = ReplicaStatus(
            replica_id=self._REPLICA_ID,
            shard=0,
            role="primary",
            ready=True,
            node_id="stub",
            pid=os.getpid(),
            incarnation="stub",
            applied_seq=core.applied_seq,
            commit_seq=self._log.commit_seq,
            pending=self._log.pending_count,
        )
        return [ReplicaHealth(self._REPLICA_ID, 0, status)]

    async def shard_map(self) -> ShardMap:
        return self._shard_map
