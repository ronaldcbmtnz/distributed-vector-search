"""FakeTransport: réplicas simuladas con comportamiento guionizado, para probar el coordinador
sin Ray ni red. Imita las reglas de un transporte real: respeta `timeout_s` (TransportTimeout),
normaliza fallos y registra llamadas y cancelaciones."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from dvs.coordinator.transport import (
    NotPrimary,
    ReplicaOverloaded,
    ReplicaUnreachable,
    TransportTimeout,
    WriteOutcome,
)
from dvs.core.model import (
    GetResult,
    Hit,
    ReplicaStatus,
    ShardInfo,
    ShardMap,
    ShardSearchRequest,
    ShardSearchResult,
    StoredVector,
    WriteOp,
    WriteResult,
)

# never: no responde y el transporte vence su timeout (TransportTimeout)
# hang:  no responde nunca, ni siquiera con timeout: solo termina si lo cancelan
Mode = Literal["ok", "never", "hang", "unreachable", "overloaded", "unserved"]


@dataclass
class FakeReplica:
    replica_id: str
    shard: int
    mode: Mode = "ok"
    delay_s: float = 0.0
    applied_seq: int = 0
    hits: list[Hit] = field(default_factory=list)
    items: dict[str, StoredVector] = field(default_factory=dict)


def build_map(num_shards: int = 3, rf: int = 3, version: int = 1) -> ShardMap:
    shards = tuple(
        ShardInfo(s, f"s{s}/r0", tuple(f"s{s}/r{i}" for i in range(rf))) for s in range(num_shards)
    )
    return ShardMap(version, shards)


class FakeTransport:
    def __init__(self, smap: ShardMap) -> None:
        self.smap = smap
        self.map_calls = 0
        self.replicas = {
            rid: FakeReplica(rid, info.shard) for info in smap.shards for rid in info.replicas
        }
        self.calls: list[tuple[str, str]] = []  # (operación, réplica)
        self.cancelled: list[str] = []
        self.write_log: dict[str, list[WriteOp]] = {}
        self.next_seq: dict[int, int] = {}
        self.not_primary_once: set[str] = set()
        self.write_error: dict[str, type[Exception]] = {}

    def r(self, rid: str) -> FakeReplica:
        return self.replicas[rid]

    def calls_to(self, op: str) -> list[str]:
        return [rid for o, rid in self.calls if o == op]

    async def shard_map(self) -> ShardMap:
        self.map_calls += 1
        return self.smap

    async def _behave(self, rid: str, timeout_s: float) -> FakeReplica:
        rep = self.replicas[rid]
        try:
            if rep.mode == "hang":
                await asyncio.Event().wait()
            if rep.mode == "never":
                await asyncio.sleep(timeout_s)
                raise TransportTimeout(rid)
            if rep.delay_s:
                if rep.delay_s >= timeout_s:
                    await asyncio.sleep(timeout_s)
                    raise TransportTimeout(rid)
                await asyncio.sleep(rep.delay_s)
        except asyncio.CancelledError:
            self.cancelled.append(rid)
            raise
        if rep.mode == "unreachable":
            raise ReplicaUnreachable(rid)
        if rep.mode == "overloaded":
            raise ReplicaOverloaded(rid)
        return rep

    async def search(
        self, replica_id: str, req: ShardSearchRequest, timeout_s: float
    ) -> ShardSearchResult:
        self.calls.append(("search", replica_id))
        rep = await self._behave(replica_id, timeout_s)
        if rep.mode == "unserved":
            return ShardSearchResult("unserved", rep.shard, replica_id, rep.applied_seq)
        if rep.applied_seq < req.min_seq:
            return ShardSearchResult("stale", rep.shard, replica_id, rep.applied_seq)
        hits = tuple(sorted(rep.hits, key=lambda h: (h.dist, h.id))[: req.k])
        return ShardSearchResult("ok", rep.shard, replica_id, rep.applied_seq, hits, 1.0)

    async def get(
        self, replica_id: str, shard: int, collection: str, vid: str, min_seq: int, timeout_s: float
    ) -> GetResult:
        self.calls.append(("get", replica_id))
        rep = await self._behave(replica_id, timeout_s)
        if rep.mode == "unserved":
            return GetResult("unserved", shard, replica_id, rep.applied_seq)
        if rep.applied_seq < min_seq:
            return GetResult("stale", shard, replica_id, rep.applied_seq)
        item = rep.items.get(vid)
        status = "ok" if item is not None else "not_found"
        return GetResult(status, shard, replica_id, rep.applied_seq, item)

    async def write(
        self, replica_id: str, ops: Sequence[WriteOp], timeout_s: float
    ) -> list[WriteOutcome]:
        self.calls.append(("write", replica_id))
        if replica_id in self.not_primary_once:
            self.not_primary_once.discard(replica_id)
            raise NotPrimary(replica_id)
        if replica_id in self.write_error:
            raise self.write_error[replica_id](replica_id)
        rep = await self._behave(replica_id, timeout_s)
        self.write_log.setdefault(replica_id, []).extend(ops)
        out: list[WriteOutcome] = []
        for op in ops:
            seq = self.next_seq.get(rep.shard, 0) + 1
            self.next_seq[rep.shard] = seq
            out.append(WriteResult("ok", rep.shard, seq, op.id, created=True))
        return out

    async def status(self, replica_id: str, timeout_s: float) -> ReplicaStatus:
        rep = await self._behave(replica_id, timeout_s)
        role = "primary" if replica_id.endswith("/r0") else "backup"
        return ReplicaStatus(
            replica_id, rep.shard, role, True, "node", 1, "inc", rep.applied_seq, 0
        )
