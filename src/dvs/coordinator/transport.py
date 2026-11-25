"""What the coordinator needs from "a remote shard replica", independent of the runtime.

Phase 1 implements it with Ray actor calls; phase 2 with gRPC. Implementations must:
- enforce `timeout_s` (raise TransportTimeout when it expires),
- normalize runtime failures to the TransportError subclasses below,
- stop the remote work when the awaiting task is cancelled (best effort).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from dvs.core.errors import DVSError
from dvs.core.model import (
    GetResult,
    ReplicaStatus,
    ShardMap,
    ShardSearchRequest,
    ShardSearchResult,
    WriteOp,
    WriteResult,
)

#: Per-op outcome of a write: a deterministic result, or an error for that op.
WriteOutcome = WriteResult | DVSError


class TransportError(Exception):
    """Failure to talk to a replica. Never means the operation did not happen remotely."""

    def __init__(self, replica_id: str, message: str = "") -> None:
        super().__init__(f"{replica_id}: {message or type(self).__name__}")
        self.replica_id = replica_id


class ReplicaUnreachable(TransportError):
    """Connection failed or the replica process is dead."""


class ReplicaOverloaded(TransportError):
    """The replica (or the handle to it) rejected the call because its queue is full."""


class TransportTimeout(TransportError):
    """No answer within the given timeout: slow or dead, cannot tell (FLP)."""


class NotPrimary(TransportError):
    """A write reached a replica that is not the shard primary (stale routing)."""


class ShardTransport(Protocol):
    async def shard_map(self) -> ShardMap: ...

    async def search(
        self, replica_id: str, req: ShardSearchRequest, timeout_s: float
    ) -> ShardSearchResult: ...

    async def get(
        self,
        replica_id: str,
        shard: int,
        collection: str,
        vid: str,
        min_seq: int,
        timeout_s: float,
    ) -> GetResult: ...

    async def write(
        self, replica_id: str, ops: Sequence[WriteOp], timeout_s: float
    ) -> list[WriteOutcome]: ...

    async def status(self, replica_id: str, timeout_s: float) -> ReplicaStatus: ...
