"""Data model shared by every layer (core, coordinator, Ray/native transports, API).

Plain frozen dataclasses: picklable across processes and free of any runtime dependency.
Arrays are compared by identity (`eq=False`) because element-wise equality is not meaningful
for dataclass equality.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import numpy.typing as npt

Metric = Literal["l2", "cosine"]
Consistency = Literal["eventual", "read_your_writes", "strong"]
WriteKind = Literal["upsert", "insert", "update", "delete"]
Role = Literal["primary", "backup"]

JSONScalar = str | int | float | bool
MetaValue = JSONScalar | list[JSONScalar]
Metadata = dict[str, MetaValue]
Vector = npt.NDArray[np.float32]


@dataclass(frozen=True, slots=True)
class Limits:
    """Input limits enforced at the edge (validation) and defensively in the shard."""

    k_max: int = 100
    max_batch: int = 1000
    max_id_bytes: int = 128
    max_request_id_bytes: int = 128
    max_metadata_bytes: int = 4096
    max_metadata_keys: int = 32
    max_key_len: int = 64
    max_list_len: int = 32
    max_filter_clauses: int = 16
    max_in_values: int = 256


@dataclass(frozen=True, slots=True)
class CollectionSpec:
    name: str
    dim: int
    metric: Metric


# ───────────────────────────── writes ─────────────────────────────


@dataclass(frozen=True, slots=True, eq=False)
class WriteOp:
    """A validated client write. `vector` is float32 (unit-norm for cosine collections).

    For `update`, a None vector/metadata means "keep the current one". For `delete` both are None.
    `fingerprint` identifies the payload so a reused `request_id` with other content is detected.
    """

    kind: WriteKind
    collection: str
    id: str
    request_id: str
    vector: Vector | None
    metadata: Metadata | None
    fingerprint: bytes


@dataclass(frozen=True, slots=True, eq=False)
class LogEntry:
    """One position of a shard's ordered history. `epoch` is fixed (0) in phase 1."""

    seq: int
    epoch: int
    op: WriteOp


WriteStatus = Literal["ok", "already_exists", "not_found", "idempotency_conflict"]


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Deterministic outcome of applying an entry. `seq` is the write token within `shard`."""

    status: WriteStatus
    shard: int
    seq: int
    id: str
    created: bool | None = None  # upsert / insert
    existed: bool | None = None  # delete


# ───────────────────────────── reads ─────────────────────────────


@dataclass(frozen=True, slots=True)
class Hit:
    id: str
    dist: float
    metadata: Metadata
    version: int  # seq of the write that produced this version


@dataclass(frozen=True, slots=True, eq=False)
class StoredVector:
    id: str
    vector: Vector
    metadata: Metadata
    version: int


ShardReadStatus = Literal["ok", "stale", "unserved"]


@dataclass(frozen=True, slots=True, eq=False)
class ShardSearchRequest:
    """What one shard replica receives. `filter` is a parsed filter (dvs.core.filters)."""

    query_id: str
    shard: int
    collection: str
    vector: Vector
    k: int
    filter: object | None = None
    min_seq: int = 0


@dataclass(frozen=True, slots=True)
class ShardSearchResult:
    status: ShardReadStatus
    shard: int
    replica_id: str
    applied_seq: int
    hits: tuple[Hit, ...] = ()
    latency_ms: float = 0.0


@dataclass(frozen=True, slots=True, eq=False)
class GetResult:
    status: Literal["ok", "not_found", "stale", "unserved"]
    shard: int
    replica_id: str
    applied_seq: int
    item: StoredVector | None = None


# ───────────────────────────── cluster ─────────────────────────────


@dataclass(frozen=True, slots=True)
class ReplicaStatus:
    replica_id: str
    shard: int
    role: Role
    ready: bool
    node_id: str
    pid: int
    incarnation: str
    applied_seq: int
    commit_seq: int
    pending: int = 0
    buffered: int = 0


@dataclass(frozen=True, slots=True)
class ShardInfo:
    shard: int
    primary: str
    replicas: tuple[str, ...]  # replica ids, primary included
    nodes: tuple[str, ...] = ()  # node id per replica (same order), informational


@dataclass(frozen=True, slots=True)
class ShardMap:
    version: int
    shards: tuple[ShardInfo, ...] = field(default_factory=tuple)

    @property
    def num_shards(self) -> int:
        return len(self.shards)

    def shard(self, shard_id: int) -> ShardInfo:
        info = self.shards[shard_id]
        assert info.shard == shard_id, "shard map must be indexed by shard id"
        return info
