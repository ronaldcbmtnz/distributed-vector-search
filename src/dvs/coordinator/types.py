"""Request/response types of the coordinator (what the API layer talks to)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from dvs.core.filters import Filter
from dvs.core.model import Consistency, Hit, Vector

MissingReason = Literal["unreachable", "unserved", "stale"]
ShardStatus = Literal["ok", "unreachable", "unserved", "stale"]


@dataclass(frozen=True, slots=True)
class ShardReport:
    """Per-shard diagnostics returned to the client (contract §3.7 `shards`)."""

    shard: int
    replica: str | None
    status: ShardStatus
    latency_ms: float | None = None
    applied_seq: int | None = None


@dataclass(frozen=True, slots=True, eq=False)
class SearchRequest:
    query_id: str
    collection: str
    vector: Vector  # already validated (unit-norm for cosine)
    k: int
    filter: Filter | None
    consistency: Consistency
    deadline_s: float
    allow_partial: bool = True
    tokens: Mapping[int, int] = field(default_factory=dict)  # shard → seq (read_your_writes)


@dataclass(frozen=True, slots=True)
class SearchResponse:
    query_id: str
    hits: tuple[Hit, ...]
    partial: bool
    coverage: float
    missing: tuple[tuple[int, MissingReason], ...]
    consistency: Consistency
    shards: Mapping[int, ShardReport]
    hedges: int = 0  # internal counters for metrics, not part of the contract
    redirects: int = 0
