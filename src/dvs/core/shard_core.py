"""State machine of one shard replica (shared by phase 1 and phase 2).

`apply(entry)` is the ONLY way state changes. It is deterministic: applying the same ordered log
on any replica yields the same state and the same results. That is why conditional writes
(insert-if-absent, update-if-present) are decided here, at apply time, and not at the gateway.

The deduplication table (request_id → fingerprint, result) is part of this replicated state, so
it survives restarts and leader changes in phase 2 without extra mechanisms (decision D07).

Not thread-safe by design: one replica = one execution thread (decision D12).
"""

from __future__ import annotations

from collections.abc import Iterable

from dvs.core.errors import IdempotencyConflict, NotFound
from dvs.core.filters import Filter, compile_filter
from dvs.core.model import CollectionSpec, Hit, LogEntry, StoredVector, Vector, WriteResult
from dvs.core.vector_store import VectorStore


class ShardCore:
    def __init__(self, shard: int, collections: Iterable[CollectionSpec]) -> None:
        self.shard = shard
        self.applied_seq = 0
        self._stores: dict[str, VectorStore] = {
            c.name: VectorStore(c.dim, c.metric) for c in collections
        }
        self._dedup: dict[str, tuple[bytes, WriteResult]] = {}

    # ───────────────────────────── writes ─────────────────────────────

    def lookup_dedup(self, request_id: str, fingerprint: bytes) -> WriteResult | None:
        """Previous result of an applied request, or None. Same id + other payload → error."""
        prev = self._dedup.get(request_id)
        if prev is None:
            return None
        if prev[0] != fingerprint:
            raise IdempotencyConflict("request_id reused with a different payload")
        return prev[1]

    def apply(self, entry: LogEntry) -> WriteResult:
        if entry.seq != self.applied_seq + 1:
            raise ValueError(
                f"out-of-order apply: got seq {entry.seq}, expected {self.applied_seq + 1}"
            )
        op = entry.op
        prev = self._dedup.get(op.request_id)
        if prev is not None:
            # A duplicate reached the log (e.g. two concurrent retries). Deterministic no-op.
            result = (
                prev[1]
                if prev[0] == op.fingerprint
                else WriteResult("idempotency_conflict", self.shard, entry.seq, op.id)
            )
            self.applied_seq = entry.seq
            return result

        store = self._store(op.collection)
        exists = op.id in store
        seq = entry.seq
        if op.kind == "upsert":
            store.put(op.id, op.vector, op.metadata if op.metadata is not None else {}, seq)
            result = WriteResult("ok", self.shard, seq, op.id, created=not exists)
        elif op.kind == "insert":
            if exists:
                result = WriteResult("already_exists", self.shard, seq, op.id)
            else:
                store.put(op.id, op.vector, op.metadata if op.metadata is not None else {}, seq)
                result = WriteResult("ok", self.shard, seq, op.id, created=True)
        elif op.kind == "update":
            if not exists:
                result = WriteResult("not_found", self.shard, seq, op.id)
            else:
                store.put(op.id, op.vector, op.metadata, seq)
                result = WriteResult("ok", self.shard, seq, op.id)
        else:  # delete
            existed = store.remove(op.id)
            result = WriteResult("ok", self.shard, seq, op.id, existed=existed)

        self._dedup[op.request_id] = (op.fingerprint, result)
        self.applied_seq = seq
        return result

    # ───────────────────────────── reads ─────────────────────────────

    def search(self, collection: str, query: Vector, k: int, flt: Filter | None) -> list[Hit]:
        return self._store(collection).topk(query, k, compile_filter(flt))

    def get(self, collection: str, vid: str) -> StoredVector | None:
        return self._store(collection).get(vid)

    def count(self, collection: str) -> int:
        return len(self._store(collection))

    @property
    def dedup_size(self) -> int:
        return len(self._dedup)

    def _store(self, collection: str) -> VectorStore:
        store = self._stores.get(collection)
        if store is None:
            raise NotFound("unknown collection", collection=collection)
        return store
