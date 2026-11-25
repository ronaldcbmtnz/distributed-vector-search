"""Exact (brute-force) vector index for one collection inside one shard replica (decision D04).

Vectors live in a growable float32 matrix with slot reuse. Distances are computed in float64 by
accumulating one dimension at a time using only element-wise operations: every row is summed in
the same order regardless of its slot or memory alignment, so two replicas holding the same
content return bit-identical distances (determinism across replicas, REQ-2.2).
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import numpy.typing as npt

from dvs.core.model import Hit, Metadata, Metric, StoredVector, Vector


class VectorStore:
    def __init__(self, dim: int, metric: Metric, initial_capacity: int = 1024) -> None:
        if dim < 1:
            raise ValueError("dim must be >= 1")
        cap = max(1, initial_capacity)
        self.dim = dim
        self.metric: Metric = metric
        self._vecs: npt.NDArray[np.float32] = np.zeros((cap, dim), dtype=np.float32)
        self._ids: list[str | None] = [None] * cap
        self._slot: dict[str, int] = {}
        self._free: list[int] = []
        self._high = 0  # slots [0, _high) have been used at least once
        self._meta: dict[str, Metadata] = {}
        self._version: dict[str, int] = {}

    # ───────────────────────────── mutation ─────────────────────────────

    def __len__(self) -> int:
        return len(self._slot)

    def __contains__(self, vid: object) -> bool:
        return vid in self._slot

    def _alloc(self) -> int:
        if self._free:
            return self._free.pop()
        if self._high == self._vecs.shape[0]:
            grown = np.zeros((self._vecs.shape[0] * 2, self.dim), dtype=np.float32)
            grown[: self._high] = self._vecs[: self._high]
            self._vecs = grown
            self._ids.extend([None] * (grown.shape[0] - len(self._ids)))
        slot = self._high
        self._high += 1
        return slot

    def put(self, vid: str, vector: Vector | None, metadata: Metadata | None, version: int) -> None:
        """Insert or replace. For an existing id, None keeps the current vector/metadata."""
        slot = self._slot.get(vid)
        if slot is None:
            if vector is None or metadata is None:
                raise ValueError("a new id requires both vector and metadata")
            slot = self._alloc()
            self._slot[vid] = slot
            self._ids[slot] = vid
        if vector is not None:
            if vector.shape != (self.dim,):
                raise ValueError("vector shape does not match the collection dimension")
            self._vecs[slot] = vector
        if metadata is not None:
            self._meta[vid] = dict(metadata)
        self._version[vid] = version

    def remove(self, vid: str) -> bool:
        slot = self._slot.pop(vid, None)
        if slot is None:
            return False
        self._ids[slot] = None
        self._free.append(slot)
        del self._meta[vid]
        del self._version[vid]
        return True

    # ───────────────────────────── reads ─────────────────────────────

    def get(self, vid: str) -> StoredVector | None:
        slot = self._slot.get(vid)
        if slot is None:
            return None
        return StoredVector(
            id=vid,
            vector=self._vecs[slot].copy(),
            metadata=dict(self._meta[vid]),
            version=self._version[vid],
        )

    def topk(
        self, query: Vector, k: int, predicate: Callable[[Metadata], bool] | None = None
    ) -> list[Hit]:
        """Exact top-k by (distance, id). `predicate` pre-filters by metadata (REQ-2.3)."""
        if k <= 0 or not self._slot:
            return []
        if predicate is None:
            items = list(self._slot.items())
        else:
            items = [(vid, s) for vid, s in self._slot.items() if predicate(self._meta[vid])]
        if not items:
            return []
        slots = np.fromiter((s for _, s in items), dtype=np.intp, count=len(items))
        dists = self._distances(slots, query)
        n = dists.shape[0]
        if n > k:
            kth = np.partition(dists, k - 1)[k - 1]
            chosen = np.flatnonzero(dists <= kth)  # keeps every tie at the cut-off
        else:
            chosen = np.arange(n)
        ranked = sorted((float(dists[i]), items[i][0]) for i in chosen)[:k]
        return [Hit(vid, d, dict(self._meta[vid]), self._version[vid]) for d, vid in ranked]

    def _distances(self, slots: npt.NDArray[np.intp], query: Vector) -> npt.NDArray[np.float64]:
        cols = np.ascontiguousarray(self._vecs[slots].T, dtype=np.float64)  # (dim, n)
        q = query.astype(np.float64)
        acc = np.zeros(cols.shape[1], dtype=np.float64)
        if self.metric == "l2":
            for j in range(self.dim):
                diff = cols[j] - q[j]
                acc += diff * diff
            return np.sqrt(acc)
        for j in range(self.dim):  # cosine on unit vectors: 1 - <x, q>
            acc += cols[j] * q[j]
        return 1.0 - acc
