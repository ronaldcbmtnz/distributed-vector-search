"""Top-k merge of per-shard results (decision D02, REQ-2.1/2.2/2.4).

The global top-k is always contained in the union of the local top-k lists, so merging them is
exact. Ordering by `(dist, id)` makes the result independent of the order in which shards
answered. Deduplication by id (keeping the highest version) is a safety net: with fixed logical
shards and one replica per shard, the same id should never come from two shards.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable

from dvs.core.model import Hit


def hit_key(h: Hit) -> tuple[float, str]:
    return (h.dist, h.id)


def merge_topk(results: Iterable[Iterable[Hit]], k: int) -> list[Hit]:
    if k <= 0:
        return []
    best: dict[str, Hit] = {}
    for hits in results:
        for h in hits:
            cur = best.get(h.id)
            if cur is None or h.version > cur.version:
                best[h.id] = h
    return heapq.nsmallest(k, best.values(), key=hit_key)
