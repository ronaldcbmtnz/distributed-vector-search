"""T106 — merge top-k determinista: exacto frente a fuerza bruta e independiente del orden."""

import random

from hypothesis import given, settings
from hypothesis import strategies as st

from dvs.core.merge import hit_key, merge_topk
from dvs.core.model import Hit


def _hit(i: int, dist: float, version: int = 1) -> Hit:
    return Hit(id=f"id{i:04d}", dist=dist, metadata={}, version=version)


# distancias con empates frecuentes (pocos valores posibles)
dists = st.lists(st.sampled_from([0.0, 0.5, 1.0, 1.5, 2.0, 3.25]), min_size=0, max_size=60)


@settings(max_examples=200, deadline=None)
@given(dists=dists, num_parts=st.integers(1, 8), k=st.integers(1, 20), seed=st.integers(0, 999))
def test_merge_of_local_topk_equals_global_topk(
    dists: list[float], num_parts: int, k: int, seed: int
) -> None:
    hits = [_hit(i, d) for i, d in enumerate(dists)]
    rng = random.Random(seed)
    parts: list[list[Hit]] = [[] for _ in range(num_parts)]
    for h in hits:
        parts[rng.randrange(num_parts)].append(h)
    local = [sorted(p, key=hit_key)[:k] for p in parts]  # cada shard devuelve solo su top-k
    expected = sorted(hits, key=hit_key)[:k]
    assert merge_topk(local, k) == expected
    rng.shuffle(local)
    assert merge_topk(local, k) == expected  # el orden de llegada no importa


def test_tie_broken_by_id() -> None:
    a, b, c = _hit(2, 1.0), _hit(1, 1.0), _hit(3, 0.5)
    assert [h.id for h in merge_topk([[a], [b, c]], 2)] == ["id0003", "id0001"]


def test_duplicate_id_keeps_highest_version() -> None:
    old, new = _hit(1, 0.1, version=3), _hit(1, 0.9, version=7)
    assert merge_topk([[old], [new]], 5) == [new]
    assert merge_topk([[new], [old]], 5) == [new]


def test_k_zero_and_empty() -> None:
    assert merge_topk([[_hit(1, 1.0)]], 0) == []
    assert merge_topk([], 3) == []
