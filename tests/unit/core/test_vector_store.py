"""T104a — índice exacto: igual a la fuerza bruta, empates por id y determinismo bit a bit."""

import random

import numpy as np
import pytest

from dvs.core.vector_store import VectorStore

DIM = 16


def _ref_topk(vecs: dict[str, np.ndarray], q: np.ndarray, k: int, metric: str) -> list[str]:
    def dist(v: np.ndarray) -> float:
        v64, q64 = v.astype(np.float64), q.astype(np.float64)
        return float(np.linalg.norm(v64 - q64)) if metric == "l2" else 1.0 - float(v64 @ q64)

    return [vid for _, vid in sorted((dist(v), vid) for vid, v in vecs.items())[:k]]


def _unit(v: np.ndarray) -> np.ndarray:
    return (v / np.linalg.norm(v)).astype(np.float32)


@pytest.mark.parametrize("metric", ["l2", "cosine"])
def test_topk_matches_brute_force(metric: str) -> None:
    rng = np.random.default_rng(7)
    store = VectorStore(DIM, metric, initial_capacity=4)  # fuerza varios crecimientos
    vecs: dict[str, np.ndarray] = {}
    for i in range(300):
        v = rng.normal(size=DIM).astype(np.float32)
        v = _unit(v) if metric == "cosine" else v
        vecs[f"v{i}"] = v
        store.put(f"v{i}", v, {"i": i}, version=i + 1)
    for _ in range(10):
        q = rng.normal(size=DIM).astype(np.float32)
        q = _unit(q) if metric == "cosine" else q
        got = [h.id for h in store.topk(q, 10)]
        assert got == _ref_topk(vecs, q, 10, metric)


def test_ties_broken_by_id_including_at_cutoff() -> None:
    store = VectorStore(2, "l2")
    for vid in ["d", "b", "e", "a", "c"]:
        store.put(vid, np.array([1.0, 0.0], np.float32), {}, 1)
    store.put("far", np.array([9.0, 9.0], np.float32), {}, 1)
    q = np.zeros(2, np.float32)
    assert [h.id for h in store.topk(q, 3)] == ["a", "b", "c"]


def test_predicate_prefilters() -> None:
    store = VectorStore(2, "l2")
    for i in range(20):
        store.put(f"v{i}", np.array([i, 0], np.float32), {"even": i % 2 == 0}, 1)
    hits = store.topk(np.zeros(2, np.float32), 3, lambda m: m["even"] is False)
    assert [h.id for h in hits] == ["v1", "v3", "v5"]
    assert store.topk(np.zeros(2, np.float32), 3, lambda m: False) == []


def test_update_keeps_unspecified_parts_and_remove() -> None:
    store = VectorStore(2, "l2")
    store.put("x", np.array([1, 2], np.float32), {"a": 1}, 1)
    store.put("x", None, {"a": 2}, 2)
    item = store.get("x")
    assert item is not None and item.vector.tolist() == [1, 2] and item.metadata == {"a": 2}
    assert item.version == 2
    assert store.remove("x") and not store.remove("x")
    assert store.get("x") is None and len(store) == 0
    with pytest.raises(ValueError):
        store.put("y", None, {}, 1)  # id nuevo sin vector


def test_bit_identical_distances_regardless_of_insertion_order() -> None:
    rng = np.random.default_rng(3)
    dim = 7  # dimensión impar: filas con distinta alineación en memoria
    data = {f"v{i}": rng.normal(size=dim).astype(np.float32) for i in range(200)}
    a, b = VectorStore(dim, "l2", 8), VectorStore(dim, "l2", 8)
    for vid, v in data.items():
        a.put(vid, v, {}, 1)
    order = list(data)
    random.Random(1).shuffle(order)
    for vid in order[:50]:  # b inserta basura, la borra (reusa slots) y luego en otro orden
        b.put("tmp-" + vid, data[vid], {}, 1)
    for vid in order[:50]:
        b.remove("tmp-" + vid)
    for vid in order:
        b.put(vid, data[vid], {}, 1)
    q = rng.normal(size=dim).astype(np.float32)
    ha, hb = a.topk(q, 200), b.topk(q, 200)
    assert [(h.id, h.dist) for h in ha] == [(h.id, h.dist) for h in hb]  # igualdad exacta
