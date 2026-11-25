from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, TypedDict

import numpy as np
import pytest

from experiments.data_gen import DatasetParams, compute_exact_topk, generate_and_write


class LoadedDataset(TypedDict):
    manifest: dict[str, Any]
    vectors: np.ndarray
    ids_txt: list[str]
    metadata: list[dict[str, Any]]
    queries: np.ndarray
    gt_ids: np.ndarray
    gt_dist: np.ndarray


def _load_dataset(path: Path) -> LoadedDataset:
    return {
        "manifest": json.loads((path / "manifest.json").read_text(encoding="utf-8")),
        "vectors": np.load(path / "vectors.npy"),
        "ids_txt": (path / "ids.txt").read_text(encoding="utf-8").strip().splitlines(),
        "metadata": [
            json.loads(line)
            for line in (path / "metadata.jsonl").read_text(encoding="utf-8").strip().splitlines()
        ],
        "queries": np.load(path / "queries.npy"),
        "gt_ids": np.load(path / "gt_ids.npy"),
        "gt_dist": np.load(path / "gt_dist.npy"),
    }


def test_contract_shapes_types_and_correspondence(tmp_path: Path) -> None:
    out = generate_and_write(
        DatasetParams(
            name="demo",
            seed=123,
            n=64,
            dim=16,
            n_clusters=8,
            metric="L2",
            n_queries=7,
            k_gt=5,
            output_dir=tmp_path,
        )
    )
    ds = _load_dataset(out)

    manifest = ds["manifest"]
    assert manifest == {
        "seed": 123,
        "n": 64,
        "dim": 16,
        "n_clusters": 8,
        "metric": "L2",
        "n_queries": 7,
        "k_gt": 5,
    }

    vectors = ds["vectors"]
    queries = ds["queries"]
    gt_ids = ds["gt_ids"]
    gt_dist = ds["gt_dist"]

    assert isinstance(vectors, np.ndarray)
    assert vectors.shape == (64, 16)
    assert vectors.dtype == np.float32

    assert isinstance(queries, np.ndarray)
    assert queries.shape == (7, 16)
    assert queries.dtype == np.float32

    assert isinstance(gt_ids, np.ndarray)
    assert gt_ids.shape == (7, 5)
    assert gt_ids.dtype == np.int64

    assert isinstance(gt_dist, np.ndarray)
    assert gt_dist.shape == (7, 5)
    assert gt_dist.dtype == np.float64

    ids_txt = ds["ids_txt"]
    metadata = ds["metadata"]
    assert len(ids_txt) == 64
    assert len(metadata) == 64
    assert [int(x) for x in ids_txt] == list(range(64))

    for row in metadata:
        assert row["cat"].startswith("c")
        cat_num = int(row["cat"][1:])
        assert 0 <= cat_num <= 9

        price = float(row["price"])
        assert 0.0 <= price <= 1000.0

        tags = row["tags"]
        assert 1 <= len(tags) <= 3
        assert len(set(tags)) == len(tags)
        for tag in tags:
            assert tag.startswith("t")
            t_num = int(tag[1:])
            assert 0 <= t_num <= 19

        assert isinstance(row["ver"], int)

    for row_ids, row_dist in zip(gt_ids, gt_dist, strict=True):
        pairs = list(zip(row_dist.tolist(), row_ids.tolist(), strict=True))
        assert pairs == sorted(pairs, key=lambda x: (x[0], x[1]))


def test_reproducibility_same_seed(tmp_path: Path) -> None:
    params = {
        "seed": 2026,
        "n": 50,
        "dim": 12,
        "n_clusters": 5,
        "metric": "cosine",
        "n_queries": 6,
        "k_gt": 4,
    }
    d1 = generate_and_write(DatasetParams(name="a", output_dir=tmp_path, **params))
    d2 = generate_and_write(DatasetParams(name="b", output_dir=tmp_path, **params))

    ds1 = _load_dataset(d1)
    ds2 = _load_dataset(d2)

    assert ds1["manifest"] == ds2["manifest"]
    assert np.array_equal(ds1["vectors"], ds2["vectors"])
    assert np.array_equal(ds1["queries"], ds2["queries"])
    assert np.array_equal(ds1["gt_ids"], ds2["gt_ids"])
    assert np.array_equal(ds1["gt_dist"], ds2["gt_dist"])
    assert ds1["ids_txt"] == ds2["ids_txt"]
    assert ds1["metadata"] == ds2["metadata"]


def test_changes_with_different_seed(tmp_path: Path) -> None:
    base = dict(name="demo", n=50, dim=12, n_clusters=5, metric="L2", n_queries=6, k_gt=4)
    d1 = generate_and_write(DatasetParams(seed=1, output_dir=tmp_path / "one", **base))
    d2 = generate_and_write(DatasetParams(seed=2, output_dir=tmp_path / "two", **base))

    ds1 = _load_dataset(d1)
    ds2 = _load_dataset(d2)

    assert not np.array_equal(ds1["vectors"], ds2["vectors"])
    assert not np.array_equal(ds1["queries"], ds2["queries"])
    assert ds1["metadata"] != ds2["metadata"]


def test_ground_truth_matches_reference_small_case() -> None:
    vectors = np.array(
        [
            [0.0, 0.0],
            [2.0, 0.0],
            [0.0, 2.0],
            [1.0, 1.0],
        ],
        dtype=np.float32,
    )
    queries = np.array([[1.0, 0.0], [0.0, 1.5]], dtype=np.float32)
    ids = np.array([0, 1, 2, 3], dtype=np.int64)

    got_ids, got_dist = compute_exact_topk(vectors, queries, ids, metric="L2", k=3)

    expected_ids: list[list[int]] = []
    expected_dist: list[list[float]] = []
    vectors64 = vectors.astype(np.float64)
    for q in queries.astype(np.float64):
        distances = [
            (idx, math_dist)
            for idx, math_dist in (
                (int(i), float(np.linalg.norm(vectors64[i] - q))) for i in range(vectors64.shape[0])
            )
        ]
        ranked = sorted(distances, key=lambda x: (x[1], x[0]))[:3]
        expected_ids.append([x[0] for x in ranked])
        expected_dist.append([x[1] for x in ranked])

    assert np.array_equal(got_ids, np.array(expected_ids, dtype=np.int64))
    assert np.allclose(got_dist, np.array(expected_dist, dtype=np.float64))


def test_tie_break_by_id() -> None:
    vectors = np.array([[0.0, 0.0], [0.0, 0.0], [2.0, 0.0]], dtype=np.float32)
    queries = np.array([[1.0, 0.0]], dtype=np.float32)
    ids = np.array([10, 2, 7], dtype=np.int64)

    got_ids, got_dist = compute_exact_topk(vectors, queries, ids, metric="L2", k=3)
    assert got_ids.tolist() == [[2, 7, 10]]
    assert got_dist.shape == (1, 3)


def test_invalid_parameters_raise_clear_errors(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="n_clusters must be <= n"):
        generate_and_write(
            DatasetParams(
                name="x",
                seed=0,
                n=2,
                dim=4,
                n_clusters=3,
                metric="L2",
                n_queries=1,
                k_gt=1,
                output_dir=tmp_path,
            )
        )

    with pytest.raises(ValueError, match="k_gt must be <= n"):
        generate_and_write(
            DatasetParams(
                name="x",
                seed=0,
                n=2,
                dim=4,
                n_clusters=2,
                metric="L2",
                n_queries=1,
                k_gt=3,
                output_dir=tmp_path,
            )
        )

    with pytest.raises(ValueError, match="metric must be one of"):
        generate_and_write(
            DatasetParams(
                name="x",
                seed=0,
                n=2,
                dim=4,
                n_clusters=2,
                metric="dot",
                n_queries=1,
                k_gt=1,
                output_dir=tmp_path,
            )
        )


def test_make_data_target_generates_contract_files(tmp_path: Path) -> None:
    if shutil.which("make") is None:
        pytest.skip("make is not available in this environment")

    repo_root = Path(__file__).resolve().parents[2]
    data_root = tmp_path / "data"

    subprocess.run(
        [
            "make",
            "data",
            f"PY={sys.executable}",
            f"DATA_DIR={data_root}",
            "DATA_N=40",
            "DATA_DIM=8",
            "DATA_CLUSTERS=4",
            "DATA_QUERIES=5",
            "DATA_K=3",
            "DATA_SEED=99",
        ],
        cwd=repo_root,
        check=True,
    )

    for name in ("demo", "demo_cos"):
        d = data_root / name
        assert (d / "manifest.json").exists()
        assert (d / "vectors.npy").exists()
        assert (d / "ids.txt").exists()
        assert (d / "metadata.jsonl").exists()
        assert (d / "queries.npy").exists()
        assert (d / "gt_ids.npy").exists()
        assert (d / "gt_dist.npy").exists()
