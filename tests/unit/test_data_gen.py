from __future__ import annotations

import json
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, TypedDict

import numpy as np
import pytest

from experiments.data_gen import (
    DatasetParams,
    compute_exact_topk,
    generate_and_write,
    generate_dataset,
    write_dataset,
)


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


@pytest.mark.parametrize("metric", ["L2", "cosine"])
def test_ground_truth_matches_independent_reference_for_each_metric(metric: str) -> None:
    vectors = np.array(
        [[1.0, 2.0, 0.0], [-1.0, 0.5, 3.0], [2.0, -1.0, 1.0], [0.5, 1.0, -2.0]],
        dtype=np.float32,
    )
    queries = np.array([[0.25, -0.5, 1.0], [-2.0, 1.0, 0.5]], dtype=np.float32)
    ids = np.array([40, 10, 30, 20], dtype=np.int64)

    actual_ids, actual_dist = compute_exact_topk(vectors, queries, ids, metric, k=4)
    expected = []
    for query in queries.astype(np.float64):
        ranked = []
        for vector, vector_id in zip(vectors.astype(np.float64), ids.tolist(), strict=True):
            if metric == "L2":
                squared = sum(
                    (float(a) - float(b)) ** 2 for a, b in zip(vector, query, strict=True)
                )
                distance = squared**0.5
            else:
                dot = sum(float(a) * float(b) for a, b in zip(vector, query, strict=True))
                v_norm = sum(float(value) ** 2 for value in vector) ** 0.5
                q_norm = sum(float(value) ** 2 for value in query) ** 0.5
                distance = 1.0 - dot / (v_norm * q_norm)
            ranked.append((distance, vector_id))
        expected.append(sorted(ranked))

    assert actual_ids.tolist() == [[item[1] for item in row] for row in expected]
    expected_dist = [[item[0] for item in row] for row in expected]
    assert np.allclose(actual_dist, expected_dist, rtol=0, atol=1e-15)


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

    for field, value in (("n", 2.5), ("dim", True), ("n_queries", 0), ("seed", -1)):
        params = dict(
            name="invalid",
            seed=1,
            n=2,
            dim=2,
            n_clusters=1,
            metric="L2",
            n_queries=1,
            k_gt=1,
            output_dir=tmp_path,
        )
        params[field] = value
        with pytest.raises(ValueError):
            generate_and_write(DatasetParams(**params))
        assert not (tmp_path / "invalid").exists()

    for name in (".", "..", "nested/name", "C:escape"):
        with pytest.raises(ValueError, match="name"):
            generate_and_write(DatasetParams(name, 1, 2, 2, 1, "L2", 1, 1, tmp_path))


def test_make_data_target_generates_contract_files(tmp_path: Path) -> None:
    if shutil.which("make") is None:
        pytest.skip("make is not available in this environment")

    repo_root = Path(__file__).resolve().parents[2]
    data_root = tmp_path / "data"

    command = [
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
    ]
    subprocess.run(
        command,
        cwd=repo_root,
        check=True,
    )

    first_run = {
        (dataset, file.name): file.read_bytes()
        for dataset in ("demo", "demo_cos")
        for file in (data_root / dataset).iterdir()
        if file.is_file()
    }
    subprocess.run(command, cwd=repo_root, check=True)
    second_run = {
        (dataset, file.name): file.read_bytes()
        for dataset in ("demo", "demo_cos")
        for file in (data_root / dataset).iterdir()
        if file.is_file()
    }
    assert first_run == second_run

    for name in ("demo", "demo_cos"):
        d = data_root / name
        assert (d / "manifest.json").exists()
        assert (d / "vectors.npy").exists()
        assert (d / "ids.txt").exists()
        assert (d / "metadata.jsonl").exists()
        assert (d / "queries.npy").exists()
        assert (d / "gt_ids.npy").exists()
        assert (d / "gt_dist.npy").exists()


def test_cosine_rejects_zero_norm_rows_and_queries() -> None:
    ids = np.array([0, 1], dtype=np.int64)
    with pytest.raises(ValueError, match="non-zero vectors"):
        compute_exact_topk(
            np.array([[0.0, 0.0], [1.0, 0.0]], dtype=np.float32),
            np.array([[0.0, 1.0]], dtype=np.float32),
            ids,
            "cosine",
            1,
        )
    with pytest.raises(ValueError, match="non-zero queries"):
        compute_exact_topk(
            np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            np.array([[0.0, 0.0]], dtype=np.float32),
            ids,
            "cosine",
            1,
        )


def test_topk_rejects_invalid_shapes_values_and_k() -> None:
    vectors = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    queries = np.array([[1.0, 1.0]], dtype=np.float32)
    ids = np.array([0, 1], dtype=np.int64)
    for invalid_k in (0, 3, 1.5, True):
        with pytest.raises(ValueError, match="k must"):
            compute_exact_topk(vectors, queries, ids, "L2", invalid_k)
    with pytest.raises(ValueError, match="same dimension"):
        compute_exact_topk(vectors, np.ones((1, 3)), ids, "L2", 1)
    with pytest.raises(ValueError, match="finite"):
        compute_exact_topk(np.array([[np.nan, 0], [1, 1]]), queries, ids, "L2", 1)


def test_k_boundaries_and_larger_dataset(tmp_path: Path) -> None:
    small = generate_and_write(DatasetParams("small", 4, 3, 2, 1, "L2", 1, 3, tmp_path))
    assert np.load(small / "gt_ids.npy").shape == (1, 3)
    larger = generate_and_write(DatasetParams("larger", 4, 512, 24, 16, "cosine", 32, 10, tmp_path))
    assert np.load(larger / "vectors.npy").shape == (512, 24)
    assert np.isfinite(np.load(larger / "gt_dist.npy")).all()


def test_failed_persistence_keeps_published_dataset_intact(tmp_path: Path) -> None:
    target = generate_and_write(DatasetParams("published", 7, 8, 3, 2, "L2", 2, 2, tmp_path))
    keep_file = target / "keep.txt"
    keep_file.write_text("preserve this file\n", encoding="utf-8")
    before = {path.name: path.read_bytes() for path in target.iterdir() if path.is_file()}

    invalid_bundle = replace(
        generate_dataset(DatasetParams("ignored", 8, 8, 3, 2, "L2", 2, 2, tmp_path)),
        metadata=[{"invalid": object()}],
    )
    with pytest.raises(TypeError):
        write_dataset(invalid_bundle, target)

    after = {path.name: path.read_bytes() for path in target.iterdir() if path.is_file()}
    assert after == before
