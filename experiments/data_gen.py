"""T010 - Synthetic dataset generator with deterministic seed and exact ground truth.

Output contract (docs/REPARTO_TAREAS.md, section 3.5):

    data/<name>/
      manifest.json
      vectors.npy
      ids.txt
      metadata.jsonl
      queries.npy
      gt_ids.npy
      gt_dist.npy
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

_ALLOWED_METRICS = {"l2": "L2", "cosine": "cosine"}


@dataclass(frozen=True, slots=True)
class DatasetParams:
    name: str
    seed: int
    n: int
    dim: int
    n_clusters: int
    metric: str
    n_queries: int
    k_gt: int
    output_dir: Path


@dataclass(frozen=True, slots=True)
class DatasetBundle:
    manifest: dict[str, Any]
    vectors: npt.NDArray[np.float32]
    ids: npt.NDArray[np.int64]
    metadata: list[dict[str, Any]]
    queries: npt.NDArray[np.float32]
    gt_ids: npt.NDArray[np.int64]
    gt_dist: npt.NDArray[np.float64]


def _normalize_metric(metric: str) -> str:
    key = metric.strip().lower()
    if key not in _ALLOWED_METRICS:
        raise ValueError("metric must be one of: L2, cosine")
    return _ALLOWED_METRICS[key]


def _validate_name(name: str) -> str:
    value = name.strip()
    if not value:
        raise ValueError("name must be a non-empty string")
    if any(ch in value for ch in ("/", "\\")):
        raise ValueError("name must not contain path separators")
    return value


def _validate_positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def validate_params(params: DatasetParams) -> DatasetParams:
    name = _validate_name(params.name)
    n = _validate_positive_int(params.n, "n")
    dim = _validate_positive_int(params.dim, "dim")
    n_clusters = _validate_positive_int(params.n_clusters, "n_clusters")
    n_queries = _validate_positive_int(params.n_queries, "n_queries")
    k_gt = _validate_positive_int(params.k_gt, "k_gt")

    if n_clusters > n:
        raise ValueError("n_clusters must be <= n")
    if k_gt > n:
        raise ValueError("k_gt must be <= n")

    metric = _normalize_metric(params.metric)
    output_dir = Path(params.output_dir)

    return DatasetParams(
        name=name,
        seed=int(params.seed),
        n=n,
        dim=dim,
        n_clusters=n_clusters,
        metric=metric,
        n_queries=n_queries,
        k_gt=k_gt,
        output_dir=output_dir,
    )


def _normalize_rows(arr: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    norms = np.linalg.norm(arr, axis=1)
    safe = np.where(norms == 0.0, 1.0, norms)
    return arr / safe[:, None]


def _generate_vectors(
    rng: np.random.Generator, n: int, dim: int, n_clusters: int, metric: str
) -> npt.NDArray[np.float32]:
    centers = rng.normal(loc=0.0, scale=5.0, size=(n_clusters, dim))
    assignments = rng.integers(0, n_clusters, size=n)
    vectors64 = centers[assignments] + rng.normal(loc=0.0, scale=1.0, size=(n, dim))
    if metric == "cosine":
        vectors64 = _normalize_rows(vectors64)
    return vectors64.astype(np.float32)


def _generate_queries(
    rng: np.random.Generator,
    vectors: npt.NDArray[np.float32],
    n_queries: int,
    n_clusters: int,
    metric: str,
) -> npt.NDArray[np.float32]:
    dim = vectors.shape[1]
    centers = np.zeros((n_clusters, dim), dtype=np.float64)
    # Reconstruct coarse centers deterministically from a stable random sample of vectors.
    sample_idx = rng.integers(0, vectors.shape[0], size=n_clusters)
    centers[:] = vectors[sample_idx].astype(np.float64)

    assignments = rng.integers(0, n_clusters, size=n_queries)
    queries64 = centers[assignments] + rng.normal(loc=0.0, scale=0.7, size=(n_queries, dim))
    if metric == "cosine":
        queries64 = _normalize_rows(queries64)
    return queries64.astype(np.float32)


def _generate_metadata(rng: np.random.Generator, n: int) -> list[dict[str, Any]]:
    cats = rng.integers(0, 10, size=n)
    prices = rng.uniform(0.0, 1000.0, size=n)
    versions = rng.integers(1, 1_000_000, size=n)
    metadata: list[dict[str, Any]] = []

    for i in range(n):
        tag_count = int(rng.integers(1, 4))
        tags_idx = rng.choice(20, size=tag_count, replace=False)
        tags = sorted(f"t{int(t)}" for t in tags_idx)
        metadata.append(
            {
                "cat": f"c{int(cats[i])}",
                "price": float(prices[i]),
                "tags": tags,
                "ver": int(versions[i]),
            }
        )
    return metadata


def compute_exact_topk(
    vectors: npt.NDArray[np.float32],
    queries: npt.NDArray[np.float32],
    ids: npt.NDArray[np.int64],
    metric: str,
    k: int,
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]:
    metric_canonical = _normalize_metric(metric)
    vectors64 = vectors.astype(np.float64)
    queries64 = queries.astype(np.float64)

    n_queries = queries64.shape[0]
    out_ids = np.empty((n_queries, k), dtype=np.int64)
    out_dist = np.empty((n_queries, k), dtype=np.float64)

    vec_norms: npt.NDArray[np.float64] | None = None
    if metric_canonical == "cosine":
        vec_norms = np.linalg.norm(vectors64, axis=1)
        if np.any(vec_norms == 0.0):
            raise ValueError("cosine metric requires non-zero vectors")

    for i in range(n_queries):
        q = queries64[i]
        if metric_canonical == "L2":
            diff = vectors64 - q
            sq = np.sum(diff * diff, axis=1, dtype=np.float64)
            dists = np.sqrt(sq)
        else:
            q_norm = float(np.linalg.norm(q))
            if q_norm == 0.0:
                raise ValueError("cosine metric requires non-zero queries")
            dots = vectors64 @ q
            denom = vec_norms * q_norm  # type: ignore[operator]
            dists = (1.0 - (dots / denom)).astype(np.float64)

        # Deterministic ordering by (distance, id).
        ranked = np.lexsort((ids, dists))[:k]
        out_ids[i] = ids[ranked]
        out_dist[i] = dists[ranked]

    return out_ids, out_dist


def generate_dataset(params: DatasetParams) -> DatasetBundle:
    p = validate_params(params)
    rng = np.random.default_rng(p.seed)

    vectors = _generate_vectors(rng, p.n, p.dim, p.n_clusters, p.metric)
    ids = np.arange(p.n, dtype=np.int64)
    metadata = _generate_metadata(rng, p.n)
    queries = _generate_queries(rng, vectors, p.n_queries, p.n_clusters, p.metric)
    gt_ids, gt_dist = compute_exact_topk(vectors, queries, ids, p.metric, p.k_gt)

    manifest: dict[str, int | str] = {
        "seed": p.seed,
        "n": p.n,
        "dim": p.dim,
        "n_clusters": p.n_clusters,
        "metric": p.metric,
        "n_queries": p.n_queries,
        "k_gt": p.k_gt,
    }

    return DatasetBundle(
        manifest=manifest,
        vectors=vectors,
        ids=ids,
        metadata=metadata,
        queries=queries,
        gt_ids=gt_ids,
        gt_dist=gt_dist,
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_dataset(bundle: DatasetBundle, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)

    _write_json(target_dir / "manifest.json", bundle.manifest)
    np.save(target_dir / "vectors.npy", bundle.vectors)
    (target_dir / "ids.txt").write_text("\n".join(str(int(x)) for x in bundle.ids) + "\n", encoding="utf-8")

    with (target_dir / "metadata.jsonl").open("w", encoding="utf-8") as fp:
        for row in bundle.metadata:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    np.save(target_dir / "queries.npy", bundle.queries)
    np.save(target_dir / "gt_ids.npy", bundle.gt_ids)
    np.save(target_dir / "gt_dist.npy", bundle.gt_dist)


def generate_and_write(params: DatasetParams) -> Path:
    p = validate_params(params)
    bundle = generate_dataset(p)
    target = p.output_dir / p.name
    write_dataset(bundle, target)
    return target


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="experiments.data_gen",
        description="Generate synthetic vectors, queries and exact ground truth for DVS",
    )
    parser.add_argument("--name", default="demo", help="Dataset folder name under output-dir")
    parser.add_argument("--seed", type=int, default=20261125, help="Random seed")
    parser.add_argument("--n", type=int, default=5000, help="Number of vectors")
    parser.add_argument("--dim", type=int, default=128, help="Vector dimensionality")
    parser.add_argument("--n-clusters", type=int, default=32, help="Number of Gaussian clusters")
    parser.add_argument("--metric", default="L2", help="Distance metric: L2 or cosine")
    parser.add_argument("--n-queries", type=int, default=200, help="Number of queries")
    parser.add_argument("--k-gt", type=int, default=10, help="Exact top-k size")
    parser.add_argument("--output-dir", default="data", help="Output root directory")
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Delete output directory before generating the dataset",
    )
    return parser.parse_args(argv)


def _build_params(args: argparse.Namespace) -> DatasetParams:
    return DatasetParams(
        name=args.name,
        seed=args.seed,
        n=args.n,
        dim=args.dim,
        n_clusters=args.n_clusters,
        metric=args.metric,
        n_queries=args.n_queries,
        k_gt=args.k_gt,
        output_dir=Path(args.output_dir),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        params = _build_params(args)
        validated = validate_params(params)
        if args.clean_output:
            shutil.rmtree(validated.output_dir / validated.name, ignore_errors=True)
        target = generate_and_write(validated)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"dataset": str(target), "manifest": str(target / "manifest.json")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
