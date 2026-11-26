"""Validated, bounded-memory loading of datasets produced by ``experiments.data_gen``."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from dvs.client.client import DVSClient, _validate_metadata


def _load_manifest(data_dir: Path) -> tuple[dict[str, Any], int, int, str]:
    try:
        manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read dataset manifest in {data_dir}") from exc
    if not isinstance(manifest, dict):
        raise ValueError("dataset manifest must be a JSON object")
    n, dim, metric = manifest.get("n"), manifest.get("dim"), manifest.get("metric")
    if (
        isinstance(n, bool)
        or not isinstance(n, int)
        or n <= 0
        or isinstance(dim, bool)
        or not isinstance(dim, int)
        or dim <= 0
    ):
        raise ValueError("dataset manifest must contain positive integer n and dim")
    if metric not in {"L2", "cosine"}:
        raise ValueError("dataset manifest metric must be L2 or cosine")
    return manifest, n, dim, metric


def _validate_text_files(data_dir: Path, n: int) -> None:
    ids: set[str] = set()
    try:
        with (data_dir / "ids.txt").open(encoding="utf-8") as id_file:
            for line_number, line in enumerate(id_file, start=1):
                item_id = line.rstrip("\r\n")
                if not item_id:
                    raise ValueError(f"ids.txt contains an empty id at line {line_number}")
                if len(item_id.encode("utf-8")) > 128:
                    raise ValueError(f"id at line {line_number} exceeds the contract limit")
                if item_id in ids:
                    raise ValueError(f"ids.txt contains a duplicate id at line {line_number}")
                ids.add(item_id)
        if len(ids) != n:
            raise ValueError(f"manifest declares {n} vectors but ids.txt contains {len(ids)} ids")

        metadata_count = 0
        with (data_dir / "metadata.jsonl").open(encoding="utf-8") as metadata_file:
            for line_number, line in enumerate(metadata_file, start=1):
                try:
                    metadata = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"invalid metadata JSON at line {line_number}") from exc
                if not isinstance(metadata, Mapping):
                    raise ValueError(f"metadata at line {line_number} must be a JSON object")
                try:
                    _validate_metadata(metadata)
                except ValueError as exc:
                    raise ValueError(f"invalid metadata at line {line_number}: {exc}") from exc
                metadata_count += 1
        if metadata_count != n:
            raise ValueError(
                f"manifest declares {n} vectors but metadata.jsonl contains {metadata_count} rows"
            )
    except OSError as exc:
        raise ValueError(f"dataset is missing ids.txt or metadata.jsonl: {data_dir}") from exc


def _validate_vectors(vectors: np.ndarray, metric: str) -> None:
    if not (np.issubdtype(vectors.dtype, np.integer) or np.issubdtype(vectors.dtype, np.floating)):
        raise ValueError("vectors.npy must contain real numeric values")
    for start in range(0, vectors.shape[0], 8192):
        block = np.asarray(vectors[start : start + 8192])
        if not np.isfinite(block).all():
            raise ValueError("vectors.npy must contain only finite numeric values")
        if metric == "cosine" and np.any(np.linalg.norm(block.astype(np.float64), axis=1) == 0):
            raise ValueError("cosine dataset contains a zero vector")


def load_dataset(
    client: DVSClient,
    collection: str,
    data_dir: str | Path,
    *,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Upsert a T010 dataset in contract-sized batches, validating all files first."""
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= 1000
    ):
        raise ValueError("batch_size must be an integer from 1 through 1000")
    target = Path(data_dir)
    if not isinstance(collection, str) or not collection:
        raise ValueError("collection must be a non-empty string")
    manifest, n, dim, metric = _load_manifest(target)
    try:
        vectors = np.load(target / "vectors.npy", mmap_mode="r", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read vectors.npy in {target}") from exc
    if vectors.ndim != 2 or vectors.shape != (n, dim):
        raise ValueError(
            f"vectors.npy shape {getattr(vectors, 'shape', None)} differs from manifest {(n, dim)}"
        )
    _validate_vectors(vectors, metric)
    _validate_text_files(target, n)

    batches = 0
    succeeded = 0
    errors: list[dict[str, str]] = []
    with (
        (target / "ids.txt").open(encoding="utf-8") as id_file,
        (target / "metadata.jsonl").open(encoding="utf-8") as metadata_file,
    ):
        iterator = zip(vectors, id_file, metadata_file, strict=True)
        while True:
            chunk: list[dict[str, Any]] = []
            try:
                for _ in range(batch_size):
                    vector, raw_id, raw_metadata = next(iterator)
                    chunk.append(
                        {
                            "id": raw_id.rstrip("\r\n"),
                            "vector": vector.tolist(),
                            "metadata": json.loads(raw_metadata),
                        }
                    )
            except StopIteration:
                pass
            if not chunk:
                break
            response = client.batch(collection, chunk)
            results = response.get("results")
            if not isinstance(results, list) or len(results) != len(chunk):
                raise ValueError(
                    "server returned a batch result count that does not match the request"
                )
            batches += 1
            for item, result in zip(chunk, results, strict=True):
                if isinstance(result, dict) and result.get("ok") is True:
                    succeeded += 1
                else:
                    error = result.get("error", {}) if isinstance(result, dict) else {}
                    errors.append(
                        {
                            "id": item["id"],
                            "code": error.get("code", "invalid_response")
                            if isinstance(error, dict)
                            else "invalid_response",
                        }
                    )

    return {
        "collection": collection,
        "dataset": target.name,
        "vectors": n,
        "dim": dim,
        "metric": metric,
        "batches": batches,
        "succeeded": succeeded,
        "failed": n - succeeded,
        "errors": errors,
        "seed": manifest.get("seed"),
    }
