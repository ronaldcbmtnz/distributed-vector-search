"""Deterministic client workloads used by the F1 experiment harness."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np

from dvs.client import DVSClient  # type: ignore[import-untyped]
from dvs.client.errors import APIError, ClientError  # type: ignore[import-untyped]


def load_vectors(
    client: DVSClient,
    collection: str,
    dataset_dir: Path,
    run_id: str,
    batch_size: int,
) -> int:
    """Load a namespaced dataset and fail the run if any batch item fails."""
    vectors = np.load(dataset_dir / "vectors.npy", mmap_mode="r", allow_pickle=False)
    with (
        (dataset_dir / "ids.txt").open(encoding="utf-8") as ids_file,
        (dataset_dir / "metadata.jsonl").open(encoding="utf-8") as metadata_file,
    ):
        batch: list[dict[str, Any]] = []
        loaded = 0
        for vector, raw_id, raw_metadata in zip(vectors, ids_file, metadata_file, strict=True):
            metadata = json.loads(raw_metadata)
            metadata["experiment_run_id"] = run_id
            batch.append(
                {
                    "id": f"{run_id}-{raw_id.rstrip('\r\n')}",
                    "vector": vector.tolist(),
                    "metadata": metadata,
                }
            )
            if len(batch) == batch_size:
                loaded += _check_batch(client.batch(collection, batch), len(batch))
                batch.clear()
        if batch:
            loaded += _check_batch(client.batch(collection, batch), len(batch))
    return loaded


def _check_batch(response: dict[str, Any], expected: int) -> int:
    results = response.get("results")
    if not isinstance(results, list) or len(results) != expected:
        raise RuntimeError("batch response does not match the submitted item count")
    failures = [
        item for item in results if not isinstance(item, dict) or item.get("ok") is not True
    ]
    if failures:
        raise RuntimeError(f"dataset load failed for {len(failures)} vector(s)")
    return len(results)


def _failure_status(exc: Exception) -> str:
    if isinstance(exc, APIError):
        return f"error:http_{exc.status_code}:{exc.error_code or 'api_error'}"
    return f"error:{type(exc).__name__}"


def _search_rows(
    client_index: int,
    query_indices: list[int],
    *,
    base_url: str,
    api_key: str,
    collection: str,
    queries: np.ndarray,
    gt_ids: np.ndarray,
    k: int,
    run_id: str,
    history_path: Path,
    workload_started: threading.Event,
) -> list[dict[str, Any]]:
    rows = []
    with DVSClient(
        base_url=base_url,
        api_key=api_key,
        timeout=10.0,
        max_retries=0,
        history_path=str(history_path),
        client_id=f"{run_id}-search-{client_index:02d}",
    ) as client:
        for operation_index, query_index in enumerate(query_indices):
            workload_started.set()
            t_inv = time.monotonic()
            partial = None
            coverage = None
            recall: float | None = 0.0
            try:
                result = client.search(
                    collection,
                    queries[query_index].tolist(),
                    k=k,
                    consistency="eventual",
                    allow_partial=True,
                    filter={"experiment_run_id": run_id},
                )
                partial_value = result.get("partial")
                partial = partial_value if isinstance(partial_value, bool) else None
                coverage_value = result.get("coverage")
                if isinstance(coverage_value, (int, float)) and not isinstance(
                    coverage_value, bool
                ):
                    coverage = float(coverage_value)
                hits = result.get("results", [])
                if not isinstance(hits, list):
                    raise RuntimeError("search response results is not a list")
                returned = {
                    str(item["id"]) for item in hits if isinstance(item, dict) and "id" in item
                }
                expected = {f"{run_id}-{int(item)}" for item in gt_ids[query_index]}
                recall = len(returned & expected) / min(k, len(expected))
                status = "partial" if partial else "ok"
            except (ClientError, RuntimeError, ValueError, TypeError) as exc:
                status = _failure_status(exc)
            t_resp = time.monotonic()
            rows.append(
                {
                    "operation_index": operation_index,
                    "op": "search",
                    "t_inv": t_inv,
                    "t_resp": t_resp,
                    "latency_ms": (t_resp - t_inv) * 1000.0,
                    "status": status,
                    "partial": partial,
                    "coverage": coverage,
                    "recall": recall,
                }
            )
    return rows


def run_search_workload(
    *,
    base_url: str,
    api_key: str,
    collection: str,
    queries: np.ndarray,
    gt_ids: np.ndarray,
    k: int,
    run_id: str,
    clients: int,
    history_dir: Path,
    workload_started: threading.Event,
) -> list[dict[str, Any]]:
    query_count = len(queries)
    rows: list[dict[str, Any]] = []
    futures = []
    with ThreadPoolExecutor(max_workers=clients) as pool:
        for client_index in range(clients):
            indices = list(range(client_index, query_count, clients))
            if indices:
                futures.append(
                    pool.submit(
                        _search_rows,
                        client_index,
                        indices,
                        base_url=base_url,
                        api_key=api_key,
                        collection=collection,
                        queries=queries,
                        gt_ids=gt_ids,
                        k=k,
                        run_id=run_id,
                        history_path=history_dir / f"history-c{client_index:02d}.jsonl",
                        workload_started=workload_started,
                    )
                )
        for future in as_completed(futures):
            rows.extend(future.result())
    return rows


def run_e3_workload(
    *,
    base_url: str,
    api_key: str,
    collection: str,
    vectors: np.ndarray,
    metadata: list[dict[str, Any]],
    operations: int,
    clients: int,
    run_id: str,
    history_dir: Path,
    observations_path: Path,
    workload_started: threading.Event,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run concurrent GET/upsert operations over the same deterministic set of IDs."""
    if clients < 2 or clients % 2:
        raise ValueError(
            "E3 requires an even client count of at least 2 (half readers, half writers)"
        )
    shared_count = min(16, len(vectors))
    if shared_count < 1 or operations < 1:
        raise ValueError("E3 requires at least one vector and one operation")
    item_ids = [f"{run_id}-e3-{index:04d}" for index in range(shared_count)]
    versions = [0] * shared_count
    locks = [threading.Lock() for _ in range(shared_count)]
    writer_count = clients // 2
    observations: list[dict[str, Any]] = []
    observation_lock = threading.Lock()
    rows: list[dict[str, Any]] = []

    with DVSClient(
        base_url=base_url,
        api_key=api_key,
        timeout=10.0,
        max_retries=0,
        history_path=str(history_dir / "history-e3-setup.jsonl"),
        client_id=f"{run_id}-e3-setup",
    ) as setup:
        initial = []
        for index, item_id in enumerate(item_ids):
            vector = vectors[index].copy()
            vector[0] = 0.0
            item_metadata = dict(metadata[index])
            item_metadata.update({"ver": 0, "experiment_run_id": run_id})
            initial.append({"id": item_id, "vector": vector.tolist(), "metadata": item_metadata})
        _check_batch(setup.batch(collection, initial), len(initial))

    def worker(role: str, worker_index: int) -> list[dict[str, Any]]:
        local_rows: list[dict[str, Any]] = []
        history_path = history_dir / f"history-e3-{role}{worker_index:02d}.jsonl"
        with DVSClient(
            base_url=base_url,
            api_key=api_key,
            timeout=10.0,
            max_retries=0,
            history_path=str(history_path),
            client_id=f"{run_id}-e3-{role}{worker_index:02d}",
        ) as client:
            for operation_index in range(worker_index, operations, writer_count):
                item_index = operation_index % shared_count
                item_id = item_ids[item_index]
                workload_started.set()
                t_inv = time.monotonic()
                status = "ok"
                if role == "writer":
                    with locks[item_index]:
                        versions[item_index] += 1
                        version = versions[item_index]
                        vector = vectors[item_index].copy()
                        vector[0] = float(version)
                        item_metadata = dict(metadata[item_index])
                        item_metadata.update({"ver": version, "experiment_run_id": run_id})
                        try:
                            client.upsert(
                                collection,
                                item_id,
                                vector.tolist(),
                                item_metadata,
                            )
                        except (ClientError, ValueError, TypeError) as exc:
                            status = _failure_status(exc)
                else:
                    try:
                        snapshot = client.get(collection, item_id, consistency="eventual")
                        record = {"id": item_id, "response": snapshot}
                        with observation_lock:
                            observations.append(record)
                    except (ClientError, ValueError, TypeError) as exc:
                        status = _failure_status(exc)
                        with observation_lock:
                            observations.append({"id": item_id, "status": status})
                t_resp = time.monotonic()
                local_rows.append(
                    {
                        "operation_index": operation_index,
                        "op": "upsert" if role == "writer" else "get",
                        "t_inv": t_inv,
                        "t_resp": t_resp,
                        "latency_ms": (t_resp - t_inv) * 1000.0,
                        "status": status,
                        "partial": False,
                        "coverage": None,
                        "recall": None,
                    }
                )
        return local_rows

    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [
            pool.submit(worker, role, index)
            for role in ("writer", "reader")
            for index in range(writer_count)
        ]
        for future in as_completed(futures):
            rows.extend(future.result())

    observations_path.write_text(
        "".join(
            json.dumps(item, ensure_ascii=False, allow_nan=False) + "\n" for item in observations
        ),
        encoding="utf-8",
    )
    checker = {
        "observations": len(observations),
        "checked": 0,
        "mixed": 0,
        "invalid": 0,
        "tolerance": 1e-6,
        "ok": False,
        "examples": [],
    }
    return rows, checker


def run_mixed_workload(
    *,
    base_url: str,
    api_key: str,
    collection: str,
    queries: np.ndarray,
    gt_ids: np.ndarray,
    operations: int,
    clients: int,
    k: int,
    run_id: str,
    history_dir: Path,
    workload_started: threading.Event,
) -> list[dict[str, Any]]:
    """Run concurrent dataset searches and independent upserts for E1 crash experiments."""
    if clients < 2 or clients % 2:
        raise ValueError("E1 requires an even client count of at least 2")
    writer_count = reader_count = clients // 2
    rows: list[dict[str, Any]] = []

    def write_rows(worker_index: int) -> list[dict[str, Any]]:
        local_rows: list[dict[str, Any]] = []
        with DVSClient(
            base_url=base_url,
            api_key=api_key,
            timeout=10.0,
            max_retries=0,
            history_path=str(history_dir / f"history-e1-writer{worker_index:02d}.jsonl"),
            client_id=f"{run_id}-e1-writer{worker_index:02d}",
        ) as client:
            for operation_index in range(worker_index, operations, writer_count):
                workload_started.set()
                t_inv = time.monotonic()
                status = "ok"
                try:
                    vector = queries[operation_index % len(queries)].copy()
                    vector[0] = float(operation_index + 1)
                    client.upsert(
                        collection,
                        f"{run_id}-write-{operation_index:08d}",
                        vector.tolist(),
                        {
                            "cat": "c0",
                            "price": 0.0,
                            "tags": ["t0"],
                            "ver": operation_index + 1,
                            "experiment_run_id": f"{run_id}-writes",
                        },
                    )
                except (ClientError, ValueError, TypeError) as exc:
                    status = _failure_status(exc)
                t_resp = time.monotonic()
                local_rows.append(
                    {
                        "operation_index": operation_index,
                        "op": "upsert",
                        "t_inv": t_inv,
                        "t_resp": t_resp,
                        "latency_ms": (t_resp - t_inv) * 1000.0,
                        "status": status,
                        "partial": False,
                        "coverage": None,
                        "recall": None,
                    }
                )
        return local_rows

    futures = []
    with ThreadPoolExecutor(max_workers=clients) as pool:
        for worker_index in range(reader_count):
            indices = list(range(worker_index, len(queries), reader_count))
            if indices:
                futures.append(
                    pool.submit(
                        _search_rows,
                        worker_index,
                        indices,
                        base_url=base_url,
                        api_key=api_key,
                        collection=collection,
                        queries=queries,
                        gt_ids=gt_ids,
                        k=k,
                        run_id=run_id,
                        history_path=history_dir / f"history-e1-search{worker_index:02d}.jsonl",
                        workload_started=workload_started,
                    )
                )
        for worker_index in range(writer_count):
            futures.append(pool.submit(write_rows, worker_index))
        for future in as_completed(futures):
            rows.extend(future.result())
    return rows
