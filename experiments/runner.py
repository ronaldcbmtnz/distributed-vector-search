"""Reproducible F1 experiment runner. The first supported campaign is the E2 baseline."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import numpy as np

from dvs.client import DVSClient  # type: ignore[import-untyped]
from dvs.client.errors import ClientError  # type: ignore[import-untyped]
from experiments.analysis import build_report, write_latencies
from experiments.data_gen import DatasetParams, generate_and_write
from experiments.e3_checker import check_observations
from experiments.faults import FaultController
from experiments.workloads import (
    load_vectors,
    run_e3_workload,
    run_mixed_workload,
    run_search_workload,
)

ROOT = Path(__file__).resolve().parents[1]
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
FAULT_RECOVERY_TIMEOUT_S = 510.0


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )


def _admin_snapshot(base_url: str, api_key: str, endpoint: str) -> dict[str, Any]:
    try:
        response = httpx.get(
            f"{base_url.rstrip('/')}/v1/{endpoint}",
            headers={"X-API-Key": api_key},
            timeout=5.0,
        )
        return {"http_status": response.status_code, "body": response.json()}
    except (httpx.HTTPError, ValueError) as exc:
        return {"error": type(exc).__name__, "message": str(exc)}


def run_scenario(args: argparse.Namespace) -> Path:
    if not _RUN_ID.fullmatch(args.run_id):
        raise ValueError("run id must contain 1..80 safe ASCII letters, digits, '.', '_' or '-'")
    if "," in args.base_url:
        raise ValueError("experiments require one explicit base URL, not SDK gateway failover")
    if args.clients < 1 or args.queries < 1 or args.vectors < args.k or args.k < 1:
        raise ValueError("clients/queries/k must be positive and vectors must be at least k")
    if args.scenario in {"e1", "e3"} and (args.clients < 2 or args.clients % 2):
        raise ValueError(f"{args.scenario.upper()} requires an even client count of at least 2")
    if args.scenario == "e1" and (args.target_kind != "ray" or args.fault_kind != "kill-pid"):
        raise ValueError("E1 requires a Ray target and --fault-kind kill-pid")
    if args.scenario == "e3" and args.fault_kind:
        raise ValueError("E3 is a consistency workload; use it without fault injection")
    if args.batch_size < 1 or args.batch_size > 1000:
        raise ValueError("batch size must be between 1 and 1000")
    if args.fault_kind:
        if args.target_kind != "ray":
            raise ValueError(
                "Docker/SO fault injection requires --target-kind ray; the stub has no shards"
            )
        if not args.fault_target:
            raise ValueError("--fault-target is required when --fault-kind is set")
        if (
            not math.isfinite(args.fault_duration)
            or not 0 < args.fault_duration <= 3600
            or not math.isfinite(args.fault_after)
            or args.fault_after < 0
        ):
            raise ValueError("fault duration must be 0..3600s and fault delay must be non-negative")
        if args.fault_kind in {"netem", "ray-delay"}:
            if (
                args.fault_ms is None
                or not math.isfinite(args.fault_ms)
                or not 0 < args.fault_ms <= 60_000
            ):
                raise ValueError("netem/ray-delay requires --fault-ms between 0 and 60000")
        elif args.fault_ms is not None:
            raise ValueError("--fault-ms is only valid with netem or ray-delay")
        if not Path(args.output_dir).resolve().is_relative_to((ROOT / "runs").resolve()):
            raise ValueError(
                "fault experiment artifacts must be stored under the repository runs/ directory"
            )
    server_run_id = None
    if args.target_kind == "ray":
        server_run_id = os.getenv("DVS_RUN_ID", "compose").strip() or "compose"
        if not _RUN_ID.fullmatch(server_run_id):
            raise ValueError("DVS_RUN_ID must be a safe run id so server logs stay under runs/")
    run_dir = Path(args.output_dir).resolve() / str(args.run_id)
    if run_dir.exists():
        entries = list(run_dir.iterdir())
        if any(entry.name != "logs" or not entry.is_dir() for entry in entries):
            raise FileExistsError(f"run directory already contains experiment data: {run_dir}")
        existing_logs = run_dir / "logs"
        if (existing_logs / "runner.jsonl").exists():
            raise FileExistsError(f"runner log already exists for run id: {args.run_id}")
    else:
        run_dir.mkdir(parents=True)
    client_dir = run_dir / "client"
    dataset_root = run_dir / "dataset"
    logs_dir = run_dir / "logs"
    client_dir.mkdir()
    dataset_root.mkdir()
    logs_dir.mkdir(exist_ok=True)
    started_monotonic = time.monotonic()

    run: dict[str, Any] = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "target_kind": args.target_kind,
        "base_url": args.base_url.rstrip("/"),
        "collection": args.collection,
        "seed": args.seed,
        "vectors": args.vectors,
        "dim": args.dim,
        "n_clusters": args.n_clusters,
        "queries": args.queries,
        "k": args.k,
        "clients": args.clients,
        "batch_size": args.batch_size,
        "git_commit": _git_commit(),
        "started_at": _utc_now(),
        "started_mono": started_monotonic,
        "status": "running",
        "fault_status": "pending" if args.fault_kind else "not requested",
        "fault": (
            {"kind": args.fault_kind, "target": args.fault_target, "delay_ms": args.fault_ms}
            if args.fault_kind
            else None
        ),
        "server_run_id": server_run_id,
    }
    _write_json(run_dir / "run.json", run)
    (logs_dir / "runner.jsonl").write_text(
        json.dumps(
            {
                "event": "run_started",
                "ts": _utc_now(),
                "mono": started_monotonic,
                "run_id": args.run_id,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    api_key = args.api_key
    admin_key = args.admin_api_key
    rows: list[dict[str, Any]] = []
    recall: float | None = None
    e3_check: dict[str, Any] | None = None
    try:
        data_dir = generate_and_write(
            DatasetParams(
                name="demo",
                seed=args.seed,
                n=args.vectors,
                dim=args.dim,
                n_clusters=args.n_clusters,
                metric="L2",
                n_queries=args.queries,
                k_gt=args.k,
                output_dir=dataset_root,
            )
        )
        with DVSClient(
            base_url=args.base_url,
            api_key=api_key,
            timeout=10.0,
            max_retries=0,
            history_path=str(client_dir / "history-loader.jsonl"),
            client_id=f"{args.run_id}-loader",
        ) as loader:
            health = loader.health()
            _write_json(run_dir / "health_before.json", health)
            _write_json(
                run_dir / "status_before.json",
                _admin_snapshot(args.base_url, admin_key, "admin/status"),
            )
            _write_json(
                run_dir / "metrics_before.json",
                _admin_snapshot(args.base_url, admin_key, "metrics"),
            )
            loaded = load_vectors(loader, args.collection, data_dir, args.run_id, args.batch_size)
            if loaded != args.vectors:
                raise RuntimeError(f"loaded {loaded} vectors; expected {args.vectors}")

        queries = np.load(data_dir / "queries.npy", mmap_mode="r", allow_pickle=False)
        gt_ids = np.load(data_dir / "gt_ids.npy", mmap_mode="r", allow_pickle=False)
        workload_started = threading.Event()
        workload_done = threading.Event()
        fault_errors: list[Exception] = []
        fault_thread: threading.Thread | None = None
        fault_applied = False
        if args.fault_kind:
            controller = FaultController(run_dir)

            def inject_during_load() -> None:
                nonlocal fault_applied
                if not workload_started.wait(timeout=30):
                    return
                if args.fault_after:
                    workload_done.wait(timeout=args.fault_after)
                if workload_done.is_set():
                    return
                try:
                    token = controller.apply(
                        args.fault_kind, args.fault_target, delay_ms=args.fault_ms
                    )
                    fault_applied = True
                    workload_done.wait(timeout=args.fault_duration)
                    controller.recover(token)
                except Exception as exc:
                    fault_errors.append(exc)

            fault_thread = threading.Thread(target=inject_during_load, name="dvs-fault-runner")
            fault_thread.start()
        try:
            if args.scenario == "e2":
                rows = run_search_workload(
                    base_url=args.base_url,
                    api_key=api_key,
                    collection=args.collection,
                    queries=queries,
                    gt_ids=gt_ids,
                    k=args.k,
                    run_id=args.run_id,
                    clients=args.clients,
                    history_dir=client_dir,
                    workload_started=workload_started,
                )
            elif args.scenario == "e1":
                rows = run_mixed_workload(
                    base_url=args.base_url,
                    api_key=api_key,
                    collection=args.collection,
                    queries=queries,
                    gt_ids=gt_ids,
                    operations=args.queries,
                    clients=args.clients,
                    k=args.k,
                    run_id=args.run_id,
                    history_dir=client_dir,
                    workload_started=workload_started,
                )
            else:
                metadata = [
                    json.loads(line)
                    for line in (data_dir / "metadata.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                rows, _ = run_e3_workload(
                    base_url=args.base_url,
                    api_key=api_key,
                    collection=args.collection,
                    vectors=np.load(data_dir / "vectors.npy", mmap_mode="r", allow_pickle=False),
                    metadata=metadata,
                    operations=args.queries,
                    clients=args.clients,
                    run_id=args.run_id,
                    history_dir=client_dir,
                    observations_path=client_dir / "e3_observations.jsonl",
                    workload_started=workload_started,
                )
                e3_check = check_observations(client_dir / "e3_observations.jsonl")
                run["e3_check"] = e3_check
        finally:
            workload_done.set()
            if fault_thread is not None:
                fault_thread.join(timeout=FAULT_RECOVERY_TIMEOUT_S)
                if fault_thread.is_alive():
                    fault_errors.append(
                        RuntimeError("fault runner did not finish within 330 seconds")
                    )
        searches = [row for row in rows if row.get("recall") is not None]
        recall = float(np.mean([row["recall"] for row in searches])) if searches else None
        write_latencies(client_dir / "latencies.csv", rows)
        if fault_errors:
            run["fault_status"] = "failed"
            run["fault_error"] = f"{type(fault_errors[0]).__name__}: {fault_errors[0]}"
            raise RuntimeError(run["fault_error"])
        if args.fault_kind and not fault_applied:
            run["fault_status"] = "missed"
            raise RuntimeError("workload ended before the scheduled fault could be injected")
        run["fault_status"] = "recovered" if args.fault_kind else "not requested"
        run["status"] = "completed"
        run["loaded_vectors"] = loaded
        run["completed_operations"] = len(rows)
        run["completed_searches"] = sum(row["op"] == "search" for row in rows)
        run["completed_writes"] = sum(row["op"] == "upsert" for row in rows)
        run["recall_at_k"] = recall
        _write_json(
            run_dir / "status_after.json", _admin_snapshot(args.base_url, admin_key, "admin/status")
        )
        _write_json(
            run_dir / "metrics_after.json", _admin_snapshot(args.base_url, admin_key, "metrics")
        )
        return run_dir
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        run["finished_at"] = _utc_now()
        finished_monotonic = time.monotonic()
        run["finished_mono"] = finished_monotonic
        run["duration_seconds"] = finished_monotonic - started_monotonic
        if server_run_id is None:
            run["server_logs"] = {"source": None, "copied": 0}
        else:
            source = ROOT / "runs" / server_run_id / "logs"
            copied = 0
            try:
                if source.resolve() != logs_dir.resolve() and source.is_dir():
                    for log_file in sorted(source.glob("*.jsonl")):
                        if log_file.name == "runner.jsonl":
                            continue
                        destination = logs_dir / log_file.name
                        if not destination.exists():
                            shutil.copy2(log_file, destination)
                            copied += 1
                run["server_logs"] = {
                    "source": str(source),
                    "copied": copied,
                    "collected_at": _utc_now(),
                }
            except OSError as exc:
                run["server_logs"] = {
                    "source": str(source),
                    "error": f"{type(exc).__name__}: {exc}",
                }
        for name, endpoint in (
            ("status_after.json", "admin/status"),
            ("metrics_after.json", "metrics"),
        ):
            path = run_dir / name
            if not path.exists():
                _write_json(path, _admin_snapshot(args.base_url, admin_key, endpoint))
        _write_json(run_dir / "run.json", run)
        (run_dir / "report.md").write_text(
            build_report(run, rows, recall, e3_check), encoding="utf-8"
        )
        with (logs_dir / "runner.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "event": "run_finished",
                        "ts": _utc_now(),
                        "mono": finished_monotonic,
                        "run_id": args.run_id,
                        "status": run["status"],
                    }
                )
                + "\n"
            )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default=os.getenv("SCENARIO", "e2"), choices=("e1", "e2", "e3")
    )
    parser.add_argument("--base-url", default=os.getenv("EXP_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--target-kind", choices=("stub", "ray"), default="stub")
    parser.add_argument("--api-key", default=os.getenv("EXP_API_KEY", "dev-key-rw"))
    parser.add_argument("--admin-api-key", default=os.getenv("EXP_ADMIN_API_KEY", "dev-admin"))
    parser.add_argument("--collection", default="demo")
    parser.add_argument("--seed", type=int, default=int(os.getenv("DATA_SEED", "20261125")))
    parser.add_argument("--vectors", type=int, default=int(os.getenv("DATA_N", "5000")))
    parser.add_argument("--dim", type=int, default=int(os.getenv("DATA_DIM", "128")))
    parser.add_argument("--n-clusters", type=int, default=int(os.getenv("DATA_CLUSTERS", "32")))
    parser.add_argument("--queries", type=int, default=int(os.getenv("DATA_QUERIES", "200")))
    parser.add_argument("--k", type=int, default=int(os.getenv("DATA_K", "10")))
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "runs")
    parser.add_argument(
        "--fault-kind",
        choices=("docker-kill", "kill-pid", "docker-pause", "netem", "disconnect", "ray-delay"),
        help="inject a scoped fault during the workload; ray-delay calls the T120a hook",
    )
    parser.add_argument(
        "--fault-target", help="Compose service, or replica id for kill-pid/ray-delay"
    )
    parser.add_argument("--fault-ms", type=float, help="delay for netem (milliseconds)")
    parser.add_argument("--fault-after", type=float, default=1.0, help="seconds after first search")
    parser.add_argument("--fault-duration", type=float, default=5.0, help="maximum fault duration")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.run_id is None:
        args.run_id = (
            f"{args.scenario}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
        )
    try:
        output = run_scenario(args)
    except (OSError, ValueError, ClientError, RuntimeError) as exc:
        print(f"experiment failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"run_dir": str(output), "report": str(output / "report.md")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
