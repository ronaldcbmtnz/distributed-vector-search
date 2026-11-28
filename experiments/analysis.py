"""Metric definitions and report serialization for experiment runs."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import numpy as np

CSV_FIELDS = ("op", "t_inv", "t_resp", "latency_ms", "status", "partial", "coverage")


def percentile(values: list[float], p: float) -> float | None:
    """Return NumPy's linear percentile over all samples, including outliers."""
    return float(np.percentile(np.asarray(values, dtype=np.float64), p)) if values else None


def write_latencies(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in sorted(rows, key=lambda item: float(item["t_inv"])):
            writer.writerow(
                {
                    "op": row["op"],
                    "t_inv": f"{row['t_inv']:.9f}",
                    "t_resp": f"{row['t_resp']:.9f}",
                    "latency_ms": f"{row['latency_ms']:.6f}",
                    "status": row["status"],
                    "partial": "" if row.get("partial") is None else str(row["partial"]).lower(),
                    "coverage": "" if row.get("coverage") is None else f"{row['coverage']:.6f}",
                }
            )


def build_report(
    run: dict[str, Any],
    rows: list[dict[str, Any]],
    recall: float | None,
    e3_check: dict[str, Any] | None,
) -> str:
    by_operation: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_operation.setdefault(str(row["op"]), []).append(row)
    searches = by_operation.get("search", [])
    partial_count = sum(row.get("partial") is True for row in searches)
    errors = sum(str(row["status"]).startswith("error:") for row in rows)
    error_rate = 100 * errors / len(rows) if rows else 0
    partial_rate = 100 * partial_count / len(searches) if searches else None
    fault_label = run.get("fault") or "none (baseline)"
    fault_status = run.get("fault_status", "not requested")
    if run["scenario"] == "e2":
        workload = f"{run['queries']} searches over {run['vectors']} vectors"
    elif run["scenario"] == "e1":
        workload = f"{run['queries']} searches + {run['queries']} upserts"
    else:
        workload = (
            f"{run['clients'] // 2} writers + {run['clients'] // 2} readers over shared IDs; "
            f"{run['queries']} operations per role"
        )

    def ms(value: float | None) -> str:
        return "n/a" if value is None else f"{value:.3f}"

    partial_metric = (
        f"{partial_count} ({partial_rate:.2f}%)"
        if partial_rate is not None
        else "n/a (no searches)"
    )

    lines = [
        f"# Experiment {run['run_id']}",
        "",
        "## Scope",
        "",
        f"- Scenario: `{run['scenario']}`.",
        f"- Fault: `{fault_label}`; status: `{fault_status}`.",
        f"- Target kind: `{run['target_kind']}` (declared by the operator).",
        f"- Target URL: `{run['base_url']}`.",
        f"- Commit: `{run['git_commit']}`; seed: `{run['seed']}`.",
        f"- Workload: {workload}; clients={run['clients']}, top-k={run['k']}; "
        f"dataset={run['vectors']} vectors × {run['dim']} dimensions.",
        "",
        "The target kind is an operator declaration. The T119b stub is a single in-memory process "
        "and cannot demonstrate shard failures or partial-result resilience.",
        "",
        "## Results",
        "",
        "| Metric | Result |",
        "|---|---:|",
        f"| Requests | {len(rows)} |",
        f"| Successful operations | {len(rows) - errors} |",
        f"| Errors | {errors} ({error_rate:.2f}%) |",
        f"| Partial search responses | {partial_metric} |",
        *[
            f"| {operation} latency p{p} (ms) | "
            f"{ms(percentile([float(row['latency_ms']) for row in group], p))} |"
            for operation, group in sorted(by_operation.items())
            for p in (50, 95, 99)
        ],
        f"| Recall@{run['k']} vs exact ground truth | {ms(recall)} |",
    ]
    if e3_check is not None:
        lines.extend(
            [
                f"| E3 snapshots checked | {e3_check['checked']} |",
                f"| E3 mixed versions | {e3_check['mixed']} |",
                f"| E3 invalid snapshots | {e3_check['invalid']} |",
            ]
        )
    windows = run.get("fault_windows")
    if windows and windows.get("fault_applied_mono") is not None:
        lines.extend(
            [
                "",
                "## Before / during / after the fault",
                "",
                f"Fault applied at t={windows.get('fault_applied_s')} s and recovered at "
                f"t={windows.get('fault_recovered_s')} s (from the first operation; by "
                "invocation time). An operation invoked just before `fault_applied` may already "
                "be affected: the event is written when the injection command returns.",
                "",
                "| Window | Ops | Searches | Writes | Errors | Partial | p50 (ms) | p99 (ms) |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
                *[
                    f"| {name} | {w['ops']} | {w['searches']} | {w['writes']} | {w['errors']} | "
                    f"{w['partial']} | {ms(w['p50_ms'])} | {ms(w['p99_ms'])} |"
                    for name in ("before", "during", "after")
                    for w in [windows[name]]
                ],
                "",
                "Per-second series: `client/timeline.csv`.",
            ]
        )
    lines.extend(
        [
            "",
            "Latency percentiles use every recorded operation latency, including errors. Error "
            "percentage uses all operations as its denominator; partial percentage uses only "
            "search attempts. Recall averages "
            "per-search intersection with exact ground-truth IDs over k; "
            "failed searches contribute "
            "zero. Recall is `n/a` for workloads without searches.",
            "",
            "## Artifacts",
            "",
            "- `run.json`: parameters, seed, commit, target, status, and server log source.",
            "- `client/latencies.csv`: one row per attempt; "
            "invocation/response times are monotonic "
            "seconds.",
            "- `client/history-*.jsonl`: SDK request histories.",
            "- `client/e3_observations.jsonl`: E3 GET snapshots, when applicable.",
            "- `faults.jsonl`: fault request/application/recovery events, when applicable.",
            "- `status_before.json`, `status_after.json`, `metrics_before.json`, "
            "`metrics_after.json`.",
            "- `logs/*.jsonl`: runner events and collected Ray server logs, when available.",
            "- `dataset/demo/`: generated data and exact ground truth used by this run.",
            "",
        ]
    )
    return "\n".join(lines)


_WRITE_OPS = frozenset({"upsert", "insert", "update", "delete", "batch"})


def _window_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    searches = [row for row in rows if row["op"] == "search"]
    return {
        "ops": len(rows),
        "errors": sum(str(row["status"]).startswith("error:") for row in rows),
        "partial": sum(row.get("partial") is True for row in searches),
        "searches": len(searches),
        "writes": sum(row["op"] in _WRITE_OPS for row in rows),
        "p50_ms": percentile([float(row["latency_ms"]) for row in rows], 50),
        "p99_ms": percentile([float(row["latency_ms"]) for row in rows], 99),
    }


def fault_windows(rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    """Split operations by invocation time into before / during / after the fault.

    `events` are `faults.jsonl` records; their `mono` clock is the runner's own monotonic clock,
    the same one used for `t_inv`. Without a `fault_applied` event every row is "before".
    """
    applied = next((e["mono"] for e in events if e.get("event") == "fault_applied"), None)
    recovered = next((e["mono"] for e in events if e.get("event") == "fault_recovered"), None)
    groups: dict[str, list[dict[str, Any]]] = {"before": [], "during": [], "after": []}
    for row in rows:
        t = float(row["t_inv"])
        if applied is None or t < applied:
            groups["before"].append(row)
        elif recovered is None or t < recovered:
            groups["during"].append(row)
        else:
            groups["after"].append(row)
    return {
        "fault_applied_mono": applied,
        "fault_recovered_mono": recovered,
        **{name: _window_stats(group) for name, group in groups.items()},
    }


def timeline(rows: list[dict[str, Any]], bucket_s: float = 1.0) -> list[dict[str, Any]]:
    """Per-bucket counts and latency of operations, by invocation time from the first one."""
    if not rows:
        return []
    origin = min(float(row["t_inv"]) for row in rows)
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(int((float(row["t_inv"]) - origin) // bucket_s), []).append(row)
    return [
        {"t_s": index * bucket_s, "origin_mono": origin, **_window_stats(buckets[index])}
        for index in sorted(buckets)
    ]


def read_latencies(path: Path) -> list[dict[str, Any]]:
    """Inverse of `write_latencies` (types restored for the analysis functions)."""
    with path.open(newline="", encoding="utf-8") as stream:
        return [
            {
                "op": row["op"],
                "t_inv": float(row["t_inv"]),
                "t_resp": float(row["t_resp"]),
                "latency_ms": float(row["latency_ms"]),
                "status": row["status"],
                "partial": {"true": True, "false": False}.get(row["partial"]),
                "coverage": float(row["coverage"]) if row["coverage"] else None,
            }
            for row in csv.DictReader(stream)
        ]


def analyze_run(run_dir: Path, bucket_s: float = 1.0) -> dict[str, Any]:
    """Write `client/timeline.csv` and `fault_windows.json` for a finished run directory."""
    import json

    rows = read_latencies(run_dir / "client" / "latencies.csv")
    journal = run_dir / "faults.jsonl"
    events = (
        [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines() if line]
        if journal.exists()
        else []
    )
    windows = fault_windows(rows, events)
    series = timeline(rows, bucket_s)
    origin = series[0]["origin_mono"] if series else 0.0
    for key in ("fault_applied_mono", "fault_recovered_mono"):
        if windows[key] is not None:
            windows[key.replace("_mono", "_s")] = round(windows[key] - origin, 3)
    fields = ("t_s", "ops", "searches", "writes", "errors", "partial", "p50_ms", "p99_ms")
    with (run_dir / "client" / "timeline.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for item in series:
            writer.writerow(
                {k: (f"{v:.3f}" if isinstance(v, float) else v) for k, v in item.items()}
            )
    (run_dir / "fault_windows.json").write_text(
        json.dumps(windows, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    return windows


if __name__ == "__main__":
    import json
    import sys

    for argument in sys.argv[1:]:
        print(argument, json.dumps(analyze_run(Path(argument)), indent=1))
