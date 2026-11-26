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
