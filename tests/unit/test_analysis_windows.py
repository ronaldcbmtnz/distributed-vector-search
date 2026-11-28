"""Fault-window and timeline analysis of experiment runs (T121b evidence)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from experiments.analysis import (
    analyze_run,
    build_report,
    fault_windows,
    timeline,
    write_latencies,
)


def _row(
    t: float, *, op: str = "search", status: str = "ok", partial: bool = False
) -> dict[str, Any]:
    return {
        "op": op,
        "t_inv": t,
        "t_resp": t + 0.01,
        "latency_ms": 10.0,
        "status": status,
        "partial": partial if op == "search" else False,
        "coverage": 0.75 if partial else 1.0,
    }


ROWS = [
    _row(100.0),
    _row(100.5, op="upsert"),
    _row(101.2, partial=True),
    _row(101.4, op="upsert", status="error:503"),
    _row(103.0),
]
EVENTS = [
    {"event": "fault_requested", "mono": 100.8},
    {"event": "fault_applied", "mono": 101.0},
    {"event": "fault_recovered", "mono": 102.0},
]


def test_fault_windows_split_by_invocation_time() -> None:
    windows = fault_windows(ROWS, EVENTS)
    assert (windows["before"]["ops"], windows["during"]["ops"], windows["after"]["ops"]) == (
        2,
        2,
        1,
    )
    assert windows["during"]["errors"] == 1 and windows["during"]["partial"] == 1
    assert windows["before"]["writes"] == 1 and windows["after"]["errors"] == 0


def test_reads_by_id_are_not_counted_as_writes() -> None:
    stats = fault_windows([_row(1.0, op="get"), _row(2.0, op="upsert")], [])["before"]
    assert (stats["ops"], stats["searches"], stats["writes"]) == (2, 0, 1)


def test_no_fault_events_means_everything_is_before() -> None:
    windows = fault_windows(ROWS, [])
    assert windows["before"]["ops"] == len(ROWS) and windows["during"]["ops"] == 0


def test_timeline_buckets_from_first_invocation() -> None:
    series = timeline(ROWS, bucket_s=1.0)
    assert [item["t_s"] for item in series] == [0.0, 1.0, 3.0]
    assert [item["ops"] for item in series] == [2, 2, 1]
    assert series[1]["errors"] == 1


def test_analyze_run_writes_artifacts(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    write_latencies(tmp_path / "client" / "latencies.csv", ROWS)
    (tmp_path / "faults.jsonl").write_text(
        "".join(json.dumps(event) + "\n" for event in EVENTS), encoding="utf-8"
    )
    windows = analyze_run(tmp_path)
    assert windows["fault_applied_s"] == 1.0 and windows["fault_recovered_s"] == 2.0
    assert json.loads((tmp_path / "fault_windows.json").read_text())["during"]["ops"] == 2
    assert (tmp_path / "client" / "timeline.csv").read_text().startswith("t_s,ops,")


def test_report_includes_fault_window_table() -> None:
    run = {
        "run_id": "r",
        "scenario": "e1",
        "fault": {"kind": "kill-pid", "target": "s0/r0"},
        "fault_status": "recovered",
        "target_kind": "ray",
        "base_url": "http://127.0.0.1:8080",
        "git_commit": "test",
        "seed": 1,
        "queries": 3,
        "vectors": 8,
        "clients": 2,
        "k": 1,
        "dim": 2,
        "fault_windows": {**fault_windows(ROWS, EVENTS), "fault_applied_s": 1.0},
    }
    report = build_report(run, ROWS, 1.0, None)
    assert "## Before / during / after the fault" in report
    assert "| during | 2 | 1 | 1 | 1 | 1 |" in report
    assert "fault_windows" not in build_report({**run, "fault_windows": None}, ROWS, 1.0, None)
