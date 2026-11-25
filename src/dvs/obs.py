"""Observability: structured JSON logs and in-memory latency metrics (HU-10, decision D17).

Logs: one JSON object per line, to stdout and to `${DVS_RUN_DIR}/logs/<component>-<pid>.jsonl`
when DVS_RUN_DIR is set (field list agreed in docs/REPARTO_TAREAS.md §3.7). `ts` (wall clock) is
only for humans; ordering evidence comes from `mono`, `seq`, `query_id` and `request_id`.
Secrets (API keys) and full vectors are never logged: callers must not pass them, and the
formatter drops any field whose name looks like a secret as a safety net.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}
_SECRET_HINTS = ("key", "token_raw", "authorization", "secret", "password")


class JsonFormatter(logging.Formatter):
    def __init__(self, static: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.static = static or {}

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "mono": round(time.monotonic(), 6),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
            **self.static,
        }
        for k, v in vars(record).items():
            if k in _STANDARD or k.startswith("_"):
                continue
            if any(h in k.lower() for h in _SECRET_HINTS):
                continue
            doc[k] = v
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str, ensure_ascii=False)


def setup_logging(component: str, level: int = logging.INFO) -> Path | None:
    """Configure the root logger for this process. Returns the log file path, if any."""
    static = {"component": component, "pid": os.getpid()}
    fmt = JsonFormatter(static)
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    path: Path | None = None
    run_dir = os.environ.get("DVS_RUN_DIR")
    if run_dir:
        path = Path(run_dir) / "logs" / f"{component}-{os.getpid()}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    return path


# ───────────────────────────── metrics ─────────────────────────────


class Reservoir:
    """Last `maxlen` latency samples (ms); percentiles computed on demand."""

    def __init__(self, maxlen: int = 10_000) -> None:
        self._samples: deque[float] = deque(maxlen=maxlen)
        self.count = 0

    def add(self, ms: float) -> None:
        self._samples.append(ms)
        self.count += 1

    def summary(self) -> dict[str, float | int]:
        if not self._samples:
            return {"count": self.count, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
        arr = np.fromiter(self._samples, dtype=np.float64)
        p50, p95, p99 = np.percentile(arr, [50, 95, 99])
        return {
            "count": self.count,
            "p50_ms": round(float(p50), 3),
            "p95_ms": round(float(p95), 3),
            "p99_ms": round(float(p99), 3),
        }


class Metrics:
    """Gateway metrics. Shape of `snapshot()` agreed in docs/REPARTO_TAREAS.md §3.8."""

    COUNTERS = ("partial", "admission_rejected", "hedges", "stale_redirects", "rate_limited")

    def __init__(self) -> None:
        self.ops: dict[str, Reservoir] = {}
        self.shards: dict[int, Reservoir] = {}
        self.counters: dict[str, int] = dict.fromkeys(self.COUNTERS, 0)

    def observe(self, op: str, ms: float) -> None:
        self.ops.setdefault(op, Reservoir()).add(ms)

    def observe_shard(self, shard: int, ms: float) -> None:
        self.shards.setdefault(shard, Reservoir()).add(ms)

    def inc(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def snapshot(self, replication_lag: dict[str, int] | None = None) -> dict[str, Any]:
        return {
            "ops": {op: r.summary() for op, r in sorted(self.ops.items())},
            "shards": {str(s): r.summary() for s, r in sorted(self.shards.items())},
            "counters": dict(self.counters),
            "replication_lag": replication_lag or {},
        }
