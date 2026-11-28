# Experiment f1-e2-shard-down

## Scope

- Scenario: `e2`.
- Fault: `{'kind': 'ray-kill-shard', 'target': 's0', 'delay_ms': None}`; status: `recovered`.
- Target kind: `ray` (declared by the operator).
- Target URL: `http://127.0.0.1:8080`.
- Commit: `3fa150a3d467f2fc3d2e202b967ae34d0d005dff-dirty`; seed: `20261125`.
- Workload: 2000 searches over 5000 vectors; clients=8, top-k=10; dataset=5000 vectors × 128 dimensions.

The target kind is an operator declaration. The T119b stub is a single in-memory process and cannot demonstrate shard failures or partial-result resilience.

## Results

| Metric | Result |
|---|---:|
| Requests | 2000 |
| Successful operations | 2000 |
| Errors | 0 (0.00%) |
| Partial search responses | 851 (42.55%) |
| search latency p50 (ms) | 48.645 |
| search latency p95 (ms) | 85.399 |
| search latency p99 (ms) | 120.778 |
| Recall@10 vs exact ground truth | 0.893 |

## Before / during / after the fault

Fault applied at t=8.557 s and recovered at t=10.494 s (from the first operation; by invocation time). An operation invoked just before `fault_applied` may already be affected: the event is written when the injection command returns.

| Window | Ops | Searches | Writes | Errors | Partial | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| before | 1235 | 1235 | 0 | 0 | 86 | 48.916 | 125.145 |
| during | 309 | 309 | 0 | 0 | 309 | 49.048 | 78.977 |
| after | 456 | 456 | 0 | 0 | 456 | 47.886 | 79.389 |

Per-second series: `client/timeline.csv`.

Latency percentiles use every recorded operation latency, including errors. Error percentage uses all operations as its denominator; partial percentage uses only search attempts. Recall averages per-search intersection with exact ground-truth IDs over k; failed searches contribute zero. Recall is `n/a` for workloads without searches.

## Artifacts

- `run.json`: parameters, seed, commit, target, status, and server log source.
- `client/latencies.csv`: one row per attempt; invocation/response times are monotonic seconds.
- `client/history-*.jsonl`: SDK request histories.
- `client/e3_observations.jsonl`: E3 GET snapshots, when applicable.
- `faults.jsonl`: fault request/application/recovery events, when applicable.
- `status_before.json`, `status_after.json`, `metrics_before.json`, `metrics_after.json`.
- `logs/*.jsonl`: runner events and collected Ray server logs, when available.
- `dataset/demo/`: generated data and exact ground truth used by this run.
