# Experiment f1-e2-raydelay

## Scope

- Scenario: `e2`.
- Fault: `{'kind': 'ray-delay', 'target': 's1/r0', 'delay_ms': 500.0}`; status: `recovered`.
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
| Partial search responses | 0 (0.00%) |
| search latency p50 (ms) | 47.740 |
| search latency p95 (ms) | 88.705 |
| search latency p99 (ms) | 137.781 |
| Recall@10 vs exact ground truth | 1.000 |

## Before / during / after the fault

Fault applied at t=8.792 s and recovered at t=17.238 s (from the first operation; by invocation time). An operation invoked just before `fault_applied` may already be affected: the event is written when the injection command returns.

| Window | Ops | Searches | Writes | Errors | Partial | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| before | 1257 | 1257 | 0 | 0 | 0 | 49.964 | 120.355 |
| during | 743 | 743 | 0 | 0 | 0 | 44.193 | 153.145 |
| after | 0 | 0 | 0 | 0 | 0 | n/a | n/a |

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
