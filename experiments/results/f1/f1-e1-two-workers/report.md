# Experiment f1-e1-two-workers

## Scope

- Scenario: `e1`.
- Fault: `{'kind': 'docker-kill', 'target': 'ray-worker-1', 'delay_ms': None}`; status: `recovered`.
- Target kind: `ray` (declared by the operator).
- Target URL: `http://127.0.0.1:8080`.
- Commit: `3fa150a3d467f2fc3d2e202b967ae34d0d005dff-dirty`; seed: `20261125`.
- Workload: 3000 searches + 3000 upserts; clients=8, top-k=10; dataset=5000 vectors × 128 dimensions.

The target kind is an operator declaration. The T119b stub is a single in-memory process and cannot demonstrate shard failures or partial-result resilience.

## Results

| Metric | Result |
|---|---:|
| Requests | 6000 |
| Successful operations | 3296 |
| Errors | 2704 (45.07%) |
| Partial search responses | 0 (0.00%) |
| search latency p50 (ms) | 24.020 |
| search latency p95 (ms) | 132.674 |
| search latency p99 (ms) | 144.592 |
| upsert latency p50 (ms) | 4.223 |
| upsert latency p95 (ms) | 2015.783 |
| upsert latency p99 (ms) | 2020.077 |
| Recall@10 vs exact ground truth | 1.000 |

## Before / during / after the fault

Fault applied at t=2.216 s and recovered at t=37.307 s (from the first operation; by invocation time). An operation invoked just before `fault_applied` may already be affected: the event is written when the injection command returns.

| Window | Ops | Searches | Writes | Errors | Partial | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| before | 538 | 238 | 300 | 4 | 0 | 28.597 | 145.277 |
| during | 2883 | 2762 | 121 | 121 | 0 | 22.897 | 2008.502 |
| after | 2579 | 0 | 2579 | 2579 | 0 | 3.717 | 2019.526 |

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
