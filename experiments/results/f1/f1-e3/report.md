# Experiment f1-e3

## Scope

- Scenario: `e3`.
- Fault: `none (baseline)`; status: `not requested`.
- Target kind: `ray` (declared by the operator).
- Target URL: `http://127.0.0.1:8080`.
- Commit: `3fa150a3d467f2fc3d2e202b967ae34d0d005dff-dirty`; seed: `20261125`.
- Workload: 4 writers + 4 readers over shared IDs; 2000 operations per role; clients=8, top-k=10; dataset=5000 vectors × 128 dimensions.

The target kind is an operator declaration. The T119b stub is a single in-memory process and cannot demonstrate shard failures or partial-result resilience.

## Results

| Metric | Result |
|---|---:|
| Requests | 4000 |
| Successful operations | 4000 |
| Errors | 0 (0.00%) |
| Partial search responses | n/a (no searches) |
| get latency p50 (ms) | 12.756 |
| get latency p95 (ms) | 15.942 |
| get latency p99 (ms) | 19.666 |
| upsert latency p50 (ms) | 13.735 |
| upsert latency p95 (ms) | 19.056 |
| upsert latency p99 (ms) | 24.142 |
| Recall@10 vs exact ground truth | n/a |
| E3 snapshots checked | 2000 |
| E3 mixed versions | 0 |
| E3 invalid snapshots | 0 |

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
