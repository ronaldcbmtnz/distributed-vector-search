from __future__ import annotations

import json
import subprocess
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from experiments import runner
from experiments.analysis import build_report, percentile
from experiments.e3_checker import check_observations
from experiments.faults import FaultController, FaultError
from experiments.workloads import _check_batch

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def short_run_dir() -> Iterator[Path]:
    runs = ROOT / "runs"
    runs.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".test-run-", dir=runs) as path:
        yield Path(path)


def test_percentiles_and_empty_sample() -> None:
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.5
    assert percentile([], 99) is None


def test_report_uses_correct_denominators_for_errors_and_partial_searches() -> None:
    run = {
        "run_id": "metric-test",
        "scenario": "e1",
        "fault": None,
        "fault_status": "not requested",
        "target_kind": "stub",
        "base_url": "http://127.0.0.1:8080",
        "git_commit": "test",
        "seed": 1,
        "queries": 2,
        "vectors": 8,
        "clients": 2,
        "k": 1,
        "dim": 2,
    }
    rows = [
        {"op": "search", "status": "partial", "partial": True, "latency_ms": 1.0, "t_inv": 1.0},
        {"op": "search", "status": "ok", "partial": False, "latency_ms": 2.0, "t_inv": 2.0},
        {
            "op": "upsert",
            "status": "error:timeout",
            "partial": False,
            "latency_ms": 3.0,
            "t_inv": 3.0,
        },
        {"op": "upsert", "status": "ok", "partial": False, "latency_ms": 4.0, "t_inv": 4.0},
    ]

    report = build_report(run, rows, 0.5, None)

    assert "| Errors | 1 (25.00%) |" in report
    assert "| Partial search responses | 1 (50.00%) |" in report
    assert "Error percentage uses all operations as its denominator" in report


def test_batch_validation_rejects_partial_load() -> None:
    with pytest.raises(RuntimeError, match="load failed"):
        _check_batch({"results": [{"ok": True}, {"ok": False}]}, 2)
    with pytest.raises(RuntimeError, match="item count"):
        _check_batch({"results": []}, 1)


def test_e3_checker_accepts_consistent_and_reports_mixed_versions(tmp_path) -> None:
    path = tmp_path / "observations.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"id": "a", "vector": [7.0, 1.0], "metadata": {"ver": 7}}),
                json.dumps({"id": "b", "vector": [8.0], "metadata": {"ver": 9}}),
                json.dumps({"id": "c", "vector": [], "metadata": {"ver": 1}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    result = check_observations(path)
    assert result["observations"] == 3
    assert result["checked"] == 2
    assert result["mixed"] == 1
    assert result["invalid"] == 1
    assert result["ok"] is False


def test_e3_checker_rejects_malformed_json(tmp_path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text("{bad}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="line 1"):
        check_observations(path)


def test_fault_scope_rejects_unknown_service_before_docker(short_run_dir) -> None:
    controller = FaultController(short_run_dir)
    with pytest.raises(FaultError, match="unsupported Compose service"):
        controller.apply("docker-kill", "unrelated-container")


def test_netem_is_limited_to_project_workers(short_run_dir) -> None:
    controller = FaultController(short_run_dir)
    with pytest.raises(FaultError, match="limited to the three"):
        controller.apply("netem", "gateway", delay_ms=100)


def test_inspect_rejects_container_outside_compose_project(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(
        controller,
        "_run",
        lambda _args, **_kwargs: json.dumps(
            [
                {
                    "Config": {
                        "Labels": {
                            "com.docker.compose.project": "unrelated",
                            "com.docker.compose.service": "ray-worker-1",
                        }
                    }
                }
            ]
        ),
    )
    with pytest.raises(FaultError, match="outside the verified"):
        controller._inspect("container-id")


@pytest.mark.parametrize("inspect_data", ["null", "[]", "[null]", '[{"Config": []}]'])
def test_inspect_rejects_malformed_docker_responses(
    short_run_dir, monkeypatch, inspect_data
) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(controller, "_run", lambda _args, **_kwargs: inspect_data)

    with pytest.raises(FaultError, match="malformed container"):
        controller._inspect("container-id")


def test_netem_refuses_to_replace_existing_qdisc(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(controller, "_service", lambda _service: ("worker-id", {}))
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs) -> str:
        commands.append(command)
        return "qdisc fq_codel 0: root refcnt 2"

    monkeypatch.setattr(controller, "_run", run)
    with pytest.raises(FaultError, match="non-default qdisc"):
        controller.apply("netem", "ray-worker-1", delay_ms=200)
    assert len(commands) == 1
    assert "add" not in commands[0]


def test_replica_pid_is_mapped_only_to_matching_worker_host(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)

    def service(name: str, *, require_running: bool = True) -> tuple[str, dict]:
        del require_running
        ip = "10.0.0.7" if name == "ray-worker-2" else f"10.0.0.{name[-1]}"
        return (
            f"{name}-id",
            {
                "Config": {"Hostname": f"{name}-host"},
                "NetworkSettings": {"Networks": {"dvs-f1_dvs": {"IPAddress": ip}}},
            },
        )

    monkeypatch.setattr(controller, "_service", service)
    container_id, replica_id, pid = controller._resolve_replica_container(
        "s1/r2", [{"replica_id": "s1/r2", "host": "10.0.0.7", "pid": 918}]
    )
    assert (container_id, replica_id, pid) == ("ray-worker-2-id", "s1/r2", 918)


@pytest.mark.parametrize(
    "failure", [OSError("docker missing"), subprocess.TimeoutExpired(["docker"], 1)]
)
def test_fault_runner_reports_external_command_errors(short_run_dir, monkeypatch, failure) -> None:
    controller = FaultController(short_run_dir)

    def fail(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr("experiments.faults.subprocess.run", fail)
    with pytest.raises(FaultError, match="unavailable or timed out"):
        controller._run(["docker", "info"])


def test_pause_fault_is_journaled_and_health_checked_on_recovery(
    short_run_dir, monkeypatch
) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(controller, "_service", lambda _service: ("scoped-id", {}))
    monkeypatch.setattr(controller, "_inspect", lambda _container: {})
    monkeypatch.setattr(controller, "_wait_cluster_healthy", lambda: None)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        controller, "_run", lambda command, **_kwargs: commands.append(command) or ""
    )

    token = controller.apply("docker-pause", "ray-worker-1")
    controller.recover(token)

    assert [command[1] for command in commands] == ["pause", "unpause"]
    events = [
        json.loads(line) for line in controller.journal.read_text(encoding="utf-8").splitlines()
    ]
    assert [item["event"] for item in events] == [
        "fault_requested",
        "fault_applied",
        "fault_recovered",
    ]
    assert all("ts" in item and "mono" in item for item in events)


def test_recovery_command_failure_is_journaled(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(controller, "_service", lambda _service: ("scoped-id", {}))
    monkeypatch.setattr(controller, "_inspect", lambda _container: {})

    def fail_unpause(command: list[str], **_kwargs) -> str:
        if command[1] == "unpause":
            raise FaultError("unpause failed")
        return ""

    monkeypatch.setattr(controller, "_run", fail_unpause)
    token = controller.apply("docker-pause", "ray-worker-1")

    with pytest.raises(FaultError, match="unpause failed"):
        controller.recover(token)

    events = [
        json.loads(line) for line in controller.journal.read_text(encoding="utf-8").splitlines()
    ]
    assert [item["event"] for item in events] == [
        "fault_requested",
        "fault_applied",
        "fault_recovery_failed",
    ]


def test_fault_injector_errors_are_recorded(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)

    def reject(_service: str, **_kwargs) -> tuple[str, dict[str, Any]]:
        raise FaultError("service is not running")

    monkeypatch.setattr(controller, "_service", reject)
    with pytest.raises(FaultError, match="not running"):
        controller.apply("docker-kill", "ray-worker-1")
    events = [
        json.loads(line) for line in controller.journal.read_text(encoding="utf-8").splitlines()
    ]
    assert [item["event"] for item in events] == ["fault_requested", "fault_failed"]


def test_ray_delay_uses_t120a_hook_and_clears_same_replica(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)
    monkeypatch.setattr(
        controller,
        "_replicas",
        lambda: [{"replica_id": "s1/r2", "host": "worker-2", "pid": 123}],
    )
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        controller,
        "_gateway_command",
        lambda *args: commands.append(args) or "{}",
    )
    monkeypatch.setattr(controller, "_wait_cluster_healthy", lambda: None)

    token = controller.apply("ray-delay", "s1/r2", delay_ms=175)
    controller.recover(token)

    assert commands[0] == (
        "delay",
        "--replica",
        "s1/r2",
        "--ms",
        "175",
        "--mode",
        "async",
        "--ops",
        "search",
    )
    assert commands[1] == ("clear", "--replica", "s1/r2")


def test_runner_validates_fault_parameters_before_creating_run(short_run_dir) -> None:
    args = runner._parser().parse_args(
        [
            "--scenario",
            "e2",
            "--target-kind",
            "ray",
            "--fault-kind",
            "ray-delay",
            "--fault-target",
            "s0/r1",
            "--run-id",
            "bad-delay",
            "--output-dir",
            str(short_run_dir),
        ]
    )

    with pytest.raises(ValueError, match="requires --fault-ms"):
        runner.run_scenario(args)
    assert not (short_run_dir / "bad-delay").exists()


def test_e2_runner_integrates_fault_lifecycle_and_preserves_run_artifacts(
    short_run_dir, monkeypatch
) -> None:
    dataset = short_run_dir / "source"
    dataset.mkdir()
    np.save(dataset / "vectors.npy", np.ones((2, 128), dtype=np.float32))
    np.save(dataset / "queries.npy", np.ones((2, 128), dtype=np.float32))
    np.save(dataset / "gt_ids.npy", np.asarray([[0], [1]], dtype=np.int64))
    (dataset / "ids.txt").write_text("0\n1\n", encoding="utf-8")
    (dataset / "metadata.jsonl").write_text(
        '{"cat":"c0","price":1.0,"tags":["t0"],"ver":1}\n'
        '{"cat":"c1","price":2.0,"tags":["t1"],"ver":1}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(runner, "generate_and_write", lambda _params: dataset)
    monkeypatch.setattr(runner, "_admin_snapshot", lambda *_args: {"ok": True})

    class FakeClient:
        def __init__(self, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def health(self) -> dict[str, str]:
            return {"status": "ok"}

        def batch(self, _collection: str, items: list[dict[str, Any]]) -> dict[str, Any]:
            return {"results": [{"ok": True} for _ in items]}

    monkeypatch.setattr(runner, "DVSClient", FakeClient)
    applied = threading.Event()
    recovered = threading.Event()

    class FakeFaultController:
        def __init__(self, _run_dir: Path) -> None:
            pass

        def apply(self, kind: str, target: str, *, delay_ms: float | None = None) -> dict[str, str]:
            assert (kind, target) == ("docker-pause", "ray-worker-1")
            assert delay_ms is None
            applied.set()
            return {"kind": kind, "target": target}

        def recover(self, _token: dict[str, str]) -> None:
            recovered.set()

    monkeypatch.setattr(runner, "FaultController", FakeFaultController)

    def workload(**kwargs) -> list[dict[str, Any]]:
        kwargs["workload_started"].set()
        assert applied.wait(timeout=2)
        return [
            {
                "op": "search",
                "t_inv": 1.0,
                "t_resp": 1.01,
                "latency_ms": 10.0,
                "status": "ok",
                "partial": False,
                "coverage": 1.0,
                "recall": 1.0,
            }
        ]

    monkeypatch.setattr(runner, "run_search_workload", workload)
    monkeypatch.setenv("DVS_RUN_ID", "runner-fault-test")
    args = runner._parser().parse_args(
        [
            "--scenario",
            "e2",
            "--target-kind",
            "ray",
            "--fault-kind",
            "docker-pause",
            "--fault-target",
            "ray-worker-1",
            "--fault-after",
            "0",
            "--fault-duration",
            "1",
            "--vectors",
            "2",
            "--queries",
            "2",
            "--k",
            "1",
            "--run-id",
            "runner-fault-test",
            "--output-dir",
            str(short_run_dir),
        ]
    )
    output = runner.run_scenario(args)
    assert recovered.is_set()
    run = json.loads((output / "run.json").read_text(encoding="utf-8"))
    assert run["status"] == "completed"
    assert run["fault_status"] == "recovered"
    assert (output / "report.md").is_file()
    assert (output / "client" / "latencies.csv").is_file()


def test_fault_evidence_path_cannot_escape_runs(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("experiments.faults.ROOT", tmp_path)
    with pytest.raises(FaultError, match="under"):
        FaultController(tmp_path / "outside")


def test_kill_shard_fault_kills_every_replica_of_that_shard(short_run_dir, monkeypatch) -> None:
    controller = FaultController(short_run_dir)
    replicas = [{"replica_id": f"s{s}/r{r}"} for s in range(3) for r in range(3)]
    monkeypatch.setattr(controller, "_replicas", lambda: replicas)
    monkeypatch.setattr(controller, "_wait_cluster_healthy", lambda: None)
    killed: list[tuple[str, ...]] = []
    monkeypatch.setattr(controller, "_gateway_command", lambda *args: killed.append(args) or "")

    token = controller.apply("ray-kill-shard", "s1")
    controller.recover(token)

    assert killed == [("kill", "--replica", "s1/r0", "s1/r1", "s1/r2", "--no-restart")]
    assert token["replicas"] == ["s1/r0", "s1/r1", "s1/r2"]
    with pytest.raises(FaultError, match="shard id"):
        controller.apply("ray-kill-shard", "ray-worker-1")
