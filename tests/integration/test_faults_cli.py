"""T120a — CLI de ganchos de fallos: info / delay / clear / kill (REPARTO §3.4)."""

import json
import time
import uuid
from collections.abc import AsyncIterator

import numpy as np
import pytest

from dvs.config import Config
from dvs.core.model import ShardSearchRequest
from dvs.ray_impl import faults
from dvs.ray_impl.bootstrap import RayDeployment, start, stop

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_local")]


@pytest.fixture
async def dep() -> AsyncIterator[RayDeployment]:
    cfg = Config.model_validate(
        {
            "cluster": {"num_shards": 1, "replication_factor": 3, "placement": "pack"},
            "actors": {"num_cpus": 0.1},
            "collections": [{"name": "demo", "dim": 2, "metric": "l2"}],
            "fault_injection": {"enabled": True},
        }
    )
    d = await start(cfg, cluster=f"f{uuid.uuid4().hex[:6]}", detached=False)
    yield d
    await stop(d)


def cli(dep: RayDeployment, capsys: pytest.CaptureFixture[str], *args: str) -> dict:  # type: ignore[type-arg]
    assert faults.main(["--cluster", dep.transport.cluster, *args]) == 0
    return json.loads(capsys.readouterr().out)


async def test_info_lists_replicas_with_pid_and_role(
    dep: RayDeployment, capsys: pytest.CaptureFixture[str]
) -> None:
    out = cli(dep, capsys, "info")
    reps = {r["replica_id"]: r for r in out["replicas"]}
    assert set(reps) == {"s0/r0", "s0/r1", "s0/r2"}
    assert sorted(r["role"] for r in reps.values()) == ["backup", "backup", "primary"]
    assert all(r["pid"] > 0 and "host" in r for r in reps.values())


async def test_delay_then_clear(dep: RayDeployment, capsys: pytest.CaptureFixture[str]) -> None:
    req = ShardSearchRequest("q", 0, "demo", np.zeros(2, np.float32), 1)
    cli(
        dep,
        capsys,
        "delay",
        "--replica",
        "s0/r1",
        "--ms",
        "300",
        "--mode",
        "async",
        "--ops",
        "search",
    )
    t0 = time.monotonic()
    await dep.transport.search("s0/r1", req, 2.0)
    assert time.monotonic() - t0 >= 0.3
    cli(dep, capsys, "clear", "--replica", "s0/r1")
    t0 = time.monotonic()
    await dep.transport.search("s0/r1", req, 2.0)
    assert time.monotonic() - t0 < 0.2


async def test_kill_without_restart(dep: RayDeployment, capsys: pytest.CaptureFixture[str]) -> None:
    cli(dep, capsys, "kill", "--replica", "s0/r2", "--no-restart")
    time.sleep(0.5)
    out = cli(dep, capsys, "info")
    dead = next(r for r in out["replicas"] if r["replica_id"] == "s0/r2")
    assert "error" in dead
