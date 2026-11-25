"""T110a — sonda: la semántica de Ray de la que depende el diseño de la Fase 1.

Son las observaciones del mapa de dependencias (docs/fase1_ray/03_...), convertidas en
comprobaciones reproducibles. Si una versión futura de Ray cambia algo, este test lo detecta.
"""

import asyncio
import itertools
import os
import time

import pytest
import ray
from ray.exceptions import ActorDiedError, PendingCallsLimitExceeded, RayActorError, RayTaskError

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_local")]


@ray.remote
class Probe:
    def __init__(self) -> None:
        self.state: list[object] = []
        self.reconstructed = ray.get_runtime_context().was_current_actor_reconstructed

    async def put(self, x: object) -> None:
        self.state.append(x)

    async def info(self) -> dict[str, object]:
        return {"pid": os.getpid(), "reconstructed": self.reconstructed, "n": len(self.state)}

    async def sleep(self, s: float) -> str:
        try:
            await asyncio.sleep(s)
            self.state.append("finished")
            return "finished"
        except asyncio.CancelledError:
            self.state.append("cancelled")
            raise

    async def log(self) -> list[object]:
        return list(self.state)

    def block(self, s: float) -> None:
        time.sleep(s)


def _wait_new_pid(h: "ray.actor.ActorHandle", old_pid: int) -> tuple[dict, set[str]]:  # type: ignore[type-arg]
    errors: set[str] = set()
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            info = ray.get(h.info.remote(), timeout=2)
            if info["pid"] != old_pid:
                return info, errors
        except Exception as e:
            errors.add(type(e).__name__)
        time.sleep(0.05)
    raise AssertionError("actor did not come back")


def test_reconstructed_actor_comes_back_empty_after_kill_9() -> None:
    h = Probe.options(max_restarts=-1).remote()
    ray.get([h.put.remote(i) for i in range(5)])
    before = ray.get(h.info.remote())
    assert before["reconstructed"] is False and before["n"] == 5
    os.kill(before["pid"], 9)
    after, _errors = _wait_new_pid(h, before["pid"])
    assert after["reconstructed"] is True
    assert after["n"] == 0  # Ray recupera el proceso, no los datos


def test_without_restarts_the_caller_gets_actor_died() -> None:
    h = Probe.options(max_restarts=0).remote()
    pid = ray.get(h.info.remote())["pid"]
    os.kill(pid, 9)
    with pytest.raises(ActorDiedError):
        ray.get(h.info.remote(), timeout=10)
    assert issubclass(ActorDiedError, RayActorError)


async def test_local_timeout_does_not_stop_remote_but_cancel_does() -> None:
    h = Probe.options(max_concurrency=8).remote()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(h.sleep.remote(0.5), timeout=0.05)
    await asyncio.sleep(0.8)
    assert await h.log.remote() == ["finished"]  # siguió ejecutando en remoto

    ref = h.sleep.remote(5)
    await asyncio.sleep(0.2)
    ray.cancel(ref)
    with pytest.raises(RayTaskError):
        await ref
    assert (await h.log.remote())[-1] == "cancelled"


def test_max_pending_calls_rejects_on_submit() -> None:
    h = Probe.options(max_pending_calls=2, max_concurrency=1).remote()
    ray.get(h.info.remote())
    refs = []
    with pytest.raises(PendingCallsLimitExceeded):
        for _ in range(6):
            refs.append(h.block.remote(0.2))
    assert len(refs) == 2
    ray.get(refs)


def test_get_if_exists_is_idempotent() -> None:
    a = Probe.options(name="probe-idem", get_if_exists=True).remote()
    ray.get(a.put.remote(1))
    b = Probe.options(name="probe-idem", get_if_exists=True).remote()
    assert ray.get(b.info.remote())["n"] == 1
    ray.kill(a)


def test_async_actor_call_order_is_not_guaranteed_informative() -> None:
    """Informativo: Ray no garantiza el orden en actores async. No se afirma que haya
    inversiones (es probabilístico); se registra cuántas hubo para el mapa de dependencias."""
    h = Probe.options(max_concurrency=64).remote()
    ray.get([h.put.remote(i) for i in range(2000)])
    seen = ray.get(h.log.remote())
    inversions = sum(1 for x, y in itertools.pairwise(seen) if y < x)
    print(f"\n[ray-order] inversiones en 2000 llamadas async: {inversions}")
    assert sorted(seen) == list(range(2000))  # llegan todas, en el orden que sea
