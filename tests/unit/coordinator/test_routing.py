"""T112a — orden de réplicas por nivel, sospecha local con TTL y caché del mapa."""

import pytest

from dvs.coordinator.routing import ReplicaRouter, ShardMapCache, SuspicionTracker
from dvs.core.model import ShardInfo

from .fakes import FakeTransport, build_map

INFO = ShardInfo(0, "s0/r0", ("s0/r0", "s0/r1", "s0/r2"))


class FakeClock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


def test_strong_reads_only_primary() -> None:
    router = ReplicaRouter(SuspicionTracker(1.0))
    assert router.read_order(INFO, "strong") == ["s0/r0"]


def test_eventual_rotates_round_robin() -> None:
    router = ReplicaRouter(SuspicionTracker(1.0))
    firsts = [router.read_order(INFO, "eventual")[0] for _ in range(6)]
    assert firsts == ["s0/r0", "s0/r1", "s0/r2", "s0/r0", "s0/r1", "s0/r2"]
    assert sorted(router.read_order(INFO, "read_your_writes")) == list(INFO.replicas)


def test_suspects_go_last_until_ttl_expires() -> None:
    clock = FakeClock()
    sus = SuspicionTracker(2.0, clock)
    router = ReplicaRouter(sus)
    sus.suspect("s0/r0")
    for _ in range(3):
        assert router.read_order(INFO, "eventual")[-1] == "s0/r0"
    clock.t += 2.0
    assert not sus.is_suspect("s0/r0")
    sus.suspect("s0/r1")
    sus.clear("s0/r1")
    assert not sus.is_suspect("s0/r1")


async def test_map_cache_ttl_and_invalidate() -> None:
    clock = FakeClock()
    t = FakeTransport(build_map())
    cache = ShardMapCache(t, ttl_s=2.0, clock=clock)
    await cache.get()
    await cache.get()
    assert t.map_calls == 1
    clock.t += 2.0
    await cache.get()
    assert t.map_calls == 2
    cache.invalidate()
    await cache.get()
    assert t.map_calls == 3


class _FailingMap:
    """Transport whose shard_map fails on demand (the map's owner died)."""

    def __init__(self, smap: object) -> None:
        self.smap = smap
        self.fail = False
        self.calls = 0

    async def shard_map(self) -> object:
        self.calls += 1
        if self.fail:
            raise RuntimeError("catalog died")
        return self.smap


async def test_map_cache_serves_last_known_map_when_refresh_fails() -> None:
    clock = FakeClock()
    smap = build_map()
    t = _FailingMap(smap)
    cache = ShardMapCache(t, ttl_s=2.0, clock=clock)  # type: ignore[arg-type]
    assert await cache.get() is smap
    t.fail = True
    clock.t += 2.0
    assert await cache.get() is smap  # stale but usable
    assert await cache.get() is smap and t.calls == 2  # no retry storm within the TTL
    cache.invalidate()
    assert await cache.get() is smap and t.calls == 3
    t.fail = False
    clock.t += 2.0
    assert await cache.get() is smap and t.calls == 4


async def test_map_cache_first_fetch_failure_propagates() -> None:
    t = _FailingMap(build_map())
    t.fail = True
    cache = ShardMapCache(t, ttl_s=2.0, clock=FakeClock())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        await cache.get()
