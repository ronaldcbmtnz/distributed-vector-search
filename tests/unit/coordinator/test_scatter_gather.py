"""T112b — scatter-gather: plazo global, hedge, redirección RYW, parciales y cancelación."""

import asyncio

import numpy as np
import pytest

from dvs.coordinator.routing import ReplicaRouter, SuspicionTracker
from dvs.coordinator.scatter_gather import ScatterGather
from dvs.coordinator.types import SearchRequest
from dvs.core.errors import Unavailable
from dvs.core.model import Consistency, Hit

from .fakes import FakeTransport, build_map

ATTEMPT = 0.03
DEADLINE = 0.25


def _hits(shard: int, n: int = 3) -> list[Hit]:
    return [
        Hit(f"s{shard}-v{i}", dist=float(shard + i * 3), metadata={}, version=1) for i in range(n)
    ]


def setup(num_shards: int = 3) -> tuple[FakeTransport, ScatterGather]:
    smap = build_map(num_shards)
    t = FakeTransport(smap)
    for rep in t.replicas.values():
        rep.hits = _hits(rep.shard)
        rep.applied_seq = 10
    return t, ScatterGather(t, ReplicaRouter(SuspicionTracker(5.0)), ATTEMPT)


def req(
    consistency: Consistency = "eventual",
    k: int = 4,
    tokens: dict[int, int] | None = None,
    allow_partial: bool = True,
    deadline: float = DEADLINE,
) -> SearchRequest:
    return SearchRequest(
        query_id="q1",
        collection="demo",
        vector=np.zeros(2, np.float32),
        k=k,
        filter=None,
        consistency=consistency,
        deadline_s=deadline,
        allow_partial=allow_partial,
        tokens=tokens or {},
    )


async def test_all_ok_merges_global_topk() -> None:
    t, sg = setup()
    resp = await sg.search(t.smap, req(k=4))
    assert [h.id for h in resp.hits] == ["s0-v0", "s1-v0", "s2-v0", "s0-v1"]
    assert (resp.partial, resp.coverage, resp.missing) == (False, 1.0, ())
    assert all(r.status == "ok" and r.applied_seq == 10 for r in resp.shards.values())
    assert len(t.calls_to("search")) == 3  # una réplica por shard, en paralelo


async def test_slow_replica_triggers_one_hedge_and_is_cancelled() -> None:
    t, sg = setup()
    t.r("s0/r0").mode = "never"
    resp = await sg.search(t.smap, req())
    assert not resp.partial and resp.hedges == 1
    assert resp.shards[0].replica == "s0/r1"
    assert "s0/r0" in t.cancelled  # el intento lento se cancela al ganar el hedge
    assert sg.router.suspicion.is_suspect("s0/r0")


async def test_only_one_hedge_per_shard() -> None:
    t, sg = setup()
    for rid in ("s0/r0", "s0/r1", "s0/r2"):
        t.r(rid).mode = "never"
    resp = await sg.search(t.smap, req())
    assert t.calls_to("search").count("s0/r2") == 0  # 1 intento + 1 hedge, no más
    assert resp.missing == ((0, "unreachable"),)


async def test_unreachable_replica_fails_over() -> None:
    t, sg = setup()
    t.r("s1/r0").mode = "unreachable"
    resp = await sg.search(t.smap, req())
    assert not resp.partial and resp.shards[1].replica == "s1/r1"
    assert resp.hedges == 0  # failover por error, no hedge


async def test_unserved_replica_fails_over() -> None:
    t, sg = setup()
    t.r("s2/r0").mode = "unserved"
    resp = await sg.search(t.smap, req())
    assert resp.shards[2].replica == "s2/r1"


async def test_whole_shard_down_gives_marked_partial_quickly() -> None:
    t, sg = setup()
    for rid in ("s1/r0", "s1/r1", "s1/r2"):
        t.r(rid).mode = "unreachable"
    loop = asyncio.get_running_loop()
    start = loop.time()
    resp = await sg.search(t.smap, req())
    assert loop.time() - start < DEADLINE  # no espera al plazo: no queda a quién preguntar
    assert resp.partial and resp.coverage == pytest.approx(2 / 3)
    assert resp.missing == ((1, "unreachable"),)
    assert resp.shards[1].status == "unreachable" and resp.shards[1].replica is None
    assert all(not h.id.startswith("s1-") for h in resp.hits)


async def test_answer_within_deadline_when_a_shard_hangs() -> None:
    t, sg = setup()
    for rid in ("s2/r0", "s2/r1", "s2/r2"):
        t.r(rid).mode = "hang"
    loop = asyncio.get_running_loop()
    start = loop.time()
    resp = await sg.search(t.smap, req())
    elapsed = loop.time() - start
    assert DEADLINE * 0.9 <= elapsed <= DEADLINE + 0.1
    assert resp.partial and resp.missing == ((2, "unreachable"),)
    assert set(t.cancelled) >= {"s2/r0", "s2/r1"}  # lo pendiente se cancela


async def test_allow_partial_false_raises_unavailable() -> None:
    t, sg = setup()
    for rid in ("s0/r0", "s0/r1", "s0/r2"):
        t.r(rid).mode = "unreachable"
    with pytest.raises(Unavailable) as e:
        await sg.search(t.smap, req(allow_partial=False))
    assert e.value.details["missing_shards"] == [{"shard": 0, "reason": "unreachable"}]


async def test_read_your_writes_redirects_stale_backup_to_primary() -> None:
    t, sg = setup()
    sg.router.read_order(t.smap.shard(1), "eventual")  # avanza el round-robin: empieza por r1
    t.r("s1/r1").applied_seq = 5  # backup atrasado respecto al token
    t.r("s1/r0").applied_seq = 12
    resp = await sg.search(t.smap, req("read_your_writes", tokens={1: 12}))
    assert resp.shards[1].replica == "s1/r0" and resp.shards[1].applied_seq == 12
    assert resp.redirects == 1
    assert t.calls_to("search").count("s1/r0") == 1


async def test_tokens_only_apply_to_their_shard_and_level() -> None:
    t, sg = setup()
    for rep in t.replicas.values():
        rep.applied_seq = 5
    resp = await sg.search(t.smap, req("eventual", tokens={0: 99}))  # eventual ignora tokens
    assert not resp.partial and resp.redirects == 0


async def test_stale_everywhere_reports_stale() -> None:
    t, sg = setup()
    t.r("s0/r0").mode = "unreachable"  # el primario no puede servir el token
    for rid in ("s0/r1", "s0/r2"):
        t.r(rid).applied_seq = 1
    resp = await sg.search(t.smap, req("read_your_writes", tokens={0: 50}))
    assert resp.missing == ((0, "stale"),)


async def test_strong_only_queries_primaries() -> None:
    t, sg = setup()
    resp = await sg.search(t.smap, req("strong"))
    assert sorted(t.calls_to("search")) == ["s0/r0", "s1/r0", "s2/r0"]
    assert resp.consistency == "strong" and not resp.partial


async def test_strong_with_primary_down_is_missing() -> None:
    t, sg = setup()
    t.r("s2/r0").mode = "unreachable"
    resp = await sg.search(t.smap, req("strong"))
    assert resp.missing == ((2, "unreachable"),)
    assert "s2/r1" not in t.calls_to("search")


async def test_result_independent_of_response_timing() -> None:
    t, sg = setup()
    base = [h.id for h in (await sg.search(t.smap, req(k=6))).hits]
    for delays in ([0.02, 0.0, 0.01], [0.0, 0.015, 0.005]):
        t2, sg2 = setup()
        for s, d in enumerate(delays):
            for i in range(3):
                t2.r(f"s{s}/r{i}").delay_s = d
        assert [h.id for h in (await sg2.search(t2.smap, req(k=6))).hits] == base
