"""T110b / T110c / T114 — el sistema de la Fase 1 sobre Ray real (un nodo local).

Cubre: escritura → búsqueda/lectura, dedup, condicionales, ventana `stale` de un backup
atrasado frente a RYW/`strong`, réplica reconstruida (no lista) y primario caído.
"""

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterator

import numpy as np
import pytest
import ray

from dvs.config import Config
from dvs.coordinator.types import SearchRequest
from dvs.core.errors import Unavailable
from dvs.core.hashing import shard_for
from dvs.core.model import ShardSearchRequest, WriteResult
from dvs.core.validation import make_write_op
from dvs.ray_impl.bootstrap import RayDeployment, start, stop
from dvs.ray_impl.catalog_actor import replica_actor_name

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_local")]

DIM = 4
S = 2


def config(max_restarts: int = 0) -> Config:
    return Config.model_validate(
        {
            "cluster": {
                "num_shards": S,
                "replication_factor": 3,
                "write_quorum": 2,
                "placement": "pack",
            },
            "timeouts_ms": {"write": 1500, "attempt": 120},
            "actors": {"num_cpus": 0.1, "max_restarts": max_restarts},
            "collections": [{"name": "demo", "dim": DIM, "metric": "l2"}],
            "fault_injection": {"enabled": True},
        }
    )


@pytest.fixture
async def dep(request: pytest.FixtureRequest) -> AsyncIterator[RayDeployment]:
    restarts = getattr(request, "param", 0)
    d = await start(config(restarts), cluster=f"t{uuid.uuid4().hex[:8]}", detached=False)
    yield d
    await stop(d)


def ids_in_shard(shard: int, n: int, prefix: str = "v") -> list[str]:
    out, i = [], 0
    while len(out) < n:
        vid = f"{prefix}{i}"
        if shard_for("demo", vid, S) == shard:
            out.append(vid)
        i += 1
    return out


def upsert(vid: str, vec: list[float], meta: dict | None = None, rid: str | None = None):  # type: ignore[no-untyped-def]
    spec = config().collection("demo")
    assert spec is not None
    return make_write_op(
        "upsert",
        spec,
        vid,
        rid or f"r-{vid}-{uuid.uuid4().hex[:6]}",
        config().limits.to_core(),
        vec,
        meta or {},
    )


def search_req(
    consistency: str = "eventual",
    k: int = 5,
    tokens: dict | None = None,  # type: ignore[type-arg]
    q: list[float] | None = None,
) -> SearchRequest:
    return SearchRequest(
        query_id=uuid.uuid4().hex[:8],
        collection="demo",
        vector=np.array(q or [0.0] * DIM, np.float32),
        k=k,
        filter=None,
        consistency=consistency,
        deadline_s=1.0,
        tokens=tokens or {},  # type: ignore[arg-type]
    )


def handle(dep: RayDeployment, rid: str):  # type: ignore[no-untyped-def]
    return ray.get_actor(replica_actor_name(dep.transport.cluster, rid))


# ───────────────────────────── básico ─────────────────────────────


async def test_bootstrap_publishes_map_with_one_primary_per_shard(dep: RayDeployment) -> None:
    smap = await dep.backend.shard_map()
    assert smap.num_shards == S
    health = await dep.backend.status()
    assert len(health) == S * 3 and all(h.status and h.status.ready for h in health)
    for info in smap.shards:
        roles = [h.status.role for h in health if h.shard == info.shard and h.status]
        assert sorted(roles) == ["backup", "backup", "primary"]
        assert info.replicas[0] == info.primary


async def test_write_then_search_and_get_strong(dep: RayDeployment) -> None:
    rng = np.random.default_rng(0)
    vecs = {f"v{i}": rng.normal(size=DIM).round(3).tolist() for i in range(30)}
    outs = await dep.backend.write(
        [upsert(v, x, {"i": i}) for i, (v, x) in enumerate(vecs.items())]
    )
    assert all(isinstance(o, WriteResult) and o.status == "ok" for o in outs)
    resp = await dep.backend.search(search_req("strong", k=5))
    expected = sorted(vecs, key=lambda v: (float(np.linalg.norm(np.array(vecs[v]))), v))[:5]
    assert [h.id for h in resp.hits] == expected and not resp.partial
    got = await dep.backend.get("demo", "v3", "strong", 0, 1.0)
    assert got.status == "ok" and got.item is not None and got.item.metadata == {"i": 3}


async def test_retry_with_same_request_id_is_idempotent(dep: RayDeployment) -> None:
    op = upsert("dup", [1, 2, 3, 4], rid="fixed-request-id")
    (first,) = await dep.backend.write([op])
    (again,) = await dep.backend.write([op])
    assert isinstance(first, WriteResult) and again == first  # mismo token, no se reaplica
    other = upsert("dup", [9, 9, 9, 9], rid="fixed-request-id")
    (conflict,) = await dep.backend.write([other])
    assert getattr(conflict, "code", None) == "idempotency_conflict"


async def test_conditional_writes(dep: RayDeployment) -> None:
    spec, lim = config().collection("demo"), config().limits.to_core()
    assert spec is not None
    ins = lambda rid: make_write_op("insert", spec, "c1", rid, lim, [1, 1, 1, 1])  # noqa: E731
    (a,) = await dep.backend.write([ins("i1")])
    (b,) = await dep.backend.write([ins("i2")])
    upd = make_write_op("update", spec, "nope", "u1", lim, metadata={"x": 1})
    (c,) = await dep.backend.write([upd])
    assert (a.status, b.status, c.status) == ("ok", "already_exists", "not_found")  # type: ignore[union-attr]


# ───────────────────────────── visibilidad ─────────────────────────────


async def test_lagging_backup_shows_stale_window_and_ryw_hides_it(dep: RayDeployment) -> None:
    smap = await dep.backend.shard_map()
    info = smap.shard(0)
    lagging = info.replicas[2]
    await handle(dep, lagging).debug_set_delay.remote(1500, "async", ["replicate"])
    (vid,) = ids_in_shard(0, 1, prefix="lag")
    (res,) = await dep.backend.write([upsert(vid, [0, 0, 0, 0])])  # W=2: primario + el otro backup
    assert isinstance(res, WriteResult)

    req = ShardSearchRequest("q", 0, "demo", np.zeros(DIM, np.float32), 5, None, min_seq=res.seq)
    direct = await dep.transport.search(lagging, req, 1.0)
    assert direct.status == "stale" and direct.applied_seq < res.seq  # existe la ventana
    eventual = await dep.transport.search(
        lagging, ShardSearchRequest("q", 0, "demo", np.zeros(DIM, np.float32), 5), 1.0
    )
    assert vid not in [h.id for h in eventual.hits]  # `eventual` desde ese backup no la ve

    for _ in range(3):  # RYW con token la ve siempre, venga de la réplica que venga
        ryw = await dep.backend.search(search_req("read_your_writes", tokens={0: res.seq}))
        assert vid in [h.id for h in ryw.hits] and not ryw.partial
    strong = await dep.backend.search(search_req("strong"))
    assert vid in [h.id for h in strong.hits]

    await handle(dep, lagging).debug_clear.remote()
    deadline = time.monotonic() + 5
    while (await dep.transport.status(lagging, 1.0)).applied_seq < res.seq:
        assert time.monotonic() < deadline, "el backup no se puso al día"
        await asyncio.sleep(0.05)
    caught = await dep.transport.search(lagging, req, 1.0)
    assert caught.status == "ok" and vid in [h.id for h in caught.hits]


# ───────────────────────────── fallos (sin tolerancia: F1) ─────────────────────────────


@pytest.mark.parametrize("dep", [-1], indirect=True)
async def test_rebuilt_replica_is_not_ready_and_never_serves(dep: RayDeployment) -> None:
    smap = await dep.backend.shard_map()
    victim = smap.shard(1).replicas[1]
    await dep.backend.write([upsert(v, [1, 0, 0, 0]) for v in ids_in_shard(1, 5)])
    before = await dep.transport.status(victim, 1.0)
    os.kill(before.pid, 9)
    deadline = time.monotonic() + 20
    while True:
        try:
            after = await dep.transport.status(victim, 1.0)
            if after.incarnation != before.incarnation:
                break
        except Exception:
            pass
        assert time.monotonic() < deadline, "Ray no reconstruyó la réplica"
        await asyncio.sleep(0.1)
    assert after.ready is False and after.applied_seq == 0  # volvió vacía: no lista
    req = ShardSearchRequest("q", 1, "demo", np.zeros(DIM, np.float32), 5)
    assert (await dep.transport.search(victim, req, 1.0)).status == "unserved"
    for _ in range(6):  # el round-robin pasa por la víctima, pero nunca da datos incompletos
        resp = await dep.backend.search(search_req("eventual", k=10))
        assert not resp.partial and len([h for h in resp.hits if h.id in ids_in_shard(1, 5)]) == 5


async def test_dead_primary_blocks_writes_but_backups_serve_reads(dep: RayDeployment) -> None:
    smap = await dep.backend.shard_map()
    info = smap.shard(1)
    ids = ids_in_shard(1, 3, prefix="d")
    await dep.backend.write([upsert(v, [1, 0, 0, 0]) for v in ids])
    await asyncio.sleep(0.3)  # deja que los avisos de commit lleguen a los backups
    ray.kill(handle(dep, info.primary), no_restart=True)
    await asyncio.sleep(0.5)
    (out,) = await dep.backend.write([upsert(ids[0], [2, 0, 0, 0])])  # mismo shard 1
    assert isinstance(out, Unavailable)  # sin primario no hay escrituras (no hay failover en F1)
    eventual = await dep.backend.search(search_req("eventual", k=10))
    assert eventual.shards[1].status == "ok"  # los backups siguen sirviendo lecturas
    strong = await dep.backend.search(search_req("strong", k=10))
    assert (1, "unreachable") in strong.missing  # `strong` necesita al primario
