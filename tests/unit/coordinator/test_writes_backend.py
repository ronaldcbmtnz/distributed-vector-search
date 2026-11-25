"""T113a / T112c — camino de escritura y fachada Backend (get, status)."""

import numpy as np
import pytest

from dvs.coordinator.backend import CoordinatorBackend
from dvs.coordinator.transport import ReplicaOverloaded, ReplicaUnreachable
from dvs.core.errors import DeadlineExceeded, Overloaded, Unavailable
from dvs.core.hashing import shard_for
from dvs.core.model import CollectionSpec, Limits, StoredVector, WriteResult
from dvs.core.validation import make_write_op

from .fakes import FakeTransport, build_map

SPEC = CollectionSpec("demo", 2, "l2")


def backend(t: FakeTransport, write_timeout_s: float = 0.2) -> CoordinatorBackend:
    return CoordinatorBackend(
        t, attempt_s=0.03, write_timeout_s=write_timeout_s, suspect_ttl_s=5.0, map_ttl_s=60.0
    )


def op(vid: str, rid: str | None = None):  # type: ignore[no-untyped-def]
    return make_write_op("upsert", SPEC, vid, rid or f"req-{vid}", Limits(), [1, 2])


def ids_for_shards(num_shards: int = 3) -> dict[int, str]:
    found: dict[int, str] = {}
    i = 0
    while len(found) < num_shards:
        found.setdefault(shard_for("demo", f"v{i}", num_shards), f"v{i}")
        i += 1
    return found


# ───────────────────────────── escrituras ─────────────────────────────


async def test_writes_grouped_by_shard_and_returned_in_order() -> None:
    t = FakeTransport(build_map())
    ids = ids_for_shards()
    ops = [op(ids[2]), op(ids[0]), op(ids[1]), op(ids[2] + "-b")]
    out = await backend(t).write(ops)
    assert [o.id for o in out if isinstance(o, WriteResult)] == [o.id for o in ops]
    for shard, vid in ids.items():  # cada primario recibe solo lo suyo
        assert all(shard_for("demo", o.id, 3) == shard for o in t.write_log[f"s{shard}/r0"])
        assert vid in [o.id for o in t.write_log[f"s{shard}/r0"]]
    assert set(t.calls_to("write")) == {"s0/r0", "s1/r0", "s2/r0"}


async def test_not_primary_refreshes_map_and_retries_once() -> None:
    t = FakeTransport(build_map())
    b = backend(t)
    vid = ids_for_shards()[1]
    t.not_primary_once.add("s1/r0")
    out = await b.write([op(vid)])
    assert isinstance(out[0], WriteResult)
    assert t.calls_to("write") == ["s1/r0", "s1/r0"] and t.map_calls == 2


@pytest.mark.parametrize(
    ("error", "expected"),
    [(ReplicaUnreachable, Unavailable), (ReplicaOverloaded, Overloaded)],
)
async def test_transport_errors_map_to_contract_errors(error: type, expected: type) -> None:
    t = FakeTransport(build_map())
    vid = ids_for_shards()[0]
    t.write_error["s0/r0"] = error
    (out,) = await backend(t).write([op(vid)])
    assert isinstance(out, expected)


async def test_timeout_is_deadline_exceeded_outcome_unknown() -> None:
    t = FakeTransport(build_map())
    vid = ids_for_shards()[2]
    t.r("s2/r0").mode = "never"
    (out,) = await backend(t, write_timeout_s=0.05).write([op(vid)])
    assert isinstance(out, DeadlineExceeded)


async def test_one_shard_failing_does_not_affect_others() -> None:
    t = FakeTransport(build_map())
    ids = ids_for_shards()
    t.write_error["s1/r0"] = ReplicaUnreachable
    out = await backend(t).write([op(ids[0]), op(ids[1]), op(ids[2])])
    assert isinstance(out[0], WriteResult) and isinstance(out[2], WriteResult)
    assert isinstance(out[1], Unavailable)


# ───────────────────────────── lecturas por id ─────────────────────────────


def _item(vid: str, version: int) -> StoredVector:
    return StoredVector(vid, np.array([1, 2], np.float32), {"v": version}, version)


async def test_get_ok_and_not_found_are_terminal() -> None:
    t = FakeTransport(build_map())
    b = backend(t)
    vid = ids_for_shards()[0]
    for i in range(3):
        t.r(f"s0/r{i}").items[vid] = _item(vid, 1)
    got = await b.get("demo", vid, "eventual", 0, 0.2)
    assert got.status == "ok" and got.item is not None and got.item.version == 1
    missing = await b.get("demo", vid + "-nope", "eventual", 0, 0.2)
    assert missing.status == "not_found" and missing.item is None
    assert len(t.calls_to("get")) == 2  # not_found es terminal: no prueba otras réplicas


async def test_get_read_your_writes_goes_to_primary_when_stale() -> None:
    t = FakeTransport(build_map())
    b = backend(t)
    vid = ids_for_shards()[0]
    t.r("s0/r0").applied_seq, t.r("s0/r0").items[vid] = 9, _item(vid, 9)
    for i in (1, 2):
        t.r(f"s0/r{i}").applied_seq = 3
    b.router.read_order(t.smap.shard(0), "eventual")  # el round-robin empieza por un backup
    got = await b.get("demo", vid, "read_your_writes", 9, 0.2)
    assert got.status == "ok" and got.replica_id == "s0/r0"


async def test_get_with_no_replica_raises_unavailable() -> None:
    t = FakeTransport(build_map())
    vid = ids_for_shards()[0]
    for i in range(3):
        t.r(f"s0/r{i}").mode = "unreachable"
    with pytest.raises(Unavailable):
        await backend(t).get("demo", vid, "eventual", 0, 0.2)


async def test_status_reports_unreachable_replicas() -> None:
    t = FakeTransport(build_map())
    t.r("s2/r1").mode = "unreachable"
    health = await backend(t).status()
    assert len(health) == 9
    down = [h for h in health if h.status is None]
    assert [(h.replica_id, h.error) for h in down] == [("s2/r1", "ReplicaUnreachable")]
