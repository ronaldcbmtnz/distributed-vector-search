"""T104b / T105 — máquina de estados del shard y deduplicación."""

import numpy as np
import pytest

from dvs.core.errors import IdempotencyConflict, NotFound
from dvs.core.filters import parse
from dvs.core.model import CollectionSpec, Limits, LogEntry, WriteKind, WriteOp
from dvs.core.shard_core import ShardCore
from dvs.core.validation import make_write_op

SPEC = CollectionSpec("demo", 3, "l2")
LIM = Limits()


class Log:
    """Ayuda: construye entradas consecutivas y las aplica."""

    def __init__(self) -> None:
        self.core = ShardCore(0, [SPEC])
        self.seq = 0

    def op(self, kind: WriteKind, vid: str, rid: str, vec=None, meta=None) -> WriteOp:  # type: ignore[no-untyped-def]
        return make_write_op(kind, SPEC, vid, rid, LIM, vec, meta)

    def apply(self, op: WriteOp):  # type: ignore[no-untyped-def]
        self.seq += 1
        return self.core.apply(LogEntry(self.seq, 0, op))


def test_upsert_creates_then_replaces() -> None:
    log = Log()
    r1 = log.apply(log.op("upsert", "a", "r1", [1, 0, 0], {"v": 1}))
    r2 = log.apply(log.op("upsert", "a", "r2", [0, 1, 0], {"v": 2}))
    assert (r1.status, r1.created, r1.seq) == ("ok", True, 1)
    assert (r2.status, r2.created, r2.seq) == ("ok", False, 2)
    item = log.core.get("demo", "a")
    assert item is not None and item.vector.tolist() == [0, 1, 0] and item.metadata == {"v": 2}
    assert item.version == 2 and log.core.applied_seq == 2


def test_insert_conditional() -> None:
    log = Log()
    assert log.apply(log.op("insert", "a", "r1", [1, 0, 0])).status == "ok"
    r = log.apply(log.op("insert", "a", "r2", [9, 9, 9], {"x": 1}))
    assert r.status == "already_exists"
    item = log.core.get("demo", "a")
    assert item is not None and item.vector.tolist() == [1, 0, 0]  # sin cambios
    assert log.core.applied_seq == 2  # la entrada fallida también consume posición


def test_update_conditional_and_partial() -> None:
    log = Log()
    assert log.apply(log.op("update", "a", "r1", meta={"x": 1})).status == "not_found"
    log.apply(log.op("upsert", "a", "r2", [1, 2, 3], {"x": 1, "y": 2}))
    assert log.apply(log.op("update", "a", "r3", meta={"z": 3})).status == "ok"
    item = log.core.get("demo", "a")
    assert item is not None and item.vector.tolist() == [1, 2, 3]
    assert item.metadata == {"z": 3}  # PATCH reemplaza los metadatos completos
    log.apply(log.op("update", "a", "r4", vec=[0, 0, 1]))
    item = log.core.get("demo", "a")
    assert item is not None and item.vector.tolist() == [0, 0, 1] and item.metadata == {"z": 3}


def test_delete_then_not_visible_then_reinsert() -> None:
    log = Log()
    log.apply(log.op("upsert", "a", "r1", [1, 0, 0]))
    r = log.apply(log.op("delete", "a", "r2"))
    assert (r.status, r.existed) == ("ok", True)
    assert log.core.get("demo", "a") is None
    assert log.core.search("demo", np.zeros(3, np.float32), 5, None) == []
    assert log.apply(log.op("delete", "a", "r3")).existed is False
    assert log.apply(log.op("insert", "a", "r4", [0, 1, 0])).status == "ok"


def test_search_with_filter_and_k() -> None:
    log = Log()
    for i in range(10):
        log.apply(log.op("upsert", f"v{i}", f"r{i}", [i, 0, 0], {"even": i % 2 == 0}))
    q = np.zeros(3, np.float32)
    hits = log.core.search("demo", q, 3, parse({"even": True}))
    assert [h.id for h in hits] == ["v0", "v2", "v4"]
    assert all(h.metadata["even"] is True for h in hits)
    assert len(log.core.search("demo", q, 100, None)) == 10  # menos de k: todos, sin error


def test_vector_and_metadata_always_from_same_version() -> None:
    log = Log()
    for ver in range(1, 30):
        log.apply(log.op("upsert", "a", f"r{ver}", [ver, 0, 0], {"ver": ver}))
        (hit,) = log.core.search("demo", np.zeros(3, np.float32), 1, None)
        assert hit.dist == pytest.approx(ver) and hit.metadata["ver"] == ver


def test_out_of_order_apply_is_rejected() -> None:
    log = Log()
    op = log.op("upsert", "a", "r1", [1, 0, 0])
    with pytest.raises(ValueError):
        log.core.apply(LogEntry(2, 0, op))


def test_unknown_collection() -> None:
    with pytest.raises(NotFound):
        ShardCore(0, [SPEC]).search("nope", np.zeros(3, np.float32), 1, None)


# ───────────────────────────── T105: deduplicación ─────────────────────────────


def test_dedup_lookup_returns_original_result() -> None:
    log = Log()
    op = log.op("upsert", "a", "r1", [1, 0, 0])
    first = log.apply(op)
    assert log.core.lookup_dedup("r1", op.fingerprint) == first
    assert log.core.lookup_dedup("unknown", op.fingerprint) is None


def test_dedup_conflict_on_different_payload() -> None:
    log = Log()
    log.apply(log.op("upsert", "a", "r1", [1, 0, 0]))
    other = log.op("upsert", "a", "r1", [2, 0, 0])
    with pytest.raises(IdempotencyConflict):
        log.core.lookup_dedup("r1", other.fingerprint)


def test_duplicate_in_log_is_a_deterministic_noop() -> None:
    """Escenario del contraejemplo v1/v2: un reintento tardío de v1 no pisa a v2."""
    log = Log()
    v1 = log.op("upsert", "a", "r1", [1, 0, 0], {"v": 1})
    first = log.apply(v1)
    log.apply(log.op("upsert", "a", "r2", [2, 0, 0], {"v": 2}))
    again = log.apply(v1)  # el reintento de v1 llega tarde
    assert again == first
    item = log.core.get("demo", "a")
    assert item is not None and item.metadata == {"v": 2}  # v2 sigue vigente
    assert log.core.applied_seq == 3


def test_duplicate_in_log_with_other_payload_is_conflict_noop() -> None:
    log = Log()
    log.apply(log.op("upsert", "a", "r1", [1, 0, 0]))
    r = log.apply(log.op("upsert", "b", "r1", [5, 5, 5]))
    assert r.status == "idempotency_conflict"
    assert log.core.get("demo", "b") is None


def test_failed_conditional_is_also_deduplicated() -> None:
    log = Log()
    log.apply(log.op("insert", "a", "r1", [1, 0, 0]))
    op = log.op("insert", "a", "r2", [1, 0, 0])
    r = log.apply(op)
    assert r.status == "already_exists"
    assert log.core.lookup_dedup("r2", op.fingerprint) == r  # el reintento ve el mismo 409
