"""T108 — log de replicación: commit contiguo con quórum y backups que solo aplican prefijos."""

import random

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

from dvs.core.model import CollectionSpec, Limits, WriteResult
from dvs.core.replication import BackupLog, PrimaryLog
from dvs.core.shard_core import ShardCore
from dvs.core.validation import make_write_op

SPEC = CollectionSpec("demo", 2, "l2")


def _op(i: int):  # type: ignore[no-untyped-def]
    return make_write_op("upsert", SPEC, f"v{i % 5}", f"r{i}", Limits(), [i, 0], {"i": i})


# ───────────────────────────── PrimaryLog ─────────────────────────────


def test_commit_needs_quorum_counting_primary() -> None:
    p = PrimaryLog("p", quorum=2)
    e = p.append(_op(1))
    assert p.commit_seq == 0 and p.advance() == []
    assert p.ack(e.seq, "b1") == [e]
    assert p.commit_seq == 1 and p.pending_count == 0


def test_commit_is_contiguous() -> None:
    p = PrimaryLog("p", quorum=2)
    e1, e2, e3 = p.append(_op(1)), p.append(_op(2)), p.append(_op(3))
    assert p.ack(e3.seq, "b1") == []  # 3 tiene quórum pero 1 y 2 no
    assert p.ack(e2.seq, "b2") == []
    assert p.ack(e1.seq, "b2") == [e1, e2, e3]
    assert p.commit_seq == 3


def test_duplicate_and_self_acks_do_not_count() -> None:
    p = PrimaryLog("p", quorum=3)
    e = p.append(_op(1))
    assert p.ack(e.seq, "b1") == []
    assert p.ack(e.seq, "b1") == []  # el mismo backup dos veces no suma
    assert p.ack(e.seq, "p") == []  # el primario ya cuenta implícitamente
    assert p.ack(e.seq, "b2") == [e]


def test_quorum_one_commits_on_advance() -> None:
    p = PrimaryLog("p", quorum=1)
    e = p.append(_op(1))
    assert p.advance() == [e] and p.is_committed(1)


def test_late_acks_after_commit_are_ignored() -> None:
    p = PrimaryLog("p", quorum=2)
    e = p.append(_op(1))
    p.ack(e.seq, "b1")
    assert p.ack(e.seq, "b2") == [] and p.commit_seq == 1


# ───────────────────────────── BackupLog ─────────────────────────────


def test_backup_applies_only_contiguous_and_committed() -> None:
    p = PrimaryLog("p", quorum=1)
    entries = [p.append(_op(i)) for i in range(1, 6)]
    b = BackupLog()
    b.receive([entries[2], entries[0], entries[4]])  # 3, 1, 5 desordenados
    assert b.ready_to_apply() == []  # nada confirmado todavía
    b.advance_commit(5)
    assert [e.seq for e in b.ready_to_apply()] == [1]  # falta la 2: se detiene
    b.receive([entries[1], entries[1], entries[3]])  # 2 duplicada, 4
    assert [e.seq for e in b.ready_to_apply()] == [2, 3, 4, 5]
    assert b.applied_seq == 5 and b.buffered == 0


def test_commit_notice_before_entry_is_applied_on_receive() -> None:
    """Hallado por la simulación: el aviso de commit puede adelantar a la entrada."""
    p = PrimaryLog("p", quorum=1)
    e1 = p.append(_op(1))
    b = BackupLog()
    b.advance_commit(1)
    assert b.ready_to_apply() == []
    b.receive([e1])
    assert b.ready_to_apply() == [e1]


def test_holds_reports_applied_and_buffered() -> None:
    p = PrimaryLog("p", quorum=1)
    e1, e2, e3 = p.append(_op(1)), p.append(_op(2)), p.append(_op(3))
    b = BackupLog()
    b.receive([e1, e3], commit_seq=1)
    b.ready_to_apply()
    assert b.holds(1) and b.holds(3) and not b.holds(2)
    assert e2.seq == 2


def test_receive_can_carry_commit_seq() -> None:
    p = PrimaryLog("p", quorum=1)
    e1, e2 = p.append(_op(1)), p.append(_op(2))
    b = BackupLog()
    b.receive([e1, e2], commit_seq=1)
    assert b.ready_to_apply() == [e1] and b.commit_seq == 1


def test_backup_commit_never_regresses_and_old_entries_ignored() -> None:
    p = PrimaryLog("p", quorum=1)
    e1, e2 = p.append(_op(1)), p.append(_op(2))
    b = BackupLog()
    b.receive([e1, e2])
    b.advance_commit(1)
    b.advance_commit(0)
    assert [e.seq for e in b.ready_to_apply()] == [1]
    assert b.receive([e1]) == [1] and b.buffered == 1  # e1 ya aplicada: no se re-encola


# ─────────────── simulación: entregas desordenadas, duplicadas y con retrasos ───────────────


@settings(max_examples=60, deadline=None)
@given(seed=st.integers(0, 10_000), n=st.integers(1, 25), quorum=st.integers(1, 3))
def test_replicas_always_apply_a_prefix_and_converge(seed: int, n: int, quorum: int) -> None:
    rng = random.Random(seed)
    primary_log = PrimaryLog("p", quorum=quorum)
    primary = ShardCore(0, [SPEC])
    backups = {name: (BackupLog(), ShardCore(0, [SPEC])) for name in ("b1", "b2")}
    committed: list[int] = []
    results: dict[int, WriteResult] = {}

    # Mensajes en vuelo: ("rep", destino, entrada) / ("ack", origen, seq) / ("commit", destino, c)
    inflight: list[tuple[str, str, object]] = []
    for i in range(1, n + 1):
        e = primary_log.append(_op(i))
        for name in backups:
            inflight.append(("rep", name, e))
            if rng.random() < 0.3:
                inflight.append(("rep", name, e))  # entrega duplicada

    def commit(entries: list) -> None:  # type: ignore[type-arg]
        for e in entries:
            results[e.seq] = primary.apply(e)
            committed.append(e.seq)
        if entries:
            for name in backups:
                inflight.append(("commit", name, primary_log.commit_seq))

    commit(primary_log.advance())
    while inflight:
        kind, who, payload = inflight.pop(rng.randrange(len(inflight)))
        if kind == "rep":
            log, core = backups[who]
            for seq in log.receive([payload]):  # type: ignore[list-item]
                inflight.append(("ack", who, seq))
            for e in log.ready_to_apply():  # el commit pudo llegar antes que la entrada
                core.apply(e)
        elif kind == "ack":
            commit(primary_log.ack(payload, who))  # type: ignore[arg-type]
        else:
            log, core = backups[who]
            log.advance_commit(payload)  # type: ignore[arg-type]
            for e in log.ready_to_apply():
                core.apply(e)
        # Invariante en cada paso: cada backup aplicó un prefijo de lo confirmado
        for log, core in backups.values():
            assert core.applied_seq == log.applied_seq <= primary_log.commit_seq
            assert committed[: core.applied_seq] == list(range(1, core.applied_seq + 1))

    assert primary_log.commit_seq == n  # con todos los mensajes entregados, todo se confirma
    for _, core in backups.values():
        assert core.applied_seq == n
        for vid in (f"v{i}" for i in range(5)):
            a, b = primary.get("demo", vid), core.get("demo", vid)
            assert (a is None) == (b is None)
            if a is not None and b is not None:
                assert np.array_equal(a.vector, b.vector) and a.metadata == b.metadata
