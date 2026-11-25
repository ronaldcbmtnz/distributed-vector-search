"""Per-shard replication log bookkeeping, without any transport (phase 1 primary-backup).

PrimaryLog
    Assigns consecutive `seq`, collects acks and commits CONTIGUOUSLY: entry n is committed only
    when it has `quorum` copies (the primary counts as one) AND every entry < n is committed.

BackupLog
    Buffers entries by `seq` (they may arrive out of order or duplicated: Ray does not order
    calls to async actors) and releases them for application only contiguously and up to the
    known `commit_seq`.

Invariant: every replica applies a PREFIX of the committed history (contract G5 / REQ-4.1).
Phase 2 replaces this module with Raft; the invariant stays the same.
"""

from __future__ import annotations

from collections.abc import Iterable

from dvs.core.model import LogEntry, WriteOp


class PrimaryLog:
    def __init__(self, replica_id: str, quorum: int, *, epoch: int = 0, start_seq: int = 0) -> None:
        if quorum < 1:
            raise ValueError("quorum must be >= 1")
        self.replica_id = replica_id
        self.quorum = quorum
        self.epoch = epoch
        self.last_seq = start_seq
        self.commit_seq = start_seq
        self._pending: dict[int, LogEntry] = {}
        self._acks: dict[int, set[str]] = {}

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def append(self, op: WriteOp) -> LogEntry:
        self.last_seq += 1
        entry = LogEntry(self.last_seq, self.epoch, op)
        self._pending[entry.seq] = entry
        self._acks[entry.seq] = set()
        return entry

    def ack(self, seq: int, replica_id: str) -> list[LogEntry]:
        """Record that `replica_id` holds `seq`. Returns the entries that became committed."""
        acks = self._acks.get(seq)
        if acks is not None and replica_id != self.replica_id:
            acks.add(replica_id)
        return self.advance()

    def advance(self) -> list[LogEntry]:
        """Commit every contiguous entry that already has a quorum (needed when quorum == 1)."""
        newly: list[LogEntry] = []
        nxt = self.commit_seq + 1
        while nxt in self._pending and 1 + len(self._acks[nxt]) >= self.quorum:
            newly.append(self._pending.pop(nxt))
            del self._acks[nxt]
            self.commit_seq = nxt
            nxt += 1
        return newly

    def is_committed(self, seq: int) -> bool:
        return seq <= self.commit_seq


class BackupLog:
    def __init__(self, *, start_seq: int = 0) -> None:
        self.applied_seq = start_seq
        self.commit_seq = start_seq
        self._buffer: dict[int, LogEntry] = {}

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    def receive(self, entries: Iterable[LogEntry], commit_seq: int | None = None) -> list[int]:
        """Store entries (duplicates/old ones ignored). Returns the seqs it now holds (to ack).

        The commit notice may arrive BEFORE the entry it refers to, so callers must run
        `ready_to_apply()` after `receive()` as well as after `advance_commit()`.
        """
        held: list[int] = []
        for e in entries:
            if e.seq > self.applied_seq:
                self._buffer.setdefault(e.seq, e)
            held.append(e.seq)  # an old/duplicate entry is still held (already applied or buffered)
        if commit_seq is not None:
            self.advance_commit(commit_seq)
        return held

    def advance_commit(self, commit_seq: int) -> None:
        if commit_seq > self.commit_seq:
            self.commit_seq = commit_seq

    def ready_to_apply(self) -> list[LogEntry]:
        """Pop the entries that can be applied now: contiguous and <= commit_seq."""
        out: list[LogEntry] = []
        nxt = self.applied_seq + 1
        while nxt <= self.commit_seq and nxt in self._buffer:
            out.append(self._buffer.pop(nxt))
            self.applied_seq = nxt
            nxt += 1
        return out
