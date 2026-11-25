"""ShardReplica: one replica of one shard, as an async Ray actor (phase 1 only).

Wraps the pure ShardCore + PrimaryLog/BackupLog. One event loop thread per actor: code between
two `await`s is atomic, so a search never observes a half-applied write (decision D12).

Roles (fixed in phase 1, no failover):
- primary: validates dedup, assigns `seq`, replicates to backups, commits with W copies
  (itself included), applies, then answers. Publishes `commit_seq` to the backups.
- backup: buffers entries by `seq` (Ray does not order calls to async actors) and applies only
  contiguous, committed entries.

A replica rebuilt by Ray (`max_restarts > 0`) comes back with EMPTY state; it marks itself
not ready and never serves reads, so the client is never silently given incomplete data.
Expected conditions are returned as explicit statuses, never raised (Ray wraps exceptions).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import ray

from dvs.core.errors import DeadlineExceeded, DVSError, IdempotencyConflict
from dvs.core.model import (
    CollectionSpec,
    GetResult,
    LogEntry,
    ReplicaStatus,
    Role,
    ShardSearchRequest,
    ShardSearchResult,
    WriteOp,
    WriteResult,
)
from dvs.core.replication import BackupLog, PrimaryLog
from dvs.core.shard_core import ShardCore

log = logging.getLogger("dvs.replica")

WriteOutcome = WriteResult | DVSError


@dataclass(frozen=True, slots=True)
class WriteBatchReply:
    status: Literal["ok", "not_primary", "not_ready"]
    outcomes: list[WriteOutcome] = field(default_factory=list)


@ray.remote
class ShardReplica:
    def __init__(
        self,
        replica_id: str,
        shard: int,
        collections: list[CollectionSpec],
        write_timeout_s: float,
        fault_injection: bool = False,
    ) -> None:
        ctx = ray.get_runtime_context()
        self.replica_id = replica_id
        self.shard = shard
        self.collections = collections
        self.write_timeout_s = write_timeout_s
        self.fault_injection = fault_injection
        self.reconstructed = bool(ctx.was_current_actor_reconstructed)
        self.ready = not self.reconstructed
        self.node_id = ctx.get_node_id()
        self.pid = os.getpid()
        self.incarnation = uuid.uuid4().hex[:12]
        self.core = ShardCore(shard, collections)
        self.role: Role | None = None
        self.peers: dict[str, Any] = {}
        self.plog: PrimaryLog | None = None
        self.blog = BackupLog()
        self._waiters: dict[int, asyncio.Future[WriteResult]] = {}
        self._inflight: dict[str, tuple[bytes, asyncio.Future[WriteResult]]] = {}
        self._delay_s = 0.0
        self._delay_mode: Literal["block", "async"] = "block"
        self._delay_ops: frozenset[str] = frozenset()
        # asyncio keeps only weak references to tasks: hold background replication tasks
        # until they finish, or they could be garbage-collected mid-flight.
        self._bg: set[asyncio.Task[None]] = set()
        self.out_of_order = 0  # entries received ahead of a gap (evidence for the dependency map)
        log.info(
            "replica started",
            extra={
                "event": "replica.started",
                "replica_id": replica_id,
                "shard": shard,
                "incarnation": self.incarnation,
                "reconstructed": self.reconstructed,
            },
        )

    # ───────────────────────────── control plane ─────────────────────────────

    async def configure(self, role: Role, quorum: int, peers: dict[str, Any]) -> ReplicaStatus:
        """Set the (fixed) role. Primary receives its backups' handles."""
        self.role = role
        if role == "primary":
            self.plog = PrimaryLog(self.replica_id, quorum, start_seq=self.core.applied_seq)
            self.peers = dict(peers)
        return await self.status()

    async def status(self) -> ReplicaStatus:
        commit = self.plog.commit_seq if self.plog else self.blog.commit_seq
        return ReplicaStatus(
            replica_id=self.replica_id,
            shard=self.shard,
            role=self.role or "backup",
            ready=self.ready and self.role is not None,
            node_id=self.node_id,
            pid=self.pid,
            incarnation=self.incarnation,
            applied_seq=self.core.applied_seq,
            commit_seq=commit,
            pending=self.plog.pending_count if self.plog else 0,
            buffered=self.blog.buffered,
        )

    # ───────────────────────────── write path (primary) ─────────────────────────────

    async def write(self, ops: Sequence[WriteOp]) -> WriteBatchReply:
        await self._maybe_delay("write")
        if self.role != "primary" or self.plog is None:
            return WriteBatchReply("not_primary")
        if not self.ready:
            return WriteBatchReply("not_ready")
        loop = asyncio.get_running_loop()
        pending: list[asyncio.Future[WriteResult] | WriteOutcome] = []
        new_entries: list[LogEntry] = []
        # No `await` from here to the end of the loop: dedup check + seq assignment are atomic.
        for op in ops:
            try:
                prev = self.core.lookup_dedup(op.request_id, op.fingerprint)
            except IdempotencyConflict as e:
                pending.append(e)
                continue
            if prev is not None:
                pending.append(prev)
                continue
            inflight = self._inflight.get(op.request_id)
            if inflight is not None:
                fp, fut = inflight
                pending.append(fut if fp == op.fingerprint else IdempotencyConflict("reused"))
                continue
            entry = self.plog.append(op)
            fut = loop.create_future()
            self._waiters[entry.seq] = fut
            self._inflight[op.request_id] = (op.fingerprint, fut)
            new_entries.append(entry)
            pending.append(fut)

        if new_entries:
            for peer_id, handle in self.peers.items():
                task = asyncio.ensure_future(self._replicate_to(peer_id, handle, new_entries))
                self._bg.add(task)
                task.add_done_callback(self._bg.discard)
            self._on_commit(self.plog.advance())  # quorum == 1

        futures = [p for p in pending if isinstance(p, asyncio.Future)]
        if futures:
            await asyncio.wait(futures, timeout=self.write_timeout_s)
        outcomes: list[WriteOutcome] = []
        for p in pending:
            if isinstance(p, asyncio.Future):
                if p.done():
                    outcomes.append(p.result())
                else:  # stays pending: may still commit; a retry with the same request_id finds it
                    outcomes.append(DeadlineExceeded("quorum not reached in time; outcome unknown"))
            else:
                outcomes.append(p)
        return WriteBatchReply("ok", outcomes)

    async def _replicate_to(self, peer_id: str, handle: Any, entries: list[LogEntry]) -> None:
        assert self.plog is not None
        try:
            held: list[int] = await handle.replicate.remote(entries, self.plog.commit_seq)
        except Exception as e:
            log.warning(
                "replicate failed",
                extra={
                    "event": "write.replicate_failed",
                    "peer": peer_id,
                    "error": type(e).__name__,
                },
            )
            return
        for seq in held:
            self._on_commit(self.plog.ack(seq, peer_id))

    def _on_commit(self, entries: list[LogEntry]) -> None:
        if not entries:
            return
        for e in entries:
            result = self.core.apply(e)
            self._inflight.pop(e.op.request_id, None)
            fut = self._waiters.pop(e.seq, None)
            if fut is not None and not fut.done():
                fut.set_result(result)
        assert self.plog is not None
        for handle in self.peers.values():  # fire-and-forget commit notice
            handle.advance_commit.remote(self.plog.commit_seq)

    # ───────────────────────────── write path (backup) ─────────────────────────────

    async def replicate(self, entries: list[LogEntry], commit_seq: int) -> list[int]:
        await self._maybe_delay("replicate")
        if self.role != "backup" or not self.ready:
            return []  # a broken/empty replica does not count as a copy
        self.out_of_order += sum(1 for e in entries if e.seq > 1 and not self.blog.holds(e.seq - 1))
        held = self.blog.receive(entries, commit_seq)
        self._apply_ready()
        return held

    async def advance_commit(self, commit_seq: int) -> None:
        await self._maybe_delay("replicate")
        if self.role == "backup" and self.ready:
            self.blog.advance_commit(commit_seq)
            self._apply_ready()

    def _apply_ready(self) -> None:
        for e in self.blog.ready_to_apply():
            self.core.apply(e)

    # ───────────────────────────── reads ─────────────────────────────

    async def search(self, req: ShardSearchRequest) -> ShardSearchResult:
        await self._maybe_delay("search")
        start = time.monotonic()
        applied = self.core.applied_seq
        if not self.ready or self.role is None:
            return ShardSearchResult("unserved", self.shard, self.replica_id, applied)
        if applied < req.min_seq:  # never wait here: it would block the actor (deadlock)
            return ShardSearchResult("stale", self.shard, self.replica_id, applied)
        hits = self.core.search(req.collection, req.vector, req.k, req.filter)  # type: ignore[arg-type]
        return ShardSearchResult(
            "ok",
            self.shard,
            self.replica_id,
            applied,
            tuple(hits),
            (time.monotonic() - start) * 1000,
        )

    async def get(self, collection: str, vid: str, min_seq: int) -> GetResult:
        await self._maybe_delay("search")
        applied = self.core.applied_seq
        if not self.ready or self.role is None:
            return GetResult("unserved", self.shard, self.replica_id, applied)
        if applied < min_seq:
            return GetResult("stale", self.shard, self.replica_id, applied)
        item = self.core.get(collection, vid)
        status = "ok" if item is not None else "not_found"
        return GetResult(status, self.shard, self.replica_id, applied, item)

    # ───────────────────────────── fault injection (experiments) ─────────────────────────────

    async def debug_set_delay(
        self, ms: float, mode: Literal["block", "async"] = "block", ops: Sequence[str] = ()
    ) -> None:
        self._require_fault_injection()
        self._delay_s = ms / 1000.0
        self._delay_mode = mode
        self._delay_ops = frozenset(ops or ("search", "write", "replicate"))

    async def debug_clear(self) -> None:
        self._require_fault_injection()
        self._delay_s = 0.0

    async def debug_info(self) -> dict[str, Any]:
        st = await self.status()
        return {
            "replica_id": st.replica_id,
            "shard": st.shard,
            "role": st.role,
            "ready": st.ready,
            "node_id": st.node_id,
            "pid": st.pid,
            "incarnation": st.incarnation,
            "reconstructed": self.reconstructed,
            "applied_seq": st.applied_seq,
            "commit_seq": st.commit_seq,
            "out_of_order": self.out_of_order,
            "delay_ms": self._delay_s * 1000,
        }

    def _require_fault_injection(self) -> None:
        if not self.fault_injection:
            raise PermissionError("fault injection is disabled in the configuration")

    async def _maybe_delay(self, op: str) -> None:
        if self._delay_s <= 0 or op not in self._delay_ops:
            return
        if self._delay_mode == "block":
            time.sleep(self._delay_s)  # simulates a CPU-bound slow node: blocks the actor
        else:
            await asyncio.sleep(self._delay_s)
