"""Parallel fan-out to every shard with a global deadline, hedging and marked partials (D09).

Per shard, replicas are tried in the router's order:
- an attempt slower than `attempt_s` triggers ONE hedge to the next replica (first answer wins);
- `stale` (replica behind the client's token) redirects to the primary, without waiting;
- `unserved` (replica not ready) or a transport error fails over to the next replica;
- when the global deadline expires, pending attempts are cancelled and the shard is reported
  missing with the most informative reason seen (stale > unserved > unreachable).

Shards are queried in parallel, never sequentially: latency is the max, not the sum.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from dvs.coordinator.routing import ReplicaRouter
from dvs.coordinator.transport import (
    ReplicaOverloaded,
    ShardTransport,
    TransportError,
)
from dvs.coordinator.types import (
    MissingReason,
    SearchRequest,
    SearchResponse,
    ShardReport,
)
from dvs.core.errors import DVSError, Unavailable
from dvs.core.merge import merge_topk
from dvs.core.model import (
    Consistency,
    Hit,
    ShardInfo,
    ShardMap,
    ShardSearchRequest,
    ShardSearchResult,
)

log = logging.getLogger("dvs.coordinator")

#: Extra time over the global deadline to collect shard tasks that already gave up.
_GRACE_S = 0.05


class _ReadResult(Protocol):
    @property
    def status(self) -> str: ...

    @property
    def applied_seq(self) -> int: ...


@dataclass(slots=True)
class ShardRead[T]:
    """Outcome of reading one shard: a terminal result or a missing reason."""

    shard: int
    result: T | None
    replica: str | None
    reason: MissingReason | None
    latency_ms: float | None
    hedges: int = 0
    redirects: int = 0


async def read_one_shard[R: _ReadResult](
    info: ShardInfo,
    consistency: Consistency,
    deadline: float,
    call: Callable[[str, float], Awaitable[R]],
    router: ReplicaRouter,
    attempt_s: float,
    *,
    terminal: frozenset[str] = frozenset({"ok"}),
    max_hedges: int = 1,
    context: str = "",
) -> ShardRead[R]:
    """Try the replicas of one shard until a terminal answer or the deadline."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    order = deque(router.read_order(info, consistency))
    tried: set[str] = set()
    running: dict[asyncio.Task[R], str] = {}
    seen: set[MissingReason] = set()
    hedges = redirects = 0

    def launch(rid: str) -> None:
        tried.add(rid)
        if rid in order:
            order.remove(rid)
        task = asyncio.ensure_future(call(rid, max(0.0, deadline - loop.time())))
        running[task] = rid

    def launch_next() -> bool:
        while order:
            rid = order.popleft()
            if rid not in tried:
                launch(rid)
                return True
        return False

    def outcome(result: R | None, replica: str | None) -> ShardRead[R]:
        reason: MissingReason | None = None
        if result is None:
            reason = (
                "stale" if "stale" in seen else "unserved" if "unserved" in seen else "unreachable"
            )
        latency = (loop.time() - start) * 1000 if result is not None else None
        return ShardRead(info.shard, result, replica, reason, latency, hedges, redirects)

    launch_next()
    attempt_deadline = loop.time() + attempt_s
    try:
        while running:
            now = loop.time()
            if now >= deadline:
                break
            can_hedge = hedges < max_hedges and bool(order)
            wake = min(deadline, attempt_deadline) if can_hedge else deadline
            done, _ = await asyncio.wait(
                running, timeout=max(0.0, wake - now), return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                if loop.time() >= deadline:
                    break
                for rid in running.values():  # slow: suspicion only changes routing
                    router.suspicion.suspect(rid)
                if launch_next():
                    hedges += 1
                    log.info(
                        "hedge",
                        extra={"event": "search.hedge", "shard": info.shard, "ctx": context},
                    )
                attempt_deadline = loop.time() + attempt_s
                continue
            for task in done:
                rid = running.pop(task)
                try:
                    res = task.result()
                except TransportError as e:
                    if not isinstance(e, ReplicaOverloaded):
                        router.suspicion.suspect(rid)
                    seen.add("unreachable")
                    continue
                except DVSError:
                    seen.add("unserved")
                    continue
                if res.status in terminal:
                    router.suspicion.clear(rid)
                    return outcome(res, rid)
                if res.status == "stale":
                    seen.add("stale")
                    if info.primary not in tried:
                        launch(info.primary)
                        redirects += 1
                        log.info(
                            "stale redirect",
                            extra={"event": "search.stale_redirect", "shard": info.shard},
                        )
                else:
                    seen.add("unserved")
            if not running and not launch_next():
                break
        return outcome(None, None)
    finally:
        for task in running:
            task.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)


class ScatterGather:
    def __init__(
        self,
        transport: ShardTransport,
        router: ReplicaRouter,
        attempt_s: float,
        max_hedges: int = 1,
    ) -> None:
        self.transport = transport
        self.router = router
        self.attempt_s = attempt_s
        self.max_hedges = max_hedges

    async def search(self, smap: ShardMap, req: SearchRequest) -> SearchResponse:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + req.deadline_s
        tasks = [
            asyncio.create_task(self._one(info, req, deadline), name=f"shard-{info.shard}")
            for info in smap.shards
        ]
        done, pending = await asyncio.wait(tasks, timeout=req.deadline_s + _GRACE_S)
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        reads: list[ShardRead[ShardSearchResult]] = []
        for info, t in zip(smap.shards, tasks, strict=True):
            if t in done and not t.cancelled() and t.exception() is None:
                reads.append(t.result())
            else:
                reads.append(ShardRead(info.shard, None, None, "unreachable", None))

        hits_per_shard: list[tuple[Hit, ...]] = []
        reports: dict[int, ShardReport] = {}
        missing: list[tuple[int, MissingReason]] = []
        for r in reads:
            if r.result is not None:
                hits_per_shard.append(r.result.hits)
                reports[r.shard] = ShardReport(
                    r.shard, r.replica, "ok", r.latency_ms, r.result.applied_seq
                )
            else:
                assert r.reason is not None
                missing.append((r.shard, r.reason))
                reports[r.shard] = ShardReport(r.shard, None, r.reason)

        if missing and not req.allow_partial:
            raise Unavailable(
                "some shards did not answer in time",
                missing_shards=[{"shard": s, "reason": why} for s, why in missing],
            )
        n = smap.num_shards
        log.info(
            "search done",
            extra={
                "event": "search.done",
                "query_id": req.query_id,
                "partial": bool(missing),
                "coverage": (n - len(missing)) / n if n else 1.0,
            },
        )
        return SearchResponse(
            query_id=req.query_id,
            hits=tuple(merge_topk(hits_per_shard, req.k)),
            partial=bool(missing),
            coverage=(n - len(missing)) / n if n else 1.0,
            missing=tuple(missing),
            consistency=req.consistency,
            shards=reports,
            hedges=sum(r.hedges for r in reads),
            redirects=sum(r.redirects for r in reads),
        )

    async def _one(
        self, info: ShardInfo, req: SearchRequest, deadline: float
    ) -> ShardRead[ShardSearchResult]:
        min_seq = req.tokens.get(info.shard, 0) if req.consistency == "read_your_writes" else 0
        sreq = ShardSearchRequest(
            query_id=req.query_id,
            shard=info.shard,
            collection=req.collection,
            vector=req.vector,
            k=req.k,
            filter=req.filter,
            min_seq=min_seq,
        )

        def call(rid: str, timeout_s: float) -> Awaitable[ShardSearchResult]:
            return self.transport.search(rid, sreq, timeout_s)

        return await read_one_shard(
            info,
            req.consistency,
            deadline,
            call,
            self.router,
            self.attempt_s,
            max_hedges=self.max_hedges,
            context=req.query_id,
        )
