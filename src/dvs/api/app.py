"""External REST API (contract: docs/fase1_ray/01_contrato_api.md). Backend-agnostic.

Every handler follows the same order:
authenticate → authorize per collection → rate limit → validate (stateless) → admission gate →
backend call → map outcome to the contract.
The backend may be attached after startup (`app.state.backend`); until then /v1/health is 503.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from dvs.api.admission import Gate, RateLimiter
from dvs.api.auth import Action, Authenticator, Principal
from dvs.api.errors import error_body, result_error, status_for
from dvs.api.schemas import BatchBody, InsertBody, SearchBody, UpdateBody, UpsertBody
from dvs.config import Config
from dvs.coordinator.backend import Backend
from dvs.coordinator.transport import WriteOutcome
from dvs.coordinator.types import SearchRequest, SearchResponse
from dvs.core.errors import DVSError, InvalidArgument, NotFound, Overloaded, Unavailable
from dvs.core.filters import parse as parse_filter
from dvs.core.model import CollectionSpec, Consistency, WriteKind, WriteResult
from dvs.core.validation import (
    make_write_op,
    validate_batch_size,
    validate_deadline_ms,
    validate_k,
    validate_vector,
)
from dvs.obs import Metrics

log = logging.getLogger("dvs.api")

_CONSISTENCY: tuple[Consistency, ...] = ("eventual", "read_your_writes", "strong")

Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_app(
    config: Config,
    backend: Backend | None = None,
    *,
    metrics: Metrics | None = None,
    lifespan: Lifespan | None = None,
) -> FastAPI:
    app = FastAPI(title="Distributed vector search", version="1", lifespan=lifespan)
    app.state.backend = backend
    app.state.metrics = metrics or Metrics()
    auth = Authenticator(config)
    adm = config.admission
    search_gate = Gate("search", adm.search_concurrency)
    write_gate = Gate("write", adm.write_concurrency)
    limiter = RateLimiter(adm.rate_per_key, adm.burst_per_key)
    limits = config.limits.to_core()
    t = config.timeouts_ms

    # ───────────────────────────── helpers ─────────────────────────────

    def get_backend(request: Request) -> Backend:
        b: Backend | None = request.app.state.backend
        if b is None:
            raise Unavailable("service is starting")
        return b

    def m() -> Metrics:
        metrics_obj: Metrics = app.state.metrics
        return metrics_obj

    def principal(request: Request, collection: str | None, action: Action | None) -> Principal:
        p = auth.authenticate(request.headers.get("authorization"))
        if collection is not None and action is not None:
            if config.collection(collection) is None:
                raise NotFound("unknown collection", collection=collection)
            auth.authorize(p, collection, action)
        if not limiter.allow(p.client):
            m().inc("rate_limited")
            raise Overloaded("rate limit exceeded", client=p.client)
        return p

    def collection_spec(name: str) -> CollectionSpec:
        spec = config.collection(name)
        if spec is None:
            raise NotFound("unknown collection", collection=name)
        return spec

    def token(r: WriteResult) -> dict[str, int]:
        return {"shard": r.shard, "seq": r.seq}

    def unwrap(outcome: WriteOutcome) -> WriteResult:
        if isinstance(outcome, DVSError):
            raise outcome
        err = result_error(outcome)
        if err is not None:
            raise err
        return outcome

    async def write_one(
        request: Request,
        kind: WriteKind,
        collection: str,
        vid: Any,
        request_id: Any,
        vector: Any = None,
        metadata: Any = None,
    ) -> WriteResult:
        principal(request, collection, "write")
        op = make_write_op(
            kind, collection_spec(collection), vid, request_id, limits, vector, metadata
        )
        start = time.monotonic()
        with write_gate.enter():
            (outcome,) = await get_backend(request).write([op])
        m().observe("write", (time.monotonic() - start) * 1000)
        log.info(
            "write",
            extra={
                "event": "write.done",
                "request_id": op.request_id,
                "kind": kind,
                "collection": collection,
                "status": getattr(outcome, "status", getattr(outcome, "code", "?")),
            },
        )
        return unwrap(outcome)

    # ───────────────────────────── error handlers ─────────────────────────────

    @app.exception_handler(DVSError)
    async def _dvs_error(_: Request, e: DVSError) -> JSONResponse:
        if isinstance(e, Overloaded):
            m().inc("admission_rejected")
        return JSONResponse(error_body(e), status_code=status_for(e))

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, e: RequestValidationError) -> JSONResponse:
        errors = [{"loc": list(x.get("loc", [])), "msg": x.get("msg")} for x in e.errors()]
        err = InvalidArgument("malformed request body", errors=errors)
        return JSONResponse(error_body(err), status_code=400)

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, e: Exception) -> JSONResponse:
        log.exception("unexpected error", extra={"event": "api.internal_error"})
        err = DVSError("internal error")
        return JSONResponse(error_body(err), status_code=500)

    # ───────────────────────────── operación ─────────────────────────────

    @app.get("/v1/health")
    async def health(request: Request) -> dict[str, Any]:
        smap = await get_backend(request).shard_map()
        return {"status": "ok", "shards": smap.num_shards, "map_version": smap.version}

    @app.get("/v1/metrics")
    async def metrics_endpoint(request: Request) -> dict[str, Any]:
        principal(request, None, None)
        return m().snapshot(await replication_lag(get_backend(request)))

    @app.get("/v1/admin/status")
    async def admin_status(request: Request) -> dict[str, Any]:
        auth.require_admin(principal(request, None, None))
        backend = get_backend(request)
        smap = await backend.shard_map()
        replicas: list[dict[str, Any]] = []
        for h in await backend.status():
            if h.status is None:
                replicas.append({"replica_id": h.replica_id, "shard": h.shard, "error": h.error})
            else:
                s = h.status
                replicas.append(
                    {
                        "replica_id": s.replica_id,
                        "shard": s.shard,
                        "role": s.role,
                        "ready": s.ready,
                        "node_id": s.node_id,
                        "pid": s.pid,
                        "incarnation": s.incarnation,
                        "applied_seq": s.applied_seq,
                        "commit_seq": s.commit_seq,
                    }
                )
        return {"map_version": smap.version, "replicas": replicas}

    # ───────────────────────────── escrituras ─────────────────────────────

    @app.put("/v1/collections/{c}/vectors/{vid}")
    async def upsert(c: str, vid: str, body: UpsertBody, request: Request) -> dict[str, Any]:
        r = await write_one(request, "upsert", c, vid, body.request_id, body.vector, body.metadata)
        return {"id": r.id, "token": token(r), "created": bool(r.created)}

    @app.post("/v1/collections/{c}/vectors", status_code=201)
    async def insert(c: str, body: InsertBody, request: Request) -> dict[str, Any]:
        r = await write_one(
            request, "insert", c, body.id, body.request_id, body.vector, body.metadata
        )
        return {"id": r.id, "token": token(r)}

    @app.patch("/v1/collections/{c}/vectors/{vid}")
    async def update(c: str, vid: str, body: UpdateBody, request: Request) -> dict[str, Any]:
        r = await write_one(request, "update", c, vid, body.request_id, body.vector, body.metadata)
        return {"id": r.id, "token": token(r)}

    @app.delete("/v1/collections/{c}/vectors/{vid}")
    async def delete(c: str, vid: str, request: Request, request_id: str = "") -> dict[str, Any]:
        r = await write_one(request, "delete", c, vid, request_id or None)
        return {"id": r.id, "token": token(r), "existed": bool(r.existed)}

    @app.post("/v1/collections/{c}/vectors:batch")
    async def batch(c: str, body: BatchBody, request: Request) -> dict[str, Any]:
        principal(request, c, "write")
        spec = collection_spec(c)
        validate_batch_size(len(body.items), limits)
        results: list[dict[str, Any] | None] = [None] * len(body.items)
        ops, positions = [], []
        for i, it in enumerate(body.items):
            try:
                ops.append(
                    make_write_op(
                        "upsert", spec, it.id, it.request_id, limits, it.vector, it.metadata
                    )
                )
                positions.append(i)
            except DVSError as e:
                results[i] = {"id": it.id, "ok": False, "error": error_body(e)["error"]}
        if ops:
            with write_gate.enter():
                outcomes = await get_backend(request).write(ops)
            for i, op, outcome in zip(positions, ops, outcomes, strict=True):
                try:
                    r = unwrap(outcome)
                    results[i] = {"id": op.id, "ok": True, "token": token(r)}
                except DVSError as e:
                    results[i] = {"id": op.id, "ok": False, "error": error_body(e)["error"]}
        return {"results": results}

    # ───────────────────────────── lecturas ─────────────────────────────

    @app.get("/v1/collections/{c}/vectors/{vid}")
    async def get_vector(
        c: str, vid: str, request: Request, consistency: str = "eventual", min_seq: int = 0
    ) -> dict[str, Any]:
        principal(request, c, "read")
        collection_spec(c)
        if consistency not in _CONSISTENCY or min_seq < 0:
            raise InvalidArgument("invalid consistency or min_seq", allowed=list(_CONSISTENCY))
        start = time.monotonic()
        res = await get_backend(request).get(
            c,
            vid,
            consistency,
            min_seq,
            t.search_deadline_default / 1000,  # type: ignore[arg-type]
        )
        m().observe("get", (time.monotonic() - start) * 1000)
        if res.status != "ok" or res.item is None:
            raise NotFound("id not found", id=vid)
        item = res.item
        return {
            "id": item.id,
            "vector": item.vector.tolist(),
            "metadata": item.metadata,
            "seq": item.version,
            "applied_seq": res.applied_seq,
        }

    @app.post("/v1/collections/{c}/search")
    async def search(c: str, body: SearchBody, request: Request) -> dict[str, Any]:
        principal(request, c, "read")
        spec = collection_spec(c)
        if body.consistency not in _CONSISTENCY:
            raise InvalidArgument("invalid consistency", allowed=list(_CONSISTENCY))
        deadline_ms = (
            t.search_deadline_default
            if body.deadline_ms is None
            else validate_deadline_ms(
                body.deadline_ms, t.search_deadline_min, t.search_deadline_max
            )
        )
        req = SearchRequest(
            query_id=f"q-{uuid.uuid4().hex[:12]}",
            collection=c,
            vector=validate_vector(body.vector, spec),
            k=validate_k(body.k, limits),
            filter=parse_filter(
                body.filter,
                max_clauses=limits.max_filter_clauses,
                max_in_values=limits.max_in_values,
                max_key_len=limits.max_key_len,
            ),
            consistency=body.consistency,  # type: ignore[arg-type]
            deadline_s=deadline_ms / 1000,
            allow_partial=body.allow_partial,
            tokens={tk.shard: max(tk.seq, 0) for tk in body.tokens},
        )
        start = time.monotonic()
        with search_gate.enter():
            resp = await get_backend(request).search(req)
        record_search(resp, (time.monotonic() - start) * 1000)
        return search_body(resp)

    def record_search(resp: SearchResponse, ms: float) -> None:
        metrics_obj = m()
        metrics_obj.observe("search", ms)
        for s, rep in resp.shards.items():
            if rep.latency_ms is not None:
                metrics_obj.observe_shard(s, rep.latency_ms)
        if resp.partial:
            metrics_obj.inc("partial")
        metrics_obj.inc("hedges", resp.hedges)
        metrics_obj.inc("stale_redirects", resp.redirects)

    return app


def search_body(resp: SearchResponse) -> dict[str, Any]:
    return {
        "query_id": resp.query_id,
        "results": [{"id": h.id, "distance": h.dist, "metadata": h.metadata} for h in resp.hits],
        "partial": resp.partial,
        "coverage": resp.coverage,
        "missing_shards": [{"shard": s, "reason": why} for s, why in resp.missing],
        "consistency": resp.consistency,
        "shards": {
            str(s): {
                "replica": r.replica,
                "status": r.status,
                "latency_ms": None if r.latency_ms is None else round(r.latency_ms, 3),
                "applied_seq": r.applied_seq,
            }
            for s, r in sorted(resp.shards.items())
        },
    }


async def replication_lag(backend: Backend) -> dict[str, int]:
    """commit_seq of each shard's primary minus applied_seq of every replica of that shard."""
    statuses = [h.status for h in await backend.status() if h.status is not None]
    commit = {s.shard: s.commit_seq for s in statuses if s.role == "primary"}
    return {
        s.replica_id: max(0, commit.get(s.shard, s.applied_seq) - s.applied_seq) for s in statuses
    }
