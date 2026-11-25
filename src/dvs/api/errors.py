"""Contract error codes → HTTP status (docs/fase1_ray/01_contrato_api.md §4)."""

from __future__ import annotations

from typing import Any

from dvs.core.errors import AlreadyExists, DVSError, IdempotencyConflict, NotFound
from dvs.core.model import WriteResult

HTTP_STATUS: dict[str, int] = {
    "invalid_argument": 400,
    "unauthenticated": 401,
    "permission_denied": 403,
    "not_found": 404,
    "already_exists": 409,
    "idempotency_conflict": 409,
    "overloaded": 429,
    "unavailable": 503,
    "deadline_exceeded": 504,
    "internal": 500,
}


def status_for(e: DVSError) -> int:
    return HTTP_STATUS.get(e.code, 500)


def error_body(e: DVSError) -> dict[str, Any]:
    return {"error": {"code": e.code, "message": e.message, "details": e.details}}


def result_error(r: WriteResult) -> DVSError | None:
    """Deterministic non-ok write results become contract errors."""
    if r.status == "already_exists":
        return AlreadyExists("id already exists", id=r.id)
    if r.status == "not_found":
        return NotFound("id not found", id=r.id)
    if r.status == "idempotency_conflict":
        return IdempotencyConflict("request_id reused with a different payload", id=r.id)
    return None
