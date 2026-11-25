"""Errors of the observable contract.

Each error carries the contract `code` (docs/fase1_ray/01_contrato_api.md §4). The mapping to
HTTP status lives in the API layer, not here: the core does not know about HTTP.
"""

from __future__ import annotations

from typing import Any


class DVSError(Exception):
    """Base error. `code` is the contract error code; `details` is JSON-serializable context."""

    code: str = "internal"

    def __init__(self, message: str = "", **details: Any) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details: dict[str, Any] = details

    def __reduce__(self) -> tuple[Any, ...]:
        # Keep errors picklable across processes (Ray) with their details.
        return (_rebuild, (type(self), self.message, self.details))


def _rebuild(cls: type[DVSError], message: str, details: dict[str, Any]) -> DVSError:
    return cls(message, **details)


class InvalidArgument(DVSError):
    """Malformed input. Never retried, never reaches the log (poison pill)."""

    code = "invalid_argument"


class Unauthenticated(DVSError):
    code = "unauthenticated"


class PermissionDenied(DVSError):
    code = "permission_denied"


class NotFound(DVSError):
    code = "not_found"


class AlreadyExists(DVSError):
    code = "already_exists"


class IdempotencyConflict(DVSError):
    """A `request_id` was reused with a different payload."""

    code = "idempotency_conflict"


class Overloaded(DVSError):
    """Admission/queue limit reached. Retryable with backoff."""

    code = "overloaded"


class Unavailable(DVSError):
    """No replica able to serve (or no quorum). Retryable with the same request_id."""

    code = "unavailable"


class DeadlineExceeded(DVSError):
    """A write was not confirmed in time: outcome UNKNOWN. Retry with the same request_id."""

    code = "deadline_exceeded"
