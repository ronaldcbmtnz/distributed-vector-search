"""Errors raised by the public HTTP clients."""

from __future__ import annotations

from typing import Any


class ClientError(Exception):
    """Base error for failures communicating with the DVS HTTP API."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.details = details or {}
        self.request_id = request_id


class APIError(ClientError):
    """An HTTP error response defined by the observable API contract."""


class TransportError(ClientError):
    """A timeout or connection failure after the retry budget was exhausted.

    ``maybe_applied`` is False only when the request certainly never reached a gateway
    (connection refused/unreachable). Timeouts and failures after sending leave the outcome of a
    write UNKNOWN: it may have been applied, so it must be retried with the same request_id.
    """

    def __init__(
        self, message: str, *, timeout: bool = False, maybe_applied: bool | None = None
    ) -> None:
        super().__init__(message)
        self.timeout = timeout
        self.maybe_applied = timeout if maybe_applied is None else maybe_applied


class ProtocolError(ClientError):
    """The server returned a successful status with an invalid JSON response."""
