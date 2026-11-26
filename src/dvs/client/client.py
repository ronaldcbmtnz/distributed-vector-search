"""Synchronous and asynchronous clients for the versioned DVS HTTP contract."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from dvs.client.errors import APIError, ClientError, ProtocolError, TransportError
from dvs.client.history import HistoryWriter
from dvs.client.retry import RetryPolicy

_LOG = logging.getLogger("dvs.client")
_RETRYABLE_STATUSES = {429, 503, 504}
_CONSISTENCIES = {"eventual", "read_your_writes", "strong"}
_UNSET = object()


@dataclass(frozen=True, slots=True)
class _Request:
    method: str
    path: str
    operation: str
    collection: str | None = None
    item_id: str | None = None
    request_id: str | None = None
    body: dict[str, Any] | None = None
    params: dict[str, Any] | None = None


def _base_urls(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        value = os.environ.get("DVS_BASE_URL", "http://127.0.0.1:8080")
    raw_urls = value.split(",") if isinstance(value, str) else value
    urls: list[str] = []
    for raw_url in raw_urls:
        url = str(raw_url).strip().rstrip("/")
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"invalid DVS base URL: {url!r}")
        if parsed.query or parsed.fragment:
            raise ValueError("DVS base URLs must not contain a query or fragment")
        urls.append(url)
    if not urls:
        raise ValueError("at least one DVS base URL is required")
    return tuple(urls)


def _validate_collection(collection: str) -> str:
    if not isinstance(collection, str) or not collection:
        raise ValueError("collection must be a non-empty string")
    return quote(collection, safe="")


def _validate_id(item_id: str) -> str:
    if not isinstance(item_id, str) or not item_id:
        raise ValueError("id must be a non-empty string")
    if len(item_id.encode("utf-8")) > 128:
        raise ValueError("id must be at most 128 bytes when encoded as UTF-8")
    return quote(item_id, safe="")


def _request_id(value: str | None) -> str:
    request_id = str(uuid.uuid4()) if value is None else value
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("request_id must be a non-empty string")
    if len(request_id.encode("utf-8")) > 128:
        raise ValueError("request_id must be at most 128 bytes when encoded as UTF-8")
    return request_id


def _validate_vector(vector: Sequence[float]) -> list[float | int]:
    if isinstance(vector, (str, bytes)) or not isinstance(vector, Sequence) or not vector:
        raise ValueError("vector must be a non-empty sequence of finite numbers")
    values: list[float | int] = []
    for value in vector:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("vector values must be finite numbers")
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError("vector values must be finite numbers")
        values.append(value)
    return values


def _validate_consistency(consistency: str) -> None:
    if consistency not in _CONSISTENCIES:
        raise ValueError(f"consistency must be one of {sorted(_CONSISTENCIES)}")


def _validate_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata must be a mapping")
    values = dict(metadata)

    def valid_scalar(item: Any) -> bool:
        if isinstance(item, (str, bool)):
            return True
        if isinstance(item, (int, float)):
            try:
                return math.isfinite(float(item))
            except OverflowError:
                return False
        return False

    for key, value in values.items():
        if not isinstance(key, str) or not key or key.startswith("$"):
            raise ValueError("metadata keys must be non-empty strings not starting with '$'")
        items = value if isinstance(value, list) else [value]
        if not all(valid_scalar(item) for item in items):
            raise ValueError("metadata values must be finite scalars or lists of finite scalars")
    return values


class _SharedClient:
    def __init__(
        self,
        *,
        base_url: str | Sequence[str] | None,
        api_key: str | None,
        timeout: float,
        retry_policy: RetryPolicy | None,
        max_retries: int | None,
        history_path: str | None,
        client_id: str | None,
        random_source: Callable[[], float] | None,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        if retry_policy is not None and max_retries is not None:
            raise ValueError("provide retry_policy or max_retries, not both")
        self.base_urls = _base_urls(base_url)
        self.api_key = api_key if api_key is not None else os.environ.get("DVS_API_KEY")
        self.timeout = timeout
        self.retry_policy = retry_policy or RetryPolicy(
            max_retries=3 if max_retries is None else max_retries
        )
        self.client_id = client_id or os.environ.get("DVS_CLIENT_ID") or str(uuid.uuid4())
        self._history = HistoryWriter(history_path or os.environ.get("DVS_HISTORY_PATH"))
        self._random = random_source or random.random
        self._url_lock = threading.Lock()
        self._url_cursor = 0
        self._token_lock = threading.Lock()
        self._tokens: dict[int, int] = {}
        self._tokens_by_id: dict[tuple[str, str], dict[str, int]] = {}

    def _next_base_url(self) -> str:
        with self._url_lock:
            url = self.base_urls[self._url_cursor % len(self.base_urls)]
            self._url_cursor += 1
            return url

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    @staticmethod
    def _parse_response(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProtocolError("server returned a non-JSON response") from exc
        if not isinstance(payload, dict):
            raise ProtocolError("server response must be a JSON object")
        return payload

    def _error_from_response(self, response: httpx.Response) -> APIError:
        try:
            payload = response.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {}
        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        if not isinstance(error, dict):
            error = {}
        code = error.get("code")
        message = error.get("message")
        details = error.get("details")
        return APIError(
            message
            if isinstance(message, str)
            else f"DVS API returned HTTP {response.status_code}",
            status_code=response.status_code,
            error_code=code if isinstance(code, str) else None,
            details=details if isinstance(details, dict) else {},
        )

    def _remember_token(
        self, collection: str | None, item_id: str | None, value: Any
    ) -> dict[str, int]:
        if not isinstance(value, dict):
            raise ProtocolError("write response is missing its token")
        shard, seq = value.get("shard"), value.get("seq")
        if (
            isinstance(shard, bool)
            or not isinstance(shard, int)
            or shard < 0
            or isinstance(seq, bool)
            or not isinstance(seq, int)
            or seq < 0
        ):
            raise ProtocolError("write response contains an invalid token")
        token = {"shard": shard, "seq": seq}
        with self._token_lock:
            self._tokens[shard] = max(seq, self._tokens.get(shard, 0))
            if collection is not None and item_id is not None:
                self._tokens_by_id[(collection, item_id)] = token
        return token

    def _remember_response_tokens(self, request: _Request, payload: dict[str, Any]) -> None:
        if request.operation in {"upsert", "insert", "update", "delete"}:
            self._remember_token(request.collection, request.item_id, payload.get("token"))
        elif request.operation == "batch":
            results = payload.get("results")
            if not isinstance(results, list):
                raise ProtocolError("batch response is missing its results list")
            for result in results:
                if not isinstance(result, dict):
                    raise ProtocolError("batch response contains an invalid result")
                if result.get("ok") is True:
                    item_id = result.get("id")
                    if not isinstance(item_id, str):
                        raise ProtocolError("batch result is missing its id")
                    self._remember_token(request.collection, item_id, result.get("token"))

    def _tokens_for_search(
        self, consistency: str, tokens: Iterable[Mapping[str, int]] | None
    ) -> list[dict[str, int]]:
        collected: dict[int, int] = {}
        if tokens is not None and consistency != "read_your_writes":
            raise ValueError("tokens are only meaningful with read_your_writes consistency")
        if consistency == "read_your_writes":
            with self._token_lock:
                collected.update(self._tokens)
        if tokens is not None:
            for item in tokens:
                shard, seq = item.get("shard"), item.get("seq")
                if (
                    isinstance(shard, bool)
                    or not isinstance(shard, int)
                    or shard < 0
                    or isinstance(seq, bool)
                    or not isinstance(seq, int)
                    or seq < 0
                ):
                    raise ValueError("each token must contain non-negative integer shard and seq")
                collected[shard] = max(seq, collected.get(shard, 0))
        return [{"shard": shard, "seq": seq} for shard, seq in sorted(collected.items())]

    def _get_min_seq(
        self,
        collection: str,
        item_id: str,
        consistency: str,
        min_seq: int | None,
    ) -> int | None:
        _validate_consistency(consistency)
        if min_seq is not None and (
            isinstance(min_seq, bool) or not isinstance(min_seq, int) or min_seq < 0
        ):
            raise ValueError("min_seq must be a non-negative integer")
        if min_seq is not None and consistency != "read_your_writes":
            raise ValueError("min_seq is only meaningful with read_your_writes consistency")
        if min_seq is not None:
            return min_seq
        if consistency == "read_your_writes":
            with self._token_lock:
                token = self._tokens_by_id.get((collection, item_id))
                return token["seq"] if token else 0
        return None

    def _write_history(
        self,
        request: _Request,
        *,
        started_mono: float,
        started_wall: str,
        outcome: str,
        status_code: int | None,
        error_code: str | None,
        response: dict[str, Any] | None,
    ) -> None:
        response_mono = time.monotonic()
        records: list[dict[str, Any]] = []
        if request.operation == "batch":
            items = request.body.get("items", []) if request.body else []
            results = response.get("results", []) if response is not None else []
            if isinstance(items, list) and isinstance(results, list) and len(items) == len(results):
                item_results = list(zip(items, results, strict=True))
            elif isinstance(items, list):
                item_results = [(item, None) for item in items]
            else:
                item_results = []
            for item, result in item_results:
                if not isinstance(item, dict):
                    continue
                if result is not None and not isinstance(result, dict):
                    continue
                item_error = result.get("error", {}) if isinstance(result, dict) else {}
                item_token = result.get("token") if isinstance(result, dict) else None
                succeeded = isinstance(result, dict) and result.get("ok") is True
                records.append(
                    {
                        "client_id": self.client_id,
                        "op": "upsert",
                        "collection": request.collection,
                        "id": item.get("id"),
                        "request_id": item.get("request_id"),
                        "t_inv_mono": started_mono,
                        "t_resp_mono": response_mono,
                        "t_inv_wall": started_wall,
                        "outcome": "ok" if succeeded else outcome,
                        "http_status": status_code,
                        "error_code": (
                            item_error.get("code") if isinstance(item_error, dict) else error_code
                        ),
                        "token": item_token if isinstance(item_token, dict) else None,
                        "result_summary": {"batch_item_ok": succeeded},
                    }
                )
        if not records:
            token: dict[str, int] | None = None
            if response is not None:
                token_value = response.get("token")
                if isinstance(token_value, dict):
                    token = token_value
            records.append(
                {
                    "client_id": self.client_id,
                    "op": request.operation,
                    "collection": request.collection,
                    "id": request.item_id,
                    "request_id": request.request_id,
                    "t_inv_mono": started_mono,
                    "t_resp_mono": response_mono,
                    "t_inv_wall": started_wall,
                    "outcome": outcome,
                    "http_status": status_code,
                    "error_code": error_code,
                    "token": token,
                    "result_summary": self._summarize(request.operation, response),
                }
            )
        try:
            for record in records:
                self._history.write(record)
        except OSError:
            _LOG.exception("could not append client operation history")

    @staticmethod
    def _summarize(operation: str, response: dict[str, Any] | None) -> dict[str, Any] | None:
        if response is None:
            return None
        if operation == "search":
            results = response.get("results")
            return {
                "result_count": len(results) if isinstance(results, list) else None,
                "partial": response.get("partial"),
                "coverage": response.get("coverage"),
            }
        if operation == "batch":
            results = response.get("results")
            if isinstance(results, list):
                return {
                    "item_count": len(results),
                    "succeeded": sum(
                        isinstance(item, dict) and item.get("ok") is True for item in results
                    ),
                }
        if operation in {"upsert", "insert", "update", "delete"}:
            return {
                key: response[key]
                for key in ("id", "created", "existed", "token")
                if key in response
            }
        return {"response_fields": sorted(response)}

    @staticmethod
    def _spec(
        method: str,
        path: str,
        operation: str,
        *,
        collection: str | None = None,
        item_id: str | None = None,
        request_id: str | None = None,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> _Request:
        return _Request(method, path, operation, collection, item_id, request_id, body, params)

    def _upsert_request(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None,
        request_id: str | None,
    ) -> _Request:
        rid = _request_id(request_id)
        encoded_id = _validate_id(item_id)
        return self._spec(
            "PUT",
            f"/v1/collections/{_validate_collection(collection)}/vectors/{encoded_id}",
            "upsert",
            collection=collection,
            item_id=item_id,
            request_id=rid,
            body={
                "vector": _validate_vector(vector),
                "metadata": _validate_metadata(metadata),
                "request_id": rid,
            },
        )

    def _insert_request(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None,
        request_id: str | None,
    ) -> _Request:
        rid = _request_id(request_id)
        _validate_id(item_id)
        return self._spec(
            "POST",
            f"/v1/collections/{_validate_collection(collection)}/vectors",
            "insert",
            collection=collection,
            item_id=item_id,
            request_id=rid,
            body={
                "id": item_id,
                "vector": _validate_vector(vector),
                "metadata": _validate_metadata(metadata),
                "request_id": rid,
            },
        )

    def _update_request(
        self,
        collection: str,
        item_id: str,
        *,
        vector: Sequence[float] | object,
        metadata: Mapping[str, Any] | object,
        request_id: str | None,
    ) -> _Request:
        if vector is _UNSET and metadata is _UNSET:
            raise ValueError("update requires vector, metadata, or both")
        rid = _request_id(request_id)
        body: dict[str, Any] = {"request_id": rid}
        if vector is not _UNSET:
            body["vector"] = _validate_vector(vector)  # type: ignore[arg-type]
        if metadata is not _UNSET:
            body["metadata"] = _validate_metadata(metadata)  # type: ignore[arg-type]
        encoded_id = _validate_id(item_id)
        return self._spec(
            "PATCH",
            f"/v1/collections/{_validate_collection(collection)}/vectors/{encoded_id}",
            "update",
            collection=collection,
            item_id=item_id,
            request_id=rid,
            body=body,
        )

    def _delete_request(self, collection: str, item_id: str, request_id: str | None) -> _Request:
        rid = _request_id(request_id)
        return self._spec(
            "DELETE",
            f"/v1/collections/{_validate_collection(collection)}/vectors/{_validate_id(item_id)}",
            "delete",
            collection=collection,
            item_id=item_id,
            request_id=rid,
            params={"request_id": rid},
        )

    def _get_request(
        self,
        collection: str,
        item_id: str,
        consistency: str,
        min_seq: int | None,
    ) -> _Request:
        seq = self._get_min_seq(collection, item_id, consistency, min_seq)
        params: dict[str, Any] = {"consistency": consistency}
        if seq is not None:
            params["min_seq"] = seq
        return self._spec(
            "GET",
            f"/v1/collections/{_validate_collection(collection)}/vectors/{_validate_id(item_id)}",
            "get",
            collection=collection,
            item_id=item_id,
            params=params,
        )

    def _batch_request(self, collection: str, items: Iterable[Mapping[str, Any]]) -> _Request:
        prepared: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise ValueError("each batch item must be a mapping")
            item_id = item.get("id")
            _validate_id(item_id)
            rid = _request_id(item.get("request_id"))
            prepared.append(
                {
                    "id": item_id,
                    "vector": _validate_vector(item.get("vector")),
                    "metadata": _validate_metadata(item.get("metadata")),
                    "request_id": rid,
                }
            )
        if not prepared:
            raise ValueError("batch must contain at least one item")
        return self._spec(
            "POST",
            f"/v1/collections/{_validate_collection(collection)}/vectors:batch",
            "batch",
            collection=collection,
            body={"items": prepared},
        )

    def _search_request(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        k: int,
        filter: Mapping[str, Any] | None,
        consistency: str,
        tokens: Iterable[Mapping[str, int]] | None,
        allow_partial: bool,
        deadline_ms: int | None,
    ) -> _Request:
        _validate_consistency(consistency)
        if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= 100:
            raise ValueError("k must be an integer from 1 through 100")
        if deadline_ms is not None and (
            isinstance(deadline_ms, bool)
            or not isinstance(deadline_ms, int)
            or not 50 <= deadline_ms <= 5000
        ):
            raise ValueError("deadline_ms must be from 50 through 5000")
        if not isinstance(allow_partial, bool):
            raise ValueError("allow_partial must be a boolean")
        if filter is not None and not isinstance(filter, Mapping):
            raise ValueError("filter must be a mapping")
        body: dict[str, Any] = {
            "vector": _validate_vector(vector),
            "k": k,
            "consistency": consistency,
            "tokens": self._tokens_for_search(consistency, tokens),
            "allow_partial": allow_partial,
        }
        if filter is not None:
            body["filter"] = dict(filter)
        if deadline_ms is not None:
            body["deadline_ms"] = deadline_ms
        return self._spec(
            "POST",
            f"/v1/collections/{_validate_collection(collection)}/search",
            "search",
            collection=collection,
            body=body,
        )

    def _simple_request(self, operation: str) -> _Request:
        if operation == "health":
            return self._spec("GET", "/v1/health", operation)
        if operation == "metrics":
            return self._spec("GET", "/v1/metrics", operation)
        return self._spec("GET", "/v1/admin/status", "admin_status")


class DVSClient(_SharedClient):
    """Synchronous contract client.

    Parameters default to ``DVS_BASE_URL`` and ``DVS_API_KEY``. A comma-separated
    ``DVS_BASE_URL`` enables bounded gateway failover. All mutating calls generate a
    request ID once and retain it across retries.
    """

    def __init__(
        self,
        base_url: str | Sequence[str] | None = None,
        api_key: str | None = None,
        *,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        max_retries: int | None = None,
        history_path: str | None = None,
        client_id: str | None = None,
        transport: httpx.BaseTransport | None = None,
        random_source: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            retry_policy=retry_policy,
            max_retries=max_retries,
            history_path=history_path,
            client_id=client_id,
            random_source=random_source,
        )
        self._http = httpx.Client(transport=transport, timeout=timeout, headers=self._headers())

    def __enter__(self) -> DVSClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _send(self, request: _Request) -> dict[str, Any]:
        started_mono = time.monotonic()
        started_wall = datetime.now(UTC).isoformat()
        response_data: dict[str, Any] | None = None
        status_code: int | None = None
        error_code: str | None = None
        outcome = "error"
        try:
            for attempt in range(self.retry_policy.max_retries + 1):
                url = self._next_base_url() + request.path
                status_code = None
                try:
                    response = self._http.request(
                        request.method,
                        url,
                        json=request.body,
                        params=request.params,
                    )
                    status_code = response.status_code
                except httpx.TimeoutException as exc:
                    if attempt < self.retry_policy.max_retries:
                        time.sleep(self.retry_policy.delay(attempt, self._random()))
                        continue
                    raise TransportError("request timed out", timeout=True) from exc
                except httpx.TransportError as exc:
                    if attempt < self.retry_policy.max_retries:
                        time.sleep(self.retry_policy.delay(attempt, self._random()))
                        continue
                    raise TransportError("could not connect to a DVS gateway") from exc

                if (
                    response.status_code in _RETRYABLE_STATUSES
                    and attempt < self.retry_policy.max_retries
                ):
                    time.sleep(self.retry_policy.delay(attempt, self._random()))
                    continue
                if not response.is_success:
                    raise self._error_from_response(response)
                response_data = self._parse_response(response)
                self._remember_response_tokens(request, response_data)
                outcome = "ok"
                return response_data
            raise TransportError("request retry budget was exhausted")
        except ClientError as exc:
            exc.request_id = request.request_id
            error_code = exc.error_code
            status_code = exc.status_code if exc.status_code is not None else status_code
            outcome = (
                "timeout"
                if (isinstance(exc, TransportError) and exc.timeout)
                or exc.status_code == 504
                or exc.error_code == "deadline_exceeded"
                else "error"
            )
            raise
        finally:
            self._write_history(
                request,
                started_mono=started_mono,
                started_wall=started_wall,
                outcome=outcome,
                status_code=status_code,
                error_code=error_code,
                response=response_data,
            )

    def health(self) -> dict[str, Any]:
        return self._send(self._simple_request("health"))

    def metrics(self) -> dict[str, Any]:
        return self._send(self._simple_request("metrics"))

    def admin_status(self) -> dict[str, Any]:
        return self._send(self._simple_request("admin_status"))

    def upsert(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None = None,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._send(self._upsert_request(collection, item_id, vector, metadata, request_id))

    def insert(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None = None,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._send(self._insert_request(collection, item_id, vector, metadata, request_id))

    def update(
        self,
        collection: str,
        item_id: str,
        *,
        vector: Sequence[float] | object = _UNSET,
        metadata: Mapping[str, Any] | object = _UNSET,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._send(
            self._update_request(
                collection, item_id, vector=vector, metadata=metadata, request_id=request_id
            )
        )

    def delete(
        self, collection: str, item_id: str, *, request_id: str | None = None
    ) -> dict[str, Any]:
        return self._send(self._delete_request(collection, item_id, request_id))

    def get(
        self,
        collection: str,
        item_id: str,
        *,
        consistency: str = "eventual",
        min_seq: int | None = None,
    ) -> dict[str, Any]:
        return self._send(self._get_request(collection, item_id, consistency, min_seq))

    def batch(self, collection: str, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        return self._send(self._batch_request(collection, items))

    def search(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        k: int = 10,
        filter: Mapping[str, Any] | None = None,
        consistency: str = "eventual",
        tokens: Iterable[Mapping[str, int]] | None = None,
        allow_partial: bool = True,
        deadline_ms: int | None = None,
    ) -> dict[str, Any]:
        return self._send(
            self._search_request(
                collection,
                vector,
                k=k,
                filter=filter,
                consistency=consistency,
                tokens=tokens,
                allow_partial=allow_partial,
                deadline_ms=deadline_ms,
            )
        )


class AsyncDVSClient(_SharedClient):
    """Asynchronous counterpart to :class:`DVSClient` using ``httpx.AsyncClient``."""

    def __init__(
        self,
        base_url: str | Sequence[str] | None = None,
        api_key: str | None = None,
        *,
        timeout: float = 5.0,
        retry_policy: RetryPolicy | None = None,
        max_retries: int | None = None,
        history_path: str | None = None,
        client_id: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        random_source: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            retry_policy=retry_policy,
            max_retries=max_retries,
            history_path=history_path,
            client_id=client_id,
            random_source=random_source,
        )
        self._http = httpx.AsyncClient(
            transport=transport, timeout=timeout, headers=self._headers()
        )

    async def __aenter__(self) -> AsyncDVSClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _send(self, request: _Request) -> dict[str, Any]:
        started_mono = time.monotonic()
        started_wall = datetime.now(UTC).isoformat()
        response_data: dict[str, Any] | None = None
        status_code: int | None = None
        error_code: str | None = None
        outcome = "error"
        try:
            for attempt in range(self.retry_policy.max_retries + 1):
                url = self._next_base_url() + request.path
                status_code = None
                try:
                    response = await self._http.request(
                        request.method,
                        url,
                        json=request.body,
                        params=request.params,
                    )
                    status_code = response.status_code
                except httpx.TimeoutException as exc:
                    if attempt < self.retry_policy.max_retries:
                        await asyncio.sleep(self.retry_policy.delay(attempt, self._random()))
                        continue
                    raise TransportError("request timed out", timeout=True) from exc
                except httpx.TransportError as exc:
                    if attempt < self.retry_policy.max_retries:
                        await asyncio.sleep(self.retry_policy.delay(attempt, self._random()))
                        continue
                    raise TransportError("could not connect to a DVS gateway") from exc

                if (
                    response.status_code in _RETRYABLE_STATUSES
                    and attempt < self.retry_policy.max_retries
                ):
                    await asyncio.sleep(self.retry_policy.delay(attempt, self._random()))
                    continue
                if not response.is_success:
                    raise self._error_from_response(response)
                response_data = self._parse_response(response)
                self._remember_response_tokens(request, response_data)
                outcome = "ok"
                return response_data
            raise TransportError("request retry budget was exhausted")
        except ClientError as exc:
            exc.request_id = request.request_id
            error_code = exc.error_code
            status_code = exc.status_code if exc.status_code is not None else status_code
            outcome = (
                "timeout"
                if (isinstance(exc, TransportError) and exc.timeout)
                or exc.status_code == 504
                or exc.error_code == "deadline_exceeded"
                else "error"
            )
            raise
        finally:
            self._write_history(
                request,
                started_mono=started_mono,
                started_wall=started_wall,
                outcome=outcome,
                status_code=status_code,
                error_code=error_code,
                response=response_data,
            )

    async def health(self) -> dict[str, Any]:
        return await self._send(self._simple_request("health"))

    async def metrics(self) -> dict[str, Any]:
        return await self._send(self._simple_request("metrics"))

    async def admin_status(self) -> dict[str, Any]:
        return await self._send(self._simple_request("admin_status"))

    async def upsert(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None = None,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._send(
            self._upsert_request(collection, item_id, vector, metadata, request_id)
        )

    async def insert(
        self,
        collection: str,
        item_id: str,
        vector: Sequence[float],
        metadata: Mapping[str, Any] | None = None,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._send(
            self._insert_request(collection, item_id, vector, metadata, request_id)
        )

    async def update(
        self,
        collection: str,
        item_id: str,
        *,
        vector: Sequence[float] | object = _UNSET,
        metadata: Mapping[str, Any] | object = _UNSET,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._send(
            self._update_request(
                collection, item_id, vector=vector, metadata=metadata, request_id=request_id
            )
        )

    async def delete(
        self, collection: str, item_id: str, *, request_id: str | None = None
    ) -> dict[str, Any]:
        return await self._send(self._delete_request(collection, item_id, request_id))

    async def get(
        self,
        collection: str,
        item_id: str,
        *,
        consistency: str = "eventual",
        min_seq: int | None = None,
    ) -> dict[str, Any]:
        return await self._send(self._get_request(collection, item_id, consistency, min_seq))

    async def batch(self, collection: str, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        return await self._send(self._batch_request(collection, items))

    async def search(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        k: int = 10,
        filter: Mapping[str, Any] | None = None,
        consistency: str = "eventual",
        tokens: Iterable[Mapping[str, int]] | None = None,
        allow_partial: bool = True,
        deadline_ms: int | None = None,
    ) -> dict[str, Any]:
        return await self._send(
            self._search_request(
                collection,
                vector,
                k=k,
                filter=filter,
                consistency=consistency,
                tokens=tokens,
                allow_partial=allow_partial,
                deadline_ms=deadline_ms,
            )
        )
