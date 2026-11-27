"""Revisión del SDK: clasificación del resultado en la historia y vectores NumPy.

En la historia, "timeout" significa resultado DESCONOCIDO: el checker debe tratar esa escritura
como posiblemente aplicada. Un 503 en una escritura puede llegar después de que el primario la
aplicara (el actor muere a mitad), así que también es desconocido.
"""

import json
from pathlib import Path

import httpx
import numpy as np
import pytest

from dvs.client import DVSClient
from dvs.client.errors import APIError, TransportError

OK_WRITE = {"id": "a", "token": {"shard": 0, "seq": 1}, "created": True}
UNAVAILABLE = {"error": {"code": "unavailable", "message": "x", "details": {}}}


def _client(tmp_path: Path, handler) -> DVSClient:  # type: ignore[no-untyped-def]
    return DVSClient(
        "http://gw",
        api_key="k",
        max_retries=0,
        history_path=str(tmp_path / "h.jsonl"),
        transport=httpx.MockTransport(handler),
    )


def _last_outcome(tmp_path: Path) -> str:
    lines = (tmp_path / "h.jsonl").read_text().splitlines()
    return str(json.loads(lines[-1])["outcome"])


def test_write_ending_in_503_is_recorded_as_unknown(tmp_path: Path) -> None:
    c = _client(tmp_path, lambda req: httpx.Response(503, json=UNAVAILABLE))
    with pytest.raises(APIError):
        c.upsert("demo", "a", [1.0, 0.0])
    assert _last_outcome(tmp_path) == "timeout"


def test_read_ending_in_503_is_a_plain_error(tmp_path: Path) -> None:
    c = _client(tmp_path, lambda req: httpx.Response(503, json=UNAVAILABLE))
    with pytest.raises(APIError):
        c.search("demo", [1.0, 0.0])
    assert _last_outcome(tmp_path) == "error"


def test_connection_refused_write_is_known_not_applied(tmp_path: Path) -> None:
    def refuse(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    c = _client(tmp_path, refuse)
    with pytest.raises(TransportError) as e:
        c.upsert("demo", "a", [1.0, 0.0])
    assert e.value.maybe_applied is False
    assert _last_outcome(tmp_path) == "error"


def test_failure_after_sending_a_write_is_unknown(tmp_path: Path) -> None:
    def drop(req: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError("connection dropped mid-response", request=req)

    c = _client(tmp_path, drop)
    with pytest.raises(TransportError) as e:
        c.delete("demo", "a")
    assert e.value.maybe_applied is True
    assert _last_outcome(tmp_path) == "timeout"


def test_numpy_vectors_are_accepted(tmp_path: Path) -> None:
    seen: list[dict] = []  # type: ignore[type-arg]

    def ok(req: httpx.Request) -> httpx.Response:
        seen.append(json.loads(req.content))
        return httpx.Response(200, json=OK_WRITE)

    c = _client(tmp_path, ok)
    c.upsert("demo", "a", np.array([1.5, -2.0], dtype=np.float32))
    assert seen[0]["vector"] == [1.5, -2.0]
