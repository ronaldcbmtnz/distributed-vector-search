from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from dvs.client import APIError, AsyncDVSClient, DVSClient, RetryPolicy, TransportError
from dvs.client.cli import main


def _ok(payload: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


def test_retries_transient_response_with_same_request_id_and_fails_over() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        seen.append((request.url.host, payload["request_id"]))
        if len(seen) == 1:
            return _ok({"error": {"code": "unavailable", "message": "retry"}}, 503)
        return _ok({"id": "item", "created": True, "token": {"shard": 2, "seq": 8}})

    transport = httpx.MockTransport(handler)
    policy = RetryPolicy(max_retries=1, base_delay_s=0, max_delay_s=0)
    with DVSClient(
        "http://first.test,http://second.test",
        api_key="secret",
        transport=transport,
        retry_policy=policy,
        random_source=lambda: 0,
    ) as client:
        result = client.upsert("demo", "item", [1.0], request_id="stable-id")

    assert result["token"] == {"shard": 2, "seq": 8}
    assert seen == [("first.test", "stable-id"), ("second.test", "stable-id")]


def test_mutation_generates_request_id_when_omitted() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content)["request_id"])
        return _ok({"id": "item", "created": True, "token": {"shard": 0, "seq": 1}})

    with DVSClient("http://gateway.test", transport=httpx.MockTransport(handler)) as client:
        client.upsert("demo", "item", [1])

    assert len(seen) == 1
    assert str(uuid.UUID(seen[0])) == seen[0]


def test_transport_timeout_retries_then_returns_success() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectTimeout("temporary timeout")
        return _ok({"status": "ok"})

    with DVSClient(
        "http://gateway.test",
        transport=httpx.MockTransport(handler),
        max_retries=1,
        random_source=lambda: 0,
    ) as client:
        assert client.health() == {"status": "ok"}
    assert calls == 2


def test_connection_error_retries_then_fails_over() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("gateway connection refused")
        return _ok({"status": "ok"})

    with DVSClient(
        "http://gateway.test",
        transport=httpx.MockTransport(handler),
        max_retries=1,
        random_source=lambda: 0,
    ) as client:
        assert client.health() == {"status": "ok"}
    assert calls == 2


def test_non_retryable_http_error_preserves_contract_code() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _ok({"error": {"code": "invalid_argument", "message": "bad vector"}}, 400)

    with (
        DVSClient(
            "http://gateway.test", transport=httpx.MockTransport(handler), max_retries=3
        ) as client,
        pytest.raises(APIError) as caught,
    ):
        client.search("demo", [1.0], k=1)

    assert caught.value.status_code == 400
    assert caught.value.error_code == "invalid_argument"
    assert calls == 1


def test_retry_exhaustion_reports_transport_timeout() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow gateway")

    with (
        DVSClient(
            "http://gateway.test",
            transport=httpx.MockTransport(handler),
            max_retries=0,
        ) as client,
        pytest.raises(TransportError, match="timed out") as caught,
    ):
        client.upsert("demo", "item", [1])
    assert caught.value.timeout is True
    assert caught.value.request_id is not None
    assert str(uuid.UUID(caught.value.request_id)) == caught.value.request_id


def test_ryw_write_token_is_used_by_get_and_search() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "PUT":
            return _ok({"id": "item", "created": True, "token": {"shard": 3, "seq": 19}})
        if request.method == "GET":
            assert request.url.params["min_seq"] == "19"
            return _ok({"id": "item", "seq": 19, "applied_seq": 19, "vector": [1], "metadata": {}})
        body = json.loads(request.content)
        assert body["consistency"] == "read_your_writes"
        assert body["tokens"] == [{"shard": 3, "seq": 19}]
        return _ok(
            {
                "query_id": "q-1",
                "results": [{"id": "item", "distance": 0.0, "metadata": {}}],
                "partial": False,
                "coverage": 1.0,
                "missing_shards": [],
                "consistency": "read_your_writes",
                "shards": {},
            }
        )

    with DVSClient("http://gateway.test", transport=httpx.MockTransport(handler)) as client:
        client.upsert("demo", "item", [1], request_id="write-1")
        client.get("demo", "item", consistency="read_your_writes")
        result = client.search("demo", [1], consistency="read_your_writes")

    assert result["results"][0]["id"] == "item"
    assert [request.method for request in requests] == ["PUT", "GET", "POST"]


def test_history_is_one_redacted_record_per_logical_operation(tmp_path: Path) -> None:
    history_file = tmp_path / "history" / "client.jsonl"

    def handler(_: httpx.Request) -> httpx.Response:
        return _ok({"id": "item", "created": True, "token": {"shard": 0, "seq": 1}})

    with DVSClient(
        "http://gateway.test",
        api_key="do-not-write-this-key",
        history_path=str(history_file),
        client_id="client-1",
        transport=httpx.MockTransport(handler),
    ) as client:
        client.upsert("demo", "item", [99.0], request_id="rid-1")

    records = [json.loads(line) for line in history_file.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    record = records[0]
    assert {
        "client_id",
        "op",
        "collection",
        "id",
        "request_id",
        "t_inv_mono",
        "t_resp_mono",
        "t_inv_wall",
        "outcome",
        "http_status",
        "error_code",
        "token",
        "result_summary",
    } <= set(record)
    serialized = json.dumps(record)
    assert "do-not-write-this-key" not in serialized
    assert "99.0" not in serialized


def test_unknown_write_outcome_is_recorded_as_timeout_with_retry_id(tmp_path: Path) -> None:
    history_file = tmp_path / "timeouts.jsonl"

    def handler(_: httpx.Request) -> httpx.Response:
        return _ok(
            {"error": {"code": "deadline_exceeded", "message": "unknown write outcome"}}, 504
        )

    with (
        DVSClient(
            "http://gateway.test",
            history_path=str(history_file),
            transport=httpx.MockTransport(handler),
            max_retries=0,
        ) as client,
        pytest.raises(APIError) as caught,
    ):
        client.upsert("demo", "item", [1], request_id="retry-this-write")

    assert caught.value.request_id == "retry-this-write"
    record = json.loads(history_file.read_text(encoding="utf-8"))
    assert record["outcome"] == "timeout"
    assert record["http_status"] == 504
    assert record["error_code"] == "deadline_exceeded"


def test_batch_history_records_each_item_request_id(tmp_path: Path) -> None:
    history_file = tmp_path / "batch.jsonl"

    def handler(request: httpx.Request) -> httpx.Response:
        items = json.loads(request.content)["items"]
        return _ok(
            {
                "results": [
                    {"id": item["id"], "ok": True, "token": {"shard": 0, "seq": i + 1}}
                    for i, item in enumerate(items)
                ]
            }
        )

    with DVSClient(
        "http://gateway.test",
        history_path=str(history_file),
        transport=httpx.MockTransport(handler),
    ) as client:
        client.batch("demo", [{"id": "a", "vector": [1]}, {"id": "b", "vector": [2]}])

    records = [json.loads(line) for line in history_file.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 2
    assert [record["op"] for record in records] == ["upsert", "upsert"]
    assert [record["id"] for record in records] == ["a", "b"]
    assert all(record["request_id"] for record in records)
    assert all(record["outcome"] == "ok" for record in records)


def test_client_validates_search_limits_before_sending() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        pytest.fail("invalid request should not reach transport")

    with (
        DVSClient("http://gateway.test", transport=httpx.MockTransport(handler)) as client,
        pytest.raises(ValueError, match="k must"),
    ):
        client.search("demo", [1], k=101)


def test_async_client_uses_same_contract_and_closes() -> None:
    async def run() -> dict[str, Any]:
        class AsyncStaticTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, _: httpx.Request) -> httpx.Response:
                return _ok({"status": "ok"})

        async with AsyncDVSClient(
            "http://gateway.test",
            transport=AsyncStaticTransport(),
        ) as client:
            return await client.health()

    assert asyncio.run(run()) == {"status": "ok"}


def test_cli_dispatches_command_and_prints_json(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    class FakeClient:
        def __init__(self, **_: Any) -> None:
            pass

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_: object) -> None:
            pass

        def upsert(
            self,
            collection: str,
            item_id: str,
            vector: list[int],
            metadata: Any,
            *,
            request_id: str | None,
        ) -> dict[str, Any]:
            return {
                "id": item_id,
                "collection": collection,
                "length": len(vector),
                "metadata": metadata,
                "request_id": request_id,
            }

    monkeypatch.setattr("dvs.client.cli.DVSClient", FakeClient)
    status = main(["upsert", "demo", "item", "--vector", "[1, 2]", "--request-id", "rid"])
    output = json.loads(capsys.readouterr().out)

    assert status == 0
    assert output == {
        "id": "item",
        "collection": "demo",
        "length": 2,
        "metadata": None,
        "request_id": "rid",
    }
