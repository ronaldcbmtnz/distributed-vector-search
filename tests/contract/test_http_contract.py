"""Black-box HTTP tests for the observable contract (T119)."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

_DIM = 128
# With DVS_BASE_URL the suite is pure black-box HTTP against that server (stub, Ray, native).
# Without it, it runs against the in-process stub so `make test-contract` works out of the box.
_TARGET = os.environ.get("DVS_BASE_URL", "").split(",", maxsplit=1)[0].strip()
_RW = {"Authorization": f"Bearer {os.environ.get('DVS_API_KEY', 'dev-key-rw')}"}
_RO = {"Authorization": f"Bearer {os.environ.get('DVS_READONLY_API_KEY', 'dev-key-ro')}"}
_ADMIN = {"Authorization": f"Bearer {os.environ.get('DVS_ADMIN_API_KEY', 'dev-admin')}"}
_DEMO = "/v1/collections/demo"


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    if _TARGET:
        async with httpx.AsyncClient(base_url=_TARGET.rstrip("/"), timeout=10.0) as http:
            yield http
        return
    from tools.stub_server.app import create_stub_app  # no server given: in-process stub

    transport = httpx.ASGITransport(app=create_stub_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://stub", timeout=10.0) as http:
        yield http


@pytest.fixture
async def vector_ids(
    client: httpx.AsyncClient,
) -> Callable[[str], str]:
    created: list[tuple[str, str]] = []

    def create(collection: str = "demo") -> str:
        item_id = f"t119-{uuid.uuid4().hex}"
        created.append((collection, item_id))
        return item_id

    yield create
    for collection, item_id in created:
        await client.delete(
            f"/v1/collections/{collection}/vectors/{item_id}",
            params={"request_id": str(uuid.uuid4())},
            headers=_RW,
        )


def _vector(*values: float) -> list[float]:
    if len(values) > _DIM:
        raise ValueError("test vector is longer than the demo collection dimension")
    return [*values, *([0.0] * (_DIM - len(values)))]


def _request_id() -> str:
    return str(uuid.uuid4())


def _assert_token(token: Any) -> None:
    assert isinstance(token, dict)
    assert isinstance(token.get("shard"), int) and not isinstance(token["shard"], bool)
    assert token["shard"] >= 0
    assert isinstance(token.get("seq"), int) and not isinstance(token["seq"], bool)
    assert token["seq"] >= 0


def _assert_error(response: httpx.Response, status: int, code: str) -> None:
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str)
    assert isinstance(body["error"]["details"], dict)


async def test_health_authentication_and_operational_endpoints(client: httpx.AsyncClient) -> None:
    health = await client.get("/v1/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    _assert_error(await client.get(f"{_DEMO}/vectors/no-auth"), 401, "unauthenticated")
    _assert_error(
        await client.put(
            f"{_DEMO}/vectors/forbidden",
            json={"vector": _vector(1), "request_id": _request_id()},
            headers=_RO,
        ),
        403,
        "permission_denied",
    )
    metrics = await client.get("/v1/metrics", headers=_RW)
    assert metrics.status_code == 200
    assert {"ops", "shards", "counters", "replication_lag"} <= set(metrics.json())
    status = await client.get("/v1/admin/status", headers=_ADMIN)
    assert status.status_code == 200
    assert isinstance(status.json().get("replicas"), list)


async def test_upsert_insert_update_get_delete_and_idempotency(
    client: httpx.AsyncClient, vector_ids: Callable[[str], str]
) -> None:
    item_id = vector_ids()
    path = f"{_DEMO}/vectors/{item_id}"
    upsert_body = {
        "vector": _vector(1, 0),
        "metadata": {"category": "red", "labels": ["a", "b"]},
        "request_id": _request_id(),
    }
    created = await client.put(path, json=upsert_body, headers=_RW)
    assert created.status_code == 200
    created_body = created.json()
    assert created_body["id"] == item_id and created_body["created"] is True
    _assert_token(created_body["token"])

    replay = await client.put(path, json=upsert_body, headers=_RW)
    assert replay.status_code == 200
    assert replay.json() == created_body
    conflict = await client.put(
        path,
        json={**upsert_body, "vector": _vector(2, 0)},
        headers=_RW,
    )
    _assert_error(conflict, 409, "idempotency_conflict")

    replacement = await client.put(
        path,
        json={
            "vector": _vector(0, 1),
            "metadata": {"category": "blue"},
            "request_id": _request_id(),
        },
        headers=_RW,
    )
    assert replacement.status_code == 200 and replacement.json()["created"] is False
    _assert_token(replacement.json()["token"])

    missing_id = vector_ids()
    inserted = await client.post(
        f"{_DEMO}/vectors",
        json={"id": missing_id, "vector": _vector(1), "request_id": _request_id()},
        headers=_RW,
    )
    assert inserted.status_code == 201
    _assert_token(inserted.json()["token"])
    duplicate = await client.post(
        f"{_DEMO}/vectors",
        json={"id": missing_id, "vector": _vector(1), "request_id": _request_id()},
        headers=_RW,
    )
    _assert_error(duplicate, 409, "already_exists")

    patched = await client.patch(
        path,
        json={"metadata": {"category": "green"}, "request_id": _request_id()},
        headers=_RW,
    )
    assert patched.status_code == 200
    patched_vector = await client.patch(
        path,
        json={"vector": _vector(0, 0, 1), "request_id": _request_id()},
        headers=_RW,
    )
    assert patched_vector.status_code == 200
    fetched = await client.get(
        path,
        params={
            "consistency": "read_your_writes",
            "min_seq": patched_vector.json()["token"]["seq"],
        },
        headers=_RW,
    )
    assert fetched.status_code == 200
    value = fetched.json()
    assert value["id"] == item_id
    assert value["vector"] == _vector(0, 0, 1)
    assert value["metadata"] == {"category": "green"}
    assert value["seq"] >= replacement.json()["token"]["seq"]
    assert value["applied_seq"] >= value["seq"]

    missing_update = await client.patch(
        f"{_DEMO}/vectors/{vector_ids()}",
        json={"metadata": {"x": 1}, "request_id": _request_id()},
        headers=_RW,
    )
    _assert_error(missing_update, 404, "not_found")

    delete_request_id = _request_id()
    deleted = await client.delete(path, params={"request_id": delete_request_id}, headers=_RW)
    assert deleted.status_code == 200 and deleted.json()["existed"] is True
    _assert_token(deleted.json()["token"])
    delete_replay = await client.delete(path, params={"request_id": delete_request_id}, headers=_RW)
    assert delete_replay.status_code == 200
    assert delete_replay.json() == deleted.json()
    missing = await client.get(path, headers=_RW)
    _assert_error(missing, 404, "not_found")
    deleted_missing = await client.delete(
        f"{_DEMO}/vectors/{vector_ids()}",
        params={"request_id": _request_id()},
        headers=_RW,
    )
    assert deleted_missing.status_code == 200 and deleted_missing.json()["existed"] is False


async def test_batch_validation_and_exact_filtered_search(
    client: httpx.AsyncClient, vector_ids: Callable[[str], str]
) -> None:
    cohort = uuid.uuid4().hex
    ids = [vector_ids() for _ in range(3)]
    batch = await client.post(
        f"{_DEMO}/vectors:batch",
        json={
            "items": [
                {
                    "id": ids[0],
                    "vector": _vector(1),
                    "metadata": {"cohort": cohort, "kind": "keep", "price": 20, "tags": ["common"]},
                    "request_id": _request_id(),
                },
                {
                    "id": ids[1],
                    "vector": _vector(-1),
                    "metadata": {"cohort": cohort, "kind": "keep", "price": 50, "tags": ["common"]},
                    "request_id": _request_id(),
                },
                {
                    "id": ids[2],
                    "vector": _vector(0.5),
                    "metadata": {"cohort": cohort, "kind": "skip", "price": 30, "tags": ["common"]},
                    "request_id": _request_id(),
                },
                {
                    "id": vector_ids(),
                    "vector": [0.0],
                    "metadata": {"cohort": cohort},
                    "request_id": _request_id(),
                },
            ]
        },
        headers=_RW,
    )
    assert batch.status_code == 200
    results = batch.json()["results"]
    assert len(results) == 4
    assert all(results[index]["ok"] for index in range(3))
    assert all(not results[index]["ok"] for index in (3,))
    for result in results[:3]:
        _assert_token(result["token"])
    assert results[3]["error"]["code"] == "invalid_argument"

    response = await client.post(
        f"{_DEMO}/search",
        json={
            "vector": _vector(),
            "k": 3,
            "filter": {
                "cohort": cohort,
                "kind": {"$in": ["keep"]},
                "price": {"$gte": 10, "$lte": 60},
                "tags": "common",
            },
            "consistency": "strong",
        },
        headers=_RO,
    )
    assert response.status_code == 200
    body = response.json()
    assert set(body) >= {
        "query_id",
        "results",
        "partial",
        "coverage",
        "missing_shards",
        "consistency",
        "shards",
    }
    hits = body["results"]
    assert [hit["id"] for hit in hits] == sorted(ids[:2])
    assert all(hit["metadata"]["cohort"] == cohort for hit in hits)
    assert [hit["distance"] for hit in hits] == pytest.approx([1.0, 1.0])
    assert body["partial"] is False
    assert body["coverage"] == 1.0
    assert body["missing_shards"] == []
    assert body["consistency"] == "strong"
    assert isinstance(body["shards"], dict)


async def test_read_your_writes_tokens_and_cosine_metric(
    client: httpx.AsyncClient, vector_ids: Callable[[str], str]
) -> None:
    item_id = vector_ids("demo_cos")
    cohort = uuid.uuid4().hex  # isolates this test from other vectors in a shared server
    path = f"/v1/collections/demo_cos/vectors/{item_id}"
    write = await client.put(
        path,
        json={
            "vector": _vector(2),
            "metadata": {"fresh": True, "cohort": cohort},
            "request_id": _request_id(),
        },
        headers=_RW,
    )
    assert write.status_code == 200
    token = write.json()["token"]
    _assert_token(token)

    fetched = await client.get(
        path,
        params={"consistency": "read_your_writes", "min_seq": token["seq"]},
        headers=_RW,
    )
    assert fetched.status_code == 200
    assert fetched.json()["metadata"] == {"fresh": True, "cohort": cohort}
    assert fetched.json()["seq"] >= token["seq"]

    search = await client.post(
        "/v1/collections/demo_cos/search",
        json={
            "vector": _vector(1),
            "k": 1,
            "filter": {"cohort": cohort},
            "consistency": "read_your_writes",
            "tokens": [token],
        },
        headers=_RW,
    )
    assert search.status_code == 200
    assert search.json()["results"][0]["id"] == item_id
    assert search.json()["results"][0]["distance"] == pytest.approx(0.0, abs=1e-7)

    zero = await client.put(
        f"/v1/collections/demo_cos/vectors/{vector_ids('demo_cos')}",
        json={"vector": _vector(), "request_id": _request_id()},
        headers=_RW,
    )
    _assert_error(zero, 400, "invalid_argument")


async def test_invalid_inputs_unknown_collection_and_permissions(
    client: httpx.AsyncClient, vector_ids: Callable[[str], str]
) -> None:
    bad_dimension = await client.put(
        f"{_DEMO}/vectors/{vector_ids()}",
        json={"vector": [1.0], "request_id": _request_id()},
        headers=_RW,
    )
    _assert_error(bad_dimension, 400, "invalid_argument")
    bad_k = await client.post(f"{_DEMO}/search", json={"vector": _vector(), "k": 0}, headers=_RW)
    _assert_error(bad_k, 400, "invalid_argument")
    bad_filter = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "filter": {"price": {"$wat": 10}}},
        headers=_RW,
    )
    _assert_error(bad_filter, 400, "invalid_argument")
    bad_deadline = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "deadline_ms": 49},
        headers=_RW,
    )
    _assert_error(bad_deadline, 400, "invalid_argument")
    missing_request_id = await client.put(
        f"{_DEMO}/vectors/{vector_ids()}", json={"vector": _vector(1)}, headers=_RW
    )
    _assert_error(missing_request_id, 400, "invalid_argument")
    empty_update = await client.patch(
        f"{_DEMO}/vectors/{vector_ids()}", json={"request_id": _request_id()}, headers=_RW
    )
    _assert_error(empty_update, 400, "invalid_argument")
    unknown = await client.get("/v1/collections/no-such-collection/vectors/id", headers=_ADMIN)
    _assert_error(unknown, 404, "not_found")
    no_admin = await client.get("/v1/admin/status", headers=_RW)
    _assert_error(no_admin, 403, "permission_denied")


async def test_search_accepts_contract_maximum_k_and_deadline(client: httpx.AsyncClient) -> None:
    response = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "k": 100, "deadline_ms": 5000},
        headers=_RO,
    )
    assert response.status_code == 200
    assert len(response.json()["results"]) <= 100


async def test_concurrent_duplicate_request_id_returns_original_result(
    client: httpx.AsyncClient, vector_ids: Callable[[str], str]
) -> None:
    item_id = vector_ids()
    path = f"{_DEMO}/vectors/{item_id}"
    body = {"vector": _vector(3), "metadata": {"concurrent": True}, "request_id": _request_id()}
    first, second = await asyncio.gather(
        client.put(path, json=body, headers=_RW),
        client.put(path, json=body, headers=_RW),
    )
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    _assert_token(first.json()["token"])


@pytest.mark.faults
async def test_partial_search_is_explicit_when_faults_are_prepared(
    client: httpx.AsyncClient,
) -> None:
    if os.environ.get("DVS_FAULTS_EXPECT_PARTIAL") != "1":
        pytest.skip("set DVS_FAULTS_EXPECT_PARTIAL=1 after preparing an unavailable shard")
    response = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "k": 10, "allow_partial": True, "deadline_ms": 50},
        headers=_RO,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["partial"] is True
    assert 0 <= body["coverage"] < 1
    assert body["missing_shards"]
    assert all(
        item["reason"] in {"unreachable", "unserved", "stale"} for item in body["missing_shards"]
    )


@pytest.mark.faults
async def test_search_without_partial_results_returns_unavailable_when_faults_are_prepared(
    client: httpx.AsyncClient,
) -> None:
    if os.environ.get("DVS_FAULTS_EXPECT_PARTIAL") != "1":
        pytest.skip("set DVS_FAULTS_EXPECT_PARTIAL=1 after preparing an unavailable shard")
    response = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "k": 10, "allow_partial": False, "deadline_ms": 50},
        headers=_RO,
    )
    _assert_error(response, 503, "unavailable")
