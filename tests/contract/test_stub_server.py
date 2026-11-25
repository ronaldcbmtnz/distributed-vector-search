from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from tools.stub_server.app import create_stub_app

_RW = {"Authorization": "Bearer dev-key-rw"}
_RO = {"Authorization": "Bearer dev-key-ro"}
_ADMIN = {"Authorization": "Bearer dev-admin"}
_DEMO = "/v1/collections/demo"
_DIM = 128


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = create_stub_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://stub"
    ) as test_client:
        yield test_client


def _vector(*values: float) -> list[float]:
    return [*values, *([0.0] * (_DIM - len(values)))]


async def test_health_and_authentication(client: httpx.AsyncClient) -> None:
    health = await client.get("/v1/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    unauthenticated = await client.get(f"{_DEMO}/vectors/missing")
    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["error"]["code"] == "unauthenticated"

    forbidden = await client.put(
        f"{_DEMO}/vectors/forbidden",
        json={"vector": _vector(1.0), "request_id": "forbidden-write"},
        headers=_RO,
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["error"]["code"] == "permission_denied"


async def test_upsert_dedup_update_get_and_delete(client: httpx.AsyncClient) -> None:
    path = f"{_DEMO}/vectors/item"
    body = {
        "vector": _vector(1.0, 0.0),
        "metadata": {"category": "red"},
        "request_id": "upsert-item-1",
    }
    created = await client.put(path, json=body, headers=_RW)
    retried = await client.put(path, json=body, headers=_RW)
    assert created.status_code == 200 and created.json()["created"] is True
    assert retried.json() == created.json()

    conflict = await client.put(
        path,
        json={**body, "vector": _vector(2.0, 0.0)},
        headers=_RW,
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"

    token = created.json()["token"]
    updated = await client.patch(
        path,
        json={"metadata": {"category": "green"}, "request_id": "patch-item-1"},
        headers=_RW,
    )
    assert updated.status_code == 200
    fetched = await client.get(
        path,
        params={"consistency": "read_your_writes", "min_seq": token["seq"]},
        headers=_RW,
    )
    assert fetched.status_code == 200
    assert fetched.json()["metadata"] == {"category": "green"}
    assert fetched.json()["vector"] == _vector(1.0, 0.0)

    deleted = await client.delete(
        path, params={"request_id": "delete-item-1"}, headers=_RW
    )
    assert deleted.status_code == 200 and deleted.json()["existed"] is True
    missing = await client.get(path, headers=_RW)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"


async def test_batch_and_exact_filtered_search(client: httpx.AsyncClient) -> None:
    items = [
        {"id": "alpha", "vector": _vector(1.0), "metadata": {"group": "keep"}, "request_id": "b1"},
        {"id": "beta", "vector": _vector(-1.0), "metadata": {"group": "keep"}, "request_id": "b2"},
        {
            "id": "excluded",
            "vector": _vector(0.5),
            "metadata": {"group": "skip"},
            "request_id": "b3",
        },
        {"id": "invalid", "vector": [0.0], "request_id": "b4"},
    ]
    batch = await client.post(f"{_DEMO}/vectors:batch", json={"items": items}, headers=_RW)
    assert batch.status_code == 200
    assert [result["ok"] for result in batch.json()["results"]] == [True, True, True, False]
    assert batch.json()["results"][3]["error"]["code"] == "invalid_argument"

    search = await client.post(
        f"{_DEMO}/search",
        json={"vector": _vector(), "k": 3, "filter": {"group": "keep"}},
        headers=_RO,
    )
    result = search.json()
    assert search.status_code == 200
    assert [hit["id"] for hit in result["results"]] == ["alpha", "beta"]
    assert [hit["distance"] for hit in result["results"]] == [1.0, 1.0]
    assert result["partial"] is False
    assert result["coverage"] == 1.0
    assert result["missing_shards"] == []


async def test_cosine_collection_and_admin_endpoints(client: httpx.AsyncClient) -> None:
    cosine_path = "/v1/collections/demo_cos"
    write = await client.put(
        f"{cosine_path}/vectors/same",
        json={"vector": _vector(2.0), "request_id": "cosine-1"},
        headers=_RW,
    )
    assert write.status_code == 200
    search = await client.post(
        f"{cosine_path}/search", json={"vector": _vector(1.0)}, headers=_RW
    )
    assert search.status_code == 200
    assert search.json()["results"][0]["distance"] == pytest.approx(0.0)

    metrics = await client.get("/v1/metrics", headers=_RW)
    assert metrics.status_code == 200
    assert {"ops", "shards", "counters", "replication_lag"} <= set(metrics.json())
    status = await client.get("/v1/admin/status", headers=_ADMIN)
    assert status.status_code == 200
    assert status.json()["replicas"][0]["ready"] is True
