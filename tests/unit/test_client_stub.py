from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from dvs.client import APIError, AsyncDVSClient
from tools.stub_server.app import create_stub_app


@pytest.fixture
async def stub_client() -> AsyncIterator[AsyncDVSClient]:
    transport = httpx.ASGITransport(app=create_stub_app())
    async with AsyncDVSClient(
        "http://stub",
        "dev-key-rw",
        max_retries=0,
        transport=transport,
    ) as client:
        yield client


async def test_async_sdk_crud_search_and_ryw_against_stub(stub_client: AsyncDVSClient) -> None:
    item_id = f"sdk-{uuid.uuid4().hex}"
    vector = [1.0] + [0.0] * 127

    write = await stub_client.upsert("demo", item_id, vector, {"category": "sdk-test"})
    assert write["id"] == item_id
    assert write["created"] is True
    assert write["token"]["seq"] >= 0

    fetched = await stub_client.get("demo", item_id, consistency="read_your_writes")
    assert fetched["vector"] == vector
    assert fetched["metadata"] == {"category": "sdk-test"}

    result = await stub_client.search("demo", vector, consistency="read_your_writes", k=1)
    assert result["partial"] is False
    assert result["results"][0]["id"] == item_id

    await stub_client.update("demo", item_id, metadata={"category": "updated"})
    assert (await stub_client.get("demo", item_id, consistency="read_your_writes"))["metadata"] == {
        "category": "updated"
    }

    await stub_client.delete("demo", item_id)
    with pytest.raises(APIError) as caught:
        await stub_client.get("demo", item_id)
    assert caught.value.status_code == 404
    assert caught.value.error_code == "not_found"
