"""T115a–c / T117 / T113b / T115e — API REST de punta a punta (API → coordinador → núcleo).

Mismo contrato que verán los tests de contrato de B, pero sin red ni Ray.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import numpy as np
import pytest
import yaml

from dvs.api.app import create_app
from dvs.config import Config
from dvs.coordinator.backend import CoordinatorBackend
from dvs.core.hashing import shard_for

from .local_transport import LocalTransport

ROOT = Path(__file__).resolve().parents[3]
RW = {"Authorization": "Bearer dev-key-rw"}
RO = {"Authorization": "Bearer dev-key-ro"}
ADMIN = {"Authorization": "Bearer dev-admin"}
URL = "/v1/collections/demo"


def make_config(**overrides: Any) -> Config:
    raw = yaml.safe_load((ROOT / "deploy/ray/config.local.yaml").read_text())
    raw["collections"] = [
        {"name": "demo", "dim": 4, "metric": "l2"},
        {"name": "demo_cos", "dim": 4, "metric": "cosine"},
    ]
    for section, values in overrides.items():
        raw[section] = {**raw.get(section, {}), **values}
    return Config.model_validate(raw)


def build(config: Config | None = None, backend_wrapper: Any = None) -> tuple[Any, LocalTransport]:
    cfg = config or make_config()
    transport = LocalTransport(cfg.collection_specs, num_shards=2)
    backend = CoordinatorBackend(
        transport, attempt_s=0.05, write_timeout_s=1, suspect_ttl_s=1, map_ttl_s=60
    )
    app = create_app(cfg, backend_wrapper(backend) if backend_wrapper else backend)
    return app, transport


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app, _ = build()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://t") as c:
        yield c


async def put(
    c: httpx.AsyncClient, vid: str, vec: list[float], meta: Any = None, rid: str | None = None
):  # type: ignore[no-untyped-def]
    body = {"vector": vec, "metadata": meta or {}, "request_id": rid or f"r-{vid}-{vec}"}
    return await c.put(f"{URL}/vectors/{vid}", json=body, headers=RW)


# ───────────────────────────── auth ─────────────────────────────


async def test_health_needs_no_key(client: httpx.AsyncClient) -> None:
    r = await client.get("/v1/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}, {"Authorization": "x"}])
async def test_unauthenticated(client: httpx.AsyncClient, headers: dict[str, str]) -> None:
    r = await client.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=headers)
    assert r.status_code == 401 and r.json()["error"]["code"] == "unauthenticated"


async def test_permission_denied(client: httpx.AsyncClient) -> None:
    r = await put(client, "a", [1, 2, 3, 4])
    assert r.status_code == 200
    w = await client.put(
        f"{URL}/vectors/a", json={"vector": [1, 1, 1, 1], "request_id": "x"}, headers=RO
    )
    assert w.status_code == 403 and w.json()["error"]["code"] == "permission_denied"
    s = await client.post(
        "/v1/collections/demo_cos/search", json={"vector": [1, 0, 0, 0]}, headers=RO
    )
    assert s.status_code == 403
    assert (await client.get("/v1/admin/status", headers=RW)).status_code == 403


# ───────────────────────────── escrituras ─────────────────────────────


async def test_upsert_create_then_replace_with_token(client: httpx.AsyncClient) -> None:
    r1 = await put(client, "a", [1, 2, 3, 4], {"v": 1})
    r2 = await put(client, "a", [4, 3, 2, 1], {"v": 2})
    assert r1.json()["created"] is True and r2.json()["created"] is False
    tok = r2.json()["token"]
    assert tok["shard"] == shard_for("demo", "a", 2) and tok["seq"] == r1.json()["token"]["seq"] + 1


async def test_retry_same_request_id_and_conflict(client: httpx.AsyncClient) -> None:
    a = await put(client, "a", [1, 2, 3, 4], rid="same")
    b = await put(client, "a", [1, 2, 3, 4], rid="same")
    assert a.json() == b.json()  # mismo token: no se reaplicó
    c = await put(client, "a", [9, 9, 9, 9], rid="same")
    assert c.status_code == 409 and c.json()["error"]["code"] == "idempotency_conflict"


async def test_insert_update_delete_semantics(client: httpx.AsyncClient) -> None:
    body = {"id": "x", "vector": [1, 1, 1, 1], "metadata": {"a": 1}, "request_id": "i1"}
    assert (await client.post(f"{URL}/vectors", json=body, headers=RW)).status_code == 201
    dup = await client.post(f"{URL}/vectors", json={**body, "request_id": "i2"}, headers=RW)
    assert dup.status_code == 409 and dup.json()["error"]["code"] == "already_exists"
    nf = await client.patch(
        f"{URL}/vectors/nope", json={"metadata": {}, "request_id": "u0"}, headers=RW
    )
    assert nf.status_code == 404
    up = await client.patch(
        f"{URL}/vectors/x", json={"metadata": {"b": 2}, "request_id": "u1"}, headers=RW
    )
    assert up.status_code == 200
    got = (await client.get(f"{URL}/vectors/x", headers=RW)).json()
    assert got["metadata"] == {"b": 2} and got["vector"] == [
        1,
        1,
        1,
        1,
    ]  # PATCH reemplaza metadatos
    d1 = await client.delete(f"{URL}/vectors/x", params={"request_id": "d1"}, headers=RW)
    d2 = await client.delete(f"{URL}/vectors/x", params={"request_id": "d2"}, headers=RW)
    assert d1.json()["existed"] is True and d2.json()["existed"] is False
    assert (await client.get(f"{URL}/vectors/x", headers=RW)).status_code == 404


@pytest.mark.parametrize(
    "body",
    [
        {"vector": [1, 2, 3], "request_id": "r"},  # dimensión
        {"vector": [1, 2, 3, 4], "metadata": {"a": {"b": 1}}, "request_id": "r"},
        {"vector": [1, 2, 3, 4]},  # sin request_id
        {"vector": [1, 2, 3, 4], "request_id": "r", "unknown": 1},
        {"vector": "nope", "request_id": "r"},
    ],
)
async def test_invalid_writes_are_400(client: httpx.AsyncClient, body: dict[str, Any]) -> None:
    r = await client.put(f"{URL}/vectors/a", json=body, headers=RW)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_argument"


async def test_unknown_collection_is_not_found(client: httpx.AsyncClient) -> None:
    r = await client.post("/v1/collections/zzz/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
    assert r.status_code == 404 and r.json()["error"]["code"] == "not_found"


async def test_batch_reports_per_item(client: httpx.AsyncClient) -> None:
    items = [
        {"id": "b1", "vector": [1, 0, 0, 0], "request_id": "rb1"},
        {"id": "b2", "vector": [1, 0], "request_id": "rb2"},  # inválido
        {"id": "b3", "vector": [0, 1, 0, 0], "metadata": {"t": "x"}, "request_id": "rb3"},
    ]
    r = await client.post(f"{URL}/vectors:batch", json={"items": items}, headers=RW)
    res = r.json()["results"]
    assert [x["ok"] for x in res] == [True, False, True]
    assert res[1]["error"]["code"] == "invalid_argument" and "token" in res[2]


# ───────────────────────────── lecturas ─────────────────────────────


async def test_search_exact_topk_with_filter_and_shape(client: httpx.AsyncClient) -> None:
    rng = np.random.default_rng(1)
    vecs = {f"v{i}": rng.normal(size=4).round(3).tolist() for i in range(25)}
    for i, (vid, v) in enumerate(vecs.items()):
        await put(client, vid, v, {"even": i % 2 == 0})
    r = await client.post(f"{URL}/search", json={"vector": [0, 0, 0, 0], "k": 5}, headers=RW)
    body = r.json()
    expected = sorted(vecs, key=lambda v: (float(np.linalg.norm(vecs[v])), v))[:5]
    assert [x["id"] for x in body["results"]] == expected
    assert body["partial"] is False and body["coverage"] == 1.0 and body["missing_shards"] == []
    assert body["consistency"] == "eventual" and set(body["shards"]) == {"0", "1"}
    assert body["query_id"].startswith("q-")
    f = await client.post(
        f"{URL}/search", json={"vector": [0, 0, 0, 0], "k": 5, "filter": {"even": True}}, headers=RW
    )
    assert all(x["metadata"]["even"] is True for x in f.json()["results"])


async def test_cosine_distance(client: httpx.AsyncClient) -> None:
    hdr = RW
    await client.put(
        "/v1/collections/demo_cos/vectors/a",
        json={"vector": [1, 0, 0, 0], "request_id": "c1"},
        headers=hdr,
    )
    await client.put(
        "/v1/collections/demo_cos/vectors/b",
        json={"vector": [0, 2, 0, 0], "request_id": "c2"},
        headers=hdr,
    )
    r = await client.post(
        "/v1/collections/demo_cos/search", json={"vector": [3, 0, 0, 0], "k": 2}, headers=hdr
    )
    res = r.json()["results"]
    assert res[0]["id"] == "a" and res[0]["distance"] == pytest.approx(0.0)
    assert res[1]["distance"] == pytest.approx(1.0)  # ortogonal: 1 − cos = 1


@pytest.mark.parametrize(
    "body",
    [
        {"vector": [0, 0, 0, 0], "k": 0},
        {"vector": [0, 0, 0, 0], "k": 101},
        {"vector": [0, 0, 0]},
        {"vector": [0, 0, 0, 0], "filter": {"x": {"$regex": "a"}}},
        {"vector": [0, 0, 0, 0], "consistency": "linearizable"},
        {"vector": [0, 0, 0, 0], "deadline_ms": 10},
    ],
)
async def test_invalid_searches_are_400(client: httpx.AsyncClient, body: dict[str, Any]) -> None:
    r = await client.post(f"{URL}/search", json=body, headers=RW)
    assert r.status_code == 400, r.json()


async def test_get_consistency_levels_and_validation(client: httpx.AsyncClient) -> None:
    tok = (await put(client, "g", [1, 2, 3, 4])).json()["token"]
    for level in ("eventual", "strong"):
        assert (
            await client.get(f"{URL}/vectors/g", params={"consistency": level}, headers=RW)
        ).status_code == 200
    ryw = await client.get(
        f"{URL}/vectors/g",
        params={"consistency": "read_your_writes", "min_seq": tok["seq"]},
        headers=RW,
    )
    assert ryw.status_code == 200 and ryw.json()["seq"] == tok["seq"]
    bad = await client.get(f"{URL}/vectors/g", params={"consistency": "weird"}, headers=RW)
    assert bad.status_code == 400


# ───────────────────────────── degradación ─────────────────────────────


async def test_partial_results_are_marked_and_allow_partial_false_is_503() -> None:
    app, transport = build()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://t") as c:
        await put(c, "p", [1, 1, 1, 1])
        transport.down.add(1)
        r = await c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
        body = r.json()
        assert r.status_code == 200 and body["partial"] is True and body["coverage"] == 0.5
        assert body["missing_shards"] == [{"shard": 1, "reason": "unreachable"}]
        assert body["shards"]["1"]["status"] == "unreachable"
        strict = await c.post(
            f"{URL}/search", json={"vector": [0, 0, 0, 0], "allow_partial": False}, headers=RW
        )
        assert strict.status_code == 503 and strict.json()["error"]["code"] == "unavailable"
        (s1_id,) = [v for v in (f"w{i}" for i in range(50)) if shard_for("demo", v, 2) == 1][:1]
        w = await put(c, s1_id, [1, 1, 1, 1])
        assert w.status_code == 503  # el primario del shard caído no está


# ───────────────────────────── backpressure ─────────────────────────────


class SlowSearch:
    def __init__(self, inner: CoordinatorBackend) -> None:
        self.inner, self.release = inner, asyncio.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def search(self, req: Any) -> Any:
        await self.release.wait()
        return await self.inner.search(req)


async def test_admission_rejects_immediately_when_full() -> None:
    cfg = make_config(admission={"search_concurrency": 1})
    slow: list[SlowSearch] = []
    app, _ = build(cfg, backend_wrapper=lambda b: slow.append(SlowSearch(b)) or slow[0])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://t") as c:
        first = asyncio.create_task(
            c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
        )
        await asyncio.sleep(0.05)
        second = await c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
        assert second.status_code == 429 and second.json()["error"]["code"] == "overloaded"
        slow[0].release.set()
        assert (await first).status_code == 200


async def test_rate_limit_per_key() -> None:
    app, _ = build(make_config(admission={"rate_per_key": 0.001, "burst_per_key": 2}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://t") as c:
        codes = [
            (await c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)).status_code
            for _ in range(3)
        ]
        assert codes == [200, 200, 429]
        assert (
            await c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RO)
        ).status_code == 200


# ───────────────────────────── observabilidad ─────────────────────────────


async def test_metrics_and_admin_status_shapes(client: httpx.AsyncClient) -> None:
    await put(client, "m", [1, 2, 3, 4])
    await client.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
    met = (await client.get("/v1/metrics", headers=RW)).json()
    assert set(met) == {"ops", "shards", "counters", "replication_lag"}
    assert met["ops"]["search"]["count"] == 1 and {"p50_ms", "p95_ms", "p99_ms"} <= set(
        met["ops"]["search"]
    )
    assert {"partial", "admission_rejected", "hedges", "stale_redirects"} <= set(met["counters"])
    st = (await client.get("/v1/admin/status", headers=ADMIN)).json()
    assert st["map_version"] == 1 and {r["replica_id"] for r in st["replicas"]} == {
        "s0/r0",
        "s1/r0",
    }


async def test_logs_carry_ids_but_never_the_api_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import logging

    from dvs.obs import setup_logging

    monkeypatch.setenv("DVS_RUN_DIR", str(tmp_path))
    path = setup_logging("gateway-test")
    try:
        app, _ = build()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://t") as c:
            await put(c, "k", [1, 2, 3, 4], rid="req-visible-123")
            await c.post(f"{URL}/search", json={"vector": [0, 0, 0, 0]}, headers=RW)
    finally:
        for h in list(logging.getLogger().handlers):
            h.flush()
    assert path is not None
    text = path.read_text()
    lines = [json.loads(line) for line in text.splitlines()]
    assert "dev-key-rw" not in text  # REQ-9.3
    assert any(x.get("request_id") == "req-visible-123" for x in lines)  # REQ-10.1
    assert any(x.get("event") == "search.done" and x.get("query_id") for x in lines)
    assert all({"ts", "mono", "component", "pid"} <= set(x) for x in lines)
