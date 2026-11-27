"""Revisión del runner: las capturas de estado/métricas usan la autenticación del contrato.

Antes se enviaba `X-API-Key` y la API respondía 401, así que `status_*.json` y `metrics_*.json`
de cada experimento quedaban sin evidencia.
"""

from typing import Any

import httpx
import pytest

from experiments import runner


def test_admin_snapshot_sends_bearer_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_get(url: str, headers: dict[str, str], timeout: float) -> httpx.Response:
        captured.update(url=url, headers=headers)
        return httpx.Response(200, json={"replicas": []}, request=httpx.Request("GET", url))

    monkeypatch.setattr(runner.httpx, "get", fake_get)
    snap = runner._admin_snapshot("http://gw:8080/", "dev-admin", "admin/status")
    assert captured["url"] == "http://gw:8080/v1/admin/status"
    assert captured["headers"] == {"Authorization": "Bearer dev-admin"}
    assert snap == {"http_status": 200, "body": {"replicas": []}}
