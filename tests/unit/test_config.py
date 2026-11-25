"""T109 — configuración: carga del YAML de ejemplo y rechazo de configuraciones incoherentes."""

import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from dvs.config import Config, load_config

EXAMPLE = Path(__file__).resolve().parents[2] / "deploy" / "ray" / "config.ray.yaml"


def _minimal(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {"collections": [{"name": "demo", "dim": 4, "metric": "l2"}]}
    base.update(overrides)
    return base


def test_example_config_loads() -> None:
    cfg = load_config(EXAMPLE)
    assert cfg.cluster.quorum == 2
    assert cfg.collection("demo") is not None
    assert cfg.collection("nope") is None


def test_example_contains_agreed_dev_keys() -> None:
    cfg = load_config(EXAMPLE)
    rw = cfg.api_keys[hashlib.sha256(b"dev-key-rw").hexdigest()]
    ro = cfg.api_keys[hashlib.sha256(b"dev-key-ro").hexdigest()]
    admin = cfg.api_keys[hashlib.sha256(b"dev-admin").hexdigest()]
    assert set(rw.write) == {"demo", "demo_cos"}
    assert ro.read == ["demo"] and ro.write == []
    assert admin.admin


def test_load_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DVS_CONFIG", str(EXAMPLE))
    assert load_config().cluster.num_shards >= 1


def test_default_quorum_is_majority() -> None:
    cfg = Config.model_validate(_minimal(cluster={"replication_factor": 5}))
    assert cfg.cluster.quorum == 3


@pytest.mark.parametrize(
    "overrides",
    [
        {"cluster": {"replication_factor": 3, "write_quorum": 4}},
        {"collections": [{"name": "demo", "dim": 0, "metric": "l2"}]},
        {"collections": [{"name": "Bad Name", "dim": 4, "metric": "l2"}]},
        {
            "collections": [
                {"name": "a", "dim": 4, "metric": "l2"},
                {"name": "a", "dim": 4, "metric": "l2"},
            ]
        },
        {"collections": []},
        {"timeouts_ms": {"attempt": 400}},
        {"timeouts_ms": {"search_deadline_min": 500}},
        {"api_keys": {"not-a-digest": {"client": "x"}}},
        {"api_keys": {"0" * 64: {"client": "x", "read": ["missing"]}}},
        {"unknown_section": {}},
    ],
)
def test_rejects_incoherent_config(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Config.model_validate(_minimal(**overrides))
