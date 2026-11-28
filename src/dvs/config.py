"""Deployment configuration (YAML) validated with pydantic.

The same file shape serves phase 1 (Ray) and phase 2 (native); sections that do not apply are
ignored. API keys are stored as sha256 hex digests, never in clear text.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dvs.core.model import CollectionSpec, Limits

_NAME_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ClusterCfg(_Strict):
    num_shards: int = Field(4, ge=1, le=256)
    replication_factor: int = Field(3, ge=1, le=9)
    write_quorum: int | None = Field(None, ge=1)  # None → majority of replication_factor
    placement: Literal["strict_spread", "spread", "pack"] = "strict_spread"

    @model_validator(mode="after")
    def _quorum(self) -> ClusterCfg:
        if self.write_quorum is not None and self.write_quorum > self.replication_factor:
            raise ValueError("write_quorum cannot exceed replication_factor")
        return self

    @property
    def quorum(self) -> int:
        return self.write_quorum or self.replication_factor // 2 + 1


class TimeoutsCfg(_Strict):
    search_deadline_default: int = Field(300, ge=1)
    search_deadline_min: int = Field(50, ge=1)
    search_deadline_max: int = Field(5000, ge=1)
    attempt: int = Field(120, ge=1)
    write: int = Field(2000, ge=1)
    suspect_ttl: int = Field(2000, ge=0)
    map_cache_ttl: int = Field(2000, ge=0)

    @model_validator(mode="after")
    def _order(self) -> TimeoutsCfg:
        if not (
            self.search_deadline_min <= self.search_deadline_default <= self.search_deadline_max
        ):
            raise ValueError("search deadlines must satisfy min <= default <= max")
        if self.attempt >= self.search_deadline_default:
            raise ValueError("attempt timeout must be shorter than the default search deadline")
        return self


class LimitsCfg(_Strict):
    k_max: int = Field(100, ge=1)
    max_batch: int = Field(1000, ge=1)
    max_id_bytes: int = Field(128, ge=1)
    max_request_id_bytes: int = Field(128, ge=1)
    max_metadata_bytes: int = Field(4096, ge=2)
    max_metadata_keys: int = Field(32, ge=0)
    max_key_len: int = Field(64, ge=1)
    max_list_len: int = Field(32, ge=0)
    max_filter_clauses: int = Field(16, ge=1)
    max_in_values: int = Field(256, ge=1)

    def to_core(self) -> Limits:
        return Limits(**self.model_dump())


class AdmissionCfg(_Strict):
    search_concurrency: int = Field(64, ge=1)
    write_concurrency: int = Field(256, ge=1)
    rate_per_key: float = Field(500, gt=0)
    burst_per_key: int = Field(1000, ge=1)


class ActorsCfg(_Strict):
    max_concurrency: int = Field(64, ge=1)
    max_restarts: int = Field(0, ge=-1)
    max_pending_calls: int = Field(1000, ge=-1)
    num_cpus: float = Field(0.5, ge=0)


class CollectionCfg(_Strict):
    name: str
    dim: int = Field(ge=1, le=4096)
    metric: Literal["l2", "cosine"]

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError("collection name must match [a-z0-9_-]{1,64}")
        return v

    def to_core(self) -> CollectionSpec:
        return CollectionSpec(self.name, self.dim, self.metric)


class ApiKeyCfg(_Strict):
    client: str
    read: list[str] = Field(default_factory=list)
    write: list[str] = Field(default_factory=list)
    admin: bool = False


class FaultInjectionCfg(_Strict):
    enabled: bool = False


class Config(_Strict):
    cluster: ClusterCfg = ClusterCfg()
    timeouts_ms: TimeoutsCfg = TimeoutsCfg()
    limits: LimitsCfg = LimitsCfg()
    admission: AdmissionCfg = AdmissionCfg()
    actors: ActorsCfg = ActorsCfg()
    collections: list[CollectionCfg]
    api_keys: dict[str, ApiKeyCfg] = Field(default_factory=dict)  # sha256(key) → permissions
    fault_injection: FaultInjectionCfg = FaultInjectionCfg()

    @model_validator(mode="after")
    def _consistency(self) -> Config:
        names = [c.name for c in self.collections]
        if not names:
            raise ValueError("at least one collection is required")
        if len(set(names)) != len(names):
            raise ValueError("duplicate collection names")
        known = set(names)
        for digest, perms in self.api_keys.items():
            if not _SHA256_RE.match(digest):
                raise ValueError(f"api_keys entries must be sha256 hex digests: {digest[:12]}…")
            unknown = (set(perms.read) | set(perms.write)) - known
            if unknown:
                raise ValueError(f"api key '{perms.client}' references unknown {sorted(unknown)}")
        return self

    def collection(self, name: str) -> CollectionSpec | None:
        for c in self.collections:
            if c.name == name:
                return c.to_core()
        return None

    @property
    def collection_specs(self) -> tuple[CollectionSpec, ...]:
        return tuple(c.to_core() for c in self.collections)


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    """Load from `path`, or from the `DVS_CONFIG` environment variable."""
    target = path or os.environ.get("DVS_CONFIG")
    if not target:
        raise ValueError("no config path given and DVS_CONFIG is not set")
    raw = yaml.safe_load(Path(target).read_text(encoding="utf-8"))
    # Experiment override (E1 runs with and without actor restarts on the same mounted config).
    restarts = os.environ.get("DVS_ACTORS_MAX_RESTARTS", "").strip()
    if restarts and isinstance(raw, dict):
        actors = raw.setdefault("actors", {})
        if isinstance(actors, dict):
            actors["max_restarts"] = restarts
    return Config.model_validate(raw)
