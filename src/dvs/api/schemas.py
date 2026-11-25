"""Request bodies of the contract. Field types are loose on purpose: semantic validation is done
by dvs.core.validation so every phase reports the same `invalid_argument` errors."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class UpsertBody(_Body):
    vector: Any
    metadata: Any = None
    request_id: Any


class InsertBody(_Body):
    id: Any
    vector: Any
    metadata: Any = None
    request_id: Any


class UpdateBody(_Body):
    vector: Any = None
    metadata: Any = None
    request_id: Any


class BatchBody(_Body):
    items: list[InsertBody] = Field(min_length=1)


class Token(_Body):
    shard: int = Field(ge=0)
    seq: int = Field(ge=0)


class SearchBody(_Body):
    vector: Any
    k: Any = 10
    filter: Any = None
    consistency: str = "eventual"
    tokens: list[Token] = Field(default_factory=list)
    allow_partial: bool = True
    deadline_ms: Any = None
