"""Stateless input validation (REQ-1.6, REQ-2.6, REQ-2.7).

Runs at the edge (gateway) so invalid input never reaches a shard log. Stateful conditions
(insert-if-absent, update-if-present) are NOT checked here: they are evaluated when the entry is
applied, identically on every replica (see dvs.core.shard_core).
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from dvs.core.errors import InvalidArgument
from dvs.core.model import CollectionSpec, Limits, Metadata, MetaValue, Vector, WriteKind, WriteOp

# ───────────────────────────── identifiers ─────────────────────────────


def validate_id(value: object, max_bytes: int, what: str = "id") -> str:
    if not isinstance(value, str) or not value:
        raise InvalidArgument(f"{what} must be a non-empty string")
    if len(value.encode("utf-8")) > max_bytes:
        raise InvalidArgument(f"{what} too long", max_bytes=max_bytes)
    if any(unicodedata.category(ch) == "Cc" for ch in value):
        raise InvalidArgument(f"{what} contains control characters")
    return value


# ───────────────────────────── vectors ─────────────────────────────


def validate_vector(raw: object, spec: CollectionSpec) -> Vector:
    """Return a float32 copy (unit-norm for cosine collections) or raise InvalidArgument."""
    if isinstance(raw, list | tuple) and any(isinstance(x, bool) for x in raw):
        raise InvalidArgument("vector elements must be numbers")
    if isinstance(raw, str | bytes) or raw is None:
        raise InvalidArgument("vector must be a list of numbers")
    try:
        arr64 = np.asarray(raw, dtype=np.float64)
    except (TypeError, ValueError) as e:
        raise InvalidArgument("vector must be a list of numbers") from e
    if arr64.ndim != 1 or arr64.shape[0] != spec.dim:
        raise InvalidArgument("wrong vector dimension", expected=spec.dim, got=int(arr64.size))
    if not np.all(np.isfinite(arr64)):
        raise InvalidArgument("vector contains NaN or Inf")
    if spec.metric == "cosine":
        norm = float(np.linalg.norm(arr64))
        if norm == 0.0:
            raise InvalidArgument("zero vector is not allowed in a cosine collection")
        arr64 = arr64 / norm
    if float(np.max(np.abs(arr64), initial=0.0)) > float(np.finfo(np.float32).max):
        raise InvalidArgument("vector values overflow float32")
    return arr64.astype(np.float32)


# ───────────────────────────── metadata ─────────────────────────────


def _valid_scalar(v: object) -> bool:
    if isinstance(v, bool | str):
        return True
    if isinstance(v, int | float):
        return math.isfinite(float(v))
    return False


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def validate_metadata(raw: object, limits: Limits) -> Metadata:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise InvalidArgument("metadata must be an object")
    if len(raw) > limits.max_metadata_keys:
        raise InvalidArgument("too many metadata keys", max_keys=limits.max_metadata_keys)
    out: Metadata = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key or len(key) > limits.max_key_len:
            raise InvalidArgument("invalid metadata key", max_key_len=limits.max_key_len)
        if key.startswith("$"):
            raise InvalidArgument("metadata keys cannot start with '$'", key=key)
        if isinstance(value, list):
            if len(value) > limits.max_list_len:
                raise InvalidArgument("metadata list too long", key=key)
            if not all(_valid_scalar(x) for x in value):
                raise InvalidArgument("metadata list items must be finite scalars", key=key)
            out[key] = list(value)
        elif _valid_scalar(value):
            out[key] = value
        else:
            raise InvalidArgument("metadata values must be finite scalars or lists", key=key)
    if len(canonical_json(out).encode("utf-8")) > limits.max_metadata_bytes:
        raise InvalidArgument("metadata too large", max_bytes=limits.max_metadata_bytes)
    return out


# ───────────────────────────── search params ─────────────────────────────


def validate_k(k: object, limits: Limits) -> int:
    if not isinstance(k, int) or isinstance(k, bool) or not 1 <= k <= limits.k_max:
        raise InvalidArgument("k out of range", min=1, max=limits.k_max)
    return k


def validate_deadline_ms(value: object, minimum: int, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise InvalidArgument("deadline_ms out of range", min=minimum, max=maximum)
    return value


def validate_batch_size(n: int, limits: Limits) -> None:
    if not 1 <= n <= limits.max_batch:
        raise InvalidArgument("batch size out of range", min=1, max=limits.max_batch)


# ───────────────────────────── writes ─────────────────────────────


def fingerprint(
    kind: WriteKind, collection: str, vid: str, vector: Vector | None, metadata: Metadata | None
) -> bytes:
    """Digest of the write payload: equal payload ⇔ equal fingerprint."""
    h = hashlib.blake2b(digest_size=16)
    for part in (kind, collection, vid):
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    h.update(b"V" + vector.tobytes() if vector is not None else b"-")
    h.update(b"\x00")
    h.update(canonical_json(metadata).encode("utf-8") if metadata is not None else b"-")
    return h.digest()


def make_write_op(
    kind: WriteKind,
    spec: CollectionSpec,
    vid: object,
    request_id: object,
    limits: Limits,
    vector: Sequence[float] | object | None = None,
    metadata: Mapping[str, MetaValue] | object | None = None,
) -> WriteOp:
    """Validate a client write and build the WriteOp that will go to the shard log."""
    vid_s = validate_id(vid, limits.max_id_bytes)
    rid = validate_id(request_id, limits.max_request_id_bytes, what="request_id")
    vec: Vector | None
    meta: Metadata | None
    if kind in ("upsert", "insert"):
        if vector is None:
            raise InvalidArgument(f"{kind} requires a vector")
        vec = validate_vector(vector, spec)
        meta = validate_metadata(metadata, limits)
    elif kind == "update":
        if vector is None and metadata is None:
            raise InvalidArgument("update requires vector and/or metadata")
        vec = validate_vector(vector, spec) if vector is not None else None
        meta = validate_metadata(metadata, limits) if metadata is not None else None
    elif kind == "delete":
        vec, meta = None, None
    else:  # pragma: no cover - guarded by the type
        raise InvalidArgument("unknown write kind")
    return WriteOp(
        kind=kind,
        collection=spec.name,
        id=vid_s,
        request_id=rid,
        vector=vec,
        metadata=meta,
        fingerprint=fingerprint(kind, spec.name, vid_s, vec, meta),
    )
