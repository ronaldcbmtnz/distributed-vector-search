"""T102b — validación sin estado: cada regla y su caso límite."""

import numpy as np
import pytest

from dvs.core.errors import InvalidArgument
from dvs.core.model import CollectionSpec, Limits
from dvs.core.validation import (
    make_write_op,
    validate_deadline_ms,
    validate_id,
    validate_k,
    validate_metadata,
    validate_vector,
)

L2 = CollectionSpec("demo", 4, "l2")
COS = CollectionSpec("demo_cos", 4, "cosine")
LIM = Limits()


# ─── ids ───
def test_id_ok_and_limits() -> None:
    assert validate_id("a" * 128, 128) == "a" * 128
    assert validate_id("ñandú-1", 128) == "ñandú-1"
    for bad in ["", None, 5, "a" * 129, "tab\there", "nl\n"]:
        with pytest.raises(InvalidArgument):
            validate_id(bad, 128)


def test_id_limit_counts_utf8_bytes() -> None:
    with pytest.raises(InvalidArgument):
        validate_id("ñ" * 65, 128)  # 130 bytes


# ─── vectors ───
def test_vector_l2_kept_as_float32() -> None:
    v = validate_vector([1, 2, 3, 4], L2)
    assert v.dtype == np.float32 and v.tolist() == [1.0, 2.0, 3.0, 4.0]


def test_vector_cosine_normalized() -> None:
    v = validate_vector([3, 0, 4, 0], COS)
    assert np.isclose(np.linalg.norm(v), 1.0)
    assert np.allclose(v, [0.6, 0, 0.8, 0])


@pytest.mark.parametrize(
    "raw",
    [
        [1, 2, 3],
        [1, 2, 3, 4, 5],
        [[1, 2], [3, 4]],
        [1, 2, float("nan"), 4],
        [1, 2, float("inf"), 4],
        [1, 2, 1e39, 4],  # desborda float32
        [True, 1, 2, 3],
        ["1", "2", "3", "x"],
        "1234",
        None,
    ],
)
def test_vector_invalid(raw: object) -> None:
    with pytest.raises(InvalidArgument):
        validate_vector(raw, L2)


def test_zero_vector_rejected_only_in_cosine() -> None:
    validate_vector([0, 0, 0, 0], L2)
    with pytest.raises(InvalidArgument):
        validate_vector([0, 0, 0, 0], COS)


# ─── metadata ───
def test_metadata_ok() -> None:
    m = {"cat": "c1", "price": 9.5, "n": 3, "ok": False, "tags": ["a", "b"]}
    assert validate_metadata(m, LIM) == m
    assert validate_metadata(None, LIM) == {}


@pytest.mark.parametrize(
    "raw",
    [
        "x",
        {"": 1},
        {"$x": 1},
        {"k" * 65: 1},
        {"a": None},
        {"a": {"nested": 1}},
        {"a": float("nan")},
        {"a": [1, [2]]},
        {"a": list(range(33))},
        {f"k{i}": i for i in range(33)},
        {"big": "x" * 5000},
    ],
)
def test_metadata_invalid(raw: object) -> None:
    with pytest.raises(InvalidArgument):
        validate_metadata(raw, LIM)


# ─── search params ───
def test_k_bounds() -> None:
    assert validate_k(1, LIM) == 1 and validate_k(100, LIM) == 100
    for bad in [0, 101, -1, 1.5, True, "3"]:
        with pytest.raises(InvalidArgument):
            validate_k(bad, LIM)


def test_deadline_bounds() -> None:
    assert validate_deadline_ms(50, 50, 5000) == 50
    for bad in [49, 5001, 100.0, None]:
        with pytest.raises(InvalidArgument):
            validate_deadline_ms(bad, 50, 5000)


# ─── write ops ───
def test_make_upsert_and_fingerprint() -> None:
    a = make_write_op("upsert", L2, "v1", "r1", LIM, [1, 2, 3, 4], {"c": 1})
    b = make_write_op("upsert", L2, "v1", "r2", LIM, [1, 2, 3, 4], {"c": 1})
    c = make_write_op("upsert", L2, "v1", "r1", LIM, [1, 2, 3, 5], {"c": 1})
    d = make_write_op("upsert", L2, "v1", "r1", LIM, [1, 2, 3, 4], {"c": 2})
    assert a.fingerprint == b.fingerprint  # el request_id no forma parte del contenido
    assert len({a.fingerprint, c.fingerprint, d.fingerprint}) == 3
    assert a.metadata == {"c": 1} and a.vector is not None


def test_fingerprint_ignores_metadata_key_order() -> None:
    a = make_write_op("upsert", L2, "v", "r", LIM, [1, 2, 3, 4], {"x": 1, "y": 2})
    b = make_write_op("upsert", L2, "v", "r", LIM, [1, 2, 3, 4], {"y": 2, "x": 1})
    assert a.fingerprint == b.fingerprint


def test_write_kind_rules() -> None:
    with pytest.raises(InvalidArgument):
        make_write_op("upsert", L2, "v", "r", LIM)  # sin vector
    with pytest.raises(InvalidArgument):
        make_write_op("update", L2, "v", "r", LIM)  # sin nada que actualizar
    up = make_write_op("update", L2, "v", "r", LIM, metadata={"a": 1})
    assert up.vector is None and up.metadata == {"a": 1}
    de = make_write_op("delete", L2, "v", "r", LIM, vector=[1, 2, 3, 4])
    assert de.vector is None and de.metadata is None
    with pytest.raises(InvalidArgument):
        make_write_op("upsert", L2, "v", "", LIM, [1, 2, 3, 4])  # request_id vacío
