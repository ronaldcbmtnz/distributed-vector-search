"""T103 — lenguaje de filtros: parseo, validación y evaluación."""

import pytest

from dvs.core.errors import InvalidArgument
from dvs.core.filters import Eq, In, Range, compile_filter, parse

META = {"cat": "c1", "price": 50, "tags": ["t1", "t3"], "flag": True, "score": 0.5}


def matches(raw: dict[str, object], meta: dict[str, object] = META) -> bool:
    pred = compile_filter(parse(raw))
    assert pred is not None
    return pred(meta)  # type: ignore[arg-type]


def test_none_and_empty_mean_no_filter() -> None:
    assert parse(None) is None
    assert parse({}) is None
    assert compile_filter(None) is None


def test_equality() -> None:
    assert matches({"cat": "c1"})
    assert not matches({"cat": "c2"})


def test_equality_is_type_strict() -> None:
    assert matches({"flag": True})
    assert not matches({"flag": 1})
    assert not matches({"price": "50"})
    assert matches({"price": 50.0})  # int y float comparan numéricamente


def test_list_field_contains() -> None:
    assert matches({"tags": "t3"})
    assert not matches({"tags": "t2"})


def test_in() -> None:
    assert matches({"cat": {"$in": ["c0", "c1"]}})
    assert not matches({"cat": {"$in": ["c0", "c2"]}})
    assert matches({"tags": {"$in": ["t9", "t1"]}})


def test_range() -> None:
    assert matches({"price": {"$gte": 50, "$lt": 60}})
    assert not matches({"price": {"$gt": 50}})
    assert matches({"score": {"$lte": 0.5}})
    assert not matches({"cat": {"$gte": 0}})  # string nunca cumple un rango
    assert not matches({"flag": {"$gte": 0}})  # bool tampoco


def test_conjunction_and_missing_field() -> None:
    assert matches({"cat": "c1", "price": {"$lt": 100}, "tags": "t1"})
    assert not matches({"cat": "c1", "price": {"$lt": 10}})
    assert not matches({"missing": "x"})


def test_canonical_order() -> None:
    a = parse({"price": {"$lt": 3}, "cat": "c1"})
    b = parse({"cat": "c1", "price": {"$lt": 3}})
    assert a == b
    assert a is not None and isinstance(a.clauses[0], Eq) and isinstance(a.clauses[1], Range)
    assert isinstance(parse({"x": {"$in": [1]}}).clauses[0], In)  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "raw",
    [
        ["not", "a", "dict"],
        {"cat": {"$regex": "c.*"}},
        {"cat": {}},
        {"cat": {"$in": []}},
        {"cat": {"$in": "c1"}},
        {"cat": {"$in": ["c1"], "$gt": 1}},
        {"price": {"$gt": "10"}},
        {"price": {"$gt": float("nan")}},
        {"price": float("inf")},
        {"cat": None},
        {"cat": ["c1"]},
        {"$or": "x"},
        {"": "x"},
    ],
)
def test_invalid_filters(raw: object) -> None:
    with pytest.raises(InvalidArgument):
        parse(raw)  # type: ignore[arg-type]


def test_limits() -> None:
    with pytest.raises(InvalidArgument):
        parse({f"f{i}": 1 for i in range(17)}, max_clauses=16)
    with pytest.raises(InvalidArgument):
        parse({"x": {"$in": list(range(300))}}, max_in_values=256)
    with pytest.raises(InvalidArgument):
        parse({"k" * 65: 1}, max_key_len=64)
