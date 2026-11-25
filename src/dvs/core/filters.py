"""Metadata filter language (REQ-2.3): parse → validated AST → compiled predicate.

Grammar (all clauses are combined with AND):

    {"field": value}                      equality
    {"field": {"$in": [v1, v2, ...]}}     membership
    {"field": {"$gte": a, "$lt": b}}      numeric range ($gt, $gte, $lt, $lte; any subset)

Semantics:
- A missing field never matches.
- Equality is type-strict: `True` does not equal `1` (unlike Python's `==`); ints and floats
  compare numerically.
- If the metadata value is a list, a clause matches when ANY element matches.
- Ranges only match numbers (never bools or strings).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from dvs.core.errors import InvalidArgument
from dvs.core.model import JSONScalar, Metadata, MetaValue

_RANGE_OPS = ("$gt", "$gte", "$lt", "$lte")


@dataclass(frozen=True, slots=True)
class Eq:
    field: str
    value: JSONScalar


@dataclass(frozen=True, slots=True)
class In:
    field: str
    values: tuple[JSONScalar, ...]


@dataclass(frozen=True, slots=True)
class Range:
    field: str
    gt: float | None = None
    gte: float | None = None
    lt: float | None = None
    lte: float | None = None


Clause = Eq | In | Range


@dataclass(frozen=True, slots=True)
class Filter:
    clauses: tuple[Clause, ...]


Predicate = Callable[[Metadata], bool]


def _is_number(v: object) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)


def _is_scalar(v: object) -> bool:
    if isinstance(v, bool | str):
        return True
    return _is_number(v) and math.isfinite(float(v))  # type: ignore[arg-type]


def parse(
    raw: Mapping[str, object] | None,
    *,
    max_clauses: int = 16,
    max_in_values: int = 256,
    max_key_len: int = 64,
) -> Filter | None:
    """Validate a client filter. Returns None for "no filter". Raises InvalidArgument."""
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise InvalidArgument("filter must be an object")
    if not raw:
        return None
    if len(raw) > max_clauses:
        raise InvalidArgument("filter has too many clauses", max_clauses=max_clauses)
    clauses: list[Clause] = []
    for fld, spec in raw.items():
        if not isinstance(fld, str) or not fld or len(fld) > max_key_len or fld.startswith("$"):
            raise InvalidArgument("invalid filter field name", field=str(fld)[:max_key_len])
        clauses.append(_parse_clause(fld, spec, max_in_values))
    # Canonical order: same logical filter → same AST, whatever the client's key order.
    clauses.sort(key=lambda c: (c.field, type(c).__name__))
    return Filter(tuple(clauses))


def _parse_clause(fld: str, spec: object, max_in_values: int) -> Clause:
    if not isinstance(spec, Mapping):
        if not _is_scalar(spec):
            raise InvalidArgument("filter value must be a finite scalar", field=fld)
        return Eq(fld, spec)  # type: ignore[arg-type]
    ops = set(spec)
    if not ops:
        raise InvalidArgument("empty operator object", field=fld)
    if "$in" in ops:
        if ops != {"$in"}:
            raise InvalidArgument("$in cannot be combined with other operators", field=fld)
        values = spec["$in"]
        if not isinstance(values, list) or not values:
            raise InvalidArgument("$in needs a non-empty list", field=fld)
        if len(values) > max_in_values:
            raise InvalidArgument("$in list too long", field=fld, max_in_values=max_in_values)
        if not all(_is_scalar(v) for v in values):
            raise InvalidArgument("$in values must be finite scalars", field=fld)
        return In(fld, tuple(values))
    unknown = ops - set(_RANGE_OPS)
    if unknown:
        raise InvalidArgument("unknown filter operator", field=fld, operators=sorted(unknown))
    bounds: dict[str, float] = {}
    for op in ops:
        v = spec[op]
        if not _is_number(v) or not math.isfinite(float(v)):
            raise InvalidArgument("range bounds must be finite numbers", field=fld)
        bounds[op[1:]] = float(v)
    return Range(fld, **bounds)


# ───────────────────────────── evaluation ─────────────────────────────


def _scalar_eq(a: object, b: object) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a is b
    if _is_number(a) and _is_number(b):
        return float(a) == float(b)  # type: ignore[arg-type]
    return type(a) is type(b) and a == b


def _any(value: MetaValue, test: Callable[[object], bool]) -> bool:
    if isinstance(value, list):
        return any(test(v) for v in value)
    return test(value)


def _in_range(r: Range) -> Callable[[object], bool]:
    def test(v: object) -> bool:
        if not _is_number(v):
            return False
        x = float(v)  # type: ignore[arg-type]
        return (
            (r.gt is None or x > r.gt)
            and (r.gte is None or x >= r.gte)
            and (r.lt is None or x < r.lt)
            and (r.lte is None or x <= r.lte)
        )

    return test


def _clause_test(c: Clause) -> Callable[[object], bool]:
    if isinstance(c, Eq):
        return lambda v: _scalar_eq(v, c.value)
    if isinstance(c, In):
        return lambda v: any(_scalar_eq(v, x) for x in c.values)
    return _in_range(c)


def compile_filter(flt: Filter | None) -> Predicate | None:
    """Turn a parsed filter into a predicate over a metadata dict (None = match all)."""
    if flt is None or not flt.clauses:
        return None
    tests = [(c.field, _clause_test(c)) for c in flt.clauses]

    def predicate(meta: Metadata) -> bool:
        return all(fld in meta and _any(meta[fld], test) for fld, test in tests)

    return predicate
