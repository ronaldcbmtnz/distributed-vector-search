"""T101 — hash estable (colección, id) → shard."""

import os
import subprocess
import sys
from collections import Counter

import pytest

from dvs.core.hashing import shard_for

IDS = [f"vec-{i}" for i in range(50)]


def _in_subprocess(seed: str) -> list[int]:
    code = (
        "from dvs.core.hashing import shard_for\n"
        f"print(','.join(str(shard_for('demo', i, 8)) for i in {IDS!r}))"
    )
    env = {**os.environ, "PYTHONHASHSEED": seed}
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    ).stdout.strip()
    return [int(x) for x in out.split(",")]


def test_stable_across_processes_with_different_hash_seeds() -> None:
    here = [shard_for("demo", i, 8) for i in IDS]
    assert _in_subprocess("1") == here
    assert _in_subprocess("12345") == here


def test_collection_is_part_of_the_key() -> None:
    ids = [f"x{i}" for i in range(200)]
    a = [shard_for("a", i, 16) for i in ids]
    b = [shard_for("b", i, 16) for i in ids]
    assert a != b


def test_no_concatenation_ambiguity() -> None:
    assert shard_for("ab", "c", 1 << 30) != shard_for("a", "bc", 1 << 30)


def test_uniform_distribution() -> None:
    n, s = 10_000, 8
    counts = Counter(shard_for("demo", f"id-{i}", s) for i in range(n))
    expected = n / s
    chi2 = sum((counts[k] - expected) ** 2 / expected for k in range(s))
    # 7 grados de libertad: chi² < 24.32 con p > 0.001
    assert chi2 < 24.32, counts


def test_single_shard_and_invalid() -> None:
    assert shard_for("demo", "anything", 1) == 0
    with pytest.raises(ValueError):
        shard_for("demo", "x", 0)
