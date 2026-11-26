"""Checker for mixed vector/metadata versions in E3 observations."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


def check_observations(path: str | Path, *, tolerance: float = 1e-6) -> dict[str, Any]:
    """Check JSONL GET snapshots; a snapshot is mixed when metadata.ver != vector[0]."""
    if not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError("tolerance must be finite and non-negative")

    checked = mixed = invalid = 0
    examples: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, raw in enumerate(stream, start=1):
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}") from exc
            if not isinstance(record, dict):
                invalid += 1
                continue
            snapshot = record.get("response", record)
            vector = snapshot.get("vector") if isinstance(snapshot, dict) else None
            metadata = snapshot.get("metadata") if isinstance(snapshot, dict) else None
            version = metadata.get("ver") if isinstance(metadata, dict) else None
            first_value = vector[0] if isinstance(vector, list) and vector else None
            if (
                isinstance(version, bool)
                or not isinstance(version, (int, float))
                or not math.isfinite(float(version))
                or isinstance(first_value, bool)
                or not isinstance(first_value, (int, float))
                or not math.isfinite(float(first_value))
            ):
                invalid += 1
                if len(examples) < 10:
                    examples.append({"line": line_number, "kind": "invalid_snapshot"})
                continue
            checked += 1
            if not math.isclose(float(version), float(first_value), rel_tol=0.0, abs_tol=tolerance):
                mixed += 1
                if len(examples) < 10:
                    examples.append(
                        {
                            "line": line_number,
                            "id": snapshot.get("id"),
                            "metadata_ver": version,
                            "vector_0": first_value,
                        }
                    )
    return {
        "observations": checked + invalid,
        "checked": checked,
        "mixed": mixed,
        "invalid": invalid,
        "tolerance": tolerance,
        "ok": mixed == 0 and invalid == 0,
        "examples": examples,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observations", type=Path, help="JSONL snapshots returned by E3 readers")
    parser.add_argument("--tolerance", type=float, default=1e-6)
    args = parser.parse_args(argv)
    try:
        result = check_observations(args.observations, tolerance=args.tolerance)
    except (OSError, ValueError) as exc:
        print(f"E3 checker failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
