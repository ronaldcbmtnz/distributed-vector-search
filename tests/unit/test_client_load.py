from __future__ import annotations

import json
import shutil
import uuid
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from dvs.client.load import load_dataset


class FakeClient:
    def __init__(self, fail_id: str | None = None) -> None:
        self.calls: list[tuple[str, list[dict[str, Any]]]] = []
        self.fail_id = fail_id

    def batch(self, collection: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        self.calls.append((collection, items))
        return {"results": [{"id": item["id"], "ok": item["id"] != self.fail_id} for item in items]}


def _dataset(path: Path, *, n: int = 5, metric: str = "L2") -> None:
    path.mkdir()
    (path / "manifest.json").write_text(
        json.dumps({"seed": 7, "n": n, "dim": 2, "metric": metric}), encoding="utf-8"
    )
    vectors = np.asarray([[float(index + 1), 0.0] for index in range(n)], dtype=np.float32)
    np.save(path / "vectors.npy", vectors)
    (path / "ids.txt").write_text("".join(f"id-{index}\n" for index in range(n)), encoding="utf-8")
    (path / "metadata.jsonl").write_text(
        "".join(json.dumps({"index": index}) + "\n" for index in range(n)), encoding="utf-8"
    )


@pytest.fixture
def data_dir() -> Iterator[Path]:
    path = Path.cwd() / ".test-data" / uuid.uuid4().hex
    path.mkdir(parents=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
        with suppress(OSError):
            path.parent.rmdir()


def test_load_dataset_streams_contract_sized_batches(data_dir: Path) -> None:
    data_dir = data_dir / "synthetic"
    _dataset(data_dir)
    client = FakeClient()

    result = load_dataset(client, "demo", data_dir, batch_size=2)  # type: ignore[arg-type]

    assert result["vectors"] == 5
    assert result["batches"] == 3
    assert result["succeeded"] == 5 and result["failed"] == 0
    assert [len(items) for _, items in client.calls] == [2, 2, 1]
    assert [item["id"] for _, batch in client.calls for item in batch] == [
        f"id-{index}" for index in range(5)
    ]


def test_load_dataset_reports_per_item_batch_errors(data_dir: Path) -> None:
    data_dir = data_dir / "synthetic"
    _dataset(data_dir, n=2)
    client = FakeClient(fail_id="id-1")

    result = load_dataset(client, "demo", data_dir, batch_size=2)  # type: ignore[arg-type]

    assert result["succeeded"] == 1 and result["failed"] == 1
    assert result["errors"] == [{"id": "id-1", "code": "invalid_response"}]


def test_load_dataset_rejects_mismatched_files_before_writing(data_dir: Path) -> None:
    data_dir = data_dir / "synthetic"
    _dataset(data_dir, n=3)
    (data_dir / "ids.txt").write_text("only-one-id\n", encoding="utf-8")
    client = FakeClient()

    with pytest.raises(ValueError, match=r"ids.txt contains"):
        load_dataset(client, "demo", data_dir)  # type: ignore[arg-type]
    assert client.calls == []
