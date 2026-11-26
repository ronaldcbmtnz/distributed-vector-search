"""Thread-safe JSONL operation history writer."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any


class HistoryWriter:
    """Append one redacted JSON object per logical client operation."""

    def __init__(self, path: str | Path | None) -> None:
        self.path = Path(path) if path is not None else None
        self._lock = threading.Lock()

    def write(self, record: dict[str, Any]) -> None:
        if self.path is None:
            return
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as history:
                history.write(line + "\n")
