"""Bounded exponential backoff with full jitter for transient HTTP failures."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry configuration. ``max_retries`` excludes the initial attempt."""

    max_retries: int = 3
    base_delay_s: float = 0.05
    max_delay_s: float = 1.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ValueError("max_retries must be non-negative")
        if (
            not math.isfinite(self.base_delay_s)
            or not math.isfinite(self.max_delay_s)
            or self.base_delay_s < 0
            or self.max_delay_s < 0
        ):
            raise ValueError("retry delays must be non-negative")
        if self.max_delay_s < self.base_delay_s:
            raise ValueError("max_delay_s must be greater than or equal to base_delay_s")

    def delay(self, retry_number: int, random_value: float) -> float:
        """Return full-jitter delay for a zero-based retry number."""
        if retry_number < 0:
            raise ValueError("retry_number must be non-negative")
        jitter = min(1.0, max(0.0, random_value))
        ceiling = min(self.max_delay_s, self.base_delay_s * (2**retry_number))
        return ceiling * jitter
