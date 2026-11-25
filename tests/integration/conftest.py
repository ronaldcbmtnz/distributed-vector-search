"""Fixtures de integración: Ray local por módulo (no hace falta Docker)."""

from collections.abc import Iterator

import pytest

ray = pytest.importorskip("ray")


@pytest.fixture(scope="module")
def ray_local() -> Iterator[None]:
    ray.init(num_cpus=16, include_dashboard=False, log_to_driver=False, namespace="dvs-test")
    yield
    ray.shutdown()
