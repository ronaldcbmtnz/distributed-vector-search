"""Wire phase 1 together: Catalog (+ replicas) on Ray and a CoordinatorBackend on top."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dvs.config import Config
from dvs.coordinator.backend import CoordinatorBackend
from dvs.ray_impl.catalog_actor import Catalog
from dvs.ray_impl.transport import RayShardTransport


@dataclass
class RayDeployment:
    catalog: Any
    transport: RayShardTransport
    backend: CoordinatorBackend


async def start(
    config: Config,
    *,
    cluster: str = "dvs",
    detached: bool = True,
    namespace: str | None = None,
) -> RayDeployment:
    """Create or reuse the Catalog, bootstrap replicas and return a ready backend.

    Requires `ray.init(...)` to have been called by the process (the gateway is a Ray driver).
    """
    catalog = Catalog.options(  # type: ignore[attr-defined]
        name=f"{cluster}:catalog",
        lifetime="detached" if detached else None,
        get_if_exists=True,
        num_cpus=0,
    ).remote(config.model_dump(), cluster, detached)
    await catalog.bootstrap.remote()
    transport = RayShardTransport(catalog, cluster=cluster, namespace=namespace)
    t = config.timeouts_ms
    backend = CoordinatorBackend(
        transport,
        attempt_s=t.attempt / 1000.0,
        write_timeout_s=t.write / 1000.0,
        suspect_ttl_s=t.suspect_ttl / 1000.0,
        map_ttl_s=t.map_cache_ttl / 1000.0,
    )
    return RayDeployment(catalog, transport, backend)


async def stop(dep: RayDeployment) -> None:
    import ray

    await dep.catalog.shutdown.remote()
    ray.kill(dep.catalog, no_restart=True)
