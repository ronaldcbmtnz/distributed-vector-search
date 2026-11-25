"""Catalog: creates and places the shard replicas, picks primaries and publishes the map.

Ray provides the mechanisms we document as dependencies:
- placement groups (one per shard, STRICT_SPREAD): replicas of a shard on distinct nodes;
- named actors in the GCS: the directory where the gateway finds each replica;
- `get_if_exists`: idempotent bootstrap if the gateway restarts.
What Ray does NOT decide (and is ours): which replica is primary, the quorum and the map version.
Primaries are balanced across nodes using the node each replica actually landed on.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from dvs.config import Config
from dvs.core.model import ShardInfo, ShardMap
from dvs.ray_impl.shard_actor import ShardReplica

log = logging.getLogger("dvs.catalog")

_STRATEGY = {"strict_spread": "STRICT_SPREAD", "spread": "SPREAD", "pack": "PACK"}


def replica_actor_name(cluster: str, replica_id: str) -> str:
    return f"{cluster}:{replica_id}"


@ray.remote
class Catalog:
    def __init__(self, config: dict[str, Any], cluster: str = "dvs", detached: bool = True) -> None:
        self.cfg = Config.model_validate(config)
        self.cluster = cluster
        self.detached = detached
        self._map: ShardMap | None = None
        self._pgs: list[Any] = []
        self._handles: dict[str, Any] = {}

    async def bootstrap(self) -> ShardMap:
        """Create (or reuse) every replica, choose primaries, configure roles. Idempotent."""
        if self._map is not None:
            return self._map
        cl, act = self.cfg.cluster, self.cfg.actors
        rf = cl.replication_factor
        lifetime = "detached" if self.detached else None
        collections = list(self.cfg.collection_specs)
        placements: dict[int, list[str]] = {}
        for s in range(cl.num_shards):
            pg = placement_group(
                [{"CPU": act.num_cpus}] * rf,
                strategy=_STRATEGY[cl.placement],
                lifetime=lifetime,
            )
            await pg.ready()
            self._pgs.append(pg)
            placements[s] = []
            for i in range(rf):
                rid = f"s{s}/r{i}"
                handle = ShardReplica.options(  # type: ignore[attr-defined]
                    name=replica_actor_name(self.cluster, rid),
                    lifetime=lifetime,
                    get_if_exists=True,
                    num_cpus=act.num_cpus,
                    max_concurrency=act.max_concurrency,
                    max_restarts=act.max_restarts,
                    max_pending_calls=act.max_pending_calls,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg, placement_group_bundle_index=i
                    ),
                ).remote(
                    rid,
                    s,
                    collections,
                    self.cfg.timeouts_ms.write / 1000.0,
                    self.cfg.fault_injection.enabled,
                )
                self._handles[rid] = handle
                placements[s].append(rid)

        nodes = {rid: (await h.status.remote()).node_id for rid, h in self._handles.items()}
        primaries_per_node: Counter[str] = Counter()
        shards: list[ShardInfo] = []
        for s, rids in placements.items():
            primary = min(rids, key=lambda r: (primaries_per_node[nodes[r]], r))
            primaries_per_node[nodes[primary]] += 1
            ordered = (primary, *[r for r in rids if r != primary])
            shards.append(ShardInfo(s, primary, ordered, tuple(nodes[r] for r in ordered)))

        for info in shards:  # backups first: they must accept replication before writes arrive
            for rid in info.replicas[1:]:
                await self._handles[rid].configure.remote("backup", cl.quorum, {})
        for info in shards:
            peers = {rid: self._handles[rid] for rid in info.replicas[1:]}
            await self._handles[info.primary].configure.remote("primary", cl.quorum, peers)

        self._map = ShardMap(version=1, shards=tuple(shards))
        log.info("catalog ready", extra={"event": "catalog.ready", "shards": cl.num_shards})
        return self._map

    async def shard_map(self) -> ShardMap:
        if self._map is None:
            raise RuntimeError("catalog not bootstrapped")
        return self._map

    async def replica_names(self) -> dict[str, str]:
        return {rid: replica_actor_name(self.cluster, rid) for rid in self._handles}

    async def shutdown(self) -> None:
        """Kill replicas and release placement groups (tests and `make down`)."""
        for h in self._handles.values():
            ray.kill(h, no_restart=True)
        for pg in self._pgs:
            remove_placement_group(pg)
        self._handles.clear()
        self._pgs.clear()
        self._map = None
