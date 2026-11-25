"""T111 — placement en un clúster Ray de varios nodos (simulado en una máquina).

Con `strict_spread`, cada shard tiene sus réplicas en nodos distintos y los primarios quedan
repartidos entre nodos (diferencia máxima de 1). Es la dependencia "anti-afinidad la da el
scheduler de Ray" del mapa de dependencias.
"""

import uuid
from collections import Counter
from collections.abc import Iterator

import numpy as np
import pytest
import ray
from ray.cluster_utils import Cluster

from dvs.config import Config
from dvs.coordinator.types import SearchRequest
from dvs.core.model import WriteResult
from dvs.core.validation import make_write_op
from dvs.ray_impl.bootstrap import start, stop

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def three_node_cluster() -> Iterator[Cluster]:
    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0})
    for _ in range(3):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, namespace="dvs-placement", log_to_driver=False)
    yield cluster
    ray.shutdown()
    cluster.shutdown()


async def test_strict_spread_places_replicas_on_distinct_nodes(three_node_cluster: Cluster) -> None:
    cfg = Config.model_validate(
        {
            "cluster": {"num_shards": 4, "replication_factor": 3, "placement": "strict_spread"},
            "actors": {"num_cpus": 0.1},
            "collections": [{"name": "demo", "dim": 4, "metric": "l2"}],
        }
    )
    dep = await start(cfg, cluster=f"p{uuid.uuid4().hex[:6]}", detached=False)
    try:
        smap = await dep.backend.shard_map()
        health = {h.replica_id: h.status for h in await dep.backend.status()}
        worker_nodes = {
            n["NodeID"] for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU")
        }
        primaries: Counter[str] = Counter()
        for info in smap.shards:
            nodes = [health[r].node_id for r in info.replicas]  # type: ignore[union-attr]
            assert len(set(nodes)) == 3, f"shard {info.shard}: réplicas en el mismo nodo"
            assert set(nodes) <= worker_nodes  # nada en el head (num_cpus=0)
            primaries[health[info.primary].node_id] += 1  # type: ignore[union-attr]
        assert max(primaries.values()) - min(primaries.values()) <= 1, primaries

        spec, lim = cfg.collection("demo"), cfg.limits.to_core()
        assert spec is not None
        ops = [
            make_write_op("upsert", spec, f"v{i}", f"r{i}", lim, [i, 0, 0, 0]) for i in range(20)
        ]
        assert all(isinstance(o, WriteResult) for o in await dep.backend.write(ops))
        resp = await dep.backend.search(
            SearchRequest("q", "demo", np.zeros(4, np.float32), 3, None, "strong", 1.0)
        )
        assert [h.id for h in resp.hits] == ["v0", "v1", "v2"] and not resp.partial
    finally:
        await stop(dep)
