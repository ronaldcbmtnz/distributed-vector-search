"""Phase-1 gateway entrypoint: `uvicorn dvs.api.main_ray:app`.

Environment (docs/REPARTO_TAREAS.md §3.3):
  DVS_CONFIG       path of the YAML configuration (required)
  DVS_RAY_ADDRESS  "auto" (join the running cluster, default) or "local" (start an in-process Ray)
  DVS_NAMESPACE    Ray namespace (default "dvs")
  DVS_CLUSTER      cluster name prefix for actor names (default "dvs")
  DVS_RUN_DIR      if set, JSON logs go to $DVS_RUN_DIR/logs/

With a real cluster the catalog and replicas are detached: restarting the gateway reuses them.
With "local" they live and die with this process.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import ray
from fastapi import FastAPI

from dvs.api.app import create_app
from dvs.config import load_config
from dvs.obs import setup_logging
from dvs.ray_impl.bootstrap import start, stop

CONFIG = load_config()
ADDRESS = os.environ.get("DVS_RAY_ADDRESS", "auto")
NAMESPACE = os.environ.get("DVS_NAMESPACE", "dvs")
CLUSTER = os.environ.get("DVS_CLUSTER", "dvs")
LOCAL = ADDRESS == "local"

log = logging.getLogger("dvs.gateway")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging("gateway")
    if LOCAL:
        ray.init(namespace=NAMESPACE, include_dashboard=False, log_to_driver=False)
    else:
        ray.init(address=ADDRESS, namespace=NAMESPACE, log_to_driver=False)
    dep = await start(CONFIG, cluster=CLUSTER, detached=not LOCAL, namespace=NAMESPACE)
    app.state.backend = dep.backend
    smap = await dep.backend.shard_map()
    log.info(
        "gateway ready",
        extra={"event": "gateway.ready", "shards": smap.num_shards, "local": LOCAL},
    )
    try:
        yield
    finally:
        app.state.backend = None
        if LOCAL:
            await stop(dep)
        ray.shutdown()


app = create_app(CONFIG, lifespan=lifespan)
