"""Fault hooks inside Ray for experiments (agreed interface: docs/REPARTO_TAREAS.md §3.4).

    python -m dvs.ray_impl.faults info
    python -m dvs.ray_impl.faults delay --replica s1/r2 --ms 200 --mode block [--ops search write]
    python -m dvs.ray_impl.faults clear --replica s1/r2
    python -m dvs.ray_impl.faults kill  --replica s1/r2 [s1/r0 ...] [--no-restart]

Run it from any container attached to the Ray cluster (e.g. the gateway). Output is JSON on
stdout. `delay`/`clear` require `fault_injection.enabled: true` in the configuration; `kill`
works always (it is the runtime's own `ray.kill`). Process-level kills (`kill -9`) and network
faults are done by the docker-level scripts using the `pid`/`host` printed by `info`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import ray

from dvs.ray_impl.catalog_actor import replica_actor_name


def _replica(cluster: str, rid: str) -> Any:
    return ray.get_actor(replica_actor_name(cluster, rid))


def _hosts() -> dict[str, str]:
    return {n["NodeID"]: n.get("NodeManagerHostname", "") for n in ray.nodes()}


def run(args: argparse.Namespace) -> dict[str, Any]:
    ts = {"ts": time.time(), "mono": time.monotonic(), "cmd": args.cmd}
    if args.cmd == "info":
        catalog = ray.get_actor(f"{args.cluster}:catalog")
        names: dict[str, str] = ray.get(catalog.replica_names.remote())
        hosts = _hosts()
        out = []
        for rid in sorted(names):
            try:
                info = ray.get(_replica(args.cluster, rid).debug_info.remote(), timeout=3)
                info["host"] = hosts.get(info["node_id"], "")
            except Exception as e:
                info = {"replica_id": rid, "error": type(e).__name__}
            out.append(info)
        return {**ts, "replicas": out}
    if args.cmd == "kill":  # several replicas in one connection (a whole shard at once)
        for rid in args.replica:
            ray.kill(_replica(args.cluster, rid), no_restart=args.no_restart)
        return {**ts, "replicas": args.replica, "no_restart": args.no_restart}
    handle = _replica(args.cluster, args.replica)
    if args.cmd == "delay":
        ray.get(handle.debug_set_delay.remote(args.ms, args.mode, args.ops or []), timeout=5)
        return {**ts, "replica": args.replica, "ms": args.ms, "mode": args.mode, "ops": args.ops}
    if args.cmd == "clear":
        ray.get(handle.debug_clear.remote(), timeout=5)
        return {**ts, "replica": args.replica}
    raise SystemExit(f"unknown command {args.cmd}")


def parse(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="dvs.ray_impl.faults", description=__doc__.split("\n\n")[0])
    p.add_argument("--address", default="auto", help="Ray cluster address (default: auto)")
    p.add_argument("--namespace", default="dvs")
    p.add_argument("--cluster", default="dvs", help="cluster name used by the gateway")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info", help="list replicas with shard, role, node, pid, host")
    d = sub.add_parser("delay", help="inject latency in a replica")
    d.add_argument("--replica", required=True)
    d.add_argument("--ms", type=float, required=True)
    d.add_argument("--mode", choices=["block", "async"], default="block")
    d.add_argument("--ops", nargs="*", choices=["search", "write", "replicate"])
    c = sub.add_parser("clear", help="remove injected latency")
    c.add_argument("--replica", required=True)
    k = sub.add_parser("kill", help="ray.kill one or more replica actors")
    k.add_argument("--replica", required=True, nargs="+")
    k.add_argument("--no-restart", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse(argv)
    if not ray.is_initialized():
        ray.init(address=args.address, namespace=args.namespace, log_to_driver=False)
    print(json.dumps(run(args), default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
