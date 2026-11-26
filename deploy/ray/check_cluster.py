"""Return success only when the expected number of Ray nodes is registered and alive."""

from __future__ import annotations

import argparse
import logging
import sys

import ray


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="ray-head:6379")
    parser.add_argument("--expected-nodes", type=int, default=4)
    args = parser.parse_args()
    if args.expected_nodes < 1:
        parser.error("--expected-nodes must be positive")

    try:
        ray.init(
            address=args.address,
            namespace="dvs-compose-check",
            logging_level=logging.ERROR,
            log_to_driver=False,
        )
        alive_nodes = [node for node in ray.nodes() if node.get("Alive")]
        if len(alive_nodes) != args.expected_nodes:
            print(
                f"expected {args.expected_nodes} live Ray nodes; found {len(alive_nodes)}",
                file=sys.stderr,
            )
            return 1
        return 0
    except Exception as exc:
        print(f"Ray cluster health check failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
