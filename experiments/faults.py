"""Project-scoped Docker/OS fault injection for the F1 Ray Compose deployment."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "ray" / "docker-compose.yml"
PROJECT = "dvs-f1"
SERVICES = {"ray-head", "ray-worker-1", "ray-worker-2", "ray-worker-3", "gateway"}
WORKERS = {"ray-worker-1", "ray-worker-2", "ray-worker-3"}
NETEM_HANDLE = "7e57:"


class FaultError(RuntimeError):
    """A requested fault was unsafe, unavailable, or could not be recovered."""


def _wall_time() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class FaultController:
    """Inject faults only into services with verified labels from this repository's Compose app."""

    def __init__(self, run_dir: str | Path, *, timeout: float = 30.0) -> None:
        self.run_dir = Path(run_dir).resolve()
        runs_root = (ROOT / "runs").resolve()
        if not self.run_dir.is_relative_to(runs_root):
            raise FaultError(f"fault evidence must be written under {runs_root}")
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.journal = self.run_dir / "faults.jsonl"
        self.timeout = timeout

    def _run(self, args: list[str], *, timeout: float | None = None) -> str:
        try:
            result = subprocess.run(
                args,
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout if timeout is None else timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FaultError(f"command unavailable or timed out: {args[0]}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip()[-1000:]
            raise FaultError(f"command failed ({result.returncode}): {detail or args[0]}")
        return result.stdout.strip()

    def _compose(self, *args: str) -> list[str]:
        return [
            "docker",
            "compose",
            "--project-name",
            PROJECT,
            "--file",
            str(COMPOSE_FILE),
            *args,
        ]

    def _inspect(self, container_id: str) -> dict[str, Any]:
        raw = self._run(["docker", "inspect", container_id])
        try:
            items = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise FaultError("Docker returned malformed container inspect data") from exc
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            raise FaultError("Docker returned malformed container inspect data")
        item = items[0]
        config = item.get("Config", {})
        if not isinstance(config, dict):
            raise FaultError("Docker returned malformed container configuration")
        labels = config.get("Labels") or {}
        if not isinstance(labels, dict):
            raise FaultError("Docker returned malformed container labels")
        service = labels.get("com.docker.compose.service")
        project = labels.get("com.docker.compose.project")
        if project != PROJECT or service not in SERVICES:
            raise FaultError("container is outside the verified DVS Compose project")
        if not isinstance(item, dict):
            raise FaultError("Docker inspect result is not a container object")
        return cast(dict[str, Any], item)

    def _service(self, service: str, *, require_running: bool = True) -> tuple[str, dict[str, Any]]:
        if service not in SERVICES:
            raise FaultError(f"unsupported Compose service: {service}")
        ids = self._run(self._compose("ps", "--quiet", service)).splitlines()
        ids = [item.strip() for item in ids if item.strip()]
        if len(ids) != 1:
            raise FaultError(f"expected one container for {service}, found {len(ids)}")
        container_id = ids[0]
        info = self._inspect(container_id)
        actual_service = info.get("Config", {}).get("Labels", {}).get("com.docker.compose.service")
        if actual_service != service:
            raise FaultError("Compose returned a container for a different service")
        if require_running and info.get("State", {}).get("Status") != "running":
            raise FaultError(f"{service} is not running")
        return container_id, info

    def _gateway_command(self, *args: str) -> str:
        self._service("gateway")
        command = self._compose(
            "exec",
            "--no-TTY",
            "gateway",
            "python",
            "-m",
            "dvs.ray_impl.faults",
            "--address",
            "ray://ray-head:10001",
            "--namespace",
            "dvs",
            "--cluster",
            "dvs",
            *args,
        )
        return self._run(command)

    def _replicas(self) -> list[dict[str, Any]]:
        try:
            data = json.loads(self._gateway_command("info"))
            replicas = data["replicas"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise FaultError("T120a faults info returned an invalid replica list") from exc
        if not isinstance(replicas, list):
            raise FaultError("T120a faults info did not return a replica list")
        return [item for item in replicas if isinstance(item, dict)]

    def info(self) -> dict[str, Any]:
        containers = []
        for service in sorted(SERVICES):
            try:
                _, info = self._service(service, require_running=False)
                state = info.get("State", {})
                containers.append(
                    {
                        "service": service,
                        "status": state.get("Status"),
                        "health": (state.get("Health") or {}).get("Status"),
                    }
                )
            except FaultError as exc:
                containers.append({"service": service, "error": str(exc)})
        return {"project": PROJECT, "containers": containers}

    def _resolve_replica_container(
        self, replica_id: str, replicas: list[dict[str, Any]]
    ) -> tuple[str, str, int]:
        matches = [item for item in replicas if item.get("replica_id") == replica_id]
        if len(matches) != 1:
            raise FaultError(f"replica id is not uniquely present in T120a info: {replica_id}")
        replica = matches[0]
        host = replica.get("host")
        pid = replica.get("pid")
        if (
            not isinstance(host, str)
            or not host
            or isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 0
        ):
            raise FaultError("T120a info is missing a valid replica host or PID")
        containers: list[tuple[str, str]] = []
        for service in WORKERS:
            container_id, info = self._service(service)
            config = info.get("Config", {})
            settings = info.get("NetworkSettings", {})
            networks = settings.get("Networks", {}) if isinstance(settings, dict) else {}
            if not isinstance(networks, dict):
                raise FaultError("Docker inspect network list is malformed")
            network_ips = {
                network.get("IPAddress")
                for network in networks.values()
                if isinstance(network, dict)
            }
            identities = {config.get("Hostname"), *network_ips}
            if host in identities:
                containers.append((service, container_id))
        if len(containers) != 1:
            raise FaultError("replica host does not map to exactly one verified worker container")
        return containers[0][1], replica_id, pid

    def _project_network(self, container: dict[str, Any]) -> str:
        settings = container.get("NetworkSettings", {})
        names = settings.get("Networks", {}) if isinstance(settings, dict) else {}
        if not isinstance(names, dict):
            raise FaultError("Docker inspect network list is malformed")
        candidates = [name for name in names if isinstance(name, str) and name.endswith("_dvs")]
        if len(candidates) != 1:
            raise FaultError("target does not have exactly one DVS Compose network")
        network = candidates[0]
        try:
            items = json.loads(self._run(["docker", "network", "inspect", network]))
        except json.JSONDecodeError as exc:
            raise FaultError("could not inspect the target Compose network") from exc
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            raise FaultError("could not inspect the target Compose network")
        data = items[0]
        labels = data.get("Labels") or {}
        if not isinstance(labels, dict):
            raise FaultError("Docker returned malformed network labels")
        if labels.get("com.docker.compose.project") != PROJECT:
            raise FaultError("network is outside the verified DVS Compose project")
        return network

    def apply(self, kind: str, target: str, *, delay_ms: float | None = None) -> dict[str, Any]:
        """Apply a fault and return a recovery token. Every action first verifies Compose labels."""
        token: dict[str, Any] = {"kind": kind, "target": target}
        if delay_ms is not None:
            token["delay_ms"] = delay_ms
        self._record("fault_requested", token)
        try:
            if kind == "docker-kill":
                container_id, _ = self._service(target)
                self._run(["docker", "kill", "--signal", "KILL", container_id])
                token["container_id"] = container_id
            elif kind == "docker-pause":
                container_id, _ = self._service(target)
                self._run(["docker", "pause", container_id])
                token["container_id"] = container_id
            elif kind == "kill-pid":
                container_id, replica_id, pid = self._resolve_replica_container(
                    target, self._replicas()
                )
                self._run(["docker", "exec", "--user", "0", container_id, "kill", "-9", str(pid)])
                token.update({"container_id": container_id, "replica_id": replica_id, "pid": pid})
            elif kind == "ray-delay":
                if delay_ms is None or not 0 < delay_ms <= 60_000:
                    raise FaultError("Ray delay must be between 0 and 60000 milliseconds")
                matches = [
                    item
                    for item in self._replicas()
                    if item.get("replica_id") == target and "error" not in item
                ]
                if len(matches) != 1:
                    raise FaultError(
                        f"replica id is not uniquely available in T120a info: {target}"
                    )
                self._gateway_command(
                    "delay",
                    "--replica",
                    target,
                    "--ms",
                    f"{delay_ms:g}",
                    "--mode",
                    "async",
                    "--ops",
                    "search",
                )
                token["delay_ms"] = delay_ms
            elif kind == "netem":
                if target not in WORKERS:
                    raise FaultError(
                        "tc netem is limited to the three verified Ray worker services"
                    )
                if delay_ms is None or not 0 < delay_ms <= 60_000:
                    raise FaultError("netem delay must be between 0 and 60000 milliseconds")
                container_id, _ = self._service(target)
                current = self._run(
                    ["docker", "exec", container_id, "tc", "qdisc", "show", "dev", "eth0"]
                )
                if "netem" in current or not any(
                    "noqueue" in line and "root" in line for line in current.splitlines()
                ):
                    raise FaultError(
                        "eth0 has a non-default qdisc; refusing to replace existing network state"
                    )
                self._run(
                    [
                        "docker",
                        "exec",
                        "--user",
                        "0",
                        container_id,
                        "tc",
                        "qdisc",
                        "add",
                        "dev",
                        "eth0",
                        "root",
                        "handle",
                        NETEM_HANDLE,
                        "netem",
                        "delay",
                        f"{delay_ms:g}ms",
                    ]
                )
                token["container_id"] = container_id
            elif kind == "disconnect":
                container_id, container = self._service(target)
                network = self._project_network(container)
                self._run(["docker", "network", "disconnect", network, container_id])
                token.update({"container_id": container_id, "network": network})
            else:
                raise FaultError(f"unsupported fault kind: {kind}")
        except Exception as exc:
            self._record(
                "fault_failed",
                {**token, "error": f"{type(exc).__name__}: {exc}"},
            )
            raise
        try:
            self._record("fault_applied", token)
        except OSError:
            try:
                self.recover(token)
            finally:
                raise
        return token

    def recover(self, token: dict[str, Any]) -> None:
        kind = token.get("kind")
        target = token.get("target")
        container_id = token.get("container_id")
        try:
            if kind == "docker-kill":
                self._run(
                    self._compose(
                        "up",
                        "--detach",
                        "--no-deps",
                        "--wait",
                        "--wait-timeout",
                        "300",
                        str(target),
                    ),
                    timeout=320,
                )
            elif kind == "docker-pause":
                self._inspect(str(container_id))
                self._run(["docker", "unpause", str(container_id)])
            elif kind == "kill-pid":
                self._service(self._worker_service(str(container_id)))
            elif kind == "ray-delay":
                self._gateway_command("clear", "--replica", str(target))
            elif kind == "netem":
                info = self._inspect(str(container_id))
                if info.get("State", {}).get("Status") != "running":
                    raise FaultError(
                        "cannot clear netem because its verified worker is not running"
                    )
                current = self._run(
                    ["docker", "exec", str(container_id), "tc", "qdisc", "show", "dev", "eth0"]
                )
                if NETEM_HANDLE not in current or "netem" not in current:
                    raise FaultError(
                        "owned netem qdisc is absent or changed; refusing broad qdisc cleanup"
                    )
                self._run(
                    [
                        "docker",
                        "exec",
                        "--user",
                        "0",
                        str(container_id),
                        "tc",
                        "qdisc",
                        "del",
                        "dev",
                        "eth0",
                        "root",
                    ]
                )
            elif kind == "disconnect":
                info = self._inspect(str(container_id))
                network = str(token["network"])
                if network not in info.get("NetworkSettings", {}).get("Networks", {}):
                    self._run(["docker", "network", "connect", network, str(container_id)])
            else:
                raise FaultError(f"cannot recover unknown fault kind: {kind}")
            self._wait_cluster_healthy()
        except Exception as exc:
            self._record(
                "fault_recovery_failed",
                {**token, "error": f"{type(exc).__name__}: {exc}"},
            )
            raise
        self._record("fault_recovered", token)

    def hold(
        self,
        kind: str,
        target: str,
        duration_seconds: float,
        *,
        delay_ms: float | None = None,
    ) -> None:
        if not 0 < duration_seconds <= 3600:
            raise FaultError("fault duration must be between 0 and 3600 seconds")
        token = self.apply(kind, target, delay_ms=delay_ms)
        try:
            time.sleep(duration_seconds)
        finally:
            self.recover(token)

    def _wait_cluster_healthy(self, timeout_seconds: float = 180.0) -> None:
        """Require four live Ray nodes and a healthy gateway after restoring a fault."""
        deadline = time.monotonic() + timeout_seconds
        last_error = "health checks have not completed"
        while time.monotonic() < deadline:
            try:
                self._run(
                    self._compose(
                        "exec",
                        "--no-TTY",
                        "ray-head",
                        "python",
                        "/workspace/deploy/ray/check_cluster.py",
                        "--expected-nodes",
                        "4",
                    )
                )
                self._run(
                    self._compose(
                        "exec",
                        "--no-TTY",
                        "gateway",
                        "python",
                        "-c",
                        "import urllib.request; "
                        "r=urllib.request.urlopen('http://127.0.0.1:8080/v1/health', timeout=5); "
                        "raise SystemExit(0 if r.status == 200 else 1)",
                    )
                )
                return
            except FaultError as exc:
                last_error = str(exc)
                time.sleep(2)
        raise FaultError(
            f"DVS Compose cluster did not recover within {timeout_seconds:g}s: {last_error}"
        )

    def _worker_service(self, container_id: str) -> str:
        for service in WORKERS:
            ids = self._run(self._compose("ps", "--quiet", service)).splitlines()
            if container_id in ids:
                self._inspect(container_id)
                return service
        raise FaultError("process-kill recovery target is no longer a verified worker")

    def _record(self, event: str, token: dict[str, Any]) -> None:
        record = {
            "event": event,
            "ts": _wall_time(),
            "mono": time.monotonic(),
            **token,
        }
        with self.journal.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-dir", type=Path, required=True, help="run directory for faults.jsonl"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("info", help="inspect only the DVS Compose project and its worker replica map")
    run = sub.add_parser("run", help="apply a scoped fault, hold it, then recover it")
    run.add_argument(
        "--kind",
        required=True,
        choices=("docker-kill", "kill-pid", "docker-pause", "netem", "disconnect", "ray-delay"),
    )
    run.add_argument("--target", required=True, help="Compose service, or replica id for kill-pid")
    run.add_argument("--duration-seconds", type=float, default=5.0)
    run.add_argument("--delay-ms", type=float)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    controller = FaultController(args.run_dir)
    try:
        if args.command == "info":
            data = controller.info()
            try:
                data["replicas"] = controller._replicas()
            except FaultError as exc:
                data["replica_error"] = str(exc)
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            controller.hold(args.kind, args.target, args.duration_seconds, delay_ms=args.delay_ms)
            print(json.dumps({"status": "recovered", "kind": args.kind, "target": args.target}))
        return 0
    except FaultError as exc:
        print(f"fault runner refused or failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
