"""SSH and rsync helpers. Never runs a shell on the local machine for user input."""
from __future__ import annotations

import dataclasses
import ipaddress
import json
import os
import pathlib
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor

@dataclasses.dataclass(frozen=True)
class Node:
    id: int
    ssh_host: str
    rdma_ip: str
    role: str

@dataclasses.dataclass(frozen=True)
class Topology:
    nodes: tuple[Node, ...]
    replicas: tuple[int, ...]
    clients: tuple[int, ...]
    tcp_port: int
    ib_port: int
    device: str

    @classmethod
    def load(cls, path: pathlib.Path) -> "Topology":
        doc = json.loads(path.read_text())
        nodes = tuple(Node(**entry) for entry in doc["nodes"])
        ids = [node.id for node in nodes]
        if not nodes or len(ids) != len(set(ids)) or min(ids) < 1 or max(ids) > 255:
            raise ValueError("node IDs must be distinct and in 1..255")
        if len({n.ssh_host for n in nodes}) != len(nodes):
            raise ValueError("SSH hosts must be distinct for the eight-node setup")
        if len({n.rdma_ip for n in nodes}) != len(nodes):
            raise ValueError("RDMA IPs must be distinct")
        for n in nodes:
            ipaddress.ip_address(n.rdma_ip)
            if n.role not in ("replica", "client"):
                raise ValueError(f"bad role: {n.role}")
        replicas = tuple(doc["replicas"])
        clients = tuple(doc["clients"])
        if len(replicas) not in (3, 5) or len(replicas) % 2 == 0:
            raise ValueError("exactly 3 or 5 replicas required")
        if set(replicas).intersection(clients) or set(replicas + clients) != set(ids):
            raise ValueError("replicas/clients must partition the nodes")
        by_id = {n.id: n for n in nodes}
        for x in replicas:
            if by_id[x].role != "replica":
                raise ValueError("replica role mismatch")
        for x in clients:
            if by_id[x].role != "client":
                raise ValueError("client role mismatch")
        if len(clients) == 0:
            raise ValueError("at least one client required")
        tcp_port = int(doc.get("tcp_port", 18515))
        if not 1 <= tcp_port <= 65535:
            raise ValueError("TCP port out of range")
        return cls(nodes, replicas, clients, tcp_port,
                   int(doc.get("ib_port", 1)), str(doc.get("device", "")))

    def peers_arg(self) -> str:
        return ",".join(f"{n.id}@{n.rdma_ip}" for n in sorted(self.nodes, key=lambda n: n.id))

    def server_ids(self) -> str:
        return ",".join(map(str, self.replicas))

    def client_ids(self) -> str:
        return ",".join(map(str, self.clients))

    def by_id(self, node_id: int) -> Node:
        return next(node for node in self.nodes if node.id == node_id)

class Remote:
    def __init__(self, ssh_user: str, identity: str | None = None, dry_run: bool = False):
        self.user = ssh_user
        self.identity = os.path.expanduser(identity) if identity else None
        self.dry_run = dry_run

    def _ssh_options(self) -> list[str]:
        result = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                  "-o", "ConnectTimeout=10"]
        if self.identity:
            result += ["-i", self.identity]
        return result

    def _target(self, node: Node) -> str:
        return f"{self.user}@{node.ssh_host}"

    def command(self, node: Node, script: str, check: bool = True) -> str:
        cmd = ["ssh", *self._ssh_options(), self._target(node),
               "bash", "-lc", shlex.quote(script)]
        if self.dry_run:
            print("DRY RUN:", shlex.join(cmd))
            return ""
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if check and proc.returncode:
            raise RuntimeError(f"ssh {node.id}: {proc.stderr.strip()} {proc.stdout.strip()}")
        return proc.stdout

    def deploy(self, node: Node, source: pathlib.Path, remote_dir: str) -> None:
        self.command(node, f"mkdir -p {shlex.quote(remote_dir)}")
        cmd = ["rsync", "-az", "--delete", "--exclude=.git", "--exclude=build",
               "--exclude=results", "--exclude=__pycache__",
               "-e", shlex.join(["ssh", *self._ssh_options()]),
               f"{source}/", f"{self._target(node)}:{remote_dir}/"]
        if self.dry_run:
            print("DRY RUN:", shlex.join(cmd))
            return
        subprocess.run(cmd, check=True)

    def collect(self, node: Node, remote_file: str, destination: pathlib.Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        cmd = ["scp", *self._ssh_options(), f"{self._target(node)}:{remote_file}",
               str(destination)]
        if self.dry_run:
            print("DRY RUN:", shlex.join(cmd))
            return
        subprocess.run(cmd, check=True)

    def parallel(self, nodes: tuple[Node, ...], fn) -> None:
        with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
            futures = {pool.submit(fn, n): n for n in nodes}
            for future, node in futures.items():
                try:
                    future.result()
                except Exception as exc:
                    raise RuntimeError(f"node {node.id}: {exc}") from exc
