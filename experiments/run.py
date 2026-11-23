#!/usr/bin/env python3
"""Single-command CloudLab controller; all IPs come from the topology JSON."""
from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import pathlib
import shlex
import subprocess
import sys
import time
import uuid

from cluster import Remote, Topology

ROOT = pathlib.Path(__file__).resolve().parents[1]

def sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()

def require_clean_tree() -> None:
    if subprocess.run(["git", "diff", "--quiet"], cwd=ROOT).returncode or subprocess.run(
            ["git", "diff", "--cached", "--quiet"], cwd=ROOT).returncode:
        raise RuntimeError("working tree is modified; commit changes before running experiments")

def cases(suite: dict):
    allowed = {"cas", "mu", "handoff"}
    if set(suite["backends"]) - allowed:
        raise ValueError("unknown backend")
    for backend, distribution, workers, outstanding, repeat in itertools.product(
            suite["backends"], suite["distributions"], suite.get("workers", [1]),
            suite["outstanding"], range(suite["repetitions"])):
        if outstanding < 1 or outstanding > 512:
            raise ValueError("outstanding must be 1..512")
        if distribution not in ("uniform", "zipf_0.5", "zipf_0.99"):
            raise ValueError("unsupported lock distribution")
        if workers < 1 or workers > 16 or workers * outstanding > 512:
            raise ValueError("logical workers * outstanding must be 1..512")
        yield dict(backend=backend, distribution=distribution,
                   workers=workers, outstanding=outstanding, repeat=repeat)

def arg(name: str, value) -> str:
    return f"--{name}={value}"

def process_command(topo: Topology, node, suite: dict, case: dict,
                    smoke: bool = False) -> list[str]:
    common = [
        arg("node-id", node.id),
        arg("peers", topo.peers_arg()),
        arg("tcp-port", topo.tcp_port),
        arg("ib-port", topo.ib_port),
        arg("device", topo.device),
    ]
    if smoke:
        return ["./build/lock_rdma_smoke", *common]
    common += [
        arg("replicas", topo.server_ids()),
        arg("clients", topo.client_ids()),
        arg("backend", case["backend"]),
        arg("locks", suite["locks"]),
        arg("outstanding", case["outstanding"]),
        arg("workers", case.get("workers", 1)),
        arg("distribution", case["distribution"]),
        arg("mu-lanes", suite.get("mu_lanes", 1)),
        arg("prepared-window", suite.get("prepared_window", 8)),
        arg("warmup-seconds", suite["warmup_seconds"]),
        arg("duration-seconds", suite["duration_seconds"]),
        arg("watchdog-seconds", suite["watchdog_seconds"]),
        arg("seed", suite["seed"] + case["repeat"]),
    ]
    return ["./build/lock_server" if node.role == "replica" else "./build/lock_client",
            *common]

def start_node(remote: Remote, node, remote_dir: str,
               run_name: str, argv: list[str]) -> None:
    folder = f"{remote_dir}/runs/{run_name}"
    cmd = shlex.join(argv)
    script = (f"mkdir -p {shlex.quote(folder)} && cd {shlex.quote(remote_dir)} && "
              f"nohup {cmd} > {shlex.quote(folder + '/node.log')} 2>&1 < /dev/null &")
    # Keep PID and background job in the same remote shell; do not rely on ssh job control.
    launch = (f"mkdir -p {shlex.quote(folder)}; cd {shlex.quote(remote_dir)}; "
              f"nohup {cmd} > {shlex.quote(folder + '/node.log')} 2>&1 < /dev/null "
              f"& echo $! > {shlex.quote(folder + '/pid')}")
    remote.command(node, launch)

def alive_or_matching(remote: Remote, node, folder: str, token: str) -> bool:
    script = (f"test -f {shlex.quote(folder + '/node.log')} && "
              f"grep -q {shlex.quote(token)} {shlex.quote(folder + '/node.log')}")
    return bool(remote.command(node, script, check=False) == "" and
                remote.command(node,
                    f"if {script}; then echo YES; else echo NO; fi", check=False).strip() == "YES")

def wait_for(remote: Remote, nodes, folder: str, token: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pending = []
        for node in nodes:
            script = (f"if grep -q {shlex.quote(token)} "
                      f"{shlex.quote(folder + '/node.log')} 2>/dev/null; "
                      f"then echo YES; else echo NO; fi")
            if remote.command(node, script, check=False).strip() != "YES":
                pending.append(node.id)
        if not pending:
            return
        if not remote.dry_run:
            time.sleep(1)
        else:
            return
    raise TimeoutError(f"waiting for {token}; pending nodes: {pending}")

def kill_node(remote: Remote, node, folder: str) -> None:
    script = (f"if test -f {shlex.quote(folder + '/pid')}; then "
              f"pid=$(cat {shlex.quote(folder + '/pid')}); "
              f"kill \"$pid\" 2>/dev/null || true; fi")
    remote.command(node, script, check=False)

def run_case(remote: Remote, topo: Topology, suite: dict, case: dict,
             remote_dir: str, output_dir: pathlib.Path, smoke: bool = False) -> dict:
    name = ("smoke" if smoke else
            f"{case['backend']}-{case['distribution']}-k{case.get('workers', 1)}-w{case['outstanding']}-r{case['repeat']}")
    folder = f"{remote_dir}/runs/{name}"
    result_dir = output_dir / name
    result_dir.mkdir(parents=True, exist_ok=True)
    manifest = dict(case=case, name=name, valid=False, error=None,
                    nodes=[n.id for n in topo.nodes])
    try:
        remote.parallel(topo.nodes, lambda node: start_node(
            remote, node, remote_dir, name, process_command(topo, node, suite, case, smoke)))
        wait_for(remote, topo.nodes, folder, "RDMA_SMOKE_OK" if smoke else "READY node=",
                 min(120, suite.get("watchdog_seconds", 90)))
        if not smoke:
            duration = (suite["warmup_seconds"] + suite["duration_seconds"] +
                        suite["watchdog_seconds"])
            wait_for(remote, tuple(topo.by_id(n) for n in topo.clients),
                     folder, "RESULT ", duration)
        if not smoke:
            results = []
            for node_id in topo.clients:
                node = topo.by_id(node_id)
                log = remote.command(node, f"cat {shlex.quote(folder + '/node.log')}")
                found = [line.split("RESULT ", 1)[1] for line in log.splitlines()
                         if line.startswith("RESULT ")]
                if len(found) != 1:
                    raise ValueError(f"node {node_id} did not emit exactly one RESULT")
                record = json.loads(found[0])
                if (record.get("node") != node_id or
                    record.get("backend") != case["backend"] or
                    record.get("workers") != case.get("workers", 1) or
                    record.get("outstanding_per_worker") != case["outstanding"]):
                    raise ValueError(f"invalid result identity from node {node_id}")
                if record.get("measurement_seconds", 0) <= 0:
                    raise ValueError(f"invalid measurement duration from node {node_id}")
                if record.get("completed", 0) <= 0:
                    raise ValueError(f"no completed pairs from node {node_id}")
                results.append(record)
            for node in topo.nodes:
                log = remote.command(node, f"cat {shlex.quote(folder + '/node.log')}")
                if any(line.startswith("ERROR ") for line in log.splitlines()):
                    raise RuntimeError(f"node {node.id} reported a runtime error")
            manifest["client_results"] = results
            manifest["total_completed"] = sum(x["completed"] for x in results)
            manifest["aggregate_goodput_pairs_per_second"] = sum(
                x["completed"] / x["measurement_seconds"] for x in results)
        manifest["valid"] = True
    except Exception as exc:
        manifest["error"] = str(exc)
        raise
    finally:
        for node in topo.nodes:
            try:
                remote.collect(node, folder + "/node.log", result_dir / f"node-{node.id}.log")
            except Exception as exc:
                manifest.setdefault("collection_errors", []).append(str(exc))
        remote.parallel(topo.nodes, lambda node: kill_node(remote, node, folder))
        (result_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topology", type=pathlib.Path,
                    default=ROOT / "experiments/topologies/apt8.json")
    ap.add_argument("--suite", type=pathlib.Path,
                    default=ROOT / "experiments/suites/first.json")
    ap.add_argument("--ssh-user", default=None)
    ap.add_argument("--identity", default=None)
    ap.add_argument("--remote-dir", default=None)
    ap.add_argument("--skip-deploy", action="store_true")
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-backend", choices=("cas", "mu", "handoff"))
    ap.add_argument("--max-trials", type=int)
    args = ap.parse_args()
    topo = Topology.load(args.topology)
    suite = json.loads(args.suite.read_text())
    if suite["replicas"] != len(topo.replicas):
        raise ValueError("suite replica count does not match topology")
    if suite.get("mu_lanes", 1) != 1:
        raise ValueError("first implementation supports one Mu lane")
    require_clean_tree()
    commit = sha()
    remote_dir = args.remote_dir or f"/tmp/lock-{commit[:12]}"
    if not remote_dir.startswith("/tmp/lock-"):
        raise ValueError("remote_dir must start with /tmp/lock- for safe cleanup")
    user = args.ssh_user or __import__("getpass").getuser()
    remote = Remote(user, args.identity, args.dry_run)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = ROOT / "results" / f"{timestamp}-{uuid.uuid4().hex[:8]}"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "suite.json").write_text(json.dumps(suite, indent=2) + "\n")
    (output_dir / "topology.json").write_text(args.topology.read_text())
    (output_dir / "source.json").write_text(
        json.dumps({"commit": commit, "remote_dir": remote_dir}, indent=2) + "\n")
    if not args.skip_deploy:
        remote.parallel(topo.nodes, lambda n: remote.deploy(n, ROOT, remote_dir))
    if not args.skip_build:
        def build(node):
            cmd = (f"cd {shlex.quote(remote_dir)} && "
                   f"cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DLOCK_WITH_RDMA=ON "
                   f"&& cmake --build build -j 4")
            remote.command(node, cmd)
        remote.parallel(topo.nodes, build)
    all_cases = [{}] if args.smoke else list(cases(suite))
    if args.only_backend and not args.smoke:
        all_cases = [c for c in all_cases if c["backend"] == args.only_backend]
    if args.max_trials:
        all_cases = all_cases[:args.max_trials]
    valid = []
    for case in all_cases:
        print("RUN", json.dumps(case), flush=True)
        record = run_case(remote, topo, suite, case, remote_dir, output_dir, args.smoke)
        if not remote.dry_run and not record["valid"]:
            raise RuntimeError(f"invalid run: {record['name']}")
        valid.append(record)
    print("OUTPUT", output_dir)
    print("VALID_TRIALS", len(valid))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("ERROR", exc, file=sys.stderr)
        sys.exit(1)
