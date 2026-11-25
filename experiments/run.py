#!/usr/bin/env python3
"""Single-command CloudLab controller; all IPs come from the topology JSON."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import pathlib
import shlex
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from cluster import Remote, Topology
from recovery_control import RecoveryControl
from metrics import SUMMARY_FIELDS, summarize, format_summary, format_client

ROOT = pathlib.Path(__file__).resolve().parents[1]

def sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()

def cases(suite: dict):
    allowed = {"cas", "mu", "handoff"}
    if set(suite["backends"]) - allowed:
        raise ValueError("unknown backend")
    locks = suite["locks"]
    critical_section_ops = suite.get("critical_section_ops", 1)
    if type(locks) is not int or not 1 <= locks <= 1000:
        raise ValueError("locks must be an integer in 1..1000")
    if type(critical_section_ops) is not int or critical_section_ops not in (0, 1):
        raise ValueError("critical_section_ops must be 0 or 1")
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
                   workers=workers, outstanding=outstanding, repeat=repeat,
                   locks=locks, critical_section_ops=critical_section_ops,
                   handoff_gc=int(backend == "handoff" and suite.get("handoff_gc", False)))

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
    if node.role == "client":
        common.append(arg("critical-section-ops", case["critical_section_ops"]))
        if "latency_sample_capacity" in suite:
            common.append(arg("latency-sample-capacity", suite["latency_sample_capacity"]))
    if node.role == "replica" and suite.get("profile_leader", False):
        common.append(arg("profile-leader", 1))
    if node.role == "replica" and case["backend"] in ("mu", "handoff"):
        common.extend([arg("replication-batch", suite.get("replication_batch", 8)),
                       arg("leader-comm-batch", suite.get("leader_comm_batch", 64))])
    if node.role == "replica" and case["backend"] == "mu":
        common.append(arg("mu-inflight-window", suite.get("mu_inflight_window", 512)))
    if case["backend"] == "mu" and suite.get("mu_recovery", False):
        common.append(arg("mu-recovery", 1))
        if node.role == "replica":
            common.extend([arg("mu-recovery-publication", int(suite.get("mu_recovery_publication", True))),
                           arg("mu-recovery-replay", int(suite.get("mu_recovery_replay", True)))])
    if case["backend"] == "handoff":
        common.append(arg("handoff-gc", int(suite.get("handoff_gc", False))))
        for key in ("handoff_lease_us", "handoff_lease_scan_us", "handoff_heartbeat_us"):
            common.append(arg(key.replace("_", "-"), suite.get(key, 0)))
        if node.role == "client":
            common.append(arg("handoff-trace", int(suite.get("handoff_trace", False))))
    if node.role == "client" and case["backend"] == "handoff":
        common.append(arg("handoff-peer-read",
                          int(suite.get("handoff_peer_read", False))))
    if node.role == "replica" and case["backend"] == "handoff":
        common.append(arg("handoff-inflight-window",
                          suite.get("handoff_inflight_window", 512)))
    common += [
        arg("replicas", topo.server_ids()),
        arg("clients", topo.client_ids()),
        arg("backend", case["backend"]),
        arg("locks", case["locks"]),
        arg("outstanding", case["outstanding"]),
        arg("workers", case.get("workers", 1)),
        arg("distribution", case["distribution"]),
        arg("mu-lanes", suite.get("mu_lanes", 1)
            if case["backend"] in ("mu", "handoff") else 1),
        arg("prepared-window", suite.get("prepared_window", 8)),
        arg("warmup-seconds", suite["warmup_seconds"]),
        arg("duration-seconds", suite["duration_seconds"]),
        arg("watchdog-seconds", suite["watchdog_seconds"]),
        arg("seed", suite["seed"] + case["repeat"]),
    ]
    return ["./build/lock_server" if node.role == "replica" else "./build/lock_client",
            *common]

def start_node(remote: Remote, node, remote_dir: str,
               run_name: str, argv: list[str], control=None) -> None:
    folder = f"{remote_dir}/runs/{run_name}"
    if control is not None and node.role == "replica":
        # Supervisor owns the child and publishes its real PID. Its own
        # diagnostics and PID are separate from the server log and PID.
        cmd = shlex.join(control.agent_command(node.id, folder, argv))
        launch = (f"mkdir -p {shlex.quote(folder)}; cd {shlex.quote(remote_dir)}; "
                  f"rm -f {shlex.quote(folder + '/pid')} {shlex.quote(folder + '/node.log')} "
                  f"{shlex.quote(folder + '/control.pid')}; "
                  f"nohup {cmd} > {shlex.quote(folder + '/control.log')} 2>&1 < /dev/null &")
        remote.command(node, launch)
        return
    cmd = shlex.join(argv)
    script = (f"mkdir -p {shlex.quote(folder)} && cd {shlex.quote(remote_dir)} && "
              f"nohup {cmd} > {shlex.quote(folder + '/node.log')} 2>&1 < /dev/null &")
    # Keep PID and background job in the same remote shell; do not rely on ssh job control.
    launch = (f"mkdir -p {shlex.quote(folder)}; cd {shlex.quote(remote_dir)}; "
              f"nohup {cmd} > {shlex.quote(folder + '/node.log')} 2>&1 < /dev/null "
              f"& echo $! > {shlex.quote(folder + '/pid')}")
    remote.command(node, launch)

def wait_for(remote: Remote, nodes, folder: str, token: str, timeout: float) -> None:
    """Poll all nodes concurrently and report early startup failures."""
    deadline = time.monotonic() + timeout
    log = shlex.quote(folder + "/node.log")
    pid = shlex.quote(folder + "/pid")
    match = shlex.quote(token)

    def probe(node):
        script = (
            f"if test -f {log}; then "
            f"if grep -q '^ERROR ' {log}; then grep -m 1 '^ERROR ' {log}; "
            f"elif grep -Fq -- {match} {log}; then echo READY; "
            f"elif test -f {pid} && ! kill -0 \"$(cat {pid})\" 2>/dev/null; "
            f"then echo EXITED; tail -n 3 {log}; "
            f"else echo PENDING; fi; "
            f"else echo PENDING; fi"
        )
        return node.id, remote.command(node, script, check=False).strip()

    while time.monotonic() < deadline:
        with ThreadPoolExecutor(max_workers=max(1, len(nodes))) as pool:
            statuses = list(pool.map(probe, nodes))
        pending = []
        for node_id, status in statuses:
            if status == "READY":
                continue
            if status.startswith("ERROR ") or status.startswith("EXITED"):
                raise RuntimeError(f"node {node_id}: {status}")
            pending.append(node_id)
        if not pending or remote.dry_run:
            return
        time.sleep(1)
    raise TimeoutError(f"waiting for {token}; pending nodes: {pending}")


def kill_node(remote: Remote, node, folder: str) -> None:
    script = (f"if test -f {shlex.quote(folder + '/pid')}; then "
              f"pid=$(cat {shlex.quote(folder + '/pid')}); "
              f"kill \"$pid\" 2>/dev/null || true; fi")
    remote.command(node, script, check=False)

def kill_supervisor(remote: Remote, node, folder: str) -> None:
    pid = shlex.quote(folder + "/control.pid")
    # The supervisor terminates/reaps its child on SIGTERM if still active.
    remote.command(node, f"if test -f {pid}; then "
                   f"kill \"$(cat {pid})\" 2>/dev/null || true; rm -f {pid}; fi",
                   check=False)

def run_case(remote: Remote, topo: Topology, suite: dict, case: dict,
             remote_dir: str, output_dir: pathlib.Path, smoke: bool = False) -> dict:
    name = ("smoke" if smoke else
            f"{case['backend']}-{case['distribution']}-k{case.get('workers', 1)}-w{case['outstanding']}"
            f"-l{case['locks']}-cs{case['critical_section_ops']}-gc{case['handoff_gc']}-r{case['repeat']}")
    folder = f"{remote_dir}/runs/{name}"
    result_dir = output_dir / name
    result_dir.mkdir(parents=True, exist_ok=True)
    manifest = dict(case=case, name=name, valid=False, error=None,
                    nodes=[n.id for n in topo.nodes])
    if remote.dry_run:
        for node in topo.nodes:
            start_node(remote, node, remote_dir, name,
                       process_command(topo, node, suite, case, smoke))
        manifest["dry_run"] = True
        (result_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return manifest
    control = None
    try:
        crash = not smoke and suite.get("crash_after_ms") is not None
        if crash:
            # Listener is on the controller's existing topology address.
            # All replica supervisors connect before any server starts.
            address = topo.by_id(topo.controller_node_id).rdma_ip
            control = RecoveryControl(address, topo.replicas, suite["crash_after_ms"],
                                      min(120, suite.get("watchdog_seconds", 90)))
        remote.parallel(topo.nodes, lambda node: start_node(
            remote, node, remote_dir, name, process_command(topo, node, suite, case, smoke),
            control))
        if control is not None:
            control.arm()
            # The local leader timer observes the actual service-start
            # marker. Do not SSH-poll readiness across the crash boundary.
            manifest["injection"] = control.crash_and_promote()
            control.close()
        else:
            wait_for(remote, topo.nodes, folder, "RDMA_SMOKE_OK" if smoke else "READY node=",
                     min(120, suite.get("watchdog_seconds", 90)))
        if not smoke:
            duration = (suite["warmup_seconds"] + suite["duration_seconds"] +
                        suite["watchdog_seconds"])
            wait_for(remote, tuple(topo.by_id(n) for n in topo.clients),
                     folder, "RESULT ", duration)
        if not smoke:
            # Fetch all eight logs concurrently; serial SSH calls dominate short trials.
            with ThreadPoolExecutor(max_workers=len(topo.nodes)) as pool:
                logs = dict(zip(
                    (n.id for n in topo.nodes),
                    pool.map(lambda n: remote.command(
                        n, f"cat {shlex.quote(folder + '/node.log')}"), topo.nodes)))
            for node_id, log in logs.items():
                if any(line.startswith("ERROR ") for line in log.splitlines()):
                    raise RuntimeError(f"node {node_id} reported a runtime error")
            results = []
            for node_id in topo.clients:
                log = logs[node_id]
                found = [line.split("RESULT ", 1)[1] for line in log.splitlines()
                         if line.startswith("RESULT ")]
                if len(found) != 1:
                    raise ValueError(f"node {node_id} did not emit exactly one RESULT")
                record = json.loads(found[0])
                if (record.get("node") != node_id or
                    record.get("backend") != case["backend"] or
                    record.get("workers") != case.get("workers", 1) or
                    record.get("outstanding_per_worker") != case["outstanding"] or
                    record.get("locks") != case["locks"] or
                    record.get("critical_section_ops") != case["critical_section_ops"] or
                    record.get("handoff_gc") != int(case["backend"] == "handoff" and
                                                     suite.get("handoff_gc", False))):
                    raise ValueError(f"invalid result identity from node {node_id}")
                if record.get("measurement_seconds", 0) <= 0:
                    raise ValueError(f"invalid measurement duration from node {node_id}")
                if record.get("completed", 0) <= 0:
                    raise ValueError(f"no completed pairs from node {node_id}")
                if (record.get("latency_samples") != record["completed"] or
                        record.get("latency_sample_capacity") != suite["latency_sample_capacity"] or
                        record["completed"] > record["latency_sample_capacity"]):
                    raise ValueError(f"invalid latency sample accounting from node {node_id}")
                results.append(record)
            manifest["client_results"] = results
            if case["backend"] == "handoff":
                for record in results:
                    for key in ("handoff_lease_us", "handoff_lease_scan_us", "handoff_heartbeat_us"):
                        if record.get(key, 0) != suite.get(key, 0):
                            raise RuntimeError("lease configuration mismatch")
                    if record.get("heartbeat_writes", 0) != record.get("heartbeat_cqes", 0):
                        raise RuntimeError("heartbeat CQE accounting mismatch")
                    if suite.get("handoff_lease_us", 0) and record.get("lease_checked", 0) < record["completed"]:
                        raise RuntimeError("missing local lease checks")
                if suite.get("handoff_lease_us", 0) or suite.get("handoff_heartbeat_us", 0):
                    reports = [json.loads(line[len("HANDOFF_LEASE_MONITOR "):])
                               for line in logs[topo.replicas[0]].splitlines()
                               if line.startswith("HANDOFF_LEASE_MONITOR ")]
                    if len(reports) != 1:
                        raise RuntimeError("expected one lease monitor report")
                    manifest["lease_monitor"] = reports[0]
                    for key, field in (("handoff_lease_us", "lease_us"),
                                       ("handoff_lease_scan_us", "scan_us"),
                                       ("handoff_heartbeat_us", "heartbeat_us")):
                        if reports[0][field] != suite.get(key, 0):
                            raise RuntimeError("leader lease configuration mismatch")
                    if suite.get("handoff_lease_scan_us", 0) and not reports[0]["head_checks"]:
                        raise RuntimeError("no observed lease heads")
                    if reports[0]["heartbeat_total"] != sum(r["heartbeat_writes"] for r in results):
                        raise RuntimeError("heartbeat publication accounting mismatch")
                if suite.get("handoff_trace", False):
                    traces = [json.loads(line[len("HANDOFF_LEASE_CLIENT "):])
                              for n in topo.clients for line in logs[n].splitlines()
                              if line.startswith("HANDOFF_LEASE_CLIENT ")]
                    if sorted(t["node"] for t in traces) != sorted(topo.clients):
                        raise RuntimeError("incomplete Handoff traces")
                    manifest["lease_clients"] = traces
            if suite.get("mu_recovery", False):
                def exactly_one(log, prefix):
                    values = [json.loads(line[len(prefix):]) for line in log.splitlines()
                              if line.startswith(prefix)]
                    if len(values) != 1:
                        raise RuntimeError(f"expected one {prefix.strip()} record")
                    return values[0]
                traces = [exactly_one(logs[n], "MU_RECOVERY_CLIENT ") for n in topo.clients]
                manifest["recovery_clients"] = traces
                if suite.get("crash_after_ms") is not None:
                    recovery = exactly_one(logs[topo.replicas[1]], "MU_RECOVERY ")
                    for tr in traces:
                        if not (0 < tr["last_old_completion_ns"] <= tr["notice_ns"] <=
                                tr["ready_ns"] <= tr["first_completion_ns"] <=
                                (suite["warmup_seconds"] + suite["duration_seconds"]) * 10**9):
                            raise RuntimeError("client did not resume during measurement")
                    if recovery["recovered_length"] != max(recovery["local_length"],
                                                           recovery["remote_length"]):
                        raise RuntimeError("invalid recovered length")
                    manifest["recovery"] = recovery
                    manifest["recovery"]["max_client_gap_ns"] = max(
                        tr["first_completion_ns"] - tr["last_old_completion_ns"] for tr in traces)
                    manifest["recovery"]["max_notice_to_completion_ns"] = max(
                        tr["first_completion_ns"] - tr["notice_ns"] for tr in traces)
                elif any(tr["notice_ns"] for tr in traces):
                    raise RuntimeError("unexpected recovery in no-crash trial")
            if suite.get("profile_leader", False) and case["backend"] in ("mu", "handoff"):
                prefix = ("LEADER_PROFILE " if case["backend"] == "mu"
                          else "HANDOFF_PROFILE ")
                manifest["leader_profile"] = [
                    line for line in logs[topo.replicas[0]].splitlines()
                    if line.startswith(prefix)]
            manifest["summary"] = summarize(case, results)
            manifest["total_completed"] = manifest["summary"]["completed"]
            manifest["aggregate_goodput_pairs_per_second"] = (
                manifest["summary"]["aggregate_pairs_per_second"])
        manifest["valid"] = True
    except Exception as exc:
        manifest["error"] = str(exc)
        raise
    finally:
        if control is not None:
            control.close()
        # Parallelize collection and cleanup across hosts to keep fast sweeps fast.
        with ThreadPoolExecutor(max_workers=len(topo.nodes)) as pool:
            tasks = [(node, pool.submit(remote.collect, node, folder + "/node.log",
                                       result_dir / f"node-{node.id}.log"))
                     for node in topo.nodes]
            for node, task in tasks:
                try:
                    task.result()
                except Exception as exc:
                    manifest.setdefault("collection_errors", []).append(
                        f"node {node.id}: {exc}")
        with ThreadPoolExecutor(max_workers=len(topo.nodes)) as pool:
            tasks = [(node, pool.submit(kill_node, remote, node, folder))
                     for node in topo.nodes]
            for node, task in tasks:
                try:
                    task.result()
                except Exception as exc:
                    manifest.setdefault("cleanup_errors", []).append(
                        f"node {node.id}: {exc}")
        if control is not None:
            # Preserve supervisor diagnostics even when bootstrapping failed.
            def collect_supervisor(node):
                try:
                    remote.collect(node, folder + "/control.log",
                                   result_dir / f"control-{node.id}.log")
                finally:
                    kill_supervisor(remote, node, folder)
            try:
                remote.parallel(tuple(topo.by_id(n) for n in topo.replicas), collect_supervisor)
            except Exception as exc:
                manifest.setdefault("cleanup_errors", []).append(str(exc))
        if manifest.get("collection_errors") or manifest.get("cleanup_errors"):
            manifest["valid"] = False
        (result_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topology", type=pathlib.Path,
                    default=ROOT / "experiments/topologies/apt8.json")
    ap.add_argument("--suite", type=pathlib.Path,
                    default=ROOT / "experiments/suites/quick.json")
    ap.add_argument("--ssh-user", default=None)
    ap.add_argument("--identity", default=None)
    ap.add_argument("--remote-dir", default=None)
    ap.add_argument("--skip-deploy", action="store_true")
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-backend", choices=("cas", "mu", "handoff"))
    ap.add_argument("--max-trials", type=int)
    ap.add_argument("--locks", type=int, choices=range(1, 1001),
                    metavar="1..1000", help="Override number of locks")
    ap.add_argument("--critical-section-ops", type=int, choices=(0, 1),
                    help="0: immediate release; 1: one completed 8-byte WRITE (default)")
    ap.add_argument("--warmup-seconds", type=int, help="Override suite warmup")
    ap.add_argument("--duration-seconds", type=int, help="Override suite measurement")
    ap.add_argument("--latency-sample-capacity", type=int,
                    help="Exact samples preallocated per client (default 1.5M per measurement second)")
    ap.add_argument("--repetitions", type=int, help="Override suite repetitions")
    ap.add_argument("--show-clients", action="store_true",
                    help="Print per-client throughput and latency details")
    ap.add_argument("--profile-leader", action="store_true",
                    help="Print one-second Mu leader diagnostics during the trial")
    ap.add_argument("--mu-recovery", type=int, choices=(0, 1),
                    help="One externally triggered Mu leader replacement")
    ap.add_argument("--mu-recovery-publication", type=int, choices=(0, 1),
                    help="Ordered byte publication (1, default); raw word experiment (0)")
    ap.add_argument("--mu-recovery-replay", type=int, choices=(0, 1),
                    help="Background follower replay (1, default)")
    ap.add_argument("--crash-after-ms", type=int,
                    help="Kill initial leader this long after its local service-start marker")
    ap.add_argument("--mu-inflight-window", type=int,
                    help="Mu leader log in-flight limit (1..512; default 512)")
    ap.add_argument("--handoff-inflight-window", type=int,
                    help="Handoff leader log in-flight limit (1..512; default 512)")
    ap.add_argument("--handoff-gc", type=int, choices=(0, 1),
                    help="NEXT reclamation and exact-ID GC batches (default 0)")
    ap.add_argument("--handoff-peer-read", type=int, choices=(0, 1),
                    help="Race one bounded predecessor READ with HANDOFF (default 0)")
    ap.add_argument("--replication-batch", type=int, choices=range(1, 17),
                    help="Maximum opportunistic compact log entries per WRITE (default 8)")
    ap.add_argument("--leader-comm-batch", type=int, choices=range(1, 65),
                    help="Maximum per-QP SEND/receive repost chain (default 64)")
    ap.add_argument("--mu-lanes", type=int, choices=range(1, 9),
                    help="Independent Mu/handoff replication lanes (1..8; default 1)")
    args = ap.parse_args()
    topo = Topology.load(args.topology)
    suite = json.loads(args.suite.read_text())
    if args.locks is not None:
        suite["locks"] = args.locks
    if args.critical_section_ops is not None:
        suite["critical_section_ops"] = args.critical_section_ops
    suite.setdefault("critical_section_ops", 1)
    if not args.smoke:
        # Validate the workload before any remote deployment.
        next(cases(suite), None)
    if args.profile_leader:
        suite["profile_leader"] = True
    for key, maximum, default in (("replication_batch", 16, 8),
                                  ("leader_comm_batch", 64, 64)):
        override = getattr(args, key)
        if override is not None:
            suite[key] = override
        if not 1 <= suite.get(key, default) <= maximum:
            raise ValueError(f"{key} must be 1..{maximum}")
    if args.mu_inflight_window is not None:
        suite["mu_inflight_window"] = args.mu_inflight_window
    if not 1 <= suite.get("mu_inflight_window", 512) <= 512:
        raise ValueError("mu_inflight_window must be 1..512")
    if args.handoff_inflight_window is not None:
        suite["handoff_inflight_window"] = args.handoff_inflight_window
    if args.mu_lanes is not None:
        suite["mu_lanes"] = args.mu_lanes
    if args.handoff_gc is not None:
        suite["handoff_gc"] = bool(args.handoff_gc)
    suite.setdefault("handoff_gc", False)
    if type(suite["handoff_gc"]) is not bool:
        raise ValueError("handoff_gc must be boolean")
    if args.handoff_peer_read is not None:
        suite["handoff_peer_read"] = bool(args.handoff_peer_read)
    if suite.get("handoff_peer_read", False) not in (False, True):
        raise ValueError("handoff_peer_read must be boolean")
    if not 1 <= suite.get("handoff_inflight_window", 512) <= 512:
        raise ValueError("handoff_inflight_window must be 1..512")
    for key in ("warmup_seconds", "duration_seconds", "repetitions"):
        override = getattr(args, key)
        if override is not None:
            suite[key] = override
    if suite["warmup_seconds"] < 0 or suite["duration_seconds"] < 1 or suite["repetitions"] < 1:
        raise ValueError("warmup must be >=0; duration and repetitions must be >=1")
    if args.latency_sample_capacity is not None:
        suite["latency_sample_capacity"] = args.latency_sample_capacity
    suite.setdefault("latency_sample_capacity", 1500000 * suite["duration_seconds"])
    capacity = suite["latency_sample_capacity"]
    if type(capacity) is not int or not 1 <= capacity <= 64000000:
        raise ValueError("latency_sample_capacity must be an integer in 1..64000000")
    for key, default in (("mu_recovery", False), ("mu_recovery_publication", True),
                         ("mu_recovery_replay", True)):
        override = getattr(args, key)
        if override is not None:
            suite[key] = bool(override)
        suite.setdefault(key, default)
        if type(suite[key]) is not bool:
            raise ValueError(f"{key} must be boolean")
    if args.crash_after_ms is not None:
        suite["crash_after_ms"] = args.crash_after_ms
    if suite.get("crash_after_ms") is not None:
        delay = suite["crash_after_ms"]
        if (not suite["mu_recovery"] or type(delay) is not int or delay <= 0 or
                delay >= (suite["warmup_seconds"] + suite["duration_seconds"] - 1) * 1000):
            raise ValueError("crash requires recovery, positive delay and >1s post-crash budget")
    if suite["mu_recovery"] and (args.smoke or
            (args.only_backend or "") not in ("", "mu") or
            (args.only_backend != "mu" and suite["backends"] != ["mu"]) or
            suite.get("mu_lanes", 1) != 1 or len(topo.replicas) != 3 or
            suite["critical_section_ops"] != 0 or len(topo.clients) > 15):
        raise ValueError("Mu recovery requires Mu only, three replicas, one lane, empty CS")
    for key in ("handoff_lease_us", "handoff_lease_scan_us", "handoff_heartbeat_us"):
        suite.setdefault(key, 0)
        if type(suite[key]) is not int or not 0 <= suite[key] <= 60_000_000:
            raise ValueError(f"{key} must be an integer in 0..60000000")
    suite.setdefault("handoff_trace", False)
    if type(suite["handoff_trace"]) is not bool:
        raise ValueError("handoff_trace must be boolean")
    if suite["handoff_lease_scan_us"] and not suite["handoff_lease_us"]:
        raise ValueError("head monitoring requires a nonzero acquisition lease")
    if any(suite[k] for k in ("handoff_lease_us", "handoff_lease_scan_us",
                              "handoff_heartbeat_us", "handoff_trace")):
        if (args.smoke or (args.only_backend or "") not in ("", "handoff") or
                (args.only_backend != "handoff" and suite["backends"] != ["handoff"]) or
                suite.get("mu_lanes", 1) != 1 or suite["critical_section_ops"] != 0 or
                suite["mu_recovery"]):
            raise ValueError("lease experiments require Handoff only, one lane, empty CS, no recovery")
    if suite["replicas"] != len(topo.replicas):
        raise ValueError("suite replica count does not match topology")
    if not 1 <= suite.get("mu_lanes", 1) <= 8:
        raise ValueError("mu_lanes must be 1..8")
    commit = sha()
    remote_dir = args.remote_dir or f"/tmp/lock-{commit[:12]}"
    if not remote_dir.startswith("/tmp/lock-"):
        raise ValueError("remote_dir must start with /tmp/lock- for safe cleanup")
    user = args.ssh_user or topo.ssh_user
    remote = Remote(user, args.identity, args.dry_run,
                    local_node_id=topo.controller_node_id)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = ROOT / "results" / f"{timestamp}-{uuid.uuid4().hex[:8]}"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "suite.json").write_text(json.dumps(suite, indent=2) + "\n")
    (output_dir / "topology.json").write_text(args.topology.read_text())
    (output_dir / "source.json").write_text(
        json.dumps({"commit": commit, "remote_dir": remote_dir,
                    "controller_node_id": topo.controller_node_id,
                    "ssh_user": user}, indent=2) + "\n")
    if not args.skip_deploy:
        remote.parallel(topo.nodes, lambda n: remote.deploy(n, ROOT, remote_dir))
    if not args.skip_build:
        def build(node):
            cmd = (f"cd {shlex.quote(remote_dir)} && "
                   f"CXX=$(command -v g++-10 || command -v c++) "
                   f"cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DLOCK_WITH_RDMA=ON "
                   f"&& cmake --build build -j 4")
            remote.command(node, cmd)
        remote.parallel(topo.nodes, build)
    all_cases = [{}] if args.smoke else list(cases(suite))
    if args.only_backend and not args.smoke:
        all_cases = [c for c in all_cases if c["backend"] == args.only_backend]
    if args.max_trials:
        all_cases = all_cases[:args.max_trials]
    print(f"PLAN trials={len(all_cases)} warmup={suite['warmup_seconds']}s "
          f"measurement={suite['duration_seconds']}s repetitions={suite['repetitions']}",
          flush=True)
    valid = []
    with (output_dir / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        output.flush()
        for case in all_cases:
            print("RUN", json.dumps(case), flush=True)
            record = run_case(remote, topo, suite, case, remote_dir, output_dir, args.smoke)
            if not remote.dry_run and not record["valid"]:
                raise RuntimeError(f"invalid run: {record['name']}")
            valid.append(record)
            if "summary" in record:
                writer.writerow(record["summary"])
                output.flush()
                print("METRICS", format_summary(record["summary"]), flush=True)
                for line in record.get("leader_profile", []):
                    print(line, flush=True)
                if "recovery" in record:
                    print("RECOVERY", json.dumps(record["recovery"]), flush=True)
                    print("INJECTION", json.dumps(record["injection"]), flush=True)
                if args.show_clients:
                    for client in record["client_results"]:
                        print("  CLIENT", format_client(client), flush=True)
    recovery_rows = [dict(**r["case"], **r["recovery"]) for r in valid if "recovery" in r]
    if recovery_rows:
        with (output_dir / "recovery.csv").open("w", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=list(recovery_rows[0]))
            writer.writeheader()
            writer.writerows(recovery_rows)
        print("RECOVERY_CSV", output_dir / "recovery.csv")
    print("SUMMARY_CSV", output_dir / "summary.csv")
    print("OUTPUT", output_dir)
    print("VALID_TRIALS", len(valid))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("ERROR", exc, file=sys.stderr)
        sys.exit(1)
