import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from cluster import Remote, Topology
from unittest import mock
import tempfile
from run import cases, process_command, wait_for, validate_transport_diagnostics, snapshot_rdma_counters

class TestExperiment(unittest.TestCase):
    def setUp(self):
        self.topo = Topology.load(ROOT / "experiments/topologies/apt8.json")
        self.suite = json.loads((ROOT / "experiments/suites/first.json").read_text())

    def test_ip_mapping_and_partition(self):
        self.assertEqual(len(self.topo.nodes), 8)
        self.assertEqual(self.topo.replicas, (1,2,3))
        self.assertEqual(self.topo.clients, (4,5,6,7,8))
        self.assertEqual(self.topo.controller_node_id, 4)
        self.assertEqual(self.topo.ssh_user, "tsengle")
        self.assertEqual(self.topo.by_id(8).rdma_ip, "10.10.1.8")
        self.assertIn("1@10.10.1.1", self.topo.peers_arg())

    def test_case_count(self):
        self.assertEqual(len(list(cases(self.suite))), 3 * 3 * 2 * 5 * 5)

    def test_fast_default_suite(self):
        quick = json.loads((ROOT / "experiments/suites/quick.json").read_text())
        self.assertEqual(len(list(cases(quick))), 3 * 2 * 2 * 5)
        self.assertEqual(quick["warmup_seconds"], 1)
        self.assertEqual(quick["duration_seconds"], 3)
        self.assertEqual(quick["repetitions"], 1)

    def test_five_replica_topology(self):
        other = Topology.load(ROOT / "experiments/topologies/apt8_five.json")
        self.assertEqual(other.replicas, (1,2,3,4,5))
        self.assertEqual(other.clients, (6,7,8))
        self.assertEqual(other.controller_node_id, 4)
        self.assertEqual(other.ssh_user, "tsengle")
        self.assertEqual(other.by_id(8).ssh_host, "10.10.1.8")

    def test_profile_leader_only_on_replicas(self):
        case = next(cases(self.suite))
        enhanced = dict(self.suite, profile_leader=True)
        server = process_command(self.topo, self.topo.by_id(1), enhanced, case)
        client = process_command(self.topo, self.topo.by_id(4), enhanced, case)
        self.assertIn("--profile-leader=1", server)
        self.assertNotIn("--profile-leader=1", client)

    def test_transport_diagnostic_flags(self):
        case = dict(next(cases(self.suite)), backend="handoff")
        suite = dict(self.suite, client_receive_depth=512,
                     server_receive_depth=256, min_rnr_timer=8, mu_lanes=1)
        validate_transport_diagnostics(suite)
        for node in self.topo.nodes:
            args = process_command(self.topo, node, suite, case)
            depth = 512 if node.role == "client" else 256
            self.assertIn(f"--receive-depth={depth}", args)
            self.assertIn("--min-rnr-timer=8", args)
            default = process_command(self.topo, node, self.suite, case)
            self.assertFalse(any(a.startswith("--receive-depth=") for a in default))
        for override in (dict(client_receive_depth=1024), dict(min_rnr_timer=0),
                         dict(server_receive_depth=True), dict(mu_lanes=2)):
            with self.assertRaises(ValueError):
                validate_transport_diagnostics(dict(suite, **override))

    def test_rdma_counter_snapshot(self):
        remote = mock.Mock()
        remote.command.return_value = "/sys/counter\t42\n/sys/unavailable\tN/A\n"
        snapshot = snapshot_rdma_counters(remote, self.topo)
        self.assertEqual(len(snapshot), 8)
        self.assertEqual(snapshot["4"]["counters"], {"/sys/counter": 42})
        remote.command.side_effect = RuntimeError("counter unavailable")
        snapshot = snapshot_rdma_counters(remote, self.topo)
        self.assertEqual(snapshot["4"]["counters"], {})
        self.assertIn("error", snapshot["4"])

    def test_handoff_recovery_flags(self):
        suite = dict(self.suite, handoff_recovery=True)
        case = dict(next(cases(suite)), backend="handoff")
        for node in self.topo.nodes:
            args = process_command(self.topo, node, suite, case)
            self.assertIn("--handoff-recovery=1", args)
            self.assertNotIn("--mu-recovery=1", args)
            self.assertEqual("--mu-recovery-publication=1" in args,
                             node.role == "replica")
        case["backend"] = "mu"
        for node in self.topo.nodes:
            self.assertNotIn("--handoff-recovery=1",
                             process_command(self.topo, node, suite, case))

    def test_latency_capacity_only_on_clients(self):
        tuned = dict(self.suite, latency_sample_capacity=8000000)
        for backend in ("cas", "mu", "handoff"):
            case = dict(next(cases(tuned)), backend=backend)
            for node in self.topo.nodes:
                args = process_command(self.topo, node, tuned, case)
                self.assertEqual("--latency-sample-capacity=8000000" in args,
                                 node.role == "client")

    def test_mu_window_only_on_server(self):
        case = dict(next(cases(self.suite)), backend="mu")
        tuned = dict(self.suite, mu_inflight_window=256)
        leader = process_command(self.topo, self.topo.by_id(1), tuned, case)
        client = process_command(self.topo, self.topo.by_id(4), tuned, case)
        self.assertIn("--mu-inflight-window=256", leader)
        self.assertNotIn("--mu-inflight-window=256", client)
        self.assertIn("--mu-inflight-window=512",
                      process_command(self.topo, self.topo.by_id(1), self.suite, case))

    def test_handoff_window_only_on_server(self):
        case = dict(next(cases(self.suite)), backend="handoff")
        tuned = dict(self.suite, handoff_inflight_window=256)
        leader = process_command(self.topo, self.topo.by_id(1), tuned, case)
        client = process_command(self.topo, self.topo.by_id(4), tuned, case)
        self.assertIn("--handoff-inflight-window=256", leader)
        self.assertNotIn("--handoff-inflight-window=256", client)

    def test_critical_section_mode_and_lock_count(self):
        for ops in (0, 1):
            tuned = dict(self.suite, locks=1, critical_section_ops=ops)
            for backend in ("cas", "mu", "handoff"):
                case = dict(next(cases(tuned)), backend=backend)
                self.assertEqual(case["locks"], 1)
                self.assertEqual(case["critical_section_ops"], ops)
                for node in self.topo.nodes:
                    args = process_command(self.topo, node, tuned, case)
                    self.assertIn("--locks=1", args)
                    self.assertEqual(f"--critical-section-ops={ops}" in args,
                                     node.role == "client")
        self.assertEqual(next(cases(self.suite))["critical_section_ops"], 1)
        for invalid in (-1, 2, True, "0"):
            with self.assertRaises(ValueError):
                next(cases(dict(self.suite, critical_section_ops=invalid)))
        with self.assertRaises(ValueError):
            next(cases(dict(self.suite, locks=0)))

    def test_gc_only_on_handoff_and_both_roles(self):
        tuned = dict(self.suite, handoff_gc=True)
        for backend in ("cas", "mu", "handoff"):
            case = next(c for c in cases(tuned) if c["backend"] == backend)
            self.assertEqual(case["handoff_gc"], int(backend == "handoff"))
            for node in self.topo.nodes:
                args = process_command(self.topo, node, tuned, case)
                self.assertEqual("--handoff-gc=1" in args, backend == "handoff")
        self.assertEqual(next(c for c in cases(self.suite)
                              if c["backend"] == "handoff")["handoff_gc"], 0)

    def test_peer_read_only_on_handoff_clients(self):
        tuned = dict(self.suite, handoff_peer_read=True)
        for backend in ("cas", "mu", "handoff"):
            case = dict(next(cases(self.suite)), backend=backend)
            for node in self.topo.nodes:
                args = process_command(self.topo, node, tuned, case)
                self.assertEqual("--handoff-peer-read=1" in args,
                                 backend == "handoff" and node.role == "client")
        case = dict(next(cases(self.suite)), backend="handoff")
        self.assertIn("--handoff-peer-read=0", process_command(
            self.topo, self.topo.by_id(4), self.suite, case))

    def test_leader_batch_flags_only_on_replicated_servers(self):
        tuned = dict(self.suite, replication_batch=16, leader_comm_batch=32)
        for backend in ("cas", "mu", "handoff"):
            case = dict(next(cases(self.suite)), backend=backend)
            for node in self.topo.nodes:
                args = process_command(self.topo, node, tuned, case)
                expected = backend != "cas" and node.role == "replica"
                self.assertEqual("--replication-batch=16" in args, expected)
                self.assertEqual("--leader-comm-batch=32" in args, expected)

    def test_mu_lanes_stay_consistent_on_clients_and_replicas(self):
        # The transport bootstrap requires both ends of each client-leader
        # link to select the same number of QPs.
        for count in (1, 2, 4, 8):
            suite = dict(self.suite, mu_lanes=count)
            for backend in ("mu", "handoff"):
                case = dict(next(cases(self.suite)), backend=backend)
                for node in self.topo.nodes:
                    args = process_command(self.topo, node, suite, case)
                    self.assertIn(f"--mu-lanes={count}", args)

    def test_mu_lanes_forwarded_to_all_nodes(self):
        tuned = dict(self.suite, mu_lanes=4)
        for backend in ("mu", "handoff"):
            case = dict(next(cases(self.suite)), backend=backend)
            for node in self.topo.nodes:
                command = process_command(self.topo, node, tuned, case)
                self.assertIn("--mu-lanes=4", command)

    def test_multi_lanes_mixed_suite_keeps_cas_single(self):
        tuned = dict(self.suite, mu_lanes=4)
        case = dict(next(cases(self.suite)), backend="cas")
        for node in self.topo.nodes:
            command = process_command(self.topo, node, tuned, case)
            self.assertIn("--mu-lanes=1", command)
            self.assertNotIn("--mu-lanes=4", command)

    def test_command_has_ips_and_identity(self):
        case = next(cases(self.suite))
        command = process_command(self.topo, self.topo.by_id(8), self.suite, case)
        self.assertIn("--node-id=8", command)
        self.assertIn("--replicas=1,2,3", command)
        self.assertIn("--clients=4,5,6,7,8", command)
        self.assertIn("--workers=1", command)
        self.assertIn("--peers=" + self.topo.peers_arg(), command)

    def test_controller_node_runs_locally(self):
        remote = Remote("tsengle", local_node_id=4)
        local = self.topo.by_id(4)
        other = self.topo.by_id(5)
        completed = mock.Mock(returncode=0, stdout="ok", stderr="")
        with mock.patch("cluster.subprocess.run", return_value=completed) as call:
            self.assertEqual(remote.command(local, "printf ok"), "ok")
            self.assertEqual(call.call_args.args[0], ["bash", "-lc", "printf ok"])
            self.assertEqual(remote.command(other, "printf ok"), "ok")
            argv = call.call_args.args[0]
            self.assertEqual(argv[0], "ssh")
            self.assertIn("tsengle@10.10.1.5", argv)

    def test_local_deploy_never_uses_ssh(self):
        remote = Remote("tsengle", local_node_id=4)
        local = self.topo.by_id(4)
        other = self.topo.by_id(5)
        with mock.patch.object(remote, "command"), mock.patch(
                "cluster.subprocess.run") as call:
            remote.deploy(local, pathlib.Path("/home/tsengle/lock"), "/tmp/lock-test")
            argv = call.call_args.args[0]
            self.assertEqual(argv[0], "rsync")
            self.assertEqual(argv[-1], "/tmp/lock-test/")
            self.assertNotIn("ssh", " ".join(argv))
            remote.deploy(other, pathlib.Path("/home/tsengle/lock"), "/tmp/lock-test")
            self.assertIn("tsengle@10.10.1.5:/tmp/lock-test/", call.call_args.args[0])

    def test_wait_for_surfaces_early_error(self):
        remote = mock.Mock()
        remote.dry_run = False
        remote.command.side_effect = lambda node, script, check=False: (
            "ERROR RDMA: ibv_reg_mr: errno=12" if node.id == 1 else "PENDING")
        with self.assertRaisesRegex(RuntimeError, "node 1: ERROR RDMA: ibv_reg_mr"):
            wait_for(remote, self.topo.nodes, "/tmp/lock-test/runs/smoke",
                     "RDMA_SMOKE_OK", 60)
        self.assertEqual(remote.command.call_count, 8)

    def test_wait_for_parallel_success(self):
        remote = mock.Mock()
        remote.dry_run = False
        remote.command.return_value = "READY"
        wait_for(remote, self.topo.nodes, "/tmp/lock-test/runs/smoke",
                 "RDMA_SMOKE_OK", 60)
        self.assertEqual(remote.command.call_count, 8)

    def test_local_log_collection_copies_directly(self):
        remote = Remote("tsengle", local_node_id=4)
        with tempfile.TemporaryDirectory() as folder:
            source = pathlib.Path(folder) / "node.log"
            destination = pathlib.Path(folder) / "archive" / "node-4.log"
            source.write_text("RESULT {}\n")
            remote.collect(self.topo.by_id(4), str(source), destination)
            self.assertEqual(destination.read_text(), "RESULT {}\n")

if __name__ == "__main__":
    unittest.main()
