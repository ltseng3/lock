import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from cluster import Remote, Topology
from unittest import mock
import tempfile
from run import cases, process_command

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

    def test_five_replica_topology(self):
        other = Topology.load(ROOT / "experiments/topologies/apt8_five.json")
        self.assertEqual(other.replicas, (1,2,3,4,5))
        self.assertEqual(other.clients, (6,7,8))
        self.assertEqual(other.controller_node_id, 4)
        self.assertEqual(other.ssh_user, "tsengle")
        self.assertEqual(other.by_id(8).ssh_host, "10.10.1.8")

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
