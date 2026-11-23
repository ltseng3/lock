import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from cluster import Topology
from run import cases, process_command

class TestExperiment(unittest.TestCase):
    def setUp(self):
        self.topo = Topology.load(ROOT / "experiments/topologies/apt8.json")
        self.suite = json.loads((ROOT / "experiments/suites/first.json").read_text())

    def test_ip_mapping_and_partition(self):
        self.assertEqual(len(self.topo.nodes), 8)
        self.assertEqual(self.topo.replicas, (1,2,3))
        self.assertEqual(self.topo.clients, (4,5,6,7,8))
        self.assertEqual(self.topo.by_id(8).rdma_ip, "10.10.1.8")
        self.assertIn("1@10.10.1.1", self.topo.peers_arg())

    def test_case_count(self):
        self.assertEqual(len(list(cases(self.suite))), 3 * 3 * 2 * 5 * 5)

    def test_five_replica_topology(self):
        other = Topology.load(ROOT / "experiments/topologies/apt8_five.json")
        self.assertEqual(other.replicas, (1,2,3,4,5))
        self.assertEqual(other.clients, (6,7,8))
        self.assertEqual(other.by_id(8).ssh_host, "10.10.1.8")

    def test_command_has_ips_and_identity(self):
        case = next(cases(self.suite))
        command = process_command(self.topo, self.topo.by_id(8), self.suite, case)
        self.assertIn("--node-id=8", command)
        self.assertIn("--replicas=1,2,3", command)
        self.assertIn("--clients=4,5,6,7,8", command)
        self.assertIn("--workers=1", command)
        self.assertIn("--peers=" + self.topo.peers_arg(), command)

if __name__ == "__main__":
    unittest.main()
