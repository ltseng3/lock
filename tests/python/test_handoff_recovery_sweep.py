import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from cluster import Topology
from handoff_recovery_sweep import suites
from run import validate_handoff_recovery


class TestHandoffRecoverySweep(unittest.TestCase):
    def test_all_variants_validate(self):
        topo = Topology.load(ROOT / "experiments/topologies/apt8.json")
        variants = list(suites("all", 100))
        self.assertEqual(len(variants), 37)
        self.assertEqual(len({name for name, _ in variants}), len(variants))
        for name, config in variants:
            validate_handoff_recovery(config, topo)
            self.assertEqual(config["backends"], ["handoff"])
            if config["handoff_client_recovery"]:
                self.assertEqual(config["handoff_activation_us"], 5000)
                self.assertEqual(config["handoff_clock_error_ppm"], 100)
            if "client_crash_node" in config:
                self.assertEqual(config["client_crash_node"], 5)

    def test_monitor_changes_one_parameter_at_a_time(self):
        for _, config in suites("monitor", 100):
            self.assertEqual(config["handoff_lease_us"], 5000)
            self.assertTrue(config["handoff_lease_scan_us"] == 100 or
                            config["handoff_heartbeat_us"] == 1000)
        for _, config in suites("lease", 100):
            self.assertEqual(config["handoff_activation_us"], 5000)
            self.assertEqual(config["handoff_lease_scan_us"], 100)
            self.assertEqual(config["handoff_heartbeat_us"], 1000)

    def test_invalid_bound(self):
        for bound in (-1, 1_000_000, None):
            with self.assertRaises(ValueError):
                list(suites("monitor", bound))


if __name__ == "__main__":
    unittest.main()
