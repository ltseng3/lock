import sys
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from metrics import SUMMARY_FIELDS, format_client, format_summary, summarize


class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.case = dict(backend="cas", distribution="uniform",
                         workers=1, outstanding=1, repeat=0)
        self.results = [
            dict(node=4, completed=300, measurement_seconds=3,
                 acquire_p50_ns=2000, acquire_p99_ns=12000,
                 total_p50_ns=3000, total_p99_ns=20000,
                 handoffs_sent=0, handoffs_received=0, late_next=0),
            dict(node=5, completed=600, measurement_seconds=3,
                 acquire_p50_ns=5000, acquire_p99_ns=23000,
                 total_p50_ns=6000, total_p99_ns=40000,
                 handoffs_sent=0, handoffs_received=0, late_next=0),
        ]

    def test_summary_rates_and_node_percentiles(self):
        s = summarize(self.case, self.results)
        self.assertEqual(set(s), set(SUMMARY_FIELDS))
        self.assertEqual(s["completed"], 900)
        self.assertEqual(s["aggregate_pairs_per_second"], 300)
        self.assertEqual(s["min_client_pairs_per_second"], 100)
        self.assertEqual(s["max_client_pairs_per_second"], 200)
        self.assertEqual(s["acquire_p50_min_us"], 2)
        self.assertEqual(s["acquire_p50_max_us"], 5)
        self.assertEqual(s["acquire_p99_worst_us"], 23)
        self.assertEqual(s["total_p99_worst_us"], 40)
        self.assertIn("goodput=300 pairs/s", format_summary(s))
        self.assertIn("acq-p99-worst-node=23.0us", format_summary(s))
        self.assertIn("node=4", format_client(self.results[0]))

    def test_handoff_counters(self):
        case = dict(self.case, backend="handoff")
        results = [dict(r) for r in self.results]
        results[0].update(handoffs_sent=8, handoffs_received=7, late_next=1)
        results[1].update(handoffs_sent=2, handoffs_received=3, late_next=4)
        summary = summarize(case, results)
        self.assertEqual(summary["handoffs_sent"], 10)
        self.assertEqual(summary["handoffs_received"], 10)
        self.assertEqual(summary["late_next"], 5)
        self.assertIn("late-next=5", format_summary(summary))

    def test_empty_results_rejected(self):
        with self.assertRaisesRegex(ValueError, "no client results"):
            summarize(self.case, [])


if __name__ == "__main__":
    unittest.main()
