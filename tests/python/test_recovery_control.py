import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))
from recovery_control import RecoveryControl


class TestRecoveryControl(unittest.TestCase):
    def test_preconnected_crash_and_promotion(self):
        # Real local TCP and child processes; no RDMA/SSH required.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            processes = []
            with RecoveryControl("127.0.0.1", (1, 2, 3), 250, 10) as control:
                try:
                    for node in (1, 2, 3):
                        folder = root / str(node)
                        folder.mkdir()
                        if node == 1:
                            # Wait for both standby handlers to be installed.
                            code = (
                                "import pathlib,time\n"
                                f"p=pathlib.Path({str(root)!r})\n"
                                "while not all((p/str(n)/'armed').exists() for n in (2,3)): time.sleep(.001)\n"
                                "print('MU_SERVICE_STARTED',flush=True)\n"
                                "while True: time.sleep(1)\n"
                            )
                        else:
                            code = (
                                "import pathlib,signal,time\n"
                                f"p=pathlib.Path({str(folder)!r})\n"
                                f"old=pathlib.Path({str(root / '1' / 'pid')!r})\n"
                                "def promote(*args):\n"
                                "    # Controller may only promote after child exit.\n"
                                "    import os\n"
                                "    if old.exists():\n"
                                "        try: os.kill(int(old.read_text()),0)\n"
                                "        except ProcessLookupError: pass\n"
                                "        else: raise RuntimeError('old leader still alive')\n"
                                "    (p/'promoted').touch()\n"
                                "    raise SystemExit(0)\n"
                                "signal.signal(signal.SIGUSR1,promote)\n"
                                "(p/'armed').touch()\n"
                                "while True: time.sleep(1)\n"
                            )
                        cmd = control.agent_command(node, folder,
                                                    [sys.executable, "-u", "-c", code])
                        processes.append(subprocess.Popen(cmd, cwd=ROOT,
                                                          stdout=subprocess.DEVNULL,
                                                          stderr=subprocess.PIPE))
                    control.arm()
                    result = control.crash_and_promote()
                    self.assertEqual(result["mode"], "preconnected_tcp")
                    self.assertEqual(result["failed"], 1)
                    self.assertEqual(result["replacement"], 2)
                    self.assertGreaterEqual(result["leader_timer_actual_ns"], 250_000_000)
                    self.assertGreaterEqual(result["leader_exit_wait_ns"], 0)
                    self.assertGreaterEqual(result["controller_notification_ack_ns"],
                                            result["controller_dispatch_ns"])
                    self.assertEqual(set(result["promotion_signal_syscall_ns"]), {"2", "3"})
                    for p in processes:
                        _, err = p.communicate(timeout=10)
                        self.assertEqual(p.returncode, 0, err.decode(errors="replace"))
                    self.assertTrue((root / "2" / "promoted").exists())
                    self.assertTrue((root / "3" / "promoted").exists())
                    self.assertFalse((root / "1" / "pid").exists())
                finally:
                    for p in processes:
                        if p.poll() is None:
                            p.terminate()
                            try:
                                p.communicate(timeout=5)
                            except subprocess.TimeoutExpired:
                                p.kill()
                                p.communicate()
                        if p.stderr:
                            p.stderr.close()

    def test_reject_invalid_membership(self):
        with self.assertRaises(ValueError):
            RecoveryControl("127.0.0.1", (1, 2, 2), 100, 1)
        with self.assertRaises(ValueError):
            RecoveryControl("127.0.0.1", (1, 2, 3), 0, 1)


if __name__ == "__main__":
    unittest.main()
