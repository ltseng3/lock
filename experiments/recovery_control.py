"""One-shot crash oracle using preconnected TCP, independent of the RDMA path.

The SSH launcher starts a supervisor for each replica before the trial.
Each supervisor owns its server child. The old leader's supervisor observes
the local start marker, waits the configured delay, SIGKILLs and reaps its
child. Only then does the controller notify the two surviving supervisors.
No SSH connection, failure detection, or election runs at the crash point.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time


class Channel:
    def __init__(self, sock, timeout):
        self.sock = sock
        sock.settimeout(timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = sock.makefile("rb")

    def send(self, message):
        self.sock.sendall(json.dumps(message, separators=(",", ":")).encode() + b"\n")

    def receive(self):
        line = self.reader.readline(8193)
        if not line or len(line) > 8192 or not line.endswith(b"\n"):
            raise RuntimeError("closed or invalid recovery control channel")
        message = json.loads(line)
        if not isinstance(message, dict):
            raise RuntimeError("invalid recovery control message")
        if message.get("kind") == "error":
            raise RuntimeError("recovery supervisor: " + str(message.get("error")))
        return message

    def close(self):
        self.reader.close()
        self.sock.close()


class RecoveryControl:
    """Controller-side listener, one authenticated connection per replica."""

    def __init__(self, address, replicas, delay_ms, timeout):
        self.address = address
        self.replicas = tuple(replicas)
        if len(self.replicas) != 3 or len(set(self.replicas)) != 3 or delay_ms <= 0:
            raise ValueError("invalid single-crash control configuration")
        self.delay_ms = delay_ms
        self.timeout = timeout
        self.token = secrets.token_hex(24)
        self.channels = {}
        family = socket.AF_INET6 if ":" in address else socket.AF_INET
        self.listener = socket.socket(family, socket.SOCK_STREAM)
        try:
            self.listener.bind((address, 0))
            self.listener.listen(len(self.replicas))
            self.port = self.listener.getsockname()[1]
        except BaseException:
            self.listener.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        self.listener.close()
        for channel in self.channels.values():
            channel.close()
        self.channels.clear()

    def agent_command(self, node_id, folder, argv):
        if node_id not in self.replicas:
            raise ValueError("only replica supervisors are supported")
        return ["python3", "-u", "experiments/recovery_control.py",
                "--agent", "--host", self.address, "--port", str(self.port),
                "--token", self.token, "--node", str(node_id),
                "--folder", str(folder), "--timeout", str(self.timeout),
                "--", *argv]

    def arm(self):
        deadline = time.monotonic() + self.timeout
        while len(self.channels) != len(self.replicas):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("waiting for recovery supervisors")
            self.listener.settimeout(remaining)
            sock, _ = self.listener.accept()
            channel = Channel(sock, remaining)
            try:
                hello = channel.receive()
                node = hello.get("node")
                token = hello.get("token")
                if (hello.get("kind") != "hello" or not isinstance(token, str) or
                        not secrets.compare_digest(token, self.token) or
                        type(node) is not int or node not in self.replicas or
                        node in self.channels):
                    raise RuntimeError("invalid or duplicate supervisor identity")
                sock.settimeout(self.timeout + self.delay_ms / 1000)
                self.channels[node] = channel
            except BaseException:
                channel.close()
                raise
        # All channels exist before any server child starts.
        for node in self.replicas:
            self.channels[node].send({
                "kind": "arm",
                "delay_ms": self.delay_ms if node == self.replicas[0] else None,
                "trial_delay_ms": self.delay_ms,
            })

    def crash_and_promote(self):
        old, replacement, other = self.replicas
        event = self.channels[old].receive()
        arrived = time.monotonic_ns()
        if (event.get("kind") != "crashed" or event.get("node") != old or
                event.get("returncode") != -signal.SIGKILL):
            raise RuntimeError("old leader did not confirm crash-stop")
        start = event["start_observed_ns"]
        killed = event["kill_requested_ns"]
        returned = event["kill_returned_ns"]
        exited = event["exit_observed_ns"]
        if not start <= killed <= returned <= exited:
            raise RuntimeError("invalid leader crash timing")
        # Both sends precede either ACK wait; never serialize promotion.
        for node in (replacement, other):
            self.channels[node].send({"kind": "promote"})
        dispatched = time.monotonic_ns()
        signals = {}
        for node in (replacement, other):
            ack = self.channels[node].receive()
            if (ack.get("kind") != "promoted" or ack.get("node") != node or
                    ack["signal_returned_ns"] < ack["signal_requested_ns"]):
                raise RuntimeError("invalid promotion acknowledgment")
            signals[str(node)] = ack["signal_returned_ns"] - ack["signal_requested_ns"]
        acknowledged = time.monotonic_ns()
        return {
            "mode": "preconnected_tcp",
            "configured_delay_ms": self.delay_ms,
            "failed": old, "replacement": replacement,
            "leader_timer_actual_ns": killed - start,
            "leader_kill_syscall_ns": returned - killed,
            "leader_exit_wait_ns": exited - returned,
            "controller_dispatch_ns": dispatched - arrived,
            "controller_notification_ack_ns": acknowledged - arrived,
            "promotion_signal_syscall_ns": signals,
        }


def observe_start(proc, log, timeout):
    deadline = time.monotonic() + timeout
    with log.open("rb") as reader:
        while time.monotonic() < deadline:
            for line in reader:
                if line.startswith(b"ERROR "):
                    raise RuntimeError(line.decode(errors="replace").strip())
                if line.startswith((b"MU_SERVICE_STARTED", b"HANDOFF_SERVICE_STARTED")):
                    return time.monotonic_ns()
            if proc.poll() is not None:
                raise RuntimeError("leader exited before service start")
            time.sleep(0.001)
    raise TimeoutError("waiting for leader start marker")


def supervise(host, port, token, node, folder, timeout, argv):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    proc = None
    channel = None
    log = folder / "node.log"
    pid = folder / "pid"
    supervisor_pid = folder / "control.pid"
    supervisor_pid.write_text(str(os.getpid()) + "\n")
    try:
        channel = Channel(socket.create_connection((host, port), timeout), timeout)
        channel.send({"kind": "hello", "node": node, "token": token})
        arm = channel.receive()
        delay = arm.get("delay_ms")
        trial_delay = arm.get("trial_delay_ms")
        if (arm.get("kind") != "arm" or type(trial_delay) is not int or
                trial_delay <= 0 or (delay is not None and
                (type(delay) is not int or delay != trial_delay))):
            raise RuntimeError("invalid supervisor arm")
        channel.sock.settimeout(timeout + trial_delay / 1000)
        with log.open("wb") as output:
            proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
                                    stdout=output, stderr=subprocess.STDOUT)
        pid.write_text(str(proc.pid) + "\n")
        if delay is not None:
            start = observe_start(proc, log, timeout)
            target = start + delay * 1_000_000
            # Local monotonic timer; no remote log polling or SSH at failure.
            remaining = (target - time.monotonic_ns()) / 1e9
            if remaining > 0:
                time.sleep(remaining)
            if proc.poll() is not None:
                raise RuntimeError("leader exited before scheduled crash")
            killed = time.monotonic_ns()
            proc.kill()
            returned = time.monotonic_ns()
            # Blocking waitpid avoids subprocess.wait(timeout)'s exponential
            # sleep polling, which would itself add artificial outage.
            # The controller's socket deadline and supervisor teardown bound
            # the trial if kernel exit stalls.
            code = proc.wait()
            exited = time.monotonic_ns()
            if code != -signal.SIGKILL:
                raise RuntimeError("leader did not exit by SIGKILL")
            channel.send({"kind": "crashed", "node": node, "returncode": code,
                          "start_observed_ns": start, "kill_requested_ns": killed,
                          "kill_returned_ns": returned, "exit_observed_ns": exited})
        else:
            # No signal is accepted until the controller confirms leader exit.
            if channel.receive() != {"kind": "promote"} or proc.poll() is not None:
                raise RuntimeError("invalid promotion or standby already exited")
            requested = time.monotonic_ns()
            proc.send_signal(signal.SIGUSR1)
            returned = time.monotonic_ns()
            channel.send({"kind": "promoted", "node": node,
                          "signal_requested_ns": requested,
                          "signal_returned_ns": returned})
            channel.close()
            channel = None
            # Keep owning the child until normal runner teardown.
            proc.wait()
    except BaseException as exc:
        if channel is not None:
            try:
                channel.send({"kind": "error", "node": node, "error": str(exc)})
            except Exception:
                pass
        raise
    finally:
        if proc is not None:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            # Avoid leaving a dead PID for later cleanup (PID reuse).
            if pid.exists() and pid.read_text().strip() == str(proc.pid):
                pid.unlink()
        if channel is not None:
            channel.close()
        if supervisor_pid.exists() and supervisor_pid.read_text().strip() == str(os.getpid()):
            supervisor_pid.unlink()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--agent", action="store_true", required=True)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--node", type=int, required=True)
    ap.add_argument("--folder", required=True)
    ap.add_argument("--timeout", type=float, required=True)
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    argv = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not argv:
        ap.error("missing server child command")
    # On controller failure the finally block terminates our own child.
    def terminate(_signum, _frame):
        raise SystemExit("supervisor terminated")
    signal.signal(signal.SIGTERM, terminate)
    supervise(args.host, args.port, args.token, args.node, args.folder,
              args.timeout, argv)


if __name__ == "__main__":
    main()
