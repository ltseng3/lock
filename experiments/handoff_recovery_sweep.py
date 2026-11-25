"""Generate matched Handoff client-recovery suites, without running them."""
import argparse
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent / "suites/handoff_lease_monitor.json"
LEASE_US = 5000
SCAN_US = 100
HEARTBEAT_US = 1000


def suites(family, ppm, victim=5, delay_ms=2000):
    if family not in ("ablation", "monitor", "lease", "all"):
        raise ValueError("invalid sweep family")
    if type(ppm) is not int or not 0 <= ppm < 1_000_000:
        raise ValueError("a valid explicit clock error bound is required")
    if type(victim) is not int or not 1 <= victim <= 255:
        raise ValueError("invalid victim node")
    base = json.loads(BASE.read_text())
    if type(delay_ms) is not int or not 0 < delay_ms < (
            base["warmup_seconds"] + base["duration_seconds"] - 1) * 1000:
        raise ValueError("crash must leave a post-crash measurement window")

    def variant(name, lease, scan, heartbeat, recovery=False, crash=False):
        config = dict(base, name=name, handoff_lease_us=lease,
                      handoff_lease_scan_us=scan, handoff_heartbeat_us=heartbeat,
                      handoff_client_recovery=recovery)
        if recovery:
            config.update(handoff_activation_us=LEASE_US,
                          handoff_clock_error_ppm=ppm)
        if crash:
            config.update(client_crash_node=victim,
                          client_crash_after_ms=delay_ms)
        return name, config

    if family in ("ablation", "all"):
        yield variant("baseline", 0, 0, 0)
        yield variant("lease-only", LEASE_US, 0, 0)
        yield variant("head-only", LEASE_US, SCAN_US, 0)
        yield variant("heartbeat-only", LEASE_US, 0, HEARTBEAT_US)
        yield variant("both-monitors", LEASE_US, SCAN_US, HEARTBEAT_US)
        yield variant("recovery-no-crash", LEASE_US, SCAN_US, HEARTBEAT_US, True)
        yield variant("recovery-crash", LEASE_US, SCAN_US, HEARTBEAT_US, True, True)

    if family in ("monitor", "all"):
        # Keep the other interval fixed; avoid duplicate reference points.
        intervals = [(scan, HEARTBEAT_US) for scan in (32, 100, 250, 500, 1000)]
        intervals += [(SCAN_US, hb) for hb in (100, 250, 500, 2000)]
        for scan, hb in intervals:
            for crash in (False, True):
                name = f"scan-{scan}-hb-{hb}-" + ("crash" if crash else "no-crash")
                yield variant(name, LEASE_US, scan, hb, True, crash)

    if family in ("lease", "all"):
        for lease in (500, 1000, 2000, 5000, 10000, 20000):
            for crash in (False, True):
                name = f"lease-{lease}-" + ("crash" if crash else "no-crash")
                yield variant(name, lease, SCAN_US, HEARTBEAT_US, True, crash)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", choices=("ablation", "monitor", "lease", "all"),
                        default="ablation")
    parser.add_argument("--clock-error-ppm", type=int, required=True)
    parser.add_argument("--victim", type=int, default=5)
    parser.add_argument("--delay-ms", type=int, default=2000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    variants = list(suites(args.family, args.clock_error_ppm, args.victim, args.delay_ms))
    args.output.mkdir(parents=True, exist_ok=True)
    for name, _ in variants:
        if (args.output / (name + ".json")).exists():
            parser.error("refusing to overwrite existing suite: " + name)
    for name, config in variants:
        path = args.output / (name + ".json")
        with path.open("x") as out:
            json.dump(config, out, indent=2)
            out.write("\n")
        print(path)


if __name__ == "__main__":
    main()
