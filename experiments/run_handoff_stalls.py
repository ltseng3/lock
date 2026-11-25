#!/usr/bin/env python3
"""Targeted high-concurrency Handoff transport ablation, not a protocol change."""
import argparse
import datetime
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    tag = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    folder = pathlib.Path("/tmp") / ("handoff-stalls-" + tag)
    folder.mkdir()
    remote = "/tmp/lock-handoff-stalls-" + tag
    base = json.loads((ROOT / "experiments/suites/handoff_recovery.json").read_text())
    base.update(distributions=["zipf_0.99"], outstanding=[32],
                crash_after_ms=3000, duration_seconds=7,
                repetitions=args.repetitions, profile_leader=True,
                latency_sample_capacity=8000000, collect_rdma_counters=True)
    # The no-crash controls retain recovery instrumentation and publication.
    # Change one dimension at a time, with a second baseline at the end.
    variants = [
        ("baseline", 128, 128, 12, True, 64),
        ("client-rq512", 512, 128, 12, True, 64),
        ("server-rq512", 128, 512, 12, True, 64),
        ("both-rq512", 512, 512, 12, True, 64),
        ("short-rnr", 128, 128, 8, True, 64),
        ("both-rq512-short-rnr", 512, 512, 8, True, 64),
        ("no-crash-baseline", 128, 128, 12, False, 64),
        ("no-crash-both-rq512", 512, 512, 12, False, 64),
        ("prepared128", 128, 128, 12, True, 128),
        ("baseline-repeat", 128, 128, 12, True, 64),
    ]
    suites = []
    for label, client, server, timer, crash, prepared in variants:
        suite = dict(base, name=f"{tag}-{label}", client_receive_depth=client,
                     server_receive_depth=server, min_rnr_timer=timer,
                     prepared_window=prepared)
        if not crash:
            suite.pop("crash_after_ms")
        suites.append(suite)
    (folder / "plan.json").write_text(json.dumps(suites, indent=2))
    names = {s["name"] for s in suites}
    output = pathlib.Path("/tmp/handoff-stalls-data.json")

    def export():
        runs = []
        for p in sorted((ROOT / "results").glob("*/suite.json")):
            suite = json.loads(p.read_text())
            if suite.get("name") not in names:
                continue
            source = p.parent / "source.json"
            trials = [json.loads(m.read_text()) for m in sorted(p.parent.rglob("manifest.json"))]
            runs.append(dict(suite=suite, source=json.loads(source.read_text()),
                             result_dir=str(p.parent), trials=trials))
        payload = dict(batch=tag, planned_trials=len(suites)*args.repetitions,
                       planned_suites=suites, runs=runs)
        temp = output.with_suffix(".tmp")
        temp.write_text(json.dumps(payload))
        temp.replace(output)
        print("EXPORTED", sum(len(r["trials"]) for r in runs), "trials", output, flush=True)

    print("PLAN", len(suites)*args.repetitions, "trials", folder, flush=True)
    try:
        for i, suite in enumerate(suites):
            path = folder / (suite["name"] + ".json")
            path.write_text(json.dumps(suite, indent=2))
            print("SUITE", i+1, "/", len(suites), suite["name"], flush=True)
            cmd = [sys.executable, "experiments/run.py", "--suite", str(path),
                   "--remote-dir", remote]
            if i:
                cmd += ["--skip-build", "--skip-deploy"]
            subprocess.run(cmd, cwd=ROOT, check=True)
            export()
    finally:
        export()
    print("BATCH_COMPLETE", flush=True)


if __name__ == "__main__":
    main()
