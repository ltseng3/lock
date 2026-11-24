"""Plot a single recovery trial from its archived manifest.

Time axes are relative to each client's local start-barrier receipt, not
synchronized wall clocks. The notice/ready spans are client observations.
"""
import argparse
import json
from pathlib import Path
import csv

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("manifest", type=Path)
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()
    record = json.loads(args.manifest.read_text())
    traces = record["recovery_clients"]
    if not traces:
        raise ValueError("no recovery traces")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    bucket = traces[0]["bucket_ns"]
    if any(t["bucket_ns"] != bucket for t in traces):
        raise ValueError("inconsistent trace bins")
    n = min(len(t["buckets"]) for t in traces)
    # The final bin can be partial; omit it.
    times = [(i + 0.5) * bucket / 1e9 for i in range(n-1)]
    rates = [sum(t["buckets"][i] for t in traces) * 1e9 / bucket
             for i in range(n-1)]
    output = args.output or args.manifest.with_name("recovery-throughput.png")
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(times, [r/1e6 for r in rates], linewidth=1.1, label="Aggregate")
    if "recovery" in record:
        notice = [t["notice_ns"]/1e9 for t in traces]
        ready = [t["ready_ns"]/1e9 for t in traces]
        ax.axvspan(min(notice), max(ready), alpha=0.15, label="Client recovery notice to ready")
    ax.set_xlabel("Seconds since local start barrier (approx. aligned)")
    ax.set_ylabel("Completed pairs/s (millions)")
    ax.set_ylim(bottom=0)
    ax.grid(alpha=0.2)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    with output.with_suffix(".csv").open("w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["seconds_since_barrier", "pairs_per_second"])
        w.writerows(zip(times, rates))
    print(output)

if __name__ == "__main__":
    main()
