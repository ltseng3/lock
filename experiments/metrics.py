"""Controller-side, per-trial goodput and latency summaries.

Each client reports its own latency percentiles. Percentiles from separate
clients cannot be pooled without raw samples; report per-node ranges and maxima.
"""
from __future__ import annotations

SUMMARY_FIELDS = (
    "backend", "distribution", "workers", "outstanding", "repeat",
    "locks", "critical_section_ops", "handoff_gc",
    "gc_next_reclaimed", "gc_ack_records", "gc_ack_messages",
    "clients", "completed", "aggregate_pairs_per_second",
    "min_client_pairs_per_second", "max_client_pairs_per_second",
    "acquire_p50_min_us", "acquire_p50_max_us", "acquire_p99_worst_us",
    "total_p50_min_us", "total_p50_max_us", "total_p99_worst_us",
    "handoffs_sent", "handoffs_received", "late_next",
    "activation_by_read", "activation_by_handoff",
    "read_attempts", "local_probes", "read_misses", "read_skipped", "redundant_handoffs",
)


def summarize(case: dict, results: list[dict]) -> dict:
    if not results:
        raise ValueError("no client results for summary")
    rates = [r["completed"] / r["measurement_seconds"] for r in results]

    def microseconds(key: str, fn) -> float:
        return fn(r[key] for r in results) / 1000.0

    return {
        **{key: case[key] for key in
           ("backend", "distribution", "workers", "outstanding", "repeat",
            "locks", "critical_section_ops")},
        "handoff_gc": case.get("handoff_gc", 0),
        **{key: sum(r.get(key, 0) for r in results) for key in
           ("gc_next_reclaimed", "gc_ack_records", "gc_ack_messages")},
        "clients": len(results),
        "completed": sum(r["completed"] for r in results),
        "aggregate_pairs_per_second": sum(rates),
        "min_client_pairs_per_second": min(rates),
        "max_client_pairs_per_second": max(rates),
        "acquire_p50_min_us": microseconds("acquire_p50_ns", min),
        "acquire_p50_max_us": microseconds("acquire_p50_ns", max),
        "acquire_p99_worst_us": microseconds("acquire_p99_ns", max),
        "total_p50_min_us": microseconds("total_p50_ns", min),
        "total_p50_max_us": microseconds("total_p50_ns", max),
        "total_p99_worst_us": microseconds("total_p99_ns", max),
        "handoffs_sent": sum(r.get("handoffs_sent", 0) for r in results),
        "handoffs_received": sum(r.get("handoffs_received", 0) for r in results),
        "late_next": sum(r.get("late_next", 0) for r in results),
        **{key: sum(r.get(key, 0) for r in results) for key in (
            "activation_by_read", "activation_by_handoff",
            "read_attempts", "local_probes", "read_misses", "read_skipped", "redundant_handoffs")},
    }


def format_summary(s: dict) -> str:
    message = (
        f"{s['backend']} {s['distribution']} k={s['workers']} "
        f"out={s['outstanding']} r={s['repeat']} "
        f"goodput={s['aggregate_pairs_per_second']:,.0f} pairs/s "
        f"client-rates=[{s['min_client_pairs_per_second']:,.0f},"
        f"{s['max_client_pairs_per_second']:,.0f}]/s "
        f"acq-p50-node-range=[{s['acquire_p50_min_us']:.1f},"
        f"{s['acquire_p50_max_us']:.1f}]us "
        f"acq-p99-worst-node={s['acquire_p99_worst_us']:.1f}us "
        f"total-p99-worst-node={s['total_p99_worst_us']:.1f}us "
        f"completed={s['completed']} locks={s['locks']}"
        f" cs-ops={s['critical_section_ops']}"
    )
    if s["backend"] == "handoff":
        message += (
            f" handoff-sent={s['handoffs_sent']}"
            f" received={s['handoffs_received']}"
            f" late-next={s['late_next']}"
            f" by-read={s['activation_by_read']} by-handoff={s['activation_by_handoff']}"
            f" reads={s['read_attempts']} local-probes={s['local_probes']}"
            f" read-misses={s['read_misses']} read-skipped={s['read_skipped']}"
            f" redundant-handoffs={s['redundant_handoffs']}"
            f" gc={s['handoff_gc']} gc-next={s['gc_next_reclaimed']}"
            f" gc-records={s['gc_ack_records']} gc-messages={s['gc_ack_messages']}"
        )
    return message


def format_client(r: dict) -> str:
    return (
        f"node={r['node']} goodput={r['completed'] / r['measurement_seconds']:,.0f} pairs/s "
        f"acq-p50={r['acquire_p50_ns'] / 1000:.1f}us "
        f"acq-p99={r['acquire_p99_ns'] / 1000:.1f}us "
        f"total-p50={r['total_p50_ns'] / 1000:.1f}us "
        f"total-p99={r['total_p99_ns'] / 1000:.1f}us"
    )
