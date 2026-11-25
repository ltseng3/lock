# Handoff transport stall diagnostics

## Observation and hypothesis

At 640 outstanding requests, high-contention workloads can show synchronized
millisecond-scale acquisition tails and throughput dips despite spare log
pipeline capacity. The client-facing receive queues have 128 buffers per QP,
while the leader permits 512 outstanding response SENDs per client QP.
ORDER, NEXT and release acknowledgements share this queue. A transient lack
of receive WQEs can cause RNR NAK retries and head-of-line blocking; client
peer HANDOFFs can propagate delays through contended ownership chains.
The current encoded min_rnr_timer=12 advertises 640 microseconds. Encoding 8
advertises 160 microseconds. These facts motivate a controlled ablation,
not a conclusion that RNR is the root cause.

## Diagnostic experiment

Run on the eight-node, single-lane topology:

    python3 -u experiments/run_handoff_stalls.py

The suite keeps 640 outstanding requests, Zipf 0.99, 1000 locks, empty
critical sections, ordered publication, background replay and GC/READ off.
It compares receive depth 128 against 512 independently at clients and
servers, varies the timer encoding, includes no-crash recovery-enabled
controls, a prepared-window-128 control and a repeated baseline.
Defaults remain unchanged in normal runs. The suite accepts optional
client_receive_depth/server_receive_depth (128..512) and min_rnr_timer
(1..31). The latter is an encoded value, not a duration. Both server and
client binaries accept receive-depth and min-rnr-timer options.

The diagnostic runner captures available read-only InfiniBand sysfs
counters before launch and after cleanup, outside measured execution.
Counters are device/port wide, not QP-specific; unsupported or unreadable
counters are not evidence of zero retries. Use deltas only for matching
counter paths and nondecreasing values, and note that crash cleanup and
other traffic may contribute. Successful RNR retries need not produce
error CQEs. If available, rq_num_rnr and sq_num_rnr are stronger evidence
than retry-exhaustion counters.

Compare per-client and aggregate raw throughput bins, tail latency, and
counter deltas. If increasing only client receive depth eliminates dips,
suspect client-facing receives (including peer HANDOFFs); a server-only
improvement instead points to request ingress. A shorter RNR timer that
reduces the tail without changing depth supports a retry-delay mechanism.
If none improve, instrument poll gaps, queue backlog and marker credits
at finer resolution. No changes to ownership, replication or recovery
semantics are made in this diagnostic branch. Do not change defaults
until hardware measurements justify a change.
