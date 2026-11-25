# Fixed acquisition leases and advisory monitoring

Based on recovery/mu (409a87b). Failure-free Handoff only; existing Mu recovery unchanged. No client exclusion, lease renewal, timeout-based regrant or Handoff leader recovery.

Each acquisition starts its own lease at validated activation (predecessor-free ORDER or HANDOFF; peer READ if enabled). Queue waiting is not charged. Reuse acquired timestamp and check elapsed monotonic time before retirement of an empty CS; fail closed on overrun. The leader periodically samples unresolved prepared heads and counts suspicion once per unchanged head. Head observation age is NOT the client's lease age: delayed ORDER/NEXT/HANDOFF, retirement markers and scans can shift observation earlier or later. No timeout changes ownership.

Optional heartbeat: one increasing 8-byte counter per client, 64-byte-strided reserved MR words. Inline signaled WRITE on existing client-leader QP, one outstanding at a time; no response, no receive WQE, no extra thread or QP. Leader communication loop samples locally; five unchanged periods trigger suspicion. Clients drain heartbeat CQEs before Stop; runner validates total published counters. Heartbeat does not renew a lease.

Config: handoff_lease_us (0 disables), handoff_lease_scan_us (0 disables; requires lease), handoff_heartbeat_us (0 disables), handoff_trace (raw 10ms throughput). Flags apply only to Handoff with one lane, empty CS, no failover. Existing log and message format unchanged. Compare (lease,scan,heartbeat)=(0,0,0),(5000,0,0),(5000,100,0),(5000,100,1000) with identical trace/profiling. Counts include warmup/drain. Head counts are diagnostic snapshots at Stop; a final scan may precede Stop slightly.

Run python3 experiments/run.py --suite=experiments/suites/handoff_lease_monitor.json --max-trials=1 --remote-dir=/tmp/lock-lease-monitor-first

A safe timeout-based regrant requires a conservative activation/expiry bound or published deadline with missing-publication handling; typical RDMA delay is not a bound. One-sided renewal additionally requires fresh expiry observation, exact acquisition identity, timely CQE and old-authority closure. Protected DMA requires fencing or bounded quiescence. None are claimed here.
