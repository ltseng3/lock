# Eight-node CloudLab runs

The experiment controller runs on **node 4 (10.10.1.4)**, and uses the SSH user **tsengle** by default.

The topology files contain `"controller_node_id": 4` and `"ssh_user": "tsengle"`. For node 4, the controller runs shell commands and rsync locally and copies logs directly; **no SSH or SCP to 10.10.1.4** is attempted. All other nodes are accessed by SSH as `tsengle@10.10.1.X`. The SSH and RDMA addresses remain independently configurable via `ssh_host` and `rdma_ip`.

Run from the repository checkout on node 4:

    git checkout main && git pull origin main
    python3 experiments/run.py --smoke
    python3 experiments/run.py --only-backend=cas --max-trials=1
    python3 experiments/run.py

You can override the username with `--ssh-user=...` if needed. **The controller must actually be running on node 4**, since commands targeting controller_node_id are executed locally rather than over SSH. For a different controller machine, update `controller_node_id` in the topology before running.

The default layout has replica nodes 1–3 and client nodes 4–8. For the optional five-replica experiment, nodes 1–5 are replicas and nodes 6–8 are clients; node 4 remains the local controller, regardless of its experimental role.

    python3 experiments/run.py --topology experiments/topologies/apt8_five.json --suite experiments/suites/five_small.json

Install CMake, a C++20 compiler, libibverbs-dev and rsync on all eight nodes, and ensure SSH connectivity from node 4 to the **other seven** hosts. The InfiniBand port must be active and memory-registration limits must support the configured MR sizes (512 MiB by default on servers). Raw logs and a run manifest are saved under `results/`.

See docs/validation.md for the correctness boundary. Automated CI cannot verify CloudLab hardware behavior.

## Ubuntu 20.04 / CMake 3.16.3

CMake 3.16.3 is supported; no CMake upgrade is required. A C++20-capable
compiler and standard library are required (GCC 10+). If the default compiler
is GCC 9, install `g++-10` on each node and configure a fresh build directory
using `CXX=g++-10 cmake -S . -B build -DLOCK_WITH_RDMA=ON`. The experiment runner automatically prefers `g++-10` when installed. Do not reuse
a build directory configured with a different compiler. The legacy CTest
invocation is `(cd build && ctest --output-on-failure)`.

## Smoke startup diagnostics and RDMA memory registration

The runner polls the eight nodes concurrently and reports early `ERROR` lines
without waiting for the whole watchdog interval. SCP log collection is quiet.

If smoke reports `ibv_reg_mr: errno=12`, check `ulimit -l` on the affected
node. This is the maximum locked memory in KiB. Clients register 128 MiB and
servers register 512 MiB; a default limit of 65536 KiB (64 MiB) is insufficient.
On Ubuntu, configure a higher soft and hard `memlock` limit for the
experiment user through `/etc/security/limits.d/`, then start a **new login
session** (including on local controller node 4) and verify `ulimit -l`.
Other resource constraints may also cause ENOMEM.

## Fast exploratory sweeps and stdout metrics

The default `experiments/suites/quick.json` tests three backends, uniform and
Zipf-0.99 access, workers 1/4, and outstanding windows 1/2/4/8/16:
**60 cases**, one repetition each, with **1 second warmup + 3 seconds
measurement** per case. The default is for quickly locating throughput
saturation and contention, **not** for stable published measurements.

Run on node 4:

    python3 experiments/run.py --only-backend=cas --max-trials=1
    python3 experiments/run.py
    python3 experiments/run.py --show-clients
    python3 experiments/run.py --duration-seconds=5 --warmup-seconds=2

The original full sweep remains unchanged:

    python3 experiments/run.py --suite experiments/suites/first.json

After each valid trial, `METRICS` prints aggregate completed lock+write pairs
per second (the sum of per-client rates), the min/max client rates, the range
of client acquire p50 values, the **worst individual client's** acquire and
end-to-end p99 latencies, and total completed pairs. Handoff trials also
show aggregated handoff and late-NEXT counters. **These are NOT pooled
cluster-wide percentiles**; the client sends percentile summaries rather
than raw latency samples. `--show-clients` expands to one row per client.

Incremental numeric summaries are written to
`results/<run>/summary.csv`, alongside per-trial manifests and logs.
The runner now reads logs, collects results, and kills leftover processes
across hosts concurrently to reduce control-plane overhead. For a repeated
sweep of the **same already-deployed commit**, you can additionally pass
`--skip-deploy --skip-build --remote-dir=<existing-dir>` once the binaries
have been verified, but do not reuse binaries after source changes.

## Mu leader diagnostics and working-tree experiments

The experiment runner accepts modified/uncommitted source files: it deploys the
files from the current checkout and **does not block on a dirty working tree**.
The `source.json` commit SHA identifies the most recent commit, **not** a
guarantee that uncommitted content matches that revision. For reproducible
results, archive your local patch separately.

For a single Mu diagnostic run, use a suite that selects only Mu, uniform
access, four workers and 16 outstanding requests per worker, then pass
`--profile-leader`. With this flag, leader node 1 emits approximately
one-second `LEADER_PROFILE` records, which are also echoed on node 4.
Each record reports acquire/release receives, log commits, signaled replication
WRITE completions, send completions, outgoing responses, and maximum pending
or in-flight log work. `max_inflight` matching the configured Mu admission window
(default 512, adjustable with `--mu-inflight-window`) indicates it was full; growing `max_pending` can indicate backlog.

This counter-based diagnostic does not attribute CPU cycles or NIC service
time and is not, by itself, proof of a specific bottleneck. It adds a small
amount of instrumentation overhead and is disabled by default.

## Mu throughput hot-path tuning

The Mu leader now uses a larger default replication admission window of **512**
log entries (formerly 128), configurable via `--mu-inflight-window=N` (1–512).
The 3-replica log posts a WRITE to both followers, but only requires a
completion for one follower to establish the leader+follower majority. The
other WRITE is periodically signaled to retire work requests on its QP;
the registered log slots are never reused. For 5 replicas, completion from
two followers establishes the local+2 majority. The transport forces a
signaled WR at least once per 16 posted WRs on each QP so unacknowledged work
requests cannot accumulate indefinitely. Mu leader responses similarly
use selective signaling. The client and leader reuse CQ output buffers
and use a stack buffer for the standard 64-entry CQ poll.

With your existing single-trial `/tmp/mu-throughput.json` (Mu/uniform,
4 logical workers/client, 16 outstanding per worker), compare the new code
against your previous run:

    python3 experiments/run.py --suite=/tmp/mu-throughput.json --profile-leader --mu-inflight-window=512

To isolate the effect of the admission window, pass `--mu-inflight-window=128`
and compare. This is **not** a multi-case sweep. The leader log remains
non-recycling (default 512 MiB server MR), so a faster implementation may
exhaust it on long runs; the 3-second suite is for initial diagnosis, not
unbounded throughput testing. CI checks compilation and unit tests without
RDMA hardware and cannot verify the performance or device-level semantics.
