# Validation and limitations

The default topology has three replicas (nodes 1–3) and five client nodes (4–8), each with explicit SSH and RDMA addresses. A second topology and suite exercise five replicas on the same eight machines.

## Automated checks

The GitHub workflow builds with libibverbs and runs core, FIFO, handoff-queue, longer handoff-trace and workload-picker unit tests, plus Python topology/controller tests. These run without InfiniBand equipment and do not demonstrate hardware protocol correctness.

The full CloudLab smoke run connects all eight peers using RC QPs and checks SEND/RECV, a completed 8-byte RDMA WRITE, and a successful remote CAS to the first replica. The leader checks both values and sends an acknowledgment.

The experiment controller verifies every client reports a nonzero count and the configured identity, checks per-node error lines, archives all logs and marks incomplete trials invalid. Run it from a CloudLab node with SSH access to all specified IP addresses.

## Correctness boundary

- Static, homogeneous nodes and a fixed leader. Missing, late, malformed, or mismatched control messages cause errors or the watchdog; there is no timeout-based takeover.
- No leader election, crash recovery, clock-bounded grant enforcement or durable storage. This is a failure-free common-path prototype; it is not a complete crash-recoverable lock.
- A completed protected RDMA WRITE precedes local retirement. Handoff SEND and release-marker RDMA WRITE are independent. Incoming activation requires a committed ORDER plus START or a matching predecessor HANDOFF.
- Log slots are never reused. A leader stops on exhaustion; increase registered memory or shorten a trial. Default server MR is 512 MiB and client MR is 128 MiB, which require appropriate memlock limits.
- The worker count represents logical independent request streams in one client event loop, not separate OS threads. Clients can set outstanding requests per stream from one upward.
- No result may be presented as a measured speedup without running the binaries on actual target hardware.

## Mu compact-log and split-leader validation

Mu uses a separate 8-byte mutation record with explicit bounds: 8-bit node ID,
16-bit lock ID, 39-bit nonzero sequence, and 1-bit acquire/release kind. Values
outside the range are rejected rather than truncated. The global slot is the
log order, so it need not appear inside the word. In this failure-free prototype
there is no replay/leader takeover; log contents alone are not evidence of a
crash-recoverable service.

The Mu leader has one communication thread and one replication/state-application
thread. Each owns distinct RC QPs and a distinct CQ; two bounded SPSC queues
carry validated mutations and committed responses. Mu replication WRITEs are
8-byte inline one-sided RDMA WRITEs to every follower; follower processes sleep
after initialization and do not poll CQs or run application logic in the measured
path. A local record plus enough signaled follower WRITE completions establishes
a quorum; the commit prefix is applied in global slot order. Full source
messages remain only in the leader's bounded in-flight ring.

This change has unit tests for the encoding and concurrent queue. It has NOT
been hardware-tested or validated for performance on CloudLab. Successful
compilation does not establish RDMA WRITE correctness, FIFO under load, or
failover safety. Test low concurrency, quorum ordering, completion handling,
five-replica operation, sustained runs, and corrupted/out-of-range requests
before relying on the new path.

## Handoff compact-header layout

The handoff backend writes only an 8-byte, packed ACQUIRE header to the
replicated append-only log using inline one-sided RDMA WRITEs. Full request
metadata remains in the leader's bounded local in-flight ring. For each
assigned slot the leader's registered memory also reserves a separate,
adjacent 8-byte release marker, addressed as

    marker_offset = kLogOffset + capacity * 8 + slot * 8

where capacity is calculated consistently from the leader's exported memory
size. Header and marker ranges do not overlap the last 16 MiB scratch region.
The marker is initialized to zero and the releasing client writes its full
request ID to the marker after the protected data WRITE completes. The leader
checks the marker against the committed holder's request ID before ACKing
retirement, and scans only locks with committed, outstanding holders.

The handoff leader has two threads and separate completion queues: one thread
owns client QPs and sends ORDER/NEXT/START/ACK; the other owns follower QPs,
the ordered quorum log, lock queue state, and marker checks. Follower processes
remain passive after the RC bootstrap. Client-to-client HANDOFF notification
remains direct, with late NEXT handled by the client as before.

This is an engineering implementation with no verified fault recovery.
Hardware latency, ordered commits, early/late notifications, the sidecar
addressing, and operation under contention MUST be validated on the target
RDMA devices; a green compilation/unit-test workflow is not sufficient.
