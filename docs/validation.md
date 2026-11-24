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

A predecessor-free committed ORDER is now sufficient to start the client
without sending a second START message. For requests with a predecessor, the
leader sends NEXT and the predecessor transfers ownership using HANDOFF.
The client still verifies that ORDER came from the leader and preserves
early-HANDOFF and late-NEXT handling.

## Parallel Mu lanes: ordering and hardware boundary

With `--mu-lanes=N`, each lock is assigned to exactly one lane by
`lock_id % N`. The lane has an independent worker, quorum completion queue,
and follower QPs; committed order is strict within a lane. Global log slots
use `local_slot*N+lane` to preserve disjoint 8-byte header/marker sidecar
addresses. There is no cross-lane global mutation order (distinct locks do
not depend on such an order in the common-path prototype). All replicas
must bootstrap the same number of lane QPs before clients start. Followers
remain CPU-passive for one-sided writes.

A regression trace verifies that retirement before committing a successor
avoids assigning an already-retired predecessor, while a nonretired predecessor
still enforces FIFO waiting. Unit tests also cover 1/2/4/8 lane mapping,
address uniqueness and bounds. CI cannot validate device-level CQ routing,
multi-QP setup, NIC progress, per-lock exclusivity under contention, or any
failure/recovery property. Start with low load before throughput sweeps.

## Targeted and bounded handoff retirement checks

The handoff leader no longer polls every active lock on every empty-CQ loop.
Before committing an acquisition it checks only the same lock's retirement
frontier, removing visible predecessors and preserving strict FIFO. Independently,
each lane scans all active locks periodically (32-us minimum spacing) so an
RDMA one-sided retirement marker is eventually acknowledged even when no more
commits arrive. Active-list membership has an O(1) index maintained on
swap-removal; unit tests cover targeted release, unrelated-lock isolation,
out-of-order evidence, invalid identifiers, and full-scan progress after
targeted removals.

A 32-us periodic interval is a scheduling tradeoff, not a hardware durability
guarantee. Progress and latency still require tests on the RDMA adapters,
especially when multiple prepared successors exist for a contended lock.

## Independent client QPs and marker backpressure

Both Mu and handoff leader lanes now poll disjoint client communication CQs,
post responses to disjoint per-client QPs and dispatch requests directly to
the corresponding lane's SPSC queue. The client chooses the lane before
transmission. The client still has one polling thread and a shared CQ but
validates that leader replies arrive on the expected QP lane. The lane-zero
QP retains READY/STOP barrier traffic and peer-to-peer HANDOFF uses the
ordinary client-to-client QP.

The connection bootstrap exchanges one QP per lane for both replica-peer and
client/leader-peer relationships. Each extra client QP has its own receive
buffers; completion WR IDs encode the receive-QP lane and slot so reposting
is directed to the same QP. Only one thread posts/polls each leader QP/CQ.

Handoff client marker buffers are bounded and cannot be reused before the
corresponding WRITE CQE. If all are occupied, the complete operation remains
pending and resumes on a marker completion, rather than failing with
`marker-buffer pool exhausted`. Verify that direct handoffs still make
progress when markers are delayed and that all marker WRs have completed
before the client sends STOP. These are correctness requirements, not
guaranteed by CI's hardware-free tests.

## Peer READ activation safety boundary

Only an ordered successor may probe its exact predecessor. An aligned word
published after the protected data WRITE CQE is compared to the full
RequestId, not a boolean; ring collisions cause misses, not false grants.
This assumes coherent CPU/NIC memory and aligned 64-bit access on the
target platform. No support is claimed for process restart, failover, or
reusing request IDs inside a live session.

The client event loop serializes READ CQEs and HANDOFF receives. A READ winner
records the expected late HANDOFF identity (request, lock, predecessor,
position); consume it exactly once without reactivation, even after slot
reuse. A HANDOFF winner keeps its pending READ result buffer and generation
until CQE, and ignores the later successful proof for activation. The ledger
is bounded (4096 entries), and peer READ depth is bounded to one per peer.
Shutdown waits for all expected HANDOFFs, outstanding READs, marker WRs
and release ACKs before the all-client barrier; the completion region stays
registered until all clients have drained. ACKs reclaim retirement records
because the leader emits any NEXT before retirement ACK on the same
ordered lane/client QP.

The hardware-free fake transport test drives production client transitions:
READ first, delayed HANDOFF after slot reuse, HANDOFF first with READ still
outstanding after retirement, mismatched proof, ring collision, local probe,
and early HANDOFF. It also checks leader NEXT-before-ACK ordering.
CI cannot establish coherent DMA visibility, remote READ progress,
hardware exclusivity, or performance; physical testing remains required.
