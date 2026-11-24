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

Each Mu lane has one communication thread and one replication/state-application
thread. Each owns distinct RC QPs and a distinct CQ; two bounded SPSC queues
carry validated mutations and committed responses. Mu replicates contiguous
batches of 8-byte records via inline one-sided RDMA WRITEs to every follower;
followers sleep after initialization. Quorum progress uses completed follower
prefixes, applied in increasing lane-local order. Full source messages remain
only in the leader's bounded in-flight ring.

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
use contiguous lane ranges of cache-line-aligned length:
`global_slot = lane * lane_capacity + local_slot`. The same global slot
identifies both the header and its marker sidecar entry. There is no cross-lane global mutation order (distinct locks do
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

Handoff client markers are posted inline and use a bounded WR credit
window, not registered payload buffers. All are signaled on the leader's
base QP and checked against a monotonically increasing FIFO completion
token in a separate work-ID namespace. If credits are exhausted, the
operation remains pending and resumes on a marker completion. Verify that direct handoffs still make
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

## Compact batch replication invariants

A lane owns a disjoint contiguous range of compact physical slots.
The range length is rounded down to eight words; the final remainder
is unused. All replicas must agree on memory size, lane count and
layout. Handoff markers use the same physical slot index as their
corresponding header. Neither log wraparound nor recovery is implemented.

A batch is a nonempty contiguous range, copied into local log memory and
posted inline to every follower. Every follower's signaled WRITE CQE
identifies its exclusive end; validate completion against the FIFO of
posted batch ends. The quorum prefix is the majority order statistic of
follower prefixes (leader counts as one). Apply only entries below that
prefix, once and in increasing lane-local order. No cross-lane total
order is implied. The apply ring is bounded separately from the per-follower
SQ credit (512 outstanding batches); a slow follower can exert backpressure
even if a quorum is making progress. A partial verbs post or CQ error is
fatal, not silently retried. A successful CQE is not a crash-persistent
commit record, and recovery needs explicit epochs, fencing, a negotiated
layout and a safe prefix reconstruction protocol.

Communication batches group already available responses by destination
without changing FIFO order on any client/lane QP. Copy received messages
before reposting buffers. Tail SEND completions retire a bounded per-peer
prefix of WR credits. A bounded staging batch retains responses when
credits are unavailable; the existing inter-thread queue still fails
closed on overflow. Barriers reserve SQ capacity and keep token zero.
Do not add a delay waiting to fill a batch.

### Optional retirement GC

With `--handoff-gc=1`, a predecessor with a committed successor needs no
ReleaseAck. Its unique validated NEXT provides the information required to
send HANDOFF and discard the local retired record. A late NEXT reclaims
the record after sending HANDOFF; an early NEXT avoids inserting it at all.
The state owner still checks every retirement marker in order but emits
explicit reclamation notifications only when removing the final prepared
entry. Once removed without a successor, the entry cannot acquire one.
The communication worker packs up to five exact request IDs per GC SEND
on the existing ordered client lane QP. No timer, no cumulative sequence
assumption across locks, no new QP or thread. Singletons flush immediately.
The client checks sender, lane, exact ID, uniqueness and retired state before
erasing a complete batch. In GC mode, new admissions stop when the sum of
active and retained records reaches 4096; existing work and GC continue.
STOP requires all marker CQEs, HANDOFF receipts and retired records to drain.
For each final prepared owner, the explicit GC packet certifies that
preceding markers have been processed by the state owner.
The baseline per-retirement ACK remains selectable (default). Do not mix
GC modes or builds between endpoints. Recovery fencing and reclamation
across membership changes are not implemented.

Hardware-free leader_batch tests exercise the production compact log and
response sender through a recording transport: singleton flush, contiguous
payloads and offsets, ring reuse with a late non-quorum CQE, three/five-node
majority progress, malformed completions, slow-follower credits, and
NEXT-before-ACK ordering across interleaved destination batches. These tests
do not substitute for real verbs and multi-lane hardware testing.

Both replication and response posting use the same fixed-capacity per-QP
completion-prefix tracker. This avoids heap allocations for completion
bookkeeping and makes exact CQE validation independent of the protocol.

## Empty critical-section workload

Zero application operations do not skip ordering or ownership checks.
After validated activation, the client queues its slot for a local release
at the end of the current event handler, before handling another CQE or
submitting new work. A same-client HANDOFF can append another ready slot;
the iterative drain avoids recursion and prevents nested release from
invalidating request-map iterators. No new request is submitted during the
drain, so its storage is bounded by the operation window. The ordinary
nonempty critical section still releases only after a protected WRITE CQE.
Regression tests drive CAS unlock, Mu replicated release, a same-client
prepared handoff chain, and a peer READ winner with a late redundant
HANDOFF through the actual client state machine. No RDMA device is used
by these tests. The one-sided markers and their CQEs, ReleaseAck reclamation,
proof validation, and shutdown barrier remain necessary in both workloads.

## Fixed latency sample budget

Clients allocate and initialize two exact uint64_t latency arrays before
sending Ready. Each measured completion records one acquisition and one
total-latency sample by index; the measured path never grows these arrays.
The default capacity is 1.5 million samples per measurement second per
client, adjustable via --latency-sample-capacity. A trial fails rather
than truncate or allocate on overflow. The runner checks completed pairs
equal the number of stored samples. Exact nearest-rank p50/p99/p99.9
percentiles are computed by sorting the populated ranges after the Stop
barrier. This instrumentation is independent of the backend and recovery.
