# Recovery design

## Scope and status

The initial implementation supports one controller-triggered leader crash,
three replicas, a predetermined replacement, one replication lane and
surviving clients with empty critical sections. Mu and Handoff leader-only recovery are implemented on this branch;
Handoff hardware validation is pending. Neither election, repeated
failure, client exclusion, leases nor replica rejoin is implemented yet.
Never treat a missing client's watchdog timeout as proof of safe expiration.

## Shared log and epochs

The 8-byte acquisition/release format is unchanged. A request ID is stable
across leadership changes. The epoch in a control message is its execution
authorization epoch, not necessarily the epoch that appended its acquisition.

Wire version 2 splits the former 32-bit version field into 16-bit version
and 16-bit epoch; messages remain 64 bytes, including GC messages.
All participants must be rebuilt together. Epoch zero is implicit.
The recovery-only log layout reserves 4 KiB before the final 16 MiB scratch
region for 512 aligned 8-byte epoch boundary records. Each record stores a
16-bit epoch and 48-bit first command slot; an empty epoch may share a
boundary with the next epoch. No wraparound is allowed.

For the supported single crash, the replacement fences the failed leader,
obtains the stable contiguous prefixes and repairs the shorter suffix.
The old log is immutable. It then publishes epoch 1's boundary to the
surviving follower, waits for its WRITE completion, and records it locally
before sending RecoveryBegin/Ready. Both surviving replicas constitute the
original majority. No extra per-command WRITE is required for epochs.
Boundary bytes alone do not prove consensus or commitment after another
failure; extending to multiple elections needs a durable quorum history,
conflicting-suffix reconciliation and publication semantics for metadata.
The existing DMA ordering and fencing assumptions remain necessary.

Mu background replay is bounded by the observed contiguous length minus
the posted-minus-applied window (or zero). Replay advances only over
commands whose predecessors are known committed. The remaining suffix is
applied after repair. Client reconciliation matches request identities and
actual execution phase, not whether an ACK was sent by the failed leader.

## Handoff recovery

Handoff logs acquisition headers only. Retirement markers and direct
HANDOFFs do not appear in the replicated acquisition history. Recover
the entire valid prefix; never delete a log entry because of a client
report. Reconstruct live queues separately.

Before using epoch 1 authority, every participating client must freeze old
admission/activation, quiesce protected operations, report a complete
snapshot of its unfinished requests, and promise to wait for resume.
Reports are bound to the new epoch and authenticated by peer identity.
A submitted but unlogged acquire may retry with the same request ID.
An ORDER alone does not prove ownership. A genuine acquired/completed
request proves that predecessors on its lock have relinquished ownership.
A quiesced active request cannot be executed twice.

The first implementation requires all configured clients to report; it
fails closed otherwise. With all clients alive and unique monotonically
allocated IDs, an acquired request absent from a complete snapshot of
unfinished requests has finished. A sparse live queue is rebuilt by log
slot; no full history scan at failover. Deferred releases that have already
finished the critical section must not be reexecuted. Preserve log slots
and request IDs; new epoch-local chain positions may be reassigned.
Install all new predecessor/successor relationships before enabling
execution. Reject old-epoch peer HANDOFFs even when their sender survives.
Do not discard old marker credits before fencing the old QP. Initially
disable peer READ and retirement GC in recovery mode.

A future missing-client path needs actual lease semantics: the old epoch
cannot be extended by a HANDOFF, and expiration must bound protected DMA
or resource-side fencing. A later verified owner can resolve an earlier
missing predecessor; a merely ordered successor cannot. Do not bypass a
possible old owner without proof of relinquishment or safe expiration.
Late excluded clients require explicit rejoin. Application atomicity is
not guaranteed by lock recovery.

## Validation

Unit tests cover exact compact encoding, contiguous prefixes, ordered
commit progress, epoch boundary history and Mu client recovery. Handoff
tests must additionally cover missing ORDER, delayed HANDOFF, deferred
retirement, inconsistent ownership reports, and queue reconstruction.
CloudLab is needed to verify DMA visibility, fencing and end-to-end
recovery. Timing starts at the external recovery trigger; report the
injection/process cleanup and client completion gap separately.

### Queue reconstruction implementation

The hardware-independent HandoffRecoveryIndex records each acquisition's
slot, lock and original per-lock ordinal. It retains only the index and
per-lock counts, not a second copy of each lock's historical queue. Given
complete quiesced snapshots from all configured clients, it validates
identity, issued-through bounds, original ORDER metadata and the prepared
window. Unfinished logged requests must form a suffix for each lock; a
hole is an invariant violation. The reconstructed HandoffState uses the
normal reservation/commit transitions without re-appending recovered
headers. The physical log stays unchanged. Unlogged pending IDs are
returned separately for normal admission. The recovery-only Handoff
layout uses 8-byte headers, 8-byte retirement words, 1-byte publication
marks and the shared 4-KiB boundary table. Failure-free layouts are unchanged.

### Runtime and client snapshot

Both backends call the same compact-prefix recovery routine: seal, repair,
establish the epoch boundary, then finish replay. Handoff clients preconnect
to the designated replacement. RecoveryBegin fences the old leader QP,
discards obsolete marker credits only after fencing, and freezes activation.
RecoveryHandoffRequest carries Pending, Ordered or Finished (completed CS
with deferred marker), plus original slot/position for ordered requests.
RecoveryDone carries the highest issued sequence and report count. Old
peer HANDOFFs are ignored even if their sender remains alive.

The replacement installs all ORDER/NEXT relationships with bounded per-peer
SEND batches and drains their CQEs before sending RecoveryReady. Clients
only install relationships while paused. A new-epoch peer HANDOFF that
arrives before Ready is buffered and validated on resume. A finished
deferred retirement is completed once locally without a new marker or CS.
Only the unlogged pending IDs are admitted again. The promoted runtime
owns the restored HandoffState and resumes appending at the recovered length.
The first implementation requires all clients and empty critical sections;
no leases or client exclusion are claimed.

### Running and interpreting the benchmark

Use the handoff_recovery.json suite with --max-trials=1 for initial
hardware validation. The default prepared window is 64, and compact
in-flight window 512. The runner rejects mixed recovery backends and
unsupported GC/READ or multi-lane configurations. Recovery time includes
the epoch boundary WRITE completion and relationship installation; it
excludes controller injection and failed-process cleanup. The archived
manifest includes per-client 10-ms samples and both leader profiles when
profiling is enabled. To measure instrumentation overhead separately,
use a suite without crash_after_ms with recovery enabled; compare against
a matched no-recovery run. A successful empty-section run is not proof
of safety for protected WRITEs or client crash recovery.

### Bounded recovery work and memory lifetime

With background replay keeping up, recovery processes the remaining
pipeline suffix plus the difference in replica lengths and the live
client snapshots. The Handoff index uses a dense per-client sequence
array and a per-lock count; reconstruction sorts only the live requests.
Historical indexes are retained until process teardown on both recovery
backends so large memory deallocation does not inflate takeover latency.
This intentionally trades memory for a predictable recovery path under
the single-replacement scope. Index memory and replay lag should be
measured before designing a longer-running reclamation policy.

The deterministic tests also generate multiple acquisition histories and
valid per-lock completion frontiers, compare the reconstructed order and
slots against an independent model, and exercise normal retirement after
reconstruction. Client regression tests cover old marker CQEs after
fencing, including a new marker credit that must not be consumed by the
old completion. These tests do not simulate NIC DMA ordering.
