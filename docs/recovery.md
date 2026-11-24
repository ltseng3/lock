# Recovery design

## Scope and status

The initial implementation supports one controller-triggered leader crash,
three replicas, a predetermined replacement, one replication lane and
surviving clients with empty critical sections. Mu recovery is implemented;
Handoff recovery is being developed separately. Neither election, repeated
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

## Handoff design (planned integration)

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
