# Handoff client recovery

Status: opt-in implementation, with deterministic unit and runner tests.
Physical RDMA fault behavior, clock assumptions and performance require
hardware validation before production use.

## Scope

The repository contains three lock backends: CAS, Mu, and Handoff.
Handoff uses Mu as its replicated ordering substrate but transfers ownership
directly between clients. This extension targets Handoff only. Preserve CAS
and standalone Mu semantics and existing Mu leader recovery.

Initial configuration: healthy leader and followers, one lane, one injected
client crash-stop, exclusive locks, empty critical sections, no client rejoin,
no renewal, peer READ and retirement GC disabled. Combined leader/client
failures and arbitrary protected RDMA operations are out of scope.

## Invariants

- Committed acquisition order determines FIFO. Recovery removes failed and
  already executed acquisitions, but never changes the relative order of
  surviving unexecuted acquisitions. New arrivals cannot overtake them.
- One execution per logical acquisition, even across recovery attempts.
- At most one valid execution authority per lock. Retirement is irreversible.
- No activation without exact committed identity, authorized predecessor or
  leader START, and an unexpired authority attempt.
- A failed completion, missed heartbeat, or old observed head is not proof
  of expired ownership.
- Recovery is per lock; unaffected locks and peers continue to make progress.
- Preserve the 8-byte compact log, follower passive common path, existing
  batching and zero additional synchronous fast-path network round trips.
- The communication worker owns client QPs; the replication/state worker
  exclusively owns lock queues. Cross this boundary using bounded events
  and actions, not shared mutable protocol state.

## Fixed authority contract

Current activation-based hold timers do not bound when a delayed ORDER or
HANDOFF can create authority. Keep a fixed nonrenewable holding lease T
(initially 5000 us) and introduce a bounded activation window A (initially
5000 us). A is an admission/activation timeout, not a lease renewal.

For the initial attempt the client starts its local activation timer before
posting ACQUIRE. It may activate only before A elapses, then exercise
authority for at most T. Check every activation path, including buffered
HANDOFF, predecessor-free ORDER and local handoff. At equality expire.
A waiting timeout does not silently discard a committed logical acquisition:
the leader must resolve it, retaining FIFO order for eligible survivors.

For a request committed at leader-local time c, the client submission
causally precedes c. Given clock rates in [1-rho, 1+rho] relative to physical
time, the leader can conservatively wait until
c + (A+T)*(1+rho)/(1-rho), rounded upward. No clock-offset synchronization
or assumed network-latency bound is needed. Verify and document a sound
clock-rate bound and clocks advancing through process pauses before enabling
timeout-based regrant. Missing clock assumptions must fail closed.

For a recovered attempt, the client starts its new activation timer before
sending its recovery-ready report; the leader timestamps receipt (or a
later preparation event). The same causal bound applies. Never start a
fresh timer on receipt of a delayed ORDER. A delayed attempt that has already
expired must return to reconciliation without losing its logical FIFO rank.
If a waiter repeatedly expires, recover or retry in FIFO order rather
than spin or silently bypass it. Progress requires eventual timely delivery
and processing. Measure activation expiration separately from hold expiration.

A single submission-relative deadline is a simpler alternative with a
tighter bound but consumes the holding budget while waiting. Published
activation deadlines require missing-publication handling; synchronized
absolute deadlines require a clock-offset bound. Neither is required here.

Empty critical sections are deliberate: expiration alone cannot fence
outstanding protected RDMA WRITEs or application execution. General
protected operations require a resource-fencing or bounded-quiescence
contract before being supported.

## Detection and isolation

Generalize the existing opt-in recoverable-peer transport support, without
changing default fatal behavior or Mu leader-recovery semantics. A peer
failure emits one event; stop posts and receive reposts on isolated QPs,
account for partial SEND chains and flush completions, and abandon only
that destination's response credits. Never retry a partially posted chain
as a whole. Local programming errors, invalid messages and unexpected
replica failures remain fatal. HANDOFF failure must not prevent retirement
marker publication by a quiesced sender.

Keep existing asynchronous 8-byte heartbeat mailboxes and head scans.
The receive-ring allocator originally started at the same 0x40000 address
as the heartbeat mailboxes. Reserve all 256 cache-line-strided slots before
starting the receive rings; this changes only local receive-buffer addresses
and reduces their reserved space by 16 KiB. Re-run monitoring baselines with
the corrected layout; do not assume previous counters were unaffected.
Heartbeat suspicion and peer transport failure can initiate exclusion
under an explicit irreversible-for-this-run policy. Head age identifies
a stalled chain, not the actual holder. Healthy but expired requests can
also trigger lock repair. Do not exclude a whole client solely for head age.
Report false suspicions and false exclusions; distinguish these from actual
crash injection. With optional head-age monitoring disabled, recovery still
checks authority deadlines on the existing retirement scan. Head-only
recovery cannot guarantee timely exclusion of an idle crashed client that
has no outstanding operations or transport failures; keep heartbeat enabled
for the primary crash trials. Exclusion stops new admission but does not revoke authority
already in flight. Do not confuse peer QP isolation with global resource
fencing.

## FIFO-preserving repair

Keep the normal path unchanged. Use a per-lock NORMAL / RECOVERING state.
On exclusion, scan current lock queues (no reverse index initially).
Remove failed requests that have never been prepared; if a failed request
is already prepared, or authority has expired, repair that lock. Existing
retirement evidence may resolve some entries.

1. Freeze preparation, retain original logical FIFO order and append new
   arrivals behind existing waiters. Drain already reserved/posted log
   entries to normal commitment without publishing new old-chain authority.
   Account for request SPSC, waiting, reservations, append_pending, in-flight
   log entries, prepared entries and staged responses.
2. Observe matching retirement markers. A missing head marker can hide
   successful downstream HANDOFFs. Wait for all possibly authorized
   unresolved entries to retire or reach their conservative expiry bounds.
3. Reconcile with surviving clients using exact logical acquisition and
   old attempt identity. Each survivor closes the old attempt, reports
   never-activated or executed/quiesced, and acknowledges that stale
   activation cannot resume. Do not wait for reports from excluded clients;
   if another participant becomes unavailable, stop or explicitly extend
   the failure model rather than skip an unproven authority. Reject
   inconsistent reports. Never replay a completed acquisition, including
   locally completed requests with missing markers or deferred retirement.
4. Preserve relative FIFO of never-executed survivors, including unprepared
   waiters. Issue fresh attempts in that order before newer arrivals.
   Keep monotonic positions/log slots; never reuse a marker identity.
   Fresh wire identities and monotonically increasing positions distinguish
   rebuilt authority; no separate per-lock wire generation is needed.
   Old messages cannot start or modify a new attempt. The first survivor
   receives leader-authorized ORDER without a predecessor, subsequent owners
   use normal direct HANDOFF.
5. Reopen admission, release obsolete bookkeeping and maintain exact
   reservation/credit counts. Recovery completes only once survivors can
   proceed; the run must also handle a queue that becomes empty.

The implementation retains logical FIFO rank at the leader and logical
operation timing at the client, with a fresh wire RequestId per recovery
attempt. A separate per-lock generation is an alternative, not a requirement.
Fresh wire IDs keep the existing exact-RequestId marker scheme safe; simply
reusing a RequestId across generations would allow late marker WRITEs to
retire the wrong attempt unless slots and marker validation are redesigned.
The client must not choose fresh FIFO rank on its own. Map the fresh attempt
to the retained logical request and rank under a leader-controlled handshake.
Choose the smallest explicit recovery-only message set after tests specify
the transitions; do not repurpose unrelated fields or overload local
operation-slot generation. A leader epoch and per-lock chain generation
are different concepts. No new replicated recovery metadata is required
under the healthy-leader assumption; do not claim durability across
leader replacement.

A simpler unordered retry would break FIFO. In-place predecessor splicing
could reduce repair latency but must first establish downstream execution
state and invalidate old grants. Start with conservative whole unresolved
suffix reconciliation. Scan current active state on failure rather than
maintain an index on every normal acquisition. Optimize only after measuring.

## Crash injection and validation

Reuse the local process-supervision approach in experiments/recovery_control.py.
Timed SIGKILL relative to the selected client's observed start marker; record
request, syscall return and exit confirmation. Do not feed the controller's
crash knowledge into Handoff recovery. Protocol-point crashes and delayed
messages belong in deterministic fake-transport tests first. Keep the
controller alive independently of the victim process.

The startup barrier still requires all configured clients; initially inject
only after it completes. Stop and result validation account for confirmed
victim exit and every survivor, not missing logs as implicit success.
Do not allow exclusion alone to masquerade as injected crash confirmation.

## Evaluation plan

Primary lease and activation window remain 5000 us. Match the existing
one-lane Handoff suite: 1000 locks, three replicas, five clients, uniform
and Zipf 0.99, four workers/client, 8 or 16 outstanding/worker, prepared
window 64, replication batch 16, communication batch 64, empty CS.

Compare baseline, lease only, head scan only with lease, heartbeat only
with lease, both monitors, and recovery enabled (no crash). Compare crash
trials with recovery enabled and a matched four-client no-crash control.
Sweep one parameter at a time: scan 32/100/250/500/1000 us; heartbeat
100/250/500/1000/2000 us; initially five missed intervals. If needed later,
vary holding lease 500/1000/2000/5000/10000/20000 us with activation window
fixed, explicitly recording both. No adaptive leases.

Measure goodput, tail latency, actual heartbeat posts/CQEs/skips, scan
work, CPU cost, suspicions/exclusions, expired attempts, affected locks,
FIFO violations, detection, expiry wait, repair, first affected-lock
completion and unaffected-lock progress. Use monotonic local intervals;
do not subtract unrelated host clock epochs. Report incomplete trials,
survivor accounting and injection teardown separately. Keep existing
10-ms traces and add recovery events for finer timing.

## Implementation order

- [x] Per-destination response isolation and regression tests.
- [x] Reserve heartbeat mailboxes outside receive buffers.
- [x] Opt-in recoverable client transport primitives (single lane); retain
  fatal defaults and the existing Mu old-leader path.
- [x] Connect transport events to Handoff runtime, direct HANDOFF failure
  handling, admission and stop barrier.
- [ ] Validate physical QP failure/teardown and clock assumptions on hardware.
- [x] Reusable deterministic client crash supervision, client service-start
  marker and local process test.
- [x] Wire client crash supervision and survivor result validation into runner.
- [x] Pure conservative lease arithmetic and boundary tests.
- [x] Enforce the activation gate and stale-identity handling; deterministic
  expiry and delayed-activation tests.
- [x] Per-lock recovery, survivor reconciliation, FIFO-preserving rebuild
  and stale retirement/activation tests; no new common-path RPC.
- [ ] Adversarial tests: failure before/after HANDOFF and marker, failed
  waiting/reserved/in-flight requests, repeated signals, partial SENDs,
  delayed old ORDER/NEXT/HANDOFF/ACK, local handoff, shutdown and hot locks.
- [x] Generate monitoring ablations, fixed-lease crash sweeps and reproducible manifests.
- [ ] Run the hardware sweeps; add per-lock first-completion and unaffected-lock
  progress measurements. Current repair counters describe leader reconciliation,
  not the first resumed critical section.

Run existing CAS/Mu/Handoff tests throughout. Hardware tests must establish
QP teardown behavior and progress; unit tests alone are insufficient.
Each stage is independently reviewable. Enable recovery explicitly and only
with a justified clock-rate bound and the restricted empty-CS configuration.

## Running the experiments

Generate matched suites from the existing lease-monitor baseline. The clock
bound is deliberately required rather than guessed. Set CLOCK_ERROR_PPM to
a justified bound for the test machines; setting it to zero asserts ideal
clock rates and is not a safe default.

```sh
python3 experiments/handoff_recovery_sweep.py --family ablation \
    --clock-error-ppm "$CLOCK_ERROR_PPM" --output /tmp/handoff-ablation
python3 experiments/handoff_recovery_sweep.py --family monitor \
    --clock-error-ppm "$CLOCK_ERROR_PPM" --output /tmp/handoff-monitor
python3 experiments/run.py --topology experiments/topologies/apt8.json \
    --suite /tmp/handoff-ablation/recovery-crash.json
```

The primary families keep both lease and activation window at 5000 us.
The monitor family varies one monitor interval at a time. An optional
`--family lease` generates a separate fixed-per-trial holding-lease
sensitivity sweep, leaving activation at 5000 us. It is not needed for the
initial fixed-lease milestone. Each generated suite is independently
runnable, with the same workload and seed settings. Use `--family all`
only if both primary and sensitivity suites are wanted.

Crash variants default to client 5 at 2000 ms after its own service-start
marker; override with `--victim` and `--delay-ms`. No crash information
is passed to the lock protocol. No-crash recovery is a distinct baseline.
For the four-client capacity control, run the no-crash recovery suite
with a topology that omits the victim client. Crash-trial aggregate
goodput counts only survivors across the full measurement interval.
Report the victim's missing counters separately: leader reconciliation
counts may include work performed by that victim before its crash, so
only no-crash trials can require exact final counter equality.

Start with a single short trial and inspect the manifest for the confirmed
exit, exact excluded identity, completed repairs, survivor results and
errors. Inspect traces for pauses before expanding the sweep. The runner
does not silently classify missing results as successful recovery.
