# Roadmap

What Keystone does not do yet, why each gap exists, and what closing it would
actually cost. Ordered by how much it matters.

A design document that lists only strengths is a sales brochure. These are the
things I would want an interviewer to ask about.

## 1. Log compaction and snapshots

**The gap.** The Raft log grows without bound. A replica rebuilding from empty
replays every entry ever committed. Disk use is a function of total writes, not
of live data.

This is the most serious gap. Everything else on this list is a performance or
convenience limitation; this one eventually makes the system unoperable, because
replacing a failed replica gets slower every day the cluster stays up.

**What it takes.** Standard Raft snapshotting (§7):

- Serialize the MVCC store at an applied index — every live key at its newest
  version at or below the snapshot index, plus the dedupe cache and the GC
  watermark, since both are state-machine state that must survive.
- Truncate the log below that index.
- `InstallSnapshot` RPC for followers that have fallen behind the truncation
  point.
- The subtle part: snapshotting must not stall the apply loop. That means
  either a copy-on-write store or a bounded write pause, and the skip list
  would need an immutable-view mechanism to do it well.

**Estimate.** Two to three weeks including crash-consistency tests. The
serialization format needs the same care as the log format — a snapshot that
cannot be read by a newer binary is an upgrade outage.

## 2. Follower reads

**The gap.** Every read goes to the leader. Adding replicas increases fault
tolerance but adds no read capacity, which is backwards for a read-heavy
control-plane workload.

**What it takes.** Two modes, in increasing difficulty:

*Snapshot reads on followers* — a follower serves a read at its own applied
index, which is consistent but possibly stale. Cheap to implement and correct
for consumers that already tolerate staleness, which is most change-feed-driven
projections. The API needs an explicit opt-in so nobody gets stale reads by
accident.

*Linearizable follower reads* — the follower asks the leader for its current
commit index (a read-index barrier), waits until its own applied index reaches
it, then serves locally. One round trip to the leader per batch of reads rather
than per read, so a follower under load amortises it to nearly nothing. This
preserves linearizability exactly.

**Estimate.** One week for snapshot reads, two for read-index. The API design
— how a caller expresses its staleness tolerance without making the common case
verbose — is most of the work.

## 3. Leadership transfer

**The gap.** `StepDown()` makes a leader abdicate, but a new leader is then
elected the ordinary way, costing up to one full election timeout. During a
rolling deploy that is a visible write pause on every replica restart.

**What it takes.** Raft §3.10: the outgoing leader picks the most caught-up
follower, brings it fully up to date with a final `AppendEntries`, then sends
`TimeoutNow` to make it campaign immediately at the next term. The follower
wins instantly because it is already current.

**Estimate.** A few days. Small, self-contained, and it turns a deploy from
"brief pause" to "no pause" — the best ratio of operational value to complexity
on this list.

## 4. Dynamic membership changes

**The gap.** Cluster membership is static configuration. Adding or replacing a
replica requires restarting the cluster with a new peer list.

**What it takes.** Single-server membership changes (Raft §4.1, which is
simpler and safer than joint consensus): add or remove one server at a time via
a configuration entry in the log itself. Plus a learner/non-voting state, so a
new replica can catch up without counting toward quorum — without learners, a
3-node cluster briefly becomes a 4-node cluster needing 3 votes while the new
member is still replaying.

**Estimate.** Two weeks. Membership changes are where Raft implementations
historically get their safety bugs, so this needs proportionally more test
investment than its line count suggests.

## 5. Horizontal write scaling: sharding

**The gap.** One Raft group, one leader, one write pipeline. Write throughput
does not scale with cluster size.

**Why it is not done.** This is a deliberate trade, not an oversight. For
control-plane metadata — small working set, absolute consistency requirements,
thousands of writes per second rather than millions — a single group is the
right answer, and it buys a system whose correctness argument fits in one
document. Sharding would cost all of that.

**What it takes**, if the workload genuinely outgrew a single group:

- **Range-partitioned shards**, each its own Raft group, with a shard map that
  is itself replicated metadata.
- **A replacement for log-index timestamps.** This is the expensive part. Commit
  timestamps being log indexes is what makes the current consistency argument
  short; across shards, log indexes are meaningless. A hybrid logical clock (or
  a TrueTime-style bounded-uncertainty clock) would be required, which
  reintroduces exactly the clock dependency the current design avoids.
- **Two-phase commit** for cross-shard transactions, with a durable coordinator
  and recovery for coordinator failure.
- **Shard splits and merges** with live traffic.

**Estimate.** A quarter, minimum, with a substantially harder correctness
story. Worth doing when measurements say the single group is the bottleneck —
not before.

## 6. Durable change feed

**The gap.** The change feed is an in-memory ring buffer (8,192 entries). A
consumer offline longer than the buffer's coverage gets `ErrCursorExpired` and
must re-bootstrap with a full range scan.

**What it takes.** Serve replay from the Raft log rather than a side buffer.
The log already holds every change in order, and sequence numbers already *are*
log indexes, so the cursor semantics do not change at all. Consumer retention
would then be bounded by log retention rather than by a separate buffer — which
makes this partly dependent on item 1, since compaction defines how far back
the log goes.

**Estimate.** One week once snapshots exist, because the interesting case is a
consumer whose cursor falls below the compaction point, and that needs the
bootstrap-from-snapshot path to exist.

## 7. Transport hardening

**The gap.** Peer and client traffic are both plain HTTP with no transport
security. The admin API is protected by a shared bearer token.

**What it takes.** Mutual TLS on the peer plane with per-node certificates;
TLS plus a real authentication mechanism on the client plane; per-tenant
identity derived from the authenticated principal rather than from a
client-supplied `X-Keystone-Tenant` header.

That last point deserves emphasis: **the current tenancy model trusts a
header.** That is fine for a service behind a trusted gateway that sets the
header itself, and it is not fine for anything reachable by untrusted callers.
The key-space isolation is real and tested; the *identity* is asserted, not
verified.

**Estimate.** One to two weeks, mostly certificate lifecycle and rotation
rather than the TLS wiring.

## 8. Storage engine

**The gap.** The entire store is in memory. The log is durable, so nothing is
lost, but the dataset must fit in RAM and restart time is replay time.

**What it takes.** An LSM tree or B-tree on disk with a block cache, with the
MVCC version chains mapped onto it. The interfaces are already narrow enough
for this to be a contained change — the store is reached only through
`mvcc.Store` — but the semantics that matter (snapshot reads at a version, GC
at a watermark, ordered range scans, `MaxVersionInRange`) all need care in a
disk-backed implementation.

**Estimate.** A month or more. Worth it when the working set genuinely exceeds
memory; for control-plane metadata that point may never arrive.

## 9. Formal verification

**The gap.** Correctness rests on reasoning plus a chaos suite. Neither is a
proof.

**What it takes.** A TLA+ specification of the commit protocol — Raft plus
Keystone's OCC validation and the lease-read rule — model-checked for
strict serializability. The lease rule in particular is a timing argument, and
timing arguments are where informal reasoning is weakest.

**Estimate.** Two to three weeks for someone fluent in TLA+. High value per
line of output: model checking finds the interleavings that a randomized chaos
test needs luck to reach.

## Smaller items

- **Jepsen harness.** The in-process chaos suite is good, but Jepsen tests the
  real binaries over real networks with an independent checker. Different bug
  class.
- **Batch/pipelined proposals.** Group concurrent commits into one log entry to
  raise write throughput without touching the consistency model.
- **gRPC transport.** Lower per-RPC overhead than HTTP/JSON on the peer plane,
  and a generated client for non-Go callers.
- **Structured audit log.** Who changed what, when — a control-plane store is
  exactly where compliance wants this.
- **Backup and restore.** Consistent point-in-time export at a given log index,
  and a documented restore path.
- **Adaptive quota defaults.** Learn a tenant's normal envelope and alarm on
  departures from it rather than on a fixed threshold.

## What I would do first

With one engineer-month: **leadership transfer** (days, removes deploy pauses),
then **log compaction and snapshots** (weeks, removes the ceiling on cluster
lifetime).

Those two turn Keystone from a system that demonstrates the right ideas into
one that can actually be operated for years. Everything else on this list is an
optimization by comparison.
