# Consistency

Keystone provides **strict serializability**: every transaction appears to
execute atomically at a single point in time, that point lies between the
transaction's submission and its acknowledgement, and the resulting order is
consistent with real time. If transaction A returns before transaction B is
submitted, every client sees A's effects before B's.

This is the strongest guarantee a distributed store can offer, and it is the
only one that makes a control plane simple to build on. Anything weaker leaks
into every caller: an instance-launch workflow that reads its own write and
does not see it has to be written defensively, and defensive workflows are
where duplicate resources and orphaned capacity come from.

This document explains why the guarantee holds, and exactly where its limits
are.

## The argument in one paragraph

Keystone is a single Raft group. Raft gives a single, totally ordered log that
a majority has durably accepted. Every replica applies that log in order
through a deterministic state machine, so every replica reaches the same state.
A transaction's commit timestamp **is** its Raft log index, so the serial order
is the log order — there is no second ordering mechanism that could disagree
with it. Writes are only acknowledged after their entry is committed, and reads
are served only by a leader holding a valid lease. Therefore the serialization
point of every operation lies inside its real-time interval, which is exactly
strict serializability.

The rest of this document is the detail behind each of those clauses.

## Commit timestamps are log indexes

Most MVCC systems need a timestamp authority: a sequencer, a hybrid logical
clock, or something like TrueTime. Keystone needs none, because it already has
a total order it trusts — the Raft log — and it reuses it.

```
log index 41  →  commit timestamp 41
log index 42  →  commit timestamp 42
```

When the state machine applies entry *i*, it writes every mutation in that
transaction at version *i*. A read at snapshot *T* sees, for each key, the
newest version ≤ *T*.

Three things follow immediately:

- **Timestamps are unique and monotonic.** Two transactions cannot share a
  commit timestamp, because they cannot share a log index.
- **Clock skew is not a correctness concern.** No decision in the commit path
  reads a wall clock. A machine with a badly wrong clock produces wrong log
  lines and wrong metrics timestamps; it cannot produce a wrong commit order.
  Contrast this with a system that assigns timestamps from local clocks, where
  skew turns directly into stale reads.
- **The state machine is deterministic.** Replay of the same log from the same
  starting state yields byte-identical state on every replica. `TestReplicasConvergeAfterHealing`
  asserts this after a partition storm: same applied index *and* identical
  contents on all five replicas.

Wall-clock time appears in exactly one place in the data path — the
`wall_time` field on a change-feed event — and it is explicitly advisory. It is
there for human debugging. Nothing in the system reads it back.

## Reads: leader leases, not quorum reads

A naive linearizable read costs a round of consensus. Keystone instead uses
**leader leases**, the standard optimization from §6.4 of the Raft dissertation.

The rule: a leader may serve a read from local state only while it holds a
lease, and it holds a lease only while it has heard a heartbeat acknowledgement
from a majority within the last `LeaseFactor × ElectionTimeout` (0.8 by
default).

The safety argument is a timing one. A new leader cannot be elected without a
majority of votes, and no follower will vote before its own election timeout
elapses without hearing from the current leader. So if the old leader heard
from a majority at time *t*, no new leader can be elected before
*t + ElectionTimeout*. Serving reads only until *t + 0.8 × ElectionTimeout*
leaves a 20% margin against clock-rate error between machines.

This is the one place where Keystone depends on time, and the dependency is
narrow and worth stating precisely:

> Lease reads assume bounded **clock drift rate**, not bounded clock offset.
> Two machines may disagree about what time it is by an arbitrary amount; what
> must hold is that their clocks advance at roughly the same rate — within 20%
> over one election timeout. This is a far weaker assumption than synchronized
> clocks, and it is satisfied by any machine whose oscillator is not broken.

If a leader loses contact with its majority, the lease expires and reads start
failing with `no_lease` rather than returning possibly stale data. That is the
correct trade: a control plane would rather be told "ask again in 50ms" than be
told something false. `TestLeaseReadRequiresQuorumContact` covers this.

For callers who want to opt out of the coordination cost entirely, see
*Follower reads* under Limitations.

## Writes: optimistic concurrency validated at apply time

Keystone uses optimistic concurrency control. A transaction:

1. takes a snapshot (a lease read, yielding a read timestamp `T`),
2. reads and computes locally, buffering its writes,
3. submits a command carrying its read set, its range references, its explicit
   preconditions, and its mutations.

Validation happens **when the command is applied**, not when it is proposed.
This detail is the whole design, and it is worth being explicit about why.

If validation happened on the leader before proposing, two concurrent
transactions could both validate against the same pre-state, both get proposed,
and both commit — a lost update. By validating inside the state machine, at the
log index that will become the commit timestamp, validation sees the effects of
every transaction ordered before it. Validation is therefore serial, by
construction, with no locks and no latches.

It also means validation is deterministic: every replica independently reaches
the same commit-or-abort decision for every entry. A replica that decided
differently would diverge, and the convergence test would catch it.

Three kinds of conflict are detected:

| Kind | Cause | Client action |
|---|---|---|
| `read_write` | A key in the read set changed after `T` | Retry with a fresh snapshot |
| `phantom` | A key appeared in, or vanished from, a scanned range after `T` | Retry with a fresh snapshot |
| `condition` | An explicit precondition (`exists`, `absent`, `version`) did not hold | Do **not** retry blindly — the caller's assumption was wrong |

The distinction in the last row matters operationally. A `conflict` means "you
lost a race, try again"; a `precondition_failed` means "the world is not what
you thought". Collapsing them into one retryable error is how a create-if-absent
loop turns into an infinite retry storm during an incident.

### Phantoms

Serializability requires more than checking the keys you read. A transaction
that scans `tenants/acme/instances/` and counts 9 results, then writes a 10th
after checking a quota of 10, must abort if someone else inserted into that
range concurrently — even though no key it read was modified.

Keystone handles this by validating **ranges**, not just keys. A transaction
submits the ranges it scanned; at apply time the engine asks the store for the
maximum version written anywhere in each range (`MaxVersionInRange`) and aborts
if anything in the range changed after `T`. The skip list makes this an ordered
walk rather than a full scan.

This is conservative — an insert far from the keys actually examined still
aborts the transaction — but it is correct, and a control plane's contention is
almost always on hot keys rather than wide ranges.

## Idempotency: exactly-once effects under retry

Distributed systems cannot offer exactly-once *delivery*, but they can offer
exactly-once *effects*, and for a control plane that is the property that
matters. A create-instance call that runs twice because a response was lost
must not produce two instances.

Every mutating request carries a `txn_id`. The state machine keeps a bounded
cache mapping `txn_id` to the outcome of its first application. On applying a
command whose ID is already present, it returns the cached result and applies
nothing.

Two details make this actually work:

- **The dedupe check happens before validation.** A duplicate must not be
  re-validated, because re-validation would abort it — its read versions are
  stale by now — and the caller would wrongly conclude its write never
  happened. This is the single subtlest ordering constraint in the engine.
- **Eviction is in log order, not by wall-clock TTL.** A time-based cache
  would evict at different moments on different replicas, making the state
  machine non-deterministic. The cache holds the last N entries by log
  position, so every replica forgets the same entry at the same index.

The cache bounds the retry window: with 65,536 entries and a sustained 5,000
commits/sec, a client has roughly 13 seconds to retry safely. Clients should
not retry an ambiguous write after longer than that without re-reading state.
`TestDuplicateProposalIsIdempotent` covers the mechanism.

## What failure modes look like

The contract is explicit about retry safety because guessing is what produces
duplicate resources.

| Code | Meaning | Retryable? |
|---|---|---|
| `not_leader` | This replica cannot serve writes. `leader_addr` names who can. | Yes, after redirect |
| `no_lease` | Leader cannot currently prove it leads. | Yes — do **not** fail over on this alone |
| `conflict` | Lost an optimistic race. | Yes, with a fresh snapshot |
| `precondition_failed` | A stated condition did not hold. | No — re-read and re-decide |
| `throttled` | Over tenant quota. | Yes, after `retry_after_ms` |
| `overloaded` | Shed to protect the service. | Yes, with backoff |
| `unavailable` | Outcome genuinely unknown (leadership changed mid-commit). | Yes — **with the same `txn_id`** |

The `unavailable` case is the one people get wrong. When leadership changes
after a proposal is accepted but before it commits, the client cannot know
whether the write happened. Retrying with the same `txn_id` is safe and
correct: if the original committed, the dedupe cache returns its result; if it
did not, the retry commits normally. Retrying with a *fresh* ID in that
situation is how you get two of something.

## Limitations, stated plainly

These are real and deliberate. A design document that only lists strengths is
marketing.

**Single Raft group.** Write throughput is bounded by one leader. This is the
correct choice for control-plane metadata — where the working set is small,
consistency requirements are absolute, and the write rate is measured in
thousands per second, not millions — but it does not scale horizontally for
writes. Sharding is discussed in [roadmap.md](roadmap.md); it would require
replacing log-index timestamps with a hybrid logical clock and adding two-phase
commit for cross-shard transactions, which is a substantially harder system to
verify. Doing it before it is needed would be the wrong trade.

**No log compaction or snapshots.** The log grows without bound, and a new
replica must replay it from the beginning. This is the most significant gap
and the first item on the roadmap. For a metadata workload with a bounded key
count it is survivable for a long time; it is not acceptable indefinitely.

**Reads scale by adding nothing.** All linearizable reads go to the leader.
Follower reads with a read-index barrier would let read capacity scale with
replica count while preserving linearizability, at the cost of one round trip
to the leader per batch. Snapshot reads on followers would be cheaper still and
are appropriate for the change-feed consumers that dominate read volume.

**Lease reads assume bounded clock drift rate.** Stated in full above. A
machine whose clock runs 20%+ fast relative to its peers could, in principle,
serve a stale read during a partition. Setting `LeaseFactor` to 0 disables
lease reads and forces a consensus round for every read — the safe fallback if
you cannot trust your hardware.

**Range validation is conservative.** Any change within a validated range
aborts, even one that could not have affected the transaction's decision.

## How these claims are tested

Every guarantee above has a test that tries to break it:

- `TestRegisterStaysLinearizableUnderPartitionStorm` — four writers and two
  readers hammer a single key for 12 seconds while a fault injector cycles
  through node isolation, minority partitions, and 20–50% packet loss. The
  recorded history (~12,000 operations, ~10,000 commits across 41 injected
  faults) is checked for real-time order violations, duplicate commit
  timestamps, and reads that observed the future.
- `TestMinorityCannotCommitDuringPartition` — a leader pinned into a minority
  must refuse the write, and that write must not appear after healing.
- `TestReplicasConvergeAfterHealing` — after a storm, all five replicas must
  agree on applied index *and* on every key's bytes.
- `TestConcurrentTransfersPreserveInvariant` — a bank-transfer workload
  (8 accounts, 6 workers, 25 transfers each) where the total must be
  unchanged on **every replica**, not just the leader.
- `TestMinorityPartitionCannotCommit`, `TestLeaderFailoverPreservesCommittedEntries`,
  `TestDivergentFollowerLogIsRepaired` — Raft-level safety properties.

The linearizability checker deserves a note. General-purpose checkers search
for a valid serialization and are exponential in the worst case. Keystone
doesn't need the search: because commit timestamps *are* log indexes, the
serialization order is directly observable in the responses. The checker only
has to verify that real-time order agrees with commit order, that no two writes
claim the same version, and that no read saw a value written after that read
returned. That is linear, and it makes the check cheap enough to run on every
CI build rather than nightly.
