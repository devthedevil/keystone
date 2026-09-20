# Testing

Keystone's tests are organised around a single idea: **a distributed system is
only as good as its behaviour when things are broken.** Tests that only cover
the happy path measure whether the code compiles in a useful order.

Every layer is tested at the level where its guarantees are actually
observable, and the whole system is tested under continuous fault injection.

## Running them

```bash
make test          # everything, with the race detector
make test-short    # skips the chaos suite (~40s)
make test-chaos    # just the chaos suite, verbose

go test ./... -race -count=1
go test ./test/ -race -run TestRegisterStaysLinearizable -v
```

All suites pass under `-race`. Current wall-clock on a single-CPU container:

```
internal/raft     3.0s
internal/mvcc     1.0s
internal/txn      2.4s
internal/quota    1.0s
internal/metrics  1.0s
internal/stream   1.1s
internal/cluster 19.1s
test (chaos)     28.9s
```

## The layers

### Raft — `internal/raft`

Consensus is tested against the safety properties from the paper, not against
its implementation details. The in-memory `Network` transport supports
`Partition`, `Isolate`, `SetLatency` and `SetDropRate`, which is what makes
these possible at all.

| Test | Property |
|---|---|
| `TestSingleNodeElectsItself` | A single-node group makes progress |
| `TestLeaderElectionAndReplication` | A cluster converges on one leader and replicates |
| `TestLeaderFailoverPreservesCommittedEntries` | Committed entries survive leader loss |
| `TestMinorityPartitionCannotCommit` | A minority cannot commit — no split brain |
| `TestDivergentFollowerLogIsRepaired` | A follower with conflicting entries is repaired |
| `TestLeaseReadRequiresQuorumContact` | An isolated leader loses its read lease |
| `TestFileStorageReplaysAfterRestart` | The WAL replays correctly after a crash |

### MVCC — `internal/mvcc`

Snapshot semantics, where the bugs live in version visibility.

Covered: reads at a snapshot see the newest version at or below it; tombstones
hide values from later snapshots but not earlier ones; scans are ordered and
respect limits; GC keeps the version a watermark-visible read still needs;
fully deleted keys are reclaimed entirely.

That fourth one is the subtle case. Collecting "everything below the watermark"
is wrong — a read *at* the watermark still needs the newest version at or below
it. Getting this wrong produces phantom not-founds that are nearly impossible
to reproduce.

### Transactions — `internal/txn`

Runs against a real three-node in-process Raft cluster, not a mock. Mocking
consensus here would test the mock.

| Test | Property |
|---|---|
| `TestTransactionReadWriteRoundTrip` | Basic commit and read-back |
| `TestWriteWriteConflictAbortsSecondTransaction` | Lost updates are impossible |
| `TestPhantomInsertAbortsScanningTransaction` | Range validation catches phantoms |
| `TestDuplicateProposalIsIdempotent` | A retried `txn_id` applies once |
| `TestConcurrentTransfersPreserveInvariant` | The invariant holds on **every replica** |
| `TestGCRespectsOpenSnapshot` | An open snapshot blocks collection |
| `TestReadOnlyTransactionSkipsReplication` | Read-only commits append nothing |

`TestConcurrentTransfersPreserveInvariant` is the load-bearing one: 8 accounts
seeded with 100 each, 6 workers doing 25 random transfers. The total must be
800 afterwards — checked on every replica, not just the leader. Checking only
the leader would miss exactly the bug this test exists to find, which is a
non-deterministic state machine.

### Change feed — `internal/stream`

Ordering, resumption, and the failure mode that matters.

Covered: changes arrive in log order; a cursor resumes exclusive of itself;
replay hands over to the live feed with no gap; prefix filters do not leak
across tenants; an expired cursor is rejected rather than silently starting
late; concurrent subscribers all see every change.

`TestSlowConsumerIsDroppedRatherThanStallingReplication` is the important one.
It asserts that `Publish` completes even when a subscriber never reads, and
that the dropped subscriber learns *why*. A broker that blocks here means one
slow consumer adds replication latency for every tenant.

### Admission control — `internal/quota`

Token buckets, reserve-then-settle accounting, priority shedding and the AIMD
controller, all driven by an injected fake clock. A rate limiter tested with
`time.Sleep` is a flaky test that eventually gets deleted.

### HTTP integration — `internal/cluster`

Three real replicas on loopback, talking over the real HTTP peer transport,
exercised through the real client router.

Covered: replication round-trip; cross-tenant isolation for both point reads
and ranges; conditional writes (create-if-absent, stale CAS, valid CAS);
idempotency-key deduplication; follower redirect with `not_leader`; atomic
batch transactions where a failed precondition leaves nothing behind; change
feed ordering over SSE; quota administration and throttling with `Retry-After`;
status and metrics content; and liveness without quorum.

That last one — `TestLivenessDoesNotDependOnQuorum` — encodes an operational
rule as a test. A liveness probe that fails during a partition gets replicas
killed by the orchestrator exactly when the cluster needs them alive. It is the
kind of thing that gets "fixed" by someone who has not been paged by it, so it
is pinned by a test with the reason in the comment.

### Chaos — `test/`

Continuous fault injection under live load. The monkey cycles through node
isolation, minority partitions, and 20–50% packet loss with latency, healing
between each.

**`TestRegisterStaysLinearizableUnderPartitionStorm`** — four writers and two
readers hammer one key for 12 seconds while the network is broken and repaired
repeatedly. The full history is checked afterwards. A representative run:

```
history: 12,313 operations (9,936 commits, 25 expected failures)
         across 41 injected faults
```

**`TestReplicasConvergeAfterHealing`** — a mixed read/write/delete workload
through a storm, then every replica must agree on applied index *and* on every
key's bytes:

```
9,431 commits across 31 injected faults;
all 5 replicas converged at applied index 9,493
```

**`TestMinorityCannotCommitDuringPartition`** — a leader pinned into a minority
must refuse the write, and that write must not appear after healing.

## The linearizability checker

General-purpose linearizability checkers search for a valid serialization and
are exponential in the worst case, which is why most projects run them nightly
on small histories.

Keystone does not need the search. Because commit timestamps **are** Raft log
indexes, the serialization order is directly observable in the responses —
every write returns the exact position it occupies in the total order. The
checker only has to verify three things:

1. **No two writes share a version.** Two writes at the same log index is split
   brain, definitionally.
2. **Real-time order agrees with commit order.** If write A returned before
   write B was submitted, A's version must be lower. A violation means a stale
   leader committed out of order.
3. **No read saw the future.** A read must not return a value whose write had
   not started when the read returned.

That is linear in the history size, which makes it cheap enough to run on every
CI build instead of nightly. Turning an exponential search into three linear
checks is a direct payoff from using log indexes as timestamps — the same
decision that removes clocks from the consistency argument.

## Failures must be documented failures

Under chaos, operations fail constantly and that is correct. But
`assertExpectedFailure` asserts that every failure is one the wire contract
describes — `not_leader`, `no_lease`, `conflict`, `unavailable`,
`DeadlineExceeded`, and so on.

An error of any other shape fails the test. "It returned an error" is fine;
"it returned an error nobody documented" is a bug, because a caller cannot
write correct retry logic against errors it was never told about.

## What the tests found

Two real defects were found by tests rather than by review, both by exercising
the system rather than its parts:

**Quota changes were not replicated.** A benchmark showed a tenant capped at
40 RU/s being throttled on one replica and unlimited on the other two, with the
setting lost entirely on restart. The admin handler applied the policy to local
state. Fixed by routing configuration through the log as a transaction against
a reserved `__system/` key space, which makes it cluster-wide, durable and
failover-safe. Verified by capping a tenant via one replica, restarting the
entire cluster, and confirming the cap still held.

**Redirects named a node, not an address.** A client bootstrapped from a single
endpoint could never follow a `not_leader` redirect, because the hint was an
opaque node ID it had no way to resolve. The same investigation found
`ErrReadOnlyReplica` unmapped in the error table, returning a 500 instead of a
redirect for admin operations against a follower. Both fixed; all redirects now
render through one helper so the header, the body field and the resolved
address cannot drift apart.

Neither was reachable from the unit tests. Both were obvious within seconds of
running the real binaries against each other, which is the argument for having
integration and chaos layers at all.

## What is not tested

Stated plainly, because an untested claim is a guess:

- **No formal verification.** No TLA+ model of the commit protocol. Chaos
  testing samples the interleaving space; it does not cover it. See
  [roadmap.md](roadmap.md).
- **No Jepsen.** The chaos suite is in-process. Jepsen would test the real
  binaries over real networks with an independent checker — a different and
  broader bug class.
- **No sustained soak.** The longest run is 30 seconds. Slow leaks (memory,
  goroutines, file descriptors) would not show up.
- **No large-scale performance testing.** Benchmarks are from a single-CPU
  container and indicate shape, not capacity.
- **No upgrade/downgrade matrix.** Rollback safety is argued from the encoding
  format, not demonstrated by running mixed-version clusters.
