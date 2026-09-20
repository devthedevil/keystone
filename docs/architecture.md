# Architecture

Keystone is a strongly consistent, transactional key-value store for
control-plane metadata. This document describes how it is put together and why
each piece is shaped the way it is.

If you want the correctness argument, read [consistency.md](consistency.md)
instead. This is the structural tour.

## The shape of the problem

A control-plane metadata store is not a general-purpose database. Its workload
has a specific shape, and the design follows from it:

- **Small values, many keys.** Resource records, not blobs. Values are capped
  at 1 MiB and the cap is enforced at the edge, because a large value in the
  replication log hurts every tenant's latency, not just the writer's.
- **Read-heavy, but writes must be exactly right.** A stale read is usually
  survivable for a fraction of a second. A lost or duplicated write creates an
  orphaned resource that costs money and needs a human.
- **Failure is the normal case.** At fleet scale, some host is always being
  patched, some link is always flapping. "Works when the network is healthy" is
  not a design.
- **Multi-tenant.** One team's batch reconciliation job must not be able to
  turn into another team's outage.

Everything below is a consequence of one of those four facts.

## Component map

```
                        ┌──────────────────────────────────────┐
   client traffic  ───► │  internal/server   HTTP client plane │
                        │  tenancy · admission · error mapping │
                        └───────────────┬──────────────────────┘
                                        │
                        ┌───────────────▼──────────────────────┐
                        │  internal/txn      Coordinator       │
                        │  snapshots · buffered txns · retry   │
                        └───────────────┬──────────────────────┘
                                        │ Propose(command)
                        ┌───────────────▼──────────────────────┐
   peer traffic ──────► │  internal/raft     replication       │
                        │  elections · log · leases · WAL      │
                        └───────────────┬──────────────────────┘
                                        │ Apply(index, entry)
                        ┌───────────────▼──────────────────────┐
                        │  internal/txn      Engine (the FSM)  │
                        │  OCC validation · dedupe · GC        │
                        └────┬─────────────────────────┬───────┘
                             │                         │
              ┌──────────────▼──────┐   ┌──────────────▼───────┐
              │  internal/mvcc      │   │  internal/stream     │
              │  skip list · MVCC   │   │  change feed broker  │
              └─────────────────────┘   └──────────────────────┘

        cross-cutting: internal/quota (admission), internal/metrics
```

Data flows down on the write path and the *only* way to change state is to go
through the log. Nothing writes to the store directly — not the admin API, not
garbage collection, not quota configuration. That single rule is what makes
every replica converge.

## internal/raft — replication

A from-scratch Raft implementation: leader election, log replication, and the
commit rules including the §5.4.2 restriction that a leader may only commit
entries from its own term by counting replicas for an entry *of that term*.
A no-op entry is appended on election so a new leader can commit its
predecessor's entries promptly.

Things worth calling out:

**Leader leases.** A leader that has heard from a majority within
`0.8 × ElectionTimeout` may serve reads locally. See consistency.md for the
safety argument. The factor is 0.8 rather than 1.0 to leave margin against
clock *rate* differences between machines.

**Fast log backtracking.** On an `AppendEntries` rejection the follower returns
a conflict index, so the leader skips a whole term in one round trip rather
than decrementing `nextIndex` one entry at a time. Without this, repairing a
follower that missed 10,000 entries takes 10,000 round trips.

**Voluntary step-down.** `StepDown()` lets a leader abdicate on request. This is
what makes a rolling restart boring: drain client traffic, hand off leadership,
wait for a successor, then stop. A true §3.10 leadership *transfer* (which
picks the most caught-up follower and prompts it to campaign immediately) is on
the roadmap; the current implementation costs one election timeout.

**Two transports.** `transport_mem.go` is an in-memory network with
`Partition`, `Isolate`, `SetLatency` and `SetDropRate` — this is what the chaos
suite drives. `transport_http.go` is the real one. Having both behind one
interface is what makes the fault-injection tests possible at all; retrofitting
that seam later is much harder than designing for it.

**Storage.** `FileStorage` is an append-only WAL: each record is
length-prefixed with a CRC32. Replay tolerates a torn tail — a record that was
half-written when the machine lost power is discarded, which is exactly the
correct behaviour, since it was by definition never acknowledged. Hard state
(current term, voted-for) is written to a temp file and renamed, because that
is the only cheap way to get an atomic update on a POSIX filesystem.

## internal/mvcc — the store

A skip list of keys, each holding an ascending chain of versions.

The skip list is there for **ordered range scans**, which a hash map cannot do
and which the transaction layer needs for phantom detection. Probabilistic
balancing (p=0.25, max level 12) gets O(log n) search without the rebalancing
complexity of a B-tree, which matters more for a component that must be
obviously correct than one that must be maximally fast.

Version chains are searched by binary search for "newest version ≤ readTS".
Deletes are tombstones, so a snapshot taken before a delete still sees the
value.

`MaxVersionInRange(start, end)` is the primitive behind phantom detection: it
walks the ordered structure and returns the highest version written anywhere in
the range, which lets the engine detect an insert into a range a transaction
scanned.

## internal/txn — transactions

Two halves that are easy to confuse, so: the **Engine** is the replicated state
machine (it runs on every replica, deterministically, on the apply loop); the
**Coordinator** is the client-facing API (it runs on the leader and talks to
Raft).

### Engine

Applies commands in log order. For each:

1. Dedupe check — if this `txn_id` was already applied, return the cached
   result and apply nothing.
2. Validate — read-set versions, range references, explicit preconditions.
3. Mutate at `commitTS = log index`.
4. Publish changes to the broker.

Commands are normalized before encoding (sorted read sets, sorted mutations) so
the same logical transaction produces byte-identical log entries. Validation
touches only replicated state; no wall clocks, no random numbers, no map
iteration order.

Garbage collection is a **command**, not a background sweep. The leader
proposes `OpGC` with a watermark; every replica performs the identical
collection at the identical index. A local background sweep would let replicas
forget different things at different times, which is divergence by another
name.

### Coordinator

Offers snapshots, point reads, scans, buffered transactions with
read-your-writes, and `Execute` — a retry loop with exponential backoff and
full jitter that replays a transaction body on conflict.

Two details carry weight:

**Snapshot pinning.** An open snapshot is refcounted, and the GC watermark is
`min(applied − retention, oldestOpenSnapshot − 1)`. A long-running scan
therefore cannot have the versions it is reading collected out from under it.
The other side of this coin is that an abandoned snapshot blocks reclamation
indefinitely, which is why `open_snapshots` is an exported metric with an alarm
on it in the runbook.

**Read-only transactions skip replication.** A transaction with no mutations
commits without appending to the log. Under a read-heavy control-plane
workload this removes the large majority of would-be log traffic.

## internal/stream — change feed

An in-memory ring buffer (8,192 entries by default) with prefix-filtered
subscriptions, published to from the apply loop.

The load-bearing decision: **`Publish` never blocks.** A subscriber that cannot
keep up is dropped with `ErrSlowConsumer` rather than being allowed to stall
the apply loop. Blocking would mean one slow change-feed consumer adds
replication latency for every tenant on the cluster — a single consumer's
problem becoming everyone's outage.

Consumers resume from the last sequence they processed. A cursor older than the
retention window fails with `ErrCursorExpired` and a message telling the
consumer exactly how to recover: re-bootstrap with a range scan and resume from
that scan's `read_ts`. Failing loudly is deliberate; silently starting from the
oldest retained change would leave a hole the consumer cannot detect.

Sequence numbers are log indexes, so a change-feed cursor and a read timestamp
are the same kind of thing. That is what makes "scan, then stream from the
scan's timestamp" a correct bootstrap with no gap and no overlap.

## internal/quota — admission control

Rate limiting in *request units* rather than requests, because a request is not
a unit of work:

```
ReadCost(keys, bytes)  = 1 + keys/4 + bytes/4096
WriteCost(muts, bytes) = 2·muts + bytes/1024
```

Writes cost more because they consume the log, which is the shared, serial
resource. A 1,000-key scan costs far more than a point read, because it is.

Three mechanisms stack:

**Per-tenant token buckets** with reserve-then-settle accounting. A request
reserves an estimate up front and settles the actual cost afterwards, allowing
a bucket to go into deficit. This is what stops a scan whose cost is unknown
until it completes from either being rejected pessimistically or being charged
nothing.

**Priority classes** — `critical`, `high`, `normal`, `bulk`. Clients cannot
self-select `critical`; that class is reserved for internal operations. Under
load, classes shed at different health thresholds (bulk at 0.7, normal at 0.3,
high at 0.1, critical never). A batch reconciliation job degrades before an
instance launch does.

**AIMD overload controller.** Health multiplicatively decreases (×0.9) when
observed latency exceeds the target and additively increases (+0.01) when it
does not, floored at 0.05. Multiplicative-decrease/additive-increase because
overload needs a fast response and recovery needs a cautious one; the reverse
oscillates.

Quota configuration is itself replicated through the log (see below), so a
policy change applies on every replica and survives failover and restart.

## internal/server — the client plane

Middleware chain: recover → request ID → telemetry → tenancy → per-handler
admission.

**Tenancy** maps a logical key to `tenant + "/" + key`. Isolation is by key
space, so a scan cannot walk out of its tenant: the range end is computed with
`prefixEnd`. Tenant names are regex-validated and must start with an
alphanumeric character.

That last rule is what makes the reserved `__system/` prefix unreachable. System
keys hold replicated configuration — currently tenant quotas — written through
the log like any other transaction. An earlier version applied quota changes
locally on whichever replica served the admin call; that made a tenant's limit
depend on which host you happened to reach, and lose its setting on failover.
Routing configuration through the log fixed it, and the fix is verifiable: set
a quota via one replica, restart the whole cluster, and the limit is still
enforced.

**Error mapping** has one job: answer "may I retry this exact request?" Every
branch of `writeTxnError` sets `retryable` and, where relevant,
`retry_after_ms` and `leader_addr`. Redirects all render through a single
helper so the header, the body field, and the resolved address cannot drift
apart.

**Two listeners.** Client traffic and peer replication traffic are served on
separate ports with separate `http.Server`s. A flood of client requests
therefore cannot starve the heartbeats that keep the leader alive. Without this
bulkhead, a load spike becomes an election storm, and an election storm turns a
latency problem into an availability problem.

**Conditional writes** map onto HTTP's own concurrency headers: `If-Match: 42`
is compare-and-swap, `If-None-Match: *` is create-if-absent. Ordinary HTTP
tooling can do optimistic concurrency without learning a new vocabulary.

## internal/cluster — assembly and lifecycle

Wires the pieces together and owns startup and shutdown ordering.

Shutdown drains in the order that minimises client impact: stop accepting new
client work → step down as leader → wait for a successor → stop the coordinator
→ drain peer traffic → stop Raft → close storage. Getting this order wrong is
how a "graceful" restart produces a 30-second write outage.

## Dependencies

Keystone has none. Everything above — Raft, MVCC, the skip list, the metrics
registry and its Prometheus text exposition, the HTTP layer — is standard
library only.

This was partly forced (the build environment had no module proxy) and partly
chosen. For a tier-0 dependency the trade is defensible on its own terms: no
transitive supply chain, no version skew during an incident, nothing between a
stack trace and the code that produced it. The cost is real too — a hand-rolled
metrics registry is a thing that can have bugs, and a mature Raft library has
had far more eyes on it than this one. For a production tier-0 service, the
honest recommendation is to keep the architecture and swap in a
battle-tested Raft implementation.

## Where to read next

- [consistency.md](consistency.md) — the strict-serializability argument and
  its limits
- [throttling.md](throttling.md) — the request-unit model in detail
- [runbook.md](runbook.md) — alarms, dashboards, deploys, break-glass
- [testing.md](testing.md) — what is tested and how
- [roadmap.md](roadmap.md) — what is missing and what it would take
