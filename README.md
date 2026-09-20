# Keystone

A strongly consistent, highly available, transactional key-value store for
control-plane metadata. Written in Go, from scratch, with no third-party
dependencies.

Keystone is the kind of system a cloud provider's control plane sits on: a
tier-0 dependency where a lost or duplicated write becomes an orphaned resource
that costs money and needs a human, and where "works when the network is
healthy" is not a design.

```
┌──────────────────────────────────────────────────────────────┐
│  Strict serializability   commit timestamp = Raft log index  │
│  MVCC                     snapshot reads, no read locks      │
│  Leader leases            linearizable reads without a round │
│  Multi-tenancy            key-space isolation + request units│
│  Change feed              resumable CDC stream over SSE      │
│  Replicated config        quotas survive failover & restart  │
│  Zero dependencies        standard library only              │
└──────────────────────────────────────────────────────────────┘
```

## Quick start

```bash
make build

# three replicas on one host
make run-local

# in another shell
export KEYSTONE_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003
export KEYSTONE_ADMIN_TOKEN=secret

keystonectl status
keystonectl put services/compute/v1 '{"region":"us-ashburn-1"}'
keystonectl get services/compute/v1
keystonectl scan -prefix services/
keystonectl watch -prefix services/          # live change feed
keystonectl bench -n 3000 -c 16              # small load generator
```

Every replica accepts every request. A write sent to a follower is answered
with the leader's address and the client follows it automatically — including a
client that was configured with only one endpoint.

## Why it looks like this

### Commit timestamps are Raft log indexes

Most MVCC systems need a timestamp authority: a sequencer, a hybrid logical
clock, or something like TrueTime. Keystone already has a total order it trusts
— the Raft log — and reuses it. Applying log entry *i* writes every mutation at
version *i*.

Three things fall out of that one decision:

- **Clock skew cannot cause a stale read.** No wall clock is consulted anywhere
  in the commit path.
- **Timestamps are unique and monotonic** by construction, since two
  transactions cannot share a log index.
- **Linearizability checking becomes linear instead of exponential.** The
  serialization order is directly observable in the responses, so the checker
  verifies three properties rather than searching for a valid ordering. It runs
  on every build rather than nightly.

### Validation happens at apply time, not propose time

A transaction takes a snapshot, computes locally, and submits its read set,
range references, preconditions and mutations. The state machine validates when
it *applies* the command — at the log index that becomes the commit timestamp.

Validating on the leader before proposing would let two concurrent transactions
both validate against the same pre-state and both commit: a lost update.
Validating inside the state machine makes validation serial by construction,
with no locks, and deterministic, so every replica independently reaches the
same commit-or-abort decision.

### The API answers "may I retry?"

Every error carries a machine-readable code and an explicit `retryable` flag.
The distinction between `conflict` ("you lost a race, try again") and
`precondition_failed` ("the world is not what you thought") is enforced,
because collapsing them is how a create-if-absent loop becomes an infinite
retry storm during an incident.

The subtle case is `unavailable`, returned when leadership changes after a
proposal is accepted but before it commits. The outcome is genuinely unknown.
Retrying with the *same* `txn_id` is safe — the dedupe cache returns the
original result if it committed. Retrying with a fresh ID is how you get two of
something.

### Client and peer traffic are on separate listeners

A flood of client requests cannot starve the heartbeats that keep the leader
alive. Without that bulkhead, a load spike becomes an election storm, and an
election storm turns a latency problem into an availability problem.

## Alignment with the role

This project was built against the Oracle Cloud Infrastructure *Principal
Software Engineer, Core Infrastructure* job description (Seattle, Job ID
334805), which describes a tier-0 control-plane storage service. Each
responsibility maps to working, tested code.

| Responsibility from the JD | Where it lives | Evidence |
|---|---|---|
| Strongly consistent, highly available transactional KV store | `internal/raft`, `internal/txn` | Raft from scratch; 3- and 5-node clusters under test |
| **Strict serializability** | `internal/txn/engine.go` | commit TS = log index; [consistency.md](docs/consistency.md); linearizability checked under partition storm |
| **MVCC** | `internal/mvcc` | Skip list + per-key version chains; snapshot reads; tombstones |
| **Data-plane architecture** | `internal/server`, `internal/cluster` | Split client/peer listeners; middleware chain; tenant key-space routing |
| **Streaming** | `internal/stream`, `GET /v1/stream` | Resumable CDC over SSE; cursors are log indexes; slow consumers dropped, never blocking the apply loop |
| **Garbage collection** | `internal/mvcc/store.go`, `internal/txn` | GC is a *replicated command*, not a local sweep; watermark respects open snapshots |
| **Cost-based throttling** | `internal/quota` | Request-unit cost model; reserve-then-settle; AIMD overload controller; [throttling.md](docs/throttling.md) |
| **Read/write scalability** | `internal/raft/node.go` | Leader leases remove a consensus round from reads; read-only txns skip replication entirely |
| **Multi-tenancy** | `internal/server/handlers.go` | Key-space isolation; per-tenant buckets; `TestTenantsCannotReadEachOther` |
| **Performance isolation** | `internal/quota/limiter.go` | Priority classes with per-class shed floors; concurrency limits separate from rate limits |
| **Resiliency** | `test/chaos_test.go` | Continuous partition/isolation/packet-loss injection under live load |
| **Observability** | `internal/metrics`, `/metrics`, `/v1/status` | Prometheus exposition hand-rolled; alarms and dashboard in [runbook.md](docs/runbook.md) |
| **Deployment safety** | `internal/cluster/node.go`, `cmd/keystoned` | SIGTERM is a drain request; ordered shutdown with leadership handoff |
| **Automation / operability** | `cmd/keystonectl` | Status, quota, GC, step-down, watch, bench — through the same client library applications use |
| **Design quality / mentoring** | `docs/` | Six documents covering the correctness argument, the cost model, operations, testing, and an honest list of gaps |

## Measured behaviour

Single-CPU container, three replicas on loopback. These indicate shape, not
capacity.

```
read-mostly (90% reads, 16 concurrent)   ~8,000 ops/s   p50 1.4ms   p99 10.6ms
write-only (16 concurrent)               ~2,400 ops/s   p50 4.5ms   p99 19.3ms
```

Under continuous fault injection:

```
12,313 operations, 9,936 commits across 41 injected faults — linearizable
 9,431 commits across 31 injected faults — 5 replicas byte-identical after healing
```

Tenant isolation, verified across a full cluster restart:

```
tenant capped at 40 RU/s via ONE replica
  noisy tenant:  400 writes,   45 ops/s,  224 throttled
  quiet tenant:  400 writes, 3,561 ops/s,   0 throttled
kill all three nodes, restart from disk
  noisy tenant:  200 writes,   48 ops/s,  109 throttled   ← cap replayed from the log
```

## API

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/kv/{key}` | Point read at a linearizable snapshot |
| `PUT` | `/v1/kv/{key}` | `If-Match: <version>` = CAS; `If-None-Match: *` = create |
| `DELETE` | `/v1/kv/{key}` | `If-Match` supported |
| `GET` | `/v1/range` | `start`/`end` or `prefix`, `limit`; single snapshot |
| `POST` | `/v1/txn` | Atomic preconditions + mutations |
| `GET` | `/v1/stream` | SSE change feed; `from`, `prefix` |
| `GET` | `/v1/status` | Replication and store state |
| `GET` | `/metrics` | Prometheus text exposition |
| `GET` | `/health/live` | Quorum-independent by design |
| `GET` | `/health/ready` | Should this replica take traffic? |
| `POST` | `/v1/admin/gc` | Replicated collection pass |
| `GET`/`PUT` | `/v1/admin/quota[/{tenant}]` | Replicated tenant policy |
| `POST` | `/v1/admin/stepdown` | Abdicate leadership for a drain |

Headers: `X-Keystone-Tenant`, `X-Keystone-Class` (`high`/`normal`/`bulk`),
`Idempotency-Key`, `X-Keystone-Admin-Token`.

### Go client

```go
c, _ := client.New(client.Options{
    Endpoints: []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"},
    Tenant:    "compute",
})

// Optimistic read/modify/write, retried on conflict, idempotent under retry.
_, err := c.Update(ctx, []string{"quota/instances"}, func(cur map[string]api.KV) ([]api.Mutation, error) {
    n := parse(cur["quota/instances"].Value)
    if n >= limit {
        return nil, ErrQuotaExceeded
    }
    return []api.Mutation{{Key: "quota/instances", Value: encode(n + 1)}}, nil
})
```

The client follows leader redirects, classifies retryable errors from the
server's own flags, backs off with full jitter, and reuses one `txn_id` across
retries of the same logical operation.

## Layout

```
cmd/keystoned      server daemon
cmd/keystonectl    operator CLI
internal/raft      consensus: elections, log, leases, WAL, transports
internal/mvcc      skip list, version chains, GC
internal/txn       state machine (OCC validation, dedupe) + coordinator
internal/stream    change-feed broker
internal/quota     request units, token buckets, AIMD overload control
internal/metrics   Prometheus registry
internal/server    HTTP client plane, tenancy, error mapping
internal/cluster   assembly and lifecycle
pkg/api            wire contract
pkg/client         Go client
test/              chaos and linearizability harness
docs/              architecture, consistency, throttling, runbook, testing, roadmap
```

## Documentation

- [architecture.md](docs/architecture.md) — how the pieces fit and why
- [consistency.md](docs/consistency.md) — the strict-serializability argument,
  and exactly where its limits are
- [throttling.md](docs/throttling.md) — the request-unit model
- [runbook.md](docs/runbook.md) — alarms, dashboards, deploys, break-glass
- [testing.md](docs/testing.md) — what is tested, and what is not
- [roadmap.md](docs/roadmap.md) — known gaps with honest cost estimates

## Known limitations

The full list with cost estimates is in [roadmap.md](docs/roadmap.md). The ones
that matter most:

- **No log compaction or snapshots.** The log grows without bound and a new
  replica replays it from the beginning. This is the first thing I would fix.
- **Single Raft group.** Write throughput does not scale horizontally. This is
  a deliberate trade for control-plane metadata, not an oversight — sharding
  would require replacing log-index timestamps with a hybrid logical clock and
  adding two-phase commit, and it should wait until measurements demand it.
- **Reads go to the leader.** Follower reads with a read-index barrier would
  let read capacity scale with replica count.
- **Tenancy is asserted, not authenticated.** The tenant comes from a header.
  That is correct behind a trusted gateway and wrong anywhere else.
- **Store is in memory.** The log is durable so nothing is lost, but the
  dataset must fit in RAM.

## On the zero-dependency choice

Everything here — Raft, MVCC, the skip list, the metrics registry and its
Prometheus exposition format — is standard library only.

This was partly forced (the build environment had no module proxy) and partly
chosen. For a tier-0 dependency the trade is defensible: no transitive supply
chain, no version skew during an incident, nothing between a stack trace and
the code that produced it.

The cost is real too. A hand-rolled metrics registry is a thing that can have
bugs, and a mature Raft library has had far more eyes on it than this one. For
a production tier-0 service the honest recommendation is to keep the
architecture and swap in a battle-tested consensus implementation.

## License

MIT. See [LICENSE](LICENSE).
