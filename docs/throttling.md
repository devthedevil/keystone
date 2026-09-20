# Throttling and multi-tenancy

A shared control-plane store has one failure mode that matters more than any
other: one tenant's bad afternoon becoming everyone's outage. A backfill script
with no backoff, a retry loop with no jitter, a reconciliation job that scans
the whole key space every minute — none of these are malicious, and all of them
can saturate a single-leader system.

Keystone's answer has three layers, applied in this order:

1. **Fleet-level overload protection** — is the service healthy enough to take
   this class of work at all?
2. **Per-tenant quotas** — does this tenant have credit?
3. **Per-tenant concurrency limits** — is this tenant already using more than
   its share of in-flight slots?

The ordering is deliberate and is the single most important thing in this
document. Overload protection comes *first*, before tenant quotas, because a
tenant's credit was sized for a healthy service. When the service is degraded,
"you are within your quota" is no longer a reason to admit low-priority work.

## Cost: request units, not requests

Rate limiting by request count is rate limiting by a unit that does not exist.
A point read and a 1,000-key scan are both "one request", and treating them
alike means either strangling the cheap case or letting the expensive one
through unmetered.

Keystone meters in **request units** (RU):

```
ReadCost(keys, bytes)      = 1 + keys/4 + bytes/4096
WriteCost(mutations, bytes) = 2·mutations + bytes/1024
```

Worked examples:

| Operation | Cost |
|---|---|
| Point read of a 200-byte value | 1 RU |
| Scan of 100 keys totalling 40 KB | 1 + 25 + 10 = 36 RU |
| Single 500-byte write | 2 RU |
| Transaction with 10 mutations, 8 KB total | 20 + 8 = 28 RU |

Two properties are encoded in those formulas:

**Writes cost more than reads at the same size.** A write consumes the
replication log — the one serial, shared resource in the system. Reads served
under the leader lease consume only local CPU. The 2× base on writes reflects
that a write's cost is borne by every replica and every other tenant, not just
the writer.

**Cost is superlinear in nothing, but proportional to work.** A scan is charged
for the keys it *scanned*, not the keys it *returned*. A scan that walks
100,000 tombstones to return 3 live keys is expensive, and it should be: that
is exactly the access pattern that hurts.

### Reserve, then settle

The cost of a scan is not known until it completes. Charging up front requires
guessing; charging only afterwards lets an unbounded number of expensive
requests start concurrently.

Keystone reserves an estimate at admission and settles the difference on
completion:

```go
estimate := quota.ReadCost(limit, 0)     // assume the scan hits its limit
settle, ok := s.admit(w, r, estimate)
if !ok { return }
...
settle(quota.ReadCost(res.ScannedKeys, bytes))  // charge what it really cost
```

Settlement may push a bucket into deficit — that is intentional. A request that
turned out to cost far more than estimated has already consumed the resource;
pretending otherwise just means the next tenant pays for it. The deficit is
paid off by the refill rate before that tenant is admitted again.

The inverse case matters too: a scan over a sparse range that returns nothing
is refunded most of its reservation, so probing a mostly-empty key space is not
punished as if it were a full table walk.

## Priority classes

Clients set `X-Keystone-Class`:

| Class | Intended for | Shed floor |
|---|---|---|
| `critical` | Operations that restore service health | never shed |
| `high` | Interactive control-plane work: create, update, delete | 0.10 |
| `normal` | Ordinary reads (the default) | 0.30 |
| `bulk` | Scans, exports, backfills | 0.70 |

**Clients cannot self-select `critical`.** `ParseClass` maps unknown and
unrecognised header values to `normal`, and `critical` is assigned
server-side only. A priority class that any caller can claim is not a priority
class; within a week every client sets the highest one it can.

The shed floors produce graceful degradation rather than a cliff. As health
falls from 1.0:

- below 0.70 — bulk work starts being refused; backfills and exports stall
- below 0.30 — ordinary reads start being refused
- below 0.10 — even interactive writes are refused
- critical work is never refused, because shedding the operations that restore
  health is how a degraded service becomes an unrecoverable one

So a reconciliation job degrades well before an instance launch does, and an
instance launch degrades well before the operator's break-glass tooling does.

## The overload controller

Health is a single number in [0.05, 1.0] driven by observed request latency:

```go
if latency > target {
    health *= 0.9      // multiplicative decrease
} else {
    health += 0.01     // additive increase
}
```

AIMD, with the asymmetry in the familiar direction: **fast to back off, slow to
recover.** Overload needs a response within a few requests; recovery needs to
be cautious, because ramping back up quickly after a brief improvement is how a
system oscillates between saturated and idle instead of settling.

The floor of 0.05 exists so the controller can always recover. A health of
exactly zero, with additive increase, takes a very long time to climb out of.

The controller measures *latency*, not queue depth or CPU, because latency is
what the caller experiences and what the SLO is written against. It also
deliberately excludes the change-feed endpoint from its observations: `/v1/stream`
is a long-lived response by design, and feeding its duration into a latency
controller would make every healthy streaming consumer look like an overload
signal.

## Per-tenant quotas

Each tenant gets a token bucket:

| Field | Default | Meaning |
|---|---|---|
| `rate_per_sec` | 2000 | RU refilled per second — sustained throughput |
| `burst` | 4000 | Bucket capacity — how much idle credit can accumulate |
| `max_concurrent` | 64 | In-flight request limit |

Burst exists because control-plane traffic is bursty by nature: a deployment
touches many records at once and then goes quiet. A bucket with no burst
capacity punishes exactly the workload shape the system is built for.

`max_concurrent` is a separate control from the rate, and it catches a
different failure. A tenant issuing 64 concurrent 10,000-key scans is within
its RU budget on paper but is occupying every worker and every open snapshot.
Concurrency limits bound resource *occupancy*; rate limits bound resource
*consumption*. You need both.

When throttled, the response carries how long to wait:

```
HTTP 429
Retry-After: 0.240
{"error":{"code":"throttled","retry_after_ms":240,"retryable":true}}
```

The wait is computed from the actual deficit and refill rate
(`deficit / rate`), not a fixed constant, so a client that obeys it arrives
exactly when it can be served. Clients should still apply jitter — the Go
client does — because a fleet of clients given the identical delay will retry
in lockstep and re-create the overload they are backing off from.

## Configuration is replicated

Quota changes go through the Raft log, not through local state.

```
PUT /v1/admin/quota/{tenant}
        │
        ▼
  commit a transaction writing __system/quota/{tenant}
        │
        ▼
  every replica applies it on its apply loop → limiter.SetPolicy
```

This was not the original design, and the reason it changed is worth recording.
The first version applied the policy locally on whichever replica served the
admin call. That version passed its unit tests and failed the first realistic
benchmark: a tenant capped at 40 RU/s was throttled on one node and unlimited
on the other two, and the setting vanished entirely on restart.

Routing configuration through the log fixes all of it at once:

- **Cluster-wide.** Every replica converges on the same policy.
- **Durable.** A restart replays it from the log.
- **Survives failover.** The replica that takes over already has it.
- **Auditable.** A quota change appears in the change feed like any other write.

The `__system/` key space is unreachable from the client API: tenant names must
match `^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`, so no tenant can construct a key
under a prefix beginning with an underscore.

Verified end to end: set a 40 RU/s cap on one replica, kill all three nodes,
restart, and the cap is still enforced from the replayed log.

```
set tight quota for tenant 'noisy' via ONE replica
  noisy floods:  400 writes, 45 ops/s,  224 throttled
  quiet tenant:  400 writes, 3561 ops/s, 0 throttled
kill all nodes, restart from disk
  noisy floods:  200 writes, 48 ops/s,  109 throttled
```

## Tenant isolation

Throttling is only half of multi-tenancy. The other half is that tenants cannot
see each other at all.

Logical key `foo` for tenant `acme` is stored as `acme/foo`. Range scans have
their bounds rewritten to the tenant's prefix, with the exclusive end computed
by `prefixEnd`, so a scan cannot walk past the end of its own key space no
matter what bounds the client supplies. The change feed is filtered by the same
physical prefix, so a watcher sees only its tenant's mutations.

`TestTenantsCannotReadEachOther` covers both the point-read and range cases.

## Operating it

Inspect live admission state:

```bash
keystonectl quota
# TENANT  TOKENS  IN FLIGHT  ADMITTED  THROTTLED  SHED  UNITS CHARGED
```

Set a policy:

```bash
keystonectl quota -rate 5000 -burst 10000 -max-concurrent 128 acme
```

Relevant metrics:

| Metric | Watch for |
|---|---|
| `keystone_admission_health` | Sustained below 0.9 means the controller is fighting something |
| `keystone_tenant_throttled_total{tenant}` | A tenant newly appearing here is usually a deployed regression |
| `keystone_tenant_shed_total{tenant}` | Any sustained shedding is a real incident |
| `keystone_tenant_admitted_total{tenant}` | Which tenant is actually consuming the cluster |

The break-glass control is `SetHealth()`, which pins the controller. See
[runbook.md](runbook.md) for when that is the right move and when it is a way
to make an incident worse.
