# Runbook

Operational guide for Keystone. Written for someone paged at 3am who has not
read the architecture document.

## First 60 seconds

```bash
keystonectl status
```

```
ENDPOINT               NODE  STATE     TERM  LEADER  LEASE  COMMIT  APPLIED  KEYS  WATERMARK  HEALTH
http://10.0.0.1:8080   n1    follower  7     n2      false  91423   91423    8812  90400      1.00
http://10.0.0.2:8080   n2    leader    7     n2      true   91423   91423    8812  90400      1.00
http://10.0.0.3:8080   n3    follower  7     n2      false  91421   91421    8812  90400      1.00
```

Read it in this order:

1. **Is there exactly one leader?** Zero leaders means an ongoing election.
   Two leaders *in the same term* would be a correctness bug and is the only
   thing on this page that warrants waking a second person.
2. **Do the terms agree?** A node with a much higher term is flapping and
   repeatedly forcing elections.
3. **Does the leader hold a lease?** `LEASE false` on the leader means it
   cannot reach a majority. Reads are failing with `no_lease` right now.
4. **Is APPLIED close to COMMIT on every node?** A large gap means a replica is
   applying slowly or replaying a backlog.
5. **Is HEALTH near 1.00?** Below 0.7, bulk traffic is being shed.

Health endpoints, for automation:

- `GET /health/live` — is the process alive? **Deliberately does not require
  quorum.** A liveness probe that fails during a partition gets the replicas
  killed by the orchestrator at the exact moment the cluster needs them to stay
  up and re-form a quorum. This has taken down real systems; do not "fix" it.
- `GET /health/ready` — should this replica receive client traffic?

## Alarms

Tuned to the things that actually predict user impact.

| Alarm | Condition | Severity | First move |
|---|---|---|---|
| No leader | `sum(keystone_raft_is_leader) == 0` for 30s | page | Check network between replicas; check for a flapping node with a high term |
| Two leaders | `sum(keystone_raft_is_leader) > 1` for 10s | page | Capture `/v1/status` from every node *before* touching anything. If terms differ this is normal transient; if terms match, escalate |
| Lease loss | `keystone_raft_lease_valid == 0` on the leader for 30s | page | Leader cannot reach a majority; reads are failing |
| Apply lag | `keystone_raft_apply_lag > 1000` for 2m | page | A replica is falling behind; check its disk and CPU |
| Term churn | `increase(keystone_raft_term[5m]) > 5` | page | Election storm; see below |
| Error rate | `rate(keystone_errors_total{code!~"not_found\|conflict"}[5m])` above baseline | page | Conflicts and not-founds are normal; everything else is not |
| Health degraded | `keystone_admission_health < 0.7` for 5m | page | Service is shedding bulk work |
| Latency | p99 `keystone_request_duration_seconds` > 250ms for 5m | page | Check leader disk latency first |
| Open snapshots | `keystone_open_snapshots > 100` for 10m | ticket | A client is leaking snapshots; GC is blocked |
| GC stalled | `keystone_gc_watermark` unchanged for 30m while commits rise | ticket | Usually the same root cause as above |
| Store growth | `keystone_store_versions / keystone_store_keys > 50` | ticket | Version chains are not being collected |
| Tenant throttling | `rate(keystone_tenant_throttled_total[5m]) > 0` for a tenant that was quiet yesterday | ticket | Usually a deployed regression in that tenant's retry logic |

## Dashboard

Four rows, in the order you want them during an incident.

**Consensus** — `keystone_raft_term`, `keystone_raft_is_leader` by node,
`keystone_raft_lease_valid`, `keystone_raft_commit_index`,
`keystone_raft_apply_lag`.

**Traffic** — request rate by route and status, p50/p90/p99 from
`keystone_request_duration_seconds`, `keystone_errors_total` by code.

**Tenancy** — `keystone_tenant_admitted_total` and
`keystone_tenant_throttled_total` by tenant, `keystone_tenant_in_flight`,
`keystone_admission_health`.

**Storage** — `keystone_store_keys`, `keystone_store_versions`,
`keystone_store_bytes`, `keystone_gc_watermark`, `keystone_open_snapshots`,
`keystone_stream_watchers`.

## Common situations

### Election storm

**Symptoms:** term climbing steadily, leadership moving between nodes, writes
timing out intermittently.

**Cause, most of the time:** the peer plane cannot get heartbeats through
within the election timeout. Either the network is genuinely lossy, or the
leader's peer listener is starved.

Keystone runs client and peer traffic on separate listeners specifically to
prevent the second case, so if you are seeing a storm under client load,
check that the separation is actually in effect — a misconfigured deployment
that points both at the same port reintroduces the failure mode.

**Actions:**

1. Look at `keystone_raft_term` per node. One node with a much higher term is
   the instigator; it is timing out and campaigning. Take it out of rotation.
2. Check packet loss and latency between peer addresses.
3. If the network is simply slow, raise `-election-timeout` (and
   `-heartbeat` proportionally — keep heartbeat at roughly 1/5 of the
   election timeout). This trades failover speed for stability, which is the
   right trade when the alternative is no leader at all.

### Leader has no lease

**Symptoms:** reads failing with `no_lease`, leader present.

The leader has not heard from a majority recently. It is correctly refusing to
serve possibly-stale reads.

**Do not restart the leader.** That converts a read outage into a read *and*
write outage and starts an election on an already-unhealthy network. Find out
why a majority is unreachable: check the other replicas' liveness, check the
peer network, check for an in-progress deploy that took down two nodes at once.

### Write latency spike

Check, in order:

1. **Leader disk.** Every write is an fsync to the WAL. `fsync` latency is the
   floor on write latency. A degraded disk on the leader is the single most
   common cause.
2. **Apply lag.** If followers are behind, commits wait on them.
3. **Value sizes.** A tenant that started writing 900 KB values will slow
   replication for everyone. Check `keystone_store_bytes` growth rate.
4. **Admission health.** If it is below 1.0 the controller already believes
   things are slow.

`-fsync=false` trades durability for latency. It is a legitimate emergency
lever, and it means that a simultaneous power loss on a majority of replicas
can lose acknowledged writes. Do not leave it set.

### Garbage collection is not reclaiming

**Symptoms:** `keystone_store_versions` climbing, `keystone_gc_watermark` flat.

The watermark is `min(applied − retention, oldestOpenSnapshot − 1)`. If
`keystone_open_snapshots` is high and not falling, a client is holding
snapshots open — typically an abandoned paginated scan or a crashed consumer
that never closed its stream.

**Actions:**

1. Confirm with `keystone_open_snapshots`.
2. Identify the client; the usual culprit is a scan loop that exits early on
   error without releasing.
3. Force a pass once snapshots drain: `keystonectl gc`.
4. Reduce `-retention-indexes` if the history window is simply too wide for
   the write rate.

A client-side fix is the real fix. The server deliberately does not time out
snapshots, because silently collecting versions out from under a live reader
would turn a resource leak into a correctness bug.

### A tenant is overwhelming the cluster

```bash
keystonectl quota                                        # who is consuming what
keystonectl quota -rate 200 -burst 400 <tenant>          # clamp them
```

The change replicates through the log and takes effect on every replica within
one commit. It survives failover and restart.

To make the tenant's traffic shed first without clamping its rate, have them
set `X-Keystone-Class: bulk`. If they will not, the quota clamp is the blunt
instrument that works without their cooperation.

### Break-glass: pinning admission health

`Limiter.SetHealth()` overrides the AIMD controller.

**Pinning it low** (e.g. 0.2) sheds everything except `high` and `critical`.
Use this when the cluster is saturated and you need headroom to run recovery
operations.

**Pinning it high** (1.0) disables shedding. This is almost always the wrong
move during an incident: the controller is shedding *because* latency exceeded
the target, and removing the protection converts a partial degradation into a
full one. The narrow legitimate case is a false positive — the latency target
is misconfigured for this hardware and the service is actually fine.

Whichever direction, it is an override. Set a timer to remove it.

## Deployments

Keystone is designed so a rolling restart is boring. The order matters.

**Per replica:**

1. Remove it from the client load balancer. Wait for in-flight requests to
   drain (`keystone_tenant_in_flight` → 0 for that node).
2. If it is the leader, step down: `keystonectl -endpoints <that-node> stepdown`.
   Wait for a new leader in `keystonectl status`. This costs one election
   timeout; a true leadership transfer would make it near-instant and is on the
   roadmap.
3. `SIGTERM` the process. Keystone treats SIGTERM as a drain request and runs
   its shutdown sequence: stop client traffic → step down → wait for a
   successor → stop the coordinator → drain peer traffic → stop Raft → close
   storage. Allow `-drain-grace` (default 20s).
4. Start the new version. Wait for `/health/ready` and for its applied index to
   catch up to the leader's commit index.
5. Return it to the load balancer.

**Rules:**

- **One replica at a time.** A 3-node cluster tolerates one failure. Taking two
  down is a write outage, whatever the deploy tool says.
- **Wait for catch-up before proceeding.** A replica that is still replaying
  counts as down for availability purposes even though its process is up.
- **Bake.** Leave the first upgraded replica in service for at least one full
  traffic cycle before continuing. A mixed-version cluster is the cheapest
  place to discover an encoding incompatibility.

**Rollback:** the same procedure in reverse. The log format is
length-prefixed, CRC-checked JSON, and command encoding is stable, so an older
binary can read a newer binary's log as long as no new command types were
introduced. If a release adds a command type, it is not rollback-safe and must
say so in its release notes.

## Recovery

### Replacing a lost replica

Start a new process with the same node ID, the same `-peers`, and an empty
data directory. It will be caught up by the leader.

**Caveat, and it is a real one:** there is no log compaction or snapshotting
yet, so catch-up replays the log from the beginning. On a long-lived cluster
this can take a long time and generates substantial leader load. Do it during a
quiet period. This is the first item on the [roadmap](roadmap.md) for exactly
this reason.

### Total cluster loss

Restart all replicas pointing at their existing data directories. Committed
state is recovered from the WAL, including replicated configuration such as
tenant quotas. If a majority of data directories are intact, the cluster
re-forms with no data loss.

If a majority is genuinely lost, acknowledged writes are lost. There is no
clever recovery; restore from a change-feed consumer's downstream copy or from
a backup. This is the case that justifies having a durable change-feed
consumer.

### Corrupted log tail

Handled automatically. Each WAL record carries a CRC32 and a length prefix;
replay stops at the first record that fails either check and truncates there. A
record half-written when a machine lost power was by definition never
acknowledged, so discarding it is correct.

Corruption in the *middle* of a log is not recoverable and indicates failing
hardware. Replace the replica.

## Capacity

Rules of thumb from the single-CPU container the benchmarks were run on. Treat
them as shapes, not numbers.

```
read-mostly (90% reads, 16 concurrent):  ~8,000 ops/s  p50 1.4ms  p99 10.6ms
write-only (16 concurrent):              ~2,400 ops/s  p50 4.5ms  p99 19.3ms
```

- **Writes are bounded by the leader**, since there is one Raft group. Scaling
  writes means sharding, which is a roadmap item with real design cost.
- **Reads are bounded by the leader too**, because linearizable reads use the
  leader lease. Follower reads would fix this and are on the roadmap.
- **Memory is the store plus the version chains.** Watch
  `keystone_store_versions / keystone_store_keys`; a healthy ratio is single
  digits. Consistently above 50 means GC is not keeping up.
- **Disk is the log**, which currently grows without bound.

## Escalation

Wake someone else for:

- Two leaders in the **same term** (a correctness bug — capture
  `/v1/status` and the logs from every node *first*)
- A replica reporting state that contradicts another replica at the same
  applied index (divergence)
- Any suspected data loss of acknowledged writes
- Log corruption outside the tail

For everything else, the levers above are the levers.
