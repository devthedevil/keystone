// Package quota implements multi-tenant admission control.
//
// A tier-0 multi-tenant store fails in a characteristic way: one tenant starts
// a badly written scan loop, the leader's CPU saturates, and every other
// tenant's control plane stalls. Rate limiting by request count does not stop
// this, because a request that scans a million keys and a request that reads
// one key are not the same request.
//
// Keystone therefore meters work in Request Units (RU), charges tenants for the
// work they actually caused, and sheds load by priority class when the service
// is overloaded regardless of whether any single tenant is over quota.
package quota

import (
	"fmt"
	"sync"
	"time"
)

// Class is the priority of a request. Classes exist so that the traffic which
// keeps the fleet recoverable is the last thing to be shed.
type Class int

const (
	// ClassCritical is reserved for operations that restore service health:
	// leader handoff, quota resets, control-plane recovery writes. It bypasses
	// tenant quotas and is never shed.
	ClassCritical Class = iota
	// ClassHigh is interactive control-plane work: create, update, delete.
	ClassHigh
	// ClassNormal is ordinary reads.
	ClassNormal
	// ClassBulk is large scans, exports and backfills. It is shed first.
	ClassBulk
)

func (c Class) String() string {
	switch c {
	case ClassCritical:
		return "critical"
	case ClassHigh:
		return "high"
	case ClassNormal:
		return "normal"
	case ClassBulk:
		return "bulk"
	default:
		return "unknown"
	}
}

// ParseClass maps a request header value to a class, defaulting to normal.
// Callers cannot self-select ClassCritical; that is assigned server-side.
func ParseClass(s string) Class {
	switch s {
	case "high":
		return ClassHigh
	case "bulk":
		return ClassBulk
	default:
		return ClassNormal
	}
}

// Cost is the metered work of one operation.
type Cost struct {
	Units float64
}

// ReadCost prices a read. The base unit covers request handling; scanned keys
// and transferred bytes are charged separately so that a wide scan cannot hide
// behind a single request count.
func ReadCost(keysScanned, bytes int) Cost {
	return Cost{Units: 1 + float64(keysScanned)/4.0 + float64(bytes)/4096.0}
}

// WriteCost prices a write. Writes cost more than reads because every write is
// replicated to a quorum and fsynced, and because each version competes for
// garbage collection capacity later.
func WriteCost(mutations, bytes int) Cost {
	return Cost{Units: 2*float64(mutations) + float64(bytes)/1024.0}
}

// TenantPolicy is the quota assigned to one tenant.
type TenantPolicy struct {
	// RatePerSec is sustained throughput in request units per second.
	RatePerSec float64
	// Burst is the maximum credit a tenant may accumulate while idle.
	Burst float64
	// MaxConcurrent bounds simultaneous in-flight requests, which limits the
	// damage a single tenant can do to tail latency even while under its rate
	// quota.
	MaxConcurrent int
}

// DefaultPolicy is applied to tenants without an explicit policy.
var DefaultPolicy = TenantPolicy{RatePerSec: 2000, Burst: 4000, MaxConcurrent: 64}

// Decision is the outcome of an admission check.
type Decision struct {
	Allowed    bool
	Reason     string
	RetryAfter time.Duration
	Class      Class
	Tenant     string
}

// TenantStats is per-tenant telemetry, exported to metrics and to the
// per-tenant dashboard described in docs/runbook.md.
type TenantStats struct {
	Tenant      string  `json:"tenant"`
	Tokens      float64 `json:"tokens"`
	InFlight    int     `json:"in_flight"`
	Admitted    uint64  `json:"admitted"`
	Throttled   uint64  `json:"throttled"`
	Shed        uint64  `json:"shed"`
	UnitsCharge float64 `json:"units_charged"`
}

type bucket struct {
	policy   TenantPolicy
	tokens   float64
	last     time.Time
	inFlight int

	admitted  uint64
	throttled uint64
	shed      uint64
	charged   float64
}

// Limiter admits or rejects work.
type Limiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	policies map[string]TenantPolicy
	def      TenantPolicy
	now      func() time.Time

	// health is the AIMD admission multiplier in (0, 1]. It falls when the
	// service is slow and recovers gradually when it is healthy.
	health        float64
	latencyTarget time.Duration

	shedTotal uint64
}

// Options configures the limiter.
type Options struct {
	Default TenantPolicy
	// LatencyTarget is the service-level objective the overload controller
	// defends. Sustained latency above it causes bulk traffic to be shed.
	LatencyTarget time.Duration
	// Now is injectable for deterministic tests.
	Now func() time.Time
}

// New creates a limiter.
func New(opts Options) *Limiter {
	if opts.Default.RatePerSec == 0 {
		opts.Default = DefaultPolicy
	}
	if opts.LatencyTarget <= 0 {
		opts.LatencyTarget = 50 * time.Millisecond
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Limiter{
		buckets:       map[string]*bucket{},
		policies:      map[string]TenantPolicy{},
		def:           opts.Default,
		now:           opts.Now,
		health:        1.0,
		latencyTarget: opts.LatencyTarget,
	}
}

// SetPolicy assigns a tenant-specific quota.
func (l *Limiter) SetPolicy(tenant string, p TenantPolicy) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.policies[tenant] = p
	if b, ok := l.buckets[tenant]; ok {
		b.policy = p
		if b.tokens > p.Burst {
			b.tokens = p.Burst
		}
	}
}

// ResetPolicy returns a tenant to the default policy. The existing bucket is
// kept rather than recreated, so removing an override cannot hand a tenant a
// free full burst.
func (l *Limiter) ResetPolicy(tenant string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.policies, tenant)
	if b, ok := l.buckets[tenant]; ok {
		b.policy = l.def
		if b.tokens > l.def.Burst {
			b.tokens = l.def.Burst
		}
	}
}

func (l *Limiter) bucketFor(tenant string) *bucket {
	b, ok := l.buckets[tenant]
	if ok {
		return b
	}
	p, ok := l.policies[tenant]
	if !ok {
		p = l.def
	}
	b = &bucket{policy: p, tokens: p.Burst, last: l.now()}
	l.buckets[tenant] = b
	return b
}

func (l *Limiter) refill(b *bucket) {
	now := l.now()
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.last = now
	b.tokens += elapsed.Seconds() * b.policy.RatePerSec
	if b.tokens > b.policy.Burst {
		b.tokens = b.policy.Burst
	}
}

// Admit reserves capacity for a request estimated to cost `estimate` units.
//
// The estimate is deliberately cheap to compute; the true cost is settled by
// Charge once the work is done, and a tenant that consistently underestimates
// simply runs a deficit and is throttled sooner. This "reserve then settle"
// pattern avoids the alternative of executing the request twice to price it.
func (l *Limiter) Admit(tenant string, class Class, estimate Cost) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Critical traffic is always admitted: shedding it is how a degraded
	// service becomes an unrecoverable one.
	if class == ClassCritical {
		b := l.bucketFor(tenant)
		b.inFlight++
		b.admitted++
		return Decision{Allowed: true, Class: class, Tenant: tenant}
	}

	// Fleet-level overload protection, applied before tenant quotas: when the
	// service is unhealthy, low-priority work is refused even if the tenant has
	// credit, because the credit was sized for a healthy service.
	if floor := classFloor(class); l.health < floor {
		l.shedTotal++
		b := l.bucketFor(tenant)
		b.shed++
		return Decision{
			Allowed:    false,
			Reason:     fmt.Sprintf("overloaded: health %.2f below %s floor %.2f", l.health, class, floor),
			RetryAfter: 250 * time.Millisecond,
			Class:      class,
			Tenant:     tenant,
		}
	}

	b := l.bucketFor(tenant)
	l.refill(b)

	if b.policy.MaxConcurrent > 0 && b.inFlight >= b.policy.MaxConcurrent {
		b.throttled++
		return Decision{
			Allowed:    false,
			Reason:     fmt.Sprintf("tenant concurrency limit %d reached", b.policy.MaxConcurrent),
			RetryAfter: 20 * time.Millisecond,
			Class:      class,
			Tenant:     tenant,
		}
	}

	if b.tokens < estimate.Units {
		deficit := estimate.Units - b.tokens
		wait := time.Duration(deficit / b.policy.RatePerSec * float64(time.Second))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		b.throttled++
		return Decision{
			Allowed:    false,
			Reason:     "tenant request-unit quota exhausted",
			RetryAfter: wait,
			Class:      class,
			Tenant:     tenant,
		}
	}

	b.tokens -= estimate.Units
	b.inFlight++
	b.admitted++
	b.charged += estimate.Units
	return Decision{Allowed: true, Class: class, Tenant: tenant}
}

// Charge settles the difference between the estimated and actual cost and
// releases the concurrency slot. It must be called for every admitted request.
func (l *Limiter) Charge(tenant string, estimate, actual Cost) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bucketFor(tenant)
	if b.inFlight > 0 {
		b.inFlight--
	}
	delta := actual.Units - estimate.Units
	// A tenant may go into deficit; the bucket simply takes longer to refill,
	// which is the throttle.
	b.tokens -= delta
	b.charged += delta
	if b.tokens > b.policy.Burst {
		b.tokens = b.policy.Burst
	}
}

// Observe feeds a completed request's latency to the overload controller.
//
// The controller is AIMD: it backs off quickly when the service degrades and
// recovers slowly, which is the behaviour that avoids oscillation under load.
func (l *Limiter) Observe(latency time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if latency > l.latencyTarget {
		l.health *= 0.9
		if l.health < 0.05 {
			l.health = 0.05
		}
		return
	}
	l.health += 0.01
	if l.health > 1 {
		l.health = 1
	}
}

// Health returns the current admission multiplier.
func (l *Limiter) Health() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.health
}

// SetHealth overrides the controller, used by tests and by the operator
// "shed bulk traffic now" break-glass control documented in the runbook.
func (l *Limiter) SetHealth(h float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if h < 0.05 {
		h = 0.05
	}
	if h > 1 {
		h = 1
	}
	l.health = h
}

// classFloor is the health level below which a class stops being admitted.
func classFloor(c Class) float64 {
	switch c {
	case ClassBulk:
		return 0.7
	case ClassNormal:
		return 0.3
	case ClassHigh:
		return 0.1
	default:
		return 0
	}
}

// Stats returns per-tenant telemetry.
func (l *Limiter) Stats() []TenantStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]TenantStats, 0, len(l.buckets))
	for tenant, b := range l.buckets {
		l.refill(b)
		out = append(out, TenantStats{
			Tenant:      tenant,
			Tokens:      b.tokens,
			InFlight:    b.inFlight,
			Admitted:    b.admitted,
			Throttled:   b.throttled,
			Shed:        b.shed,
			UnitsCharge: b.charged,
		})
	}
	return out
}

// ShedTotal is the fleet-wide count of load-shed requests.
func (l *Limiter) ShedTotal() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shedTotal
}
