package quota

import (
	"sync"
	"testing"
	"time"
)

// fakeClock makes token-bucket behaviour deterministic instead of flaky.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestLimiter(p TenantPolicy) (*Limiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1700000000, 0)}
	return New(Options{Default: p, Now: clk.now}), clk
}

func TestBucketExhaustsAndRefills(t *testing.T) {
	l, clk := newTestLimiter(TenantPolicy{RatePerSec: 100, Burst: 100, MaxConcurrent: 1000})

	spent := 0.0
	for spent < 100 {
		d := l.Admit("t1", ClassNormal, Cost{Units: 10})
		if !d.Allowed {
			break
		}
		l.Charge("t1", Cost{Units: 10}, Cost{Units: 10})
		spent += 10
	}
	if spent != 100 {
		t.Fatalf("burst should allow exactly 100 units, allowed %v", spent)
	}

	d := l.Admit("t1", ClassNormal, Cost{Units: 10})
	if d.Allowed {
		t.Fatal("request admitted past an exhausted bucket")
	}
	if d.RetryAfter <= 0 {
		t.Fatal("a throttled response must tell the client when to retry")
	}

	clk.advance(500 * time.Millisecond) // 50 units
	if d := l.Admit("t1", ClassNormal, Cost{Units: 50}); !d.Allowed {
		t.Fatalf("bucket did not refill at the configured rate: %+v", d)
	}
	if d := l.Admit("t1", ClassNormal, Cost{Units: 1}); d.Allowed {
		t.Fatal("bucket refilled beyond the elapsed time")
	}
}

func TestTenantsAreIsolated(t *testing.T) {
	l, _ := newTestLimiter(TenantPolicy{RatePerSec: 10, Burst: 10, MaxConcurrent: 100})

	// A noisy tenant drains its own bucket.
	for i := 0; i < 20; i++ {
		l.Admit("noisy", ClassNormal, Cost{Units: 1})
	}
	if d := l.Admit("noisy", ClassNormal, Cost{Units: 1}); d.Allowed {
		t.Fatal("noisy tenant was not throttled")
	}
	// The quiet tenant is unaffected.
	if d := l.Admit("quiet", ClassNormal, Cost{Units: 5}); !d.Allowed {
		t.Fatalf("a noisy neighbour starved an unrelated tenant: %+v", d)
	}
}

func TestConcurrencyCapLimitsTailLatencyDamage(t *testing.T) {
	l, _ := newTestLimiter(TenantPolicy{RatePerSec: 100000, Burst: 100000, MaxConcurrent: 3})
	for i := 0; i < 3; i++ {
		if d := l.Admit("t", ClassNormal, Cost{Units: 1}); !d.Allowed {
			t.Fatalf("request %d rejected below the concurrency cap", i)
		}
	}
	if d := l.Admit("t", ClassNormal, Cost{Units: 1}); d.Allowed {
		t.Fatal("concurrency cap was not enforced")
	}
	l.Charge("t", Cost{Units: 1}, Cost{Units: 1}) // one finishes
	if d := l.Admit("t", ClassNormal, Cost{Units: 1}); !d.Allowed {
		t.Fatal("slot was not released on charge")
	}
}

func TestActualCostIsSettledAfterTheFact(t *testing.T) {
	l, _ := newTestLimiter(TenantPolicy{RatePerSec: 1, Burst: 100, MaxConcurrent: 10})

	// A scan that was estimated cheap but turned out to be enormous.
	est := Cost{Units: 1}
	if d := l.Admit("t", ClassNormal, est); !d.Allowed {
		t.Fatal("first request should be admitted")
	}
	l.Charge("t", est, Cost{Units: 99})

	if d := l.Admit("t", ClassNormal, Cost{Units: 5}); d.Allowed {
		t.Fatal("tenant was not charged for the work it actually caused")
	}
}

func TestOverloadShedsBulkBeforeCritical(t *testing.T) {
	l, _ := newTestLimiter(TenantPolicy{RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 1000})

	// Degrade the service: sustained latency above target.
	for i := 0; i < 50; i++ {
		l.Observe(500 * time.Millisecond)
	}
	if h := l.Health(); h >= 0.7 {
		t.Fatalf("health did not degrade under sustained latency: %.2f", h)
	}

	if d := l.Admit("t", ClassBulk, Cost{Units: 1}); d.Allowed {
		t.Fatal("bulk traffic must be shed first under overload")
	}
	if d := l.Admit("t", ClassCritical, Cost{Units: 1}); !d.Allowed {
		t.Fatal("critical traffic must never be shed: the service must stay recoverable")
	}

	// Recovery is gradual.
	for i := 0; i < 300; i++ {
		l.Observe(time.Millisecond)
	}
	if h := l.Health(); h < 0.9 {
		t.Fatalf("health did not recover: %.2f", h)
	}
	if d := l.Admit("t", ClassBulk, Cost{Units: 1}); !d.Allowed {
		t.Fatal("bulk traffic was not restored after recovery")
	}
}

func TestCostModelPricesWorkNotRequests(t *testing.T) {
	cheap := ReadCost(1, 100)
	wide := ReadCost(100000, 10<<20)
	if wide.Units <= cheap.Units*100 {
		t.Fatalf("a scan over 100k keys must cost far more than a point read: %v vs %v", wide.Units, cheap.Units)
	}
	if WriteCost(1, 1024).Units <= ReadCost(1, 1024).Units {
		t.Fatal("a replicated write should cost more than a local read")
	}
}

func TestClassParsingIgnoresClientEscalation(t *testing.T) {
	if ParseClass("critical") == ClassCritical {
		t.Fatal("clients must not be able to self-select the critical class")
	}
	if ParseClass("bulk") != ClassBulk || ParseClass("high") != ClassHigh || ParseClass("") != ClassNormal {
		t.Fatal("class parsing is wrong")
	}
}
