package metrics

import (
	"strings"
	"testing"
)

func TestExpositionFormat(t *testing.T) {
	r := NewRegistry()
	r.Counter("keystone_commits_total", "Committed transactions.", Labels{"tenant": "a"}).Add(3)
	r.Counter("keystone_commits_total", "Committed transactions.", Labels{"tenant": "b"}).Inc()
	r.Gauge("keystone_raft_term", "Current Raft term.", nil).Set(7)
	r.GaugeFunc("keystone_applied_index", "Applied log index.", nil, func() float64 { return 42 })
	h := r.Histogram("keystone_commit_seconds", "Commit latency.", []float64{0.01, 0.1, 1}, nil)
	h.Observe(0.005)
	h.Observe(0.5)

	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := sb.String()

	for _, want := range []string{
		"# TYPE keystone_commits_total counter",
		`keystone_commits_total{tenant="a"} 3`,
		`keystone_commits_total{tenant="b"} 1`,
		"keystone_raft_term 7",
		"keystone_applied_index 42",
		`keystone_commit_seconds_bucket{le="0.01"} 1`,
		`keystone_commit_seconds_bucket{le="1"} 2`,
		`keystone_commit_seconds_bucket{le="+Inf"} 2`,
		"keystone_commit_seconds_count 2",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("exposition output missing %q\n---\n%s", want, out)
		}
	}
}

func TestCounterIdentityIsStablePerLabelSet(t *testing.T) {
	r := NewRegistry()
	a := r.Counter("x_total", "h", Labels{"k": "v"})
	b := r.Counter("x_total", "h", Labels{"k": "v"})
	a.Inc()
	b.Inc()
	if got := a.Value(); got != 2 {
		t.Fatalf("same label set should resolve to one counter, got %d", got)
	}
}

func TestQuantileIsConservative(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("lat", "h", []float64{0.01, 0.1, 1}, nil)
	for i := 0; i < 99; i++ {
		h.Observe(0.005)
	}
	h.Observe(0.9)
	if q := h.Quantile(0.5); q != 0.01 {
		t.Fatalf("p50 should land in the first bucket, got %v", q)
	}
	if q := h.Quantile(0.999); q != 1 {
		t.Fatalf("p99.9 should report the outlier bucket, got %v", q)
	}
}
