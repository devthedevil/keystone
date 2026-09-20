// Package metrics is a small Prometheus-compatible instrumentation library.
//
// It exists because Keystone deliberately has no third-party dependencies: the
// exposition format is a stable, documented text protocol, and reimplementing
// the handful of metric types a storage service needs is cheaper than owning a
// dependency in a tier-0 binary.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Labels are metric dimensions.
type Labels map[string]string

func (l Labels) key() string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(l[k])
	}
	return b.String()
}

func (l Labels) render() string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, l[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Counter is a monotonically increasing value.
type Counter struct {
	v      uint64
	labels Labels
}

// Inc adds one.
func (c *Counter) Inc() { atomic.AddUint64(&c.v, 1) }

// Add adds n.
func (c *Counter) Add(n uint64) { atomic.AddUint64(&c.v, n) }

// Value reads the counter.
func (c *Counter) Value() uint64 { return atomic.LoadUint64(&c.v) }

// Gauge is a value that can go up and down.
type Gauge struct {
	bits   uint64
	labels Labels
}

// Set stores v.
func (g *Gauge) Set(v float64) { atomic.StoreUint64(&g.bits, math.Float64bits(v)) }

// Add adds delta.
func (g *Gauge) Add(delta float64) {
	for {
		old := atomic.LoadUint64(&g.bits)
		nv := math.Float64frombits(old) + delta
		if atomic.CompareAndSwapUint64(&g.bits, old, math.Float64bits(nv)) {
			return
		}
	}
}

// Value reads the gauge.
func (g *Gauge) Value() float64 { return math.Float64frombits(atomic.LoadUint64(&g.bits)) }

// Histogram is a cumulative distribution with fixed buckets.
type Histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []uint64
	sum    float64
	total  uint64
	labels Labels
}

// DefaultLatencyBuckets covers sub-millisecond replication through
// multi-second pathological cases.
var DefaultLatencyBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Observe records a value, usually seconds.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.total++
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
		}
	}
}

// Quantile returns an interpolation-free estimate from bucket boundaries. It is
// good enough for alarm thresholds and deliberately conservative: it reports
// the upper bound of the bucket the quantile falls into.
func (h *Histogram) Quantile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	want := uint64(math.Ceil(q * float64(h.total)))
	for i, c := range h.counts {
		if c >= want {
			return h.bounds[i]
		}
	}
	return math.Inf(1)
}

type family struct {
	name       string
	help       string
	kind       string
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	gaugeFns   map[string]func() float64
	fnLabels   map[string]Labels
}

// Registry holds metric families and renders the exposition format.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
	order    []string
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: map[string]*family{}}
}

func (r *Registry) familyFor(name, help, kind string) *family {
	f, ok := r.families[name]
	if !ok {
		f = &family{
			name: name, help: help, kind: kind,
			counters:   map[string]*Counter{},
			gauges:     map[string]*Gauge{},
			histograms: map[string]*Histogram{},
			gaugeFns:   map[string]func() float64{},
			fnLabels:   map[string]Labels{},
		}
		r.families[name] = f
		r.order = append(r.order, name)
	}
	return f
}

// Counter returns the counter for a name and label set, creating it if needed.
func (r *Registry) Counter(name, help string, labels Labels) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.familyFor(name, help, "counter")
	k := labels.key()
	c, ok := f.counters[k]
	if !ok {
		c = &Counter{labels: labels}
		f.counters[k] = c
	}
	return c
}

// Gauge returns the gauge for a name and label set.
func (r *Registry) Gauge(name, help string, labels Labels) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.familyFor(name, help, "gauge")
	k := labels.key()
	g, ok := f.gauges[k]
	if !ok {
		g = &Gauge{labels: labels}
		f.gauges[k] = g
	}
	return g
}

// GaugeFunc registers a gauge sampled at scrape time. It is the right shape for
// values that are owned elsewhere, such as the Raft commit index.
func (r *Registry) GaugeFunc(name, help string, labels Labels, fn func() float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.familyFor(name, help, "gauge")
	k := labels.key()
	f.gaugeFns[k] = fn
	f.fnLabels[k] = labels
}

// Histogram returns the histogram for a name and label set.
func (r *Registry) Histogram(name, help string, buckets []float64, labels Labels) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.familyFor(name, help, "histogram")
	k := labels.key()
	h, ok := f.histograms[k]
	if !ok {
		if len(buckets) == 0 {
			buckets = DefaultLatencyBuckets
		}
		h = &Histogram{bounds: buckets, counts: make([]uint64, len(buckets)), labels: labels}
		f.histograms[k] = h
	}
	return h
}

// WriteTo renders the registry in Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.RLock()
	names := append([]string(nil), r.order...)
	fams := make([]*family, 0, len(names))
	for _, n := range names {
		fams = append(fams, r.families[n])
	}
	r.mu.RUnlock()

	var written int64
	emit := func(format string, args ...any) error {
		n, err := fmt.Fprintf(w, format, args...)
		written += int64(n)
		return err
	}

	for _, f := range fams {
		if err := emit("# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind); err != nil {
			return written, err
		}
		for _, c := range sortedCounters(f) {
			if err := emit("%s%s %d\n", f.name, c.labels.render(), c.Value()); err != nil {
				return written, err
			}
		}
		for _, g := range sortedGauges(f) {
			if err := emit("%s%s %s\n", f.name, g.labels.render(), formatFloat(g.Value())); err != nil {
				return written, err
			}
		}
		for k, fn := range f.gaugeFns {
			if err := emit("%s%s %s\n", f.name, f.fnLabels[k].render(), formatFloat(fn())); err != nil {
				return written, err
			}
		}
		for _, h := range sortedHistograms(f) {
			h.mu.Lock()
			for i, b := range h.bounds {
				lbl := mergeLabels(h.labels, "le", formatFloat(b))
				if err := emit("%s_bucket%s %d\n", f.name, lbl.render(), h.counts[i]); err != nil {
					h.mu.Unlock()
					return written, err
				}
			}
			inf := mergeLabels(h.labels, "le", "+Inf")
			err := emit("%s_bucket%s %d\n%s_sum%s %s\n%s_count%s %d\n",
				f.name, inf.render(), h.total,
				f.name, h.labels.render(), formatFloat(h.sum),
				f.name, h.labels.render(), h.total)
			h.mu.Unlock()
			if err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func mergeLabels(l Labels, k, v string) Labels {
	out := make(Labels, len(l)+1)
	for lk, lv := range l {
		out[lk] = lv
	}
	out[k] = v
	return out
}

func sortedCounters(f *family) []*Counter {
	keys := make([]string, 0, len(f.counters))
	for k := range f.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*Counter, 0, len(keys))
	for _, k := range keys {
		out = append(out, f.counters[k])
	}
	return out
}

func sortedGauges(f *family) []*Gauge {
	keys := make([]string, 0, len(f.gauges))
	for k := range f.gauges {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*Gauge, 0, len(keys))
	for _, k := range keys {
		out = append(out, f.gauges[k])
	}
	return out
}

func sortedHistograms(f *family) []*Histogram {
	keys := make([]string, 0, len(f.histograms))
	for k := range f.histograms {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*Histogram, 0, len(keys))
	for _, k := range keys {
		out = append(out, f.histograms[k])
	}
	return out
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
