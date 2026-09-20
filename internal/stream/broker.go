// Package stream turns the replicated commit log into an ordered, resumable
// change feed.
//
// Control-plane consumers (workflow engines, cache invalidators, audit
// pipelines) need to react to metadata changes without polling. Because every
// change is tagged with the Raft index that produced it, a consumer can resume
// from exactly where it stopped, and ordering is identical on every replica.
package stream

import (
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	// ErrCursorExpired means the requested resume point has already been
	// evicted from the retention buffer. The consumer must re-bootstrap with a
	// full scan at the current snapshot and resume from there.
	ErrCursorExpired = errors.New("stream: resume cursor is older than the retention buffer")
	// ErrSlowConsumer is delivered to a subscription that could not keep up.
	ErrSlowConsumer = errors.New("stream: subscriber fell too far behind and was dropped")
	// ErrClosed is returned by a closed subscription.
	ErrClosed = errors.New("stream: subscription closed")
)

// ChangeKind discriminates mutation types.
type ChangeKind string

const (
	KindPut    ChangeKind = "put"
	KindDelete ChangeKind = "delete"
)

// Change is one mutation from one committed transaction.
type Change struct {
	// Seq is the Raft log index of the transaction that produced the change.
	// Changes from the same transaction share a Seq and are delivered together.
	Seq      uint64     `json:"seq"`
	Kind     ChangeKind `json:"kind"`
	Tenant   string     `json:"tenant"`
	Key      string     `json:"key"`
	Value    []byte     `json:"value,omitempty"`
	TxnID    string     `json:"txn_id,omitempty"`
	WallTime time.Time  `json:"wall_time"`
}

// Options configures the broker.
type Options struct {
	// RetentionEntries bounds the replay buffer.
	RetentionEntries int
	// SubscriberQueue bounds per-subscriber buffering before it is dropped.
	SubscriberQueue int
}

func (o *Options) withDefaults() {
	if o.RetentionEntries <= 0 {
		o.RetentionEntries = 8192
	}
	if o.SubscriberQueue <= 0 {
		o.SubscriberQueue = 1024
	}
}

// Broker fans committed changes out to subscribers.
type Broker struct {
	opts Options

	mu       sync.Mutex
	buf      []Change // retention ring, ascending by Seq
	subs     map[uint64]*Subscription
	nextSub  uint64
	lastSeq  uint64
	dropped  uint64
	fanouts  uint64
	watchers int
}

// NewBroker creates a broker.
func NewBroker(opts Options) *Broker {
	opts.withDefaults()
	return &Broker{opts: opts, subs: map[uint64]*Subscription{}}
}

// Publish records changes and fans them out. It is called from the Raft apply
// loop and must not block: a subscriber that cannot keep up is dropped rather
// than allowed to stall replication. Availability of the write path outranks
// the convenience of any single consumer.
func (b *Broker) Publish(changes ...Change) {
	if len(changes) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range changes {
		b.buf = append(b.buf, c)
		if c.Seq > b.lastSeq {
			b.lastSeq = c.Seq
		}
	}
	if over := len(b.buf) - b.opts.RetentionEntries; over > 0 {
		b.buf = append([]Change(nil), b.buf[over:]...)
	}

	for id, s := range b.subs {
		if !s.offer(changes) {
			s.fail(ErrSlowConsumer)
			delete(b.subs, id)
			b.dropped++
			b.watchers--
		}
	}
	b.fanouts += uint64(len(changes))
}

// Subscribe returns a subscription delivering every change with Seq > from
// whose key carries the given prefix. Passing from == 0 starts at the oldest
// retained change.
func (b *Broker) Subscribe(from uint64, prefix string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buf) > 0 && from > 0 && from < b.buf[0].Seq-1 {
		return nil, ErrCursorExpired
	}

	s := &Subscription{
		out:     make(chan Change, b.opts.SubscriberQueue),
		errCh:   make(chan error, 1),
		prefix:  prefix,
		maxPend: b.opts.SubscriberQueue,
		broker:  b,
	}
	b.nextSub++
	s.id = b.nextSub

	// Replay the retained tail before going live, so the consumer sees a
	// continuous sequence across the handoff.
	var backlog []Change
	for _, c := range b.buf {
		if c.Seq > from {
			backlog = append(backlog, c)
		}
	}
	if !s.offer(backlog) {
		return nil, ErrCursorExpired
	}
	b.subs[s.id] = s
	b.watchers++
	return s, nil
}

func (b *Broker) unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[id]; ok {
		delete(b.subs, id)
		b.watchers--
	}
}

// Stats reports broker health for the metrics endpoint.
type Stats struct {
	Watchers      int    `json:"watchers"`
	Retained      int    `json:"retained"`
	LastSeq       uint64 `json:"last_seq"`
	DroppedSubs   uint64 `json:"dropped_subscribers"`
	ChangesFanned uint64 `json:"changes_fanned_out"`
}

// Stats returns a snapshot of broker counters.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		Watchers:      b.watchers,
		Retained:      len(b.buf),
		LastSeq:       b.lastSeq,
		DroppedSubs:   b.dropped,
		ChangesFanned: b.fanouts,
	}
}

// Subscription is a live change feed.
type Subscription struct {
	id      uint64
	out     chan Change
	errCh   chan error
	prefix  string
	maxPend int
	broker  *Broker
	closeOn sync.Once
}

// offer enqueues changes without blocking. It reports false if the subscriber
// is too far behind.
func (s *Subscription) offer(changes []Change) bool {
	for _, c := range changes {
		if s.prefix != "" && !strings.HasPrefix(c.Key, s.prefix) {
			continue
		}
		select {
		case s.out <- c:
		default:
			return false
		}
	}
	return true
}

func (s *Subscription) fail(err error) {
	s.closeOn.Do(func() {
		select {
		case s.errCh <- err:
		default:
		}
		close(s.out)
	})
}

// Changes is the delivery channel. It is closed when the subscription ends.
func (s *Subscription) Changes() <-chan Change { return s.out }

// Err returns the termination cause once Changes is closed.
func (s *Subscription) Err() error {
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}

// Close releases the subscription.
func (s *Subscription) Close() {
	s.broker.unsubscribe(s.id)
	s.fail(ErrClosed)
}
