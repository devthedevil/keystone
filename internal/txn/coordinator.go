package txn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand"
	"sort"
	"sync"
	"time"

	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/raft"
)

// Replicator is the slice of the Raft node the coordinator depends on. Keeping
// it an interface lets the transaction layer be tested without a cluster.
type Replicator interface {
	Propose(ctx context.Context, data []byte) (any, error)
	ReadLease(ctx context.Context) error
	IsLeader() bool
	Leader() raft.NodeID
	LastApplied() uint64
}

// Errors surfaced to callers.
var (
	// ErrRetriesExhausted means the transaction kept losing races. It is a
	// contention signal, not a correctness failure.
	ErrRetriesExhausted = errors.New("txn: retries exhausted under contention")
	// ErrReadOnlyReplica means this replica cannot serve the request.
	ErrReadOnlyReplica = errors.New("txn: replica is not the leader")
)

// ConflictError carries the abort reason.
type ConflictError struct{ Conflict Conflict }

func (e *ConflictError) Error() string {
	return fmt.Sprintf("txn: aborted (%s) on key %q: expected version %d, observed %d",
		e.Conflict.Kind, e.Conflict.Key, e.Conflict.Expected, e.Conflict.Observed)
}

// Options configures the coordinator.
type Options struct {
	// RetentionIndexes is how much version history to keep behind the applied
	// index, expressed in log entries. It bounds how stale a follower read or a
	// long scan may be before it is collected out from under the reader.
	RetentionIndexes uint64
	// GCInterval is how often the leader proposes a collection pass.
	GCInterval time.Duration
	// MaxRetries bounds automatic retries on conflict.
	MaxRetries int
	// BaseBackoff is the first retry delay; it doubles with full jitter.
	BaseBackoff time.Duration
	Logger      *slog.Logger
}

func (o *Options) withDefaults() {
	if o.RetentionIndexes == 0 {
		o.RetentionIndexes = 10000
	}
	if o.GCInterval <= 0 {
		o.GCInterval = 30 * time.Second
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 5
	}
	if o.BaseBackoff <= 0 {
		o.BaseBackoff = 2 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Coordinator is the client-facing transaction API on a single replica.
type Coordinator struct {
	rep    Replicator
	engine *Engine
	opts   Options
	log    *slog.Logger

	snaps *snapshotTracker

	mu            sync.Mutex
	lastWatermark uint64
	rnd           *mathrand.Rand

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewCoordinator wires a coordinator to a replica and its state machine.
func NewCoordinator(rep Replicator, engine *Engine, opts Options) *Coordinator {
	opts.withDefaults()
	return &Coordinator{
		rep:    rep,
		engine: engine,
		opts:   opts,
		log:    opts.Logger.With("component", "txn"),
		snaps:  newSnapshotTracker(),
		rnd:    mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
		stopCh: make(chan struct{}),
	}
}

// Start launches the background garbage collector.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.gcLoop()
}

// Stop halts background work.
func (c *Coordinator) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

type snapshotTracker struct {
	mu   sync.Mutex
	open map[uint64]int
}

func newSnapshotTracker() *snapshotTracker { return &snapshotTracker{open: map[uint64]int{}} }

func (t *snapshotTracker) acquire(ts uint64) {
	t.mu.Lock()
	t.open[ts]++
	t.mu.Unlock()
}

func (t *snapshotTracker) release(ts uint64) {
	t.mu.Lock()
	if t.open[ts] <= 1 {
		delete(t.open, ts)
	} else {
		t.open[ts]--
	}
	t.mu.Unlock()
}

func (t *snapshotTracker) oldest() (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var min uint64
	found := false
	for ts := range t.open {
		if !found || ts < min {
			min, found = ts, true
		}
	}
	return min, found
}

func (t *snapshotTracker) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.open)
}

// Snapshot acquires a linearizable read timestamp on the leader and registers
// it so garbage collection cannot reclaim versions the caller still needs.
// The returned release function must always be called.
func (c *Coordinator) Snapshot(ctx context.Context) (uint64, func(), error) {
	if err := c.rep.ReadLease(ctx); err != nil {
		return 0, nil, err
	}
	ts := c.rep.LastApplied()
	c.snaps.acquire(ts)
	var once sync.Once
	return ts, func() { once.Do(func() { c.snaps.release(ts) }) }, nil
}

// OpenSnapshots reports how many read snapshots are currently pinned.
func (c *Coordinator) OpenSnapshots() int { return c.snaps.count() }

// ---------------------------------------------------------------------------
// Single-shot reads
// ---------------------------------------------------------------------------

// Get performs a linearizable point read.
func (c *Coordinator) Get(ctx context.Context, key string) (mvcc.KV, error) {
	ts, release, err := c.Snapshot(ctx)
	if err != nil {
		return mvcc.KV{}, err
	}
	defer release()
	return c.engine.Store().Get(key, ts)
}

// Scan performs a linearizable range read.
func (c *Coordinator) Scan(ctx context.Context, start, end string, limit int) (mvcc.ScanResult, uint64, error) {
	ts, release, err := c.Snapshot(ctx)
	if err != nil {
		return mvcc.ScanResult{}, 0, err
	}
	defer release()
	return c.engine.Store().Scan(start, end, limit, ts), ts, nil
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

// Txn is an interactive, optimistic transaction. Reads are served from a fixed
// snapshot; writes are buffered locally and validated atomically at commit.
// Nothing is locked, so a slow client cannot block the rest of the fleet —
// the price is that a contended transaction may have to retry.
type Txn struct {
	co      *Coordinator
	id      string
	tenant  string
	readTS  uint64
	release func()

	reads  map[string]uint64
	ranges []RangeRef
	writes map[string]Mutation
	conds  []Condition

	// ScannedKeys accumulates work done on behalf of this transaction, which
	// the server converts into request units for throttling.
	ScannedKeys int
	BytesRead   int

	done bool
}

// Begin opens a transaction at the current linearizable snapshot.
func (c *Coordinator) Begin(ctx context.Context, tenant string) (*Txn, error) {
	ts, release, err := c.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &Txn{
		co:      c,
		id:      newTxnID(),
		tenant:  tenant,
		readTS:  ts,
		release: release,
		reads:   map[string]uint64{},
		writes:  map[string]Mutation{},
	}, nil
}

// ID returns the idempotency key of the transaction.
func (t *Txn) ID() string { return t.id }

// ReadTS returns the snapshot timestamp.
func (t *Txn) ReadTS() uint64 { return t.readTS }

// Get reads a key at the transaction snapshot, observing the transaction's own
// buffered writes.
func (t *Txn) Get(key string) (mvcc.KV, error) {
	if m, ok := t.writes[key]; ok {
		if m.Delete {
			return mvcc.KV{}, mvcc.ErrNotFound
		}
		return mvcc.KV{Key: key, Value: m.Value}, nil
	}
	store := t.co.engine.Store()
	// Record the observed version even when the key is absent: "absent" is a
	// fact that another transaction can invalidate by creating the key.
	t.reads[key] = store.VersionAt(key, t.readTS)
	kv, err := store.Get(key, t.readTS)
	if err == nil {
		t.BytesRead += len(kv.Value)
	}
	t.ScannedKeys++
	return kv, err
}

// Scan reads a range at the transaction snapshot and records it for phantom
// detection.
func (t *Txn) Scan(start, end string, limit int) mvcc.ScanResult {
	res := t.co.engine.Store().Scan(start, end, limit, t.readTS)
	t.ranges = append(t.ranges, RangeRef{Start: start, End: end})
	t.ScannedKeys += res.ScannedKeys

	// Overlay buffered writes so the transaction reads its own effects.
	merged := make(map[string]mvcc.KV, len(res.KVs))
	for _, kv := range res.KVs {
		merged[kv.Key] = kv
		t.BytesRead += len(kv.Value)
	}
	for key, m := range t.writes {
		inRange := key >= start && (end == "" || key < end)
		if !inRange {
			continue
		}
		if m.Delete {
			delete(merged, key)
			continue
		}
		merged[key] = mvcc.KV{Key: key, Value: m.Value}
	}
	out := make([]mvcc.KV, 0, len(merged))
	for _, kv := range merged {
		out = append(out, kv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		res.More = true
	}
	res.KVs = out
	return res
}

// Put buffers a write.
func (t *Txn) Put(key string, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	t.writes[key] = Mutation{Key: key, Value: cp}
}

// Delete buffers a deletion.
func (t *Txn) Delete(key string) { t.writes[key] = Mutation{Key: key, Delete: true} }

// Require adds an explicit precondition, evaluated atomically at commit.
func (t *Txn) Require(c Condition) { t.conds = append(t.conds, c) }

// Rollback discards the transaction and releases its snapshot.
func (t *Txn) Rollback() {
	if t.done {
		return
	}
	t.done = true
	if t.release != nil {
		t.release()
	}
}

// Command materialises the transaction for replication. Exported for tests and
// for the admin "explain" endpoint.
func (t *Txn) Command() *Command {
	cmd := &Command{
		Op:         OpCommit,
		Tenant:     t.tenant,
		TxnID:      t.id,
		ReadTS:     t.readTS,
		Ranges:     t.ranges,
		Conditions: t.conds,
	}
	for k, v := range t.reads {
		// A key the transaction also wrote does not need read validation
		// beyond what the write itself implies, but keeping it makes the
		// transaction strictly serializable rather than merely snapshot
		// isolated, which is what callers of a control plane expect.
		cmd.Reads = append(cmd.Reads, ReadRef{Key: k, Version: v})
	}
	for _, m := range t.writes {
		cmd.Mutations = append(cmd.Mutations, m)
	}
	return cmd
}

// Commit validates and applies the transaction atomically.
//
// A read-only transaction with no buffered writes commits locally: it already
// read from a linearizable snapshot, so replicating an empty command would add
// a round trip and no information.
func (t *Txn) Commit(ctx context.Context) (Result, error) {
	if t.done {
		return Result{}, errors.New("txn: already finished")
	}
	defer t.Rollback()

	if len(t.writes) == 0 && len(t.conds) == 0 {
		return Result{Committed: true, CommitTS: t.readTS}, nil
	}

	cmd := t.Command()
	data, err := cmd.Encode()
	if err != nil {
		return Result{}, err
	}
	out, err := t.co.rep.Propose(ctx, data)
	if err != nil {
		return Result{}, err
	}
	res, ok := out.(Result)
	if !ok {
		return Result{}, fmt.Errorf("txn: unexpected apply result %T", out)
	}
	if !res.Committed && res.Conflict != nil {
		return res, &ConflictError{Conflict: *res.Conflict}
	}
	return res, nil
}

// Execute runs fn inside a transaction and retries it on conflict with
// exponential backoff and full jitter.
//
// Retrying is the coordinator's job rather than the caller's because the
// correct retry needs a fresh snapshot: replaying the same buffered writes
// against a stale read set would abort forever.
func (c *Coordinator) Execute(ctx context.Context, tenant string, fn func(*Txn) error) (Result, error) {
	var lastConflict *Conflict
	for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
		tx, err := c.Begin(ctx, tenant)
		if err != nil {
			return Result{}, err
		}
		if err := fn(tx); err != nil {
			tx.Rollback()
			return Result{}, err
		}
		res, err := tx.Commit(ctx)
		if err == nil {
			return res, nil
		}
		var ce *ConflictError
		if !errors.As(err, &ce) {
			return Result{}, err
		}
		lastConflict = &ce.Conflict

		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(c.backoff(attempt)):
		}
	}
	if lastConflict != nil {
		return Result{Conflict: lastConflict}, fmt.Errorf("%w: last conflict %s on %q",
			ErrRetriesExhausted, lastConflict.Kind, lastConflict.Key)
	}
	return Result{}, ErrRetriesExhausted
}

// backoff returns an exponentially growing delay with full jitter, which
// spreads retry storms instead of synchronising them.
func (c *Coordinator) backoff(attempt int) time.Duration {
	d := c.opts.BaseBackoff << attempt
	if d > 250*time.Millisecond {
		d = 250 * time.Millisecond
	}
	c.mu.Lock()
	j := c.rnd.Int63n(int64(d) + 1)
	c.mu.Unlock()
	return time.Duration(j)
}

// ---------------------------------------------------------------------------
// Garbage collection
// ---------------------------------------------------------------------------

func (c *Coordinator) gcLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.opts.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := c.RunGC(ctx); err != nil && !errors.Is(err, ErrReadOnlyReplica) {
				c.log.Warn("gc pass failed", "err", err)
			}
			cancel()
		}
	}
}

// Watermark computes the highest index that is safe to collect: everything
// below the oldest pinned snapshot and below the retention floor.
func (c *Coordinator) Watermark() uint64 {
	applied := c.rep.LastApplied()
	var wm uint64
	if applied > c.opts.RetentionIndexes {
		wm = applied - c.opts.RetentionIndexes
	}
	if oldest, ok := c.snaps.oldest(); ok && oldest > 0 && oldest-1 < wm {
		wm = oldest - 1
	}
	return wm
}

// RunGC proposes a collection pass. Only the leader proposes; every replica
// then performs exactly the same collection when it applies the command.
func (c *Coordinator) RunGC(ctx context.Context) (Result, error) {
	if !c.rep.IsLeader() {
		return Result{}, ErrReadOnlyReplica
	}
	wm := c.Watermark()

	c.mu.Lock()
	if wm <= c.lastWatermark {
		c.mu.Unlock()
		return Result{Committed: true}, nil // nothing new to reclaim
	}
	c.mu.Unlock()

	cmd := &Command{Op: OpGC, Watermark: wm}
	data, err := cmd.Encode()
	if err != nil {
		return Result{}, err
	}
	out, err := c.rep.Propose(ctx, data)
	if err != nil {
		return Result{}, err
	}
	c.mu.Lock()
	c.lastWatermark = wm
	c.mu.Unlock()

	if res, ok := out.(Result); ok {
		return res, nil
	}
	return Result{Committed: true}, nil
}

func newTxnID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A weaker ID still preserves idempotency within a process lifetime.
		return fmt.Sprintf("txn-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// SetID overrides the transaction's idempotency key with one supplied by the
// client, so that a client-side retry across a leader change is deduplicated.
func (t *Txn) SetID(id string) {
	if id != "" {
		t.id = id
	}
}

// CommitRaw replicates a caller-constructed command. It backs the one-shot
// transaction endpoint, where the client submits its own read set gathered from
// earlier requests instead of holding a server-side transaction open.
func (c *Coordinator) CommitRaw(ctx context.Context, cmd *Command) (Result, error) {
	data, err := cmd.Encode()
	if err != nil {
		return Result{}, err
	}
	out, err := c.rep.Propose(ctx, data)
	if err != nil {
		return Result{}, err
	}
	res, ok := out.(Result)
	if !ok {
		return Result{}, fmt.Errorf("txn: unexpected apply result %T", out)
	}
	if !res.Committed && res.Conflict != nil {
		return res, &ConflictError{Conflict: *res.Conflict}
	}
	return res, nil
}

// Engine exposes the state machine for read-only introspection.
func (c *Coordinator) Engine() *Engine { return c.engine }

// IsLeader reports whether this replica can accept writes.
func (c *Coordinator) IsLeader() bool { return c.rep.IsLeader() }
