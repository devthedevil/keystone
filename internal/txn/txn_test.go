package txn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/raft"
	"github.com/devthedevil/keystone/internal/stream"
)

type testCluster struct {
	t      *testing.T
	nodes  map[raft.NodeID]*raft.Node
	coords map[raft.NodeID]*Coordinator
	engs   map[raft.NodeID]*Engine
	net    *raft.Network
	ids    []raft.NodeID
}

func newTestCluster(t *testing.T, size int, opts Options) *testCluster {
	t.Helper()
	c := &testCluster{
		t:      t,
		nodes:  map[raft.NodeID]*raft.Node{},
		coords: map[raft.NodeID]*Coordinator{},
		engs:   map[raft.NodeID]*Engine{},
		net:    raft.NewNetwork(),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts.Logger = logger
	for i := 0; i < size; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("n%d", i+1)))
	}
	for i, id := range c.ids {
		var peers []raft.NodeID
		for _, o := range c.ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		eng := NewEngine(mvcc.New(), stream.NewBroker(stream.Options{}), EngineOptions{Logger: logger})
		n, err := raft.NewNode(raft.Config{
			ID:                id,
			Peers:             peers,
			Storage:           raft.NewMemStorage(),
			Transport:         c.net.Transport(id),
			FSM:               eng,
			HeartbeatInterval: 20 * time.Millisecond,
			ElectionTimeout:   150 * time.Millisecond,
			Logger:            logger,
			Rand:              rand.New(rand.NewSource(int64(i*104729 + 7))),
		})
		if err != nil {
			t.Fatalf("node: %v", err)
		}
		c.net.Register(id, n)
		c.nodes[id] = n
		c.engs[id] = eng
		c.coords[id] = NewCoordinator(n, eng, opts)
	}
	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, co := range c.coords {
			co.Stop()
		}
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *testCluster) leader(timeout time.Duration) *Coordinator {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.IsLeader() {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				err := n.ReadLease(ctx)
				cancel()
				if err == nil {
					return c.coords[id]
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader with a valid lease")
	return nil
}

func ctxT(t *testing.T, d time.Duration) (context.Context, func()) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

func TestTransactionReadWriteRoundTrip(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	co := c.leader(3 * time.Second)

	ctx, cancel := ctxT(t, 3*time.Second)
	defer cancel()

	res, err := co.Execute(ctx, "tenant-a", func(tx *Txn) error {
		tx.Put("svc/vcn/1", []byte(`{"cidr":"10.0.0.0/16"}`))
		tx.Put("svc/vcn/2", []byte(`{"cidr":"10.1.0.0/16"}`))
		return nil
	})
	if err != nil || !res.Committed {
		t.Fatalf("commit failed: %v %+v", err, res)
	}

	kv, err := co.Get(ctx, "svc/vcn/1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(kv.Value) != `{"cidr":"10.0.0.0/16"}` {
		t.Fatalf("unexpected value %q", kv.Value)
	}
	if kv.Version != res.CommitTS {
		t.Fatalf("version %d should equal commit index %d", kv.Version, res.CommitTS)
	}
}

func TestWriteWriteConflictAbortsSecondTransaction(t *testing.T) {
	c := newTestCluster(t, 3, Options{MaxRetries: 1})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 5*time.Second)
	defer cancel()

	if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
		tx.Put("counter", []byte("0"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Two transactions read the same key at the same snapshot.
	a, err := co.Begin(ctx, "t")
	if err != nil {
		t.Fatalf("begin a: %v", err)
	}
	b, err := co.Begin(ctx, "t")
	if err != nil {
		t.Fatalf("begin b: %v", err)
	}
	if _, err := a.Get("counter"); err != nil {
		t.Fatalf("a read: %v", err)
	}
	if _, err := b.Get("counter"); err != nil {
		t.Fatalf("b read: %v", err)
	}
	a.Put("counter", []byte("1"))
	b.Put("counter", []byte("2"))

	if res, err := a.Commit(ctx); err != nil || !res.Committed {
		t.Fatalf("first committer should win: %v %+v", err, res)
	}
	res, err := b.Commit(ctx)
	if err == nil || res.Committed {
		t.Fatal("second committer must abort: lost update was allowed")
	}
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Conflict.Kind != ConflictReadWrite {
		t.Fatalf("expected a read/write conflict, got %v", err)
	}

	kv, _ := co.Get(ctx, "counter")
	if string(kv.Value) != "1" {
		t.Fatalf("aborted transaction leaked a write: %q", kv.Value)
	}
}

func TestPhantomInsertAbortsScanningTransaction(t *testing.T) {
	c := newTestCluster(t, 3, Options{MaxRetries: 1})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 5*time.Second)
	defer cancel()

	if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
		tx.Put("quota/a/res-1", []byte("1"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A "check the quota then add one more" transaction.
	scanner, err := co.Begin(ctx, "t")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	scanner.Scan("quota/a/", "quota/a0", 100)

	// Someone else inserts into the very range that was scanned.
	if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
		tx.Put("quota/a/res-2", []byte("1"))
		return nil
	}); err != nil {
		t.Fatalf("interloper: %v", err)
	}

	scanner.Put("quota/a/res-3", []byte("1"))
	res, err := scanner.Commit(ctx)
	if err == nil || res.Committed {
		t.Fatal("phantom insert was not detected: the scan was not serializable")
	}
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Conflict.Kind != ConflictPhantom {
		t.Fatalf("expected a phantom conflict, got %v", err)
	}
}

func TestDuplicateProposalIsIdempotent(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 5*time.Second)
	defer cancel()

	tx, err := co.Begin(ctx, "t")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Put("idem", []byte("v1"))
	cmd := tx.Command()
	data, _ := cmd.Encode()

	// Simulate a client that timed out and retried after the write landed.
	out1, err := co.rep.Propose(ctx, data)
	if err != nil {
		t.Fatalf("first propose: %v", err)
	}
	out2, err := co.rep.Propose(ctx, data)
	if err != nil {
		t.Fatalf("retry propose: %v", err)
	}
	r1, r2 := out1.(Result), out2.(Result)
	if !r1.Committed || !r2.Committed {
		t.Fatalf("both results should report committed: %+v %+v", r1, r2)
	}
	if !r2.Duplicate {
		t.Fatal("retry was not recognised as a duplicate")
	}
	if r1.CommitTS != r2.CommitTS {
		t.Fatalf("retry reported a different commit timestamp: %d vs %d", r1.CommitTS, r2.CommitTS)
	}
	if got := co.engine.Stats().Commits; got != 1 {
		t.Fatalf("duplicate was applied twice: %d commits", got)
	}
}

// TestConcurrentTransfersPreserveInvariant is the serializability check: many
// concurrent transfers between accounts must never create or destroy value.
// Under snapshot isolation alone this test would fail with write skew.
func TestConcurrentTransfersPreserveInvariant(t *testing.T) {
	c := newTestCluster(t, 3, Options{MaxRetries: 50, BaseBackoff: time.Millisecond})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 60*time.Second)
	defer cancel()

	const accounts = 8
	const startBalance = 100
	const workers = 6
	const transfers = 25

	if _, err := co.Execute(ctx, "bank", func(tx *Txn) error {
		for i := 0; i < accounts; i++ {
			tx.Put(acctKey(i), encodeInt(startBalance))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for i := 0; i < transfers; i++ {
				from := rnd.Intn(accounts)
				to := rnd.Intn(accounts)
				if from == to {
					continue
				}
				amount := int64(rnd.Intn(10) + 1)
				_, err := co.Execute(ctx, "bank", func(tx *Txn) error {
					src, err := tx.Get(acctKey(from))
					if err != nil {
						return err
					}
					dst, err := tx.Get(acctKey(to))
					if err != nil {
						return err
					}
					sb, db := decodeInt(src.Value), decodeInt(dst.Value)
					if sb < amount {
						return nil // insufficient funds: commit nothing
					}
					tx.Put(acctKey(from), encodeInt(sb-amount))
					tx.Put(acctKey(to), encodeInt(db+amount))
					return nil
				})
				if err != nil && !errors.Is(err, ErrRetriesExhausted) {
					errCh <- err
					return
				}
			}
		}(int64(w*31 + 5))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("worker failed: %v", err)
	}

	// The invariant must hold, and it must hold on every replica.
	deadline := time.Now().Add(10 * time.Second)
	for {
		converged := true
		for id := range c.engs {
			total := int64(0)
			res := c.engs[id].Store().Scan("acct/", "acct0", 1000, ^uint64(0))
			if len(res.KVs) != accounts {
				converged = false
				break
			}
			for _, kv := range res.KVs {
				total += decodeInt(kv.Value)
			}
			if total != accounts*startBalance {
				if c.nodes[id].IsLeader() {
					t.Fatalf("invariant violated on leader %s: total=%d want=%d", id, total, accounts*startBalance)
				}
				converged = false
				break
			}
		}
		if converged {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not converge on the invariant")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGCRespectsOpenSnapshot(t *testing.T) {
	c := newTestCluster(t, 3, Options{RetentionIndexes: 1})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 10*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
			tx.Put("k", []byte(fmt.Sprintf("v%d", i)))
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Pin an old snapshot, then write more and collect.
	pinnedTS, release, err := co.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for i := 5; i < 10; i++ {
		if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
			tx.Put("k", []byte(fmt.Sprintf("v%d", i)))
			return nil
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if _, err := co.RunGC(ctx); err != nil {
		t.Fatalf("gc: %v", err)
	}

	// The pinned reader must still see its own snapshot.
	kv, err := co.engine.Store().Get("k", pinnedTS)
	if err != nil {
		t.Fatalf("GC collected a pinned snapshot: %v", err)
	}
	if string(kv.Value) != "v4" {
		t.Fatalf("pinned snapshot sees the wrong version: %q", kv.Value)
	}
	release()

	// Once released, collection may advance past it.
	if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
		tx.Put("k", []byte("final"))
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := co.RunGC(ctx); err != nil {
		t.Fatalf("gc 2: %v", err)
	}
	if st := co.engine.Store().Stats(); st.Versions > 3 {
		t.Fatalf("collector did not reclaim shadowed versions: %+v", st)
	}
}

func TestReadOnlyTransactionSkipsReplication(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	co := c.leader(3 * time.Second)
	ctx, cancel := ctxT(t, 5*time.Second)
	defer cancel()

	if _, err := co.Execute(ctx, "t", func(tx *Txn) error {
		tx.Put("ro", []byte("v"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := co.engine.AppliedIndex()

	tx, err := co.Begin(ctx, "t")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Get("ro"); err != nil {
		t.Fatalf("read: %v", err)
	}
	res, err := tx.Commit(ctx)
	if err != nil || !res.Committed {
		t.Fatalf("read-only commit: %v %+v", err, res)
	}
	if co.engine.AppliedIndex() != before {
		t.Fatal("a read-only transaction appended to the replicated log")
	}
}

func acctKey(i int) string { return fmt.Sprintf("acct/%03d", i) }

func encodeInt(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func decodeInt(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
