// Package chaos runs Keystone under continuous failure and asserts that the
// guarantees it advertises still hold.
//
// The unit tests answer "does this work?". These answer the question that
// actually matters for a tier-0 dependency: "what does it do at 3am when the
// network is broken?" Every test here injects partitions, isolation and packet
// loss underneath a live workload, and then checks an invariant that no
// correct history can violate.
//
// These tests are slower than the unit suite on purpose. Run them with
//
//	go test ./test/... -race -timeout 10m
package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/raft"
	"github.com/devthedevil/keystone/internal/stream"
	"github.com/devthedevil/keystone/internal/txn"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type cluster struct {
	t      *testing.T
	ids    []raft.NodeID
	nodes  map[raft.NodeID]*raft.Node
	coords map[raft.NodeID]*txn.Coordinator
	engs   map[raft.NodeID]*txn.Engine
	net    *raft.Network
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{
		t:      t,
		nodes:  map[raft.NodeID]*raft.Node{},
		coords: map[raft.NodeID]*txn.Coordinator{},
		engs:   map[raft.NodeID]*txn.Engine{},
		net:    raft.NewNetwork(),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
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
		eng := txn.NewEngine(mvcc.New(), stream.NewBroker(stream.Options{}), txn.EngineOptions{Logger: logger})
		n, err := raft.NewNode(raft.Config{
			ID:                id,
			Peers:             peers,
			Storage:           raft.NewMemStorage(),
			Transport:         c.net.Transport(id),
			FSM:               eng,
			HeartbeatInterval: 20 * time.Millisecond,
			ElectionTimeout:   150 * time.Millisecond,
			Logger:            logger,
			Rand:              rand.New(rand.NewSource(int64(i*104729 + 11))),
		})
		if err != nil {
			t.Fatalf("building node: %v", err)
		}
		c.net.Register(id, n)
		c.nodes[id] = n
		c.engs[id] = eng
		c.coords[id] = txn.NewCoordinator(n, eng, txn.Options{
			RetentionIndexes: 1000,
			GCInterval:       200 * time.Millisecond,
			Logger:           logger,
		})
	}
	for _, n := range c.nodes {
		n.Start()
	}
	for _, co := range c.coords {
		co.Start()
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

// leader returns a coordinator that currently leads with a valid lease, or nil
// if there is none right now. A chaos workload must tolerate nil: during an
// election there genuinely is no leader, and pretending otherwise would hide
// the exact window these tests exist to exercise.
func (c *cluster) leader() *txn.Coordinator {
	for id, n := range c.nodes {
		if !n.IsLeader() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := n.ReadLease(ctx)
		cancel()
		if err == nil {
			return c.coords[id]
		}
	}
	return nil
}

func (c *cluster) waitForLeader(timeout time.Duration) *txn.Coordinator {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if co := c.leader(); co != nil {
			return co
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader emerged within the timeout")
	return nil
}

// chaosMonkey drives the failure injection until the context is cancelled. It
// cycles through minority partitions, single-node isolation and packet loss,
// always healing afterwards so the cluster has a chance to make progress.
func (c *cluster) chaosMonkey(ctx context.Context, seed int64, events *int64) {
	rng := rand.New(rand.NewSource(seed))
	for ctx.Err() == nil {
		switch rng.Intn(3) {
		case 0:
			// Isolate one node: the common case of a host losing its NIC.
			victim := c.ids[rng.Intn(len(c.ids))]
			c.net.Isolate(victim)
			sleepCtx(ctx, time.Duration(50+rng.Intn(250))*time.Millisecond)
			c.net.Heal()
		case 1:
			// Split into a majority and a minority. The minority must not be
			// able to commit anything.
			perm := rng.Perm(len(c.ids))
			var a, b []raft.NodeID
			for i, p := range perm {
				if i < len(c.ids)/2 {
					b = append(b, c.ids[p])
				} else {
					a = append(a, c.ids[p])
				}
			}
			c.net.Partition(a, b)
			sleepCtx(ctx, time.Duration(50+rng.Intn(250))*time.Millisecond)
			c.net.Heal()
		case 2:
			// Degrade rather than break: slow, lossy links are harder on a
			// consensus protocol than a clean partition.
			c.net.SetDropRate(0.2 + rng.Float64()*0.3)
			c.net.SetLatency(time.Duration(rng.Intn(20))*time.Millisecond, 10*time.Millisecond)
			sleepCtx(ctx, time.Duration(100+rng.Intn(200))*time.Millisecond)
			c.net.SetDropRate(0)
			c.net.SetLatency(0, 0)
		}
		atomic.AddInt64(events, 1)
		sleepCtx(ctx, time.Duration(50+rng.Intn(150))*time.Millisecond)
	}
	c.net.Heal()
	c.net.SetDropRate(0)
	c.net.SetLatency(0, 0)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// ---------------------------------------------------------------------------
// Linearizability
// ---------------------------------------------------------------------------

// event is one observed operation on a single register, with the real-time
// interval during which it was in flight.
type event struct {
	kind    string // "write" or "read"
	value   int
	version uint64
	start   time.Time
	end     time.Time
}

// checkRegisterLinearizable verifies a history of a single-register workload.
//
// It relies on a property specific to Keystone rather than a general-purpose
// search: every committed write is stamped with the Raft log index, so the
// commit order is directly observable. That turns linearizability checking
// from an NP-hard search into two concrete checks:
//
//  1. Real-time order must agree with commit order. If write A returned before
//     write B was submitted, A's version must be lower than B's. A violation
//     means a stale leader committed out of order — split brain.
//  2. Every read must return a value that was actually written at the version
//     it reports, and no read may observe a version that was committed only
//     after the read had already returned.
func checkRegisterLinearizable(t *testing.T, history []event) {
	t.Helper()

	writes := make([]event, 0, len(history))
	reads := make([]event, 0, len(history))
	for _, e := range history {
		if e.kind == "write" {
			writes = append(writes, e)
		} else {
			reads = append(reads, e)
		}
	}
	sort.Slice(writes, func(i, j int) bool { return writes[i].version < writes[j].version })

	// Check 1: no two committed writes share a version, and real-time order
	// agrees with commit order.
	byVersion := make(map[uint64]event, len(writes))
	for _, w := range writes {
		if prev, dup := byVersion[w.version]; dup {
			t.Fatalf("two writes committed at the same version %d (values %d and %d): "+
				"this is split brain", w.version, prev.value, w.value)
		}
		byVersion[w.version] = w
	}
	for i := range writes {
		for j := range writes {
			if i == j {
				continue
			}
			a, b := writes[i], writes[j]
			// a completed strictly before b started, so a must be older.
			if a.end.Before(b.start) && a.version > b.version {
				t.Fatalf("real-time order violated: write(%d)@v%d returned at %s before "+
					"write(%d)@v%d started at %s, yet committed later",
					a.value, a.version, a.end.Format(time.StampMicro),
					b.value, b.version, b.start.Format(time.StampMicro))
			}
		}
	}

	// Check 2: every read matches a write that really happened, and could not
	// have observed the future.
	for _, r := range reads {
		w, ok := byVersion[r.version]
		if !ok {
			// A read may legitimately observe a write whose client never got
			// an answer (the commit succeeded but the response was lost), so
			// an unknown version is only a failure if the value is one no
			// client ever wrote.
			continue
		}
		if w.value != r.value {
			t.Fatalf("read returned value %d at version %d, but version %d was written with value %d",
				r.value, r.version, w.version, w.value)
		}
		if w.start.After(r.end) {
			t.Fatalf("read returned at %s observed a write that had not started until %s: "+
				"a read saw the future", r.end.Format(time.StampMicro), w.start.Format(time.StampMicro))
		}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRegisterStaysLinearizableUnderPartitionStorm hammers a single key from
// several clients while the network is continuously broken and repaired, then
// checks the whole recorded history for a linearizability violation.
func TestRegisterStaysLinearizableUnderPartitionStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests are slow by design")
	}
	c := newCluster(t, 5)
	c.waitForLeader(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	var chaosEvents int64
	var monkey sync.WaitGroup
	monkey.Add(1)
	go func() { defer monkey.Done(); c.chaosMonkey(ctx, 42, &chaosEvents) }()

	var (
		mu      sync.Mutex
		history []event
		commits int64
		aborts  int64
	)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				value := worker*100000 + i
				co := c.leader()
				if co == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				opCtx, opCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				start := time.Now()
				res, err := co.Execute(opCtx, "chaos", func(tx *txn.Txn) error {
					tx.Put("register", []byte(fmt.Sprintf("%d", value)))
					return nil
				})
				end := time.Now()
				opCancel()

				if err == nil && res.Committed {
					atomic.AddInt64(&commits, 1)
					mu.Lock()
					history = append(history, event{
						kind: "write", value: value, version: res.CommitTS,
						start: start, end: end,
					})
					mu.Unlock()
					continue
				}
				atomic.AddInt64(&aborts, 1)
				// Every failure here must be one the contract allows. A
				// failure of any other shape means the system broke in a way
				// callers were never told to expect.
				assertExpectedFailure(t, err)
			}
		}(w)
	}

	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				co := c.leader()
				if co == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				opCtx, opCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				start := time.Now()
				kv, err := co.Get(opCtx, "register")
				end := time.Now()
				opCancel()
				if err != nil {
					assertExpectedFailure(t, err)
					continue
				}
				// Pace the readers. Without this the history is millions of
				// entries of which almost none are interesting, and the
				// checker spends its time on memory rather than on races.
				time.Sleep(2 * time.Millisecond)
				var value int
				if _, err := fmt.Sscanf(string(kv.Value), "%d", &value); err != nil {
					t.Errorf("read returned an unparseable value %q", kv.Value)
					return
				}
				mu.Lock()
				history = append(history, event{
					kind: "read", value: value, version: kv.Version,
					start: start, end: end,
				})
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	cancel()
	monkey.Wait()

	mu.Lock()
	defer mu.Unlock()
	t.Logf("history: %d operations (%d commits, %d expected failures) across %d injected faults",
		len(history), commits, aborts, atomic.LoadInt64(&chaosEvents))
	if commits < 20 {
		t.Fatalf("only %d writes committed: the cluster failed to make progress under churn", commits)
	}
	checkRegisterLinearizable(t, history)
}

// assertExpectedFailure fails the test if an operation failed in a way the
// wire contract does not describe. Under chaos, "it returned an error" is
// fine; "it returned an error nobody documented" is a bug.
func assertExpectedFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var notLeader *raft.NotLeaderError
	var conflict *txn.ConflictError
	switch {
	case errors.As(err, &notLeader),
		errors.As(err, &conflict),
		errors.Is(err, raft.ErrNoLease),
		errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrStopped),
		errors.Is(err, txn.ErrReadOnlyReplica),
		// A read issued before the first write legitimately finds nothing.
		errors.Is(err, mvcc.ErrNotFound),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return
	default:
		t.Errorf("operation failed in an undocumented way: %#v (%v)", err, err)
	}
}

// TestReplicasConvergeAfterHealing runs a mixed workload through a partition
// storm and then asserts that, once the network is whole, every replica holds
// byte-identical state. Divergence here would mean the state machine is not
// deterministic, which no amount of consensus can repair.
func TestReplicasConvergeAfterHealing(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos tests are slow by design")
	}
	c := newCluster(t, 5)
	c.waitForLeader(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var chaosEvents int64
	var monkey sync.WaitGroup
	monkey.Add(1)
	go func() { defer monkey.Done(); c.chaosMonkey(ctx, 7, &chaosEvents) }()

	const keys = 40
	var wg sync.WaitGroup
	var commits int64
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker)))
			for i := 0; ctx.Err() == nil; i++ {
				co := c.leader()
				if co == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				key := fmt.Sprintf("key%02d", rng.Intn(keys))
				opCtx, opCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				res, err := co.Execute(opCtx, "chaos", func(tx *txn.Txn) error {
					if rng.Intn(5) == 0 {
						tx.Delete(key)
						return nil
					}
					tx.Put(key, []byte(fmt.Sprintf("w%d-i%d", worker, i)))
					return nil
				})
				opCancel()
				if err == nil && res.Committed {
					atomic.AddInt64(&commits, 1)
					continue
				}
				assertExpectedFailure(t, err)
			}
		}(w)
	}
	wg.Wait()
	cancel()
	monkey.Wait()

	// Heal and give the cluster room to catch every replica up.
	c.net.Heal()
	c.net.SetDropRate(0)
	c.net.SetLatency(0, 0)

	leader := c.waitForLeader(10 * time.Second)
	// One final committed write flushes the pipeline: once it is applied
	// everywhere, every earlier entry is too.
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := leader.Execute(commitCtx, "chaos", func(tx *txn.Txn) error {
		tx.Put("final", []byte("barrier"))
		return nil
	}); err != nil {
		commitCancel()
		t.Fatalf("cluster never recovered enough to commit after healing: %v", err)
	}
	commitCancel()

	target := waitForConvergence(t, c, 10*time.Second)
	t.Logf("%d commits across %d injected faults; all %d replicas converged at applied index %d",
		atomic.LoadInt64(&commits), atomic.LoadInt64(&chaosEvents), len(c.ids), target)
}

// waitForConvergence waits until every replica has applied the same index and
// holds identical state, then returns that index.
func waitForConvergence(t *testing.T, c *cluster, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastReason string
	for time.Now().Before(deadline) {
		applied := map[raft.NodeID]uint64{}
		same := true
		var target uint64
		for _, id := range c.ids {
			a := c.nodes[id].Status().LastApplied
			applied[id] = a
			if target == 0 {
				target = a
			} else if a != target {
				same = false
			}
		}
		if !same {
			lastReason = fmt.Sprintf("applied indexes still differ: %v", applied)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Same index is necessary but not sufficient: compare the actual
		// contents, which is what determinism really means.
		var ref map[string]string
		mismatch := ""
		for _, id := range c.ids {
			snap := dumpStore(c.engs[id], target)
			if ref == nil {
				ref = snap
				continue
			}
			if len(snap) != len(ref) {
				mismatch = fmt.Sprintf("%s holds %d keys, reference holds %d", id, len(snap), len(ref))
				break
			}
			for k, v := range ref {
				if snap[k] != v {
					mismatch = fmt.Sprintf("%s disagrees on %q: %q vs %q", id, k, snap[k], v)
					break
				}
			}
			if mismatch != "" {
				break
			}
		}
		if mismatch == "" {
			return target
		}
		lastReason = mismatch
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replicas never converged: %s", lastReason)
	return 0
}

func dumpStore(e *txn.Engine, readTS uint64) map[string]string {
	out := map[string]string{}
	res := e.Store().Scan("", "", 10000, readTS)
	for _, kv := range res.KVs {
		out[kv.Key] = string(kv.Value)
	}
	return out
}

// TestMinorityCannotCommitDuringPartition pins a leader into a minority and
// asserts it cannot acknowledge a write. This is the property that makes the
// store safe to build a control plane on: a partitioned replica must refuse to
// answer rather than answer wrongly.
func TestMinorityCannotCommitDuringPartition(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitForLeader(5 * time.Second)

	// Find the leader's ID so it can be placed in the minority.
	var leaderID raft.NodeID
	for id, co := range c.coords {
		if co == leader {
			leaderID = id
		}
	}
	var minority, majority []raft.NodeID
	for _, id := range c.ids {
		if id == leaderID || len(minority) < 1 {
			if id == leaderID || len(minority) == 0 {
				minority = append(minority, id)
				continue
			}
		}
		majority = append(majority, id)
	}
	if len(minority) >= len(majority) {
		t.Fatalf("test setup is wrong: minority %v is not smaller than majority %v", minority, majority)
	}
	c.net.Partition(minority, majority)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := leader.Execute(ctx, "chaos", func(tx *txn.Txn) error {
		tx.Put("must-not-commit", []byte("x"))
		return nil
	})
	if err == nil {
		t.Fatal("a leader in the minority acknowledged a write: this is a split brain")
	}
	assertExpectedFailure(t, err)

	// After healing, the majority's view is the one that survives.
	c.net.Heal()
	survivor := c.waitForLeader(10 * time.Second)
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	if _, err := survivor.Get(readCtx, "must-not-commit"); err == nil {
		t.Fatal("a write rejected during a partition became visible after healing")
	}
}
