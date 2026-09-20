package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// counterFSM records every applied entry so tests can assert on log order.
type counterFSM struct {
	mu      sync.Mutex
	applied [][]byte
}

func (f *counterFSM) Apply(e Entry) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, append([]byte(nil), e.Data...))
	return len(f.applied)
}

func (f *counterFSM) snapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.applied))
	copy(out, f.applied)
	return out
}

type cluster struct {
	t     *testing.T
	nodes map[NodeID]*Node
	fsms  map[NodeID]*counterFSM
	net   *Network
	ids   []NodeID
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{
		t:     t,
		nodes: map[NodeID]*Node{},
		fsms:  map[NodeID]*counterFSM{},
		net:   NewNetwork(),
	}
	for i := 0; i < size; i++ {
		c.ids = append(c.ids, NodeID(fmt.Sprintf("n%d", i+1)))
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i, id := range c.ids {
		var peers []NodeID
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		fsm := &counterFSM{}
		n, err := NewNode(Config{
			ID:                id,
			Peers:             peers,
			Storage:           NewMemStorage(),
			Transport:         c.net.Transport(id),
			FSM:               fsm,
			HeartbeatInterval: 20 * time.Millisecond,
			ElectionTimeout:   150 * time.Millisecond,
			Logger:            logger,
			Rand:              rand.New(rand.NewSource(int64(i*7919 + 13))),
		})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		c.nodes[id] = n
		c.fsms[id] = fsm
		c.net.Register(id, n)
	}
	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) waitLeader(timeout time.Duration) (*Node, NodeID) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := []NodeID{}
		for id, n := range c.nodes {
			if n.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return c.nodes[leaders[0]], leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader elected within %s", timeout)
	return nil, ""
}

// waitLeaderAmong waits for a leader drawn from the supplied set, which is how
// tests assert that a majority partition makes progress.
func (c *cluster) waitLeaderAmong(ids []NodeID, timeout time.Duration) (*Node, NodeID) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if c.nodes[id].IsLeader() {
				return c.nodes[id], id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v within %s", ids, timeout)
	return nil, ""
}

func TestSingleNodeElectsItself(t *testing.T) {
	c := newCluster(t, 1)
	n, _ := c.waitLeader(2 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := n.Propose(ctx, []byte("hello")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if got := len(c.fsms[n.cfg.ID].snapshot()); got != 1 {
		t.Fatalf("expected 1 applied entry, got %d", got)
	}
}

func TestLeaderElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3)
	leader, id := c.waitLeader(3 * time.Second)

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("v%02d", i))); err != nil {
			cancel()
			t.Fatalf("propose %d: %v", i, err)
		}
		cancel()
	}

	// Every replica must converge on the same prefix, in the same order.
	deadline := time.Now().Add(3 * time.Second)
	for {
		ok := true
		want := c.fsms[id].snapshot()
		for other := range c.nodes {
			got := c.fsms[other].snapshot()
			if len(got) != len(want) {
				ok = false
				break
			}
			for i := range want {
				if string(got[i]) != string(want[i]) {
					t.Fatalf("divergent log at %d on %s: %q vs %q", i, other, got[i], want[i])
				}
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaderFailoverPreservesCommittedEntries(t *testing.T) {
	c := newCluster(t, 5)
	leader, oldID := c.waitLeader(3 * time.Second)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("pre-%d", i))); err != nil {
			cancel()
			t.Fatalf("propose: %v", err)
		}
		cancel()
	}

	// Kill the leader.
	c.net.Isolate(oldID)

	survivors := []NodeID{}
	for _, id := range c.ids {
		if id != oldID {
			survivors = append(survivors, id)
		}
	}
	newLeader, newID := c.waitLeaderAmong(survivors, 5*time.Second)
	if newID == oldID {
		t.Fatal("isolated node should not be leader")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := newLeader.Propose(ctx, []byte("post-failover")); err != nil {
		t.Fatalf("propose after failover: %v", err)
	}

	// All ten pre-failover entries must survive on the new leader.
	applied := c.fsms[newID].snapshot()
	if len(applied) < 11 {
		t.Fatalf("committed entries lost: have %d want >= 11", len(applied))
	}
	for i := 0; i < 10; i++ {
		if string(applied[i]) != fmt.Sprintf("pre-%d", i) {
			t.Fatalf("entry %d corrupted: %q", i, applied[i])
		}
	}
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(3 * time.Second)

	minority := []NodeID{c.ids[0], c.ids[1]}
	majority := []NodeID{c.ids[2], c.ids[3], c.ids[4]}
	c.net.Partition(minority, majority)

	// The majority side must still elect and commit.
	leader, _ := c.waitLeaderAmong(majority, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, []byte("majority-write")); err != nil {
		t.Fatalf("majority must make progress: %v", err)
	}

	// The minority side must not commit anything.
	for _, id := range minority {
		n := c.nodes[id]
		if !n.IsLeader() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := n.Propose(ctx, []byte("minority-write"))
		cancel()
		if err == nil {
			t.Fatal("minority partition committed a write: split brain")
		}
	}
}

func TestDivergentFollowerLogIsRepaired(t *testing.T) {
	c := newCluster(t, 3)
	leader, leaderID := c.waitLeader(3 * time.Second)

	// Isolate a follower, write while it is away, then heal it.
	var followerID NodeID
	for _, id := range c.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	c.net.Isolate(followerID)

	for i := 0; i < 15; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("w%d", i))); err != nil {
			cancel()
			t.Fatalf("propose: %v", err)
		}
		cancel()
	}
	c.net.Heal()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if len(c.fsms[followerID].snapshot()) == 15 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower did not catch up: applied %d", len(c.fsms[followerID].snapshot()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaseReadRequiresQuorumContact(t *testing.T) {
	c := newCluster(t, 3)
	leader, leaderID := c.waitLeader(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leader.ReadLease(ctx); err != nil {
		t.Fatalf("healthy leader should hold a lease: %v", err)
	}

	// Cut the leader off: its lease must expire, and lease reads must fail
	// rather than serve a potentially stale value.
	c.net.Isolate(leaderID)
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := leader.ReadLease(ctx)
		cancel()
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated leader kept serving lease reads")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFileStorageReplaysAfterRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	entries := []Entry{
		{Term: 1, Index: 1, Type: EntryNoOp},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("alpha")},
		{Term: 2, Index: 3, Type: EntryNormal, Data: []byte("beta")},
	}
	if err := st.Append(entries); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := st.SaveHardState(HardState{Term: 2, VotedFor: "n1"}); err != nil {
		t.Fatalf("hard state: %v", err)
	}
	if err := st.TruncateFrom(3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := st.Append([]Entry{{Term: 3, Index: 3, Type: EntryNormal, Data: []byte("gamma")}}); err != nil {
		t.Fatalf("re-append: %v", err)
	}
	st.Close()

	reopened, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	hs, _ := reopened.LoadHardState()
	if hs.Term != 2 || hs.VotedFor != "n1" {
		t.Fatalf("hard state not recovered: %+v", hs)
	}
	idx, term := reopened.Last()
	if idx != 3 || term != 3 {
		t.Fatalf("last entry wrong after replay: idx=%d term=%d", idx, term)
	}
	got, _ := reopened.Entries(3, 4)
	if len(got) != 1 || string(got[0].Data) != "gamma" {
		t.Fatalf("truncation not replayed: %+v", got)
	}
}
