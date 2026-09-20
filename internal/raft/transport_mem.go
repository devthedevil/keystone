package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// ErrUnreachable simulates a network failure.
var ErrUnreachable = errors.New("raft: peer unreachable")

// Network is an in-process transport fabric with fault injection. It is the
// backbone of the chaos tests: partitions, one-way loss and latency can be
// introduced deterministically, without touching the OS network stack.
type Network struct {
	mu        sync.RWMutex
	nodes     map[NodeID]*Node
	group     map[NodeID]int // replicas in different groups cannot communicate
	latency   time.Duration
	jitter    time.Duration
	dropRate  float64
	rnd       *rand.Rand
	rpcCount  uint64
	dropCount uint64
}

// NewNetwork creates an empty fabric.
func NewNetwork() *Network {
	return &Network{
		nodes: map[NodeID]*Node{},
		group: map[NodeID]int{},
		rnd:   rand.New(rand.NewSource(1)),
	}
}

// Register attaches a replica to the fabric.
func (nw *Network) Register(id NodeID, n *Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = n
	nw.group[id] = 0
}

// Partition splits the fabric: replicas listed in different groups can no
// longer exchange RPCs. Replicas not named stay in group 0.
func (nw *Network) Partition(groups ...[]NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.group {
		nw.group[id] = 0
	}
	for gi, g := range groups {
		for _, id := range g {
			nw.group[id] = gi + 1
		}
	}
}

// Isolate cuts a single replica off from every other replica.
func (nw *Network) Isolate(id NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	max := 0
	for _, g := range nw.group {
		if g > max {
			max = g
		}
	}
	nw.group[id] = max + 1
}

// Heal restores full connectivity.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.group {
		nw.group[id] = 0
	}
	nw.dropRate = 0
}

// SetLatency injects a fixed delay plus uniform jitter on every RPC.
func (nw *Network) SetLatency(d, jitter time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.latency, nw.jitter = d, jitter
}

// SetDropRate drops a fraction of RPCs uniformly at random.
func (nw *Network) SetDropRate(p float64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dropRate = p
}

// Stats returns delivered and dropped RPC counts.
func (nw *Network) Stats() (rpcs, drops uint64) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.rpcCount, nw.dropCount
}

func (nw *Network) route(from, to NodeID) (*Node, time.Duration, error) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.rpcCount++
	if nw.group[from] != nw.group[to] {
		nw.dropCount++
		return nil, 0, ErrUnreachable
	}
	if nw.dropRate > 0 && nw.rnd.Float64() < nw.dropRate {
		nw.dropCount++
		return nil, 0, ErrUnreachable
	}
	n, ok := nw.nodes[to]
	if !ok {
		nw.dropCount++
		return nil, 0, ErrUnreachable
	}
	d := nw.latency
	if nw.jitter > 0 {
		d += time.Duration(nw.rnd.Int63n(int64(nw.jitter)))
	}
	return n, d, nil
}

// Transport returns the Transport a given replica should use.
func (nw *Network) Transport(id NodeID) Transport { return &memTransport{nw: nw, from: id} }

type memTransport struct {
	nw   *Network
	from NodeID
}

func (t *memTransport) delay(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (t *memTransport) RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	peer, d, err := t.nw.route(t.from, to)
	if err != nil {
		return nil, err
	}
	if err := t.delay(ctx, d); err != nil {
		return nil, err
	}
	resp := peer.HandleRequestVote(req)
	if err := t.delay(ctx, d); err != nil {
		return nil, err
	}
	// Re-check connectivity on the return path so healed partitions do not
	// deliver stale replies through a still-broken link.
	if _, _, err := t.nw.route(to, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *memTransport) AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	peer, d, err := t.nw.route(t.from, to)
	if err != nil {
		return nil, err
	}
	if err := t.delay(ctx, d); err != nil {
		return nil, err
	}
	resp := peer.HandleAppendEntries(req)
	if err := t.delay(ctx, d); err != nil {
		return nil, err
	}
	if _, _, err := t.nw.route(to, t.from); err != nil {
		return nil, err
	}
	return resp, nil
}
