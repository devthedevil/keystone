package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Config configures a replica.
type Config struct {
	ID        NodeID
	Peers     []NodeID // other members of the group, excluding ID
	Storage   Storage
	Transport Transport
	FSM       FSM

	// HeartbeatInterval is how often a leader sends AppendEntries.
	HeartbeatInterval time.Duration
	// ElectionTimeout is the base follower timeout. The effective timeout is
	// randomised in [ElectionTimeout, 2*ElectionTimeout) to avoid split votes.
	ElectionTimeout time.Duration
	// LeaseFactor scales the read lease relative to ElectionTimeout. It must be
	// below 1.0 to leave headroom for clock drift between replicas.
	LeaseFactor float64

	Logger *slog.Logger
	// Rand seeds election jitter; nil uses a time-seeded source.
	Rand *rand.Rand
}

func (c *Config) withDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 10 * c.HeartbeatInterval
	}
	if c.LeaseFactor <= 0 || c.LeaseFactor >= 1 {
		c.LeaseFactor = 0.8
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(c.ID))))
	}
}

type pending struct {
	term uint64
	ch   chan proposalResult
}

type proposalResult struct {
	index  uint64
	result any
	err    error
}

// Node is a single Raft replica.
type Node struct {
	cfg Config
	log *slog.Logger

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    NodeID
	leader      NodeID
	commitIndex uint64
	lastApplied uint64

	electionDeadline time.Time
	nextHeartbeat    time.Time

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	ackTime    map[NodeID]time.Time
	inflight   map[NodeID]bool

	// leaderStart is the index of the no-op entry appended when this replica
	// became leader. Reads are only safe once it has been applied, because only
	// then is the leader guaranteed to have learned every previously committed
	// entry (Raft §8).
	leaderStart uint64

	pendingProps map[uint64]pending

	appliedWait chan struct{} // closed and replaced whenever lastApplied advances

	applySignal chan struct{}
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	stopped     bool

	// observability counters
	elections    uint64
	appliedTotal uint64
}

// NewNode creates a replica from durable state. Call Start to begin ticking.
func NewNode(cfg Config) (*Node, error) {
	cfg.withDefaults()
	if cfg.Storage == nil || cfg.Transport == nil || cfg.FSM == nil {
		return nil, errors.New("raft: Storage, Transport and FSM are required")
	}
	hs, err := cfg.Storage.LoadHardState()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:          cfg,
		log:          cfg.Logger.With("component", "raft", "node", string(cfg.ID)),
		state:        Follower,
		currentTerm:  hs.Term,
		votedFor:     hs.VotedFor,
		nextIndex:    map[NodeID]uint64{},
		matchIndex:   map[NodeID]uint64{},
		ackTime:      map[NodeID]time.Time{},
		inflight:     map[NodeID]bool{},
		pendingProps: map[uint64]pending{},
		appliedWait:  make(chan struct{}),
		applySignal:  make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	n.resetElectionDeadlineLocked()
	return n, nil
}

// Start launches the background loops.
func (n *Node) Start() {
	n.wg.Add(2)
	go n.runLoop()
	go n.applyLoop()
}

// Stop shuts the replica down and waits for its goroutines.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.stopped = true
		n.mu.Unlock()
		close(n.stopCh)
	})
	n.wg.Wait()
}

func (n *Node) quorum() int { return (len(n.cfg.Peers)+1)/2 + 1 }

func (n *Node) resetElectionDeadlineLocked() {
	base := n.cfg.ElectionTimeout
	jitter := time.Duration(n.cfg.Rand.Int63n(int64(base)))
	n.electionDeadline = time.Now().Add(base + jitter)
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

func (n *Node) runLoop() {
	defer n.wg.Done()
	tick := n.cfg.HeartbeatInterval / 2
	if tick < 5*time.Millisecond {
		tick = 5 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	now := time.Now()
	switch n.state {
	case Leader:
		if now.After(n.nextHeartbeat) {
			n.nextHeartbeat = now.Add(n.cfg.HeartbeatInterval)
			n.broadcastAppendLocked()
		}
	default:
		if now.After(n.electionDeadline) {
			n.startElectionLocked()
		}
	}
	n.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------------

func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.cfg.ID
	n.leader = ""
	n.elections++
	n.resetElectionDeadlineLocked()
	term := n.currentTerm
	if err := n.cfg.Storage.SaveHardState(HardState{Term: term, VotedFor: n.cfg.ID}); err != nil {
		n.log.Error("persist hard state failed", "err", err)
		return
	}
	lastIdx, lastTerm := n.cfg.Storage.Last()
	n.log.Info("starting election", "term", term, "last_index", lastIdx)

	req := &RequestVoteRequest{Term: term, CandidateID: n.cfg.ID, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
	votes := 1
	var voteMu sync.Mutex

	for _, peer := range n.cfg.Peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				return
			}
			if n.state != Candidate || n.currentTerm != term || !resp.VoteGranted {
				return
			}
			voteMu.Lock()
			votes++
			got := votes
			voteMu.Unlock()
			if got >= n.quorum() {
				n.becomeLeaderLocked()
			}
		}()
	}
	// Single-replica groups elect themselves immediately.
	if n.quorum() == 1 {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader NodeID) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.cfg.Storage.SaveHardState(HardState{Term: term, VotedFor: ""}); err != nil {
			n.log.Error("persist hard state failed", "err", err)
		}
	}
	if n.state == Leader {
		n.log.Info("stepping down", "term", term)
		n.failPendingLocked(ErrLeadershipLost)
	}
	n.state = Follower
	n.leader = leader
	n.leaderStart = 0
	n.resetElectionDeadlineLocked()
}

func (n *Node) becomeLeaderLocked() {
	if n.state == Leader {
		return
	}
	n.state = Leader
	n.leader = n.cfg.ID
	lastIdx, _ := n.cfg.Storage.Last()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
		n.ackTime[p] = time.Time{}
	}
	n.log.Info("became leader", "term", n.currentTerm, "last_index", lastIdx)

	// Commit a no-op so that entries from previous terms become committable and
	// lease reads become safe.
	noop := Entry{Term: n.currentTerm, Index: lastIdx + 1, Type: EntryNoOp}
	if err := n.cfg.Storage.Append([]Entry{noop}); err != nil {
		n.log.Error("append no-op failed", "err", err)
		return
	}
	n.leaderStart = noop.Index
	n.nextHeartbeat = time.Now().Add(n.cfg.HeartbeatInterval)
	n.maybeAdvanceCommitLocked()
	n.broadcastAppendLocked()
}

// HandleRequestVote processes an incoming vote request.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}
	// A leader with a live lease refuses votes: this prevents a replica that was
	// merely partitioned from disrupting a healthy leader (Raft §6, "pre-vote"
	// in spirit).
	if n.state == Leader && n.leaseValidLocked() {
		return &RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	granted := false
	if n.votedFor == "" || n.votedFor == req.CandidateID {
		lastIdx, lastTerm := n.cfg.Storage.Last()
		upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
		if upToDate {
			granted = true
			n.votedFor = req.CandidateID
			if err := n.cfg.Storage.SaveHardState(HardState{Term: n.currentTerm, VotedFor: n.votedFor}); err != nil {
				n.log.Error("persist vote failed", "err", err)
				granted = false
			} else {
				n.resetElectionDeadlineLocked()
			}
		}
	}
	return &RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted}
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

const maxEntriesPerAppend = 256

func (n *Node) broadcastAppendLocked() {
	for _, p := range n.cfg.Peers {
		n.sendAppendLocked(p)
	}
}

func (n *Node) sendAppendLocked(peer NodeID) {
	if n.state != Leader || n.inflight[peer] {
		return
	}
	next := n.nextIndex[peer]
	if next == 0 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm, ok := n.cfg.Storage.Term(prevIdx)
	if !ok {
		prevIdx, prevTerm = 0, 0
		next = 1
	}
	entries, err := n.cfg.Storage.Entries(next, next+maxEntriesPerAppend)
	if err != nil {
		n.log.Error("read log failed", "err", err)
		return
	}
	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	sentAt := time.Now()
	n.inflight[peer] = true

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
		defer cancel()
		resp, err := n.cfg.Transport.AppendEntries(ctx, peer, req)

		n.mu.Lock()
		defer n.mu.Unlock()
		n.inflight[peer] = false
		if err != nil {
			return
		}
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			return
		}
		if n.state != Leader || n.currentTerm != term {
			return
		}
		n.ackTime[peer] = sentAt
		if resp.Success {
			if len(req.Entries) > 0 {
				last := req.Entries[len(req.Entries)-1].Index
				if last > n.matchIndex[peer] {
					n.matchIndex[peer] = last
				}
				n.nextIndex[peer] = n.matchIndex[peer] + 1
			}
			n.maybeAdvanceCommitLocked()
			// Keep streaming if the follower is still behind.
			if lastIdx, _ := n.cfg.Storage.Last(); n.nextIndex[peer] <= lastIdx {
				n.sendAppendLocked(peer)
			}
			return
		}
		// Fast backtracking using the follower's hint.
		if resp.ConflictIndex > 0 {
			n.nextIndex[peer] = resp.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.sendAppendLocked(peer)
	}()
}

// HandleAppendEntries processes replication and heartbeat traffic.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	lastIdx, _ := n.cfg.Storage.Last()
	if req.Term < n.currentTerm {
		return &AppendEntriesResponse{Term: n.currentTerm, Success: false, LastIndex: lastIdx}
	}
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leader = req.LeaderID
	n.resetElectionDeadlineLocked()

	// Consistency check.
	myTerm, ok := n.cfg.Storage.Term(req.PrevLogIndex)
	if !ok {
		return &AppendEntriesResponse{Term: n.currentTerm, Success: false, ConflictIndex: lastIdx + 1, LastIndex: lastIdx}
	}
	if myTerm != req.PrevLogTerm {
		// Skip the whole conflicting term in one round trip.
		first := req.PrevLogIndex
		for first > 1 {
			if t, ok := n.cfg.Storage.Term(first - 1); !ok || t != myTerm {
				break
			}
			first--
		}
		return &AppendEntriesResponse{Term: n.currentTerm, Success: false, ConflictIndex: first, LastIndex: lastIdx}
	}

	// Append, truncating only on a genuine conflict.
	for i, e := range req.Entries {
		if t, ok := n.cfg.Storage.Term(e.Index); ok {
			if t == e.Term {
				continue // already have it
			}
			if err := n.cfg.Storage.TruncateFrom(e.Index); err != nil {
				n.log.Error("truncate failed", "err", err)
				return &AppendEntriesResponse{Term: n.currentTerm, Success: false, LastIndex: lastIdx}
			}
		}
		if err := n.cfg.Storage.Append(req.Entries[i:]); err != nil {
			n.log.Error("append failed", "err", err)
			return &AppendEntriesResponse{Term: n.currentTerm, Success: false, LastIndex: lastIdx}
		}
		break
	}

	lastIdx, _ = n.cfg.Storage.Last()
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(req.LeaderCommit, lastIdx)
		n.signalApplyLocked()
	}
	return &AppendEntriesResponse{Term: n.currentTerm, Success: true, LastIndex: lastIdx}
}

func (n *Node) maybeAdvanceCommitLocked() {
	if n.state != Leader {
		return
	}
	lastIdx, _ := n.cfg.Storage.Last()
	for idx := lastIdx; idx > n.commitIndex; idx-- {
		term, ok := n.cfg.Storage.Term(idx)
		if !ok {
			continue
		}
		// Raft §5.4.2: a leader may only count replicas for entries from its
		// own term. Earlier entries commit transitively.
		if term != n.currentTerm {
			break
		}
		count := 1 // self
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.signalApplyLocked()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

func (n *Node) signalApplyLocked() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			n.mu.Lock()
			n.failPendingLocked(ErrStopped)
			n.mu.Unlock()
			return
		case <-n.applySignal:
			n.drainApply()
		case <-time.After(20 * time.Millisecond):
			n.drainApply()
		}
	}
}

func (n *Node) drainApply() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		idx := n.lastApplied + 1
		ents, err := n.cfg.Storage.Entries(idx, idx+1)
		if err != nil || len(ents) == 0 {
			n.mu.Unlock()
			return
		}
		entry := ents[0]
		p, hasPending := n.pendingProps[idx]
		if hasPending {
			delete(n.pendingProps, idx)
		}
		n.mu.Unlock()

		var result any
		if entry.Type == EntryNormal {
			result = n.cfg.FSM.Apply(entry)
		}

		n.mu.Lock()
		n.lastApplied = idx
		n.appliedTotal++
		close(n.appliedWait)
		n.appliedWait = make(chan struct{})
		n.mu.Unlock()

		if hasPending {
			if p.term != entry.Term {
				p.ch <- proposalResult{index: idx, err: ErrLeadershipLost}
			} else {
				p.ch <- proposalResult{index: idx, result: result}
			}
		}
	}
}

func (n *Node) failPendingLocked(err error) {
	for idx, p := range n.pendingProps {
		p.ch <- proposalResult{index: idx, err: err}
		delete(n.pendingProps, idx)
	}
}

// ---------------------------------------------------------------------------
// Client surface
// ---------------------------------------------------------------------------

// Propose replicates data and blocks until the entry is applied by this
// replica's state machine, returning whatever the FSM returned.
//
// A non-nil error does not always mean the command did not take effect: on
// ErrLeadershipLost the outcome is genuinely unknown, which is why every
// Keystone command carries a client-supplied idempotency key.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.state != Leader {
		leader := n.leader
		n.mu.Unlock()
		return nil, &NotLeaderError{Leader: leader}
	}
	lastIdx, _ := n.cfg.Storage.Last()
	entry := Entry{Term: n.currentTerm, Index: lastIdx + 1, Type: EntryNormal, Data: data}
	if err := n.cfg.Storage.Append([]Entry{entry}); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	ch := make(chan proposalResult, 1)
	n.pendingProps[entry.Index] = pending{term: entry.Term, ch: ch}
	n.maybeAdvanceCommitLocked() // single-replica groups commit immediately
	n.broadcastAppendLocked()
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pendingProps, entry.Index)
		n.mu.Unlock()
		return nil, ctx.Err()
	case res := <-ch:
		return res.result, res.err
	}
}

// leaseValidLocked reports whether a quorum has acknowledged this leader
// recently enough that no other replica can have started an election.
func (n *Node) leaseValidLocked() bool {
	if n.state != Leader {
		return false
	}
	if n.quorum() == 1 {
		return true
	}
	acks := make([]time.Time, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if t, ok := n.ackTime[p]; ok && !t.IsZero() {
			acks = append(acks, t)
		}
	}
	// Need quorum-1 peer acknowledgements (self counts as one).
	need := n.quorum() - 1
	if len(acks) < need {
		return false
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i].After(acks[j]) })
	quorumAck := acks[need-1]
	lease := time.Duration(float64(n.cfg.ElectionTimeout) * n.cfg.LeaseFactor)
	return time.Now().Before(quorumAck.Add(lease))
}

// ReadLease blocks until this replica can serve a strongly consistent read:
// it is the leader, it holds a valid lease, and it has applied everything that
// was committed when it took office. Reads taken after ReadLease returns are
// linearizable.
func (n *Node) ReadLease(ctx context.Context) error {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return ErrStopped
		}
		if n.state != Leader {
			leader := n.leader
			n.mu.Unlock()
			return &NotLeaderError{Leader: leader}
		}
		if !n.leaseValidLocked() {
			n.mu.Unlock()
			return ErrNoLease
		}
		if n.lastApplied >= n.leaderStart && n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return nil
		}
		wait := n.appliedWait
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		case <-wait:
		}
	}
}

// WaitApplied blocks until the replica has applied at least idx. Followers use
// this to serve bounded-staleness reads at a caller-specified index.
func (n *Node) WaitApplied(ctx context.Context, idx uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= idx {
			n.mu.Unlock()
			return nil
		}
		wait := n.appliedWait
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		case <-wait:
		}
	}
}

// IsLeader reports leadership without lease validation.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// Leader returns the last known leader, which may be stale.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// LastApplied returns the highest applied log index.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// Status returns a snapshot for status endpoints and metrics.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	lastIdx, _ := n.cfg.Storage.Last()
	mi := make(map[NodeID]uint64, len(n.matchIndex))
	for k, v := range n.matchIndex {
		mi[k] = v
	}
	return Status{
		ID:          n.cfg.ID,
		State:       n.state.String(),
		Term:        n.currentTerm,
		Leader:      n.leader,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   lastIdx,
		LeaseValid:  n.leaseValidLocked(),
		MatchIndex:  mi,
	}
}

// Campaign forces an immediate election. It exists for tests and for operator
// tooling that drains a host before a deployment.
func (n *Node) Campaign() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		n.startElectionLocked()
	}
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// StepDown relinquishes leadership so another replica can take over. It is the
// first step of a safe deployment: drain leadership, verify a new leader has
// taken office, then restart the process.
//
// This is a voluntary abdication rather than a true leadership transfer: the
// replica simply stops acting as leader and lets the normal election run. A
// full transfer (Raft §3.10) would first bring the target fully up to date and
// then hand it a timeout-now message, which is tracked in docs/roadmap.md.
func (n *Node) StepDown() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return false
	}
	n.log.Info("stepping down on operator request", "term", n.currentTerm)
	n.state = Follower
	n.leader = ""
	n.leaderStart = 0
	n.resetElectionDeadlineLocked()
	return true
}
