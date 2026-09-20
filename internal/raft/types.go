// Package raft implements the replicated log that gives Keystone a single,
// totally ordered sequence of state machine commands.
//
// The implementation follows the Raft paper (Ongaro & Ousterhout, 2014) with
// two production-oriented additions that matter for a control-plane store:
//
//   - Leader leases, so that strongly consistent reads can be served from the
//     leader's applied state without paying a round of log replication.
//   - Fast log backtracking (conflict index hints), so that a follower that has
//     fallen far behind converges in O(1) round trips instead of O(entries).
//
// The package has no third-party dependencies.
package raft

import (
	"context"
	"errors"
	"fmt"
)

// NodeID is the stable identity of a replica within a replication group.
type NodeID string

// EntryType discriminates log entries.
type EntryType uint8

const (
	// EntryNormal carries an opaque state machine command.
	EntryNormal EntryType = iota
	// EntryNoOp is appended by a new leader so that it can safely advance the
	// commit index for entries inherited from previous terms (Raft §5.4.2).
	EntryNoOp
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoOp:
		return "noop"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// Entry is a single record in the replicated log. Index is 1-based; index 0 is
// a synthetic sentinel with term 0 that always matches.
type Entry struct {
	Term  uint64    `json:"term"`
	Index uint64    `json:"index"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

// HardState is the subset of replica state that must survive a crash.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor NodeID `json:"voted_for"`
}

// State is the role of a replica in the current term.
type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// RequestVoteRequest is sent by candidates to gather votes (Raft §5.2).
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  NodeID `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the reply to a vote request.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest replicates log entries and doubles as a heartbeat.
type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     NodeID  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesResponse carries the follower's decision plus a hint that lets
// the leader skip straight past a divergent suffix.
type AppendEntriesResponse struct {
	Term uint64 `json:"term"`
	// Success is true when the follower accepted the entries.
	Success bool `json:"success"`
	// ConflictIndex is the first index the leader should retry from when
	// Success is false.
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	// LastIndex is the follower's last log index after applying the request.
	LastIndex uint64 `json:"last_index"`
}

// Transport delivers RPCs to peers. Implementations must be safe for
// concurrent use and must respect context cancellation.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

// FSM is the replicated state machine. Apply is invoked exactly once per
// committed entry, in log order, from a single goroutine.
type FSM interface {
	Apply(entry Entry) any
}

// Errors returned by the node.
var (
	// ErrNotLeader is returned when a write is proposed to a replica that does
	// not currently believe it is the leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrLeadershipLost is returned when a proposal was accepted into the local
	// log but the entry was overwritten before it committed. The caller must
	// treat the outcome as unknown and retry idempotently.
	ErrLeadershipLost = errors.New("raft: leadership lost before commit")
	// ErrNoLease is returned when a lease read is attempted without a valid
	// leader lease.
	ErrNoLease = errors.New("raft: leader lease not held")
	// ErrStopped is returned once the node has been shut down.
	ErrStopped = errors.New("raft: node stopped")
)

// NotLeaderError carries a redirect hint so clients can retry against the
// current leader instead of blindly round-robining.
type NotLeaderError struct {
	Leader NodeID
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not leader (leader unknown)"
	}
	return fmt.Sprintf("raft: not leader (leader is %s)", e.Leader)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// Status is a point-in-time snapshot of replica state, used by /v1/status and
// by the metrics exporter.
type Status struct {
	ID          NodeID            `json:"id"`
	State       string            `json:"state"`
	Term        uint64            `json:"term"`
	Leader      NodeID            `json:"leader"`
	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	LastIndex   uint64            `json:"last_index"`
	LeaseValid  bool              `json:"lease_valid"`
	MatchIndex  map[NodeID]uint64 `json:"match_index,omitempty"`
}
