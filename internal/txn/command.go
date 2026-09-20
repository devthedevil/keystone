package txn

import (
	"encoding/json"
	"sort"
)

// OpType discriminates replicated commands.
type OpType string

const (
	// OpCommit validates and applies a transaction.
	OpCommit OpType = "commit"
	// OpGC advances the garbage collection watermark.
	//
	// Collection is replicated rather than run locally on each replica so that
	// every replica's state is byte-identical at a given log index. Local GC
	// would make replicas diverge in what old snapshots they can serve, which
	// turns a follower read into a coin flip.
	OpGC OpType = "gc"
)

// CondType is a precondition kind.
type CondType string

const (
	// CondExists requires the key to be visible.
	CondExists CondType = "exists"
	// CondAbsent requires the key to be absent or tombstoned.
	CondAbsent CondType = "absent"
	// CondVersion requires the key's latest version to equal Version.
	CondVersion CondType = "version"
)

// Condition is a caller-declared precondition, evaluated at apply time.
// Preconditions are how callers express compare-and-swap without holding a
// lock across a network round trip.
type Condition struct {
	Key     string   `json:"key"`
	Type    CondType `json:"type"`
	Version uint64   `json:"version,omitempty"`
}

// ReadRef records a key the transaction read, with the version it observed.
type ReadRef struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
}

// RangeRef records a range the transaction scanned. Validating ranges as well
// as point reads is what prevents phantoms: a transaction that listed a
// compartment and then acted on the result is aborted if anything appeared in
// or disappeared from that range before it committed.
type RangeRef struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// Mutation is a single buffered write.
type Mutation struct {
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

// Command is the unit replicated through Raft.
type Command struct {
	Op     OpType `json:"op"`
	Tenant string `json:"tenant,omitempty"`

	// TxnID is a client-supplied idempotency key. Because a proposal can time
	// out after it was actually committed, retries are unavoidable; the engine
	// deduplicates on this key so a retry is a no-op rather than a double
	// write.
	TxnID string `json:"txn_id,omitempty"`

	// ReadTS is the snapshot the transaction read at.
	ReadTS     uint64      `json:"read_ts,omitempty"`
	Reads      []ReadRef   `json:"reads,omitempty"`
	Ranges     []RangeRef  `json:"ranges,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
	Mutations  []Mutation  `json:"mutations,omitempty"`

	// Watermark is used by OpGC.
	Watermark uint64 `json:"watermark,omitempty"`
}

// normalize sorts the command's collections so that two logically identical
// transactions serialise to identical bytes. This keeps the replicated log
// diffable and makes command hashing meaningful for debugging.
func (c *Command) normalize() {
	sort.Slice(c.Reads, func(i, j int) bool { return c.Reads[i].Key < c.Reads[j].Key })
	sort.Slice(c.Mutations, func(i, j int) bool { return c.Mutations[i].Key < c.Mutations[j].Key })
	sort.Slice(c.Conditions, func(i, j int) bool {
		if c.Conditions[i].Key == c.Conditions[j].Key {
			return c.Conditions[i].Type < c.Conditions[j].Type
		}
		return c.Conditions[i].Key < c.Conditions[j].Key
	})
	sort.Slice(c.Ranges, func(i, j int) bool {
		if c.Ranges[i].Start == c.Ranges[j].Start {
			return c.Ranges[i].End < c.Ranges[j].End
		}
		return c.Ranges[i].Start < c.Ranges[j].Start
	})
}

// Encode serialises the command for the replicated log.
func (c *Command) Encode() ([]byte, error) {
	c.normalize()
	return json.Marshal(c)
}

// Decode parses a replicated command.
func Decode(b []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// ConflictKind explains why a transaction aborted.
type ConflictKind string

const (
	ConflictReadWrite  ConflictKind = "read_write"
	ConflictPhantom    ConflictKind = "phantom"
	ConflictCondition  ConflictKind = "condition"
	ConflictThrottled  ConflictKind = "throttled"
	ConflictReadTooOld ConflictKind = "read_snapshot_collected"
)

// Conflict describes an aborted transaction precisely enough for a client to
// decide whether to retry or to surface an error to its own caller.
type Conflict struct {
	Kind     ConflictKind `json:"kind"`
	Key      string       `json:"key,omitempty"`
	Expected uint64       `json:"expected_version,omitempty"`
	Observed uint64       `json:"observed_version,omitempty"`
}

// Result is the outcome of applying a command.
type Result struct {
	Committed bool      `json:"committed"`
	CommitTS  uint64    `json:"commit_ts,omitempty"`
	Conflict  *Conflict `json:"conflict,omitempty"`
	// Duplicate is true when the transaction had already been applied and the
	// cached outcome was returned.
	Duplicate bool `json:"duplicate,omitempty"`
	// GC carries the outcome of an OpGC command.
	GC *GCReport `json:"gc,omitempty"`
}

// GCReport summarises a collection pass.
type GCReport struct {
	Watermark     uint64 `json:"watermark"`
	VersionsFreed int    `json:"versions_freed"`
	KeysRemoved   int    `json:"keys_removed"`
	BytesFreed    int    `json:"bytes_freed"`
}
