// Package api defines the wire contract between Keystone servers and clients.
//
// The contract is deliberately explicit about failure: every error carries a
// machine-readable code, and retryable errors carry the information a client
// needs to retry correctly (how long to wait, and where the leader is).
// Clients that have to parse prose to decide whether to retry end up guessing,
// and guessing at a control-plane storage layer produces duplicate resources.
package api

import "time"

// Error codes returned in ErrorBody.Code.
const (
	// CodeNotLeader means this replica cannot serve writes or linearizable
	// reads. Retry against Leader.
	CodeNotLeader = "not_leader"
	// CodeNoLease means the replica believes it is the leader but cannot prove
	// it right now. Retry; do not fail over on this alone.
	CodeNoLease = "no_lease"
	// CodeConflict means the transaction lost an optimistic race. Retry with a
	// fresh snapshot.
	CodeConflict = "conflict"
	// CodePrecondition means a caller-supplied condition did not hold. Do not
	// blindly retry: the caller's assumption was wrong.
	CodePrecondition = "precondition_failed"
	// CodeThrottled means the tenant is over quota. Retry after RetryAfterMS.
	CodeThrottled = "throttled"
	// CodeOverloaded means the service shed this request to protect itself.
	CodeOverloaded = "overloaded"
	// CodeNotFound means the key has no visible version.
	CodeNotFound = "not_found"
	// CodeInvalid means the request was malformed.
	CodeInvalid = "invalid_request"
	// CodeInternal means an unexpected server-side failure.
	CodeInternal = "internal"
	// CodeUnavailable means the replica is not ready to serve.
	CodeUnavailable = "unavailable"
)

// ErrorBody is the envelope for every non-2xx response.
type ErrorBody struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
	Leader       string `json:"leader,omitempty"`
	// LeaderAddr is the leader's client-plane base URL when the replica knows
	// it. A node ID alone is only useful to a client that was configured with
	// every member; an address lets a client bootstrapped from one endpoint
	// still find the leader.
	LeaderAddr string `json:"leader_addr,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	// Retryable tells a generic client whether retrying the identical request
	// can succeed.
	Retryable bool `json:"retryable"`
}

// ErrorResponse wraps ErrorBody.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// KV is a key/value pair with its commit version.
type KV struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
}

// GetResponse is the result of a point read.
type GetResponse struct {
	KV
	// ReadTS is the snapshot the read was served from. Passing it back into a
	// later request is how a client pins a consistent view across calls.
	ReadTS uint64 `json:"read_ts"`
}

// RangeResponse is the result of a range read.
type RangeResponse struct {
	KVs         []KV   `json:"kvs"`
	More        bool   `json:"more"`
	ReadTS      uint64 `json:"read_ts"`
	ScannedKeys int    `json:"scanned_keys"`
	// NextStart is the key to resume from when More is true.
	NextStart string `json:"next_start,omitempty"`
}

// CondType mirrors the engine's precondition kinds.
type CondType string

const (
	CondExists  CondType = "exists"
	CondAbsent  CondType = "absent"
	CondVersion CondType = "version"
)

// Condition is a precondition evaluated atomically at commit.
type Condition struct {
	Key     string   `json:"key"`
	Type    CondType `json:"type"`
	Version uint64   `json:"version,omitempty"`
}

// Mutation is a write or delete.
type Mutation struct {
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

// ReadRef lets a client submit its own read set, which is how an externally
// coordinated read/modify/write becomes serializable without holding a
// server-side transaction open.
type ReadRef struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
}

// RangeRef is a scanned range submitted for phantom validation.
type RangeRef struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// TxnRequest is a one-shot transaction.
type TxnRequest struct {
	// TxnID is the idempotency key. Clients should generate it once and reuse
	// it across retries of the same logical operation.
	TxnID      string      `json:"txn_id"`
	ReadTS     uint64      `json:"read_ts,omitempty"`
	Reads      []ReadRef   `json:"reads,omitempty"`
	Ranges     []RangeRef  `json:"ranges,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
	Mutations  []Mutation  `json:"mutations,omitempty"`
}

// Conflict explains an abort.
type Conflict struct {
	Kind     string `json:"kind"`
	Key      string `json:"key,omitempty"`
	Expected uint64 `json:"expected_version,omitempty"`
	Observed uint64 `json:"observed_version,omitempty"`
}

// TxnResponse is the outcome of a transaction.
type TxnResponse struct {
	Committed bool      `json:"committed"`
	CommitTS  uint64    `json:"commit_ts,omitempty"`
	Conflict  *Conflict `json:"conflict,omitempty"`
	Duplicate bool      `json:"duplicate,omitempty"`
}

// StatusResponse is the operational view of a replica.
type StatusResponse struct {
	NodeID      string            `json:"node_id"`
	State       string            `json:"state"`
	Term        uint64            `json:"term"`
	Leader      string            `json:"leader"`
	LeaderAddr  string            `json:"leader_addr,omitempty"`
	LeaseValid  bool              `json:"lease_valid"`
	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	MatchIndex  map[string]uint64 `json:"match_index,omitempty"`

	Keys            int     `json:"keys"`
	Versions        int     `json:"versions"`
	ApproxBytes     int     `json:"approx_bytes"`
	Commits         uint64  `json:"commits"`
	Aborts          uint64  `json:"aborts"`
	OpenSnapshots   int     `json:"open_snapshots"`
	GCWatermark     uint64  `json:"gc_watermark"`
	Watchers        int     `json:"stream_watchers"`
	AdmissionHealth float64 `json:"admission_health"`

	Version string        `json:"version"`
	Uptime  time.Duration `json:"uptime_ns"`
}

// TenantQuota is the per-tenant policy exposed by the admin API.
type TenantQuota struct {
	Tenant        string  `json:"tenant"`
	RatePerSec    float64 `json:"rate_per_sec"`
	Burst         float64 `json:"burst"`
	MaxConcurrent int     `json:"max_concurrent"`
}

// GCReport summarises a replicated garbage collection pass.
type GCReport struct {
	Watermark     uint64 `json:"watermark"`
	VersionsFreed int    `json:"versions_freed"`
	KeysRemoved   int    `json:"keys_removed"`
	BytesFreed    int    `json:"bytes_freed"`
}

// GCResponse is the outcome of POST /v1/admin/gc. It is a transaction result
// because collection is replicated like any other write: every replica runs
// the identical pass at the identical log index, so replicas never diverge on
// what they have forgotten.
type GCResponse struct {
	Committed bool      `json:"committed"`
	CommitTS  uint64    `json:"commit_ts,omitempty"`
	GC        *GCReport `json:"gc,omitempty"`
}

// TenantStats is the live admission-control view of one tenant.
type TenantStats struct {
	Tenant      string  `json:"tenant"`
	Tokens      float64 `json:"tokens"`
	InFlight    int     `json:"in_flight"`
	Admitted    uint64  `json:"admitted"`
	Throttled   uint64  `json:"throttled"`
	Shed        uint64  `json:"shed"`
	UnitsCharge float64 `json:"units_charged"`
}

// ChangeEvent is one entry of the change feed, delivered as server-sent events.
type ChangeEvent struct {
	Seq      uint64    `json:"seq"`
	Kind     string    `json:"kind"`
	Tenant   string    `json:"tenant,omitempty"`
	Key      string    `json:"key"`
	Value    []byte    `json:"value,omitempty"`
	TxnID    string    `json:"txn_id,omitempty"`
	WallTime time.Time `json:"wall_time"`
}
