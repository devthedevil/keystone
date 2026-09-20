package txn

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/raft"
	"github.com/devthedevil/keystone/internal/stream"
)

// Engine is the replicated state machine. Every replica runs the same Engine
// over the same log, so every replica reaches the same state.
//
// Determinism rules the implementation obeys:
//   - The commit timestamp is the Raft log index, never a wall clock.
//   - Conflict validation reads only replicated state.
//   - Garbage collection happens through a replicated command.
//   - The deduplication cache evicts in log order, not by wall-clock TTL.
//
// SystemPrefix is the reserved key space for replicated configuration. It is
// unreachable through the client API: tenant names must begin with an
// alphanumeric character, so no tenant can ever address a key beneath it.
const SystemPrefix = "__system/"

type Engine struct {
	store      *mvcc.Store
	broker     *stream.Broker
	log        *slog.Logger
	systemHook func(key string, value []byte, deleted bool)

	mu sync.RWMutex
	// dedupe maps an idempotency key to the outcome of its first application.
	dedupe map[string]Result
	// dedupeOrder preserves insertion order for deterministic eviction.
	dedupeOrder []string
	dedupeLimit int

	appliedIndex uint64

	stats Stats
}

// Stats are engine-level counters exported to /metrics.
type Stats struct {
	Commits        uint64 `json:"commits"`
	Aborts         uint64 `json:"aborts"`
	Duplicates     uint64 `json:"duplicates"`
	MutationsApply uint64 `json:"mutations_applied"`
	GCRuns         uint64 `json:"gc_runs"`
	AppliedIndex   uint64 `json:"applied_index"`
}

// EngineOptions configures the state machine.
type EngineOptions struct {
	// DedupeEntries bounds the idempotency cache. It must be large enough to
	// cover the longest plausible client retry window.
	DedupeEntries int
	// SystemHook is invoked, in log order, for every applied mutation under
	// SystemPrefix. It is how replicated configuration (tenant quotas, for
	// example) reaches the in-memory machinery that enforces it: the write
	// goes through the log like any other, so every replica converges on the
	// same configuration and a restart replays it for free.
	//
	// It must be fast and must not block: it runs on the apply loop.
	SystemHook func(key string, value []byte, deleted bool)
	Logger     *slog.Logger
}

// NewEngine builds a state machine over the given store and change broker.
func NewEngine(store *mvcc.Store, broker *stream.Broker, opts EngineOptions) *Engine {
	if opts.DedupeEntries <= 0 {
		opts.DedupeEntries = 65536
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{
		store:       store,
		broker:      broker,
		log:         opts.Logger.With("component", "engine"),
		systemHook:  opts.SystemHook,
		dedupe:      make(map[string]Result, 1024),
		dedupeLimit: opts.DedupeEntries,
	}
}

// Store exposes the underlying engine for reads.
func (e *Engine) Store() *mvcc.Store { return e.store }

// Broker exposes the change feed.
func (e *Engine) Broker() *stream.Broker { return e.broker }

// Stats returns engine counters.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := e.stats
	s.AppliedIndex = e.appliedIndex
	return s
}

// AppliedIndex is the highest log index folded into the state machine.
func (e *Engine) AppliedIndex() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.appliedIndex
}

// Apply implements raft.FSM. It is invoked from a single goroutine, in log
// order, on every replica.
func (e *Engine) Apply(entry raft.Entry) any {
	cmd, err := Decode(entry.Data)
	if err != nil {
		// A command that cannot be decoded is a bug or corruption. Skipping it
		// on one replica but not another would diverge state, so it is skipped
		// deterministically everywhere and loudly reported.
		e.log.Error("undecodable command: skipping deterministically", "index", entry.Index, "err", err)
		e.setApplied(entry.Index)
		return Result{Committed: false}
	}

	switch cmd.Op {
	case OpCommit:
		return e.applyCommit(cmd, entry.Index)
	case OpGC:
		return e.applyGC(cmd, entry.Index)
	default:
		e.setApplied(entry.Index)
		return Result{Committed: false}
	}
}

func (e *Engine) setApplied(idx uint64) {
	e.mu.Lock()
	e.appliedIndex = idx
	e.mu.Unlock()
}

func (e *Engine) applyCommit(cmd *Command, commitTS uint64) Result {
	// Idempotency check first: a duplicate must not be re-validated, because
	// re-validation would abort it (its read versions are now stale) and the
	// caller would wrongly conclude its write never happened.
	if cmd.TxnID != "" {
		e.mu.RLock()
		prev, seen := e.dedupe[cmd.TxnID]
		e.mu.RUnlock()
		if seen {
			e.mu.Lock()
			e.stats.Duplicates++
			e.appliedIndex = commitTS
			e.mu.Unlock()
			prev.Duplicate = true
			return prev
		}
	}

	if conflict := e.validate(cmd); conflict != nil {
		res := Result{Committed: false, Conflict: conflict}
		e.finish(cmd.TxnID, res, commitTS, false)
		return res
	}

	changes := make([]stream.Change, 0, len(cmd.Mutations))
	now := time.Now()
	for _, m := range cmd.Mutations {
		if e.systemHook != nil && strings.HasPrefix(m.Key, SystemPrefix) {
			e.systemHook(strings.TrimPrefix(m.Key, SystemPrefix), m.Value, m.Delete)
		}
		if m.Delete {
			e.store.Delete(m.Key, commitTS)
			changes = append(changes, stream.Change{
				Seq: commitTS, Kind: stream.KindDelete, Tenant: cmd.Tenant,
				Key: m.Key, TxnID: cmd.TxnID, WallTime: now,
			})
			continue
		}
		e.store.Put(m.Key, m.Value, commitTS)
		changes = append(changes, stream.Change{
			Seq: commitTS, Kind: stream.KindPut, Tenant: cmd.Tenant,
			Key: m.Key, Value: m.Value, TxnID: cmd.TxnID, WallTime: now,
		})
	}

	e.mu.Lock()
	e.stats.MutationsApply += uint64(len(cmd.Mutations))
	e.mu.Unlock()

	res := Result{Committed: true, CommitTS: commitTS}
	e.finish(cmd.TxnID, res, commitTS, true)

	if e.broker != nil && len(changes) > 0 {
		e.broker.Publish(changes...)
	}
	return res
}

// validate performs optimistic concurrency control at apply time. Running
// validation inside the state machine — rather than on the coordinator before
// proposing — is what makes the check atomic with respect to the commit: no
// other transaction can slip in between validation and application, because
// the log is a total order.
func (e *Engine) validate(cmd *Command) *Conflict {
	// Point reads: the version observed must still be the latest version.
	for _, r := range cmd.Reads {
		current := e.store.LatestVersion(r.Key)
		if current != r.Version {
			return &Conflict{Kind: ConflictReadWrite, Key: r.Key, Expected: r.Version, Observed: current}
		}
	}

	// Ranges: nothing in a scanned range may have changed after the snapshot.
	for _, rg := range cmd.Ranges {
		maxTS, _ := e.store.MaxVersionInRange(rg.Start, rg.End)
		if maxTS > cmd.ReadTS {
			return &Conflict{Kind: ConflictPhantom, Key: rg.Start, Expected: cmd.ReadTS, Observed: maxTS}
		}
	}

	// Explicit preconditions.
	for _, c := range cmd.Conditions {
		switch c.Type {
		case CondExists:
			if _, err := e.store.Get(c.Key, ^uint64(0)); err != nil {
				return &Conflict{Kind: ConflictCondition, Key: c.Key}
			}
		case CondAbsent:
			if _, err := e.store.Get(c.Key, ^uint64(0)); err == nil {
				return &Conflict{Kind: ConflictCondition, Key: c.Key, Observed: e.store.LatestVersion(c.Key)}
			}
		case CondVersion:
			if got := e.store.LatestVersion(c.Key); got != c.Version {
				return &Conflict{Kind: ConflictCondition, Key: c.Key, Expected: c.Version, Observed: got}
			}
		}
	}
	return nil
}

func (e *Engine) finish(txnID string, res Result, commitTS uint64, committed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.appliedIndex = commitTS
	if committed {
		e.stats.Commits++
	} else {
		e.stats.Aborts++
	}
	if txnID == "" {
		return
	}
	e.dedupe[txnID] = res
	e.dedupeOrder = append(e.dedupeOrder, txnID)
	for len(e.dedupeOrder) > e.dedupeLimit {
		oldest := e.dedupeOrder[0]
		e.dedupeOrder = e.dedupeOrder[1:]
		delete(e.dedupe, oldest)
	}
}

func (e *Engine) applyGC(cmd *Command, commitTS uint64) Result {
	r := e.store.GC(cmd.Watermark)
	e.mu.Lock()
	e.appliedIndex = commitTS
	e.stats.GCRuns++
	e.mu.Unlock()
	e.log.Debug("gc pass", "watermark", cmd.Watermark, "versions_freed", r.VersionsFreed, "keys_removed", r.KeysRemoved)
	return Result{Committed: true, CommitTS: commitTS, GC: &GCReport{
		Watermark:     r.Watermark,
		VersionsFreed: r.VersionsFreed,
		KeysRemoved:   r.KeysRemoved,
		BytesFreed:    r.BytesFreed,
	}}
}
