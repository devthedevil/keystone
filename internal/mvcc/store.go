// Package mvcc implements the multi-version storage engine underneath
// Keystone's transaction layer.
//
// Every key maps to a chain of immutable versions ordered by commit timestamp.
// A read at timestamp T sees the newest version with commitTS <= T, so readers
// never block writers and writers never block readers. Old versions are
// reclaimed by a watermark-driven collector rather than by reference counting,
// which keeps the hot path allocation-free.
package mvcc

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrNotFound is returned when a key has no visible version at the read
// timestamp.
var ErrNotFound = errors.New("mvcc: key not found")

// Version is one immutable revision of a key.
type Version struct {
	// CommitTS is the Raft log index at which this version committed. Using the
	// log index as the timestamp means version order is exactly commit order,
	// with no clock involved.
	CommitTS uint64 `json:"commit_ts"`
	Value    []byte `json:"value,omitempty"`
	// Tombstone marks a deletion. Tombstones are retained until the GC
	// watermark passes them so that snapshot reads below the watermark still
	// observe the deletion.
	Tombstone bool `json:"tombstone,omitempty"`
	// WallTime is advisory metadata for operators; it is never used for
	// ordering or visibility decisions.
	WallTime time.Time `json:"wall_time"`
}

// chain is the version list of a single key, ascending by CommitTS.
type chain struct {
	versions []Version
}

// visible returns the newest version at or below readTS.
func (c *chain) visible(readTS uint64) (Version, bool) {
	// Binary search for the first version with CommitTS > readTS.
	i := sort.Search(len(c.versions), func(i int) bool { return c.versions[i].CommitTS > readTS })
	if i == 0 {
		return Version{}, false
	}
	return c.versions[i-1], true
}

func (c *chain) latestTS() uint64 {
	if len(c.versions) == 0 {
		return 0
	}
	return c.versions[len(c.versions)-1].CommitTS
}

func (c *chain) bytes() int {
	n := 0
	for _, v := range c.versions {
		n += len(v.Value) + 24
	}
	return n
}

// KV is a materialised key/value pair returned by reads and scans.
type KV struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
}

// Stats describes engine occupancy, exported to metrics and used by the
// capacity planning runbook.
type Stats struct {
	Keys          int    `json:"keys"`
	Versions      int    `json:"versions"`
	ApproxBytes   int    `json:"approx_bytes"`
	Tombstones    int    `json:"tombstones"`
	GCRuns        uint64 `json:"gc_runs"`
	VersionsFreed uint64 `json:"versions_freed"`
	LastWatermark uint64 `json:"last_watermark"`
}

// Store is the multi-version key/value engine.
//
// Writes are applied from the Raft apply loop, which is single threaded, so
// write contention is structurally impossible; the mutex exists to make
// concurrent readers safe.
type Store struct {
	mu   sync.RWMutex
	idx  *skipList
	stat Stats
}

// New creates an empty store.
func New() *Store {
	return &Store{idx: newSkipList(time.Now().UnixNano())}
}

// Put appends a new version of key. commitTS must be greater than the key's
// current latest version; the transaction layer guarantees this by deriving
// commitTS from the Raft log index.
func (s *Store) Put(key string, value []byte, commitTS uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.idx.getOrCreate(key)
	if len(c.versions) == 0 {
		s.stat.Keys++
	}
	if c.latestTS() >= commitTS {
		// Defensive: out-of-order application would silently corrupt snapshot
		// reads, so it is treated as a no-op rather than an overwrite.
		return
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	c.versions = append(c.versions, Version{CommitTS: commitTS, Value: cp, WallTime: time.Now()})
	s.stat.Versions++
	s.stat.ApproxBytes += len(cp) + 24
}

// Delete appends a tombstone version.
func (s *Store) Delete(key string, commitTS uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.idx.get(key)
	if c == nil {
		return // deleting an absent key is a no-op, not a tombstone
	}
	if c.latestTS() >= commitTS {
		return
	}
	c.versions = append(c.versions, Version{CommitTS: commitTS, Tombstone: true, WallTime: time.Now()})
	s.stat.Versions++
	s.stat.Tombstones++
}

// Get returns the value visible at readTS.
func (s *Store) Get(key string, readTS uint64) (KV, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.idx.get(key)
	if c == nil {
		return KV{}, ErrNotFound
	}
	v, ok := c.visible(readTS)
	if !ok || v.Tombstone {
		return KV{}, ErrNotFound
	}
	return KV{Key: key, Value: v.Value, Version: v.CommitTS}, nil
}

// LatestVersion returns the newest commit timestamp for key, including
// tombstones, and zero if the key has never existed. Optimistic concurrency
// control uses this to detect write-write and read-write conflicts.
func (s *Store) LatestVersion(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.idx.get(key)
	if c == nil {
		return 0
	}
	return c.latestTS()
}

// VersionAt returns the commit timestamp visible at readTS, or zero. It is the
// version a transaction records in its read set.
func (s *Store) VersionAt(key string, readTS uint64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.idx.get(key)
	if c == nil {
		return 0
	}
	v, ok := c.visible(readTS)
	if !ok {
		return 0
	}
	return v.CommitTS
}

// ScanResult is a page of a range scan.
type ScanResult struct {
	KVs []KV
	// ScannedKeys counts keys examined, including ones filtered out by
	// visibility. It feeds the cost model so that an expensive scan over a
	// tombstone-heavy range is charged for the work it actually caused.
	ScannedKeys int
	// More indicates the limit truncated the result.
	More bool
}

// Scan returns visible keys in [start, end) at readTS, up to limit.
func (s *Store) Scan(start, end string, limit int, readTS uint64) ScanResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var res ScanResult
	if limit <= 0 {
		limit = 1000
	}
	s.idx.scan(start, end, func(key string, c *chain) bool {
		res.ScannedKeys++
		v, ok := c.visible(readTS)
		if !ok || v.Tombstone {
			return true
		}
		if len(res.KVs) == limit {
			res.More = true
			return false
		}
		res.KVs = append(res.KVs, KV{Key: key, Value: v.Value, Version: v.CommitTS})
		return true
	})
	return res
}

// MaxVersionInRange returns the newest commit timestamp of any key in
// [start, end), including tombstones, along with the number of keys examined.
//
// Serializable transactions use this to detect phantoms: if anything in a
// scanned range changed after the transaction's snapshot, the transaction must
// abort. The cost is proportional to the number of keys in the range, which is
// why the cost model charges range validation explicitly.
func (s *Store) MaxVersionInRange(start, end string) (maxTS uint64, scanned int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.idx.scan(start, end, func(_ string, c *chain) bool {
		scanned++
		if ts := c.latestTS(); ts > maxTS {
			maxTS = ts
		}
		return true
	})
	return maxTS, scanned
}

// GCResult reports what a collection pass reclaimed.
type GCResult struct {
	Watermark     uint64 `json:"watermark"`
	VersionsFreed int    `json:"versions_freed"`
	KeysRemoved   int    `json:"keys_removed"`
	BytesFreed    int    `json:"bytes_freed"`
	Duration      string `json:"duration"`
}

// GC reclaims versions that no live snapshot can observe.
//
// For each key it keeps the newest version at or below the watermark (that
// version is still needed to answer reads at the watermark) plus everything
// above it. A key whose surviving version is a tombstone is removed outright.
//
// The watermark is the minimum of the oldest open snapshot and a retention
// floor, computed by the transaction layer. Choosing it conservatively is what
// keeps long-running scans from observing a partially collected snapshot.
func (s *Store) GC(watermark uint64) GCResult {
	started := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	res := GCResult{Watermark: watermark}
	var doomed []string

	s.idx.scan("", "", func(key string, c *chain) bool {
		if len(c.versions) == 0 {
			doomed = append(doomed, key)
			return true
		}
		keepFrom := sort.Search(len(c.versions), func(i int) bool { return c.versions[i].CommitTS > watermark })
		if keepFrom > 0 {
			keepFrom-- // retain the version visible exactly at the watermark
		}
		if keepFrom > 0 {
			for _, v := range c.versions[:keepFrom] {
				res.BytesFreed += len(v.Value) + 24
				if v.Tombstone {
					s.stat.Tombstones--
				}
			}
			res.VersionsFreed += keepFrom
			c.versions = append([]Version(nil), c.versions[keepFrom:]...)
		}
		if len(c.versions) == 1 && c.versions[0].Tombstone && c.versions[0].CommitTS <= watermark {
			res.BytesFreed += c.bytes()
			res.VersionsFreed += len(c.versions)
			s.stat.Tombstones--
			doomed = append(doomed, key)
		}
		return true
	})

	for _, k := range doomed {
		if s.idx.remove(k) {
			res.KeysRemoved++
			s.stat.Keys--
		}
	}

	s.stat.Versions -= res.VersionsFreed
	s.stat.ApproxBytes -= res.BytesFreed
	s.stat.GCRuns++
	s.stat.VersionsFreed += uint64(res.VersionsFreed)
	s.stat.LastWatermark = watermark
	res.Duration = time.Since(started).String()
	return res
}

// Stats returns a snapshot of engine occupancy.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stat
}
