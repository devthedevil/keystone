package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrCompacted is returned when entries below the first available index are
// requested.
var ErrCompacted = errors.New("raft: requested index is compacted")

// Storage persists the replicated log and hard state. All methods must be safe
// for concurrent use.
type Storage interface {
	// SaveHardState durably records term and vote before they are acted upon.
	SaveHardState(hs HardState) error
	// LoadHardState returns the persisted term and vote.
	LoadHardState() (HardState, error)
	// Append durably appends entries. Entries must be contiguous and must
	// start at LastIndex()+1.
	Append(entries []Entry) error
	// TruncateFrom removes all entries with index >= idx.
	TruncateFrom(idx uint64) error
	// Entries returns entries in [lo, hi).
	Entries(lo, hi uint64) ([]Entry, error)
	// Last returns the last index and its term.
	Last() (index, term uint64)
	// Term returns the term of idx, and whether idx is present.
	Term(idx uint64) (uint64, bool)
	// Close releases resources.
	Close() error
}

// MemStorage is a volatile Storage used by tests and by ephemeral replicas.
type MemStorage struct {
	mu      sync.RWMutex
	entries []Entry // entries[i].Index == i+1
	hs      HardState
}

// NewMemStorage returns an empty in-memory log.
func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *MemStorage) LoadHardState() (HardState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hs, nil
}

func (m *MemStorage) Append(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendLocked(entries)
}

func (m *MemStorage) appendLocked(entries []Entry) error {
	for _, e := range entries {
		want := uint64(len(m.entries)) + 1
		if e.Index != want {
			return fmt.Errorf("raft: non-contiguous append: got index %d want %d", e.Index, want)
		}
		m.entries = append(m.entries, e)
	}
	return nil
}

func (m *MemStorage) TruncateFrom(idx uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx == 0 {
		return errors.New("raft: cannot truncate the sentinel index 0")
	}
	if idx > uint64(len(m.entries)) {
		return nil
	}
	m.entries = m.entries[:idx-1]
	return nil
}

func (m *MemStorage) Entries(lo, hi uint64) ([]Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if lo == 0 {
		return nil, ErrCompacted
	}
	if hi > uint64(len(m.entries))+1 {
		hi = uint64(len(m.entries)) + 1
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]Entry, hi-lo)
	copy(out, m.entries[lo-1:hi-1])
	return out, nil
}

func (m *MemStorage) Last() (uint64, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.entries) == 0 {
		return 0, 0
	}
	e := m.entries[len(m.entries)-1]
	return e.Index, e.Term
}

func (m *MemStorage) Term(idx uint64) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if idx == 0 {
		return 0, true
	}
	if idx > uint64(len(m.entries)) {
		return 0, false
	}
	return m.entries[idx-1].Term, true
}

func (m *MemStorage) Close() error { return nil }

// ---------------------------------------------------------------------------
// File-backed storage
// ---------------------------------------------------------------------------

// record types in the write-ahead log.
const (
	recEntry    byte = 1
	recTruncate byte = 2
)

// FileStorage is a crash-safe Storage backed by an append-only, CRC-checked
// write-ahead log plus an atomically replaced hard-state file.
//
// The full log is also kept in memory, which is appropriate for a control-plane
// metadata store whose working set is small; log compaction and snapshotting
// are tracked in docs/roadmap.md.
type FileStorage struct {
	mu   sync.RWMutex
	mem  *MemStorage
	dir  string
	wal  *os.File
	bw   *bufio.Writer
	hsF  string
	sync bool
}

// OpenFileStorage opens (or creates) a log directory and replays it.
// If fsync is false, writes are buffered and flushed but not fsynced, which is
// only appropriate for tests and benchmarks.
func OpenFileStorage(dir string, fsync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{
		mem:  NewMemStorage(),
		dir:  dir,
		hsF:  filepath.Join(dir, "hardstate.json"),
		sync: fsync,
	}
	if err := fs.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "raft.wal"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fs.wal = f
	fs.bw = bufio.NewWriterSize(f, 64<<10)
	return fs, nil
}

func (f *FileStorage) replay() error {
	path := filepath.Join(f.dir, "raft.wal")
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return f.loadHS()
	}
	if err != nil {
		return err
	}
	defer file.Close()

	r := bufio.NewReaderSize(file, 64<<10)
	for {
		var hdr [9]byte // 1 byte type + 4 byte length + 4 byte crc
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // torn tail from a crash: ignore the partial record
			}
			return err
		}
		typ := hdr[0]
		n := binary.BigEndian.Uint32(hdr[1:5])
		want := binary.BigEndian.Uint32(hdr[5:9])
		if n > 64<<20 {
			break
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			break // torn record
		}
		if crc32.ChecksumIEEE(buf) != want {
			break // corrupt tail
		}
		switch typ {
		case recEntry:
			var e Entry
			if err := json.Unmarshal(buf, &e); err != nil {
				return err
			}
			if err := f.mem.Append([]Entry{e}); err != nil {
				return err
			}
		case recTruncate:
			var idx uint64
			if err := json.Unmarshal(buf, &idx); err != nil {
				return err
			}
			if err := f.mem.TruncateFrom(idx); err != nil {
				return err
			}
		}
	}
	return f.loadHS()
}

func (f *FileStorage) loadHS() error {
	b, err := os.ReadFile(f.hsF)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var hs HardState
	if err := json.Unmarshal(b, &hs); err != nil {
		return nil // ignore a torn hard-state file; term will be relearned
	}
	return f.mem.SaveHardState(hs)
}

func (f *FileStorage) writeRecord(typ byte, payload any) error {
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var hdr [9]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(buf)))
	binary.BigEndian.PutUint32(hdr[5:9], crc32.ChecksumIEEE(buf))
	if _, err := f.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.bw.Write(buf); err != nil {
		return err
	}
	return nil
}

func (f *FileStorage) flush() error {
	if err := f.bw.Flush(); err != nil {
		return err
	}
	if f.sync {
		return f.wal.Sync()
	}
	return nil
}

func (f *FileStorage) SaveHardState(hs HardState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	tmp := f.hsF + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.hsF); err != nil {
		return err
	}
	return f.mem.SaveHardState(hs)
}

func (f *FileStorage) LoadHardState() (HardState, error) { return f.mem.LoadHardState() }

func (f *FileStorage) Append(entries []Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if err := f.writeRecord(recEntry, e); err != nil {
			return err
		}
	}
	if err := f.flush(); err != nil {
		return err
	}
	return f.mem.Append(entries)
}

func (f *FileStorage) TruncateFrom(idx uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.writeRecord(recTruncate, idx); err != nil {
		return err
	}
	if err := f.flush(); err != nil {
		return err
	}
	return f.mem.TruncateFrom(idx)
}

func (f *FileStorage) Entries(lo, hi uint64) ([]Entry, error) { return f.mem.Entries(lo, hi) }
func (f *FileStorage) Last() (uint64, uint64)                 { return f.mem.Last() }
func (f *FileStorage) Term(idx uint64) (uint64, bool)         { return f.mem.Term(idx) }

func (f *FileStorage) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flush(); err != nil {
		return err
	}
	return f.wal.Close()
}
