package mvcc

import (
	"errors"
	"fmt"
	"testing"
)

func TestSnapshotReadsSeeConsistentVersions(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)
	s.Put("k", []byte("v3"), 30)

	cases := []struct {
		readTS uint64
		want   string
		ok     bool
	}{
		{5, "", false},
		{10, "v1", true},
		{19, "v1", true},
		{20, "v2", true},
		{29, "v2", true},
		{30, "v3", true},
		{1000, "v3", true},
	}
	for _, c := range cases {
		kv, err := s.Get("k", c.readTS)
		if c.ok != (err == nil) {
			t.Fatalf("readTS=%d: err=%v want ok=%v", c.readTS, err, c.ok)
		}
		if c.ok && string(kv.Value) != c.want {
			t.Fatalf("readTS=%d: got %q want %q", c.readTS, kv.Value, c.want)
		}
	}
}

func TestTombstoneHidesKeyFromLaterSnapshots(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Delete("k", 20)

	if _, err := s.Get("k", 15); err != nil {
		t.Fatal("a snapshot before the delete must still see the value")
	}
	if _, err := s.Get("k", 25); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a snapshot after the delete must not see the value: %v", err)
	}
	if got := s.LatestVersion("k"); got != 20 {
		t.Fatalf("tombstone must bump the latest version for conflict checks: %d", got)
	}
}

func TestScanIsOrderedAndBounded(t *testing.T) {
	s := New()
	for i := 0; i < 50; i++ {
		s.Put(fmt.Sprintf("tenant/a/res-%03d", i), []byte("x"), uint64(i+1))
	}
	s.Put("tenant/b/res-000", []byte("y"), 100)

	res := s.Scan("tenant/a/", "tenant/a0", 0, 200)
	if len(res.KVs) != 50 {
		t.Fatalf("prefix scan returned %d keys, want 50", len(res.KVs))
	}
	for i := 1; i < len(res.KVs); i++ {
		if res.KVs[i-1].Key >= res.KVs[i].Key {
			t.Fatal("scan output is not in ascending key order")
		}
	}

	page := s.Scan("tenant/a/", "tenant/a0", 10, 200)
	if len(page.KVs) != 10 || !page.More {
		t.Fatalf("paging broken: got %d keys more=%v", len(page.KVs), page.More)
	}
}

func TestScanRespectsSnapshot(t *testing.T) {
	s := New()
	s.Put("a", []byte("1"), 10)
	s.Put("b", []byte("2"), 30)
	res := s.Scan("", "", 0, 20)
	if len(res.KVs) != 1 || res.KVs[0].Key != "a" {
		t.Fatalf("scan leaked a future version: %+v", res.KVs)
	}
}

func TestGCKeepsWatermarkVisibleVersion(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"), 10)
	s.Put("k", []byte("v2"), 20)
	s.Put("k", []byte("v3"), 30)

	res := s.GC(25)
	if res.VersionsFreed != 1 {
		t.Fatalf("expected to free exactly the shadowed v1, freed %d", res.VersionsFreed)
	}
	// A reader still at the watermark must observe v2, not a hole.
	kv, err := s.Get("k", 25)
	if err != nil || string(kv.Value) != "v2" {
		t.Fatalf("GC broke a snapshot at the watermark: %q %v", kv.Value, err)
	}
	if kv, err := s.Get("k", 100); err != nil || string(kv.Value) != "v3" {
		t.Fatalf("GC lost the newest version: %q %v", kv.Value, err)
	}
}

func TestGCReclaimsFullyDeletedKeys(t *testing.T) {
	s := New()
	s.Put("gone", []byte("v"), 10)
	s.Delete("gone", 20)
	s.Put("stays", []byte("v"), 10)

	res := s.GC(50)
	if res.KeysRemoved != 1 {
		t.Fatalf("expected the tombstoned key to be reclaimed, removed %d", res.KeysRemoved)
	}
	if _, err := s.Get("stays", 50); err != nil {
		t.Fatal("GC removed a live key")
	}
	if st := s.Stats(); st.Keys != 1 {
		t.Fatalf("key accounting drifted: %+v", st)
	}
}

func TestOutOfOrderApplyIsRejected(t *testing.T) {
	s := New()
	s.Put("k", []byte("new"), 20)
	s.Put("k", []byte("old"), 10) // replayed entry: must not rewrite history
	kv, err := s.Get("k", 100)
	if err != nil || string(kv.Value) != "new" {
		t.Fatalf("out-of-order apply corrupted the chain: %q %v", kv.Value, err)
	}
}

func BenchmarkPut(b *testing.B) {
	s := New()
	for i := 0; i < b.N; i++ {
		s.Put(fmt.Sprintf("key-%d", i%10000), []byte("value"), uint64(i+1))
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Put(fmt.Sprintf("key-%d", i), []byte("value"), uint64(i+1))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get(fmt.Sprintf("key-%d", i%10000), 1<<62)
	}
}
