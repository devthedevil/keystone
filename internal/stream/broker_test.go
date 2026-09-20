package stream

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func put(seq uint64, key string) Change {
	return Change{Seq: seq, Kind: KindPut, Key: key, Value: []byte(key), WallTime: time.Unix(0, int64(seq))}
}

// drain collects up to n changes, failing the test if they do not arrive.
func drain(t *testing.T, s *Subscription, n int) []Change {
	t.Helper()
	out := make([]Change, 0, n)
	deadline := time.After(2 * time.Second)
	for len(out) < n {
		select {
		case c, ok := <-s.Changes():
			if !ok {
				t.Fatalf("subscription closed after %d of %d changes: %v", len(out), n, s.Err())
			}
			out = append(out, c)
		case <-deadline:
			t.Fatalf("timed out after %d of %d changes", len(out), n)
		}
	}
	return out
}

func TestSubscriberSeesChangesInLogOrder(t *testing.T) {
	b := NewBroker(Options{})
	sub, err := b.Subscribe(0, "")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for i := 1; i <= 50; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%02d", i)))
	}

	got := drain(t, sub, 50)
	for i, c := range got {
		if c.Seq != uint64(i+1) {
			t.Fatalf("change %d has seq %d: the feed must be ordered by log index", i, c.Seq)
		}
	}
}

// A consumer that reconnects must be able to resume exactly where it stopped.
// Without this, a restarted downstream projection has to re-read the whole
// key space to be sure it missed nothing.
func TestResumeFromCursorReplaysTheTail(t *testing.T) {
	b := NewBroker(Options{})
	for i := 1; i <= 20; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%02d", i)))
	}

	sub, err := b.Subscribe(12, "")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	got := drain(t, sub, 8)
	if got[0].Seq != 13 {
		t.Fatalf("resume delivered seq %d first, want 13: a cursor is exclusive", got[0].Seq)
	}
	if got[len(got)-1].Seq != 20 {
		t.Fatalf("resume stopped at seq %d, want 20", got[len(got)-1].Seq)
	}
}

// Replay has to hand over to the live feed without a gap, or a consumer that
// subscribes during steady traffic silently loses the changes published
// between "read the buffer" and "start listening".
func TestReplayHandsOverToLiveWithoutAGap(t *testing.T) {
	b := NewBroker(Options{})
	for i := 1; i <= 10; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%02d", i)))
	}

	sub, err := b.Subscribe(0, "")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for i := 11; i <= 20; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%02d", i)))
	}

	got := drain(t, sub, 20)
	for i, c := range got {
		if c.Seq != uint64(i+1) {
			t.Fatalf("gap at position %d: got seq %d, want %d", i, c.Seq, i+1)
		}
	}
}

func TestPrefixFilterExcludesOtherKeySpaces(t *testing.T) {
	b := NewBroker(Options{})
	sub, err := b.Subscribe(0, "tenantA/")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	b.Publish(
		put(1, "tenantA/one"),
		put(2, "tenantB/two"),
		put(3, "tenantA/three"),
		put(4, "tenantC/four"),
	)

	got := drain(t, sub, 2)
	if got[0].Key != "tenantA/one" || got[1].Key != "tenantA/three" {
		t.Fatalf("prefix filter leaked another tenant's changes: %v", got)
	}

	// Nothing further should arrive: the two foreign changes must not appear
	// late either.
	select {
	case c := <-sub.Changes():
		t.Fatalf("unexpected change %q delivered to a prefixed subscriber", c.Key)
	case <-time.After(100 * time.Millisecond):
	}
}

// A cursor older than the retention buffer cannot be served. Failing loudly is
// the point: silently starting from the oldest retained change would leave the
// consumer with a hole it has no way to detect.
func TestExpiredCursorIsRejected(t *testing.T) {
	b := NewBroker(Options{RetentionEntries: 8})
	for i := 1; i <= 40; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%02d", i)))
	}
	if _, err := b.Subscribe(2, ""); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("subscribing from an evicted cursor returned %v, want ErrCursorExpired", err)
	}
	// A cursor inside the retained window still works.
	sub, err := b.Subscribe(39, "")
	if err != nil {
		t.Fatalf("subscribing from a retained cursor failed: %v", err)
	}
	defer sub.Close()
	if got := drain(t, sub, 1); got[0].Seq != 40 {
		t.Fatalf("got seq %d, want 40", got[0].Seq)
	}
}

// The apply loop must never block on a slow consumer. Replication latency for
// every tenant is not an acceptable price for one stalled watcher, so the
// broker drops the subscriber instead and tells it to resume.
func TestSlowConsumerIsDroppedRatherThanStallingReplication(t *testing.T) {
	b := NewBroker(Options{SubscriberQueue: 4})
	sub, err := b.Subscribe(0, "")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 500; i++ {
			b.Publish(put(uint64(i), fmt.Sprintf("k%03d", i)))
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow consumer: the apply loop must never wait on a watcher")
	}

	// Drain whatever was buffered; the subscription must then report why it
	// ended rather than simply going quiet.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-sub.Changes():
			if !open {
				if err := sub.Err(); !errors.Is(err, ErrSlowConsumer) {
					t.Fatalf("dropped subscriber reported %v, want ErrSlowConsumer", err)
				}
				return
			}
		case <-deadline:
			t.Fatal("dropped subscriber never had its channel closed")
		}
	}
}

func TestConcurrentSubscribersAllSeeEveryChange(t *testing.T) {
	b := NewBroker(Options{})
	const subs, events = 8, 200

	var wg sync.WaitGroup
	errs := make(chan error, subs)
	for i := 0; i < subs; i++ {
		sub, err := b.Subscribe(0, "")
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(s *Subscription) {
			defer wg.Done()
			defer s.Close()
			var last uint64
			for n := 0; n < events; n++ {
				select {
				case c, ok := <-s.Changes():
					if !ok {
						errs <- fmt.Errorf("subscriber closed early at seq %d: %v", last, s.Err())
						return
					}
					if c.Seq != last+1 {
						errs <- fmt.Errorf("out of order: got %d after %d", c.Seq, last)
						return
					}
					last = c.Seq
				case <-time.After(3 * time.Second):
					errs <- fmt.Errorf("subscriber stalled at seq %d", last)
					return
				}
			}
		}(sub)
	}

	for i := 1; i <= events; i++ {
		b.Publish(put(uint64(i), fmt.Sprintf("k%03d", i)))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestStatsTrackWatchers(t *testing.T) {
	b := NewBroker(Options{})
	if got := b.Stats().Watchers; got != 0 {
		t.Fatalf("fresh broker reports %d watchers", got)
	}
	s1, _ := b.Subscribe(0, "")
	s2, _ := b.Subscribe(0, "")
	if got := b.Stats().Watchers; got != 2 {
		t.Fatalf("got %d watchers, want 2", got)
	}
	s1.Close()
	if got := b.Stats().Watchers; got != 1 {
		t.Fatalf("got %d watchers after one close, want 1", got)
	}
	s2.Close()
	// Closing twice must be safe: the HTTP handler closes on every exit path.
	s2.Close()
	if got := b.Stats().Watchers; got != 0 {
		t.Fatalf("got %d watchers after all closed, want 0", got)
	}
}
