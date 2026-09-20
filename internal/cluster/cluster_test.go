package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/devthedevil/keystone/pkg/api"
)

// harness starts a real three-replica cluster on loopback, replicating over the
// HTTP peer transport. It exercises the same code path as production rather
// than an in-process shortcut.
type harness struct {
	t     *testing.T
	nodes []*Node
	cli   *http.Client
}

func startCluster(t *testing.T, size int) *harness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	clientLns := make([]net.Listener, size)
	peerLns := make([]net.Listener, size)
	peers := map[string]string{}
	for i := 0; i < size; i++ {
		cl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		pl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		clientLns[i], peerLns[i] = cl, pl
		peers[fmt.Sprintf("n%d", i+1)] = "http://" + pl.Addr().String()
	}

	h := &harness{t: t, cli: &http.Client{Timeout: 10 * time.Second}}
	for i := 0; i < size; i++ {
		n, err := New(Options{
			NodeID:            fmt.Sprintf("n%d", i+1),
			Peers:             peers,
			ClientListener:    clientLns[i],
			PeerListener:      peerLns[i],
			HeartbeatInterval: 40 * time.Millisecond,
			ElectionTimeout:   400 * time.Millisecond,
			GCInterval:        time.Hour,
			AdminToken:        "secret",
			Logger:            logger,
		})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		h.nodes = append(h.nodes, n)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, n := range h.nodes {
			_ = n.Shutdown(ctx)
		}
	})
	return h
}

// leaderURL returns the client base URL of the current leader.
func (h *harness) leaderURL(timeout time.Duration) string {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range h.nodes {
			st := n.Raft().Status()
			if st.State == "leader" && st.LeaseValid {
				return "http://" + n.ClientAddr()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatal("no leader with a lease after cluster start")
	return ""
}

func (h *harness) do(method, url string, body io.Reader, headers map[string]string) (*http.Response, []byte) {
	h.t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		h.t.Fatalf("request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.cli.Do(req)
	if err != nil {
		h.t.Fatalf("do %s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func TestHTTPClusterReplicatesWrites(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)

	resp, body := h.do(http.MethodPut, base+"/v1/kv/vcn/ocid1", strings.NewReader(`{"cidr":"10.0.0.0/16"}`),
		map[string]string{"X-Keystone-Tenant": "networking"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put failed: %d %s", resp.StatusCode, body)
	}
	var put api.TxnResponse
	if err := json.Unmarshal(body, &put); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !put.Committed || put.CommitTS == 0 {
		t.Fatalf("unexpected commit response: %+v", put)
	}

	resp, body = h.do(http.MethodGet, base+"/v1/kv/vcn/ocid1", nil,
		map[string]string{"X-Keystone-Tenant": "networking"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get failed: %d %s", resp.StatusCode, body)
	}
	var got api.GetResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got.Value) != `{"cidr":"10.0.0.0/16"}` {
		t.Fatalf("value mismatch: %s", got.Value)
	}
	if got.Version != put.CommitTS {
		t.Fatalf("version %d should match commit %d", got.Version, put.CommitTS)
	}
}

func TestTenantsCannotReadEachOther(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)

	h.do(http.MethodPut, base+"/v1/kv/secret", strings.NewReader("tenant-a-data"),
		map[string]string{"X-Keystone-Tenant": "tenant-a"})

	resp, _ := h.do(http.MethodGet, base+"/v1/kv/secret", nil,
		map[string]string{"X-Keystone-Tenant": "tenant-b"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("tenant b read tenant a's key: status %d", resp.StatusCode)
	}

	// A range scan must not leak across the prefix boundary either.
	resp, body := h.do(http.MethodGet, base+"/v1/range?limit=100", nil,
		map[string]string{"X-Keystone-Tenant": "tenant-b"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("range failed: %d %s", resp.StatusCode, body)
	}
	var rr api.RangeResponse
	json.Unmarshal(body, &rr)
	if len(rr.KVs) != 0 {
		t.Fatalf("scan leaked %d keys across tenants: %+v", len(rr.KVs), rr.KVs)
	}
}

func TestConditionalWriteEnforcesVersion(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)
	hdr := map[string]string{"X-Keystone-Tenant": "t"}

	// Create-if-absent succeeds once.
	create := map[string]string{"X-Keystone-Tenant": "t", "If-None-Match": "*"}
	resp, body := h.do(http.MethodPut, base+"/v1/kv/cas", strings.NewReader("v1"), create)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create: %d %s", resp.StatusCode, body)
	}
	var first api.TxnResponse
	json.Unmarshal(body, &first)

	// And fails the second time.
	resp, _ = h.do(http.MethodPut, base+"/v1/kv/cas", strings.NewReader("v2"), create)
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("duplicate create should fail with 412, got %d", resp.StatusCode)
	}

	// Compare-and-swap on a stale version fails.
	stale := map[string]string{"X-Keystone-Tenant": "t", "If-Match": "1"}
	resp, _ = h.do(http.MethodPut, base+"/v1/kv/cas", strings.NewReader("v3"), stale)
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("stale CAS should fail with 412, got %d", resp.StatusCode)
	}

	// Compare-and-swap on the current version succeeds.
	fresh := map[string]string{"X-Keystone-Tenant": "t", "If-Match": fmt.Sprint(first.CommitTS)}
	resp, body = h.do(http.MethodPut, base+"/v1/kv/cas", strings.NewReader("v4"), fresh)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid CAS failed: %d %s", resp.StatusCode, body)
	}

	_, body = h.do(http.MethodGet, base+"/v1/kv/cas", nil, hdr)
	var got api.GetResponse
	json.Unmarshal(body, &got)
	if string(got.Value) != "v4" {
		t.Fatalf("unexpected final value %q", got.Value)
	}
}

func TestIdempotencyKeyDeduplicatesRetries(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)
	hdr := map[string]string{"X-Keystone-Tenant": "t", "Idempotency-Key": "fixed-key-123"}

	_, body1 := h.do(http.MethodPut, base+"/v1/kv/once", strings.NewReader("a"), hdr)
	_, body2 := h.do(http.MethodPut, base+"/v1/kv/once", strings.NewReader("b"), hdr)

	var r1, r2 api.TxnResponse
	json.Unmarshal(body1, &r1)
	json.Unmarshal(body2, &r2)
	if !r2.Duplicate {
		t.Fatal("retry with the same idempotency key was applied twice")
	}
	if r1.CommitTS != r2.CommitTS {
		t.Fatalf("duplicate reported a different commit: %d vs %d", r1.CommitTS, r2.CommitTS)
	}
	_, body := h.do(http.MethodGet, base+"/v1/kv/once", nil, map[string]string{"X-Keystone-Tenant": "t"})
	var got api.GetResponse
	json.Unmarshal(body, &got)
	if string(got.Value) != "a" {
		t.Fatalf("the replayed write overwrote the original: %q", got.Value)
	}
}

func TestFollowerRedirectsWritesToLeader(t *testing.T) {
	h := startCluster(t, 3)
	leader := h.leaderURL(10 * time.Second)

	var follower string
	for _, n := range h.nodes {
		if url := "http://" + n.ClientAddr(); url != leader {
			follower = url
			break
		}
	}
	resp, body := h.do(http.MethodPut, follower+"/v1/kv/redirect", strings.NewReader("x"),
		map[string]string{"X-Keystone-Tenant": "t"})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("follower should refuse writes, got %d", resp.StatusCode)
	}
	var errResp api.ErrorResponse
	json.Unmarshal(body, &errResp)
	if errResp.Error.Code != api.CodeNotLeader {
		t.Fatalf("expected not_leader, got %q", errResp.Error.Code)
	}
	if errResp.Error.Leader == "" || !errResp.Error.Retryable {
		t.Fatalf("redirect must name the leader and be marked retryable: %+v", errResp.Error)
	}
}

func TestTransactionEndpointIsAtomic(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)
	hdr := map[string]string{"X-Keystone-Tenant": "t", "Content-Type": "application/json"}

	req := api.TxnRequest{
		TxnID: "batch-1",
		Mutations: []api.Mutation{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2")},
			{Key: "c", Value: []byte("3")},
		},
	}
	buf, _ := json.Marshal(req)
	resp, body := h.do(http.MethodPost, base+"/v1/txn", bytes.NewReader(buf), hdr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("txn: %d %s", resp.StatusCode, body)
	}

	_, body = h.do(http.MethodGet, base+"/v1/range?limit=100", nil, hdr)
	var rr api.RangeResponse
	json.Unmarshal(body, &rr)
	if len(rr.KVs) != 3 {
		t.Fatalf("expected 3 keys from the batch, got %d", len(rr.KVs))
	}
	for i, k := range []string{"a", "b", "c"} {
		if rr.KVs[i].Key != k {
			t.Fatalf("range out of order at %d: %q", i, rr.KVs[i].Key)
		}
		if rr.KVs[i].Version != rr.KVs[0].Version {
			t.Fatal("a batch must commit at a single version")
		}
	}

	// A transaction whose condition fails must leave nothing behind.
	bad := api.TxnRequest{
		TxnID:      "batch-2",
		Conditions: []api.Condition{{Key: "a", Type: api.CondVersion, Version: 999999}},
		Mutations:  []api.Mutation{{Key: "d", Value: []byte("4")}},
	}
	buf, _ = json.Marshal(bad)
	resp, _ = h.do(http.MethodPost, base+"/v1/txn", bytes.NewReader(buf), hdr)
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", resp.StatusCode)
	}
	resp, _ = h.do(http.MethodGet, base+"/v1/kv/d", nil, hdr)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatal("a failed transaction leaked a mutation")
	}
}

func TestChangeFeedDeliversCommitsInOrder(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)
	hdr := map[string]string{"X-Keystone-Tenant": "t"}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/stream?from=0", nil)
	req.Header.Set("X-Keystone-Tenant", "t")
	resp, err := h.cli.Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d", resp.StatusCode)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 5; i++ {
			h.do(http.MethodPut, fmt.Sprintf("%s/v1/kv/stream-%d", base, i), strings.NewReader("v"), hdr)
		}
	}()

	seen := []api.ChangeEvent{}
	dec := bufioScanner(resp.Body)
	for len(seen) < 5 {
		line, err := dec()
		if err != nil {
			t.Fatalf("stream read: %v (saw %d events)", err, len(seen))
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev api.ChangeEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			continue
		}
		seen = append(seen, ev)
	}
	for i, ev := range seen {
		if want := fmt.Sprintf("stream-%d", i); ev.Key != want {
			t.Fatalf("change feed out of order at %d: got %q want %q", i, ev.Key, want)
		}
		if i > 0 && ev.Seq <= seen[i-1].Seq {
			t.Fatal("change feed sequence numbers are not increasing")
		}
	}
}

func TestQuotaAdminAndThrottling(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)

	// Admin endpoints are protected.
	resp, _ := h.do(http.MethodPut, base+"/v1/admin/quota/greedy",
		strings.NewReader(`{"rate_per_sec":1,"burst":2,"max_concurrent":4}`), nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("admin endpoint is unprotected: %d", resp.StatusCode)
	}

	admin := map[string]string{"X-Keystone-Admin-Token": "secret", "Content-Type": "application/json"}
	resp, body := h.do(http.MethodPut, base+"/v1/admin/quota/greedy",
		strings.NewReader(`{"rate_per_sec":1,"burst":2,"max_concurrent":4}`), admin)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set quota: %d %s", resp.StatusCode, body)
	}

	throttled := false
	for i := 0; i < 10; i++ {
		resp, _ := h.do(http.MethodPut, fmt.Sprintf("%s/v1/kv/k%d", base, i), strings.NewReader("v"),
			map[string]string{"X-Keystone-Tenant": "greedy"})
		if resp.StatusCode == http.StatusTooManyRequests {
			if resp.Header.Get("Retry-After") == "" {
				t.Fatal("a 429 must carry Retry-After")
			}
			throttled = true
			break
		}
	}
	if !throttled {
		t.Fatal("tenant quota was not enforced")
	}

	// A different tenant is unaffected.
	resp, _ = h.do(http.MethodPut, base+"/v1/kv/ok", strings.NewReader("v"),
		map[string]string{"X-Keystone-Tenant": "polite"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unrelated tenant was throttled: %d", resp.StatusCode)
	}
}

func TestStatusAndMetricsExposeReplicationState(t *testing.T) {
	h := startCluster(t, 3)
	base := h.leaderURL(10 * time.Second)

	_, body := h.do(http.MethodGet, base+"/v1/status", nil, nil)
	var st api.StatusResponse
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	if st.State != "leader" || !st.LeaseValid || st.Leader == "" {
		t.Fatalf("unexpected leader status: %+v", st)
	}

	_, body = h.do(http.MethodGet, base+"/metrics", nil, nil)
	out := string(body)
	for _, want := range []string{
		"keystone_raft_is_leader",
		"keystone_raft_commit_index",
		"keystone_raft_apply_lag",
		"keystone_store_keys",
		"keystone_admission_health",
		"keystone_request_duration_seconds_bucket",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}

func TestLivenessDoesNotDependOnQuorum(t *testing.T) {
	h := startCluster(t, 1) // a single replica that cannot form a quorum with peers
	url := "http://" + h.nodes[0].ClientAddr()
	resp, _ := h.do(http.MethodGet, url+"/health/live", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("liveness must not depend on quorum: %d", resp.StatusCode)
	}
}

// bufioScanner returns a line reader for server-sent events.
func bufioScanner(r io.Reader) func() (string, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 512)
	return func() (string, error) {
		for {
			if i := bytes.IndexByte(buf, '\n'); i >= 0 {
				line := string(buf[:i])
				buf = buf[i+1:]
				return line, nil
			}
			n, err := r.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
				continue
			}
			if err != nil {
				return "", err
			}
		}
	}
}
