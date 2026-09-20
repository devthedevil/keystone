package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPTransport speaks the peer protocol over HTTP/1.1 with keep-alives.
// A binary framed protocol would be cheaper, but HTTP keeps the peer plane
// debuggable with curl, which is worth more than the bytes during an incident.
type HTTPTransport struct {
	mu    sync.RWMutex
	peers map[NodeID]string // node id -> base URL, e.g. http://10.0.0.7:7001
	cli   *http.Client
}

// NewHTTPTransport builds a transport over the given peer address map.
func NewHTTPTransport(peers map[NodeID]string, timeout time.Duration) *HTTPTransport {
	cp := make(map[NodeID]string, len(peers))
	for k, v := range peers {
		cp[k] = v
	}
	return &HTTPTransport{
		peers: cp,
		cli: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: timeout / 2}).DialContext,
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
	}
}

// SetPeer adds or updates a peer address at runtime.
func (t *HTTPTransport) SetPeer(id NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

func (t *HTTPTransport) addr(id NodeID) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	a, ok := t.peers[id]
	return a, ok
}

func (t *HTTPTransport) post(ctx context.Context, to NodeID, path string, req, out any) error {
	base, ok := t.addr(to)
	if !ok {
		return fmt.Errorf("raft: unknown peer %s", to)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := t.cli.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft: peer %s returned %s", to, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t *HTTPTransport) RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	var out RequestVoteResponse
	if err := t.post(ctx, to, "/raft/vote", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	var out AppendEntriesResponse
	if err := t.post(ctx, to, "/raft/append", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PeerHandler serves the inbound side of the peer protocol. It is mounted on a
// separate listener from the client API so that a client-traffic overload
// cannot starve replication (bulkhead isolation).
func PeerHandler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, n.HandleRequestVote(&req))
	})
	mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, n.HandleAppendEntries(&req))
	})
	mux.HandleFunc("/raft/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, n.Status())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
