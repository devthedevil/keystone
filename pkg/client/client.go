// Package client is the Go client for Keystone.
//
// The client exists to make the correct thing the easy thing. Three behaviours
// that callers otherwise get wrong are built in:
//
//   - Leader discovery. Writes and linearizable reads only succeed on the
//     leader. The client follows the leader hint the server returns and
//     remembers it, so a failover costs one redirect rather than a page.
//   - Retry classification. The server says whether an error is retryable and
//     how long to wait; the client obeys that instead of retrying everything
//     (which duplicates work) or nothing (which turns a 50ms election into a
//     caller-visible outage).
//   - Idempotency. Every mutating call carries a transaction ID that is
//     generated once and reused across retries, so a retry after an ambiguous
//     timeout cannot apply a write twice.
package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devthedevil/keystone/pkg/api"
)

// Options configures a Client.
type Options struct {
	// Endpoints are client-plane base URLs, e.g. "http://10.0.0.1:8080".
	// Order does not matter; the client learns which one leads.
	Endpoints []string
	// NodeAddrs optionally maps raft node IDs to base URLs. When present the
	// client can jump straight to the leader named in a redirect instead of
	// probing endpoints in turn.
	NodeAddrs map[string]string

	// Tenant is sent on every request and scopes the key space.
	Tenant string
	// Class is the priority class ("high", "normal", "bulk"). Empty means
	// normal. Bulk work should say so: it is the first thing shed under load.
	Class string

	AdminToken string

	// MaxAttempts bounds retries of retryable failures. Zero means 5.
	MaxAttempts int
	// BaseBackoff is the first retry delay. Zero means 20ms.
	BaseBackoff time.Duration
	// MaxBackoff caps the delay. Zero means 2s.
	MaxBackoff time.Duration
	// Timeout bounds a single HTTP attempt. Zero means 10s.
	Timeout time.Duration

	HTTPClient *http.Client
}

// Client talks to a Keystone cluster.
type Client struct {
	opts  Options
	http  *http.Client
	rand  *mathrand.Rand
	randM sync.Mutex

	mu      sync.RWMutex
	current string // endpoint believed to be the leader
}

// Error is a structured server error.
type Error struct {
	Status int
	Body   api.ErrorBody
}

func (e *Error) Error() string {
	return fmt.Sprintf("keystone: %s (%d): %s", e.Body.Code, e.Status, e.Body.Message)
}

// Code returns the machine-readable error code, or "" for non-Keystone errors.
func Code(err error) string {
	var ke *Error
	if errors.As(err, &ke) {
		return ke.Body.Code
	}
	return ""
}

// IsConflict reports whether a transaction lost an optimistic race and should
// be replayed against a fresh snapshot.
func IsConflict(err error) bool { return Code(err) == api.CodeConflict }

// IsPrecondition reports whether a caller-supplied condition did not hold.
// These must not be blindly retried: the caller's assumption was wrong.
func IsPrecondition(err error) bool { return Code(err) == api.CodePrecondition }

// IsNotFound reports whether the key has no visible version.
func IsNotFound(err error) bool { return Code(err) == api.CodeNotFound }

// IsThrottled reports whether the tenant is over its quota.
func IsThrottled(err error) bool { return Code(err) == api.CodeThrottled }

// New builds a client.
func New(opts Options) (*Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, errors.New("keystone: at least one endpoint is required")
	}
	eps := make([]string, 0, len(opts.Endpoints))
	for _, e := range opts.Endpoints {
		n := normalizeEndpoint(e)
		// Validating here turns a misconfigured endpoint into one clear error
		// at construction instead of an identical parse failure on every
		// request for the lifetime of the process.
		u, err := url.Parse(n)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("keystone: endpoint %q is not an http(s) base URL", e)
		}
		if u.Path != "" || u.RawQuery != "" {
			return nil, fmt.Errorf("keystone: endpoint %q must be a bare base URL with no path", e)
		}
		eps = append(eps, n)
	}
	opts.Endpoints = eps
	if opts.NodeAddrs != nil {
		for id, a := range opts.NodeAddrs {
			opts.NodeAddrs[id] = normalizeEndpoint(a)
		}
	}
	if opts.Tenant == "" {
		opts.Tenant = "default"
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = 20 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 2 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
	return &Client{
		opts:    opts,
		http:    hc,
		rand:    mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
		current: opts.Endpoints[0],
	}, nil
}

func normalizeEndpoint(e string) string {
	e = strings.TrimSuffix(strings.TrimSpace(e), "/")
	if !strings.Contains(e, "://") {
		e = "http://" + e
	}
	return e
}

// NewTxnID returns a fresh idempotency key. Generate one per logical
// operation, not per attempt.
func NewTxnID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// ---------------------------------------------------------------------------
// Key/value operations
// ---------------------------------------------------------------------------

// Get reads the latest committed value of a key.
func (c *Client) Get(ctx context.Context, key string) (api.GetResponse, error) {
	var out api.GetResponse
	err := c.do(ctx, request{method: http.MethodGet, path: "/v1/kv/" + escapeKey(key)}, &out)
	return out, err
}

// PutOptions are the optional preconditions on a single-key write.
type PutOptions struct {
	// IfVersion, when non-zero, requires the key to be at exactly this
	// version: a compare-and-swap.
	IfVersion uint64
	// IfAbsent requires the key to have no visible version: a create.
	IfAbsent bool
	// TxnID is the idempotency key. Empty means one is generated.
	TxnID string
}

// Put writes a value, optionally under a precondition. It returns the commit
// version.
func (c *Client) Put(ctx context.Context, key string, value []byte, opt *PutOptions) (uint64, error) {
	req := request{
		method:  http.MethodPut,
		path:    "/v1/kv/" + escapeKey(key),
		body:    value,
		raw:     true,
		headers: map[string]string{},
	}
	txnID := ""
	if opt != nil {
		if opt.IfVersion != 0 {
			req.headers["If-Match"] = strconv.FormatUint(opt.IfVersion, 10)
		}
		if opt.IfAbsent {
			req.headers["If-None-Match"] = "*"
		}
		txnID = opt.TxnID
	}
	if txnID == "" {
		txnID = NewTxnID()
	}
	req.headers["Idempotency-Key"] = txnID

	var out api.TxnResponse
	if err := c.do(ctx, req, &out); err != nil {
		return 0, err
	}
	return out.CommitTS, nil
}

// DeleteOptions are the optional preconditions on a delete.
type DeleteOptions struct {
	IfVersion uint64
	TxnID     string
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string, opt *DeleteOptions) (uint64, error) {
	req := request{
		method:  http.MethodDelete,
		path:    "/v1/kv/" + escapeKey(key),
		headers: map[string]string{},
	}
	txnID := ""
	if opt != nil {
		if opt.IfVersion != 0 {
			req.headers["If-Match"] = strconv.FormatUint(opt.IfVersion, 10)
		}
		txnID = opt.TxnID
	}
	if txnID == "" {
		txnID = NewTxnID()
	}
	req.headers["Idempotency-Key"] = txnID

	var out api.TxnResponse
	if err := c.do(ctx, req, &out); err != nil {
		return 0, err
	}
	return out.CommitTS, nil
}

// ScanOptions bounds a range read.
type ScanOptions struct {
	Start  string
	End    string
	Prefix string
	Limit  int
}

// Scan reads an ordered range at a single snapshot.
func (c *Client) Scan(ctx context.Context, opt ScanOptions) (api.RangeResponse, error) {
	q := url.Values{}
	if opt.Prefix != "" {
		q.Set("prefix", opt.Prefix)
	} else {
		if opt.Start != "" {
			q.Set("start", opt.Start)
		}
		if opt.End != "" {
			q.Set("end", opt.End)
		}
	}
	if opt.Limit > 0 {
		q.Set("limit", strconv.Itoa(opt.Limit))
	}
	var out api.RangeResponse
	err := c.do(ctx, request{method: http.MethodGet, path: "/v1/range?" + q.Encode()}, &out)
	return out, err
}

// ScanAll pages through a range and invokes fn for every key. Paging is done
// by the client so that a large scan never becomes one unbounded request that
// pins a snapshot and blocks garbage collection.
func (c *Client) ScanAll(ctx context.Context, opt ScanOptions, fn func(api.KV) error) error {
	page := opt
	if page.Limit <= 0 {
		page.Limit = 500
	}
	if page.Prefix != "" {
		page.Start, page.End = page.Prefix, prefixEnd(page.Prefix)
		page.Prefix = ""
	}
	for {
		res, err := c.Scan(ctx, page)
		if err != nil {
			return err
		}
		for _, kv := range res.KVs {
			if err := fn(kv); err != nil {
				return err
			}
		}
		if !res.More || res.NextStart == "" {
			return nil
		}
		page.Start = res.NextStart
	}
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

// Txn submits a one-shot transaction: preconditions and mutations that either
// all apply at one commit timestamp or none do.
func (c *Client) Txn(ctx context.Context, req api.TxnRequest) (api.TxnResponse, error) {
	if req.TxnID == "" {
		req.TxnID = NewTxnID()
	}
	buf, err := json.Marshal(req)
	if err != nil {
		return api.TxnResponse{}, err
	}
	var out api.TxnResponse
	err = c.do(ctx, request{method: http.MethodPost, path: "/v1/txn", body: buf, contentType: "application/json"}, &out)
	return out, err
}

// Update runs an optimistic read/modify/write against a consistent snapshot
// and retries it when it loses a race.
//
// This is the shape most control-plane callers actually want: read the current
// state, decide, write the decision, and be certain no concurrent writer slid
// in between. fn receives the values of keys at one snapshot and returns the
// mutations to apply; the commit fails if any of those keys changed.
func (c *Client) Update(ctx context.Context, keys []string, fn func(cur map[string]api.KV) ([]api.Mutation, error)) (api.TxnResponse, error) {
	txnID := NewTxnID()
	var last error
	for attempt := 0; attempt < c.opts.MaxAttempts; attempt++ {
		cur := make(map[string]api.KV, len(keys))
		var reads []api.ReadRef
		var readTS uint64
		for _, k := range keys {
			got, err := c.Get(ctx, k)
			if err != nil && !IsNotFound(err) {
				return api.TxnResponse{}, err
			}
			if err == nil {
				cur[k] = got.KV
				reads = append(reads, api.ReadRef{Key: k, Version: got.Version})
				readTS = max64(readTS, got.ReadTS)
			} else {
				// Absent keys still need a reference, or another writer could
				// create the key between the read and the commit.
				reads = append(reads, api.ReadRef{Key: k, Version: 0})
			}
		}
		muts, err := fn(cur)
		if err != nil {
			return api.TxnResponse{}, err
		}
		if len(muts) == 0 {
			return api.TxnResponse{Committed: true}, nil
		}
		res, err := c.Txn(ctx, api.TxnRequest{
			TxnID: txnID, ReadTS: readTS, Reads: reads, Mutations: muts,
		})
		if err == nil {
			return res, nil
		}
		if !IsConflict(err) {
			return api.TxnResponse{}, err
		}
		last = err
		// A conflict means someone else committed first: re-read and re-decide
		// with a fresh idempotency key, because this is a different write.
		txnID = NewTxnID()
		if err := c.sleep(ctx, attempt, 0); err != nil {
			return api.TxnResponse{}, err
		}
	}
	return api.TxnResponse{}, fmt.Errorf("keystone: update gave up after %d attempts: %w", c.opts.MaxAttempts, last)
}

// ---------------------------------------------------------------------------
// Change feed
// ---------------------------------------------------------------------------

// WatchOptions configures a change-feed subscription.
type WatchOptions struct {
	// From is the sequence to resume after. Zero starts at the oldest
	// retained change.
	From uint64
	// Prefix narrows the feed to a key prefix.
	Prefix string
}

// Watch streams changes to fn until the context is cancelled or fn returns an
// error. The caller should persist the last delivered Seq: resuming from it is
// what makes a downstream projection recoverable without a full re-bootstrap.
func (c *Client) Watch(ctx context.Context, opt WatchOptions, fn func(api.ChangeEvent) error) error {
	q := url.Values{}
	if opt.From > 0 {
		q.Set("from", strconv.FormatUint(opt.From, 10))
	}
	if opt.Prefix != "" {
		q.Set("prefix", opt.Prefix)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint()+"/v1/stream?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	c.decorate(req)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decodeError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)
	var data string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if data == "" {
				continue
			}
			var ev api.ChangeEvent
			payload := data
			data = ""
			if err := json.Unmarshal([]byte(payload), &ev); err != nil {
				continue
			}
			if err := fn(ev); err != nil {
				return err
			}
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		default:
			// id:, event: and comment frames carry no payload we need here.
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

// Status returns a replica's view of the cluster.
func (c *Client) Status(ctx context.Context) (api.StatusResponse, error) {
	var out api.StatusResponse
	err := c.do(ctx, request{method: http.MethodGet, path: "/v1/status", noRedirect: true}, &out)
	return out, err
}

// StatusOf returns the view of one specific endpoint, which is what an
// operator needs when replicas disagree.
func (c *Client) StatusOf(ctx context.Context, endpoint string) (api.StatusResponse, error) {
	var out api.StatusResponse
	err := c.attempt(ctx, normalizeEndpoint(endpoint), request{method: http.MethodGet, path: "/v1/status"}, &out)
	return out, err
}

// Endpoints returns the configured client-plane endpoints.
func (c *Client) Endpoints() []string { return append([]string(nil), c.opts.Endpoints...) }

// SetQuota updates a tenant's admission policy. Requires the admin token.
func (c *Client) SetQuota(ctx context.Context, q api.TenantQuota) (api.TenantQuota, error) {
	buf, err := json.Marshal(q)
	if err != nil {
		return api.TenantQuota{}, err
	}
	var out api.TenantQuota
	err = c.do(ctx, request{
		method: http.MethodPut, path: "/v1/admin/quota/" + url.PathEscape(q.Tenant),
		body: buf, contentType: "application/json", admin: true,
	}, &out)
	return out, err
}

// ListQuotas returns the live admission view of every tenant the replica has
// seen. Requires the admin token.
func (c *Client) ListQuotas(ctx context.Context) ([]api.TenantStats, error) {
	var out []api.TenantStats
	err := c.do(ctx, request{method: http.MethodGet, path: "/v1/admin/quota", admin: true}, &out)
	return out, err
}

// RunGC triggers a replicated collection. Requires the admin token.
func (c *Client) RunGC(ctx context.Context) (api.GCResponse, error) {
	var out api.GCResponse
	err := c.do(ctx, request{method: http.MethodPost, path: "/v1/admin/gc", admin: true}, &out)
	return out, err
}

// StepDown asks the leader to abdicate, which is how a deployment drains a
// replica before restarting it. Requires the admin token.
func (c *Client) StepDown(ctx context.Context) error {
	return c.do(ctx, request{method: http.MethodPost, path: "/v1/admin/stepdown", admin: true}, nil)
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

type request struct {
	method      string
	path        string
	body        []byte
	raw         bool
	contentType string
	headers     map[string]string
	admin       bool
	// noRedirect keeps a request pinned to the current endpoint, for calls
	// whose whole purpose is to ask this replica what it thinks.
	noRedirect bool
}

func (c *Client) endpoint() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

func (c *Client) setEndpoint(e string) {
	c.mu.Lock()
	c.current = e
	c.mu.Unlock()
}

// nextEndpoint rotates to the endpoint after the current one, used when a
// redirect gives no usable address.
func (c *Client) nextEndpoint() string {
	cur := c.endpoint()
	eps := c.opts.Endpoints
	for i, e := range eps {
		if e == cur {
			return eps[(i+1)%len(eps)]
		}
	}
	return eps[0]
}

func (c *Client) do(ctx context.Context, req request, out any) error {
	var last error
	for attempt := 0; attempt < c.opts.MaxAttempts; attempt++ {
		err := c.attempt(ctx, c.endpoint(), req, out)
		if err == nil {
			return nil
		}
		last = err

		var ke *Error
		if !errors.As(err, &ke) {
			// A transport failure means this endpoint is unreachable; another
			// may not be.
			if ctx.Err() != nil {
				return err
			}
			if !req.noRedirect {
				c.setEndpoint(c.nextEndpoint())
			}
			if err := c.sleep(ctx, attempt, 0); err != nil {
				return err
			}
			continue
		}

		if ke.Body.Code == api.CodeNotLeader && !req.noRedirect {
			// Prefer an address the server gave us, then a locally configured
			// mapping, and only then blind rotation. A client bootstrapped
			// from a single endpoint can still reach the leader this way.
			switch {
			case ke.Body.LeaderAddr != "":
				c.setEndpoint(normalizeEndpoint(ke.Body.LeaderAddr))
			case c.opts.NodeAddrs[ke.Body.Leader] != "":
				c.setEndpoint(c.opts.NodeAddrs[ke.Body.Leader])
			default:
				c.setEndpoint(c.nextEndpoint())
			}
			if err := c.sleep(ctx, attempt, 0); err != nil {
				return err
			}
			continue
		}
		if !ke.Body.Retryable {
			return err
		}
		if err := c.sleep(ctx, attempt, time.Duration(ke.Body.RetryAfterMS)*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("keystone: gave up after %d attempts: %w", c.opts.MaxAttempts, last)
}

func (c *Client) attempt(ctx context.Context, endpoint string, req request, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()

	var body io.Reader
	if req.body != nil {
		body = bytes.NewReader(req.body)
	}
	hr, err := http.NewRequestWithContext(ctx, req.method, endpoint+req.path, body)
	if err != nil {
		return err
	}
	c.decorate(hr)
	if req.contentType != "" {
		hr.Header.Set("Content-Type", req.contentType)
	} else if req.raw {
		hr.Header.Set("Content-Type", "application/octet-stream")
	}
	if req.admin && c.opts.AdminToken != "" {
		hr.Header.Set("X-Keystone-Admin-Token", c.opts.AdminToken)
	}
	for k, v := range req.headers {
		hr.Header.Set(k, v)
	}

	resp, err := c.http.Do(hr)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		return decodeError(resp)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) decorate(r *http.Request) {
	r.Header.Set("X-Keystone-Tenant", c.opts.Tenant)
	if c.opts.Class != "" {
		r.Header.Set("X-Keystone-Class", c.opts.Class)
	}
}

func decodeError(resp *http.Response) error {
	buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env api.ErrorResponse
	if err := json.Unmarshal(buf, &env); err != nil || env.Error.Code == "" {
		return &Error{Status: resp.StatusCode, Body: api.ErrorBody{
			Code:      api.CodeInternal,
			Message:   strings.TrimSpace(string(buf)),
			Retryable: resp.StatusCode >= 500,
		}}
	}
	if env.Error.Leader == "" {
		env.Error.Leader = resp.Header.Get("X-Keystone-Leader")
	}
	if env.Error.LeaderAddr == "" {
		env.Error.LeaderAddr = resp.Header.Get("X-Keystone-Leader-Addr")
	}
	return &Error{Status: resp.StatusCode, Body: env.Error}
}

// sleep waits out a backoff with full jitter. Full jitter, rather than a fixed
// delay, is what stops a fleet of clients from retrying in lockstep and
// re-creating the overload they are backing off from.
func (c *Client) sleep(ctx context.Context, attempt int, hint time.Duration) error {
	d := hint
	if d <= 0 {
		exp := float64(c.opts.BaseBackoff) * math.Pow(2, float64(attempt))
		if exp > float64(c.opts.MaxBackoff) {
			exp = float64(c.opts.MaxBackoff)
		}
		c.randM.Lock()
		d = time.Duration(c.rand.Int63n(int64(exp) + 1))
		c.randM.Unlock()
	}
	if d > c.opts.MaxBackoff {
		d = c.opts.MaxBackoff
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func prefixEnd(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			out := make([]byte, i+1)
			copy(out, b[:i+1])
			out[i]++
			return string(out)
		}
	}
	return ""
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
