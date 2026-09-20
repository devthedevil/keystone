package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/quota"
	"github.com/devthedevil/keystone/internal/stream"
	"github.com/devthedevil/keystone/internal/txn"
	"github.com/devthedevil/keystone/pkg/api"
)

// ---------------------------------------------------------------------------
// Tenant key mapping
//
// Every key is stored under a "<tenant>/" prefix. Isolation is therefore a
// property of the key space rather than of the request handlers: a bug in a
// handler cannot read across tenants, because the key it can construct is
// always inside its own prefix.
// ---------------------------------------------------------------------------

func physKey(tenant, key string) string { return tenant + "/" + key }

func logicalKey(tenant, phys string) string {
	return strings.TrimPrefix(phys, tenant+"/")
}

// prefixEnd returns the exclusive upper bound of a key prefix.
func prefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "" // prefix is all 0xff: scan to the end of the key space
}

func tenantRange(tenant, start, end string) (string, string) {
	lo := physKey(tenant, start)
	if end == "" {
		return lo, prefixEnd(tenant + "/")
	}
	return lo, physKey(tenant, end)
}

// ---------------------------------------------------------------------------
// Point reads and writes
// ---------------------------------------------------------------------------

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "key is required"})
		return
	}
	estimate := quota.ReadCost(1, 0)
	settle, ok := s.admit(w, r, estimate)
	if !ok {
		return
	}

	tenant := tenantOf(r)
	ts, release, err := s.coord.Snapshot(r.Context())
	if err != nil {
		settle(estimate)
		s.writeTxnError(w, r, err)
		return
	}
	defer release()

	kv, err := s.engine.Store().Get(physKey(tenant, key), ts)
	if err != nil {
		settle(estimate)
		if errors.Is(err, mvcc.ErrNotFound) {
			s.writeError(w, r, http.StatusNotFound, api.ErrorBody{Code: api.CodeNotFound, Message: "key not found"})
			return
		}
		s.writeTxnError(w, r, err)
		return
	}
	settle(quota.ReadCost(1, len(kv.Value)))

	w.Header().Set("ETag", strconv.FormatUint(kv.Version, 10))
	s.writeJSON(w, http.StatusOK, api.GetResponse{
		KV:     api.KV{Key: key, Value: kv.Value, Version: kv.Version},
		ReadTS: ts,
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "key is required"})
		return
	}
	value, err := io.ReadAll(io.LimitReader(r.Body, int64(s.cfg.MaxValueBytes)+1))
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "unreadable body"})
		return
	}
	if len(value) > s.cfg.MaxValueBytes {
		s.writeError(w, r, http.StatusRequestEntityTooLarge, api.ErrorBody{
			Code:    api.CodeInvalid,
			Message: fmt.Sprintf("value exceeds the %d byte limit; the control plane stores metadata, not blobs", s.cfg.MaxValueBytes),
		})
		return
	}

	estimate := quota.WriteCost(1, len(value))
	settle, ok := s.admit(w, r, estimate)
	if !ok {
		return
	}
	defer settle(estimate)

	tenant := tenantOf(r)
	cmd := &txn.Command{
		Op:        txn.OpCommit,
		Tenant:    tenant,
		TxnID:     r.Header.Get("Idempotency-Key"),
		Mutations: []txn.Mutation{{Key: physKey(tenant, key), Value: value}},
	}
	// Conditional writes map onto HTTP's own concurrency headers, so ordinary
	// HTTP tooling can do compare-and-swap without learning a new vocabulary.
	if m := r.Header.Get("If-Match"); m != "" {
		v, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "If-Match must be a version number"})
			return
		}
		cmd.Conditions = append(cmd.Conditions, txn.Condition{Key: physKey(tenant, key), Type: txn.CondVersion, Version: v})
	}
	if r.Header.Get("If-None-Match") == "*" {
		cmd.Conditions = append(cmd.Conditions, txn.Condition{Key: physKey(tenant, key), Type: txn.CondAbsent})
	}

	res, err := s.coord.CommitRaw(r.Context(), cmd)
	if err != nil {
		s.writeTxnError(w, r, err)
		return
	}
	w.Header().Set("ETag", strconv.FormatUint(res.CommitTS, 10))
	s.writeJSON(w, http.StatusOK, api.TxnResponse{
		Committed: res.Committed, CommitTS: res.CommitTS, Duplicate: res.Duplicate,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	estimate := quota.WriteCost(1, 0)
	settle, ok := s.admit(w, r, estimate)
	if !ok {
		return
	}
	defer settle(estimate)

	tenant := tenantOf(r)
	cmd := &txn.Command{
		Op:        txn.OpCommit,
		Tenant:    tenant,
		TxnID:     r.Header.Get("Idempotency-Key"),
		Mutations: []txn.Mutation{{Key: physKey(tenant, key), Delete: true}},
	}
	if m := r.Header.Get("If-Match"); m != "" {
		v, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "If-Match must be a version number"})
			return
		}
		cmd.Conditions = append(cmd.Conditions, txn.Condition{Key: physKey(tenant, key), Type: txn.CondVersion, Version: v})
	}
	res, err := s.coord.CommitRaw(r.Context(), cmd)
	if err != nil {
		s.writeTxnError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, api.TxnResponse{Committed: res.Committed, CommitTS: res.CommitTS, Duplicate: res.Duplicate})
}

// ---------------------------------------------------------------------------
// Range reads
// ---------------------------------------------------------------------------

func (s *Server) handleRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	start, end := q.Get("start"), q.Get("end")
	if p := q.Get("prefix"); p != "" {
		start, end = p, prefixEnd(p)
	}
	limit := s.cfg.MaxScanLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "limit must be a positive integer"})
			return
		}
		if n < limit {
			limit = n
		}
	}

	// A scan is estimated at its limit and settled at what it actually touched,
	// so a scan over a sparse range is not overcharged and a scan that walks a
	// million tombstones is not undercharged.
	estimate := quota.ReadCost(limit, 0)
	settle, ok := s.admit(w, r, estimate)
	if !ok {
		return
	}

	tenant := tenantOf(r)
	lo, hi := tenantRange(tenant, start, end)

	ts, release, err := s.coord.Snapshot(r.Context())
	if err != nil {
		settle(estimate)
		s.writeTxnError(w, r, err)
		return
	}
	defer release()

	res := s.engine.Store().Scan(lo, hi, limit, ts)
	bytes := 0
	out := make([]api.KV, 0, len(res.KVs))
	for _, kv := range res.KVs {
		bytes += len(kv.Value)
		out = append(out, api.KV{Key: logicalKey(tenant, kv.Key), Value: kv.Value, Version: kv.Version})
	}
	settle(quota.ReadCost(res.ScannedKeys, bytes))

	resp := api.RangeResponse{KVs: out, More: res.More, ReadTS: ts, ScannedKeys: res.ScannedKeys}
	if res.More && len(out) > 0 {
		resp.NextStart = out[len(out)-1].Key + "\x00"
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

func (s *Server) handleTxn(w http.ResponseWriter, r *http.Request) {
	var req api.TxnRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "malformed transaction: " + err.Error()})
		return
	}
	if len(req.Mutations) == 0 && len(req.Conditions) == 0 {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{
			Code:    api.CodeInvalid,
			Message: "a transaction must contain at least one mutation or condition; use /v1/kv or /v1/range for reads",
		})
		return
	}

	tenant := tenantOf(r)
	bytes := 0
	for _, m := range req.Mutations {
		if len(m.Value) > s.cfg.MaxValueBytes {
			s.writeError(w, r, http.StatusRequestEntityTooLarge, api.ErrorBody{
				Code: api.CodeInvalid, Message: "value exceeds the configured limit",
			})
			return
		}
		bytes += len(m.Value)
	}

	estimate := quota.WriteCost(len(req.Mutations), bytes)
	settle, ok := s.admit(w, r, estimate)
	if !ok {
		return
	}
	defer settle(estimate)

	cmd := &txn.Command{
		Op:     txn.OpCommit,
		Tenant: tenant,
		TxnID:  req.TxnID,
		ReadTS: req.ReadTS,
	}
	for _, rd := range req.Reads {
		cmd.Reads = append(cmd.Reads, txn.ReadRef{Key: physKey(tenant, rd.Key), Version: rd.Version})
	}
	for _, rg := range req.Ranges {
		lo, hi := tenantRange(tenant, rg.Start, rg.End)
		cmd.Ranges = append(cmd.Ranges, txn.RangeRef{Start: lo, End: hi})
	}
	for _, c := range req.Conditions {
		cmd.Conditions = append(cmd.Conditions, txn.Condition{
			Key: physKey(tenant, c.Key), Type: txn.CondType(c.Type), Version: c.Version,
		})
	}
	for _, m := range req.Mutations {
		cmd.Mutations = append(cmd.Mutations, txn.Mutation{
			Key: physKey(tenant, m.Key), Value: m.Value, Delete: m.Delete,
		})
	}

	res, err := s.coord.CommitRaw(r.Context(), cmd)
	if err != nil {
		s.writeTxnError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, api.TxnResponse{
		Committed: res.Committed, CommitTS: res.CommitTS, Duplicate: res.Duplicate,
	})
}

// ---------------------------------------------------------------------------
// Change feed
// ---------------------------------------------------------------------------

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, r, http.StatusInternalServerError, api.ErrorBody{Code: api.CodeInternal, Message: "streaming unsupported"})
		return
	}
	var from uint64
	if v := r.URL.Query().Get("from"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "from must be a log index"})
			return
		}
		from = n
	}
	tenant := tenantOf(r)
	prefix := physKey(tenant, r.URL.Query().Get("prefix"))

	sub, err := s.engine.Broker().Subscribe(from, prefix)
	if err != nil {
		if errors.Is(err, stream.ErrCursorExpired) {
			// Telling the consumer exactly what to do next is the difference
			// between a self-healing pipeline and a paged operator.
			s.writeError(w, r, http.StatusGone, api.ErrorBody{
				Code:    api.CodeInvalid,
				Message: "resume cursor is older than the retention buffer; re-bootstrap with /v1/range and resume from its read_ts",
			})
			return
		}
		s.writeError(w, r, http.StatusInternalServerError, api.ErrorBody{Code: api.CodeInternal, Message: err.Error()})
		return
	}
	defer sub.Close()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			// A comment frame keeps intermediaries from reaping an idle feed.
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case c, open := <-sub.Changes():
			if !open {
				if err := sub.Err(); err != nil && errors.Is(err, stream.ErrSlowConsumer) {
					fmt.Fprintf(w, "event: error\ndata: {\"code\":\"slow_consumer\",\"message\":\"resume from the last seq you processed\"}\n\n")
					flusher.Flush()
				}
				return
			}
			ev := api.ChangeEvent{
				Seq: c.Seq, Kind: string(c.Kind), Tenant: c.Tenant,
				Key: logicalKey(tenant, c.Key), Value: c.Value, TxnID: c.TxnID, WallTime: c.WallTime,
			}
			buf, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "id: %d\nevent: change\ndata: %s\n\n", c.Seq, buf)
			flusher.Flush()
		}
	}
}

// ---------------------------------------------------------------------------
// Status, health, metrics
// ---------------------------------------------------------------------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	est := s.engine.Stats()
	store := s.engine.Store().Stats()

	mi := make(map[string]uint64, len(st.MatchIndex))
	for k, v := range st.MatchIndex {
		mi[string(k)] = v
	}
	s.writeJSON(w, http.StatusOK, api.StatusResponse{
		NodeID:          string(st.ID),
		State:           st.State,
		Term:            st.Term,
		Leader:          string(st.Leader),
		LeaderAddr:      s.leaderAddr(string(st.Leader)),
		LeaseValid:      st.LeaseValid,
		CommitIndex:     st.CommitIndex,
		LastApplied:     st.LastApplied,
		MatchIndex:      mi,
		Keys:            store.Keys,
		Versions:        store.Versions,
		ApproxBytes:     store.ApproxBytes,
		Commits:         est.Commits,
		Aborts:          est.Aborts,
		OpenSnapshots:   s.coord.OpenSnapshots(),
		GCWatermark:     store.LastWatermark,
		Watchers:        s.engine.Broker().Stats().Watchers,
		AdmissionHealth: s.limiter.Health(),
		Version:         Version,
		Uptime:          time.Since(s.started),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := s.reg.WriteTo(w); err != nil {
		s.log.Debug("metrics write failed", "err", err)
	}
	// Per-tenant counters are rendered separately because the tenant set is
	// discovered at runtime rather than declared at startup.
	for _, t := range s.limiter.Stats() {
		fmt.Fprintf(w, "keystone_tenant_admitted_total{tenant=%q} %d\n", t.Tenant, t.Admitted)
		fmt.Fprintf(w, "keystone_tenant_throttled_total{tenant=%q} %d\n", t.Tenant, t.Throttled)
		fmt.Fprintf(w, "keystone_tenant_shed_total{tenant=%q} %d\n", t.Tenant, t.Shed)
		fmt.Fprintf(w, "keystone_tenant_tokens{tenant=%q} %g\n", t.Tenant, t.Tokens)
		fmt.Fprintf(w, "keystone_tenant_in_flight{tenant=%q} %d\n", t.Tenant, t.InFlight)
	}
}

// handleLive answers "is this process alive". It must never depend on quorum:
// a liveness probe that fails during an election would restart every replica at
// the worst possible moment.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node": s.cfg.NodeID})
}

// handleReady answers "should this replica receive client traffic". Unlike
// liveness, it does depend on the replica being able to serve.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	ready := st.Leader != "" && st.CommitIndex-st.LastApplied < 1000
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	s.writeJSON(w, code, map[string]any{
		"ready":        ready,
		"state":        st.State,
		"leader_known": st.Leader != "",
		"apply_lag":    st.CommitIndex - st.LastApplied,
	})
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

func (s *Server) handleAdminGC(w http.ResponseWriter, r *http.Request) {
	res, err := s.coord.RunGC(r.Context())
	if err != nil {
		s.writeTxnError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleQuotaList(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.limiter.Stats())
}

func (s *Server) handleQuotaSet(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	if !tenantPattern.MatchString(tenant) {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "invalid tenant identifier"})
		return
	}
	var q api.TenantQuota
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&q); err != nil {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: err.Error()})
		return
	}
	if q.RatePerSec <= 0 || q.Burst <= 0 {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{Code: api.CodeInvalid, Message: "rate_per_sec and burst must be positive"})
		return
	}
	q.Tenant = tenant
	if q.MaxConcurrent <= 0 {
		q.MaxConcurrent = s.cfg.DefaultMaxConcurrent
	}

	// A quota change is replicated, not applied locally. Local application
	// would make a tenant's limit depend on which replica the operator
	// happened to reach, and would be silently lost on failover or restart —
	// exactly when an overload response matters most.
	body, err := json.Marshal(q)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, api.ErrorBody{Code: api.CodeInternal, Message: err.Error()})
		return
	}
	cmd := &txn.Command{
		Op:        txn.OpCommit,
		Tenant:    systemTenant,
		Mutations: []txn.Mutation{{Key: txn.SystemPrefix + "quota/" + tenant, Value: body}},
	}
	if _, err := s.coord.CommitRaw(r.Context(), cmd); err != nil {
		s.writeTxnError(w, r, err)
		return
	}
	s.log.Info("tenant quota updated", "tenant", tenant, "rate", q.RatePerSec, "burst", q.Burst, "max_concurrent", q.MaxConcurrent)
	s.writeJSON(w, http.StatusOK, q)
}

func (s *Server) handleStepDown(w http.ResponseWriter, r *http.Request) {
	ok := s.node.StepDown()
	code := http.StatusOK
	if !ok {
		code = http.StatusConflict
	}
	s.writeJSON(w, code, map[string]any{"stepped_down": ok, "node": s.cfg.NodeID})
}
