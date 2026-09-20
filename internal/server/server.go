// Package server exposes Keystone over HTTP.
//
// Layering, outermost first: panic recovery, request identity, telemetry,
// tenancy, admission control, then the handler. Admission runs inside the
// telemetry layer on purpose, so that shed requests still show up in latency
// and rate metrics; a throttle that is invisible on the dashboard is how a
// capacity problem becomes an outage.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/devthedevil/keystone/internal/metrics"
	"github.com/devthedevil/keystone/internal/quota"
	"github.com/devthedevil/keystone/internal/raft"
	"github.com/devthedevil/keystone/internal/txn"
	"github.com/devthedevil/keystone/pkg/api"
)

// Version is stamped at build time via -ldflags.
var Version = "dev"

// tenantPattern bounds tenant identifiers. Tenant names become key prefixes,
// so an unvalidated name would let one tenant address another's key space.
var tenantPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

// systemTenant owns replicated configuration. It deliberately fails
// tenantPattern, so no client request can ever be routed into it.
const systemTenant = "__system"

// Config configures the HTTP surface.
type Config struct {
	NodeID string
	// AdminToken, when set, is required by /v1/admin endpoints.
	AdminToken string
	// MaxValueBytes rejects oversized values before they reach the log.
	MaxValueBytes int
	// MaxScanLimit bounds a single range request.
	MaxScanLimit int
	// DefaultMaxConcurrent fills in an unset per-tenant concurrency limit.
	DefaultMaxConcurrent int
	// ClientPeers maps node IDs to their client-plane base URLs. It is
	// advisory: it exists so a redirect can name an address a client can
	// actually dial, rather than an opaque node ID.
	ClientPeers map[string]string
	Logger      *slog.Logger
}

// leaderAddr resolves a node ID to a dialable client address, or "" if this
// replica was not told one.
func (s *Server) leaderAddr(id string) string {
	if id == "" {
		return ""
	}
	return s.cfg.ClientPeers[id]
}

func (c *Config) withDefaults() {
	if c.MaxValueBytes <= 0 {
		c.MaxValueBytes = 1 << 20 // 1 MiB: the log is metadata, not blobs
	}
	if c.MaxScanLimit <= 0 {
		c.MaxScanLimit = 1000
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Server is the HTTP front end for one replica.
type Server struct {
	cfg     Config
	node    *raft.Node
	coord   *txn.Coordinator
	engine  *txn.Engine
	limiter *quota.Limiter
	reg     *metrics.Registry
	log     *slog.Logger
	started time.Time

	reqs     *metrics.Counter
	latency  *metrics.Histogram
	errCount func(code string) *metrics.Counter
}

// New builds a server.
func New(cfg Config, node *raft.Node, coord *txn.Coordinator, engine *txn.Engine, limiter *quota.Limiter, reg *metrics.Registry) *Server {
	cfg.withDefaults()
	s := &Server{
		cfg:     cfg,
		node:    node,
		coord:   coord,
		engine:  engine,
		limiter: limiter,
		reg:     reg,
		log:     cfg.Logger.With("component", "server"),
		started: time.Now(),
	}
	s.registerMetrics()
	return s
}

func (s *Server) registerMetrics() {
	l := metrics.Labels{"node": s.cfg.NodeID}
	s.reqs = s.reg.Counter("keystone_requests_total", "Client requests received.", l)
	s.latency = s.reg.Histogram("keystone_request_duration_seconds", "Client request latency.", nil, l)
	s.errCount = func(code string) *metrics.Counter {
		return s.reg.Counter("keystone_errors_total", "Client errors by code.",
			metrics.Labels{"node": s.cfg.NodeID, "code": code})
	}

	// Live gauges sampled at scrape time. Exporting Raft internals is what
	// makes replication lag alarmable instead of merely observable after the
	// fact in logs.
	s.reg.GaugeFunc("keystone_raft_term", "Current Raft term.", l, func() float64 {
		return float64(s.node.Status().Term)
	})
	s.reg.GaugeFunc("keystone_raft_is_leader", "1 when this replica is leader.", l, func() float64 {
		if s.node.IsLeader() {
			return 1
		}
		return 0
	})
	s.reg.GaugeFunc("keystone_raft_lease_valid", "1 when the leader lease is valid.", l, func() float64 {
		if s.node.Status().LeaseValid {
			return 1
		}
		return 0
	})
	s.reg.GaugeFunc("keystone_raft_commit_index", "Raft commit index.", l, func() float64 {
		return float64(s.node.Status().CommitIndex)
	})
	s.reg.GaugeFunc("keystone_raft_apply_lag", "Committed entries not yet applied.", l, func() float64 {
		st := s.node.Status()
		return float64(st.CommitIndex - st.LastApplied)
	})
	s.reg.GaugeFunc("keystone_store_keys", "Live keys.", l, func() float64 {
		return float64(s.engine.Store().Stats().Keys)
	})
	s.reg.GaugeFunc("keystone_store_versions", "Stored versions including shadowed ones.", l, func() float64 {
		return float64(s.engine.Store().Stats().Versions)
	})
	s.reg.GaugeFunc("keystone_store_bytes", "Approximate resident bytes.", l, func() float64 {
		return float64(s.engine.Store().Stats().ApproxBytes)
	})
	s.reg.GaugeFunc("keystone_gc_watermark", "Garbage collection watermark.", l, func() float64 {
		return float64(s.engine.Store().Stats().LastWatermark)
	})
	s.reg.GaugeFunc("keystone_open_snapshots", "Pinned read snapshots.", l, func() float64 {
		return float64(s.coord.OpenSnapshots())
	})
	s.reg.GaugeFunc("keystone_admission_health", "AIMD admission multiplier.", l, func() float64 {
		return s.limiter.Health()
	})
	s.reg.GaugeFunc("keystone_stream_watchers", "Active change-feed subscribers.", l, func() float64 {
		return float64(s.engine.Broker().Stats().Watchers)
	})
	s.reg.GaugeFunc("keystone_commits_total_gauge", "Committed transactions.", l, func() float64 {
		return float64(s.engine.Stats().Commits)
	})
	s.reg.GaugeFunc("keystone_aborts_total_gauge", "Aborted transactions.", l, func() float64 {
		return float64(s.engine.Stats().Aborts)
	})
}

// Handler returns the client-facing router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/kv/{key...}", s.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key...}", s.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.handleDelete)
	mux.HandleFunc("GET /v1/range", s.handleRange)
	mux.HandleFunc("POST /v1/txn", s.handleTxn)
	mux.HandleFunc("GET /v1/stream", s.handleStream)
	mux.HandleFunc("GET /v1/status", s.handleStatus)

	mux.HandleFunc("POST /v1/admin/gc", s.requireAdmin(s.handleAdminGC))
	mux.HandleFunc("GET /v1/admin/quota", s.requireAdmin(s.handleQuotaList))
	mux.HandleFunc("PUT /v1/admin/quota/{tenant}", s.requireAdmin(s.handleQuotaSet))
	mux.HandleFunc("POST /v1/admin/stepdown", s.requireAdmin(s.handleStepDown))

	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /health/live", s.handleLive)
	mux.HandleFunc("GET /health/ready", s.handleReady)

	return s.recoverer(s.telemetry(mux))
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxTenant
	ctxClass
)

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic serving request", "path", r.URL.Path, "panic", v)
				s.writeError(w, r, http.StatusInternalServerError, api.ErrorBody{
					Code: api.CodeInternal, Message: "internal error",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) telemetry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = newID()
		}
		tenant := r.Header.Get("X-Keystone-Tenant")
		if tenant == "" {
			tenant = "default"
		}
		class := quota.ParseClass(r.Header.Get("X-Keystone-Class"))

		ctx := context.WithValue(r.Context(), ctxRequestID, rid)
		ctx = context.WithValue(ctx, ctxTenant, tenant)
		ctx = context.WithValue(ctx, ctxClass, class)
		w.Header().Set("X-Request-Id", rid)

		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		s.reqs.Inc()
		next.ServeHTTP(sw, r.WithContext(ctx))

		elapsed := time.Since(start)
		// The change feed is a long-lived stream; feeding its duration to the
		// overload controller would permanently mark the service as slow.
		if r.URL.Path != "/v1/stream" {
			s.latency.Observe(elapsed.Seconds())
			s.limiter.Observe(elapsed)
		}
		if sw.code >= 400 {
			s.log.Debug("request failed", "path", r.URL.Path, "status", sw.code,
				"tenant", tenant, "request_id", rid, "duration", elapsed)
		}
	})
}

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AdminToken != "" && r.Header.Get("X-Keystone-Admin-Token") != s.cfg.AdminToken {
			s.writeError(w, r, http.StatusForbidden, api.ErrorBody{
				Code: api.CodeInvalid, Message: "admin token required",
			})
			return
		}
		h(w, r)
	}
}

func requestID(r *http.Request) string {
	if v, ok := r.Context().Value(ctxRequestID).(string); ok {
		return v
	}
	return ""
}

func tenantOf(r *http.Request) string {
	if v, ok := r.Context().Value(ctxTenant).(string); ok {
		return v
	}
	return "default"
}

func classOf(r *http.Request) quota.Class {
	if v, ok := r.Context().Value(ctxClass).(quota.Class); ok {
		return v
	}
	return quota.ClassNormal
}

// admit performs admission control. The returned settle function must be
// called with the true cost once the work is done.
func (s *Server) admit(w http.ResponseWriter, r *http.Request, estimate quota.Cost) (func(quota.Cost), bool) {
	tenant := tenantOf(r)
	if !tenantPattern.MatchString(tenant) {
		s.writeError(w, r, http.StatusBadRequest, api.ErrorBody{
			Code: api.CodeInvalid, Message: "invalid tenant identifier",
		})
		return nil, false
	}
	d := s.limiter.Admit(tenant, classOf(r), estimate)
	if !d.Allowed {
		code := api.CodeThrottled
		if strings.HasPrefix(d.Reason, "overloaded") {
			code = api.CodeOverloaded
		}
		w.Header().Set("Retry-After", strconv.FormatFloat(d.RetryAfter.Seconds(), 'f', 3, 64))
		s.writeError(w, r, http.StatusTooManyRequests, api.ErrorBody{
			Code:         code,
			Message:      d.Reason,
			RetryAfterMS: d.RetryAfter.Milliseconds(),
			Retryable:    true,
		})
		return nil, false
	}
	settled := false
	return func(actual quota.Cost) {
		if settled {
			return
		}
		settled = true
		s.limiter.Charge(tenant, estimate, actual)
	}, true
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Debug("response write failed", "err", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, body api.ErrorBody) {
	body.RequestID = requestID(r)
	s.errCount(body.Code).Inc()
	s.writeJSON(w, status, api.ErrorResponse{Error: body})
}

// writeTxnError maps internal failures onto the wire contract. Every branch
// answers one question for the client: may I retry this exact request?
func (s *Server) writeTxnError(w http.ResponseWriter, r *http.Request, err error) {
	var notLeader *raft.NotLeaderError
	switch {
	case errors.As(err, &notLeader):
		s.writeNotLeader(w, r, string(notLeader.Leader), err.Error())
	case errors.Is(err, txn.ErrReadOnlyReplica):
		// The coordinator refused before raft was consulted, so there is no
		// NotLeaderError to unwrap; the answer to the client is the same.
		s.writeNotLeader(w, r, string(s.node.Status().Leader), err.Error())
	case errors.Is(err, raft.ErrNoLease):
		s.writeError(w, r, http.StatusServiceUnavailable, api.ErrorBody{
			Code:         api.CodeNoLease,
			Message:      "leader lease not currently held",
			RetryAfterMS: 50,
			Retryable:    true,
		})
	case errors.Is(err, raft.ErrLeadershipLost):
		// The outcome is genuinely unknown. The client must retry with the same
		// idempotency key, which the engine will deduplicate.
		s.writeError(w, r, http.StatusServiceUnavailable, api.ErrorBody{
			Code:      api.CodeUnavailable,
			Message:   "leadership changed during commit; retry with the same txn_id",
			Retryable: true,
		})
	case errors.Is(err, raft.ErrStopped):
		s.writeError(w, r, http.StatusServiceUnavailable, api.ErrorBody{
			Code: api.CodeUnavailable, Message: "replica is shutting down", Retryable: true,
		})
	case errors.Is(err, context.DeadlineExceeded):
		s.writeError(w, r, http.StatusGatewayTimeout, api.ErrorBody{
			Code:      api.CodeUnavailable,
			Message:   "commit timed out; retry with the same txn_id",
			Retryable: true,
		})
	default:
		var ce *txn.ConflictError
		if errors.As(err, &ce) {
			code := api.CodeConflict
			status := http.StatusConflict
			if ce.Conflict.Kind == "condition" {
				code = api.CodePrecondition
				status = http.StatusPreconditionFailed
			}
			s.writeJSONConflict(w, r, status, code, ce.Conflict)
			return
		}
		s.log.Error("unhandled error", "err", err, "request_id", requestID(r))
		s.writeError(w, r, http.StatusInternalServerError, api.ErrorBody{
			Code: api.CodeInternal, Message: err.Error(),
		})
	}
}

// writeNotLeader is the single place a redirect is rendered, so the leader
// hint, the header and the address resolution cannot drift apart.
func (s *Server) writeNotLeader(w http.ResponseWriter, r *http.Request, leader, msg string) {
	addr := s.leaderAddr(leader)
	if leader != "" {
		w.Header().Set("X-Keystone-Leader", leader)
	}
	if addr != "" {
		w.Header().Set("X-Keystone-Leader-Addr", addr)
	}
	s.writeError(w, r, http.StatusServiceUnavailable, api.ErrorBody{
		Code:       api.CodeNotLeader,
		Message:    msg,
		Leader:     leader,
		LeaderAddr: addr,
		// A redirect is only retryable if there is somewhere to redirect to.
		// During an election there is not, and saying so stops a client from
		// hammering a replica that cannot help it.
		Retryable: true,
	})
}

func (s *Server) writeJSONConflict(w http.ResponseWriter, r *http.Request, status int, code string, c txn.Conflict) {
	s.errCount(code).Inc()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error    api.ErrorBody `json:"error"`
		Conflict api.Conflict  `json:"conflict"`
	}{
		Error: api.ErrorBody{
			Code:      code,
			Message:   fmt.Sprintf("transaction aborted: %s conflict on %q", c.Kind, c.Key),
			RequestID: requestID(r),
			Retryable: code == api.CodeConflict,
		},
		Conflict: api.Conflict{
			Kind: string(c.Kind), Key: c.Key, Expected: c.Expected, Observed: c.Observed,
		},
	})
}

func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
