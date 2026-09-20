// Package cluster assembles a complete Keystone replica from its parts and
// owns its lifecycle.
//
// Client traffic and peer replication traffic are served by two separate
// listeners. That separation is deliberate: it means a flood of client
// requests cannot starve the heartbeats that keep the group's leader alive,
// which is a failure mode where a load spike turns into an election storm.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/devthedevil/keystone/internal/metrics"
	"github.com/devthedevil/keystone/internal/mvcc"
	"github.com/devthedevil/keystone/internal/quota"
	"github.com/devthedevil/keystone/internal/raft"
	"github.com/devthedevil/keystone/internal/server"
	"github.com/devthedevil/keystone/internal/stream"
	"github.com/devthedevil/keystone/internal/txn"
)

// Options configures a replica.
type Options struct {
	NodeID string
	// Peers maps peer node IDs to their peer-plane base URLs.
	Peers map[string]string
	// ClientPeers optionally maps node IDs to their client-plane base URLs so
	// that a redirect can name an address rather than an opaque node ID.
	ClientPeers map[string]string

	ClientAddr string
	PeerAddr   string
	// ClientListener and PeerListener override address binding, which lets
	// tests take ephemeral ports before the cluster is configured.
	ClientListener net.Listener
	PeerListener   net.Listener

	// DataDir enables the crash-safe file log. Empty means an in-memory log,
	// which is only appropriate for tests.
	DataDir string
	Fsync   bool

	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	GCInterval        time.Duration
	RetentionIndexes  uint64

	AdminToken    string
	MaxValueBytes int

	DefaultRatePerSec    float64
	DefaultBurst         float64
	DefaultMaxConcurrent int
	LatencyTarget        time.Duration

	Logger *slog.Logger
}

func (o *Options) withDefaults() {
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 100 * time.Millisecond
	}
	if o.ElectionTimeout <= 0 {
		o.ElectionTimeout = 1 * time.Second
	}
	if o.GCInterval <= 0 {
		o.GCInterval = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.DefaultRatePerSec <= 0 {
		o.DefaultRatePerSec = quota.DefaultPolicy.RatePerSec
	}
	if o.DefaultBurst <= 0 {
		o.DefaultBurst = quota.DefaultPolicy.Burst
	}
	if o.DefaultMaxConcurrent <= 0 {
		o.DefaultMaxConcurrent = quota.DefaultPolicy.MaxConcurrent
	}
}

// Node is a running replica.
type Node struct {
	opts    Options
	log     *slog.Logger
	storage raft.Storage
	raft    *raft.Node
	engine  *txn.Engine
	coord   *txn.Coordinator
	limiter *quota.Limiter
	reg     *metrics.Registry
	srv     *server.Server

	clientSrv *http.Server
	peerSrv   *http.Server
	clientLn  net.Listener
	peerLn    net.Listener
}

// New builds a replica without starting it.
func New(opts Options) (*Node, error) {
	opts.withDefaults()
	if opts.NodeID == "" {
		return nil, errors.New("cluster: NodeID is required")
	}
	log := opts.Logger.With("node", opts.NodeID)

	var st raft.Storage
	if opts.DataDir == "" {
		log.Warn("using an in-memory log: committed data will not survive a restart")
		st = raft.NewMemStorage()
	} else {
		fs, err := raft.OpenFileStorage(opts.DataDir, opts.Fsync)
		if err != nil {
			return nil, fmt.Errorf("cluster: open log: %w", err)
		}
		st = fs
	}

	store := mvcc.New()
	broker := stream.NewBroker(stream.Options{})

	limiter := quota.New(quota.Options{
		Default: quota.TenantPolicy{
			RatePerSec:    opts.DefaultRatePerSec,
			Burst:         opts.DefaultBurst,
			MaxConcurrent: opts.DefaultMaxConcurrent,
		},
		LatencyTarget: opts.LatencyTarget,
	})

	// Replicated configuration is applied on the apply loop of every replica,
	// which is what makes a quota change cluster-wide, durable across a
	// restart, and identical on the replica that takes over after a failover.
	systemHook := func(key string, value []byte, deleted bool) {
		tenant, ok := strings.CutPrefix(key, "quota/")
		if !ok {
			return
		}
		if deleted {
			limiter.ResetPolicy(tenant)
			return
		}
		var q struct {
			RatePerSec    float64 `json:"rate_per_sec"`
			Burst         float64 `json:"burst"`
			MaxConcurrent int     `json:"max_concurrent"`
		}
		if err := json.Unmarshal(value, &q); err != nil {
			log.Error("ignoring unparseable replicated quota", "tenant", tenant, "err", err)
			return
		}
		limiter.SetPolicy(tenant, quota.TenantPolicy{
			RatePerSec: q.RatePerSec, Burst: q.Burst, MaxConcurrent: q.MaxConcurrent,
		})
	}

	engine := txn.NewEngine(store, broker, txn.EngineOptions{
		SystemHook: systemHook,
		Logger:     opts.Logger,
	})

	peerIDs := make([]raft.NodeID, 0, len(opts.Peers))
	peerAddrs := make(map[raft.NodeID]string, len(opts.Peers))
	for id, addr := range opts.Peers {
		if id == opts.NodeID {
			continue
		}
		peerIDs = append(peerIDs, raft.NodeID(id))
		peerAddrs[raft.NodeID(id)] = addr
	}
	transport := raft.NewHTTPTransport(peerAddrs, opts.ElectionTimeout)

	rn, err := raft.NewNode(raft.Config{
		ID:                raft.NodeID(opts.NodeID),
		Peers:             peerIDs,
		Storage:           st,
		Transport:         transport,
		FSM:               engine,
		HeartbeatInterval: opts.HeartbeatInterval,
		ElectionTimeout:   opts.ElectionTimeout,
		Logger:            opts.Logger,
	})
	if err != nil {
		return nil, err
	}

	coord := txn.NewCoordinator(rn, engine, txn.Options{
		RetentionIndexes: opts.RetentionIndexes,
		GCInterval:       opts.GCInterval,
		Logger:           opts.Logger,
	})

	reg := metrics.NewRegistry()
	srv := server.New(server.Config{
		NodeID:               opts.NodeID,
		AdminToken:           opts.AdminToken,
		MaxValueBytes:        opts.MaxValueBytes,
		ClientPeers:          opts.ClientPeers,
		DefaultMaxConcurrent: opts.DefaultMaxConcurrent,
		Logger:               opts.Logger,
	}, rn, coord, engine, limiter, reg)

	return &Node{
		opts: opts, log: log, storage: st, raft: rn, engine: engine,
		coord: coord, limiter: limiter, reg: reg, srv: srv,
	}, nil
}

// Handler exposes the client router, used by tests.
func (n *Node) Handler() http.Handler { return n.srv.Handler() }

// Raft exposes the replica for tests and tooling.
func (n *Node) Raft() *raft.Node { return n.raft }

// Coordinator exposes the transaction API.
func (n *Node) Coordinator() *txn.Coordinator { return n.coord }

// Limiter exposes admission control.
func (n *Node) Limiter() *quota.Limiter { return n.limiter }

// ClientAddr returns the bound client address once started.
func (n *Node) ClientAddr() string {
	if n.clientLn == nil {
		return n.opts.ClientAddr
	}
	return n.clientLn.Addr().String()
}

// PeerAddr returns the bound peer address once started.
func (n *Node) PeerAddr() string {
	if n.peerLn == nil {
		return n.opts.PeerAddr
	}
	return n.peerLn.Addr().String()
}

// Start binds listeners and begins replication.
func (n *Node) Start() error {
	var err error
	n.clientLn = n.opts.ClientListener
	if n.clientLn == nil {
		if n.clientLn, err = net.Listen("tcp", n.opts.ClientAddr); err != nil {
			return fmt.Errorf("cluster: bind client address: %w", err)
		}
	}
	n.peerLn = n.opts.PeerListener
	if n.peerLn == nil {
		if n.peerLn, err = net.Listen("tcp", n.opts.PeerAddr); err != nil {
			return fmt.Errorf("cluster: bind peer address: %w", err)
		}
	}

	n.raft.Start()
	n.coord.Start()

	n.clientSrv = &http.Server{
		Handler:           n.srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// No write timeout: the change feed is a long-lived response. Request
		// deadlines are enforced per handler through the request context.
		IdleTimeout: 120 * time.Second,
	}
	n.peerSrv = &http.Server{
		Handler:           raft.PeerHandler(n.raft),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		if err := n.clientSrv.Serve(n.clientLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.log.Error("client listener stopped", "err", err)
		}
	}()
	go func() {
		if err := n.peerSrv.Serve(n.peerLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.log.Error("peer listener stopped", "err", err)
		}
	}()

	n.log.Info("replica started", "client_addr", n.ClientAddr(), "peer_addr", n.PeerAddr(), "peers", len(n.opts.Peers))
	return nil
}

// Shutdown drains the replica in the order that minimises client impact:
// stop taking new client work, hand off leadership, then stop replicating.
//
// Stepping down before closing the peer listener matters. If the process exits
// while still leader, every client write fails until the remaining replicas
// notice the silence and run an election — one election timeout of hard
// downtime on every deployment, multiplied by the number of replicas.
func (n *Node) Shutdown(ctx context.Context) error {
	n.log.Info("shutdown requested")

	if n.clientSrv != nil {
		if err := n.clientSrv.Shutdown(ctx); err != nil {
			n.log.Warn("client listener drain timed out", "err", err)
		}
	}
	if n.raft.StepDown() {
		n.log.Info("leadership relinquished; waiting for a successor")
		n.waitForSuccessor(ctx)
	}
	n.coord.Stop()
	if n.peerSrv != nil {
		if err := n.peerSrv.Shutdown(ctx); err != nil {
			n.log.Warn("peer listener drain timed out", "err", err)
		}
	}
	n.raft.Stop()
	if err := n.storage.Close(); err != nil {
		return err
	}
	n.log.Info("shutdown complete")
	return nil
}

func (n *Node) waitForSuccessor(ctx context.Context) {
	deadline := time.Now().Add(2 * n.opts.ElectionTimeout)
	for time.Now().Before(deadline) {
		if leader := n.raft.Leader(); leader != "" && leader != raft.NodeID(n.opts.NodeID) {
			n.log.Info("successor elected", "leader", leader)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	n.log.Warn("no successor observed before the drain deadline; proceeding")
}
