// Command keystoned runs a Keystone replica.
//
// Configuration is flags plus environment variables, with flags winning. That
// is deliberate: the same binary is started by a developer with two flags and
// by a deployment system with a full environment, and neither should need a
// config file format to learn.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/devthedevil/keystone/internal/cluster"
	"github.com/devthedevil/keystone/internal/server"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	fs := flag.NewFlagSet("keystoned", flag.ExitOnError)

	var (
		nodeID      = fs.String("id", env("KEYSTONE_ID", ""), "this replica's node ID (required)")
		peers       = fs.String("peers", env("KEYSTONE_PEERS", ""), "comma-separated id=peer-url list for every member, including this one")
		clientPeers = fs.String("client-peers", env("KEYSTONE_CLIENT_PEERS", ""), "optional id=client-url list, so redirects can name a dialable leader address")
		clientAddr  = fs.String("client-addr", env("KEYSTONE_CLIENT_ADDR", ":8080"), "client-plane listen address")
		peerAddr    = fs.String("peer-addr", env("KEYSTONE_PEER_ADDR", ":8081"), "peer-plane (replication) listen address")
		dataDir     = fs.String("data-dir", env("KEYSTONE_DATA_DIR", ""), "durable log directory; empty means an in-memory log (tests only)")
		fsync       = fs.Bool("fsync", envBool("KEYSTONE_FSYNC", true), "fsync the log on every append; disabling trades durability for latency")

		heartbeat = fs.Duration("heartbeat", envDur("KEYSTONE_HEARTBEAT", 100*time.Millisecond), "leader heartbeat interval")
		election  = fs.Duration("election-timeout", envDur("KEYSTONE_ELECTION_TIMEOUT", time.Second), "election timeout; also bounds the leader read lease")
		gcEvery   = fs.Duration("gc-interval", envDur("KEYSTONE_GC_INTERVAL", 30*time.Second), "how often the leader proposes a garbage collection")
		retention = fs.Uint64("retention-indexes", envUint("KEYSTONE_RETENTION_INDEXES", 10000), "how many log indexes of MVCC history to keep behind the applied index")

		adminToken = fs.String("admin-token", env("KEYSTONE_ADMIN_TOKEN", ""), "shared secret for /v1/admin endpoints; empty disables the check")
		maxValue   = fs.Int("max-value-bytes", envInt("KEYSTONE_MAX_VALUE_BYTES", 1<<20), "largest accepted value")

		rate       = fs.Float64("default-rate", envFloat("KEYSTONE_DEFAULT_RATE", 2000), "default per-tenant request units per second")
		burst      = fs.Float64("default-burst", envFloat("KEYSTONE_DEFAULT_BURST", 4000), "default per-tenant burst in request units")
		maxConc    = fs.Int("default-max-concurrent", envInt("KEYSTONE_DEFAULT_MAX_CONCURRENT", 64), "default per-tenant concurrency limit")
		latTarget  = fs.Duration("latency-target", envDur("KEYSTONE_LATENCY_TARGET", 50*time.Millisecond), "latency the overload controller defends")
		logLevel   = fs.String("log-level", env("KEYSTONE_LOG_LEVEL", "info"), "debug, info, warn or error")
		logFormat  = fs.String("log-format", env("KEYSTONE_LOG_FORMAT", "text"), "text or json")
		drainGrace = fs.Duration("drain-grace", envDur("KEYSTONE_DRAIN_GRACE", 20*time.Second), "how long shutdown waits to drain before forcing")
		showVer    = fs.Bool("version", false, "print the version and exit")
	)

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "keystoned %s - a strongly consistent, transactional key-value store\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage:\n  keystoned -id n1 -peers n1=http://host1:8081,n2=http://host2:8081,n3=http://host3:8081 -data-dir /var/lib/keystone\n\nFlags:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	if *showVer {
		fmt.Println(version)
		return
	}

	log := newLogger(*logLevel, *logFormat)
	slog.SetDefault(log)
	server.Version = version

	if *nodeID == "" {
		log.Error("-id is required: every replica needs a stable identity across restarts")
		os.Exit(2)
	}
	peerMap, err := parsePeers(*peers, *nodeID)
	if err != nil {
		log.Error("invalid -peers", "err", err)
		os.Exit(2)
	}
	if *dataDir == "" {
		log.Warn("no -data-dir: running with an in-memory log, committed data will not survive a restart")
	}

	clientMap := map[string]string{}
	if strings.TrimSpace(*clientPeers) != "" {
		if clientMap, err = parsePeers(*clientPeers, *nodeID); err != nil {
			log.Error("invalid -client-peers", "err", err)
			os.Exit(2)
		}
	}

	node, err := cluster.New(cluster.Options{
		NodeID:               *nodeID,
		Peers:                peerMap,
		ClientPeers:          clientMap,
		ClientAddr:           *clientAddr,
		PeerAddr:             *peerAddr,
		DataDir:              *dataDir,
		Fsync:                *fsync,
		HeartbeatInterval:    *heartbeat,
		ElectionTimeout:      *election,
		GCInterval:           *gcEvery,
		RetentionIndexes:     *retention,
		AdminToken:           *adminToken,
		MaxValueBytes:        *maxValue,
		DefaultRatePerSec:    *rate,
		DefaultBurst:         *burst,
		DefaultMaxConcurrent: *maxConc,
		LatencyTarget:        *latTarget,
		Logger:               log,
	})
	if err != nil {
		log.Error("failed to build replica", "err", err)
		os.Exit(1)
	}

	if err := node.Start(); err != nil {
		log.Error("failed to start replica", "err", err)
		os.Exit(1)
	}
	log.Info("keystoned running",
		"version", version, "id", *nodeID,
		"client_addr", node.ClientAddr(), "peer_addr", node.PeerAddr(),
		"members", len(peerMap), "quorum", len(peerMap)/2+1)

	// SIGTERM is what an orchestrator sends before it kills a pod. Treating it
	// as a drain request, not an abort, is the difference between a rolling
	// deploy that is invisible and one that shows up as a latency spike on
	// every dependent control plane.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Info("shutdown signal received, draining", "signal", s.String())

	ctx, cancel := context.WithTimeout(context.Background(), *drainGrace)
	defer cancel()
	if err := node.Shutdown(ctx); err != nil {
		log.Error("shutdown did not complete cleanly", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

// parsePeers accepts "id=url,id=url" and requires this node to be a member,
// because a replica that is not in its own configuration can never be elected
// and fails in a way that is hard to read from the outside.
func parsePeers(spec, self string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(spec) == "" {
		out[self] = ""
		return out, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q is not in id=url form", part)
		}
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if id == "" || addr == "" {
			return nil, fmt.Errorf("peer %q has an empty id or url", part)
		}
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("peer id %q appears twice", id)
		}
		out[id] = addr
	}
	if _, ok := out[self]; !ok {
		return nil, fmt.Errorf("this node %q is not listed in -peers", self)
	}
	if len(out)%2 == 0 {
		slog.Warn("even member count: an even group tolerates no more failures than the odd group below it", "members", len(out))
	}
	return out, nil
}

func newLogger(level, format string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if strings.EqualFold(format, "json") {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envUint(key string, def uint64) uint64 {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
