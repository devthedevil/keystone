// Command keystonectl is the operator and developer CLI for Keystone.
//
// Every subcommand goes through the same client library that applications use,
// so the CLI cannot accidentally be more capable, or more correct, than the
// interface everyone else has.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/devthedevil/keystone/pkg/api"
	"github.com/devthedevil/keystone/pkg/client"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		var ke *client.Error
		if errors.As(err, &ke) && ke.Body.RequestID != "" {
			fmt.Fprintln(os.Stderr, "request-id:", ke.Body.RequestID)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `keystonectl %s

Usage:
  keystonectl [global flags] <command> [args]

Commands:
  get <key>                     read a key
  put <key> [value]             write a key (value from stdin if omitted)
  del <key>                     delete a key
  scan                          list a range or prefix
  txn [file]                    submit a transaction from JSON (stdin if omitted)
  watch                         follow the change feed
  status                        show every endpoint's view of the cluster
  gc                            trigger a replicated garbage collection (admin)
  quota [tenant]                show or set tenant quotas (admin)
  stepdown                      ask the leader to abdicate (admin)
  bench                         run a small load generator

Global flags:
  -endpoints   comma-separated client URLs (env KEYSTONE_ENDPOINTS, default http://127.0.0.1:8080)
  -tenant      tenant name (env KEYSTONE_TENANT, default "default")
  -class       priority class: high, normal, bulk (default normal)
  -admin-token admin token (env KEYSTONE_ADMIN_TOKEN)
  -timeout     per-request timeout (default 10s)
  -json        print raw JSON instead of a human table

Run "keystonectl <command> -h" for command flags.
`, version)
}

func run(args []string) error {
	gf := flag.NewFlagSet("keystonectl", flag.ContinueOnError)
	gf.SetOutput(io.Discard)
	endpoints := gf.String("endpoints", env("KEYSTONE_ENDPOINTS", "http://127.0.0.1:8080"), "")
	tenant := gf.String("tenant", env("KEYSTONE_TENANT", "default"), "")
	class := gf.String("class", env("KEYSTONE_CLASS", ""), "")
	adminToken := gf.String("admin-token", env("KEYSTONE_ADMIN_TOKEN", ""), "")
	timeout := gf.Duration("timeout", 10*time.Second, "")
	asJSON := gf.Bool("json", false, "")
	showVer := gf.Bool("version", false, "")

	if err := gf.Parse(args); err != nil {
		usage()
		return flag.ErrHelp
	}
	if *showVer {
		fmt.Println(version)
		return nil
	}
	rest := gf.Args()
	if len(rest) == 0 {
		usage()
		return flag.ErrHelp
	}

	c, err := client.New(client.Options{
		Endpoints:  strings.Split(*endpoints, ","),
		Tenant:     *tenant,
		Class:      *class,
		AdminToken: *adminToken,
		Timeout:    *timeout,
	})
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cmd, cmdArgs := rest[0], rest[1:]
	out := &printer{json: *asJSON}

	switch cmd {
	case "get":
		return cmdGet(ctx, c, out, cmdArgs)
	case "put":
		return cmdPut(ctx, c, out, cmdArgs)
	case "del", "delete":
		return cmdDel(ctx, c, out, cmdArgs)
	case "scan", "range", "ls":
		return cmdScan(ctx, c, out, cmdArgs)
	case "txn":
		return cmdTxn(ctx, c, out, cmdArgs)
	case "watch", "stream":
		return cmdWatch(ctx, c, out, cmdArgs)
	case "status":
		return cmdStatus(ctx, c, out)
	case "gc":
		return cmdGC(ctx, c, out)
	case "quota":
		return cmdQuota(ctx, c, out, cmdArgs)
	case "stepdown":
		return cmdStepDown(ctx, c)
	case "bench":
		return cmdBench(ctx, c, out, cmdArgs)
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// ---------------------------------------------------------------------------

func cmdGet(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	raw := fs.Bool("raw", false, "print only the value bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: keystonectl get <key>")
	}
	res, err := c.Get(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if *raw {
		os.Stdout.Write(res.Value)
		return nil
	}
	if p.json {
		return p.emit(res)
	}
	fmt.Printf("key      %s\nversion  %d\nread_ts  %d\nvalue    %s\n", res.Key, res.Version, res.ReadTS, res.Value)
	return nil
}

func cmdPut(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	ifVersion := fs.Uint64("if-version", 0, "commit only if the key is at this version (compare-and-swap)")
	ifAbsent := fs.Bool("if-absent", false, "commit only if the key does not exist (create)")
	txnID := fs.String("txn-id", "", "idempotency key; reuse it across retries of the same logical write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: keystonectl put <key> [value]")
	}
	key := fs.Arg(0)
	var value []byte
	if fs.NArg() >= 2 {
		value = []byte(strings.Join(fs.Args()[1:], " "))
	} else {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("reading value from stdin: %w", err)
		}
		value = b
	}
	v, err := c.Put(ctx, key, value, &client.PutOptions{
		IfVersion: *ifVersion, IfAbsent: *ifAbsent, TxnID: *txnID,
	})
	if err != nil {
		return err
	}
	if p.json {
		return p.emit(map[string]any{"key": key, "version": v})
	}
	fmt.Printf("ok  %s @ %d\n", key, v)
	return nil
}

func cmdDel(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	ifVersion := fs.Uint64("if-version", 0, "delete only if the key is at this version")
	txnID := fs.String("txn-id", "", "idempotency key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: keystonectl del <key>")
	}
	v, err := c.Delete(ctx, fs.Arg(0), &client.DeleteOptions{IfVersion: *ifVersion, TxnID: *txnID})
	if err != nil {
		return err
	}
	if p.json {
		return p.emit(map[string]any{"key": fs.Arg(0), "version": v})
	}
	fmt.Printf("deleted  %s @ %d\n", fs.Arg(0), v)
	return nil
}

func cmdScan(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	prefix := fs.String("prefix", "", "key prefix")
	start := fs.String("start", "", "inclusive start key")
	end := fs.String("end", "", "exclusive end key")
	limit := fs.Int("limit", 100, "maximum keys per page")
	all := fs.Bool("all", false, "page through the whole range")
	keysOnly := fs.Bool("keys-only", false, "print keys without values")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opt := client.ScanOptions{Prefix: *prefix, Start: *start, End: *end, Limit: *limit}

	if *all {
		var kvs []api.KV
		if err := c.ScanAll(ctx, opt, func(kv api.KV) error { kvs = append(kvs, kv); return nil }); err != nil {
			return err
		}
		return printKVs(p, api.RangeResponse{KVs: kvs}, *keysOnly)
	}
	res, err := c.Scan(ctx, opt)
	if err != nil {
		return err
	}
	return printKVs(p, res, *keysOnly)
}

func printKVs(p *printer, res api.RangeResponse, keysOnly bool) error {
	if p.json {
		return p.emit(res)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if keysOnly {
		for _, kv := range res.KVs {
			fmt.Fprintln(tw, kv.Key)
		}
	} else {
		fmt.Fprintln(tw, "KEY\tVERSION\tVALUE")
		for _, kv := range res.KVs {
			fmt.Fprintf(tw, "%s\t%d\t%s\n", kv.Key, kv.Version, truncate(string(kv.Value), 72))
		}
	}
	tw.Flush()
	if res.More {
		fmt.Printf("\n(more results; resume with -start %q, or use -all)\n", res.NextStart)
	}
	return nil
}

func cmdTxn(ctx context.Context, c *client.Client, p *printer, args []string) error {
	var r io.Reader = os.Stdin
	if len(args) > 0 && args[0] != "-" {
		f, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	var req api.TxnRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return fmt.Errorf("decoding transaction: %w", err)
	}
	res, err := c.Txn(ctx, req)
	if err != nil {
		return err
	}
	if p.json {
		return p.emit(res)
	}
	if !res.Committed {
		fmt.Printf("aborted")
		if res.Conflict != nil {
			fmt.Printf("  kind=%s key=%s expected=%d observed=%d",
				res.Conflict.Kind, res.Conflict.Key, res.Conflict.Expected, res.Conflict.Observed)
		}
		fmt.Println()
		return nil
	}
	fmt.Printf("committed @ %d", res.CommitTS)
	if res.Duplicate {
		fmt.Printf("  (duplicate: this transaction id had already committed)")
	}
	fmt.Println()
	return nil
}

func cmdWatch(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	from := fs.Uint64("from", 0, "resume after this sequence")
	prefix := fs.String("prefix", "", "only changes under this key prefix")
	if err := fs.Parse(args); err != nil {
		return err
	}
	err := c.Watch(ctx, client.WatchOptions{From: *from, Prefix: *prefix}, func(ev api.ChangeEvent) error {
		if p.json {
			return p.emit(ev)
		}
		if ev.Kind == "delete" {
			fmt.Printf("%d  DELETE  %s\n", ev.Seq, ev.Key)
		} else {
			fmt.Printf("%d  PUT     %s = %s\n", ev.Seq, ev.Key, truncate(string(ev.Value), 72))
		}
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func cmdStatus(ctx context.Context, c *client.Client, p *printer) error {
	type row struct {
		Endpoint string              `json:"endpoint"`
		Status   *api.StatusResponse `json:"status,omitempty"`
		Error    string              `json:"error,omitempty"`
	}
	var rows []row
	for _, ep := range c.Endpoints() {
		st, err := c.StatusOf(ctx, ep)
		if err != nil {
			rows = append(rows, row{Endpoint: ep, Error: err.Error()})
			continue
		}
		s := st
		rows = append(rows, row{Endpoint: ep, Status: &s})
	}
	if p.json {
		return p.emit(rows)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ENDPOINT\tNODE\tSTATE\tTERM\tLEADER\tLEASE\tCOMMIT\tAPPLIED\tKEYS\tWATERMARK\tHEALTH")
	for _, r := range rows {
		if r.Status == nil {
			fmt.Fprintf(tw, "%s\tunreachable\t\t\t\t\t\t\t\t\t%s\n", r.Endpoint, truncate(r.Error, 48))
			continue
		}
		s := r.Status
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%t\t%d\t%d\t%d\t%d\t%.2f\n",
			r.Endpoint, s.NodeID, s.State, s.Term, s.Leader, s.LeaseValid,
			s.CommitIndex, s.LastApplied, s.Keys, s.GCWatermark, s.AdmissionHealth)
	}
	tw.Flush()
	return nil
}

func cmdGC(ctx context.Context, c *client.Client, p *printer) error {
	res, err := c.RunGC(ctx)
	if err != nil {
		return err
	}
	if p.json {
		return p.emit(res)
	}
	if res.GC == nil {
		fmt.Println("nothing to collect: the watermark has not moved since the last pass")
		return nil
	}
	fmt.Printf("collected at watermark %d (log index %d): freed %d versions, removed %d keys, reclaimed ~%d bytes\n",
		res.GC.Watermark, res.CommitTS, res.GC.VersionsFreed, res.GC.KeysRemoved, res.GC.BytesFreed)
	return nil
}

func cmdQuota(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("quota", flag.ContinueOnError)
	rate := fs.Float64("rate", 0, "request units per second (required when setting)")
	burst := fs.Float64("burst", 0, "burst in request units (required when setting)")
	maxConc := fs.Int("max-concurrent", 64, "concurrent request limit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() == 0 {
		qs, err := c.ListQuotas(ctx)
		if err != nil {
			return err
		}
		if p.json {
			return p.emit(qs)
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "TENANT\tTOKENS\tIN FLIGHT\tADMITTED\tTHROTTLED\tSHED\tUNITS CHARGED")
		for _, q := range qs {
			fmt.Fprintf(tw, "%s\t%.0f\t%d\t%d\t%d\t%d\t%.0f\n",
				q.Tenant, q.Tokens, q.InFlight, q.Admitted, q.Throttled, q.Shed, q.UnitsCharge)
		}
		tw.Flush()
		return nil
	}

	if *rate <= 0 || *burst <= 0 {
		return errors.New("setting a quota requires both -rate and -burst to be positive")
	}
	q := api.TenantQuota{Tenant: fs.Arg(0), RatePerSec: *rate, Burst: *burst, MaxConcurrent: *maxConc}
	got, err := c.SetQuota(ctx, q)
	if err != nil {
		return err
	}
	if p.json {
		return p.emit(got)
	}
	fmt.Printf("tenant %s: %.0f RU/s, burst %.0f, max concurrent %d\n",
		got.Tenant, got.RatePerSec, got.Burst, got.MaxConcurrent)
	return nil
}

func cmdStepDown(ctx context.Context, c *client.Client) error {
	if err := c.StepDown(ctx); err != nil {
		return err
	}
	fmt.Println("leader asked to step down; a new election should complete within an election timeout")
	return nil
}

// cmdBench is a deliberately small load generator. It is not a benchmark
// suite; it is the thing you run to see whether a change moved latency before
// you go and set up a real one.
func cmdBench(ctx context.Context, c *client.Client, p *printer, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	n := fs.Int("n", 1000, "operations")
	conc := fs.Int("c", 16, "concurrency")
	keys := fs.Int("keys", 1000, "distinct keys")
	size := fs.Int("size", 256, "value size in bytes")
	readRatio := fs.Float64("read-ratio", 0.8, "fraction of operations that are reads")
	prefix := fs.String("prefix", "bench/", "key prefix")
	if err := fs.Parse(args); err != nil {
		return err
	}

	value := make([]byte, *size)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	type result struct {
		d   time.Duration
		err error
		wr  bool
	}
	work := make(chan int, *conc)
	results := make(chan result, *conc)
	done := make(chan struct{})

	go func() {
		defer close(work)
		for i := 0; i < *n; i++ {
			select {
			case work <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	for w := 0; w < *conc; w++ {
		go func(seed int) {
			for i := range work {
				key := fmt.Sprintf("%s%d", *prefix, (i*2654435761)%(*keys))
				isRead := float64((i*7919)%1000)/1000.0 < *readRatio
				start := time.Now()
				var err error
				if isRead {
					_, err = c.Get(ctx, key)
					if client.IsNotFound(err) {
						err = nil
					}
				} else {
					_, err = c.Put(ctx, key, value, nil)
				}
				select {
				case results <- result{d: time.Since(start), err: err, wr: !isRead}:
				case <-done:
					return
				}
			}
		}(w)
	}

	var (
		lat      []time.Duration
		failures = map[string]int{}
		reads    int
		writes   int
		started  = time.Now()
	)
	for i := 0; i < *n; i++ {
		select {
		case <-ctx.Done():
			close(done)
			return ctx.Err()
		case r := <-results:
			lat = append(lat, r.d)
			if r.wr {
				writes++
			} else {
				reads++
			}
			if r.err != nil {
				code := client.Code(r.err)
				if code == "" {
					code = "transport"
				}
				failures[code]++
			}
		}
	}
	close(done)
	elapsed := time.Since(started)

	sortDurations(lat)
	summary := map[string]any{
		"operations":  *n,
		"concurrency": *conc,
		"reads":       reads,
		"writes":      writes,
		"elapsed_ms":  elapsed.Milliseconds(),
		"ops_per_sec": float64(*n) / elapsed.Seconds(),
		"p50_ms":      ms(pct(lat, 0.50)),
		"p90_ms":      ms(pct(lat, 0.90)),
		"p99_ms":      ms(pct(lat, 0.99)),
		"max_ms":      ms(lat[len(lat)-1]),
		"failures":    failures,
	}
	if p.json {
		return p.emit(summary)
	}
	fmt.Printf("%d ops (%d reads, %d writes) at concurrency %d in %s\n", *n, reads, writes, *conc, elapsed.Round(time.Millisecond))
	fmt.Printf("throughput  %.0f ops/s\n", float64(*n)/elapsed.Seconds())
	fmt.Printf("latency     p50 %.2fms  p90 %.2fms  p99 %.2fms  max %.2fms\n",
		ms(pct(lat, 0.5)), ms(pct(lat, 0.9)), ms(pct(lat, 0.99)), ms(lat[len(lat)-1]))
	if len(failures) == 0 {
		fmt.Println("failures    none")
	} else {
		for code, n := range failures {
			fmt.Printf("failures    %s x%d\n", code, n)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------

type printer struct{ json bool }

func (p *printer) emit(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "\u2026"
}

func sortDurations(d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
}

func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * q)
	return sorted[i]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
