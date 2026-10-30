// Command loadgen drives the notification service through internal/client and
// records latency (HDR histogram), errors, retries and CPU for one run as a CSV row.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"syscall"
	"time"

	"sis2_highload/internal/client"
	"sis2_highload/internal/retry"
	"sis2_highload/internal/workload"
)

type config struct {
	protocol, target, op, payload, mode string
	concurrency                         int
	conns                               int
	rate                                float64
	maxInflight                         int
	duration, warmup, callTimeout       time.Duration
	label, store, out                   string
	serverPID                           int
	serverCPUOut                        string
	seed                                uint64
}

const (
	statusSeedEvents = 1000
	maxPrepared      = 5_000_000 // open mode prepares every request up front
)

var defaultTargets = map[string]string{"grpc": "localhost:50051", "rest": "localhost:8080", "h2c": "localhost:8081"}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func parseFlags() (config, error) {
	var cfg config
	flag.StringVar(&cfg.protocol, "protocol", "grpc", "grpc|rest|h2c")
	flag.StringVar(&cfg.target, "target", "", "server host:port (default per protocol: :50051, :8080, :8081)")
	flag.StringVar(&cfg.op, "op", "publish", "publish|batch|status")
	flag.StringVar(&cfg.payload, "payload", "small", "small|medium|large (batch always sends 100 small events)")
	flag.StringVar(&cfg.mode, "mode", "closed", "closed|open")
	flag.IntVar(&cfg.concurrency, "concurrency", 16, "closed mode: number of workers")
	flag.IntVar(&cfg.conns, "conns", 1, "grpc/h2c: number of connections, requests round-robin across them; ignored for rest (HTTP/1.1 already uses one connection per in-flight request)")
	flag.Float64Var(&cfg.rate, "rate", 1000, "open mode: requests per second")
	flag.IntVar(&cfg.maxInflight, "max-inflight", 1024, "open mode: max outstanding requests; extra ones are dropped")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "measured duration")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "unrecorded warm-up before measuring")
	flag.DurationVar(&cfg.callTimeout, "call-timeout", 300*time.Millisecond, "per-call deadline including retries")
	flag.StringVar(&cfg.label, "label", "", "run label written to the CSV")
	flag.StringVar(&cfg.store, "store", "memory", "server store, only recorded in the output")
	flag.StringVar(&cfg.out, "out", "", "CSV file to append the result row to")
	flag.IntVar(&cfg.serverPID, "server-pid", 0, "if set, measure this process's CPU time over the measured window")
	flag.StringVar(&cfg.serverCPUOut, "server-cpu-out", "", "CSV file for label,server_cpu_ms,duration_s (needs -server-pid)")
	flag.Uint64Var(&cfg.seed, "seed", 1, "seed for picking status ids")
	flag.Parse()

	if cfg.target == "" {
		cfg.target = defaultTargets[cfg.protocol]
	}
	switch {
	case cfg.target == "":
		return cfg, fmt.Errorf("unknown -protocol %q", cfg.protocol)
	case cfg.op != "publish" && cfg.op != "batch" && cfg.op != "status":
		return cfg, fmt.Errorf("unknown -op %q", cfg.op)
	case cfg.mode != "closed" && cfg.mode != "open":
		return cfg, fmt.Errorf("unknown -mode %q", cfg.mode)
	case cfg.mode == "closed" && cfg.concurrency <= 0:
		return cfg, errors.New("-concurrency must be > 0")
	case cfg.mode == "open" && (cfg.rate <= 0 || cfg.maxInflight <= 0):
		return cfg, errors.New("-rate and -max-inflight must be > 0")
	case cfg.conns <= 0:
		return cfg, errors.New("-conns must be > 0")
	case cfg.duration <= 0 || cfg.warmup < 0:
		return cfg, errors.New("-duration must be > 0 and -warmup >= 0")
	}
	if cfg.op == "batch" {
		cfg.payload = string(workload.Batch)
	} else if _, err := workload.ParseKind(cfg.payload); err != nil || cfg.payload == string(workload.Batch) {
		return cfg, fmt.Errorf("unknown -payload %q", cfg.payload)
	}
	if cfg.protocol == "rest" {
		cfg.conns = 0 // recorded as 0: HTTP/1.1 opens one connection per in-flight request
	}
	if cfg.mode == "open" {
		cfg.concurrency = 0
	} else {
		cfg.rate = 0
	}
	if cfg.label == "" {
		cfg.label = fmt.Sprintf("%s-%s-%s", cfg.protocol, cfg.op, cfg.payload)
	}
	return cfg, nil
}

// newClient builds one client per connection (sharing one retry budget) and
// round-robins across them when -conns > 1.
func newClient(cfg config) (client.Client, error) {
	opts := client.Options{CallTimeout: cfg.callTimeout, Policy: retry.DefaultPolicy(), Budget: retry.DefaultBudget()}
	if cfg.protocol == "rest" {
		return client.NewREST("http://"+cfg.target, opts), nil
	}
	rr := &roundRobin{}
	for range cfg.conns {
		if cfg.protocol == "h2c" {
			rr.clients = append(rr.clients, client.NewRESTH2C("http://"+cfg.target, opts))
			continue
		}
		grpcClient, err := client.NewGRPC(cfg.target, opts)
		if err != nil {
			_ = rr.Close()
			return nil, err
		}
		rr.clients = append(rr.clients, grpcClient)
	}
	if len(rr.clients) == 1 {
		return rr.clients[0], nil
	}
	return rr, nil
}

// requestBuilder returns a function that prepares one request; rng is only used by the status op.
func requestBuilder(ctx context.Context, cfg config, c client.Client) (func(rng *rand.Rand) request, error) {
	switch cfg.op {
	case "batch":
		return func(*rand.Rand) request {
			ids := make([]string, workload.BatchSize)
			for i := range ids {
				ids[i] = client.NewEventID()
			}
			events := workload.BatchEvents(ids)
			return func(ctx context.Context) error { _, err := c.PublishBatch(ctx, events); return err }
		}, nil
	case "status":
		ids, err := seedStatusEvents(ctx, c, workload.Kind(cfg.payload))
		if err != nil {
			return nil, err
		}
		return func(rng *rand.Rand) request {
			id := ids[rng.IntN(len(ids))]
			return func(ctx context.Context) error { _, err := c.GetStatus(ctx, id); return err }
		}, nil
	default:
		kind := workload.Kind(cfg.payload)
		return func(*rand.Rand) request {
			event := workload.Event(kind, client.NewEventID())
			return func(ctx context.Context) error { _, err := c.Publish(ctx, event); return err }
		}, nil
	}
}

func seedStatusEvents(ctx context.Context, c client.Client, kind workload.Kind) ([]string, error) {
	ids := make([]string, statusSeedEvents)
	for i := range ids {
		ids[i] = client.NewEventID()
		if _, err := c.Publish(ctx, workload.Event(kind, ids[i])); err != nil {
			return nil, fmt.Errorf("seeding status events: %w", err)
		}
	}
	return ids, nil
}

type snapshot struct {
	cpu       time.Duration
	stats     client.Stats
	serverCPU time.Duration
}

func takeSnapshot(cfg config, c client.Client) (snapshot, error) {
	snap := snapshot{cpu: selfCPU(), stats: c.Stats()}
	if cfg.serverPID > 0 {
		var err error
		if snap.serverCPU, err = processCPU(cfg.serverPID); err != nil {
			return snap, err
		}
	}
	return snap, nil
}

func selfCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func run(cfg config) error {
	ctx := context.Background()
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	build, err := requestBuilder(ctx, cfg, c)
	if err != nil {
		return err
	}
	var prepared []request
	if cfg.mode == "open" {
		if prepared, err = prepareAll(cfg, build); err != nil {
			return err
		}
	}

	start := time.Now()
	timeline := window{start: start, measureStart: start.Add(cfg.warmup), end: start.Add(cfg.warmup + cfg.duration)}
	snapshots := snapshotWindow(cfg, c, timeline)

	var rec *recorder
	var dropped int64
	if cfg.mode == "open" {
		rec, dropped = openLoop{rate: cfg.rate, maxInflight: cfg.maxInflight}.run(timeline, func(i int) request {
			return prepared[min(i, len(prepared)-1)]
		})
	} else {
		rngs := make([]*rand.Rand, cfg.concurrency)
		for i := range rngs {
			rngs[i] = rand.New(rand.NewPCG(cfg.seed, uint64(i)))
		}
		rec = runClosed(cfg.concurrency, timeline, func(worker int) request { return build(rngs[worker]) })
	}
	snaps := <-snapshots
	if snaps.err != nil {
		return snaps.err
	}

	outcome := result{
		cfg:       cfg,
		rec:       rec,
		dropped:   dropped,
		attempts:  snaps.after.stats.Attempts - snaps.before.stats.Attempts,
		retries:   snaps.after.stats.Retries - snaps.before.stats.Retries,
		clientCPU: snaps.after.cpu - snaps.before.cpu,
		serverCPU: snaps.after.serverCPU - snaps.before.serverCPU,
		finished:  time.Now(),
	}
	outcome.summary(os.Stdout)
	return writeOutputs(cfg, outcome)
}

// prepareAll builds every open-mode request before the clock starts; closed mode
// instead prepares each one right before taking its start timestamp.
func prepareAll(cfg config, build func(*rand.Rand) request) ([]request, error) {
	count := int(cfg.rate*(cfg.warmup+cfg.duration).Seconds()) + 1
	if count > maxPrepared {
		return nil, fmt.Errorf("open mode would prepare %d requests (max %d): lower -rate or -duration", count, maxPrepared)
	}
	rng := rand.New(rand.NewPCG(cfg.seed, 0))
	prepared := make([]request, count)
	for i := range prepared {
		prepared[i] = build(rng)
	}
	return prepared, nil
}

type windowSnapshots struct {
	before, after snapshot
	err           error
}

// snapshotWindow takes CPU and client-stats snapshots at the start and end of the measured window.
func snapshotWindow(cfg config, c client.Client, timeline window) <-chan windowSnapshots {
	done := make(chan windowSnapshots, 1)
	go func() {
		var snaps windowSnapshots
		time.Sleep(time.Until(timeline.measureStart))
		snaps.before, snaps.err = takeSnapshot(cfg, c)
		time.Sleep(time.Until(timeline.end))
		if snaps.err == nil {
			snaps.after, snaps.err = takeSnapshot(cfg, c)
		}
		done <- snaps
	}()
	return done
}

func writeOutputs(cfg config, outcome result) error {
	if cfg.out != "" {
		if err := appendCSV(cfg.out, csvHeader, outcome.row()); err != nil {
			return fmt.Errorf("write %s: %w", cfg.out, err)
		}
	}
	if cfg.serverCPUOut != "" && cfg.serverPID > 0 {
		row := []string{
			cfg.label,
			strconv.FormatInt(outcome.serverCPU.Milliseconds(), 10),
			strconv.FormatFloat(cfg.duration.Seconds(), 'f', 1, 64),
		}
		if err := appendCSV(cfg.serverCPUOut, serverCPUHeader, row); err != nil {
			return fmt.Errorf("write %s: %w", cfg.serverCPUOut, err)
		}
	}
	return nil
}
