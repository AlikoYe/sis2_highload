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
	var c config
	flag.StringVar(&c.protocol, "protocol", "grpc", "grpc|rest|h2c")
	flag.StringVar(&c.target, "target", "", "server host:port (default per protocol: :50051, :8080, :8081)")
	flag.StringVar(&c.op, "op", "publish", "publish|batch|status")
	flag.StringVar(&c.payload, "payload", "small", "small|medium|large (batch always sends 100 small events)")
	flag.StringVar(&c.mode, "mode", "closed", "closed|open")
	flag.IntVar(&c.concurrency, "concurrency", 16, "closed mode: number of workers")
	flag.Float64Var(&c.rate, "rate", 1000, "open mode: requests per second")
	flag.IntVar(&c.maxInflight, "max-inflight", 1024, "open mode: max outstanding requests; extra ones are dropped")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "measured duration")
	flag.DurationVar(&c.warmup, "warmup", 5*time.Second, "unrecorded warm-up before measuring")
	flag.DurationVar(&c.callTimeout, "call-timeout", 300*time.Millisecond, "per-call deadline including retries")
	flag.StringVar(&c.label, "label", "", "run label written to the CSV")
	flag.StringVar(&c.store, "store", "memory", "server store, only recorded in the output")
	flag.StringVar(&c.out, "out", "", "CSV file to append the result row to")
	flag.IntVar(&c.serverPID, "server-pid", 0, "if set, measure this process's CPU time over the measured window")
	flag.StringVar(&c.serverCPUOut, "server-cpu-out", "", "CSV file for label,server_cpu_ms,duration_s (needs -server-pid)")
	flag.Uint64Var(&c.seed, "seed", 1, "seed for picking status ids")
	flag.Parse()

	if c.target == "" {
		c.target = map[string]string{"grpc": "localhost:50051", "rest": "localhost:8080", "h2c": "localhost:8081"}[c.protocol]
	}
	switch {
	case c.target == "":
		return c, fmt.Errorf("unknown -protocol %q", c.protocol)
	case c.op != "publish" && c.op != "batch" && c.op != "status":
		return c, fmt.Errorf("unknown -op %q", c.op)
	case c.mode != "closed" && c.mode != "open":
		return c, fmt.Errorf("unknown -mode %q", c.mode)
	case c.mode == "closed" && c.concurrency <= 0:
		return c, errors.New("-concurrency must be > 0")
	case c.mode == "open" && (c.rate <= 0 || c.maxInflight <= 0):
		return c, errors.New("-rate and -max-inflight must be > 0")
	case c.duration <= 0 || c.warmup < 0:
		return c, errors.New("-duration must be > 0 and -warmup >= 0")
	}
	if c.op == "batch" {
		c.payload = string(workload.Batch)
	} else if _, err := workload.ParseKind(c.payload); err != nil || c.payload == string(workload.Batch) {
		return c, fmt.Errorf("unknown -payload %q", c.payload)
	}
	if c.mode == "open" {
		c.concurrency = 0
	} else {
		c.rate = 0
	}
	if c.label == "" {
		c.label = fmt.Sprintf("%s-%s-%s", c.protocol, c.op, c.payload)
	}
	return c, nil
}

func newClient(cfg config) (client.Client, error) {
	opts := client.Options{CallTimeout: cfg.callTimeout, Policy: retry.DefaultPolicy(), Budget: retry.DefaultBudget()}
	switch cfg.protocol {
	case "grpc":
		return client.NewGRPC(cfg.target, opts)
	case "rest":
		return client.NewREST("http://"+cfg.target, opts), nil
	default:
		return client.NewRESTH2C("http://"+cfg.target, opts), nil
	}
}

// builder returns a function that prepares one request; rng is only used by the status op.
func builder(ctx context.Context, cfg config, c client.Client) (func(rng *rand.Rand) request, error) {
	switch cfg.op {
	case "batch":
		return func(*rand.Rand) request {
			ids := make([]string, workload.BatchSize)
			for i := range ids {
				ids[i] = client.NewEventID()
			}
			es := workload.BatchEvents(ids)
			return func(ctx context.Context) error { _, err := c.PublishBatch(ctx, es); return err }
		}, nil
	case "status":
		ids, err := seed(ctx, c, workload.Kind(cfg.payload))
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
			e := workload.Event(kind, client.NewEventID())
			return func(ctx context.Context) error { _, err := c.Publish(ctx, e); return err }
		}, nil
	}
}

func seed(ctx context.Context, c client.Client, kind workload.Kind) ([]string, error) {
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
	s := snapshot{cpu: selfCPU(), stats: c.Stats()}
	if cfg.serverPID > 0 {
		var err error
		if s.serverCPU, err = processCPU(cfg.serverPID); err != nil {
			return s, err
		}
	}
	return s, nil
}

func selfCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func run(cfg config) error {
	ctx := context.Background()
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	build, err := builder(ctx, cfg, c)
	if err != nil {
		return err
	}

	// Open mode prepares every request before the clock starts; closed mode
	// prepares each one right before taking its start timestamp.
	var prepared []request
	if cfg.mode == "open" {
		n := int(cfg.rate*(cfg.warmup+cfg.duration).Seconds()) + 1
		if n > maxPrepared {
			return fmt.Errorf("open mode would prepare %d requests (max %d): lower -rate or -duration", n, maxPrepared)
		}
		rng := rand.New(rand.NewPCG(cfg.seed, 0))
		prepared = make([]request, n)
		for i := range prepared {
			prepared[i] = build(rng)
		}
	}

	start := time.Now()
	w := window{start: start, measureStart: start.Add(cfg.warmup), end: start.Add(cfg.warmup + cfg.duration)}

	type snaps struct {
		before, after snapshot
		err           error
	}
	snapc := make(chan snaps, 1)
	go func() {
		var s snaps
		time.Sleep(time.Until(w.measureStart))
		s.before, s.err = takeSnapshot(cfg, c)
		time.Sleep(time.Until(w.end))
		if s.err == nil {
			s.after, s.err = takeSnapshot(cfg, c)
		}
		snapc <- s
	}()

	var rec *recorder
	var dropped int64
	if cfg.mode == "open" {
		rec, dropped = openLoop{rate: cfg.rate, maxInflight: cfg.maxInflight}.run(w, func(i int) request {
			return prepared[min(i, len(prepared)-1)]
		})
	} else {
		rngs := make([]*rand.Rand, cfg.concurrency)
		for i := range rngs {
			rngs[i] = rand.New(rand.NewPCG(cfg.seed, uint64(i)))
		}
		rec = runClosed(cfg.concurrency, w, func(worker int) request { return build(rngs[worker]) })
	}
	s := <-snapc
	if s.err != nil {
		return s.err
	}

	res := result{
		cfg:       cfg,
		rec:       rec,
		dropped:   dropped,
		attempts:  s.after.stats.Attempts - s.before.stats.Attempts,
		retries:   s.after.stats.Retries - s.before.stats.Retries,
		clientCPU: s.after.cpu - s.before.cpu,
		serverCPU: s.after.serverCPU - s.before.serverCPU,
		finished:  time.Now(),
	}
	res.summary(os.Stdout)

	if cfg.out != "" {
		if err := appendCSV(cfg.out, csvHeader, res.row()); err != nil {
			return fmt.Errorf("write %s: %w", cfg.out, err)
		}
	}
	if cfg.serverCPUOut != "" && cfg.serverPID > 0 {
		row := []string{cfg.label, strconv.FormatInt(res.serverCPU.Milliseconds(), 10), strconv.FormatFloat(cfg.duration.Seconds(), 'f', 1, 64)}
		if err := appendCSV(cfg.serverCPUOut, []string{"label", "server_cpu_ms", "duration_s"}, row); err != nil {
			return fmt.Errorf("write %s: %w", cfg.serverCPUOut, err)
		}
	}
	return nil
}
