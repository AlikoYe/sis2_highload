package main

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"sis2_highload/internal/apierr"
)

// request is one fully prepared call; building it (ids, events) happens before timing starts.
type request func(ctx context.Context) error

const (
	minLatencyUS     = 1
	maxLatencyUS     = 60_000_000
	histogramSigFigs = 3
)

// recorder collects outcomes of requests that started inside the measured window.
type recorder struct {
	hist   *hdrhistogram.Histogram
	ok     int64
	errs   int64
	byCode map[string]int64
}

func newRecorder() *recorder {
	return &recorder{hist: hdrhistogram.New(minLatencyUS, maxLatencyUS, histogramSigFigs), byCode: map[string]int64{}}
}

func (r *recorder) record(latency time.Duration, err error) {
	if err != nil {
		r.errs++
		r.byCode[apierr.Of(err).Code]++
		return
	}
	r.ok++
	latencyUS := min(max(latency.Microseconds(), minLatencyUS), maxLatencyUS)
	_ = r.hist.RecordValue(latencyUS)
}

func (r *recorder) merge(other *recorder) {
	r.hist.Merge(other.hist)
	r.ok += other.ok
	r.errs += other.errs
	for code, count := range other.byCode {
		r.byCode[code] += count
	}
}

// window is the run timeline: [start, measureStart) is warm-up, [measureStart, end) is recorded.
type window struct {
	start, measureStart, end time.Time
}

// runClosed runs n workers back to back; latency is measured from just before each call.
func runClosed(workers int, timeline window, next func(worker int) request) *recorder {
	perWorker := make([]*recorder, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		perWorker[worker] = newRecorder()
		wg.Go(func() {
			for {
				req := next(worker)
				started := time.Now()
				if !started.Before(timeline.end) {
					return
				}
				err := req(context.Background())
				if !started.Before(timeline.measureStart) {
					perWorker[worker].record(time.Since(started), err)
				}
			}
		})
	}
	wg.Wait()
	total := newRecorder()
	for _, rec := range perWorker {
		total.merge(rec)
	}
	return total
}

// openLoop issues requests at a fixed rate regardless of how fast earlier ones complete.
// Latency is completion minus the *scheduled* send time, so a stalled generator or
// server shows up in the percentiles instead of being hidden (coordinated omission).
type openLoop struct {
	rate        float64
	maxInflight int
	wait        func(until time.Time) // nil = preciseWait
}

// spinWindow covers time.Sleep's wake-up lateness (~0.2-0.3ms on macOS), which would
// otherwise be charged to the server as latency in open mode. Costs up to one loadgen core.
const spinWindow = 500 * time.Microsecond

func preciseWait(until time.Time) {
	if sleepFor := time.Until(until) - spinWindow; sleepFor > 0 {
		time.Sleep(sleepFor)
	}
	for time.Now().Before(until) {
		runtime.Gosched()
	}
}

func (o openLoop) scheduled(start time.Time, i int) time.Time {
	return start.Add(time.Duration(float64(i) * float64(time.Second) / o.rate))
}

// run returns the recorder and how many measured requests were dropped because
// maxInflight requests were already outstanding at their scheduled time.
func (o openLoop) run(timeline window, next func(i int) request) (*recorder, int64) {
	wait := o.wait
	if wait == nil {
		wait = preciseWait
	}
	var (
		mu      sync.Mutex
		rec     = newRecorder()
		dropped int64
		wg      sync.WaitGroup
		slots   = make(chan struct{}, o.maxInflight)
	)
	for i := 0; ; i++ {
		at := o.scheduled(timeline.start, i)
		if !at.Before(timeline.end) {
			break
		}
		wait(at)
		measured := !at.Before(timeline.measureStart)
		select {
		case slots <- struct{}{}:
		default:
			if measured {
				dropped++
			}
			continue
		}
		req := next(i)
		wg.Go(func() {
			defer func() { <-slots }()
			err := req(context.Background())
			if measured {
				latency := time.Since(at)
				mu.Lock()
				rec.record(latency, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return rec, dropped
}
