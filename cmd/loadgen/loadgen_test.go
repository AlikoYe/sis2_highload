package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sis2_highload/internal/domain"
)

func instant(context.Context) error { return nil }

func newWindow(warmup, duration time.Duration) window {
	start := time.Now()
	return window{start: start, measureStart: start.Add(warmup), end: start.Add(warmup + duration)}
}

func TestOpenLoopSchedule(t *testing.T) {
	o := openLoop{rate: 1000}
	start := time.Unix(0, 0)
	if got := o.scheduled(start, 250).Sub(start); got != 250*time.Millisecond {
		t.Fatalf("request 250 at 1000/s scheduled at %v, want 250ms", got)
	}
}

func TestOpenLoopCountsEveryScheduledRequest(t *testing.T) {
	// 1000/s for 100ms measured (after 20ms warm-up) = exactly 100 measured slots.
	o := openLoop{rate: 1000, maxInflight: 64}
	rec, dropped := o.run(newWindow(20*time.Millisecond, 100*time.Millisecond), func(int) request { return instant })
	if rec.ok+dropped != 100 || dropped != 0 {
		t.Fatalf("ok=%d dropped=%d, want 100 ok and 0 dropped", rec.ok, dropped)
	}
}

// A generator stalled for 30ms must not hide that delay: the requests it was
// supposed to send meanwhile are measured from their scheduled time.
func TestOpenLoopMeasuresFromScheduledStart(t *testing.T) {
	var stalled atomic.Bool
	o := openLoop{rate: 1000, maxInflight: 64, wait: func(until time.Time) {
		time.Sleep(time.Until(until))
		if stalled.CompareAndSwap(false, true) {
			time.Sleep(30 * time.Millisecond)
		}
	}}
	rec, _ := o.run(newWindow(0, 100*time.Millisecond), func(int) request { return instant })
	if got := rec.hist.Max(); got < 25_000 {
		t.Fatalf("max latency = %dµs, want >= ~30ms from the stall", got)
	}
	if rec.ok != 100 {
		t.Fatalf("ok = %d, want 100: stalled slots must still be sent, late", rec.ok)
	}
}

func TestOpenLoopDropsWhenInflightFull(t *testing.T) {
	slow := func(context.Context) error { time.Sleep(50 * time.Millisecond); return nil }
	o := openLoop{rate: 1000, maxInflight: 5}
	rec, dropped := o.run(newWindow(0, 100*time.Millisecond), func(int) request { return slow })
	if dropped == 0 || rec.ok+dropped != 100 {
		t.Fatalf("ok=%d dropped=%d, want drops and ok+dropped=100", rec.ok, dropped)
	}
	if rec.ok > 15 {
		t.Fatalf("ok = %d, max-inflight 5 with 50ms calls allows ~10", rec.ok)
	}
}

func TestClosedLoopOnlyRecordsMeasuredWindow(t *testing.T) {
	var calls atomic.Int64
	op := func(context.Context) error { calls.Add(1); time.Sleep(time.Millisecond); return nil }
	rec := runClosed(4, newWindow(30*time.Millisecond, 50*time.Millisecond), func(int) request { return op })
	if rec.ok == 0 || rec.ok >= calls.Load() {
		t.Fatalf("recorded %d of %d calls, want warm-up calls excluded", rec.ok, calls.Load())
	}
}

func TestRecorderErrorsByCode(t *testing.T) {
	r := newRecorder()
	r.record(time.Millisecond, nil)
	r.record(0, domain.ErrUnavailable)
	r.record(0, fmt.Errorf("x: %w", domain.ErrUnavailable))
	r.record(0, context.DeadlineExceeded)
	if r.ok != 1 || r.errs != 3 {
		t.Fatalf("ok=%d errs=%d", r.ok, r.errs)
	}
	if got := breakdown(r.byCode); got != "DEADLINE_EXCEEDED=1;UNAVAILABLE=2" {
		t.Fatalf("breakdown = %q", got)
	}
}

func TestAppendCSVWritesHeaderOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	res := result{cfg: config{label: "x", duration: time.Second}, rec: newRecorder(), finished: time.Now()}
	res.rec.record(time.Millisecond, nil)
	for range 2 {
		if err := appendCSV(path, csvHeader, res.row()); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0][0] != "time" || rows[2][1] != "x" {
		t.Fatalf("rows = %v, want header + 2 data rows", rows)
	}
	for _, r := range rows {
		if len(r) != len(csvHeader) {
			t.Fatalf("row has %d columns, header %d", len(r), len(csvHeader))
		}
	}
}

func TestParsePSTime(t *testing.T) {
	tests := map[string]time.Duration{
		"0:01.23":       1230 * time.Millisecond,
		"12:34.50":      12*time.Minute + 34500*time.Millisecond,
		"01:02:03":      time.Hour + 2*time.Minute + 3*time.Second,
		"2-01:00:00.00": 49 * time.Hour,
	}
	for in, want := range tests {
		got, err := parsePSTime(in)
		if err != nil || got != want {
			t.Errorf("parsePSTime(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := parsePSTime("bogus"); err == nil {
		t.Error("parsePSTime(bogus) succeeded")
	}
	if cpu, err := processCPU(os.Getpid()); err != nil || cpu <= 0 {
		t.Errorf("processCPU(self) = %v, %v", cpu, err)
	}
}
