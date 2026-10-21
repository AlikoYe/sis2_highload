package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

var csvHeader = []string{
	"time", "label", "protocol", "op", "payload", "store", "mode", "concurrency", "rate", "duration_s",
	"requests", "ok", "errors", "err_breakdown", "dropped", "rps",
	"mean_us", "p50_us", "p90_us", "p95_us", "p99_us", "p999_us", "max_us",
	"attempts", "retries", "client_cpu_ms",
	"conns", // appended later; readers must tolerate its absence in older CSVs
}

type result struct {
	cfg       config
	rec       *recorder
	dropped   int64
	attempts  int64
	retries   int64
	clientCPU time.Duration
	serverCPU time.Duration // 0 when no -server-pid
	finished  time.Time
}

func (r result) rps() float64 { return float64(r.rec.ok) / r.cfg.duration.Seconds() }

func (r result) row() []string {
	h := r.rec.hist
	i := func(v int64) string { return strconv.FormatInt(v, 10) }
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	q := func(p float64) string { return i(h.ValueAtQuantile(p)) }
	return []string{
		r.finished.UTC().Format(time.RFC3339), r.cfg.label, r.cfg.protocol, r.cfg.op, r.cfg.payload, r.cfg.store,
		r.cfg.mode, strconv.Itoa(r.cfg.concurrency), f(r.cfg.rate), f(r.cfg.duration.Seconds()),
		i(r.rec.ok + r.rec.errs), i(r.rec.ok), i(r.rec.errs), breakdown(r.rec.byCode), i(r.dropped), f(r.rps()),
		f(h.Mean()), q(50), q(90), q(95), q(99), q(99.9), i(h.Max()),
		i(r.attempts), i(r.retries), i(r.clientCPU.Milliseconds()),
		strconv.Itoa(r.cfg.conns),
	}
}

func breakdown(m map[string]int64) string {
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	})
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	return strings.Join(parts, ";")
}

// appendCSV appends rows, writing header first if the file is new or empty.
func appendCSV(path string, header []string, rows ...[]string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w := csv.NewWriter(f)
	if st.Size() == 0 {
		_ = w.Write(header)
	}
	for _, r := range rows {
		_ = w.Write(r)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (r result) summary(w io.Writer) {
	h := r.rec.hist
	ms := func(us int64) string {
		return (time.Duration(us) * time.Microsecond).Round(10 * time.Microsecond).String()
	}
	load := fmt.Sprintf("c=%d conns=%d", r.cfg.concurrency, r.cfg.conns)
	if r.cfg.mode == "open" {
		load = fmt.Sprintf("rate=%.0f/s", r.cfg.rate)
	}
	fmt.Fprintf(w, "%s  %s %s/%s %s %s store=%s\n", r.cfg.label, r.cfg.protocol, r.cfg.op, r.cfg.payload, r.cfg.mode, load, r.cfg.store)
	fmt.Fprintf(w, "  %.0f ok/s  ok=%d errors=%d [%s] dropped=%d  attempts=%d retries=%d\n",
		r.rps(), r.rec.ok, r.rec.errs, breakdown(r.rec.byCode), r.dropped, r.attempts, r.retries)
	fmt.Fprintf(w, "  latency p50=%s p90=%s p95=%s p99=%s p99.9=%s max=%s\n",
		ms(h.ValueAtQuantile(50)), ms(h.ValueAtQuantile(90)), ms(h.ValueAtQuantile(95)),
		ms(h.ValueAtQuantile(99)), ms(h.ValueAtQuantile(99.9)), ms(h.Max()))
	fmt.Fprintf(w, "  client cpu=%s", r.clientCPU.Round(time.Millisecond))
	if r.serverCPU > 0 {
		fmt.Fprintf(w, " server cpu=%s", r.serverCPU.Round(time.Millisecond))
	}
	fmt.Fprintln(w)
}

// processCPU reads cumulative user+system CPU of another process via ps.
func processCPU(pid int) (time.Duration, error) {
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("ps -p %d: %w", pid, err)
	}
	return parsePSTime(strings.TrimSpace(string(out)))
}

// parsePSTime parses ps "time" output: [[dd-]hh:]mm:ss[.cc].
func parsePSTime(s string) (time.Duration, error) {
	if s == "" {
		return 0, errors.New("empty ps time")
	}
	var days int
	if d, rest, ok := strings.Cut(s, "-"); ok {
		n, err := strconv.Atoi(d)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", s, err)
		}
		days, s = n, rest
	}
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("ps time %q: unexpected format", s)
	}
	secs, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		return 0, fmt.Errorf("ps time %q: %w", s, err)
	}
	total := time.Duration(secs * float64(time.Second))
	units := []time.Duration{time.Minute, time.Hour}
	for i, j := len(parts)-2, 0; i >= 0; i, j = i-1, j+1 {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", s, err)
		}
		total += time.Duration(n) * units[j]
	}
	return total + time.Duration(days)*24*time.Hour, nil
}
