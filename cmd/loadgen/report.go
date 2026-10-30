package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"maps"
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

var serverCPUHeader = []string{"label", "server_cpu_ms", "duration_s"}

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

func itoa(v int64) string   { return strconv.FormatInt(v, 10) }
func ftoa(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }

func (r result) row() []string {
	hist := r.rec.hist
	quantile := func(percent float64) string { return itoa(hist.ValueAtQuantile(percent)) }
	return []string{
		r.finished.UTC().Format(time.RFC3339), r.cfg.label, r.cfg.protocol, r.cfg.op, r.cfg.payload, r.cfg.store,
		r.cfg.mode, strconv.Itoa(r.cfg.concurrency), ftoa(r.cfg.rate), ftoa(r.cfg.duration.Seconds()),
		itoa(r.rec.ok + r.rec.errs), itoa(r.rec.ok), itoa(r.rec.errs), breakdown(r.rec.byCode), itoa(r.dropped), ftoa(r.rps()),
		ftoa(hist.Mean()), quantile(50), quantile(90), quantile(95), quantile(99), quantile(99.9), itoa(hist.Max()),
		itoa(r.attempts), itoa(r.retries), itoa(r.clientCPU.Milliseconds()),
		strconv.Itoa(r.cfg.conns),
	}
}

// breakdown formats error counts as "CODE=n;CODE=n", sorted by code.
func breakdown(countByCode map[string]int64) string {
	codes := slices.Sorted(maps.Keys(countByCode))
	parts := make([]string, len(codes))
	for i, code := range codes {
		parts[i] = fmt.Sprintf("%s=%d", code, countByCode[code])
	}
	return strings.Join(parts, ";")
}

// appendCSV appends rows, writing header first if the file is new or empty.
func appendCSV(path string, header []string, rows ...[]string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w := csv.NewWriter(f)
	if info.Size() == 0 {
		_ = w.Write(header)
	}
	for _, row := range rows {
		_ = w.Write(row)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (r result) summary(w io.Writer) {
	hist := r.rec.hist
	duration := func(us int64) string {
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
		duration(hist.ValueAtQuantile(50)), duration(hist.ValueAtQuantile(90)), duration(hist.ValueAtQuantile(95)),
		duration(hist.ValueAtQuantile(99)), duration(hist.ValueAtQuantile(99.9)), duration(hist.Max()))
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
func parsePSTime(text string) (time.Duration, error) {
	if text == "" {
		return 0, errors.New("empty ps time")
	}
	var total time.Duration
	clock := text
	if daysPart, rest, ok := strings.Cut(text, "-"); ok {
		days, err := strconv.Atoi(daysPart)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", text, err)
		}
		total, clock = time.Duration(days)*24*time.Hour, rest
	}

	fields := strings.Split(clock, ":")
	if len(fields) < 2 || len(fields) > 3 {
		return 0, fmt.Errorf("ps time %q: unexpected format", text)
	}
	seconds, err := strconv.ParseFloat(fields[len(fields)-1], 64)
	if err != nil {
		return 0, fmt.Errorf("ps time %q: %w", text, err)
	}
	total += time.Duration(seconds * float64(time.Second))

	// Fields left of the seconds, read right to left, are minutes then hours.
	units := []time.Duration{time.Minute, time.Hour}
	for i, field := range slices.Backward(fields[:len(fields)-1]) {
		n, err := strconv.Atoi(field)
		if err != nil {
			return 0, fmt.Errorf("ps time %q: %w", text, err)
		}
		total += time.Duration(n) * units[len(fields)-2-i]
	}
	return total, nil
}
