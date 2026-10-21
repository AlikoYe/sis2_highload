package main

import (
	"encoding/csv"
	"io"
	"strconv"
)

var summaryHeader = []string{
	"variant", "op", "payload", "store", "mode", "concurrency", "rate", "conns", "duration_s", "reliability", "reps",
	"rps_med", "rps_min", "rps_max",
	"p50_us_med", "p50_us_min", "p50_us_max",
	"p95_us_med", "p95_us_min", "p95_us_max",
	"p99_us_med", "p99_us_min", "p99_us_max",
	"server_cpu_us_per_req_med", "server_cpu_us_per_req_min", "server_cpu_us_per_req_max",
	"client_cpu_us_per_req_med", "client_cpu_us_per_req_min", "client_cpu_us_per_req_max",
	"errors", "retries", "dropped",
}

// writeSummaryCSV writes one row per group for charting; server CPU is empty when unknown.
func writeSummaryCSV(w io.Writer, gs []group) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(summaryHeader); err != nil {
		return err
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	tri := func(s spread) []string { return []string{f(s.med), f(s.min), f(s.max)} }
	for _, g := range gs {
		row := []string{
			g.variant, g.op, g.payload, g.store, g.mode, strconv.Itoa(g.concurrency), f(g.rate),
			strconv.Itoa(g.conns), f(g.durationS), strconv.FormatBool(g.reliability), strconv.Itoa(g.reps),
		}
		row = append(row, tri(g.rps)...)
		row = append(row, tri(g.p50)...)
		row = append(row, tri(g.p95)...)
		row = append(row, tri(g.p99)...)
		if g.hasServerCPU {
			row = append(row, tri(g.serverCPUus)...)
		} else {
			row = append(row, "", "", "")
		}
		row = append(row, tri(g.clientCPUus)...)
		row = append(row, strconv.FormatInt(g.errors, 10), strconv.FormatInt(g.retries, 10), strconv.FormatInt(g.dropped, 10))
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
