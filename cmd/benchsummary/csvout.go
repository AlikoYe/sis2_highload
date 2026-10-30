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
func writeSummaryCSV(w io.Writer, groups []group) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(summaryHeader); err != nil {
		return err
	}
	ftoa := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	medMinMax := func(s spread) []string { return []string{ftoa(s.med), ftoa(s.min), ftoa(s.max)} }
	for _, g := range groups {
		row := []string{
			g.variant, g.op, g.payload, g.store, g.mode, strconv.Itoa(g.concurrency), ftoa(g.rate),
			strconv.Itoa(g.conns), ftoa(g.durationS), strconv.FormatBool(g.reliability), strconv.Itoa(g.reps),
		}
		row = append(row, medMinMax(g.rps)...)
		row = append(row, medMinMax(g.p50)...)
		row = append(row, medMinMax(g.p95)...)
		row = append(row, medMinMax(g.p99)...)
		if g.hasServerCPU {
			row = append(row, medMinMax(g.serverCPUus)...)
		} else {
			row = append(row, "", "", "")
		}
		row = append(row, medMinMax(g.clientCPUus)...)
		row = append(row, strconv.FormatInt(g.errors, 10), strconv.FormatInt(g.retries, 10), strconv.FormatInt(g.dropped, 10))
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
