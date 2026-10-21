package main

import (
	"encoding/csv"
	"strconv"
	"strings"
	"testing"
)

const header = "time,label,protocol,op,payload,store,mode,concurrency,rate,duration_s,requests,ok,errors,err_breakdown,dropped,rps,mean_us,p50_us,p90_us,p95_us,p99_us,p999_us,max_us,attempts,retries,client_cpu_ms"

func row(label, protocol string, conc int, rps, p99 float64, ok int64, extra string) string {
	i, f := strconv.Itoa, func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	n := strconv.FormatInt(ok, 10)
	return strings.Join([]string{
		"t", label, protocol, "publish", "small", "memory", "closed", i(conc), "0.0", "30.0",
		n, n, "0", "", "0", f(rps), "1", "100", "1", "200", f(p99), "1", "1", n, "2", "3000",
	}, ",") + extra
}

func mustRead(t *testing.T, csv string, cpu map[string]float64) []run {
	t.Helper()
	runs, err := readRuns(strings.NewReader(csv), cpu)
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

func TestMedianOddAndEven(t *testing.T) {
	if got := median([]float64{5, 1, 3}); got != (spread{3, 1, 5}) {
		t.Fatalf("odd median = %+v", got)
	}
	if got := median([]float64{4, 1, 3, 2}); got != (spread{2.5, 1, 4}) {
		t.Fatalf("even median = %+v", got)
	}
}

func TestGroupingAcrossRepsAndProtocols(t *testing.T) {
	csv := header + "\n" +
		row("mem-grpc-r1", "grpc", 64, 100, 900, 3000, "") + "\n" +
		row("mem-grpc-r2", "grpc", 64, 300, 1100, 3000, "") + "\n" +
		row("mem-grpc-r3", "grpc", 64, 200, 1000, 3000, "") + "\n" +
		row("mem-rest-r1", "rest", 64, 50, 2000, 3000, "") + "\n"
	cpu := map[string]float64{"mem-grpc-r1": 30, "mem-grpc-r2": 90, "mem-grpc-r3": 60}
	gs := groupRuns(mustRead(t, csv, cpu))
	if len(gs) != 2 {
		t.Fatalf("got %d groups, want 2 (grpc, rest)", len(gs))
	}
	g := gs[0]
	if g.variant != "grpc" || g.reps != 3 || g.rps != (spread{200, 100, 300}) || g.p99 != (spread{1000, 900, 1100}) {
		t.Fatalf("grpc group = %+v", g)
	}
	// 60ms server CPU over 3000 ok requests = 20µs/request.
	if !g.hasServerCPU || g.serverCPUus.med != 20 || g.clientCPUus.med != 1000 {
		t.Fatalf("cpu/req = server %v client %v", g.serverCPUus, g.clientCPUus)
	}
	if gs[1].hasServerCPU {
		t.Fatal("rest has no server CPU rows, must be marked unknown")
	}
	if g.retries != 6 {
		t.Fatalf("retries total = %d, want 6", g.retries)
	}
}

func TestConnsColumnOptional(t *testing.T) {
	oldCSV := header + "\n" + row("a", "h2c", 64, 1, 1, 1, "") + "\n" + row("b", "rest", 64, 1, 1, 1, "")
	runs := mustRead(t, oldCSV, nil)
	if runs[0].conns != 1 || runs[1].conns != 0 {
		t.Fatalf("defaults: h2c=%d rest=%d, want 1 and 0", runs[0].conns, runs[1].conns)
	}
	newCSV := header + ",conns\n" + row("a", "h2c", 64, 1, 1, 1, ",8") + "\n" + row("b", "h2c", 64, 1, 1, 1, ",1")
	gs := groupRuns(mustRead(t, newCSV, nil))
	if len(gs) != 2 || gs[0].variant != "h2c" || gs[1].variant != "h2c8" {
		t.Fatalf("variants = %v, want h2c then h2c8", []string{gs[0].variant, gs[len(gs)-1].variant})
	}
}

func TestReliabilityRunsGroupSeparately(t *testing.T) {
	csv := header + "\n" + row("mem-grpc-publish-small-c64-r1", "grpc", 64, 1, 1, 1, "") + "\n" +
		row("rel-grpc-publish-small-c64-r1", "grpc", 64, 1, 1, 1, "")
	if gs := groupRuns(mustRead(t, csv, nil)); len(gs) != 2 {
		t.Fatalf("got %d groups, want baseline and reliability apart", len(gs))
	}
}

func TestRenderHasSections(t *testing.T) {
	csv := header + "\n" + row("mem-grpc-r1", "grpc", 64, 200, 1000, 3000, "") + "\n" + row("mem-rest-r1", "rest", 64, 100, 2000, 3000, "")
	md := render(groupRuns(mustRead(t, csv, nil)), []string{"x.csv"})
	for _, want := range []string{"(a) Closed loop, memory store", "| c=64 |", "Ratios vs REST", "| 2.00 | 0.50 |"} {
		if !strings.Contains(md, want) {
			t.Errorf("summary missing %q:\n%s", want, md)
		}
	}
}

func TestSummaryCSV(t *testing.T) {
	in := header + "\n" +
		row("mem-grpc-r1", "grpc", 64, 100, 900, 3000, "") + "\n" +
		row("mem-grpc-r2", "grpc", 64, 300, 1100, 3000, "") + "\n" +
		row("mem-grpc-r3", "grpc", 64, 200, 1000, 3000, "") + "\n" +
		row("mem-rest-r1", "rest", 64, 50, 2000, 3000, "")
	gs := groupRuns(mustRead(t, in, map[string]float64{"mem-grpc-r1": 60, "mem-grpc-r2": 60, "mem-grpc-r3": 60}))

	var buf strings.Builder
	if err := writeSummaryCSV(&buf, gs); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(buf.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want header + 2 groups", len(rows))
	}
	col := map[string]int{}
	for i, name := range rows[0] {
		col[name] = i
	}
	grpc, rest := rows[1], rows[2]
	for name, want := range map[string]string{
		"variant": "grpc", "reps": "3", "rps_med": "200.0", "rps_min": "100.0", "rps_max": "300.0",
		"p99_us_med": "1000.0", "server_cpu_us_per_req_med": "20.0", "retries": "6",
	} {
		if got := grpc[col[name]]; got != want {
			t.Errorf("grpc %s = %q, want %q", name, got, want)
		}
	}
	if rest[col["variant"]] != "rest" || rest[col["server_cpu_us_per_req_med"]] != "" {
		t.Errorf("rest row: variant %q, server cpu %q (want empty when unknown)", rest[col["variant"]], rest[col["server_cpu_us_per_req_med"]])
	}
	for _, r := range rows {
		if len(r) != len(summaryHeader) {
			t.Fatalf("row has %d columns, header has %d", len(r), len(summaryHeader))
		}
	}
}
