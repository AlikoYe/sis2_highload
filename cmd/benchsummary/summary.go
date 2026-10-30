package main

import (
	"cmp"
	"encoding/csv"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// run is one CSV row from loadgen joined with its server CPU.
type run struct {
	label, protocol, op, payload, store, mode string
	concurrency, conns                        int
	rate, durationS                           float64
	ok, errors, dropped, retries              int64
	rps, p50, p95, p99                        float64 // latencies in µs
	clientCPUms, serverCPUms                  float64
	hasServerCPU                              bool
}

// variant names the protocol plus a non-default connection count, e.g. h2c8.
func (r run) variant() string {
	if r.protocol == "rest" || r.conns <= 1 {
		return r.protocol
	}
	return r.protocol + strconv.Itoa(r.conns)
}

// reliability runs are those against a fault-injecting server; bench.sh labels them rel-*.
func (r run) reliability() bool {
	return strings.HasPrefix(r.label, "rel-") || strings.Contains(r.label, "fault")
}

// groupKey identifies runs that are repetitions of the same configuration.
type groupKey struct {
	variant, op, payload, store, mode string
	concurrency, conns                int
	rate, durationS                   float64
	reliability                       bool
}

func (r run) groupKey() groupKey {
	return groupKey{r.variant(), r.op, r.payload, r.store, r.mode, r.concurrency, r.conns, r.rate, r.durationS, r.reliability()}
}

// sameConfig drops the protocol so groups can be compared across protocols.
func (k groupKey) sameConfig() groupKey {
	k.variant, k.conns = "", 0
	return k
}

type spread struct{ med, min, max float64 }

type group struct {
	groupKey
	reps                     int
	rps, p50, p95, p99       spread
	serverCPUus, clientCPUus spread // per successful request; NaN-free: zero when unknown
	hasServerCPU             bool
	errors, retries, dropped int64
}

// median also reports the min and max, which the summary shows as the spread across reps.
func median(values []float64) spread {
	if len(values) == 0 {
		return spread{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	mid := len(sorted) / 2
	med := sorted[mid]
	if len(sorted)%2 == 0 {
		med = (sorted[mid-1] + sorted[mid]) / 2
	}
	return spread{med: med, min: sorted[0], max: sorted[len(sorted)-1]}
}

const usPerMS = 1000

// perRequestUS converts total CPU milliseconds into microseconds per successful request.
func perRequestUS(cpuMS float64, ok int64) float64 {
	if ok == 0 {
		return 0
	}
	return cpuMS * usPerMS / float64(ok)
}

func groupRuns(runs []run) []group {
	byKey := map[groupKey][]run{}
	var firstSeen []groupKey
	for _, r := range runs {
		key := r.groupKey()
		if _, seen := byKey[key]; !seen {
			firstSeen = append(firstSeen, key)
		}
		byKey[key] = append(byKey[key], r)
	}
	groups := make([]group, 0, len(firstSeen))
	for _, key := range firstSeen {
		groups = append(groups, summarizeGroup(key, byKey[key]))
	}
	slices.SortStableFunc(groups, compareGroups)
	return groups
}

func summarizeGroup(key groupKey, reps []run) group {
	g := group{groupKey: key, reps: len(reps), hasServerCPU: true}
	var rps, p50, p95, p99, serverCPU, clientCPU []float64
	for _, r := range reps {
		rps = append(rps, r.rps)
		p50 = append(p50, r.p50)
		p95 = append(p95, r.p95)
		p99 = append(p99, r.p99)
		clientCPU = append(clientCPU, perRequestUS(r.clientCPUms, r.ok))
		if r.hasServerCPU {
			serverCPU = append(serverCPU, perRequestUS(r.serverCPUms, r.ok))
		} else {
			g.hasServerCPU = false
		}
		g.errors += r.errors
		g.retries += r.retries
		g.dropped += r.dropped
	}
	g.rps, g.p50, g.p95, g.p99 = median(rps), median(p50), median(p95), median(p99)
	g.serverCPUus, g.clientCPUus = median(serverCPU), median(clientCPU)
	return g
}

func compareGroups(a, b group) int {
	return cmp.Or(
		cmp.Compare(a.store, b.store),
		cmp.Compare(a.mode, b.mode),
		cmp.Compare(a.op, b.op),
		cmp.Compare(a.payload, b.payload),
		cmp.Compare(a.durationS, b.durationS),
		cmp.Compare(a.concurrency, b.concurrency),
		cmp.Compare(a.rate, b.rate),
		cmp.Compare(variantRank(a.variant), variantRank(b.variant)),
	)
}

// variantRank orders columns grpc, grpc4..., rest, h2c, h2c8...
func variantRank(variant string) string {
	switch {
	case strings.HasPrefix(variant, "grpc"):
		return "0" + variant
	case variant == "rest":
		return "1"
	default:
		return "2" + variant
	}
}

// readRuns parses a loadgen CSV by column name, so older files without "conns" still load.
func readRuns(r io.Reader, serverCPU map[string]float64) ([]run, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	columns := map[string]int{}
	for i, name := range rows[0] {
		columns[name] = i
	}
	var runs []run
	for i, row := range rows[1:] {
		line := i + 2 // 1-based, after the header
		parsed, err := parseRun(csvRow{columns: columns, values: row})
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		parsed.serverCPUms, parsed.hasServerCPU = serverCPU[parsed.label]
		runs = append(runs, parsed)
	}
	return runs, nil
}

// csvRow looks values up by column name and remembers the first parse error.
type csvRow struct {
	columns map[string]int
	values  []string
	err     error
}

func (row *csvRow) text(name string) string {
	if i, ok := row.columns[name]; ok && i < len(row.values) {
		return row.values[i]
	}
	return ""
}

func (row *csvRow) number(name string) float64 {
	v, err := strconv.ParseFloat(row.text(name), 64)
	if err != nil && row.err == nil {
		row.err = fmt.Errorf("column %q: %w", name, err)
	}
	return v
}

func parseRun(row csvRow) (run, error) {
	r := run{
		label:       row.text("label"),
		protocol:    row.text("protocol"),
		op:          row.text("op"),
		payload:     row.text("payload"),
		store:       row.text("store"),
		mode:        row.text("mode"),
		concurrency: int(row.number("concurrency")),
		rate:        row.number("rate"),
		durationS:   row.number("duration_s"),
		ok:          int64(row.number("ok")),
		errors:      int64(row.number("errors")),
		dropped:     int64(row.number("dropped")),
		retries:     int64(row.number("retries")),
		rps:         row.number("rps"),
		p50:         row.number("p50_us"),
		p95:         row.number("p95_us"),
		p99:         row.number("p99_us"),
		clientCPUms: row.number("client_cpu_ms"),
	}
	if row.err != nil {
		return run{}, row.err
	}
	switch conns := row.text("conns"); {
	case conns != "":
		n, err := strconv.Atoi(conns)
		if err != nil {
			return run{}, fmt.Errorf("conns: %w", err)
		}
		r.conns = n
	case r.protocol != "rest":
		r.conns = 1 // older CSVs predate -conns: one connection
	}
	return r, nil
}

// readServerCPU parses label,server_cpu_ms,duration_s.
func readServerCPU(r io.Reader) (map[string]float64, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	cpuByLabel := map[string]float64{}
	for _, row := range rows {
		if len(row) < 2 || row[0] == "label" {
			continue
		}
		cpuMS, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("server cpu %q: %w", row[1], err)
		}
		cpuByLabel[row[0]] = cpuMS
	}
	return cpuByLabel, nil
}
