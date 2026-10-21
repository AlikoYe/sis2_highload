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

type key struct {
	variant, op, payload, store, mode string
	concurrency, conns                int
	rate, durationS                   float64
	reliability                       bool
}

func (r run) key() key {
	return key{r.variant(), r.op, r.payload, r.store, r.mode, r.concurrency, r.conns, r.rate, r.durationS, r.reliability()}
}

// sameConfig drops the protocol so groups can be compared across protocols.
func (k key) sameConfig() key {
	k.variant, k.conns = "", 0
	return k
}

type spread struct{ med, min, max float64 }

type group struct {
	key
	reps                     int
	rps, p50, p95, p99       spread
	serverCPUus, clientCPUus spread // per successful request; NaN-free: zero when unknown
	hasServerCPU             bool
	errors, retries, dropped int64
}

func median(xs []float64) spread {
	if len(xs) == 0 {
		return spread{}
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	m := s[len(s)/2]
	if len(s)%2 == 0 {
		m = (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	return spread{med: m, min: s[0], max: s[len(s)-1]}
}

func perReq(ms float64, ok int64) float64 {
	if ok == 0 {
		return 0
	}
	return ms * 1000 / float64(ok)
}

func groupRuns(runs []run) []group {
	byKey := map[key][]run{}
	var order []key
	for _, r := range runs {
		k := r.key()
		if _, seen := byKey[k]; !seen {
			order = append(order, k)
		}
		byKey[k] = append(byKey[k], r)
	}
	groups := make([]group, 0, len(order))
	for _, k := range order {
		rs := byKey[k]
		g := group{key: k, reps: len(rs), hasServerCPU: true}
		var rps, p50, p95, p99, scpu, ccpu []float64
		for _, r := range rs {
			rps, p50, p95, p99 = append(rps, r.rps), append(p50, r.p50), append(p95, r.p95), append(p99, r.p99)
			ccpu = append(ccpu, perReq(r.clientCPUms, r.ok))
			if r.hasServerCPU {
				scpu = append(scpu, perReq(r.serverCPUms, r.ok))
			} else {
				g.hasServerCPU = false
			}
			g.errors += r.errors
			g.retries += r.retries
			g.dropped += r.dropped
		}
		g.rps, g.p50, g.p95, g.p99 = median(rps), median(p50), median(p95), median(p99)
		g.serverCPUus, g.clientCPUus = median(scpu), median(ccpu)
		groups = append(groups, g)
	}
	slices.SortStableFunc(groups, func(a, b group) int {
		return cmp.Or(
			cmp.Compare(a.store, b.store), cmp.Compare(a.mode, b.mode), cmp.Compare(a.op, b.op),
			cmp.Compare(a.payload, b.payload), cmp.Compare(a.durationS, b.durationS), cmp.Compare(a.concurrency, b.concurrency),
			cmp.Compare(a.rate, b.rate), cmp.Compare(variantRank(a.variant), variantRank(b.variant)),
		)
	})
	return groups
}

// variantRank orders columns grpc, grpc4..., rest, h2c, h2c8...
func variantRank(v string) string {
	switch {
	case strings.HasPrefix(v, "grpc"):
		return "0" + v
	case v == "rest":
		return "1"
	default:
		return "2" + v
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
	col := map[string]int{}
	for i, name := range rows[0] {
		col[name] = i
	}
	var runs []run
	for n, row := range rows[1:] {
		get := func(name string) string {
			if i, ok := col[name]; ok && i < len(row) {
				return row[i]
			}
			return ""
		}
		var perr error
		num := func(name string) float64 {
			v, err := strconv.ParseFloat(get(name), 64)
			if err != nil && perr == nil {
				perr = fmt.Errorf("row %d: column %q: %w", n+2, name, err)
			}
			return v
		}
		r := run{
			label: get("label"), protocol: get("protocol"), op: get("op"), payload: get("payload"),
			store: get("store"), mode: get("mode"),
			concurrency: int(num("concurrency")), rate: num("rate"), durationS: num("duration_s"),
			ok: int64(num("ok")), errors: int64(num("errors")), dropped: int64(num("dropped")), retries: int64(num("retries")),
			rps: num("rps"), p50: num("p50_us"), p95: num("p95_us"), p99: num("p99_us"),
			clientCPUms: num("client_cpu_ms"),
		}
		if perr != nil {
			return nil, perr
		}
		if v := get("conns"); v != "" {
			c, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("row %d: conns: %w", n+2, err)
			}
			r.conns = c
		} else if r.protocol != "rest" {
			r.conns = 1
		}
		r.serverCPUms, r.hasServerCPU = serverCPU[r.label]
		runs = append(runs, r)
	}
	return runs, nil
}

// readServerCPU parses label,server_cpu_ms,duration_s.
func readServerCPU(r io.Reader) (map[string]float64, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, row := range rows {
		if len(row) < 2 || row[0] == "label" {
			continue
		}
		v, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("server cpu %q: %w", row[1], err)
		}
		out[row[0]] = v
	}
	return out, nil
}
