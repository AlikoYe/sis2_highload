package main

import (
	"fmt"
	"slices"
	"strings"
)

func fmtRPS(v float64) string {
	switch {
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e4:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func fmtUS(v float64) string {
	if v < 1000 {
		return fmt.Sprintf("%.0fµs", v)
	}
	return fmt.Sprintf("%.2fms", v/1000)
}

func fmtSpread(s spread, reps int) string {
	if reps < 2 {
		return fmtUS(s.med)
	}
	return fmt.Sprintf("%s (%s–%s)", fmtUS(s.med), fmtUS(s.min), fmtUS(s.max))
}

func fmtCPU(g group) string {
	if !g.hasServerCPU {
		return "n/a"
	}
	return fmt.Sprintf("%.1fµs", g.serverCPUus.med)
}

func variants(gs []group) []string {
	var vs []string
	for _, g := range gs {
		if !slices.Contains(vs, g.variant) {
			vs = append(vs, g.variant)
		}
	}
	slices.SortFunc(vs, func(a, b string) int { return strings.Compare(variantRank(a), variantRank(b)) })
	return vs
}

func filter(gs []group, keep func(group) bool) []group {
	var out []group
	for _, g := range gs {
		if keep(g) {
			out = append(out, g)
		}
	}
	return out
}

// pivot writes rows = row(g) values, columns = variants, cells = cell(g).
func pivot(b *strings.Builder, gs []group, rowName string, row func(group) string, cell func(group) string) {
	vs := variants(gs)
	fmt.Fprintf(b, "| %s | %s |\n|---|%s\n", rowName, strings.Join(vs, " | "), strings.Repeat("---|", len(vs)))
	var rows []string
	cells := map[string]map[string]string{}
	for _, g := range gs {
		r := row(g)
		if cells[r] == nil {
			cells[r] = map[string]string{}
			rows = append(rows, r)
		}
		cells[r][g.variant] = cell(g)
	}
	for _, r := range rows {
		line := make([]string, len(vs))
		for i, v := range vs {
			line[i] = cmpOr(cells[r][v], "–")
		}
		fmt.Fprintf(b, "| %s | %s |\n", r, strings.Join(line, " | "))
	}
	b.WriteString("\n")
}

func cmpOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func closedCell(g group) string {
	return fmt.Sprintf("%s · %s · %s", fmtRPS(g.rps.med), fmtUS(g.p99.med), fmtCPU(g))
}

func concurrencyRow(g group) string { return fmt.Sprintf("c=%d", g.concurrency) }

// sections groups gs by (op, payload, duration) in first-seen order.
func sections(gs []group) [][]group {
	var keys []string
	by := map[string][]group{}
	for _, g := range gs {
		k := sectionTitle(g)
		if _, ok := by[k]; !ok {
			keys = append(keys, k)
		}
		by[k] = append(by[k], g)
	}
	out := make([][]group, len(keys))
	for i, k := range keys {
		out[i] = by[k]
	}
	return out
}

func sectionTitle(g group) string {
	return fmt.Sprintf("%s/%s, %.0fs runs", g.op, g.payload, g.durationS)
}

func render(gs []group, inputs []string) string {
	var b strings.Builder
	b.WriteString("# Benchmark summary\n\n")
	fmt.Fprintf(&b, "Inputs: %s\n\n", strings.Join(inputs, ", "))
	b.WriteString("Medians across repetitions. Closed-mode cells: **ok/s · p99 · server CPU per request**. ")
	b.WriteString("A digit suffix (e.g. `h2c4`) = that protocol over that many connections. Batch ok/s counts calls (100 events each).\n\n")

	closedMem := filter(gs, func(g group) bool {
		return g.store == "memory" && g.mode == "closed" && !g.reliability && g.op != "status"
	})
	if len(closedMem) > 0 {
		b.WriteString("## (a) Closed loop, memory store\n\n")
		for _, s := range sections(closedMem) {
			fmt.Fprintf(&b, "### %s\n\n", sectionTitle(s[0]))
			pivot(&b, s, "concurrency", concurrencyRow, closedCell)
		}
	}
	if status := filter(gs, func(g group) bool { return g.op == "status" && !g.reliability }); len(status) > 0 {
		b.WriteString("## (b) GetStatus, memory store\n\n")
		for _, s := range sections(status) {
			fmt.Fprintf(&b, "### %s\n\n", sectionTitle(s[0]))
			pivot(&b, s, "concurrency", concurrencyRow, closedCell)
		}
	}
	if pg := filter(gs, func(g group) bool { return g.store == "postgres" && g.mode == "closed" }); len(pg) > 0 {
		b.WriteString("## (c) Closed loop, Postgres store\n\n")
		for _, s := range sections(pg) {
			fmt.Fprintf(&b, "### %s\n\n", sectionTitle(s[0]))
			pivot(&b, s, "concurrency", concurrencyRow, closedCell)
		}
	}
	if open := filter(gs, func(g group) bool { return g.mode == "open" }); len(open) > 0 {
		b.WriteString("## (d) Open loop (fixed arrival rate)\n\n")
		b.WriteString("Cells: **p50 / p99 · dropped**. Latency is measured from the scheduled send time.\n\n")
		pivot(&b, open, "store, rate",
			func(g group) string { return fmt.Sprintf("%s, %.0f/s (%.0fs)", g.store, g.rate, g.durationS) },
			func(g group) string {
				return fmt.Sprintf("%s / %s · %d", fmtUS(g.p50.med), fmtUS(g.p99.med), g.dropped)
			})
	}
	if rel := filter(gs, func(g group) bool { return g.reliability }); len(rel) > 0 {
		b.WriteString("## (e) Reliability (server drops 5% of responses after commit)\n\n")
		b.WriteString("| protocol | config | reps | ok/s | p50 | p99 | retries | errors |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
		for _, g := range rel {
			fmt.Fprintf(&b, "| %s | %s/%s c=%d | %d | %s | %s | %s | %d | %d |\n",
				g.variant, g.op, g.payload, g.concurrency, g.reps, fmtRPS(g.rps.med), fmtUS(g.p50.med), fmtUS(g.p99.med), g.retries, g.errors)
		}
		b.WriteString("\n")
	}

	renderRatios(&b, gs)
	renderDetail(&b, gs)
	return b.String()
}

func describe(g group) string {
	load := fmt.Sprintf("c=%d", g.concurrency)
	if g.mode == "open" {
		load = fmt.Sprintf("%.0f/s", g.rate)
	}
	s := fmt.Sprintf("%s %s/%s %s %s %.0fs", g.store, g.op, g.payload, g.mode, load, g.durationS)
	if g.reliability {
		s += " faults"
	}
	return s
}

func renderRatios(b *strings.Builder, gs []group) {
	rest := map[key]group{}
	for _, g := range gs {
		if g.variant == "rest" {
			rest[g.sameConfig()] = g
		}
	}
	b.WriteString("## Ratios vs REST/HTTP1.1 (same configuration)\n\n")
	b.WriteString(">1 throughput is better; <1 p99 and CPU/req are better.\n\n")
	b.WriteString("| configuration | protocol | throughput × | p99 × | server CPU/req × |\n|---|---|---:|---:|---:|\n")
	for _, g := range gs {
		base, ok := rest[g.sameConfig()]
		if !ok || g.variant == "rest" {
			continue
		}
		cpu := "n/a"
		if g.hasServerCPU && base.hasServerCPU && base.serverCPUus.med > 0 {
			cpu = fmt.Sprintf("%.2f", g.serverCPUus.med/base.serverCPUus.med)
		}
		fmt.Fprintf(b, "| %s | %s | %.2f | %.2f | %s |\n", describe(g), g.variant,
			ratio(g.rps.med, base.rps.med), ratio(g.p99.med, base.p99.med), cpu)
	}
	b.WriteString("\n")
}

func ratio(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func renderDetail(b *strings.Builder, gs []group) {
	b.WriteString("## All groups\n\n")
	b.WriteString("p50/p95/p99 show median (min–max across reps). CPU per successful request.\n\n")
	b.WriteString("| configuration | protocol | reps | ok/s | p50 | p95 | p99 | server CPU/req | client CPU/req | errors | retries | dropped |\n")
	b.WriteString("|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|\n")
	for _, g := range gs {
		fmt.Fprintf(b, "| %s | %s | %d | %s | %s | %s | %s | %s | %.1fµs | %d | %d | %d |\n",
			describe(g), g.variant, g.reps, fmtRPS(g.rps.med),
			fmtSpread(g.p50, g.reps), fmtSpread(g.p95, g.reps), fmtSpread(g.p99, g.reps),
			fmtCPU(g), g.clientCPUus.med, g.errors, g.retries, g.dropped)
	}
}
