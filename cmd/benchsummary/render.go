package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

func fmtRPS(rps float64) string {
	switch {
	case rps >= 1e6:
		return fmt.Sprintf("%.2fM", rps/1e6)
	case rps >= 1e4:
		return fmt.Sprintf("%.1fk", rps/1e3)
	default:
		return fmt.Sprintf("%.0f", rps)
	}
}

func fmtUS(us float64) string {
	if us < usPerMS {
		return fmt.Sprintf("%.0fµs", us)
	}
	return fmt.Sprintf("%.2fms", us/usPerMS)
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

func variants(groups []group) []string {
	var names []string
	for _, g := range groups {
		if !slices.Contains(names, g.variant) {
			names = append(names, g.variant)
		}
	}
	slices.SortFunc(names, func(a, b string) int { return strings.Compare(variantRank(a), variantRank(b)) })
	return names
}

func filter(groups []group, keep func(group) bool) []group {
	var kept []group
	for _, g := range groups {
		if keep(g) {
			kept = append(kept, g)
		}
	}
	return kept
}

// pivot writes rows = rowLabel(g) values, columns = variants, cells = cell(g).
func pivot(b *strings.Builder, groups []group, rowName string, rowLabel, cell func(group) string) {
	columns := variants(groups)
	fmt.Fprintf(b, "| %s | %s |\n|---|%s\n", rowName, strings.Join(columns, " | "), strings.Repeat("---|", len(columns)))
	var rowOrder []string
	cells := map[string]map[string]string{}
	for _, g := range groups {
		label := rowLabel(g)
		if cells[label] == nil {
			cells[label] = map[string]string{}
			rowOrder = append(rowOrder, label)
		}
		cells[label][g.variant] = cell(g)
	}
	for _, label := range rowOrder {
		line := make([]string, len(columns))
		for i, variant := range columns {
			line[i] = cmp.Or(cells[label][variant], "–")
		}
		fmt.Fprintf(b, "| %s | %s |\n", label, strings.Join(line, " | "))
	}
	b.WriteString("\n")
}

func closedCell(g group) string {
	return fmt.Sprintf("%s · %s · %s", fmtRPS(g.rps.med), fmtUS(g.p99.med), fmtCPU(g))
}

func concurrencyRow(g group) string { return fmt.Sprintf("c=%d", g.concurrency) }

// sections splits groups by (op, payload, duration) in first-seen order.
func sections(groups []group) [][]group {
	var titles []string
	byTitle := map[string][]group{}
	for _, g := range groups {
		title := sectionTitle(g)
		if _, ok := byTitle[title]; !ok {
			titles = append(titles, title)
		}
		byTitle[title] = append(byTitle[title], g)
	}
	out := make([][]group, len(titles))
	for i, title := range titles {
		out[i] = byTitle[title]
	}
	return out
}

func sectionTitle(g group) string {
	return fmt.Sprintf("%s/%s, %.0fs runs", g.op, g.payload, g.durationS)
}

func render(groups []group, inputs []string) string {
	var b strings.Builder
	b.WriteString("# Benchmark summary\n\n")
	fmt.Fprintf(&b, "Inputs: %s\n\n", strings.Join(inputs, ", "))
	b.WriteString("Medians across repetitions. Closed-mode cells: **ok/s · p99 · server CPU per request**. ")
	b.WriteString("A digit suffix (e.g. `h2c4`) = that protocol over that many connections. Batch ok/s counts calls (100 events each).\n\n")

	renderClosedSections(&b, "(a) Closed loop, memory store", filter(groups, func(g group) bool {
		return g.store == "memory" && g.mode == "closed" && !g.reliability && g.op != "status"
	}))
	renderClosedSections(&b, "(b) GetStatus, memory store", filter(groups, func(g group) bool {
		return g.op == "status" && !g.reliability
	}))
	renderClosedSections(&b, "(c) Closed loop, Postgres store", filter(groups, func(g group) bool {
		return g.store == "postgres" && g.mode == "closed"
	}))
	renderOpenLoop(&b, filter(groups, func(g group) bool { return g.mode == "open" }))
	renderReliability(&b, filter(groups, func(g group) bool { return g.reliability }))
	renderRatios(&b, groups)
	renderDetail(&b, groups)
	return b.String()
}

// renderClosedSections writes one concurrency x protocol table per op/payload; nothing if empty.
func renderClosedSections(b *strings.Builder, title string, groups []group) {
	if len(groups) == 0 {
		return
	}
	fmt.Fprintf(b, "## %s\n\n", title)
	for _, section := range sections(groups) {
		fmt.Fprintf(b, "### %s\n\n", sectionTitle(section[0]))
		pivot(b, section, "concurrency", concurrencyRow, closedCell)
	}
}

func renderOpenLoop(b *strings.Builder, groups []group) {
	if len(groups) == 0 {
		return
	}
	b.WriteString("## (d) Open loop (fixed arrival rate)\n\n")
	b.WriteString("Cells: **p50 / p99 · dropped**. Latency is measured from the scheduled send time.\n\n")
	pivot(b, groups, "store, rate",
		func(g group) string { return fmt.Sprintf("%s, %.0f/s (%.0fs)", g.store, g.rate, g.durationS) },
		func(g group) string {
			return fmt.Sprintf("%s / %s · %d", fmtUS(g.p50.med), fmtUS(g.p99.med), g.dropped)
		})
}

func renderReliability(b *strings.Builder, groups []group) {
	if len(groups) == 0 {
		return
	}
	b.WriteString("## (e) Reliability (server drops 5% of responses after commit)\n\n")
	b.WriteString("| protocol | config | reps | ok/s | p50 | p99 | retries | errors |\n|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, g := range groups {
		fmt.Fprintf(b, "| %s | %s/%s c=%d | %d | %s | %s | %s | %d | %d |\n",
			g.variant, g.op, g.payload, g.concurrency, g.reps,
			fmtRPS(g.rps.med), fmtUS(g.p50.med), fmtUS(g.p99.med), g.retries, g.errors)
	}
	b.WriteString("\n")
}

func describe(g group) string {
	load := fmt.Sprintf("c=%d", g.concurrency)
	if g.mode == "open" {
		load = fmt.Sprintf("%.0f/s", g.rate)
	}
	description := fmt.Sprintf("%s %s/%s %s %s %.0fs", g.store, g.op, g.payload, g.mode, load, g.durationS)
	if g.reliability {
		description += " faults"
	}
	return description
}

func renderRatios(b *strings.Builder, groups []group) {
	restByConfig := map[groupKey]group{}
	for _, g := range groups {
		if g.variant == "rest" {
			restByConfig[g.sameConfig()] = g
		}
	}
	b.WriteString("## Ratios vs REST/HTTP1.1 (same configuration)\n\n")
	b.WriteString(">1 throughput is better; <1 p99 and CPU/req are better.\n\n")
	b.WriteString("| configuration | protocol | throughput × | p99 × | server CPU/req × |\n|---|---|---:|---:|---:|\n")
	for _, g := range groups {
		base, ok := restByConfig[g.sameConfig()]
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

func renderDetail(b *strings.Builder, groups []group) {
	b.WriteString("## All groups\n\n")
	b.WriteString("p50/p95/p99 show median (min–max across reps). CPU per successful request.\n\n")
	b.WriteString("| configuration | protocol | reps | ok/s | p50 | p95 | p99 | server CPU/req | client CPU/req | errors | retries | dropped |\n")
	b.WriteString("|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|\n")
	for _, g := range groups {
		fmt.Fprintf(b, "| %s | %s | %d | %s | %s | %s | %s | %s | %.1fµs | %d | %d | %d |\n",
			describe(g), g.variant, g.reps, fmtRPS(g.rps.med),
			fmtSpread(g.p50, g.reps), fmtSpread(g.p95, g.reps), fmtSpread(g.p99, g.reps),
			fmtCPU(g), g.clientCPUus.med, g.errors, g.retries, g.dropped)
	}
}
