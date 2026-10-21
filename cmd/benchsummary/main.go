// Command benchsummary aggregates loadgen CSVs into results/summary.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	out := flag.String("out", "results/summary.md", "markdown output path; a .csv with the same name is written next to it")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: benchsummary [-out file] [bench.csv ...]  (default: results/bench_*.csv)")
		flag.PrintDefaults()
	}
	flag.Parse()
	if err := summarize(flag.Args(), *out); err != nil {
		fmt.Fprintln(os.Stderr, "benchsummary:", err)
		os.Exit(1)
	}
}

func summarize(inputs []string, out string) error {
	if len(inputs) == 0 {
		matches, err := filepath.Glob("results/bench_*.csv")
		if err != nil {
			return err
		}
		inputs = matches
	}
	var files []string
	var runs []run
	for _, path := range inputs {
		if strings.HasSuffix(path, "_server_cpu.csv") {
			continue
		}
		rs, err := load(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		files = append(files, filepath.Base(path))
		for _, r := range rs {
			// Profiled runs carry pprof overhead; they are for flame graphs, not numbers.
			if !strings.HasPrefix(r.label, "prof-") {
				runs = append(runs, r)
			}
		}
	}
	if len(runs) == 0 {
		return errors.New("no runs found")
	}
	groups := groupRuns(runs)
	if err := os.WriteFile(out, []byte(render(groups, files)), 0o644); err != nil {
		return err
	}
	csvOut := strings.TrimSuffix(out, filepath.Ext(out)) + ".csv"
	f, err := os.Create(csvOut)
	if err != nil {
		return err
	}
	if err := writeSummaryCSV(f, groups); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Printf("wrote %s and %s (%d runs, %d groups, %d files)\n", out, csvOut, len(runs), len(groups), len(files))
	return nil
}

func load(path string) ([]run, error) {
	cpu := map[string]float64{}
	cpuPath := strings.TrimSuffix(path, ".csv") + "_server_cpu.csv"
	if f, err := os.Open(cpuPath); err == nil {
		cpu, err = readServerCPU(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", cpuPath, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readRuns(f, cpu)
}
