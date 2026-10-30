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
	runs, files, err := loadRuns(inputs)
	if err != nil {
		return err
	}
	groups := groupRuns(runs)
	if err := os.WriteFile(out, []byte(render(groups, files)), 0o644); err != nil {
		return err
	}
	csvOut := strings.TrimSuffix(out, filepath.Ext(out)) + ".csv"
	if err := writeSummaryCSVFile(csvOut, groups); err != nil {
		return err
	}
	fmt.Printf("wrote %s and %s (%d runs, %d groups, %d files)\n", out, csvOut, len(runs), len(groups), len(files))
	return nil
}

// loadRuns reads every bench CSV (default results/bench_*.csv) and returns the
// runs plus the file names used.
func loadRuns(inputs []string) ([]run, []string, error) {
	if len(inputs) == 0 {
		matches, err := filepath.Glob("results/bench_*.csv")
		if err != nil {
			return nil, nil, err
		}
		inputs = matches
	}
	var runs []run
	var files []string
	for _, path := range inputs {
		if strings.HasSuffix(path, "_server_cpu.csv") {
			continue
		}
		fileRuns, err := load(path)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		files = append(files, filepath.Base(path))
		for _, r := range fileRuns {
			// Profiled runs carry pprof overhead; they are for flame graphs, not numbers.
			if !strings.HasPrefix(r.label, "prof-") {
				runs = append(runs, r)
			}
		}
	}
	if len(runs) == 0 {
		return nil, nil, errors.New("no runs found")
	}
	return runs, files, nil
}

func writeSummaryCSVFile(path string, groups []group) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := writeSummaryCSV(f, groups); err != nil {
		f.Close()
		return err
	}
	return f.Close()
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
