package main

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"text/tabwriter"
)

var operations = append([]string{"write"}, readKinds()...)

// runIDPattern splits IDs created by runProfile: <stamp>-r<repetition>-<profile>.
var runIDPattern = regexp.MustCompile(`^.+-r(\d+)-(.+)$`)

// k6Report is the subset of the handleSummary output in k6/load.js used here.
type k6Report struct {
	Config *struct {
		RunID  string `json:"run_id"`
		Phases []struct {
			Name       string  `json:"name"`
			Kind       string  `json:"kind"`
			Target     int     `json:"target"`
			DurationMS float64 `json:"duration_ms"`
		} `json:"phases"`
	} `json:"config"`
	Metrics map[string]struct {
		Values map[string]float64 `json:"values"`
	} `json:"metrics"`
}

func (r k6Report) metric(name, key string) (float64, bool) {
	value, ok := r.Metrics[name].Values[key]
	return value, ok
}

// row is one operation in one measured phase of one run.
type row struct {
	Run, Profile, Operation   string
	Repetition, TargetRPS     int
	Requests, CompletedRPS    float64
	HTTPP95, HTTPP99, DBP95   float64
	ErrorRate, EmptyReadRate  float64
	DocumentsPerS, DroppedRun float64
}

func loadRows(dir, prefix string) ([]row, error) {
	files, err := filepath.Glob(filepath.Join(dir, prefix+"*.json"))
	if err != nil {
		return nil, err
	}
	var rows []row
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var report k6Report
		// Other result files (stats, plans, run lists) are not k6 summaries.
		if json.Unmarshal(data, &report) != nil || report.Config == nil || len(report.Config.Phases) == 0 {
			continue
		}
		match := runIDPattern.FindStringSubmatch(report.Config.RunID)
		if match == nil {
			return nil, fmt.Errorf("%s: unexpected run_id %q", file, report.Config.RunID)
		}
		repetition, _ := strconv.Atoi(match[1])
		dropped, _ := report.metric("dropped_iterations", "count")
		for _, phase := range report.Config.Phases {
			if phase.Kind != "measurement" {
				continue
			}
			seconds := phase.DurationMS / 1000
			for _, op := range operations {
				filter := fmt.Sprintf("{phase:%s,op:%s}", phase.Name, op)
				count, ok := report.metric("operations"+filter, "count")
				if !ok {
					continue
				}
				r := row{Run: report.Config.RunID, Profile: match[2], Operation: op, Repetition: repetition, TargetRPS: phase.Target, Requests: count, CompletedRPS: count / seconds, DroppedRun: dropped}
				r.HTTPP95, _ = report.metric("http_req_duration"+filter, "p(95)")
				r.HTTPP99, _ = report.metric("http_req_duration"+filter, "p(99)")
				r.DBP95, _ = report.metric("db_ms"+filter, "p(95)")
				r.ErrorRate, _ = report.metric("request_errors"+filter, "rate")
				if op == "write" {
					// k6/load.js counts documents per phase only, not per operation.
					documents, _ := report.metric(fmt.Sprintf("write_documents{phase:%s}", phase.Name), "count")
					r.DocumentsPerS = documents / seconds
				} else {
					r.EmptyReadRate, _ = report.metric("empty_read_rate"+filter, "rate")
				}
				rows = append(rows, r)
			}
		}
	}
	return rows, nil
}

// summary aggregates one profile/rate/operation over all repetitions.
type summary struct {
	Profile, Operation                      string
	TargetRPS, Runs                         int
	CompletedRPS, HTTPP95, HTTPP99, DBP95   float64 // medians
	MaxErrorRate, DocumentsPerS, DroppedMax float64
}

func summarize(rows []row) []summary {
	type key struct {
		profile, op string
		rate        int
	}
	groups := map[key][]row{}
	for _, r := range rows {
		k := key{r.Profile, r.Operation, r.TargetRPS}
		groups[k] = append(groups[k], r)
	}
	var out []summary
	for k, group := range groups {
		s := summary{Profile: k.profile, Operation: k.op, TargetRPS: k.rate, Runs: len(group)}
		s.CompletedRPS = median(group, func(r row) float64 { return r.CompletedRPS })
		s.HTTPP95 = median(group, func(r row) float64 { return r.HTTPP95 })
		s.HTTPP99 = median(group, func(r row) float64 { return r.HTTPP99 })
		s.DBP95 = median(group, func(r row) float64 { return r.DBP95 })
		s.DocumentsPerS = median(group, func(r row) float64 { return r.DocumentsPerS })
		for _, r := range group {
			s.MaxErrorRate = max(s.MaxErrorRate, r.ErrorRate)
			s.DroppedMax = max(s.DroppedMax, r.DroppedRun)
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b summary) int {
		return cmp.Or(cmp.Compare(a.TargetRPS, b.TargetRPS), cmp.Compare(slices.Index(operations, a.Operation), slices.Index(operations, b.Operation)), cmp.Compare(a.Profile, b.Profile))
	})
	return out
}

func median(rows []row, value func(row) float64) float64 {
	values := make([]float64, len(rows))
	for i, r := range rows {
		values[i] = value(r)
	}
	slices.Sort(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func summarizeCommand(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ContinueOnError)
	dir := fs.String("results", "results", "directory with k6 summaries")
	prefix := fs.String("prefix", "", "only runs whose ID starts with this, e.g. a run timestamp")
	out := fs.String("out", "comparison.csv", "CSV file name inside the results directory")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	rows, err := loadRows(*dir, *prefix)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("no k6 summaries in %s matching %q", *dir, *prefix+"*")
	}
	csvPath := filepath.Join(*dir, *out)
	if err := writeCSV(csvPath, rows); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "rate\top\tprofile\truns\treq/s\thttp p95\thttp p99\tdb p95\tdocs/s\tmax err\tdropped\t")
	for _, s := range summarize(rows) {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%.1f\t%.2f\t%.2f\t%.2f\t%.0f\t%.2f%%\t%.0f\t\n", s.TargetRPS, s.Operation, s.Profile, s.Runs, s.CompletedRPS, s.HTTPP95, s.HTTPP99, s.DBP95, s.DocumentsPerS, s.MaxErrorRate*100, s.DroppedMax)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Println("Medians over repetitions; latencies in ms. Per-run rows:", csvPath)
	return nil
}

func writeCSV(path string, rows []row) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	_ = w.Write([]string{"run", "profile", "repetition", "target_rps", "operation", "requests", "completed_rps", "http_p95_ms", "http_p99_ms", "db_p95_ms", "error_rate", "empty_read_rate", "documents_per_s", "dropped_in_run"})
	for _, r := range rows {
		_ = w.Write([]string{r.Run, r.Profile, strconv.Itoa(r.Repetition), strconv.Itoa(r.TargetRPS), r.Operation, f(r.Requests), f(r.CompletedRPS), f(r.HTTPP95), f(r.HTTPP99), f(r.DBP95), f(r.ErrorRate), f(r.EmptyReadRate), f(r.DocumentsPerS), f(r.DroppedRun)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return file.Close()
}
