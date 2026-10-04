package main

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"text/tabwriter"

	"github.com/Haleralex/pg-mongo-bench/internal/loadgen"
	"github.com/Haleralex/pg-mongo-bench/internal/profile"
)

// runIDPattern splits IDs created by runProfile: <stamp>-r<repetition>-<profile>.
var runIDPattern = regexp.MustCompile(`^.+-r(\d+)-(.+)$`)

// report is the subset of cmd/loadgen's Report used here.
type report struct {
	RunID        string         `json:"run_id"`
	Profile      string         `json:"profile"`
	Result       loadgen.Result `json:"result"`
	BackendStats map[string]any `json:"backend_stats"`
	Error        string         `json:"error"`
	repetition   int
}

func loadReports(dir, prefix string) ([]report, error) {
	files, err := filepath.Glob(filepath.Join(dir, prefix+"*-report.json"))
	if err != nil {
		return nil, err
	}
	var reports []report
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var rep report
		if err := json.Unmarshal(data, &rep); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		match := runIDPattern.FindStringSubmatch(rep.RunID)
		if match == nil || match[2] != rep.Profile {
			return nil, fmt.Errorf("%s: unexpected run_id %q for profile %q", file, rep.RunID, rep.Profile)
		}
		rep.repetition, _ = strconv.Atoi(match[1])
		reports = append(reports, rep)
	}
	return reports, nil
}

// storedBytesPerMessage is what the broker wrote per produced message: WAL for
// PostgreSQL, the log size for Kafka.
func storedBytesPerMessage(rep report) float64 {
	if rep.Result.Produced == 0 {
		return 0
	}
	for _, key := range []string{"wal_bytes", "log_bytes"} {
		if v, ok := rep.BackendStats[key].(float64); ok {
			return v / float64(rep.Result.Produced)
		}
	}
	return 0
}

// profileSummary holds medians of the closed-loop phases over repetitions.
type profileSummary struct {
	Profile             string
	Runs, Failed        int
	FillRate, DrainRate float64
	SendP99, ReceiveP99 float64
	BytesPerMsg         float64
	Duplicates          int64
}

// stepSummary holds medians of one steady rate over repetitions.
type stepSummary struct {
	Profile                  string
	Rate, Runs, Saturated    int
	Consumed, P50, P99, P999 float64
	Backlog                  float64
}

func summarize(reports []report) ([]profileSummary, []stepSummary) {
	byProfile := map[string][]report{}
	for _, rep := range reports {
		byProfile[rep.Profile] = append(byProfile[rep.Profile], rep)
	}
	var profiles []profileSummary
	var steps []stepSummary
	for name, group := range byProfile {
		ok := slices.DeleteFunc(slices.Clone(group), func(r report) bool { return r.Error != "" })
		s := profileSummary{Profile: name, Runs: len(ok), Failed: len(group) - len(ok)}
		s.FillRate = median(ok, func(r report) float64 { return r.Result.Fill.PerSecond })
		s.DrainRate = median(ok, func(r report) float64 { return r.Result.Drain.PerSecond })
		s.SendP99 = median(ok, func(r report) float64 { return r.Result.Fill.CallLatency.P99 })
		s.ReceiveP99 = median(ok, func(r report) float64 { return r.Result.Drain.CallLatency.P99 })
		s.BytesPerMsg = median(ok, storedBytesPerMessage)
		for _, r := range group {
			s.Duplicates += r.Result.Duplicates
		}
		profiles = append(profiles, s)

		rates := map[int][]loadgen.Step{}
		for _, r := range ok {
			for _, step := range r.Result.Steps {
				rates[step.TargetRate] = append(rates[step.TargetRate], step)
			}
		}
		for rate, group := range rates {
			ss := stepSummary{Profile: name, Rate: rate, Runs: len(group)}
			ss.Consumed = median(group, func(s loadgen.Step) float64 { return s.ConsumedRate })
			ss.P50 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P50 })
			ss.P99 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P99 })
			ss.P999 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P999 })
			ss.Backlog = median(group, func(s loadgen.Step) float64 { return float64(s.BacklogAtEnd) })
			for _, s := range group {
				if !s.Drained {
					ss.Saturated++
				}
			}
			steps = append(steps, ss)
		}
	}
	order := func(name string) int { return slices.Index(profile.Names(), name) }
	slices.SortFunc(profiles, func(a, b profileSummary) int { return cmp.Compare(order(a.Profile), order(b.Profile)) })
	slices.SortFunc(steps, func(a, b stepSummary) int {
		return cmp.Or(cmp.Compare(a.Rate, b.Rate), cmp.Compare(order(a.Profile), order(b.Profile)))
	})
	return profiles, steps
}

func median[T any](items []T, value func(T) float64) float64 {
	if len(items) == 0 {
		return 0
	}
	values := make([]float64, len(items))
	for i, item := range items {
		values[i] = value(item)
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
	dir := fs.String("results", "results", "directory with loadgen reports")
	prefix := fs.String("prefix", "", "only runs whose ID starts with this, e.g. a run timestamp")
	out := fs.String("out", "comparison.csv", "CSV file name inside the results directory")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	reports, err := loadReports(*dir, *prefix)
	if err != nil {
		return err
	}
	if len(reports) == 0 {
		return fmt.Errorf("no reports in %s matching %q", *dir, *prefix+"*-report.json")
	}
	csvPath := filepath.Join(*dir, *out)
	if err := writeCSV(csvPath, reports); err != nil {
		return err
	}
	profiles, steps := summarize(reports)
	if err := printTables(os.Stdout, profiles, steps); err != nil {
		return err
	}
	fmt.Println("Medians over repetitions; latencies in ms. Per-run rows:", csvPath)
	return nil
}

func printTables(w io.Writer, profiles []profileSummary, steps []stepSummary) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "profile\truns\tfailed\tfill msg/s\tsend p99\tdrain msg/s\treceive p99\tdisk B/msg\tduplicates\t")
	for _, s := range profiles {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.0f\t%.2f\t%.0f\t%.2f\t%.0f\t%d\t\n", s.Profile, s.Runs, s.Failed, s.FillRate, s.SendP99, s.DrainRate, s.ReceiveP99, s.BytesPerMsg, s.Duplicates)
	}
	fmt.Fprintln(tw, "\t\t\t\t\t\t\t\t\t")
	fmt.Fprintln(tw, "rate\tprofile\truns\tconsumed/s\te2e p50\te2e p99\te2e p99.9\tbacklog\tsaturated\t")
	for _, s := range steps {
		fmt.Fprintf(tw, "%d\t%s\t%d\t%.0f\t%.2f\t%.2f\t%.2f\t%.0f\t%d\t\n", s.Rate, s.Profile, s.Runs, s.Consumed, s.P50, s.P99, s.P999, s.Backlog, s.Saturated)
	}
	return tw.Flush()
}

func writeCSV(path string, reports []report) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	_ = w.Write([]string{"run", "profile", "repetition", "phase", "target_rate", "msgs_per_s", "p50_ms", "p99_ms", "p999_ms", "max_ms", "backlog_at_end", "completed", "error"})
	for _, rep := range reports {
		base := []string{rep.RunID, rep.Profile, strconv.Itoa(rep.repetition)}
		phase := func(name string, rate int, perSecond float64, l loadgen.Latency, backlog int64, completed bool) {
			_ = w.Write(append(slices.Clone(base), name, strconv.Itoa(rate), f(perSecond), f(l.P50), f(l.P99), f(l.P999), f(l.Max), strconv.FormatInt(backlog, 10), strconv.FormatBool(completed), rep.Error))
		}
		phase("fill", 0, rep.Result.Fill.PerSecond, rep.Result.Fill.CallLatency, 0, rep.Result.Fill.Completed)
		phase("drain", 0, rep.Result.Drain.PerSecond, rep.Result.Drain.CallLatency, 0, rep.Result.Drain.Completed)
		for _, s := range rep.Result.Steps {
			phase("steady", s.TargetRate, s.ConsumedRate, s.EndToEnd, s.BacklogAtEnd, s.Drained)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return file.Close()
}
