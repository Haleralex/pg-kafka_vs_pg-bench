package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/profile"
)

//go:embed report.html.tmpl
var reportTemplate string

// reportData is everything report.html.tmpl draws; the template embeds it as JSON.
type reportData struct {
	Experiment string             `json:"experiment"`
	Profiles   []profileSummary   `json:"profiles"`
	Steps      []stepSummary      `json:"steps"`
	CPU        map[string]cpuView `json:"cpu"` // per profile: its first run in the experiment
	Pairs      [][2]string        `json:"pairs"`
	Names      []string           `json:"names"`
	Generated  string             `json:"generated"`
}

// cpuView is docker stats CPU per container over seconds since the run started.
type cpuView struct {
	Run    string                  `json:"run"`
	Series map[string][][2]float64 `json:"series"`
}

func reportCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fs.String("results", "results", "directory with loadgen reports")
	prefix := fs.String("prefix", "", "only runs whose ID starts with this; default: the latest experiment")
	all := fs.Bool("all", false, "every report in the directory, from all experiments")
	out := fs.String("out", "report.html", "HTML file name inside the results directory")
	serve := fs.String("serve", "", "also serve the results directory on this address, e.g. :8090")
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
	experiment := *prefix
	if *prefix == "" && !*all {
		reports = latestExperiment(reports)
		experiment = reports[0].stamp
	}
	if *all {
		experiment = "all experiments"
	}
	data, err := buildReport(*dir, experiment, reports)
	if err != nil {
		return err
	}
	path := filepath.Join(*dir, *out)
	if err := writeReport(path, data); err != nil {
		return err
	}
	fmt.Println("Report:", path)
	if *serve == "" {
		return nil
	}
	return serveResults(ctx, *serve, *dir, *out)
}

func buildReport(dir, experiment string, reports []report) (reportData, error) {
	profiles, steps := summarize(reports)
	data := reportData{
		Experiment: experiment, Profiles: profiles, Steps: steps, CPU: map[string]cpuView{},
		Pairs:     [][2]string{{"pg_sync", "kafka_fsync"}, {"pg_async", "kafka"}},
		Names:     profile.Names(),
		Generated: time.Now().Format("2006-01-02 15:04"),
	}
	// One CPU timeline per profile is enough to see who is busy; repetitions look alike.
	sorted := slices.Clone(reports)
	slices.SortFunc(sorted, func(a, b report) int { return a.repetition - b.repetition })
	for _, rep := range sorted {
		if _, done := data.CPU[rep.Profile]; done {
			continue
		}
		view, err := loadCPU(filepath.Join(dir, rep.RunID+"-resources.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return data, err
		}
		view.Run = rep.RunID
		data.CPU[rep.Profile] = view
	}
	return data, nil
}

func loadCPU(path string) (cpuView, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return cpuView{}, err
	}
	var samples []resourceSample
	if err := json.Unmarshal(raw, &samples); err != nil {
		return cpuView{}, fmt.Errorf("%s: %w", path, err)
	}
	view := cpuView{Series: map[string][][2]float64{}}
	if len(samples) == 0 {
		return view, nil
	}
	start := samples[0].At
	for _, s := range samples {
		stat, ok := parseDockerStat(s.Stats)
		if !ok || stat.Container == "prometheus" || stat.Container == "grafana" {
			continue
		}
		view.Series[stat.Container] = append(view.Series[stat.Container], [2]float64{s.At.Sub(start).Seconds(), stat.CPUPercent})
	}
	return view, nil
}

func writeReport(path string, data reportData) error {
	tmpl, err := template.New("report").Parse(reportTemplate)
	if err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		return err
	}
	return file.Close()
}

// serveResults serves the results directory, so the report opens through the
// port forwarding of GitHub Codespaces; Ctrl+C stops it.
func serveResults(ctx context.Context, addr, dir, page string) error {
	server := &http.Server{Addr: addr, Handler: http.FileServer(http.Dir(dir)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Println("Serving", forwardedURL(addr)+"/"+page, "- Ctrl+C to stop")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
