// Command loadgen runs the experiment against one profile from inside the
// compose network and writes the result as JSON. cmd/bench starts it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/loadgen"
	"github.com/Haleralex/pg-mongo-bench/internal/metrics"
	"github.com/Haleralex/pg-mongo-bench/internal/profile"
	"github.com/Haleralex/pg-mongo-bench/internal/queue"
)

// Report is the file cmd/bench summarizes.
type Report struct {
	RunID        string         `json:"run_id"`
	Profile      string         `json:"profile"`
	Description  string         `json:"description"`
	StartedAt    time.Time      `json:"started_at"`
	Config       loadgen.Config `json:"config"`
	KafkaLinger  string         `json:"kafka_linger"`
	Result       loadgen.Result `json:"result"`
	BackendStats map[string]any `json:"backend_stats,omitempty"`
	Error        string         `json:"error,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg := loadgen.Defaults()
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	name := fs.String("profile", "pg_sync", "one of "+strings.Join(profile.Names(), ", "))
	runID := fs.String("run-id", "manual", "identifier stored in the report")
	out := fs.String("out", "", "report path; stdout when empty")
	linger := fs.Duration("kafka-linger", 0, "producer linger; franz-go defaults to 10ms, 0 sends each batch at once like PostgreSQL")
	metricsAddr := fs.String("metrics-addr", ":9100", "Prometheus endpoint; empty disables it")
	cfg.RegisterFlags(fs)
	_ = fs.Parse(os.Args[1:])
	if err := cfg.Validate(); err != nil {
		return err
	}
	p, ok := profile.Lookup(*name)
	if !ok {
		return fmt.Errorf("unknown profile %q", *name)
	}

	backend, err := profile.Open(ctx, p.Name, profile.Endpoints{
		PostgresURL:  os.Getenv("PG_URL"),
		KafkaBrokers: strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		// One connection per goroutine plus room for the statistics poller.
		PGConns:     int32(cfg.Producers + cfg.Consumers + 2),
		KafkaLinger: *linger,
	})
	if err != nil {
		return err
	}
	defer backend.Close()

	report := Report{RunID: *runID, Profile: p.Name, Description: p.Description, StartedAt: time.Now().UTC(), Config: cfg, KafkaLinger: linger.String()}
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "%s %s: %s\n", time.Now().Format("15:04:05"), p.Name, fmt.Sprintf(format, args...))
	}
	var observer loadgen.Observer
	if *metricsAddr != "" {
		m := metrics.NewLoadgen(p.Name, *runID)
		metricsCtx, stopMetrics := context.WithCancel(context.WithoutCancel(ctx))
		defer func() {
			// Let Prometheus scrape the final values before the process exits.
			time.Sleep(3 * time.Second)
			stopMetrics()
		}()
		if err := metrics.Serve(metricsCtx, *metricsAddr, m.Handler()); err != nil {
			return err
		}
		go pollBrokerStats(metricsCtx, backend, m)
		observer = m
	}
	report.Result, err = loadgen.Run(ctx, backend, cfg, observer, logf)
	if err != nil {
		report.Error = err.Error()
	}
	statsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if stats, statsErr := backend.Stats(statsCtx); statsErr == nil {
		report.BackendStats = stats
	} else {
		logf("%v", statsErr)
	}
	if writeErr := write(*out, report); writeErr != nil {
		return writeErr
	}
	return err
}

// pollBrokerStats refreshes broker gauges every two seconds. Errors are
// expected before Run has created the table or topic and are ignored.
func pollBrokerStats(ctx context.Context, backend queue.Backend, m *metrics.Loadgen) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		statsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if stats, err := backend.Stats(statsCtx); err == nil {
			m.SetBrokerStats(stats)
		}
		cancel()
	}
}

func write(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
