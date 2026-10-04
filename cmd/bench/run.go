package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/loadgen"
	"github.com/Haleralex/pg-mongo-bench/internal/profile"
)

type runConfig struct {
	profiles    []string
	workload    loadgen.Config
	kafkaLinger time.Duration
	repetitions int
	skipBuild   bool
	keepRunning bool
	resultDir   string
	composeFile string
}

func parseRunFlags(args []string) (runConfig, error) {
	cfg := runConfig{workload: loadgen.Defaults()}
	var profiles string
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.StringVar(&profiles, "profiles", strings.Join(profile.Names(), ","), "comma-separated profiles: "+strings.Join(profile.Names(), ", "))
	cfg.workload.RegisterFlags(fs)
	fs.DurationVar(&cfg.kafkaLinger, "kafka-linger", 0, "Kafka producer linger; 0 sends each batch at once like PostgreSQL")
	fs.IntVar(&cfg.repetitions, "repetitions", 1, "full passes over the profiles; even passes run in reverse order")
	fs.BoolVar(&cfg.skipBuild, "skip-build", false, "reuse the existing loadgen image")
	fs.BoolVar(&cfg.keepRunning, "keep-running", false, "leave the last profile's broker running")
	fs.StringVar(&cfg.resultDir, "results", "results", "directory for measurements")
	fs.StringVar(&cfg.composeFile, "compose", "compose.yaml", "Compose file")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	cfg.profiles = strings.Split(profiles, ",")
	for _, p := range cfg.profiles {
		if _, ok := profile.Lookup(p); !ok {
			return cfg, fmt.Errorf("unknown profile %q; known: %s", p, strings.Join(profile.Names(), ", "))
		}
	}
	if err := cfg.workload.Validate(); err != nil {
		return cfg, err
	}
	switch {
	case cfg.kafkaLinger < 0 || cfg.kafkaLinger > time.Second:
		return cfg, fmt.Errorf("-kafka-linger must be 0..1s")
	case cfg.repetitions < 1 || cfg.repetitions > 10:
		return cfg, fmt.Errorf("-repetitions must be 1..10")
	}
	return cfg, nil
}

type runRecord struct {
	ID         string `json:"id"`
	Profile    string `json:"profile"`
	Repetition int    `json:"repetition"`
}

type runner struct {
	cfg     runConfig
	compose compose
	stamp   string
	records []runRecord
}

func runCommand(ctx context.Context, args []string) error {
	cfg, err := parseRunFlags(args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	r := &runner{
		cfg: cfg,
		// The loadgen container writes its report into the same directory.
		compose: compose{file: cfg.composeFile, env: []string{"RESULTS_DIR=" + cfg.resultDir}},
		stamp:   time.Now().UTC().Format("20060102-150405"),
	}
	defer r.stopServices()
	return r.run(ctx)
}

func (r *runner) run(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.resultDir, 0o755); err != nil {
		return err
	}
	info, err := dockerOutput(ctx, "info", "--format", "{{json .}}")
	if err != nil {
		return fmt.Errorf("Docker must be running with Linux containers: %w", err)
	}
	if err := r.save(r.stamp+"-docker-info.json", json.RawMessage(info)); err != nil {
		return err
	}
	if err := r.compose.run(ctx, nil, "--profile", "load", "config", "--quiet"); err != nil {
		return err
	}
	if !r.cfg.skipBuild {
		if err := r.compose.run(ctx, nil, "--profile", "load", "build", "loadgen"); err != nil {
			return err
		}
	}
	if err := r.compose.run(ctx, nil, "pull", "postgres", "kafka"); err != nil {
		return err
	}
	if err := r.saveImages(ctx); err != nil {
		return err
	}
	for repetition := 1; repetition <= r.cfg.repetitions; repetition++ {
		// Reverse alternate passes to expose order and host-cache effects.
		order := slices.Clone(r.cfg.profiles)
		if repetition%2 == 0 {
			slices.Reverse(order)
		}
		for _, p := range order {
			if err := r.runProfile(ctx, repetition, p); err != nil {
				return err
			}
		}
	}
	fmt.Println("Results:", r.cfg.resultDir, "- run `make summary`")
	return nil
}

func (r *runner) saveImages(ctx context.Context) error {
	names, err := r.compose.output(ctx, "--profile", "load", "config", "--images")
	if err != nil {
		return err
	}
	images, err := dockerOutput(ctx, append([]string{"image", "inspect"}, strings.Fields(string(names))...)...)
	if err != nil {
		return err
	}
	return r.save(r.stamp+"-images.json", json.RawMessage(images))
}

func (r *runner) runProfile(ctx context.Context, repetition int, name string) error {
	p, _ := profile.Lookup(name)
	id := fmt.Sprintf("%s-r%d-%s", r.stamp, repetition, name)
	fmt.Printf("\n== %s: %s\n", id, p.Description)

	// Every profile starts from empty volumes, and only one broker runs at a time
	// so that both get the same host resources.
	if err := r.compose.run(ctx, nil, "--profile", "load", "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	if err := r.compose.run(ctx, nil, "up", "-d", "--wait", p.Service); err != nil {
		return err
	}

	log, err := os.Create(r.path(id + "-loadgen.log"))
	if err != nil {
		return err
	}
	defer log.Close()
	container := "queuebench-loadgen-" + id
	args := []string{"--profile", "load", "run", "--rm", "--no-deps", "-T", "--name", container, "loadgen",
		"-profile", name, "-run-id", id, "-out", "/results/" + id + "-report.json", "-kafka-linger", r.cfg.kafkaLinger.String()}
	cmd := r.compose.command(ctx, nil, append(args, r.cfg.workload.Args()...)...)
	cmd.Stdout = io.MultiWriter(os.Stdout, log)
	cmd.Stderr = io.MultiWriter(os.Stderr, log)

	sampleCtx, stopSampling := context.WithCancel(ctx)
	samples := make(chan []resourceSample, 1)
	go func() { samples <- sampleResources(sampleCtx, 2*time.Second) }()
	runErr := cmd.Run()
	stopSampling()
	resources := <-samples

	if ctx.Err() != nil {
		// Cancellation kills only the compose client; remove the container it started.
		_ = exec.Command("docker", "rm", "-f", container).Run()
		return ctx.Err()
	}
	if err := r.save(id+"-resources.json", resources); err != nil {
		return err
	}
	logs, err := r.compose.output(ctx, "logs", "--no-color", p.Service)
	if err != nil {
		return err
	}
	if err := os.WriteFile(r.path(id+"-broker.log"), logs, 0o644); err != nil {
		return err
	}
	if runErr != nil {
		return fmt.Errorf("%s: loadgen failed (%w); see %s", id, runErr, r.path(id+"-loadgen.log"))
	}
	r.records = append(r.records, runRecord{ID: id, Profile: name, Repetition: repetition})
	return r.save(r.stamp+"-runs.json", r.records)
}

func (r *runner) stopServices() {
	if r.cfg.keepRunning {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := r.compose.run(ctx, nil, "--profile", "load", "down", "-v", "--remove-orphans"); err != nil {
		fmt.Fprintln(os.Stderr, "bench: cleanup:", err)
	}
}

func (r *runner) path(name string) string { return filepath.Join(r.cfg.resultDir, name) }

func (r *runner) save(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return os.WriteFile(r.path(name), append(data, '\n'), 0o644)
}
