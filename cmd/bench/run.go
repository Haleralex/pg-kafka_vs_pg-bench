package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
	"github.com/Haleralex/pg-mongo-bench/internal/store/mongodb"
	"github.com/Haleralex/pg-mongo-bench/internal/store/postgres"
)

type runConfig struct {
	profiles     []string
	seedCount    int
	rates        string
	warmup       string
	step         string
	transition   string
	writePercent int
	batchSize    int
	port         int
	repetitions  int
	skipBuild    bool
	keepRunning  bool
	resultDir    string
	composeFile  string
}

var (
	ratesPattern    = regexp.MustCompile(`^[1-9]\d*(,[1-9]\d*)*$`)
	durationPattern = regexp.MustCompile(`^(\d+(\.\d+)?(ms|s|m|h))+$`)
)

func knownProfiles() []string { return append(postgres.Names(), mongodb.Names()...) }

// databaseService maps a profile to the compose service that must be running for it.
func databaseService(profile string) (string, bool) {
	if _, ok := postgres.Lookup(profile); ok {
		return "postgres", true
	}
	if _, ok := mongodb.Lookup(profile); ok {
		return "mongo", true
	}
	return "", false
}

func parseRunFlags(args []string) (runConfig, error) {
	var cfg runConfig
	var profiles string
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.StringVar(&profiles, "profiles", "pg_gin_path_ops,pg_targeted,mongo_targeted", "comma-separated profiles: "+strings.Join(knownProfiles(), ", "))
	fs.IntVar(&cfg.seedCount, "seed", 200000, "events seeded before each profile, 1000..2000000")
	fs.StringVar(&cfg.rates, "rates", "100,300,600", "comma-separated measured request rates per second")
	fs.StringVar(&cfg.warmup, "warmup", "20s", "warmup duration at 100 requests/s")
	fs.StringVar(&cfg.step, "step", "30s", "duration of each measured rate")
	fs.StringVar(&cfg.transition, "transition", "10s", "ramp duration between different rates")
	fs.IntVar(&cfg.writePercent, "write-percent", 70, "share of write requests, 0..100")
	fs.IntVar(&cfg.batchSize, "batch", 10, "events per write request, 1..1000")
	fs.IntVar(&cfg.port, "port", 18088, "host port published for the API")
	fs.IntVar(&cfg.repetitions, "repetitions", 1, "full passes over the profiles; even passes run in reverse order")
	fs.BoolVar(&cfg.skipBuild, "skip-build", false, "reuse the existing API image")
	fs.BoolVar(&cfg.keepRunning, "keep-running", false, "leave the last profile's containers running")
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
		if _, ok := databaseService(p); !ok {
			return cfg, fmt.Errorf("unknown profile %q; known: %s", p, strings.Join(knownProfiles(), ", "))
		}
	}
	switch {
	case cfg.seedCount < 1000 || cfg.seedCount > 2_000_000:
		return cfg, fmt.Errorf("-seed must be 1000..2000000")
	case !ratesPattern.MatchString(cfg.rates):
		return cfg, fmt.Errorf("-rates must look like 100,300,600")
	case !durationPattern.MatchString(cfg.warmup) || !durationPattern.MatchString(cfg.step) || !durationPattern.MatchString(cfg.transition):
		return cfg, fmt.Errorf("durations must look like 30s, 1m30s or 500ms")
	case cfg.writePercent < 0 || cfg.writePercent > 100:
		return cfg, fmt.Errorf("-write-percent must be 0..100")
	case cfg.batchSize < 1 || cfg.batchSize > 1000:
		return cfg, fmt.Errorf("-batch must be 1..1000")
	case cfg.port < 1 || cfg.port > 65535:
		return cfg, fmt.Errorf("-port must be 1..65535")
	case cfg.repetitions < 1 || cfg.repetitions > 10:
		return cfg, fmt.Errorf("-repetitions must be 1..10")
	}
	return cfg, nil
}

type runRecord struct {
	ID          string `json:"id"`
	Profile     string `json:"profile"`
	Repetition  int    `json:"repetition"`
	K6Exit      int    `json:"k6_exit"`
	Correctness string `json:"correctness"`
}

type runner struct {
	cfg      runConfig
	compose  compose
	client   *http.Client
	baseURL  string
	stamp    string
	baseline []byte // query results of the first profile; every later profile must match
	records  []runRecord
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
		cfg:     cfg,
		compose: compose{file: cfg.composeFile, env: []string{"BENCH_PORT=" + strconv.Itoa(cfg.port)}},
		client:  &http.Client{},
		baseURL: "http://127.0.0.1:" + strconv.Itoa(cfg.port),
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
	if err := r.compose.run(ctx, nil, "config", "--quiet"); err != nil {
		return err
	}
	if !r.cfg.skipBuild {
		if err := r.compose.run(ctx, nil, "build", "api"); err != nil {
			return err
		}
	}
	if err := r.compose.run(ctx, nil, "--profile", "load", "pull", "postgres", "mongo", "k6"); err != nil {
		return err
	}
	if err := r.saveImages(ctx); err != nil {
		return err
	}
	for repetition := 1; repetition <= r.cfg.repetitions; repetition++ {
		// Reverse alternate passes to expose order and filesystem-cache effects.
		order := slices.Clone(r.cfg.profiles)
		if repetition%2 == 0 {
			slices.Reverse(order)
		}
		for _, profile := range order {
			if err := r.runProfile(ctx, repetition, profile); err != nil {
				return err
			}
		}
	}
	fmt.Println("Results:", r.cfg.resultDir)
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

func (r *runner) runProfile(ctx context.Context, repetition int, profile string) error {
	id := fmt.Sprintf("%s-r%d-%s", r.stamp, repetition, profile)
	env := []string{"BENCH_PROFILE=" + profile}
	database, _ := databaseService(profile)
	fmt.Printf("\n== %s\n", id)

	// Only one database runs at a time so that both get the same host resources.
	if err := r.compose.run(ctx, env, "stop", "api", "postgres", "mongo"); err != nil {
		return err
	}
	if err := r.compose.run(ctx, env, "up", "-d", "--wait", database); err != nil {
		return err
	}
	if err := r.compose.run(ctx, env, "up", "-d", "--no-deps", "--force-recreate", "api"); err != nil {
		return err
	}
	health, err := r.waitHealthy(ctx)
	if err != nil {
		return err
	}
	if err := r.save(id+"-health.json", health); err != nil {
		return err
	}

	fmt.Printf("Seeding %d events and validating query results...\n", r.cfg.seedCount)
	seed, err := r.request(ctx, http.MethodPost, "/admin/seed", map[string]int{"count": r.cfg.seedCount, "batch_size": 1000}, 20*time.Minute)
	if err != nil {
		return err
	}
	if err := r.save(id+"-seed.json", seed); err != nil {
		return err
	}
	if err := r.saveEndpoint(ctx, id+"-before.json", http.MethodGet, "/stats"); err != nil {
		return err
	}
	if err := r.checkQueries(ctx, id); err != nil {
		return err
	}

	exitCode, err := r.runLoad(ctx, id, env)
	if err != nil {
		return err
	}
	// k6 exits with 99 when thresholds fail; those measurements are still kept.
	if exitCode != 0 && exitCode != 99 {
		return fmt.Errorf("k6 exited with %d; see %s-k6-stderr.log", exitCode, id)
	}

	for _, step := range []struct{ file, method, path string }{
		{id + "-after.json", http.MethodGet, "/stats"},
		{id + "-maintenance.json", http.MethodPost, "/admin/maintain"},
		{id + "-maintained.json", http.MethodGet, "/stats"},
	} {
		if err := r.saveEndpoint(ctx, step.file, step.method, step.path); err != nil {
			return err
		}
	}
	logs, err := r.compose.output(ctx, "logs", "--no-color", "api", database)
	if err != nil {
		return err
	}
	if err := os.WriteFile(r.path(id+"-containers.log"), logs, 0o644); err != nil {
		return err
	}
	r.records = append(r.records, runRecord{ID: id, Profile: profile, Repetition: repetition, K6Exit: exitCode, Correctness: "equal"})
	return r.save(r.stamp+"-runs.json", r.records)
}

func (r *runner) waitHealthy(ctx context.Context) (json.RawMessage, error) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		health, err := r.request(ctx, http.MethodGet, "/health", nil, 3*time.Second)
		if err == nil {
			return health, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("API did not become healthy: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// checkQueries saves plans and fails if this profile returns different events
// than the first profile for the same deterministic queries.
func (r *runner) checkQueries(ctx context.Context, id string) error {
	base := event.BaseTime.UnixMilli()
	signatures := map[string][]int64{}
	plans := map[string]json.RawMessage{}
	for _, kind := range []store.Kind{store.KindTimeline, store.KindAttributes, store.KindTags} {
		for _, tenant := range []int{1, 17, 51} {
			query := url.Values{
				"kind": {string(kind)}, "tenant": {strconv.Itoa(tenant)},
				"from_ms": {strconv.FormatInt(base, 10)}, "to_ms": {strconv.FormatInt(base+int64(r.cfg.seedCount)+1, 10)},
				"service": {"svc-00"}, "level": {"error"}, "tag": {"tag-00"}, "limit": {"50"},
			}.Encode()
			data, err := r.request(ctx, http.MethodGet, "/read?"+query, nil, time.Minute)
			if err != nil {
				return err
			}
			var result struct {
				Events []struct {
					ID int64 `json:"id"`
				} `json:"events"`
			}
			if err := json.Unmarshal(data, &result); err != nil {
				return fmt.Errorf("decode /read: %w", err)
			}
			ids := make([]int64, len(result.Events))
			for i, e := range result.Events {
				ids[i] = e.ID
			}
			signatures[fmt.Sprintf("%s-%d", kind, tenant)] = ids
			if tenant == 1 {
				if plans[string(kind)], err = r.request(ctx, http.MethodGet, "/explain?"+query, nil, time.Minute); err != nil {
					return err
				}
			}
		}
	}
	if err := r.save(id+"-correctness.json", signatures); err != nil {
		return err
	}
	if err := r.save(id+"-plans.json", plans); err != nil {
		return err
	}
	signature, err := json.Marshal(signatures) // map keys are sorted, so this is canonical
	if err != nil {
		return err
	}
	if r.baseline == nil {
		r.baseline = signature
	} else if !bytes.Equal(r.baseline, signature) {
		return fmt.Errorf("%s returned different events than the first profile; compare the *-correctness.json files", id)
	}
	return nil
}

func (r *runner) runLoad(ctx context.Context, id string, env []string) (int, error) {
	stdout, err := os.Create(r.path(id + "-k6.log"))
	if err != nil {
		return 0, err
	}
	defer stdout.Close()
	stderr, err := os.Create(r.path(id + "-k6-stderr.log"))
	if err != nil {
		return 0, err
	}
	defer stderr.Close()

	container := "docbench-k6-" + id
	k6env := map[string]string{
		"RUN_ID": id, "SEED_COUNT": strconv.Itoa(r.cfg.seedCount), "RATE_STEPS": r.cfg.rates,
		"WARMUP_DURATION": r.cfg.warmup, "STEP_DURATION": r.cfg.step, "TRANSITION_DURATION": r.cfg.transition,
		"WRITE_PERCENT": strconv.Itoa(r.cfg.writePercent), "BATCH_SIZE": strconv.Itoa(r.cfg.batchSize),
	}
	// -T: no TTY, so k6 neither waits on stdin nor fills the log with progress bars.
	args := []string{"--profile", "load", "run", "--rm", "--no-deps", "-T", "--name", container}
	for _, key := range slices.Sorted(maps.Keys(k6env)) {
		args = append(args, "-e", key+"="+k6env[key])
	}
	cmd := r.compose.command(ctx, env, append(args, "k6")...)
	cmd.Stdout, cmd.Stderr = stdout, stderr

	sampleCtx, stopSampling := context.WithCancel(ctx)
	samples := make(chan []resourceSample, 1)
	go func() { samples <- sampleResources(sampleCtx, 2*time.Second) }()
	fmt.Println("Running k6...")
	runErr := cmd.Run()
	stopSampling()
	resources := <-samples

	if ctx.Err() != nil {
		// Cancellation kills only the compose client; remove the container it started.
		_ = exec.Command("docker", "rm", "-f", container).Run()
		return 0, ctx.Err()
	}
	if err := r.save(id+"-resources.json", resources); err != nil {
		return 0, err
	}
	printTail(r.path(id+"-k6.log"), 35)
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return 0, runErr
}

func (r *runner) stopServices() {
	if r.cfg.keepRunning {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := r.compose.run(ctx, nil, "stop", "api", "postgres", "mongo"); err != nil {
		fmt.Fprintln(os.Stderr, "bench: cleanup:", err)
	}
}

func (r *runner) request(ctx context.Context, method, path string, body any, timeout time.Duration) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	}
	return data, nil
}

func (r *runner) saveEndpoint(ctx context.Context, name, method, path string) error {
	data, err := r.request(ctx, method, path, nil, 2*time.Minute)
	if err != nil {
		return err
	}
	return r.save(name, data)
}

func (r *runner) path(name string) string { return filepath.Join(r.cfg.resultDir, name) }

func (r *runner) save(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return os.WriteFile(r.path(name), append(data, '\n'), 0o644)
}

func printTail(path string, lines int) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		return
	}
	all := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	fmt.Println(strings.Join(all[max(0, len(all)-lines):], "\n"))
}
