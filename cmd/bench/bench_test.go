package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Haleralex/pg-mongo-bench/internal/loadgen"
	"github.com/Haleralex/pg-mongo-bench/internal/metrics"
	"github.com/Haleralex/pg-mongo-bench/internal/profile"
)

func TestParseRunFlags(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	if err != nil || len(cfg.profiles) != len(profile.All) || cfg.workload.Backlog != loadgen.Defaults().Backlog {
		t.Fatalf("defaults rejected: %+v %v", cfg, err)
	}
	cfg, err = parseRunFlags([]string{"-profiles", "pg_sync,kafka", "-rates", "100,200", "-consumers", "8"})
	if err != nil || len(cfg.profiles) != 2 || cfg.workload.Consumers != 8 || len(cfg.workload.Rates) != 2 {
		t.Fatalf("flags ignored: %+v %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-profiles", "pg_sync,rabbitmq"},
		{"-rates", "100,,300"},
		{"-rates", "0"},
		{"-step", "30"},
		{"-backlog", "10"},
		{"-batch", "0"},
		{"-kafka-linger", "-1ms"},
		{"extra"},
	} {
		if _, err := parseRunFlags(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}

func TestEveryProfileHasAComposeService(t *testing.T) {
	compose, err := os.ReadFile("../../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range profile.All {
		if !strings.Contains(string(compose), "\n  "+p.Service+":\n") {
			t.Fatalf("profile %s needs service %s in compose.yaml", p.Name, p.Service)
		}
	}
}

func writeTestReport(t *testing.T, dir, runID, name string, fill, p99 float64, drained bool, stats map[string]any) {
	t.Helper()
	res := loadgen.Result{
		Fill: loadgen.Throughput{PerSecond: fill}, Drain: loadgen.Throughput{PerSecond: fill * 2},
		Steps: []loadgen.Step{
			{TargetRate: 1000, ConsumedRate: 1000, EndToEnd: loadgen.Latency{P50: 1, P99: p99}, Drained: true},
			{TargetRate: 5000, ConsumedRate: 4000, EndToEnd: loadgen.Latency{P99: p99 * 10}, BacklogAtEnd: 9000, Drained: drained},
		},
		Produced: 1000,
	}
	data, err := json.Marshal(map[string]any{"run_id": runID, "profile": name, "result": res, "backend_stats": stats})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, runID+"-report.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSummarizeTakesMediansPerProfileAndRate(t *testing.T) {
	dir := t.TempDir()
	for i, fill := range []float64{100, 500, 300} {
		run := "20261004-120000-r" + string(rune('1'+i)) + "-pg_sync"
		writeTestReport(t, dir, run, "pg_sync", fill, fill/100, i != 1, map[string]any{"wal_bytes": 400000})
	}
	writeTestReport(t, dir, "20261004-120000-r1-kafka", "kafka", 9000, 0.5, true, map[string]any{"log_bytes": 300000})
	// Other files in results/ are ignored.
	if err := os.WriteFile(filepath.Join(dir, "20261004-120000-runs.json"), []byte(`[]`), 0o644); err != nil {
		t.Fatal(err)
	}
	// An older experiment, e.g. a smoke run, is left out by default.
	writeTestReport(t, dir, "20261003-090000-r1-kafka", "kafka", 1, 1, true, nil)
	reports, err := loadReports(dir, "")
	if err != nil || len(reports) != 5 {
		t.Fatalf("reports=%d err=%v", len(reports), err)
	}
	reports = latestExperiment(reports)
	if len(reports) != 4 {
		t.Fatalf("latest experiment has %d reports", len(reports))
	}
	profiles, steps := summarize(reports)
	if len(profiles) != 2 || profiles[0].Profile != "pg_sync" || profiles[1].Profile != "kafka" {
		t.Fatalf("profiles: %+v", profiles)
	}
	pg := profiles[0]
	if pg.Runs != 3 || pg.FillRate != 300 || pg.DrainRate != 600 || pg.BytesPerMsg != 400 {
		t.Fatalf("pg summary: %+v", pg)
	}
	if len(steps) != 4 || steps[0].Rate != 1000 || steps[0].Profile != "pg_sync" || steps[0].P99 != 3 {
		t.Fatalf("steps: %+v", steps)
	}
	if s := steps[2]; s.Rate != 5000 || s.Saturated != 3 || s.Backlog != 9000 || s.Consumed != 4000 {
		t.Fatalf("saturated step: %+v", s)
	}
	var out bytes.Buffer
	if err := printTables(&out, profiles, steps); err != nil || !strings.Contains(out.String(), "kafka") {
		t.Fatalf("table: %q %v", out.String(), err)
	}
	if err := writeCSV(filepath.Join(dir, "c.csv"), reports); err != nil {
		t.Fatal(err)
	}
	csv, _ := os.ReadFile(filepath.Join(dir, "c.csv"))
	if lines := strings.Count(string(csv), "\n"); lines != 1+4*4 {
		t.Fatalf("csv has %d lines", lines)
	}
}

func TestParseDockerStat(t *testing.T) {
	for line, want := range map[string]metrics.ContainerSample{
		`{"Name":"gotraining-queuebench-postgres-1","CPUPerc":"187.25%","MemUsage":"1.5GiB / 2GiB"}`:          {Container: "postgres", CPUPercent: 187.25, MemoryBytes: 1.5 * (1 << 30)},
		`{"Name":"queuebench-loadgen-20261007-170450-r1-kafka","CPUPerc":"3.00%","MemUsage":"512KiB / 1GiB"}`: {Container: "loadgen", CPUPercent: 3, MemoryBytes: 512 << 10},
		`{"Name":"gotraining-queuebench-kafka_ui-12","CPUPerc":"0%","MemUsage":"10MB / 1GB"}`:                 {Container: "kafka_ui", CPUPercent: 0, MemoryBytes: 10e6},
	} {
		got, ok := parseDockerStat([]byte(line))
		if !ok || got != want {
			t.Errorf("%s: got %+v %v, want %+v", line, got, ok, want)
		}
	}
	if _, ok := parseDockerStat([]byte(`{"Name":"x","CPUPerc":"--"}`)); ok {
		t.Error("accepted a sample without CPU")
	}
}

func TestReportRendersExperimentData(t *testing.T) {
	dir := t.TempDir()
	writeTestReport(t, dir, "20261004-120000-r1-pg_sync", "pg_sync", 300, 3, true, map[string]any{"wal_bytes": 400000})
	writeTestReport(t, dir, "20261004-120000-r1-kafka", "kafka", 9000, 0.5, true, nil)
	resources := `[
	  {"at": "2026-10-04T12:00:00Z", "stats": {"Name":"gotraining-queuebench-postgres-1","CPUPerc":"150%","MemUsage":"1GiB / 2GiB"}},
	  {"at": "2026-10-04T12:00:02Z", "stats": {"Name":"gotraining-queuebench-postgres-1","CPUPerc":"190%","MemUsage":"1GiB / 2GiB"}},
	  {"at": "2026-10-04T12:00:02Z", "stats": {"Name":"gotraining-queuebench-grafana-1","CPUPerc":"1%","MemUsage":"1GiB / 2GiB"}}
	]`
	if err := os.WriteFile(filepath.Join(dir, "20261004-120000-r1-pg_sync-resources.json"), []byte(resources), 0o644); err != nil {
		t.Fatal(err)
	}
	reports, err := loadReports(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	data, err := buildReport(dir, "20261004-120000", reports)
	if err != nil {
		t.Fatal(err)
	}
	cpu := data.CPU["pg_sync"].Series
	if len(cpu) != 1 || len(cpu["postgres"]) != 2 || cpu["postgres"][1] != [2]float64{2, 190} {
		t.Fatalf("cpu series: %+v", data.CPU)
	}
	if _, ok := data.CPU["kafka"]; ok {
		t.Fatal("kafka has no resources file")
	}
	path := filepath.Join(dir, "report.html")
	if err := writeReport(path, data); err != nil {
		t.Fatal(err)
	}
	html, _ := os.ReadFile(path)
	for _, want := range []string{`"experiment":"20261004-120000"`, `"FillRate":300`, `"postgres":[[0,150],[2,190]]`} {
		if !strings.Contains(string(html), want) {
			t.Errorf("report lacks %s", want)
		}
	}
}
