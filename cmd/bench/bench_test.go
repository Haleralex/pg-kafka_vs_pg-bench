package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRunFlags(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	if err != nil || len(cfg.profiles) != 3 || cfg.seedCount != 200000 {
		t.Fatalf("defaults rejected: %+v %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-profiles", "pg_gin_path_ops,mysql"},
		{"-rates", "100,,300"},
		{"-rates", "0"},
		{"-step", "30"},
		{"-seed", "10"},
		{"-write-percent", "101"},
		{"-read-kinds", "timeline,fulltext"},
		{"extra"},
	} {
		if _, err := parseRunFlags(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}

func TestReadKindsMatchK6(t *testing.T) {
	script, err := os.ReadFile("../../k6/load.js")
	if err != nil {
		t.Fatal(err)
	}
	quoted := make([]string, 0)
	for _, kind := range readKinds() {
		quoted = append(quoted, "'"+kind+"'")
	}
	if want := "const ALL_READ_KINDS = [" + strings.Join(quoted, ", ") + "];"; !strings.Contains(string(script), want) {
		t.Fatalf("k6/load.js must declare %s", want)
	}
}

func TestEveryProfileHasADatabase(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range knownProfiles() {
		if seen[p] {
			t.Fatalf("profile %s is registered twice", p)
		}
		seen[p] = true
		if _, ok := databaseService(p); !ok {
			t.Fatalf("profile %s has no compose service", p)
		}
	}
}

const report = `{
  "config": {"run_id": "20261004-094116-r%d-%s", "phases": [
    {"name": "warmup", "kind": "warmup", "target": 100, "duration_ms": 2000},
    {"name": "step_1_10", "kind": "measurement", "target": 10, "duration_ms": 5000}
  ]},
  "metrics": {
    "dropped_iterations": {"values": {"count": 0}},
    "operations{phase:warmup,op:write}": {"values": {"count": 999}},
    "operations{phase:step_1_10,op:write}": {"values": {"count": 35}},
    "http_req_duration{phase:step_1_10,op:write}": {"values": {"p(95)": %g, "p(99)": 3}},
    "write_documents{phase:step_1_10}": {"values": {"count": 350}},
    "operations{phase:step_1_10,op:tags}": {"values": {"count": 5}},
    "empty_read_rate{phase:step_1_10,op:tags}": {"values": {"rate": 0.2}}
  }
}`

func TestSummarizeMeasuredPhasesOnly(t *testing.T) {
	dir := t.TempDir()
	for i, p95 := range []float64{2, 10, 4} {
		name := filepath.Join(dir, fmt.Sprintf("r%d.json", i+1))
		if err := os.WriteFile(name, []byte(fmt.Sprintf(report, i+1, "pg_targeted", p95)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "runs.json"), []byte(`[{"id": "x"}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := loadRows(dir, "")
	if err != nil || len(rows) != 6 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	summaries := summarize(rows)
	if len(summaries) != 2 {
		t.Fatalf("summaries=%+v", summaries)
	}
	write, tags := summaries[0], summaries[1]
	if write.Operation != "write" || write.Profile != "pg_targeted" || write.Runs != 3 || write.HTTPP95 != 4 || write.CompletedRPS != 7 || write.DocumentsPerS != 70 {
		t.Fatalf("write summary: %+v", write)
	}
	if tags.Operation != "tags" || tags.CompletedRPS != 1 {
		t.Fatalf("tags summary: %+v", tags)
	}
	for _, r := range rows {
		if (r.Operation == "tags") != (r.EmptyReadRate == 0.2) {
			t.Fatalf("empty read rate belongs to reads only: %+v", r)
		}
	}
}
