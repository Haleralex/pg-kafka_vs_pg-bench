package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// composeProject must match the name in compose.yaml; resource sampling filters on it.
const composeProject = "gotraining-docbench"

type compose struct {
	file string
	env  []string // BENCH_PORT and other variables substituted into compose.yaml
}

func (c compose) command(ctx context.Context, extraEnv []string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", c.file}, args...)...)
	cmd.Env = append(append(os.Environ(), c.env...), extraEnv...)
	return cmd
}

// run streams output to the terminal.
func (c compose) run(ctx context.Context, extraEnv []string, args ...string) error {
	cmd := c.command(ctx, extraEnv, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (c compose) output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := c.command(ctx, nil, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func dockerOutput(ctx context.Context, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

type resourceSample struct {
	At    time.Time       `json:"at"`
	Stats json.RawMessage `json:"stats"`
}

// sampleResources records docker stats for every project container until ctx ends.
// Each docker stats call itself takes about a second, so the interval is approximate.
func sampleResources(ctx context.Context, interval time.Duration) []resourceSample {
	var samples []resourceSample
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ids, err := dockerOutput(ctx, "ps", "-q", "--filter", "label=com.docker.compose.project="+composeProject)
		if err == nil && len(bytes.TrimSpace(ids)) > 0 {
			args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, strings.Fields(string(ids))...)
			if out, err := dockerOutput(ctx, args...); err == nil {
				at := time.Now().UTC()
				scanner := bufio.NewScanner(bytes.NewReader(out))
				for scanner.Scan() {
					if line := bytes.TrimSpace(scanner.Bytes()); json.Valid(line) {
						samples = append(samples, resourceSample{At: at, Stats: json.RawMessage(bytes.Clone(line))})
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return samples
		case <-ticker.C:
		}
	}
}
