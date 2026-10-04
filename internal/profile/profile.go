// Package profile lists the compared queue configurations. Durability is paired:
// pg_sync and kafka_fsync flush to disk before acknowledging, pg_async and kafka
// acknowledge from memory.
package profile

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/queue"
	"github.com/Haleralex/pg-mongo-bench/internal/queue/kafka"
	"github.com/Haleralex/pg-mongo-bench/internal/queue/postgres"
)

type Profile struct {
	Name        string
	Service     string // compose service the profile needs
	Description string
}

var All = []Profile{
	{"pg_sync", "postgres", "SKIP LOCKED table queue, synchronous_commit=on: commit waits for the WAL fsync"},
	{"pg_async", "postgres", "SKIP LOCKED table queue, synchronous_commit=off: commit returns before the WAL fsync"},
	{"kafka", "kafka", "one broker, acks=all, log left to the OS page cache (Kafka default)"},
	{"kafka_fsync", "kafka", "one broker, acks=all, flush.messages=1: fsync before every acknowledgement"},
}

func Lookup(name string) (Profile, bool) {
	i := slices.IndexFunc(All, func(p Profile) bool { return p.Name == name })
	if i < 0 {
		return Profile{}, false
	}
	return All[i], true
}

func Names() []string {
	names := make([]string, len(All))
	for i, p := range All {
		names[i] = p.Name
	}
	return names
}

// Endpoints locates the brokers inside the compose network.
type Endpoints struct {
	PostgresURL  string
	KafkaBrokers []string
	PGConns      int32
	KafkaLinger  time.Duration
}

func Open(ctx context.Context, name string, e Endpoints) (queue.Backend, error) {
	switch name {
	case "pg_sync", "pg_async":
		commit := map[string]string{"pg_sync": "on", "pg_async": "off"}[name]
		return postgres.New(ctx, postgres.Config{URL: e.PostgresURL, SynchronousCommit: commit, MaxConns: e.PGConns})
	case "kafka", "kafka_fsync":
		return kafka.New(ctx, kafka.Config{Brokers: e.KafkaBrokers, Fsync: name == "kafka_fsync", Linger: e.KafkaLinger})
	}
	return nil, fmt.Errorf("unknown profile %q", name)
}
