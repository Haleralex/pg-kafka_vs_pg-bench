// Package event generates the deterministic documents written by every profile.
package event

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// BaseTime is the timestamp of event ID 0; event N occurs N milliseconds later.
var BaseTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// MaxID keeps BaseTime + ID milliseconds representable as a time.Duration.
const MaxID = int64(math.MaxInt64 / int64(time.Millisecond))

type Event struct {
	ID         int64     `json:"id" bson:"_id"`
	TenantID   int       `json:"tenant_id" bson:"tenant_id"`
	OccurredAt time.Time `json:"occurred_at" bson:"occurred_at"`
	Payload    Payload   `json:"payload" bson:"payload"`
}

type Payload struct {
	Service string   `json:"service" bson:"service"`
	Level   string   `json:"level" bson:"level"`
	Tags    []string `json:"tags" bson:"tags"`
	Attrs   Attrs    `json:"attrs" bson:"attrs"`
	Message string   `json:"message" bson:"message"`
}

type Attrs struct {
	Region    string `json:"region" bson:"region"`
	Status    int    `json:"status" bson:"status"`
	LatencyMS int    `json:"latency_ms" bson:"latency_ms"`
	TraceID   string `json:"trace_id" bson:"trace_id"`
}

// Separate draws avoid the tenant/service correlations introduced by id % N.
func nextRandom(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	x := *state
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

var (
	regions  = [...]string{"eu-west", "us-east", "ap-south", "ap-east"}
	statuses = [...]int{200, 201, 202, 204, 400, 404, 429, 500, 503}
	words    = [...]string{"request", "worker", "accepted", "queue", "payment", "retry", "upstream", "cache", "completed", "session", "batch", "timeout", "connection", "processed", "customer", "resource", "operation", "response", "delivery", "record", "shard", "partition", "duration", "result"}
)

// Generate returns the same event for the same ID on every call and in every process.
func Generate(id int64) Event {
	state := uint64(id) ^ 0x4f1bbcdc676d512f
	tenant := int(nextRandom(&state)%100) + 1
	service := fmt.Sprintf("svc-%02d", nextRandom(&state)%20)
	level := "info"
	if roll := nextRandom(&state) % 100; roll < 10 {
		level = "error"
	} else if roll < 30 {
		level = "warn"
	}
	tag1 := nextRandom(&state) % 20
	tag2 := nextRandom(&state) % 19
	if tag2 >= tag1 {
		tag2++
	}
	region := regions[nextRandom(&state)%uint64(len(regions))]
	status := statuses[nextRandom(&state)%uint64(len(statuses))]
	latency := int(nextRandom(&state) % 2000)
	trace := fmt.Sprintf("%016x%016x", nextRandom(&state), nextRandom(&state))
	var message strings.Builder
	message.Grow(730)
	fmt.Fprintf(&message, "event=%d trace=%s ", id, trace)
	for message.Len() < 700 {
		message.WriteString(words[nextRandom(&state)%uint64(len(words))])
		message.WriteByte(' ')
	}
	return Event{ID: id, TenantID: tenant, OccurredAt: BaseTime.Add(time.Duration(id) * time.Millisecond), Payload: Payload{
		Service: service, Level: level, Tags: []string{fmt.Sprintf("tag-%02d", tag1), fmt.Sprintf("tag-%02d", tag2)},
		Attrs: Attrs{Region: region, Status: status, LatencyMS: latency, TraceID: trace}, Message: strings.TrimSpace(message.String()),
	}}
}

// Batch generates count consecutive events starting at start.
func Batch(start int64, count int) []Event {
	events := make([]Event, count)
	for i := range events {
		events[i] = Generate(start + int64(i))
	}
	return events
}

// ValidRange reports whether IDs start..start+count-1 are all within 1..MaxID.
func ValidRange(start int64, count int) bool {
	return start > 0 && count > 0 && start <= MaxID-int64(count)+1
}
