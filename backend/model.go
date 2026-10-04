package main

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var baseTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

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

func generateEvent(id int64) Event {
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
	regions := [...]string{"eu-west", "us-east", "ap-south", "ap-east"}
	region := regions[nextRandom(&state)%4]
	statuses := [...]int{200, 201, 202, 204, 400, 404, 429, 500, 503}
	status := statuses[nextRandom(&state)%uint64(len(statuses))]
	latency := int(nextRandom(&state) % 2000)
	trace := fmt.Sprintf("%016x%016x", nextRandom(&state), nextRandom(&state))
	words := [...]string{"request", "worker", "accepted", "queue", "payment", "retry", "upstream", "cache", "completed", "session", "batch", "timeout", "connection", "processed", "customer", "resource", "operation", "response", "delivery", "record", "shard", "partition", "duration", "result"}
	var message strings.Builder
	message.Grow(730)
	fmt.Fprintf(&message, "event=%d trace=%s ", id, trace)
	for message.Len() < 700 {
		message.WriteString(words[nextRandom(&state)%uint64(len(words))])
		message.WriteByte(' ')
	}
	return Event{ID: id, TenantID: tenant, OccurredAt: baseTime.Add(time.Duration(id) * time.Millisecond), Payload: Payload{
		Service: service, Level: level, Tags: []string{fmt.Sprintf("tag-%02d", tag1), fmt.Sprintf("tag-%02d", tag2)},
		Attrs: Attrs{Region: region, Status: status, LatencyMS: latency, TraceID: trace}, Message: strings.TrimSpace(message.String()),
	}}
}

func generateBatch(start int64, count int) []Event {
	events := make([]Event, count)
	for i := range events {
		events[i] = generateEvent(start + int64(i))
	}
	return events
}

const maxEventID = int64(math.MaxInt64 / int64(time.Millisecond))

func validIDRange(start int64, count int) bool {
	return start > 0 && count > 0 && start <= maxEventID-int64(count)+1
}

type ReadQuery struct {
	Kind    string
	Tenant  int
	From    time.Time
	To      time.Time
	Service string
	Level   string
	Tag     string
	Limit   int
}

func parseReadQuery(v url.Values) (ReadQuery, error) {
	q := ReadQuery{Kind: v.Get("kind"), Service: v.Get("service"), Level: v.Get("level"), Tag: v.Get("tag"), Limit: 50}
	if q.Kind == "" {
		q.Kind = "timeline"
	}
	if q.Kind != "timeline" && q.Kind != "attributes" && q.Kind != "tags" {
		return q, fmt.Errorf("kind must be timeline, attributes, or tags")
	}
	var err error
	q.Tenant, err = strconv.Atoi(v.Get("tenant"))
	if err != nil || q.Tenant < 1 || q.Tenant > 100 {
		return q, fmt.Errorf("tenant must be between 1 and 100")
	}
	from, err := strconv.ParseInt(v.Get("from_ms"), 10, 64)
	if err != nil {
		return q, fmt.Errorf("from_ms must be a Unix millisecond timestamp")
	}
	to, err := strconv.ParseInt(v.Get("to_ms"), 10, 64)
	if err != nil || to <= from {
		return q, fmt.Errorf("to_ms must be a Unix millisecond timestamp greater than from_ms")
	}
	q.From, q.To = time.UnixMilli(from).UTC(), time.UnixMilli(to).UTC()
	if value := v.Get("limit"); value != "" {
		q.Limit, err = strconv.Atoi(value)
		if err != nil || q.Limit < 1 || q.Limit > 1000 {
			return q, fmt.Errorf("limit must be between 1 and 1000")
		}
	}
	if q.Kind == "attributes" && (q.Service == "" || q.Level == "") {
		return q, fmt.Errorf("attributes requires service and level")
	}
	if q.Kind == "tags" && q.Tag == "" {
		return q, fmt.Errorf("tags requires tag")
	}
	return q, nil
}
