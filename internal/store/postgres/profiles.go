package postgres

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

// Profile is one PostgreSQL indexing strategy: the indexes it adds and the
// predicates written so that the planner can use them.
type Profile struct {
	Name    string
	Indexes []string
	// Predicates adds the kind-specific condition; timeline has none.
	Predicates map[store.Kind]predicate
	// GINIndex has its pending list flushed before measurements.
	GINIndex string
}

// predicate appends its arguments and returns a condition referencing them.
type predicate func(q store.ReadQuery, args []any) (string, []any)

// Whole-payload containment for every kind: one GIN answers all of them.
var containment = map[store.Kind]predicate{
	store.KindAttributes: payloadContainsAttributes,
	store.KindTags:       payloadContainsTag,
	store.KindAdhoc:      payloadContainsAdhoc,
	store.KindTrace:      payloadContainsTrace,
}

var profiles = []Profile{
	{
		Name:       "pg_gin_path_ops",
		Indexes:    []string{`CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_path_ops)`},
		Predicates: containment,
		GINIndex:   "events_bench_payload",
	},
	{
		Name:       "pg_gin_ops",
		Indexes:    []string{`CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_ops)`},
		Predicates: containment,
		GINIndex:   "events_bench_payload",
	},
	{
		Name: "pg_targeted",
		Indexes: []string{
			`CREATE INDEX events_bench_attributes ON events_bench (tenant_id, (payload->>'service'), (payload->>'level'), occurred_at DESC, id DESC)`,
			`CREATE INDEX events_bench_tags ON events_bench USING gin ((payload->'tags') jsonb_path_ops)`,
		},
		Predicates: map[store.Kind]predicate{
			store.KindAttributes: extractedAttributes,
			store.KindTags:       tagArrayContains,
			// No index serves these; they measure an unplanned query on a targeted schema.
			store.KindAdhoc: payloadContainsAdhoc,
			store.KindTrace: payloadContainsTrace,
		},
		GINIndex: "events_bench_tags",
	},
}

func Lookup(name string) (Profile, bool) {
	for _, p := range profiles {
		if p.Name == name {
			return p, true
		}
	}
	return Profile{}, false
}

func Names() []string {
	names := make([]string, len(profiles))
	for i, p := range profiles {
		names[i] = p.Name
	}
	return names
}

func placeholder(args []any) string { return "$" + strconv.Itoa(len(args)) }

// jsonArg never fails for the plain maps and slices used here.
func jsonArg(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func contains(document any, args []any) (string, []any) {
	args = append(args, jsonArg(document))
	return "payload @> " + placeholder(args) + "::jsonb", args
}

func payloadContainsAttributes(q store.ReadQuery, args []any) (string, []any) {
	return contains(map[string]string{"service": q.Service, "level": q.Level}, args)
}

func payloadContainsTag(q store.ReadQuery, args []any) (string, []any) {
	return contains(map[string][]string{"tags": {q.Tag}}, args)
}

// Status stays a JSON number so that it matches the stored attrs.status.
func payloadContainsAdhoc(q store.ReadQuery, args []any) (string, []any) {
	return contains(map[string]any{"level": q.Level, "attrs": map[string]any{"region": q.Region, "status": q.Status}}, args)
}

func payloadContainsTrace(q store.ReadQuery, args []any) (string, []any) {
	return contains(map[string]any{"attrs": map[string]string{"trace_id": q.TraceID}}, args)
}

// Expressions must match the expression index definitions exactly.
func extractedAttributes(q store.ReadQuery, args []any) (string, []any) {
	args = append(args, q.Service)
	service := placeholder(args)
	args = append(args, q.Level)
	return fmt.Sprintf("payload->>'service'=%s AND payload->>'level'=%s", service, placeholder(args)), args
}

func tagArrayContains(q store.ReadQuery, args []any) (string, []any) {
	args = append(args, jsonArg([]string{q.Tag}))
	return "(payload->'tags') @> " + placeholder(args) + "::jsonb", args
}
