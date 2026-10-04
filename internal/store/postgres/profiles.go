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
	Name       string
	Indexes    []string
	Attributes predicate
	Tags       predicate
	// GINIndex has its pending list flushed before measurements.
	GINIndex string
}

// predicate appends its arguments and returns a condition referencing them.
type predicate func(q store.ReadQuery, args []any) (string, []any)

var profiles = []Profile{
	{
		Name:       "pg_gin_path_ops",
		Indexes:    []string{`CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_path_ops)`},
		Attributes: payloadContainsAttributes,
		Tags:       payloadContainsTag,
		GINIndex:   "events_bench_payload",
	},
	{
		Name:       "pg_gin_ops",
		Indexes:    []string{`CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_ops)`},
		Attributes: payloadContainsAttributes,
		Tags:       payloadContainsTag,
		GINIndex:   "events_bench_payload",
	},
	{
		Name: "pg_targeted",
		Indexes: []string{
			`CREATE INDEX events_bench_attributes ON events_bench (tenant_id, (payload->>'service'), (payload->>'level'), occurred_at DESC, id DESC)`,
			`CREATE INDEX events_bench_tags ON events_bench USING gin ((payload->'tags') jsonb_path_ops)`,
		},
		Attributes: extractedAttributes,
		Tags:       tagArrayContains,
		GINIndex:   "events_bench_tags",
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

// jsonArg never fails for the string maps and slices used here.
func jsonArg(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

// Whole-payload containment can use a payload GIN index.
func payloadContainsAttributes(q store.ReadQuery, args []any) (string, []any) {
	args = append(args, jsonArg(map[string]string{"service": q.Service, "level": q.Level}))
	return "payload @> " + placeholder(args) + "::jsonb", args
}

func payloadContainsTag(q store.ReadQuery, args []any) (string, []any) {
	args = append(args, jsonArg(map[string][]string{"tags": {q.Tag}}))
	return "payload @> " + placeholder(args) + "::jsonb", args
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
