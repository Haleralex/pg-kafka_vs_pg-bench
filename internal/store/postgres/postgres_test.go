package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

func TestPredicatesMatchProfiles(t *testing.T) {
	base := store.ReadQuery{Tenant: 3, From: event.BaseTime, To: event.BaseTime.Add(time.Hour), Service: `svc-00' OR 1=1 --`, Level: "error", Tag: "tag-03", Region: "eu-west", Status: 503, TraceID: "abc", Limit: 50}
	containment := map[store.Kind]string{
		store.KindTimeline: "", store.KindAttributes: "payload @> $4::jsonb", store.KindTags: "payload @> $4::jsonb",
		store.KindAdhoc: "payload @> $4::jsonb", store.KindTrace: "payload @> $4::jsonb",
	}
	want := map[string]map[store.Kind]string{
		"pg_gin_path_ops": containment,
		"pg_gin_ops":      containment,
		"pg_targeted": {
			store.KindTimeline: "", store.KindAttributes: "payload->>'service'=$4 AND payload->>'level'=$5", store.KindTags: "(payload->'tags') @> $4::jsonb",
			store.KindAdhoc: "payload @> $4::jsonb", store.KindTrace: "payload @> $4::jsonb",
		},
	}
	for _, name := range Names() {
		profile, _ := Lookup(name)
		s := &Store{profile: profile}
		for _, kind := range store.Kinds {
			condition, ok := want[name][kind]
			if !ok {
				t.Fatalf("%s/%s: no expected predicate", name, kind)
			}
			q := base
			q.Kind = kind
			sql, args := s.query(q)
			if strings.Contains(sql, q.Service) || !strings.Contains(sql, "occurred_at >= $2 AND occurred_at < $3") || !strings.HasSuffix(sql, "ORDER BY occurred_at DESC, id DESC LIMIT "+placeholder(args)) {
				t.Fatalf("%s/%s: unsafe or inconsistent query: %s", name, kind, sql)
			}
			if condition != "" && !strings.Contains(sql, " AND "+condition+" ORDER BY") {
				t.Fatalf("%s/%s: predicate %q does not match its index: %s", name, kind, condition, sql)
			}
			if args[len(args)-1] != q.Limit {
				t.Fatalf("%s/%s: limit is not the last argument: %v", name, kind, args)
			}
			count, countArgs := s.countQuery(q)
			if !strings.HasPrefix(count, "SELECT count(*) FROM events_bench WHERE") || strings.Contains(count, "ORDER BY") || len(countArgs) != len(args)-1 {
				t.Fatalf("%s/%s: count query differs from read filter: %s", name, kind, count)
			}
		}
	}
}

func TestAdhocContainmentKeepsStatusNumeric(t *testing.T) {
	_, args := payloadContainsAdhoc(store.ReadQuery{Level: "error", Region: "eu-west", Status: 503}, nil)
	var document struct {
		Attrs struct {
			Status json.Number `json:"status"`
		} `json:"attrs"`
	}
	decoder := json.NewDecoder(strings.NewReader(args[0].(string)))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil || document.Attrs.Status != "503" {
		t.Fatalf("status must be the JSON number 503, got %s (%v)", args[0], err)
	}
}

func TestProfilesNameTheirGINIndex(t *testing.T) {
	for _, name := range Names() {
		profile, _ := Lookup(name)
		created := strings.Join(profile.Indexes, "\n")
		if profile.GINIndex == "" || !strings.Contains(created, "INDEX "+profile.GINIndex+" ON events_bench USING gin") {
			t.Fatalf("%s: Maintain would clean %q, which the profile does not create as GIN", name, profile.GINIndex)
		}
	}
}

func TestOpenRejectsOtherDatabase(t *testing.T) {
	profile, _ := Lookup("pg_gin_ops")
	s, err := Open(context.Background(), "postgres://bench:bench@localhost:5432/production?sslmode=disable", profile, 2)
	if s != nil || err == nil || !strings.Contains(err.Error(), "docbench") {
		t.Fatalf("non-benchmark database accepted: %v %v", s, err)
	}
}
