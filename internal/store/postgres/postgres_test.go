package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

func TestPredicatesMatchProfiles(t *testing.T) {
	base := store.ReadQuery{Tenant: 3, From: event.BaseTime, To: event.BaseTime.Add(time.Hour), Service: `svc-00' OR 1=1 --`, Level: "error", Tag: "tag-03", Limit: 50}
	cases := map[string]struct{ attributes, tags string }{
		"pg_gin_path_ops": {"payload @> $4::jsonb", "payload @> $4::jsonb"},
		"pg_gin_ops":      {"payload @> $4::jsonb", "payload @> $4::jsonb"},
		"pg_targeted":     {"payload->>'service'=$4 AND payload->>'level'=$5", "(payload->'tags') @> $4::jsonb"},
	}
	for _, name := range Names() {
		want, ok := cases[name]
		if !ok {
			t.Fatalf("profile %s has no predicate expectations", name)
		}
		profile, _ := Lookup(name)
		s := &Store{profile: profile}
		for kind, condition := range map[store.Kind]string{store.KindAttributes: want.attributes, store.KindTags: want.tags, store.KindTimeline: ""} {
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
		}
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
