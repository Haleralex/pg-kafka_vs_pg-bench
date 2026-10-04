package mongodb

import (
	"slices"
	"testing"

	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

func TestFilterPathsMatchDocumentFields(t *testing.T) {
	q := store.ReadQuery{Tenant: 3, Service: "svc-01", Level: "error", Tag: "tag-03", Region: "eu-west", Status: 503, TraceID: "abc"}
	want := map[store.Kind][]string{
		store.KindTimeline:   nil,
		store.KindAttributes: {"payload.service", "payload.level"},
		store.KindTags:       {"payload.tags"},
		store.KindAdhoc:      {"payload.level", "payload.attrs.region", "payload.attrs.status"},
		store.KindTrace:      {"payload.attrs.trace_id"},
	}
	for _, kind := range store.Kinds {
		q.Kind = kind
		f := filter(q)
		var keys []string
		for _, e := range f[2:] {
			keys = append(keys, e.Key)
		}
		if f[0].Key != "tenant_id" || f[1].Key != "occurred_at" || !slices.Equal(keys, want[kind]) {
			t.Fatalf("%s: unexpected filter %v", kind, f)
		}
		if kind == store.KindAdhoc && f[4].Value != 503 {
			t.Fatalf("status must stay numeric: %#v", f[4].Value)
		}
	}
}

func TestEveryProfileIsNamed(t *testing.T) {
	for _, name := range Names() {
		if p, ok := Lookup(name); !ok || p.Name != name || len(p.Indexes) == 0 {
			t.Fatalf("profile %s is incomplete", name)
		}
	}
}
