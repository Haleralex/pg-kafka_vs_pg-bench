package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestEventDeterminismAndEncoding(t *testing.T) {
	event := generateEvent(123456)
	if !reflect.DeepEqual(event, generateEvent(123456)) {
		t.Fatal("event generation is not deterministic")
	}
	if event.OccurredAt.UnixMilli() != baseTime.UnixMilli()+123456 {
		t.Fatal("timestamp must preserve id milliseconds")
	}
	data, err := bson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := bson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(event)
	gotJSON, _ := json.Marshal(decoded)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("JSON changed after BSON round trip:\n%s\n%s", wantJSON, gotJSON)
	}
	if len(event.Payload.Message) < 699 || len(event.Payload.Message) > 720 || event.Payload.Tags[0] == event.Payload.Tags[1] {
		t.Fatal("unexpected payload size or duplicate tags")
	}
	batch := generateBatch(123455, 3)
	if !reflect.DeepEqual(batch[1], event) {
		t.Fatal("batch boundaries changed the generated document")
	}
}

func TestGeneratorDistribution(t *testing.T) {
	servicesByTenant := map[int]map[string]bool{}
	errors := 0
	for id := int64(1); id <= 10000; id++ {
		e := generateEvent(id)
		if servicesByTenant[e.TenantID] == nil {
			servicesByTenant[e.TenantID] = map[string]bool{}
		}
		servicesByTenant[e.TenantID][e.Payload.Service] = true
		if e.Payload.Level == "error" {
			errors++
		}
	}
	if errors < 850 || errors > 1150 || len(servicesByTenant) != 100 {
		t.Fatalf("unexpected weighted distribution: errors=%d tenants=%d", errors, len(servicesByTenant))
	}
	for tenant, services := range servicesByTenant {
		if len(services) < 16 {
			t.Fatalf("tenant %d sees only %d services; generator may correlate fields", tenant, len(services))
		}
	}
}

func TestQueryValidation(t *testing.T) {
	valid := url.Values{"tenant": {"5"}, "from_ms": {strconv.FormatInt(baseTime.UnixMilli(), 10)}, "to_ms": {strconv.FormatInt(baseTime.UnixMilli()+1000, 10)}}
	q, err := parseReadQuery(valid)
	if err != nil || q.Kind != "timeline" || q.Limit != 50 || !q.From.Equal(baseTime) {
		t.Fatalf("valid query failed: %+v %v", q, err)
	}
	for _, tc := range []struct{ key, value string }{{"kind", "bogus"}, {"tenant", "0"}, {"tenant", "101"}, {"from_ms", "bad"}, {"to_ms", valid.Get("from_ms")}, {"limit", "0"}, {"limit", "1001"}, {"kind", "attributes"}, {"kind", "tags"}} {
		copy, _ := url.ParseQuery(valid.Encode())
		copy.Set(tc.key, tc.value)
		if _, err := parseReadQuery(copy); err == nil {
			t.Errorf("accepted invalid %s=%s", tc.key, tc.value)
		}
	}
}

func TestPostgresPredicatesMatchProfiles(t *testing.T) {
	q := ReadQuery{Kind: "attributes", Tenant: 3, From: baseTime, To: baseTime.Add(time.Hour), Service: `svc-00' OR 1=1 --`, Level: "error", Limit: 50}
	for _, profile := range []string{"pg_gin_path_ops", "pg_gin_ops", "pg_targeted"} {
		p := &postgresStore{profile: profile}
		sql, args := p.query(q)
		if strings.Contains(sql, q.Service) || !strings.Contains(sql, "occurred_at >= $2 AND occurred_at < $3") || !strings.Contains(sql, "ORDER BY occurred_at DESC, id DESC") {
			t.Fatalf("unsafe or inconsistent query: %s", sql)
		}
		if profile == "pg_targeted" {
			if !strings.Contains(sql, "payload->>'service'=$4 AND payload->>'level'=$5") || len(args) != 6 {
				t.Fatalf("targeted predicates do not match index: %s", sql)
			}
		} else if !strings.Contains(sql, "payload @> $4::jsonb") || len(args) != 5 {
			t.Fatalf("GIN predicate does not use containment: %s", sql)
		}
		q.Kind, q.Tag = "tags", "tag-03"
		sql, _ = p.query(q)
		if profile == "pg_targeted" && !strings.Contains(sql, "(payload->'tags') @> $4::jsonb") {
			t.Fatalf("tag predicate does not match expression index: %s", sql)
		}
		q.Kind = "attributes"
	}
}

func TestInvalidRequestsNeverReachStore(t *testing.T) {
	a := &api{requestTimeout: time.Second, adminTimeout: time.Second}
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/admin/seed", `{"count":2000001}`},
		{"POST", "/admin/seed", `{"count":100,"batch_size":-1}`},
		{"POST", "/write", `{"start_id":0,"count":1}`},
		{"POST", "/write", `{"start_id":1,"count":1001}`},
		{"POST", "/write", `{"start_id":1,"count":1}{}`},
		{"POST", "/write", `{"start_id":1,"count":1,"typo":1}`},
		{"GET", "/read?tenant=1", ""},
		{"GET", "/explain?tenant=1", ""},
	} {
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s %s: got %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}

func TestIDRangeRejectsOverflow(t *testing.T) {
	if !validIDRange(maxEventID, 1) || validIDRange(maxEventID, 2) || validIDRange(1, 0) || validIDRange(-1, 1) {
		t.Fatal("invalid id range validation")
	}
}

func TestPostgresRejectsOtherDatabase(t *testing.T) {
	store, err := newPostgres(context.Background(), "postgres://bench:bench@localhost:5432/production?sslmode=disable", "pg_gin_ops", 2)
	if store != nil || err == nil || !strings.Contains(err.Error(), "docbench") {
		t.Fatalf("non-benchmark database accepted: %v %v", store, err)
	}
}
