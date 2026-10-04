package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

func TestParseReadQuery(t *testing.T) {
	valid := url.Values{"tenant": {"5"}, "from_ms": {strconv.FormatInt(event.BaseTime.UnixMilli(), 10)}, "to_ms": {strconv.FormatInt(event.BaseTime.UnixMilli()+1000, 10)}}
	q, err := ParseReadQuery(valid)
	if err != nil || q.Kind != store.KindTimeline || q.Limit != 50 || !q.From.Equal(event.BaseTime) {
		t.Fatalf("valid query failed: %+v %v", q, err)
	}
	for _, tc := range []struct{ key, value string }{{"kind", "bogus"}, {"tenant", "0"}, {"tenant", "101"}, {"from_ms", "bad"}, {"to_ms", valid.Get("from_ms")}, {"limit", "0"}, {"limit", "1001"}, {"kind", "attributes"}, {"kind", "tags"}, {"kind", "adhoc"}, {"kind", "trace"}} {
		invalid, _ := url.ParseQuery(valid.Encode())
		invalid.Set(tc.key, tc.value)
		if _, err := ParseReadQuery(invalid); err == nil {
			t.Errorf("accepted invalid %s=%s", tc.key, tc.value)
		}
	}
}

// A nil store panics if any of these requests passes validation.
func TestInvalidRequestsNeverReachStore(t *testing.T) {
	handler := New(nil, "test", time.Second, time.Second).Handler()
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/admin/seed", `{"count":2000001}`},
		{"POST", "/admin/seed", `{"count":100,"batch_size":-1}`},
		{"POST", "/write", `{"start_id":0,"count":1}`},
		{"POST", "/write", `{"start_id":1,"count":1001}`},
		{"POST", "/write", `{"start_id":1,"count":1}{}`},
		{"POST", "/write", `{"start_id":1,"count":1,"typo":1}`},
		{"GET", "/read?tenant=1", ""},
		{"GET", "/explain?tenant=1", ""},
		{"GET", "/count?tenant=1", ""},
		{"GET", "/admin/traces?seed_count=0&count=1", ""},
		{"GET", "/admin/traces?seed_count=10&count=10001", ""},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s %s: got %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}

func TestParseAdhocQuery(t *testing.T) {
	v := url.Values{"kind": {"adhoc"}, "tenant": {"5"}, "from_ms": {"1"}, "to_ms": {"2"}, "level": {"error"}, "region": {"eu-west"}, "status": {"503"}}
	if q, err := ParseReadQuery(v); err != nil || q.Status != 503 || q.Region != "eu-west" {
		t.Fatalf("valid adhoc query: %+v %v", q, err)
	}
	v.Set("status", "5xx")
	if _, err := ParseReadQuery(v); err == nil {
		t.Fatal("accepted non-integer status")
	}
}

func TestTracesComeFromSeededEvents(t *testing.T) {
	w := httptest.NewRecorder()
	New(nil, "test", time.Second, time.Second).Handler().ServeHTTP(w, httptest.NewRequest("GET", "/admin/traces?seed_count=1000&count=4", nil))
	var traces []struct {
		ID      int64  `json:"id"`
		Tenant  int    `json:"tenant"`
		TraceID string `json:"trace_id"`
	}
	if err := json.NewDecoder(w.Body).Decode(&traces); err != nil || len(traces) != 4 {
		t.Fatalf("traces: %d %v", w.Code, err)
	}
	for _, tr := range traces {
		e := event.Generate(tr.ID)
		if tr.ID < 1 || tr.ID > 1000 || e.TenantID != tr.Tenant || e.Payload.Attrs.TraceID != tr.TraceID {
			t.Fatalf("trace does not describe a seeded event: %+v", tr)
		}
	}
}
