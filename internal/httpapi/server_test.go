package httpapi

import (
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
	for _, tc := range []struct{ key, value string }{{"kind", "bogus"}, {"tenant", "0"}, {"tenant", "101"}, {"from_ms", "bad"}, {"to_ms", valid.Get("from_ms")}, {"limit", "0"}, {"limit", "1001"}, {"kind", "attributes"}, {"kind", "tags"}} {
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
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s %s: got %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
