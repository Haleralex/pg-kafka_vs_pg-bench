package httpapi

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

// ParseReadQuery validates /read, /count and /explain parameters.
func ParseReadQuery(v url.Values) (store.ReadQuery, error) {
	q := store.ReadQuery{Kind: store.Kind(v.Get("kind")), Service: v.Get("service"), Level: v.Get("level"), Tag: v.Get("tag"), Region: v.Get("region"), TraceID: v.Get("trace_id"), Limit: 50}
	if q.Kind == "" {
		q.Kind = store.KindTimeline
	}
	if !slices.Contains(store.Kinds, q.Kind) {
		return q, fmt.Errorf("kind must be one of %v", store.Kinds)
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
	if q.Kind == store.KindAttributes && (q.Service == "" || q.Level == "") {
		return q, fmt.Errorf("attributes requires service and level")
	}
	if q.Kind == store.KindTags && q.Tag == "" {
		return q, fmt.Errorf("tags requires tag")
	}
	if q.Kind == store.KindAdhoc {
		if q.Status, err = strconv.Atoi(v.Get("status")); err != nil || q.Region == "" || q.Level == "" {
			return q, fmt.Errorf("adhoc requires level, region and integer status")
		}
	}
	if q.Kind == store.KindTrace && q.TraceID == "" {
		return q, fmt.Errorf("trace requires trace_id")
	}
	return q, nil
}
