// Package store defines the contract implemented by every benchmarked database.
package store

import (
	"context"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
)

type Kind string

const (
	// Known access patterns: the targeted profiles have an index for each.
	KindTimeline   Kind = "timeline"
	KindAttributes Kind = "attributes"
	KindTags       Kind = "tags"
	// Ad hoc patterns: only "index everything" profiles have a usable index.
	KindAdhoc Kind = "adhoc" // level + attrs.region + attrs.status
	KindTrace Kind = "trace" // attrs.trace_id
)

var Kinds = []Kind{KindTimeline, KindAttributes, KindTags, KindAdhoc, KindTrace}

// ReadQuery selects one tenant's events in [From, To), newest first.
type ReadQuery struct {
	Kind    Kind
	Tenant  int
	From    time.Time
	To      time.Time
	Service string // KindAttributes
	Level   string // KindAttributes, KindAdhoc
	Tag     string // KindTags
	Region  string // KindAdhoc
	Status  int    // KindAdhoc
	TraceID string // KindTrace
	Limit   int    // ignored by Count
}

// Store is one database configured with one indexing profile. Durations are
// observed in the client: pool waits, server work, transfer and decoding.
type Store interface {
	Ping(context.Context) error
	// Reset recreates the benchmark table or collection with the profile's indexes.
	Reset(context.Context) error
	Write(context.Context, []event.Event) (time.Duration, error)
	Read(context.Context, ReadQuery) ([]event.Event, time.Duration, error)
	// Count returns how many events match, without ordering or a limit.
	Count(context.Context, ReadQuery) (int64, time.Duration, error)
	Explain(ctx context.Context, q ReadQuery, count bool) (any, error)
	Stats(context.Context) (any, error)
	// Maintain brings indexes and planner statistics to a steady state before measuring.
	Maintain(context.Context) (any, error)
	Close()
}
