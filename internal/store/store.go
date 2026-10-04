// Package store defines the contract implemented by every benchmarked database.
package store

import (
	"context"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
)

type Kind string

const (
	KindTimeline   Kind = "timeline"
	KindAttributes Kind = "attributes"
	KindTags       Kind = "tags"
)

// ReadQuery selects one tenant's events in [From, To), newest first.
type ReadQuery struct {
	Kind    Kind
	Tenant  int
	From    time.Time
	To      time.Time
	Service string // KindAttributes only
	Level   string // KindAttributes only
	Tag     string // KindTags only
	Limit   int
}

// Store is one database configured with one indexing profile. Durations are
// observed in the client: pool waits, server work, transfer and decoding.
type Store interface {
	Ping(context.Context) error
	// Reset recreates the benchmark table or collection with the profile's indexes.
	Reset(context.Context) error
	Write(context.Context, []event.Event) (time.Duration, error)
	Read(context.Context, ReadQuery) ([]event.Event, time.Duration, error)
	Explain(context.Context, ReadQuery) (any, error)
	Stats(context.Context) (any, error)
	// Maintain brings indexes and planner statistics to a steady state before measuring.
	Maintain(context.Context) (any, error)
	Close()
}
