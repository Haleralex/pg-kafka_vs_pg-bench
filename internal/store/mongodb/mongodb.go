// Package mongodb stores events as documents under configurable index profiles.
package mongodb

import (
	"context"
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

// Profile is one MongoDB indexing strategy added to the shared timeline index.
type Profile struct {
	Name    string
	Indexes []mongo.IndexModel
}

var profiles = []Profile{
	{
		Name: "mongo_targeted",
		Indexes: []mongo.IndexModel{
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "payload.service", Value: 1}, {Key: "payload.level", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_attributes")},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "payload.tags", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_tags")},
		},
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

type Store struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	profile    Profile
	poolSize   int
}

var _ store.Store = (*Store)(nil)

func Open(uri string, profile Profile, poolSize int) (*Store, error) {
	journal := true
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(uint64(poolSize)).SetRetryWrites(false).
		SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}).SetAppName("docbench"))
	if err != nil {
		return nil, err
	}
	database := client.Database("docbench")
	return &Store{client: client, database: database, collection: database.Collection("events_bench"), profile: profile, poolSize: poolSize}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.client.Ping(ctx, nil) }

func (s *Store) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.client.Disconnect(ctx)
}

func (s *Store) Reset(ctx context.Context) error {
	if err := s.collection.Drop(ctx); err != nil {
		return err
	}
	timeline := mongo.IndexModel{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_timeline")}
	_, err := s.collection.Indexes().CreateMany(ctx, append([]mongo.IndexModel{timeline}, s.profile.Indexes...))
	return err
}

func (s *Store) Write(ctx context.Context, events []event.Event) (time.Duration, error) {
	start := time.Now()
	documents := make([]any, len(events))
	for i := range events {
		documents[i] = events[i]
	}
	// Ordered InsertMany is atomic per document, not per batch, unlike PostgreSQL COPY.
	_, err := s.collection.InsertMany(ctx, documents, options.InsertMany().SetOrdered(true))
	return time.Since(start), err
}

func filter(q store.ReadQuery) bson.D {
	f := bson.D{{Key: "tenant_id", Value: q.Tenant}, {Key: "occurred_at", Value: bson.D{{Key: "$gte", Value: q.From}, {Key: "$lt", Value: q.To}}}}
	switch q.Kind {
	case store.KindAttributes:
		f = append(f, bson.E{Key: "payload.service", Value: q.Service}, bson.E{Key: "payload.level", Value: q.Level})
	case store.KindTags:
		f = append(f, bson.E{Key: "payload.tags", Value: q.Tag})
	}
	return f
}

var newestFirst = bson.D{{Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}

func (s *Store) Read(ctx context.Context, q store.ReadQuery) ([]event.Event, time.Duration, error) {
	f := filter(q)
	start := time.Now()
	cursor, err := s.collection.Find(ctx, f, options.Find().SetSort(newestFirst).SetLimit(int64(q.Limit)).SetBatchSize(int32(q.Limit)))
	if err != nil {
		return nil, time.Since(start), err
	}
	defer cursor.Close(ctx)
	events := make([]event.Event, 0, q.Limit)
	for cursor.Next(ctx) {
		var e event.Event
		if err := cursor.Decode(&e); err != nil {
			return nil, time.Since(start), err
		}
		events = append(events, e)
	}
	return events, time.Since(start), cursor.Err()
}

func extJSON(result *mongo.SingleResult) (json.RawMessage, error) {
	raw, err := result.Raw()
	if err != nil {
		return nil, err
	}
	data, err := bson.MarshalExtJSON(raw, false, false)
	return json.RawMessage(data), err
}

func (s *Store) Explain(ctx context.Context, q store.ReadQuery) (any, error) {
	find := bson.D{{Key: "find", Value: s.collection.Name()}, {Key: "filter", Value: filter(q)}, {Key: "sort", Value: newestFirst}, {Key: "limit", Value: q.Limit}}
	return extJSON(s.database.RunCommand(ctx, bson.D{{Key: "explain", Value: find}, {Key: "verbosity", Value: "executionStats"}}))
}

func (s *Store) Maintain(ctx context.Context) (any, error) {
	// MongoDB maintains its B-tree indexes during writes; no GIN-like cleanup applies.
	return map[string]any{"elapsed_ms": 0, "action": "none", "note": "MongoDB indexes are maintained during writes"}, nil
}

func (s *Store) Stats(ctx context.Context) (any, error) {
	var buildInfo struct {
		Version string `bson:"version"`
	}
	if err := s.database.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return nil, err
	}
	var sizes struct {
		Count          int64 `bson:"count"`
		Size           int64 `bson:"size"`
		StorageSize    int64 `bson:"storageSize"`
		TotalIndexSize int64 `bson:"totalIndexSize"`
		TotalSize      int64 `bson:"totalSize"`
	}
	if err := s.database.RunCommand(ctx, bson.D{{Key: "collStats", Value: s.collection.Name()}, {Key: "scale", Value: 1}}).Decode(&sizes); err != nil {
		return nil, err
	}
	indexes, err := s.indexes(ctx)
	if err != nil {
		return nil, err
	}
	server, err := extJSON(s.database.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var status map[string]json.RawMessage
	if err := json.Unmarshal(server, &status); err != nil {
		return nil, err
	}
	return map[string]any{
		"backend": "mongodb", "profile": s.profile.Name, "version": buildInfo.Version, "count": sizes.Count,
		"logical_data_bytes": sizes.Size, "data_bytes": sizes.StorageSize, "index_bytes": sizes.TotalIndexSize, "total_bytes": sizes.TotalSize,
		"indexes":     indexes,
		"settings":    map[string]any{"write_concern": map[string]any{"w": 1, "j": true}, "retry_writes": false, "storage_engine": status["storageEngine"], "wired_tiger": status["wiredTiger"]},
		"connections": status["connections"], "pool_max": s.poolSize,
	}, nil
}

func (s *Store) indexes(ctx context.Context) ([]json.RawMessage, error) {
	cursor, err := s.collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	indexes := []json.RawMessage{}
	for cursor.Next(ctx) {
		data, err := bson.MarshalExtJSON(cursor.Current, false, false)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, json.RawMessage(data))
	}
	return indexes, cursor.Err()
}
