package main

import (
	"context"
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type mongoStore struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	poolSize   int
}

func newMongo(uri string, poolSize int) (*mongoStore, error) {
	journal := true
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(uint64(poolSize)).SetRetryWrites(false).
		SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}).SetAppName("docbench"))
	if err != nil {
		return nil, err
	}
	database := client.Database("docbench")
	return &mongoStore{client: client, database: database, collection: database.Collection("events_bench"), poolSize: poolSize}, nil
}

func (m *mongoStore) Ping(ctx context.Context) error { return m.client.Ping(ctx, nil) }
func (m *mongoStore) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.client.Disconnect(ctx)
}

func (m *mongoStore) Reset(ctx context.Context) error {
	if err := m.collection.Drop(ctx); err != nil {
		return err
	}
	_, err := m.collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_timeline")},
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "payload.service", Value: 1}, {Key: "payload.level", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_attributes")},
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "payload.tags", Value: 1}, {Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}, Options: options.Index().SetName("events_bench_tags")},
	})
	return err
}

func (m *mongoStore) Write(ctx context.Context, events []Event) (time.Duration, error) {
	start := time.Now()
	documents := make([]any, len(events))
	for i := range events {
		documents[i] = events[i]
	}
	_, err := m.collection.InsertMany(ctx, documents, options.InsertMany().SetOrdered(true))
	return time.Since(start), err
}

func mongoFilter(q ReadQuery) bson.D {
	filter := bson.D{{Key: "tenant_id", Value: q.Tenant}, {Key: "occurred_at", Value: bson.D{{Key: "$gte", Value: q.From}, {Key: "$lt", Value: q.To}}}}
	switch q.Kind {
	case "attributes":
		filter = append(filter, bson.E{Key: "payload.service", Value: q.Service}, bson.E{Key: "payload.level", Value: q.Level})
	case "tags":
		filter = append(filter, bson.E{Key: "payload.tags", Value: q.Tag})
	}
	return filter
}

func mongoSort() bson.D {
	return bson.D{{Key: "occurred_at", Value: -1}, {Key: "_id", Value: -1}}
}

func (m *mongoStore) Read(ctx context.Context, q ReadQuery) ([]Event, time.Duration, error) {
	filter := mongoFilter(q)
	start := time.Now()
	cursor, err := m.collection.Find(ctx, filter, options.Find().SetSort(mongoSort()).SetLimit(int64(q.Limit)).SetBatchSize(int32(q.Limit)))
	if err != nil {
		return nil, time.Since(start), err
	}
	defer cursor.Close(ctx)
	events := make([]Event, 0, q.Limit)
	for cursor.Next(ctx) {
		var event Event
		if err := cursor.Decode(&event); err != nil {
			return nil, time.Since(start), err
		}
		events = append(events, event)
	}
	return events, time.Since(start), cursor.Err()
}

func decodeCommand(result *mongo.SingleResult) (json.RawMessage, error) {
	raw, err := result.Raw()
	if err != nil {
		return nil, err
	}
	data, err := bson.MarshalExtJSON(raw, false, false)
	return json.RawMessage(data), err
}

func (m *mongoStore) Explain(ctx context.Context, q ReadQuery) (any, error) {
	find := bson.D{{Key: "find", Value: "events_bench"}, {Key: "filter", Value: mongoFilter(q)}, {Key: "sort", Value: mongoSort()}, {Key: "limit", Value: q.Limit}}
	return decodeCommand(m.database.RunCommand(ctx, bson.D{{Key: "explain", Value: find}, {Key: "verbosity", Value: "executionStats"}}))
}

func (m *mongoStore) Maintain(ctx context.Context) (any, error) {
	// MongoDB maintains its B-tree indexes during writes; no GIN-like cleanup applies.
	return map[string]any{"elapsed_ms": 0, "action": "none", "note": "MongoDB indexes are maintained during writes"}, nil
}

func (m *mongoStore) Stats(ctx context.Context) (any, error) {
	var buildInfo struct {
		Version string `bson:"version"`
	}
	if err := m.database.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		return nil, err
	}
	var sizes struct {
		Count          int64 `bson:"count"`
		Size           int64 `bson:"size"`
		StorageSize    int64 `bson:"storageSize"`
		TotalIndexSize int64 `bson:"totalIndexSize"`
		TotalSize      int64 `bson:"totalSize"`
	}
	if err := m.database.RunCommand(ctx, bson.D{{Key: "collStats", Value: "events_bench"}, {Key: "scale", Value: 1}}).Decode(&sizes); err != nil {
		return nil, err
	}
	cursor, err := m.collection.Indexes().List(ctx)
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
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	server, err := decodeCommand(m.database.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var status map[string]json.RawMessage
	if err := json.Unmarshal(server, &status); err != nil {
		return nil, err
	}
	return map[string]any{"backend": "mongodb", "profile": "mongo_targeted", "version": buildInfo.Version, "count": sizes.Count, "logical_data_bytes": sizes.Size, "data_bytes": sizes.StorageSize, "index_bytes": sizes.TotalIndexSize, "total_bytes": sizes.TotalSize, "indexes": indexes, "settings": map[string]any{"write_concern": map[string]any{"w": 1, "j": true}, "retry_writes": false, "storage_engine": status["storageEngine"], "wired_tiger": status["wiredTiger"]}, "connections": status["connections"], "pool_max": m.poolSize}, nil
}
