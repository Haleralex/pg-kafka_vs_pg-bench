// Package postgres stores events as JSONB rows under configurable index profiles.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

// Store must target the dedicated docbench database: Reset drops its table.
type Store struct {
	pool    *pgxpool.Pool
	profile Profile
}

var _ store.Store = (*Store)(nil)

func Open(ctx context.Context, uri string, profile Profile, poolSize int) (*Store, error) {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	if config.ConnConfig.Database != "docbench" {
		return nil, fmt.Errorf("PG_URL must target the dedicated docbench database")
	}
	config.MaxConns = int32(poolSize)
	config.MinConns = 0
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	config.ConnConfig.RuntimeParams["application_name"] = "docbench"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, profile: profile}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }

func (s *Store) Reset(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	statements := append([]string{
		`DROP TABLE IF EXISTS events_bench`,
		`CREATE TABLE events_bench (id bigint PRIMARY KEY, tenant_id integer NOT NULL, occurred_at timestamptz NOT NULL, payload jsonb NOT NULL)`,
		`CREATE INDEX events_bench_timeline ON events_bench (tenant_id, occurred_at DESC, id DESC)`,
	}, s.profile.Indexes...)
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Write(ctx context.Context, events []event.Event) (time.Duration, error) {
	start := time.Now()
	values := make([][]any, len(events))
	for i, e := range events {
		payload, err := json.Marshal(e.Payload)
		if err != nil {
			return time.Since(start), err
		}
		values[i] = []any{e.ID, e.TenantID, e.OccurredAt, payload}
	}
	// COPY is one statement, so each PostgreSQL batch commits atomically.
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"events_bench"}, []string{"id", "tenant_id", "occurred_at", "payload"}, pgx.CopyFromRows(values))
	return time.Since(start), err
}

func (s *Store) query(q store.ReadQuery) (string, []any) {
	sql := `SELECT id, tenant_id, occurred_at, payload FROM events_bench WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $3`
	args := []any{q.Tenant, q.From, q.To}
	var condition string
	switch q.Kind {
	case store.KindAttributes:
		condition, args = s.profile.Attributes(q, args)
	case store.KindTags:
		condition, args = s.profile.Tags(q, args)
	}
	if condition != "" {
		sql += " AND " + condition
	}
	args = append(args, q.Limit)
	sql += ` ORDER BY occurred_at DESC, id DESC LIMIT ` + placeholder(args)
	return sql, args
}

func (s *Store) Read(ctx context.Context, q store.ReadQuery) ([]event.Event, time.Duration, error) {
	sql, args := s.query(q)
	start := time.Now()
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer rows.Close()
	events := make([]event.Event, 0, q.Limit)
	for rows.Next() {
		var e event.Event
		var payload []byte
		if err := rows.Scan(&e.ID, &e.TenantID, &e.OccurredAt, &payload); err != nil {
			return nil, time.Since(start), err
		}
		e.OccurredAt = e.OccurredAt.UTC()
		if err := json.Unmarshal(payload, &e.Payload); err != nil {
			return nil, time.Since(start), err
		}
		events = append(events, e)
	}
	return events, time.Since(start), rows.Err()
}

func (s *Store) Explain(ctx context.Context, q store.ReadQuery) (any, error) {
	sql, args := s.query(q)
	var plan json.RawMessage
	err := s.pool.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) `+sql, args...).Scan(&plan)
	return plan, err
}

func (s *Store) Maintain(ctx context.Context) (any, error) {
	start := time.Now()
	var cleaned int64
	if err := s.pool.QueryRow(ctx, `SELECT gin_clean_pending_list($1::regclass)`, s.profile.GINIndex).Scan(&cleaned); err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, `ANALYZE events_bench`); err != nil {
		return nil, err
	}
	return map[string]any{"elapsed_ms": millis(time.Since(start)), "gin_pages_cleaned": cleaned, "index": s.profile.GINIndex}, nil
}

func (s *Store) Stats(ctx context.Context) (any, error) {
	var version string
	var count, dataBytes, indexBytes, totalBytes int64
	err := s.pool.QueryRow(ctx, `SELECT version(), (SELECT count(*) FROM events_bench), pg_table_size('events_bench'), pg_indexes_size('events_bench'), pg_total_relation_size('events_bench')`).Scan(&version, &count, &dataBytes, &indexBytes, &totalBytes)
	if err != nil {
		return nil, err
	}
	indexes, err := s.indexStats(ctx)
	if err != nil {
		return nil, err
	}
	settings, err := s.settings(ctx)
	if err != nil {
		return nil, err
	}
	pool := s.pool.Stat()
	return map[string]any{
		"backend": "postgres", "profile": s.profile.Name, "version": version, "count": count,
		"data_bytes": dataBytes, "index_bytes": indexBytes, "total_bytes": totalBytes,
		"indexes": indexes, "settings": settings,
		"pool": map[string]any{"max": pool.MaxConns(), "total": pool.TotalConns(), "acquired": pool.AcquiredConns(), "acquire_duration_ms": millis(pool.AcquireDuration()), "empty_acquire_count": pool.EmptyAcquireCount()},
	}, nil
}

func (s *Store) indexStats(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `SELECT indexname, indexdef, pg_relation_size(indexname::regclass) FROM pg_indexes WHERE schemaname=current_schema() AND tablename='events_bench' ORDER BY indexname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	indexes := []map[string]any{}
	for rows.Next() {
		var name, definition string
		var size int64
		if err := rows.Scan(&name, &definition, &size); err != nil {
			return nil, err
		}
		indexes = append(indexes, map[string]any{"name": name, "definition": definition, "size_bytes": size})
	}
	return indexes, rows.Err()
}

func (s *Store) settings(ctx context.Context) (map[string]any, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, setting, COALESCE(unit, '') FROM pg_settings WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes', 'wal_level', 'shared_buffers', 'work_mem', 'maintenance_work_mem', 'gin_pending_list_limit', 'checkpoint_timeout', 'max_wal_size', 'autovacuum') ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	settings := map[string]any{}
	for rows.Next() {
		var name, value, unit string
		if err := rows.Scan(&name, &value, &unit); err != nil {
			return nil, err
		}
		if unit == "" {
			settings[name] = value
		} else {
			settings[name] = map[string]string{"value": value, "unit": unit}
		}
	}
	return settings, rows.Err()
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
