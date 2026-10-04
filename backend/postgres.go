package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresStore struct {
	pool    *pgxpool.Pool
	profile string
}

func newPostgres(ctx context.Context, uri, profile string, poolSize int) (*postgresStore, error) {
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
	return &postgresStore{pool: pool, profile: profile}, nil
}

func (p *postgresStore) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }
func (p *postgresStore) Close()                         { p.pool.Close() }

func (p *postgresStore) Reset(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	statements := []string{
		`DROP TABLE IF EXISTS events_bench`,
		`CREATE TABLE events_bench (id bigint PRIMARY KEY, tenant_id integer NOT NULL, occurred_at timestamptz NOT NULL, payload jsonb NOT NULL)`,
		`CREATE INDEX events_bench_timeline ON events_bench (tenant_id, occurred_at DESC, id DESC)`,
	}
	switch p.profile {
	case "pg_gin_path_ops":
		statements = append(statements, `CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_path_ops)`)
	case "pg_gin_ops":
		statements = append(statements, `CREATE INDEX events_bench_payload ON events_bench USING gin (payload jsonb_ops)`)
	case "pg_targeted":
		statements = append(statements,
			`CREATE INDEX events_bench_attributes ON events_bench (tenant_id, (payload->>'service'), (payload->>'level'), occurred_at DESC, id DESC)`,
			`CREATE INDEX events_bench_tags ON events_bench USING gin ((payload->'tags') jsonb_path_ops)`)
	default:
		return fmt.Errorf("unknown postgres profile %q", p.profile)
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (p *postgresStore) Write(ctx context.Context, events []Event) (time.Duration, error) {
	start := time.Now()
	values := make([][]any, len(events))
	for i, event := range events {
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			return time.Since(start), err
		}
		values[i] = []any{event.ID, event.TenantID, event.OccurredAt, payload}
	}
	// COPY is one statement, so each PostgreSQL batch commits atomically.
	_, err := p.pool.CopyFrom(ctx, pgx.Identifier{"events_bench"}, []string{"id", "tenant_id", "occurred_at", "payload"}, pgx.CopyFromRows(values))
	return time.Since(start), err
}

func (p *postgresStore) query(q ReadQuery) (string, []any) {
	sql := `SELECT id, tenant_id, occurred_at, payload FROM events_bench WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $3`
	args := []any{q.Tenant, q.From, q.To}
	switch q.Kind {
	case "attributes":
		if p.profile == "pg_targeted" {
			sql += ` AND payload->>'service'=$4 AND payload->>'level'=$5`
			args = append(args, q.Service, q.Level)
		} else {
			filter, _ := json.Marshal(map[string]string{"service": q.Service, "level": q.Level})
			sql += ` AND payload @> $4::jsonb`
			args = append(args, string(filter))
		}
	case "tags":
		if p.profile == "pg_targeted" {
			filter, _ := json.Marshal([]string{q.Tag})
			sql += ` AND (payload->'tags') @> $4::jsonb`
			args = append(args, string(filter))
		} else {
			filter, _ := json.Marshal(map[string][]string{"tags": {q.Tag}})
			sql += ` AND payload @> $4::jsonb`
			args = append(args, string(filter))
		}
	}
	args = append(args, q.Limit)
	sql += fmt.Sprintf(` ORDER BY occurred_at DESC, id DESC LIMIT $%d`, len(args))
	return sql, args
}

func (p *postgresStore) Read(ctx context.Context, q ReadQuery) ([]Event, time.Duration, error) {
	sql, args := p.query(q)
	start := time.Now()
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer rows.Close()
	events := make([]Event, 0, q.Limit)
	for rows.Next() {
		var event Event
		var payload []byte
		if err := rows.Scan(&event.ID, &event.TenantID, &event.OccurredAt, &payload); err != nil {
			return nil, time.Since(start), err
		}
		event.OccurredAt = event.OccurredAt.UTC()
		if err := json.Unmarshal(payload, &event.Payload); err != nil {
			return nil, time.Since(start), err
		}
		events = append(events, event)
	}
	return events, time.Since(start), rows.Err()
}

func (p *postgresStore) Explain(ctx context.Context, q ReadQuery) (any, error) {
	sql, args := p.query(q)
	var plan json.RawMessage
	err := p.pool.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) `+sql, args...).Scan(&plan)
	return plan, err
}

func (p *postgresStore) Maintain(ctx context.Context) (any, error) {
	start := time.Now()
	var index string
	if p.profile == "pg_targeted" {
		index = "events_bench_tags"
	} else {
		index = "events_bench_payload"
	}
	var cleaned int64
	if err := p.pool.QueryRow(ctx, `SELECT gin_clean_pending_list($1::regclass)`, index).Scan(&cleaned); err != nil {
		return nil, err
	}
	if _, err := p.pool.Exec(ctx, `ANALYZE events_bench`); err != nil {
		return nil, err
	}
	return map[string]any{"elapsed_ms": millis(time.Since(start)), "gin_pages_cleaned": cleaned, "index": index}, nil
}

func (p *postgresStore) Stats(ctx context.Context) (any, error) {
	var version string
	var count, dataBytes, indexBytes, totalBytes int64
	err := p.pool.QueryRow(ctx, `SELECT version(), (SELECT count(*) FROM events_bench), pg_table_size('events_bench'), pg_indexes_size('events_bench'), pg_total_relation_size('events_bench')`).Scan(&version, &count, &dataBytes, &indexBytes, &totalBytes)
	if err != nil {
		return nil, err
	}
	indexes := []map[string]any{}
	rows, err := p.pool.Query(ctx, `SELECT indexname, indexdef, pg_relation_size(indexname::regclass) FROM pg_indexes WHERE schemaname=current_schema() AND tablename='events_bench' ORDER BY indexname`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, definition string
		var size int64
		if err := rows.Scan(&name, &definition, &size); err != nil {
			rows.Close()
			return nil, err
		}
		indexes = append(indexes, map[string]any{"name": name, "definition": definition, "size_bytes": size})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	settings := map[string]any{}
	rows, err = p.pool.Query(ctx, `SELECT name, setting, COALESCE(unit, '') FROM pg_settings WHERE name IN ('fsync', 'synchronous_commit', 'full_page_writes', 'wal_level', 'shared_buffers', 'work_mem', 'maintenance_work_mem', 'gin_pending_list_limit', 'checkpoint_timeout', 'max_wal_size', 'autovacuum') ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	pool := p.pool.Stat()
	return map[string]any{"backend": "postgres", "profile": p.profile, "version": version, "count": count, "data_bytes": dataBytes, "index_bytes": indexBytes, "total_bytes": totalBytes, "indexes": indexes, "settings": settings, "pool": map[string]any{"max": pool.MaxConns(), "total": pool.TotalConns(), "acquired": pool.AcquiredConns(), "acquire_duration_ms": millis(pool.AcquireDuration()), "empty_acquire_count": pool.EmptyAcquireCount()}}, nil
}
