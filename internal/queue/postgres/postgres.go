// Package postgres is a work queue in a PostgreSQL table: producers insert rows,
// workers claim them with FOR UPDATE SKIP LOCKED and delete them in the same
// transaction.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Haleralex/pg-mongo-bench/internal/queue"
)

const (
	resetSQL = `
DROP TABLE IF EXISTS queue_jobs;
CREATE TABLE queue_jobs (
	id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	payload bytea NOT NULL
);
SELECT pg_stat_reset_shared('wal');`

	insertSQL = `INSERT INTO queue_jobs (payload) SELECT unnest($1::bytea[])`

	// The inner SELECT skips rows other workers hold, so workers never wait on
	// each other; ORDER BY id keeps the queue roughly FIFO.
	claimSQL = `
DELETE FROM queue_jobs
WHERE id IN (
	SELECT id FROM queue_jobs
	ORDER BY id
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
RETURNING payload`

	statsSQL = `
SELECT
	pg_total_relation_size('queue_jobs'),
	coalesce(s.n_live_tup, 0), coalesce(s.n_dead_tup, 0),
	coalesce(s.autovacuum_count, 0), coalesce(s.n_tup_ins, 0), coalesce(s.n_tup_del, 0),
	w.wal_bytes::bigint, w.wal_sync
FROM pg_stat_wal w
LEFT JOIN pg_stat_user_tables s ON s.relname = 'queue_jobs'`
)

// Config is one PostgreSQL profile.
type Config struct {
	URL string
	// SynchronousCommit "on" waits for the WAL flush on every commit; "off"
	// acknowledges before the flush and may lose the last ~600 ms on a crash.
	SynchronousCommit string
	MaxConns          int32
}

type Backend struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, cfg Config) (*Backend, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MaxConns
	poolCfg.ConnConfig.RuntimeParams["synchronous_commit"] = cfg.SynchronousCommit
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Backend{pool: pool}, nil
}

func (b *Backend) Reset(ctx context.Context, _ int) error {
	_, err := b.pool.Exec(ctx, resetSQL)
	return err
}

func (b *Backend) Producer() queue.Producer { return b }

// Send inserts the batch as one statement and one transaction.
func (b *Backend) Send(ctx context.Context, payloads [][]byte) error {
	_, err := b.pool.Exec(ctx, insertSQL, payloads)
	return err
}

func (b *Backend) NewConsumer(context.Context) (queue.Consumer, error) {
	return consumer{pool: b.pool}, nil
}

func (b *Backend) Blocking() bool { return false }

func (b *Backend) Stats(ctx context.Context) (map[string]any, error) {
	var size, live, dead, autovacuums, inserted, deleted, walBytes, walSyncs int64
	err := b.pool.QueryRow(ctx, statsSQL).Scan(&size, &live, &dead, &autovacuums, &inserted, &deleted, &walBytes, &walSyncs)
	if err != nil {
		return nil, fmt.Errorf("postgres stats: %w", err)
	}
	return map[string]any{
		"table_bytes": size, "live_tuples": live, "dead_tuples": dead, "autovacuum_count": autovacuums,
		"inserted": inserted, "deleted": deleted, "wal_bytes": walBytes, "wal_syncs": walSyncs,
	}, nil
}

func (b *Backend) Close() { b.pool.Close() }

type consumer struct {
	pool *pgxpool.Pool
}

// Receive holds the transaction while handle runs: a worker that dies before
// COMMIT releases its rows to the others.
func (c consumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	if ctx.Err() != nil {
		return 0, nil
	}
	// The claim is short; finishing it after cancellation avoids redelivery.
	ctx = context.WithoutCancel(ctx)
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit
	rows, err := tx.Query(ctx, claimSQL, limit)
	if err != nil {
		return 0, err
	}
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return 0, err
	}
	for _, p := range payloads {
		handle(p)
	}
	return len(payloads), tx.Commit(ctx)
}

func (consumer) Close() {}
