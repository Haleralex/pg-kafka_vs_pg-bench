# PostgreSQL SKIP LOCKED / Kafka queue benchmark

How far does a work queue in a PostgreSQL table go before Kafka is worth it?
The same Go load generator drives both brokers with the same message size,
batch size, worker count, acknowledgement semantics and container limits.

| Profile | Broker | Acknowledged when |
| --- | --- | --- |
| `pg_sync` | PostgreSQL 17, `FOR UPDATE SKIP LOCKED` | WAL is fsynced (`synchronous_commit=on`, the default) |
| `pg_async` | same table | commit is in memory (`synchronous_commit=off`) |
| `kafka` | Kafka 4.1, one broker, KRaft | written to the page cache (`acks=all`, Kafka default) |
| `kafka_fsync` | same broker | log is fsynced (`flush.messages=1` on the topic) |

Profiles come in durability pairs. Kafka's default does not fsync and relies on
replicas instead; with a single broker that is weaker than `pg_sync`, so compare
`pg_sync` with `kafka_fsync` and `pg_async` with `kafka`.

## Run

Requires Docker with Linux containers and Go 1.25. The repository opens in
GitHub Codespaces (`.devcontainer/` requests an 8-core machine). Shared cloud
vCPUs and Docker Desktop disks are noisy, so compare repeated runs.

```bash
make test      # vet and unit tests, no Docker
make smoke     # few-minute end-to-end check of every profile
make bench     # full experiment, 3 passes, ~45 minutes plus image pulls
make summary   # results/comparison.csv and median tables
make report    # results/report.html with charts, served on port 8090
make down      # remove containers, volumes and the Prometheus history
```

`make` passes `ARGS` to the runner; `go run ./cmd/bench run -h` lists all flags:

```bash
make bench ARGS="-profiles pg_sync,kafka_fsync -skip-build"
make bench ARGS="-consumers 16 -producers 8"        # Kafka gets 16 partitions
make bench ARGS="-payload 4096 -rates 1000,5000"
make bench ARGS="-kafka-linger 10ms"                # franz-go's default batching
make summary ARGS="-prefix 20261004-094116"   # default: the latest experiment
make bench ARGS="-monitor=false"               # without Prometheus and Grafana
```

Each profile starts from empty broker storage, and only its broker runs.
Results have timestamped names and are retained.

## Watching

**Live.** `make bench` and `make smoke` start Prometheus and Grafana and print
the dashboard address. In Codespaces open the forwarded port 3000 (the
*Ports* tab, or the printed `https://<codespace>-3000.app.github.dev` link); no
login. The dashboard refreshes every 2 seconds and shows:

| Panel | Source | What to look for |
| --- | --- | --- |
| profile, phase, target rate | loadgen | where the run is |
| sent / consumed / target per second | loadgen | consumed below target: saturation |
| backlog | loadgen | growing backlog: workers fall behind |
| e2e p50/p99, send/receive p99 | loadgen histograms | latency jumps at saturation |
| CPU and memory per container | `docker stats` via cmd/bench | loadgen near 400% means the generator, not the broker, is the limit |
| live/dead tuples, autovacuum, WAL/s | PostgreSQL statistics | dead tuples after drain slow down claims |
| lag and log size per partition | Kafka admin API | uneven lag: partition skew |

Grafana keeps running after the experiment with its history (7 days);
`make monitor` starts it again later, `make down` removes it. Histogram
percentiles in Grafana are rounded to buckets; the report has exact values.

**After the run.** `make report` writes `results/report.html` for the latest
experiment and serves it on port 8090: e2e latency against rate per profile,
throughput against target, fill/drain, bytes on disk per message, the
durability pairs side by side, CPU timelines and the median tables. Add
`ARGS="-prefix <stamp>"` for an older experiment.

## What is measured

Every run goes through four phases; one message is 256 bytes by default.

| Phase | Load | Result |
| --- | --- | --- |
| prime | a few batches through every worker | not reported; Kafka consumers join their group here |
| fill | producers only, back to back, until `-backlog` messages | produce msg/s, Send p99 |
| drain | workers only, until the backlog is empty | consume msg/s, Receive p99 |
| steady | producers on a fixed schedule per `-rates` step, workers running | end-to-end p50/p99/p99.9, backlog |

- **End-to-end latency** runs from the *scheduled* send time to the moment a
  worker handles the message. A producer that falls behind does not hide the
  delay (no coordinated omission); `send_lag` in the report shows how late it was.
- **Saturation**: a step whose backlog does not drain within `-drain-timeout`
  ends the list; higher rates are skipped.
- **Correctness**: every message carries a sequence number. A run fails if an
  acknowledged message is never consumed; redeliveries are counted as duplicates.
- **Disk B/msg**: WAL bytes written for PostgreSQL (insert + delete + vacuum),
  retained log size for Kafka.

Semantics are matched where they matter for cost:

| | PostgreSQL | Kafka |
| --- | --- | --- |
| send a batch | one `INSERT ... SELECT unnest($1)` | one `ProduceSync`, linger 0, no compression |
| claim a batch | `DELETE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT n) RETURNING` | `PollRecords(n)` |
| acknowledge | `COMMIT` of the same transaction | synchronous offset commit |
| a worker dies mid-batch | rows unlock, another worker takes them | uncommitted records are redelivered after rebalance |
| empty queue | sleep `-idle` (5 ms) and poll again | long poll on the broker |

Not compared: ordering (Kafka orders per partition, SKIP LOCKED only roughly),
replication, retention/replay, and `LISTEN/NOTIFY` wakeups, which would remove
PostgreSQL's idle-poll delay at low rates.

## What to look at

- `pg_sync` vs `kafka_fsync` in fill: group commit lets PostgreSQL share one
  fsync among concurrent producers; `flush.messages=1` fsyncs per batch.
- drain: PostgreSQL deletes leave dead tuples; `dead_tuples` and
  `autovacuum_count` in the report's `backend_stats` show how vacuum keeps up.
- steady at low rates: PostgreSQL p50 includes up to `-idle` of polling delay.
- steady at high rates: where each profile saturates.

## Layout

| Path | Responsibility |
| --- | --- |
| `cmd/bench` | Host side: Compose lifecycle, resource sampling, summaries |
| `cmd/loadgen` | Runs inside Compose against one profile and writes `*-report.json` |
| `internal/loadgen` | Phases, open-loop scheduler, sequence tracking, percentiles |
| `internal/queue` | `Backend`/`Producer`/`Consumer` contract and message layout |
| `internal/queue/postgres` | Table queue with `SKIP LOCKED` |
| `internal/queue/kafka` | Topic and consumer group via franz-go |
| `internal/profile` | Profile list and which compose service each needs |
| `internal/metrics` | Prometheus metrics of loadgen and of container stats |
| `monitoring/` | Prometheus scrape config, Grafana data source and dashboard |
| `cmd/bench/report.html.tmpl` | Charts of `make report` (Chart.js) |
