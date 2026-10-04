# PostgreSQL JSONB / MongoDB event benchmark

A k6 benchmark of batched event ingestion while querying historical events.
The same Go HTTP service, generated documents, query windows and resource limits
are used for both databases.

## Run

Requires Docker with Linux containers and Go 1.25. The repository opens in
GitHub Codespaces (`.devcontainer/` requests an 8-core machine with Docker, Go
and make). Shared cloud vCPUs are noisy, so compare repeated runs.

```bash
make test      # vet and unit tests, no Docker
make smoke     # ~2-minute end-to-end check of the three default profiles
make bench     # full experiment, 3 passes, ~25 minutes plus seeding/building
make summary   # results/comparison.csv and a median table
make down      # remove containers and database volumes
```

`make` passes `ARGS` to the runner; `go run ./cmd/bench run -h` lists all flags:

```bash
# Isolate writes or reads. Both reseed each profile.
make bench ARGS="-write-percent 100 -skip-build"
make bench ARGS="-write-percent 0 -skip-build"

# Compare both general JSONB GIN operator classes.
make bench ARGS="-profiles pg_gin_ops,pg_gin_path_ops,mongo_targeted -skip-build"

# Summarize one experiment only.
make summary ARGS="-prefix 20261004-094116"
```

The API is published on 127.0.0.1:18088 (`-port` to change). Databases are not
published to the host. Containers stop after the experiment; use `-keep-running`
to inspect the final profile interactively. Each seed recreates only
`docbench.events_bench` in this dedicated Compose project. Results have
timestamped names and are retained.

## Flexible queries

The default experiment favors B-trees: every read sorts by time with `LIMIT`,
and GIN cannot return rows in order. `make flex` asks the opposite question —
queries on fields nobody indexed for — and compares "index everything" with
"index everything":

| Profile | Index | Strength | Weakness |
| --- | --- | --- | --- |
| `pg_gin_path_ops` | GIN over the whole payload | intersects several conditions (BitmapAnd) | no order: matches are collected, then sorted |
| `mongo_wildcard` | compound wildcard `{tenant_id, payload.$**}` | ordered B-tree entry per field path | one wildcard path per query; other conditions filter fetched documents |
| `pg_targeted`, `mongo_targeted` | indexes for the known queries only | control: what an unplanned query costs | — |

| Read kind | Filter | Matches per tenant (200k seed) |
| --- | --- | --- |
| `adhoc` | `level` + `attrs.region` + `attrs.status`, newest 50 | ~5 (1/360) |
| `trace` | `attrs.trace_id` over the whole seeded range | exactly 1 |
| `adhoc_count` | the `adhoc` filter as a count, no ORDER BY/LIMIT | ~5 |

The wildcard index also stores every message string as a key, while
`jsonb_path_ops` stores hashes; compare `index_bytes` in `*-before.json`.
Use `make flex ARGS="-write-percent 0 -skip-build"` for read cost alone and
`-write-percent 100` for the cost of indexing everything.

## Layout

| Path | Responsibility |
| --- | --- |
| `cmd/api` | Configuration and wiring of the HTTP service for one profile |
| `cmd/bench` | Orchestration: Compose lifecycle, seeding, cross-profile result check, k6, resource sampling, summaries |
| `internal/event` | Deterministic event generator shared by seeding and writes |
| `internal/store` | `Store` contract and `ReadQuery` |
| `internal/store/postgres` | JSONB table and the `pg_*` index profiles with their predicates |
| `internal/store/mongodb` | Collection and the `mongo_*` index profiles |
| `internal/httpapi` | HTTP contract used by k6 and the runner |
| `k6/load.js` | Arrival-rate load, per-phase/per-operation metrics and thresholds |

A profile is data: its indexes and the query predicates that can use them live
together, so adding a strategy means adding one entry to a profile list.

## Workload

- Seed: 200,000 deterministic events, 100 tenants and 20 services.
- Envelope: ID, tenant and millisecond timestamp; payload: service, level,
  nested attributes, tags and a varying message of approximately 700 characters.
- Requests: 70% writes, 10% timeline reads, 10% service/level reads and 10% tag reads.
- Each write contains 10 events. At 600 requests/s the offered rate is about
  4,200 inserted events/s plus 180 reads/s, not 600 events/s.
- Every read filters tenant and time, sorts timestamp/ID descending, and returns
  up to 50 complete events. Attribute reads additionally filter service/level;
  tag reads test membership in the payload tag array.
- Warmup: 20 seconds at 100 requests/s. Measurements: 100, 300 and 600 requests/s,
  each held for 30 seconds, with 10-second transitions between different rates.
- `ramping-arrival-rate` schedules work independently of response time. Failed
  requests and `dropped_iterations` expose overload; a lower achieved rate is not
  reported as successful capacity.

Read windows alternate between the entire seeded range and its latter half.
Concurrent inserts fall outside those windows but maintain the same indexes.
This tests historical search under ingestion, not visibility delay of newly
arriving events or following a live log tail. Seed times are synthetic and span
only 200 seconds at the default size; vary the generator for real retention data.

## Index Configurations

Every configuration has a primary ID index and an index beginning with tenant,
then timestamp descending and ID descending.

| Profile | Additional indexes | Question |
| --- | --- | --- |
| `pg_gin_path_ops` | Full payload GIN with `jsonb_path_ops` | Cost of flexible containment search |
| `pg_gin_ops` | Full payload GIN with `jsonb_ops` | More general JSON operations, optional run |
| `pg_targeted` | Compound B-tree for tenant/service/level/time; expression GIN for tags | Known PostgreSQL access patterns |
| `mongo_targeted` | Compound indexes for tenant/service/level/time and tenant/tag/time | Known MongoDB access patterns |

These are deliberately different indexing strategies. A full-payload GIN indexes
more information than MongoDB's selected compound indexes. Use `pg_targeted`
when comparing known access patterns, and treat GIN's extra write/storage cost as
the price of broader JSON search. MongoDB wildcard indexes, arbitrary new payload
filters and GIN key-existence queries are not covered by this experiment.

GIN does not provide timestamp ordering, so all PostgreSQL profiles also get
a B-tree time index. The planner is free to choose it and filter the payload;
the saved execution plans show whether GIN actually helps a particular query.

## Measurements

`results/` contains k6 summaries, stdout/stderr, seed times, query result IDs,
execution plans, database/index sizes, periodic Docker CPU/memory/I/O samples,
container logs, image IDs/digests and Docker resource information.

Compare each steady phase separately using:

- Per-operation HTTP p50/p95/p99 and `db_ms` p50/p95/p99.
- Actual successful documents/s and attempted/completed requests/s.
- Error rates, invalid responses and dropped iterations.
- Read result counts/empty reads, index sizes and execution plans.
- API and load-generator resource use as well as database resource use.

HTTP duration includes sending, waiting and receiving, but excludes connection
setup/blocked time. `db_ms` is observed in the Go client and includes pool waits,
database work, network transfer and decoding; it is not server execution time.
Event generation occurs outside `db_ms`, but inside the HTTP request duration.
Operations retain the phase in which they started even if they finish later.

Default pass criteria: HTTP/contract errors below 1%, no dropped iterations,
and p95 below 200 ms for each operation in every steady phase. These are an
example latency budget, not a production SLO. k6 exit code 99 means threshold
failure: its measurements are preserved and the remaining profiles still run.

Before loading, the runner compares nine deterministic query result sets between
profiles. After loading, it captures size statistics, runs PostgreSQL ANALYZE and
GIN pending-list cleanup, records the maintenance time, and captures sizes again.
GIN fastupdate and background maintenance remain enabled during the timed load.

## Boundaries

Each database receives 2 CPU cores and 2 GiB of RAM; PG shared_buffers and MongoDB
WiredTiger cache are both 512 MiB. The API and k6 each receive 2 CPU cores. Only
one benchmark database is running at a time. Docker Desktop and the host page
cache still affect both measurements. The default dataset fits in memory and
the short run is exploratory; it does not establish disk-bound or sustained
production capacity. Check resource samples for swapping or client saturation.

PostgreSQL uses logged writes with synchronous_commit/fsync enabled. MongoDB
uses `w:1, j:true` on a standalone server. Both acknowledge local durable writes,
but this does not compare replicated durability or failover. PostgreSQL COPY
batches are atomic; MongoDB ordered InsertMany is atomic per document and can
partially succeed on error. Unique ID ranges avoid retries/duplicates in the
normal workload. Do not infer equivalent transaction semantics from throughput.

Not measured: replication, sharding, TTL/retention deletion, table partitioning,
compression alternatives, aggregation pipelines, updates, joins or cold-cache
reads. Time-series collections and PostgreSQL extensions are separate candidates.
Select an engine from your required semantics plus representative measurements,
not from a single fastest row in a synthetic test.

## References

- [PostgreSQL JSONB indexing](https://www.postgresql.org/docs/17/datatype-json.html#JSON-INDEXING)
- [PostgreSQL indexes and ordering](https://www.postgresql.org/docs/17/indexes-ordering.html)
- [GIN maintenance](https://www.postgresql.org/docs/17/gin-implementation.html)
- [MongoDB compound indexes](https://www.mongodb.com/docs/manual/core/indexes/index-types/index-compound/)
- [MongoDB write concern](https://www.mongodb.com/docs/manual/reference/write-concern/)
- [k6 arrival-rate executor](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/ramping-arrival-rate/)
