# pgbench test suite

End-to-end benchmark for `table_materializer`. Measures query latency and TPS
before and after the extension automatically creates Incrementally Maintained
Materialized Views (IMMVs) via `pg_ivm`.

## Prerequisites

Requires **Docker Compose v2** (the `docker compose` plugin, not the legacy
standalone `docker-compose`).  Docker Desktop 3.3+ and Docker Engine 20.10+
include it.  Verify with `docker compose version`.

## Quick start

### Via Docker Compose (recommended)

From the project root, start the database then run the benchmark in one shot:

```bash
docker compose up -d                                    # start db, wait for healthy
docker compose --profile bench run --rm bench           # run full benchmark
```

The `bench` service is declared under the `bench` profile in
`docker-compose.yml`, so it never starts with a plain `docker compose up`.
The `--profile bench` flag is required to activate it.

Override duration or concurrency inline:

```bash
DURATION=30 CLIENTS=8 THREADS=4 \
  docker compose --profile bench run --rm bench
```

> **Quick smoke-test:** `DURATION=15 CLIENTS=2 THREADS=1` cuts total run time
> to about 2 minutes.

### Directly (without Docker)

If you have `psql` and `pgbench` (PostgreSQL 18 client tools) installed locally
and a PostgreSQL 18 instance with `pg_stat_statements` and `table_materializer`
in `shared_preload_libraries`:

```bash
# From the project root
./pgbench/run.sh
```

The script reads connection parameters from the standard `PG*` environment
variables.  When run via Docker Compose the `bench` container sets these
automatically; when run locally supply them yourself:

| Variable | Docker Compose default | Direct-run default | Notes |
|---|---|---|---|
| `PGHOST` | `db` (container name) | `localhost` | |
| `PGPORT` | `5432` | `5432` | |
| `PGUSER` | `postgres` | `postgres` | |
| `PGDATABASE` | `postgres` | `postgres` | |
| `PGPASSWORD` | `postgres` | `postgres` | |
| `DURATION` | `60` | `60` | seconds per workload phase |
| `CLIENTS` | `4` | `4` | concurrent pgbench clients |
| `THREADS` | `2` | `2` | pgbench worker threads (≤ `CLIENTS`) |

```bash
PGHOST=localhost DURATION=120 CLIENTS=8 THREADS=4 ./pgbench/run.sh
```

## What the script does

1. **Init** — runs `init.sql`: drops and recreates all test tables, lowers
   heuristic thresholds so queries qualify quickly, freezes the background
   worker interval at 5 minutes so it cannot auto-create IMMVs during Phase 1.

2. **Phase 1 — baseline** — runs all four workloads against the raw tables.
   Latency and TPS are captured for comparison.  These runs also populate
   `pg_stat_statements` with the data the heuristic needs.

3. **IMMV creation** — calls `table_materializer_force_spawn()`, which runs the
   heuristic immediately and creates IMMVs for the top-scoring tables.  The
   IMMVs created are printed with their on-disk sizes.

4. **Phase 2 — with IMMVs** — re-runs the same four workloads.  The extension's
   `post_parse_analyze` hook transparently rewrites eligible queries to read
   from the IMMV instead of the source table — no query changes needed.

5. **Comparison table** — prints baseline vs. IMMV latency, the absolute delta
   (ms), and percentage improvement for each workload.

6. **Top expensive queries** — shows the five slowest queries (by mean execution
   time) captured during Phase 2.

## Workloads

| Script | Tables hit | What it stresses |
|---|---|---|
| `workload_joins.pgbench` | customers, orders, order_items, products | 4-way join through 200k and 500k unindexed rows |
| `workload_wide.pgbench` | wide_metrics | Full scan on a 40-column, 100k-row table |
| `workload_agg.pgbench` | order_items, orders, products | Static 3-way aggregation (no parameters — one query accumulates high `total_exec_time`) |
| `workload_mixed.pgbench` | events, wide_metrics | Two aggregations per transaction across two hot tables |

All foreign-key columns (`orders.customer_id`, `order_items.order_id`,
`events.user_id`, `wide_metrics.user_id`) are intentionally left unindexed to
force sequential scans and produce slow queries that exceed the heuristic
thresholds.

## Test schema

| Table | Rows | Purpose |
|---|---|---|
| `customers` | 50,000 | Lookup dimension for join workload |
| `products` | 10,000 | Lookup dimension for join and aggregation |
| `orders` | 200,000 | Fact table; `customer_id` unindexed |
| `order_items` | 500,000 | Largest fact table; `order_id` unindexed |
| `wide_metrics` | 100,000 | 40-column row; stresses I/O bandwidth |
| `events` | 500,000 | High-cardinality stream; `user_id` unindexed |

## Heuristic thresholds

`init.sql` overrides the production defaults to make queries qualify quickly on
fresh test data:

| GUC | Test value | Production default |
|---|---|---|
| `table_materializer.heuristic_min_calls` | 5 | 10 |
| `table_materializer.heuristic_min_exec_time_ms` | 1.0 | 100.0 |
| `table_materializer.interval_ms` | 300000 | 5000 |

The interval is frozen at 5 minutes (the maximum) so the background worker
does not fire during Phase 1 and contaminate the baseline measurement.  Only
`table_materializer_force_spawn()` creates IMMVs during the benchmark.

To restore production defaults after benchmarking:

```sql
ALTER SYSTEM RESET table_materializer.heuristic_min_calls;
ALTER SYSTEM RESET table_materializer.heuristic_min_exec_time_ms;
ALTER SYSTEM RESET table_materializer.interval_ms;
SELECT pg_reload_conf();
```

## Interpreting results

The comparison table printed at the end looks like:

```
──────────────────────────────────────────────────────────────────────────
  WORKLOAD                    BASE_LAT    IMMV_LAT   DELTA_LAT      BASE_TPS      IMMV_TPS
──────────────────────────────────────────────────────────────────────────
  workload_joins              27.6 ms     25.2 ms     2.4 ms    145.1    158.7  (8.6%)
  workload_wide                6.3 ms      6.2 ms     0.1 ms    634.3    648.7  (2.2%)
  workload_agg               760.1 ms    700.3 ms    59.8 ms      5.3      5.7  (7.9%)
  workload_mixed              25.6 ms     23.8 ms     1.8 ms    156.1    168.3  (7.2%)
──────────────────────────────────────────────────────────────────────────
  DELTA_LAT: baseline − immv latency (positive = IMMV is faster)
──────────────────────────────────────────────────────────────────────────
```

**DELTA_LAT** is positive when the IMMV phase is faster.  A near-zero delta
means the query was not rewritten (the source table was not in the top-N
heuristic candidates, or the IMMV covers the same row count with no
pre-filtering advantage).

The IMMVs created are `SELECT * FROM <table>` mirrors — full copies kept
up-to-date by pg_ivm triggers.  Their performance advantage comes from having
no dead tuples, better page density, and a warmer buffer cache by the time
Phase 2 starts.  Pre-aggregating or pre-filtering the IMMV query would yield
larger gains; that is a planned extension to the heuristic.

## Files

```
pgbench/
├── README.md                  this file
├── init.sql                   schema setup, threshold overrides, cleanup
├── run.sh                     full benchmark driver
├── workload_joins.pgbench     4-way join workload
├── workload_wide.pgbench      wide-table scan workload
├── workload_agg.pgbench       aggregation workload (static query)
└── workload_mixed.pgbench     dual-table aggregation workload
```
