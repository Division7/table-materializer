## Requirements

- **Docker** with the **Compose v2 plugin** (`docker compose`, not the legacy
  standalone `docker-compose`).  Docker Desktop 3.3+ and Docker Engine 20.10+
  both include it.  Verify with `docker compose version`.

## Getting Started

Build and start the database container:

```bash
docker compose build && docker compose up -d
```

Once the container is healthy, open a `psql` shell:

```bash
docker exec -it database psql --user postgres
```

Load the extension (run once per database):

```psql
CREATE EXTENSION table_materializer;
```

Verify it loaded:

```psql
-- Returns 0 on a fresh database (no expensive queries yet).
SELECT table_materializer_force_spawn();
```

Stop the stack:

```bash
docker compose down
```

Wipe all data (volumes):

```bash
docker compose down -v
```

---

## Running the Benchmark

The benchmark measures query latency and TPS **before and after** the extension
automatically creates Incrementally Maintained Materialized Views (IMMVs).

### Prerequisites

The `db` container must be running and healthy before starting the benchmark:

```bash
# Start (or confirm) the database
docker compose up -d

# Optional: watch until healthy
docker compose ps
```

### One-command run

```bash
docker compose --profile bench run --rm bench
```

Docker Compose starts a second container that:

1. Initialises the schema and test data (`pgbench/init.sql`).
2. Runs **Phase 1** — four workloads against bare tables (baseline).
3. Calls `table_materializer_force_spawn()` to create IMMVs immediately.
4. Runs **Phase 2** — the same workloads; the extension's query hook
   transparently rewrites eligible queries to read from their IMMV.
5. Prints a side-by-side latency and TPS comparison.

Total wall time at the defaults: **≈ 8–10 minutes**
(4 workloads × 2 phases × 60 s DURATION, plus schema init).

### Tuning parameters

Override any variable inline — no `.env` file required:

```bash
DURATION=30 CLIENTS=8 THREADS=4 docker compose --profile bench run --rm bench
```

| Variable     | Default | Notes                                 |
|--------------|---------|---------------------------------------|
| `DURATION`   | `60`    | Seconds per workload per phase        |
| `CLIENTS`    | `4`     | Concurrent pgbench clients            |
| `THREADS`    | `2`     | pgbench worker threads (≤ `CLIENTS`)  |
| `PGHOST`     | `db`    | Set automatically by Compose          |
| `PGPORT`     | `5432`  | Set automatically by Compose          |
| `PGUSER`     | `postgres` |                                    |
| `PGDATABASE` | `postgres` |                                    |
| `PGPASSWORD` | `postgres` |                                    |

> **Tip:** Use `DURATION=15 CLIENTS=2` for a quick smoke-test (~2 minutes total).

### Running the benchmark script directly (without Docker)

If you have `psql` and `pgbench` installed locally and a PostgreSQL 18 instance
running with `pg_stat_statements` and `table_materializer` in
`shared_preload_libraries`:

```bash
PGHOST=localhost PGPORT=5432 ./pgbench/run.sh
```

The script auto-detects all connection parameters from the standard `PG*`
environment variables. See [`pgbench/README.md`](pgbench/README.md) for the
full workload description and how to interpret the results.

### Re-running the benchmark

The schema init step in `run.sh` is idempotent — it drops and recreates all
test tables, removes any existing `*_auto_mv` IMMVs, and resets
`pg_stat_statements`.  Simply re-run the same `docker compose` command:

```bash
docker compose --profile bench run --rm bench
```

### Cleaning up after the benchmark

To restore the default GUC values changed by `init.sql`, connect to the
database and run:

```sql
ALTER SYSTEM RESET table_materializer.heuristic_min_calls;
ALTER SYSTEM RESET table_materializer.heuristic_min_exec_time_ms;
ALTER SYSTEM RESET table_materializer.interval_ms;
SELECT pg_reload_conf();
```

Or just tear down the whole stack:

```bash
docker compose down -v   # -v also removes the postgres-data volume
```

---

## Demonstrating a shifting workload

The benchmark above shows IMMVs being *created*. The extension can also let the
selected set **track a changing workload over time** — dropping IMMVs for tables
that have gone cold and creating them for newly-hot tables — so the materialized
set follows where the queries actually are.

This is driven by a per-table exponentially-weighted moving average (EWMA) of
recent query **volume** (call rate): tables that stop being queried decay toward
zero and their IMMVs are evicted, while newly-hot tables are materialized in
their place. Volume is used rather than execution time on purpose — once a table
is materialized its queries get faster, so an exec-time score would collapse and
wrongly evict the very IMMV that sped it up.

### One-command demo

```bash
docker compose up -d                                    # start db
docker compose --profile shift run --rm shift           # run the shift demo
```

The `shift` service runs the **live** background worker on a short interval and:

1. **Phase A** — hammers a 4-way join over `{customers, orders, order_items,
   products}`; the worker materializes a pre-joined IMMV (plus a mirror of the
   join root). It then prints **Snapshot 1**.
2. **Phase B** — stops touching those tables and instead hammers `{events,
   wide_metrics}`. The worker materializes IMMVs for the new hot tables, and the
   now-idle Phase-A IMMVs decay and are **dropped**. It prints **Snapshot 2**.
3. **Asserts** the selected set shifted from the A-tables to the B-tables and
   exits non-zero if it did not (prints `PASS`/`FAIL`).

Tune it inline (defaults shown):

```bash
DURATION=60 CLIENTS=8 INTERVAL_MS=2000 MAX_MV=5 \
  docker compose --profile shift run --rm shift
```

`MAX_MV=1` forces the budget so a Phase-B table must **displace** the Phase-A
IMMV (exercising the displacement path as well as the decay path).

> Run on a fresh database for the cleanest result. The in-memory MV registry
> lives in shared memory, so to re-run from a pristine state without leftover
> state from a previous demo, `docker compose restart db` first.

Inspect the live registry at any time, including each entry's recent-activity
score and how many consecutive cold ticks it has accrued:

```sql
SELECT * FROM table_materializer_list_mvs();
```

### Tuning the shift behavior

These GUCs (all `SIGHUP`-reloadable) control how aggressively the set shifts:

| GUC | Default | Notes |
|-----|---------|-------|
| `table_materializer.score_decay_alpha`  | `0.5` | EWMA weight on the newest per-tick activity delta. Higher reacts faster to shifts (shorter memory); lower is smoother. |
| `table_materializer.evict_grace_ticks`  | `3`   | Consecutive cold ticks before a cold IMMV is dropped (hysteresis against bursty workloads). |
| `table_materializer.evict_score_frac`   | `0.2` | "Cold" = current score below this fraction of the IMMV's peak score. |
| `table_materializer.max_materialized_views` | `5` | Budget cap; a hotter table can displace the weakest incumbent when full. |

---

## The write-side cost of IMMVs

The benchmarks above measure the *read* speed-up. IMMVs are not free: pg_ivm
installs maintenance triggers on the base table, so every `INSERT` / `UPDATE` /
`DELETE` also updates the view. `write_overhead.sh` quantifies that cost — it
runs a batched write workload against `orders` with and without a mirroring
IMMV and reports the slowdown (typically ~3–4× lower write throughput).

```bash
docker compose up -d
docker compose --profile writes run --rm writes
```

## Stress-testing the selection heuristic

`heuristic_stress.sh` checks that the heuristic picks the *right* tables. It
builds **16 tables across four workload profiles** (frequent+slow, frequent+fast,
slow+rare, fast+rare), drives each one, prints the per-table
`(calls, mean_ms, score, qualifies)` report straight from `pg_stat_statements`,
then asserts the worker materialized exactly the 4 genuinely-hot tables and
skipped the cheap, rare, and noise tables. A good regression target when tuning
`HEURISTIC_SCORE_EXPR` or the threshold GUCs.

```bash
docker compose up -d
docker compose --profile heuristic run --rm heuristic
```

See [`pgbench/README.md`](pgbench/README.md) for the full details, env knobs, and
sample output of both benchmarks.
