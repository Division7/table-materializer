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
