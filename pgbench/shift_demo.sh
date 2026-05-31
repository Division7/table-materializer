#!/usr/bin/env bash
# pgbench/shift_demo.sh — demonstrate that table_materializer's selected IMMVs
# shift as the workload changes.
#
# Unlike run.sh (which freezes the background worker and drives creation by hand
# for a deterministic latency benchmark), this script runs the LIVE worker on a
# short interval and lets it create AND evict IMMVs on its own while the
# workload changes underneath it:
#
#   Phase A  — hammer a 4-way join over {customers, orders, order_items,
#              products}.  The worker materializes a pre-joined IMMV (and a
#              mirror of the join root).            => Snapshot 1
#   Phase B  — stop touching those tables and instead hammer {events,
#              wide_metrics}.  The worker materializes IMMVs for the new hot
#              tables, and the now-cold Phase-A IMMVs decay and are dropped.
#                                                   => Snapshot 2
#
# The script ASSERTS the set shifted from the A-tables to the B-tables and exits
# non-zero if it did not.
#
# Usage:
#   ./pgbench/shift_demo.sh
#   DURATION=60 CLIENTS=8 MAX_MV=1 ./pgbench/shift_demo.sh
#
# Requires: psql, pgbench, and a running container (docker compose up -d).

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

DURATION="${DURATION:-45}"        # seconds per phase
CLIENTS="${CLIENTS:-4}"
THREADS="${THREADS:-2}"
INTERVAL_MS="${INTERVAL_MS:-2000}" # live worker tick during the demo
MAX_MV="${MAX_MV:-5}"             # max_materialized_views budget
POLL_TIMEOUT="${POLL_TIMEOUT:-40}" # seconds to wait for the worker to react

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONN="-h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
psql_val() { psql $CONN -tAX -c "$1"; }

run_phase() {                 # run_phase <seconds> <pgbench -f args...>
    local secs="$1"; shift
    pgbench $CONN -T "$secs" -c "$CLIENTS" -j "$THREADS" --no-vacuum "$@" \
        >/dev/null 2>&1 || true
}

# Count auto-created IMMVs (physical tables) whose name names a Phase-A / B table
count_a() { psql_val "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename ~ 'auto_mv\$' AND tablename ~ '(customers|orders|order_items|products)';"; }
count_b() { psql_val "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename ~ 'auto_mv\$' AND tablename ~ '(events|wide_metrics)';"; }
count_all() { psql_val "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename ~ 'auto_mv\$';"; }

print_separator() { printf '%s\n' "$(printf '─%.0s' {1..74})"; }

# Poll `cond_cmd` (a shell command returning 0 when satisfied) up to POLL_TIMEOUT
# seconds.  Returns 0 as soon as it is satisfied, 1 on timeout.
wait_until() {
    local desc="$1"; shift
    local waited=0
    while ! "$@"; do
        if [ "$waited" -ge "$POLL_TIMEOUT" ]; then
            echo "    (timed out after ${POLL_TIMEOUT}s waiting for: $desc)"
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    return 0
}

cond_a_present()  { [ "$(count_a)" -ge 1 ]; }
cond_slate_clean(){ [ "$(count_all)" -eq 0 ]; }
cond_shifted()    { [ "$(count_a)" -eq 0 ] && [ "$(count_b)" -ge 1 ]; }

snapshot() {                  # snapshot <title>
    echo ""
    echo "  $1"
    print_separator
    echo "  Physical IMMVs (pg_ivm):"
    psql $CONN -c "
SELECT c.relname AS immv,
       pg_size_pretty(pg_total_relation_size(i.immvrelid)) AS size
FROM pgivm.pg_ivm_immv i
JOIN pg_class c ON c.oid = i.immvrelid
ORDER BY c.relname;" || true
    echo "  Registry (table_materializer_list_mvs):"
    psql $CONN -c "
SELECT mv_name, num_source_tables, round(score::numeric, 1) AS score, cold_ticks
FROM table_materializer_list_mvs()
ORDER BY mv_name;" || true
    echo "  A-table IMMVs: $(count_a)    B-table IMMVs: $(count_b)"
    print_separator
}

# ---------------------------------------------------------------------------
echo "==> Initialising schema and test data..."
psql $CONN -f "$SCRIPT_DIR/init.sql" >/dev/null

echo "==> Configuring the live worker for the demo..."
psql $CONN -c "ALTER SYSTEM SET table_materializer.interval_ms = $INTERVAL_MS;" >/dev/null
psql $CONN -c "ALTER SYSTEM SET table_materializer.max_materialized_views = $MAX_MV;" >/dev/null
psql $CONN -c "SELECT pg_reload_conf();" >/dev/null
psql $CONN -c "
SELECT name, setting FROM pg_settings
WHERE name LIKE 'table_materializer.%'
ORDER BY name;"

# Clear any leftover registry state from a previous demo run on the same DB:
# init.sql already dropped the IMMV tables; give the live worker a few ticks to
# evict the now-stale registry entries so Phase A starts from a clean slate.
echo ""
echo "==> Waiting for a clean slate (no auto IMMVs)..."
if wait_until "all auto IMMVs cleared" cond_slate_clean; then
    echo "    clean."
else
    echo "    WARNING: stale IMMVs remain; results may be noisy. For a pristine"
    echo "    run, 'docker compose restart db' first."
fi

# ===========================================================================
echo ""
print_separator
echo "  PHASE A — workload over {customers, orders, order_items, products}"
print_separator
run_phase "$DURATION" -f "$SCRIPT_DIR/shift_a_join.pgbench"
wait_until "an A-table IMMV to be created" cond_a_present || true
snapshot "SNAPSHOT 1 — after Phase A"
A1="$(count_a)"; B1="$(count_b)"

# ===========================================================================
echo ""
print_separator
echo "  PHASE B — workload shifts to {events, wide_metrics}"
print_separator
echo "  (A-tables now idle; their IMMVs should decay and be evicted while the"
echo "   worker materializes the newly-hot B-tables)"
run_phase "$DURATION" \
    -f "$SCRIPT_DIR/shift_b_events.pgbench" \
    -f "$SCRIPT_DIR/shift_b_wide.pgbench"
wait_until "the IMMV set to shift (A dropped, B present)" cond_shifted || true
snapshot "SNAPSHOT 2 — after Phase B"
A2="$(count_a)"; B2="$(count_b)"

# ===========================================================================
echo ""
print_separator
echo "  RESULT"
print_separator
printf "  Phase A: %s A-IMMV(s), %s B-IMMV(s)\n" "$A1" "$B1"
printf "  Phase B: %s A-IMMV(s), %s B-IMMV(s)\n" "$A2" "$B2"
echo ""

fail=0
[ "$A1" -ge 1 ] || { echo "  ✗ expected ≥1 A-IMMV after Phase A (got $A1)"; fail=1; }
[ "$B1" -eq 0 ] || { echo "  ✗ expected 0 B-IMMVs after Phase A (got $B1)"; fail=1; }
[ "$B2" -ge 1 ] || { echo "  ✗ expected ≥1 B-IMMV after Phase B (got $B2)"; fail=1; }
[ "$A2" -eq 0 ] || { echo "  ✗ expected the A-IMMVs to be evicted after Phase B (got $A2)"; fail=1; }

if [ "$fail" -eq 0 ]; then
    echo "  ✓ PASS — the selected IMMV set shifted from the A-tables to the B-tables."
    print_separator
    exit 0
else
    echo "  ✗ FAIL — the IMMV set did not shift as expected."
    echo "    Try a longer DURATION (more worker ticks per phase) or check the"
    echo "    worker log: docker compose logs db | grep table_materializer"
    print_separator
    exit 1
fi
