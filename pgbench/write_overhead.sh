#!/usr/bin/env bash
# pgbench/write_overhead.sh — measure the WRITE-side cost of IMMVs.
#
# run.sh shows IMMVs make matching *reads* faster.  This script shows the other
# side of the trade-off: once an IMMV mirrors a table, every INSERT / UPDATE /
# DELETE on that base table also fires pg_ivm's incremental-maintenance triggers,
# so writes get slower.
#
#   Phase 1 (no IMMV)   — drive workload_writes against a pristine `orders`.
#   Phase 2 (with IMMV) — create an IMMV mirroring `orders` (the exact thing the
#                         extension's worker would build — a `SELECT * FROM
#                         orders` IMMV via pgivm.create_immv), then drive the
#                         identical write workload again.
#
# The IMMV is created directly here (rather than via the heuristic / force_spawn)
# so the benchmark deterministically targets the table being written — isolating
# the maintenance overhead as the only variable between the two phases.
#
# Usage:
#   ./pgbench/write_overhead.sh
#   DURATION=30 CLIENTS=8 THREADS=4 ./pgbench/write_overhead.sh
#
# Requires: psql, pgbench, and a running container (docker compose up -d).

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

# Single-client by default: the workload batches 1,500 row changes per
# transaction so the per-row IMMV maintenance cost is measurable without
# concurrent-writer lock contention (pg_ivm serializes view maintenance).
DURATION="${DURATION:-60}"   # seconds per phase
CLIENTS="${CLIENTS:-1}"
THREADS="${THREADS:-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONN="-h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
run_writes() {                       # run_writes <outfile>
    pgbench $CONN \
        -T "$DURATION" \
        -c "$CLIENTS"  \
        -j "$THREADS"  \
        --no-vacuum    \
        -f "$SCRIPT_DIR/workload_writes.pgbench" \
        -r 2>&1 | tee "$1"
}

parse_latency() { grep -m1 'latency average' "$1" | awk '{print $4}'; }
parse_tps()     { grep 'tps' "$1" | grep -v 'including' | awk '{print $3}'; }
psql_val()      { psql $CONN -tAX -c "$1"; }
print_separator() { printf '%s\n' "$(printf '─%.0s' {1..72})"; }

# ---------------------------------------------------------------------------
echo "==> Initialising schema and test data..."
psql $CONN -f "$SCRIPT_DIR/init.sql" >/dev/null
echo "    orders starts with $(psql_val 'SELECT count(*) FROM orders') rows."

# ===========================================================================
echo ""
print_separator
echo "  PHASE 1 — BASELINE WRITES  (no IMMV on orders)"
print_separator
run_writes "$TMPDIR/writes_base.txt"
BASE_LAT=$(parse_latency "$TMPDIR/writes_base.txt")
BASE_TPS=$(parse_tps     "$TMPDIR/writes_base.txt")

# ---------------------------------------------------------------------------
# Reset orders to its pristine 200k-row state so Phase 2 starts from the same
# point Phase 1 did (Phase 1's inserts/deletes drifted the row count).
echo ""
echo "==> Re-initialising schema so Phase 2 starts from the same state..."
psql $CONN -f "$SCRIPT_DIR/init.sql" >/dev/null

echo "==> Creating an IMMV mirroring orders (orders_auto_mv)..."
psql $CONN -c \
  "SELECT pgivm.create_immv('public.orders_auto_mv', 'SELECT * FROM public.orders');" >/dev/null

echo ""
echo "==> IMMV created ($(psql_val "SELECT pg_size_pretty(pg_total_relation_size('public.orders_auto_mv'))"))."
echo "    pg_ivm maintenance triggers now firing on every write to orders:"
psql $CONN -c "
SELECT tgname AS maintenance_trigger
FROM pg_trigger
WHERE tgrelid = 'public.orders'::regclass AND tgname LIKE 'IVM_trigger_%'
ORDER BY tgname;"

# ===========================================================================
echo ""
print_separator
echo "  PHASE 2 — WRITES WITH IMMV  (every write maintains orders_auto_mv)"
print_separator
run_writes "$TMPDIR/writes_immv.txt"
IMMV_LAT=$(parse_latency "$TMPDIR/writes_immv.txt")
IMMV_TPS=$(parse_tps     "$TMPDIR/writes_immv.txt")

# Confirm the IMMV stayed in sync with the base table (incremental maintenance
# kept up with the write stream — no manual REFRESH).
echo ""
echo "==> Post-run row counts (IMMV should equal base table — kept in sync):"
psql $CONN -c "
SELECT (SELECT count(*) FROM orders)          AS orders_rows,
       (SELECT count(*) FROM orders_auto_mv)  AS immv_rows;"

# ===========================================================================
echo ""
print_separator
printf "  %-16s  %12s  %12s\n" "METRIC" "NO_IMMV" "WITH_IMMV"
print_separator
printf "  %-16s  %10s ms  %10s ms\n" "write latency" "$BASE_LAT" "$IMMV_LAT"
printf "  %-16s  %12s  %12s\n"       "write TPS"     "$BASE_TPS" "$IMMV_TPS"
print_separator

# Overhead = how much slower the IMMV phase is.
overhead_pct=$(awk "BEGIN {
    if ($BASE_TPS > 0) printf \"%.1f%%\", (($BASE_TPS - $IMMV_TPS) / $BASE_TPS) * 100
    else               printf \"n/a\"
}")
slowdown=$(awk "BEGIN {
    if ($IMMV_TPS > 0) printf \"%.2fx\", $BASE_TPS / $IMMV_TPS
    else               printf \"n/a\"
}")
echo "  Write throughput dropped ${overhead_pct} with the IMMV present"
echo "  (baseline is ${slowdown} the IMMV-phase TPS) — the cost of keeping"
echo "  orders_auto_mv incrementally maintained on every write."
print_separator

# A non-trivial overhead is the expected, demonstrated result.  Flag the rare
# case where the IMMV phase was not slower (e.g. DURATION too short / noisy run).
faster=$(awk "BEGIN { print ($IMMV_TPS < $BASE_TPS) ? 1 : 0 }")
if [ "$faster" -eq 1 ]; then
    echo "  ✓ As expected, writes are slower when the IMMV exists."
else
    echo "  ! Writes were NOT slower this run — try a longer DURATION or more"
    echo "    CLIENTS; short/noisy runs can mask the maintenance overhead."
fi
print_separator
