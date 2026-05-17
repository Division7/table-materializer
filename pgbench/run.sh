#!/usr/bin/env bash
# pgbench/run.sh — end-to-end benchmark driver for table_materializer
#
# Runs every workload twice:
#   Phase 1 (baseline)   — no IMMVs, queries hit base tables
#   Phase 2 (with IMMVs) — after table_materializer_force_spawn(), the
#                          post_parse_analyze hook transparently rewrites
#                          matching queries to their IMMV equivalents
#
# Usage:
#   ./pgbench/run.sh                   # uses defaults below
#   DURATION=30 CLIENTS=8 ./pgbench/run.sh
#
# Requires: psql, pgbench, and a running container (docker-compose up -d).

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

DURATION="${DURATION:-60}"   # seconds per workload
CLIENTS="${CLIENTS:-4}"
THREADS="${THREADS:-2}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONN="-h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

WORKLOADS=(workload_joins workload_wide workload_agg workload_mixed)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

run_workload() {
    local script="$1"
    local outfile="$2"
    pgbench $CONN \
        -T "$DURATION" \
        -c "$CLIENTS"  \
        -j "$THREADS"  \
        --no-vacuum    \
        -f "$SCRIPT_DIR/${script}.pgbench" \
        -r 2>&1 | tee "$outfile"
}

# Extract the "latency average = X.XXX ms" line → X.XXX
parse_latency() {
    grep -m1 'latency average' "$1" | awk '{print $4}'
}

# Extract TPS (without initial connection time)
parse_tps() {
    grep 'tps' "$1" | grep -v 'including' | awk '{print $3}'
}

print_separator() {
    printf '%s\n' "$(printf '─%.0s' {1..72})"
}

# ---------------------------------------------------------------------------
echo "==> Initialising schema and test data..."
psql $CONN -f "$SCRIPT_DIR/init.sql"

# ===========================================================================
echo ""
print_separator
echo "  PHASE 1 — BASELINE  (no IMMVs, direct table scans)"
print_separator

declare -A BASELINE_LAT BASELINE_TPS

for script in "${WORKLOADS[@]}"; do
    echo ""
    echo "--- $script (baseline) ---"
    outfile="$TMPDIR/${script}_baseline.txt"
    run_workload "$script" "$outfile"
    BASELINE_LAT[$script]=$(parse_latency "$outfile")
    BASELINE_TPS[$script]=$(parse_tps     "$outfile")
done

# ---------------------------------------------------------------------------
echo ""
print_separator
echo "==> Forcing IMMV creation (table_materializer_force_spawn)..."
print_separator

psql $CONN -c "SELECT table_materializer_force_spawn() AS mvs_created_or_registered;"

echo ""
echo "==> IMMVs now registered:"
psql $CONN -c "
SELECT
    c.relname AS immv,
    pg_size_pretty(pg_total_relation_size(i.immvrelid)) AS size
FROM pgivm.pg_ivm_immv i
JOIN pg_class c ON c.oid = i.immvrelid
ORDER BY c.relname;
"

# Reset stats so Phase 2 numbers are clean
psql $CONN -c "SELECT pg_stat_statements_reset();" > /dev/null

# ===========================================================================
echo ""
print_separator
echo "  PHASE 2 — WITH IMMVs  (queries rewritten by post_parse_analyze hook)"
print_separator

declare -A IMMV_LAT IMMV_TPS

for script in "${WORKLOADS[@]}"; do
    echo ""
    echo "--- $script (with IMMVs) ---"
    outfile="$TMPDIR/${script}_immv.txt"
    run_workload "$script" "$outfile"
    IMMV_LAT[$script]=$(parse_latency "$outfile")
    IMMV_TPS[$script]=$(parse_tps     "$outfile")
done

# ===========================================================================
echo ""
print_separator
printf "  %-24s  %10s  %10s  %10s  %12s  %12s\n" \
    "WORKLOAD" "BASE_LAT" "IMMV_LAT" "DELTA_LAT" "BASE_TPS" "IMMV_TPS"
print_separator

for script in "${WORKLOADS[@]}"; do
    bl="${BASELINE_LAT[$script]:-0}"
    il="${IMMV_LAT[$script]:-0}"
    bt="${BASELINE_TPS[$script]:-0}"
    it="${IMMV_TPS[$script]:-0}"

    # delta_lat = baseline − immv  (positive = IMMV is faster)
    # delta_pct = delta / baseline * 100
    delta_ms=$(awk "BEGIN { printf \"%.3f\", $bl - $il }")
    delta_pct=$(awk "BEGIN {
        if ($bl > 0) printf \"%.1f%%\", (($bl - $il) / $bl) * 100
        else         printf \"n/a\"
    }")

    printf "  %-24s  %8s ms  %8s ms  %+8s ms  %12s  %12s  (%s)\n" \
        "$script" "$bl" "$il" "$delta_ms" "$bt" "$it" "$delta_pct"
done

print_separator
echo "  DELTA_LAT: baseline − immv latency (positive = IMMV is faster)"
print_separator

# ---------------------------------------------------------------------------
echo ""
echo "==> Top expensive queries after IMMV phase:"
psql $CONN -c "
SELECT rank,
       round(mean_exec_time_ms::numeric,2) AS mean_ms,
       calls,
       left(query,80) AS query
FROM top_expensive_queries();
"
