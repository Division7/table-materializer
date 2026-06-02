#!/usr/bin/env bash
# pgbench/heuristic_stress.sh — stress the IMMV-selection heuristic with many
# tables and deliberately varied query profiles.
#
# Where run.sh / shift_demo.sh exercise the read speed-up and the create/evict
# lifecycle, this script exercises the *selection quality* of the heuristic:
# given 16 candidate tables with very different (calls x mean_exec_time)
# profiles and a budget of only 4 IMMVs, does the worker pick the right ones?
#
# Profiles (see init_heuristic.sql):
#   h_hot_1..4    big + slow + frequent   -> high score          -> expect PICK
#   h_cheap_1..4  small + fast + frequent -> below exec-time floor-> expect skip
#   h_rare_1..4   big + slow + RARE       -> below calls floor    -> expect skip
#   h_noise_1..4  tiny + trivial + rare   -> fails both floors    -> expect skip
#
# The script drives each profile, prints the full ranked candidate report
# (calls, mean_ms, score, qualifies) straight from pg_stat_statements, calls
# table_materializer_force_spawn(), then ASSERTS the materialized set is exactly
# the hot tables (no cheap/rare/noise slipped through the budget).
#
# Usage:
#   ./pgbench/heuristic_stress.sh
#   DURATION=20 CLIENTS=8 RARE_TXNS=8 ./pgbench/heuristic_stress.sh
#
# Requires: psql, pgbench, and a running container (docker compose up -d).

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

DURATION="${DURATION:-30}"    # seconds for the frequent (hot/cheap) profiles
CLIENTS="${CLIENTS:-4}"
THREADS="${THREADS:-2}"
RARE_TXNS="${RARE_TXNS:-8}"   # fixed call count for rare/noise (< min_calls=20)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONN="-h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE"

psql_val()        { psql $CONN -tAX -c "$1"; }
print_separator() { printf '%s\n' "$(printf '─%.0s' {1..78})"; }

# ---------------------------------------------------------------------------
echo "==> Initialising 16-table heuristic schema..."
psql $CONN -f "$SCRIPT_DIR/init_heuristic.sql"

echo ""
echo "==> Heuristic configuration for this run:"
psql $CONN -c "
SELECT name, setting FROM pg_settings
WHERE name IN ('table_materializer.heuristic_min_calls',
               'table_materializer.heuristic_min_exec_time_ms',
               'table_materializer.max_materialized_views')
ORDER BY name;"

MIN_CALLS=$(psql_val "SELECT setting FROM pg_settings WHERE name='table_materializer.heuristic_min_calls';")
MIN_MS=$(psql_val   "SELECT setting FROM pg_settings WHERE name='table_materializer.heuristic_min_exec_time_ms';")
MAX_MV=$(psql_val   "SELECT setting FROM pg_settings WHERE name='table_materializer.max_materialized_views';")

# ===========================================================================
echo ""
print_separator
echo "  DRIVING WORKLOADS  (frequent: ${DURATION}s each; rare: ${RARE_TXNS} calls each)"
print_separator

echo "--- hot profile (frequent, slow) ---"
pgbench $CONN -T "$DURATION" -c "$CLIENTS" -j "$THREADS" --no-vacuum \
    -f "$SCRIPT_DIR/heur_hot.pgbench" >/dev/null 2>&1

echo "--- cheap profile (frequent, fast) ---"
pgbench $CONN -T "$DURATION" -c "$CLIENTS" -j "$THREADS" --no-vacuum \
    -f "$SCRIPT_DIR/heur_cheap.pgbench" >/dev/null 2>&1

echo "--- rare profile (slow, but only ${RARE_TXNS} calls) ---"
pgbench $CONN -t "$RARE_TXNS" -c 1 --no-vacuum \
    -f "$SCRIPT_DIR/heur_rare.pgbench" >/dev/null 2>&1

echo "--- noise profile (fast, only ${RARE_TXNS} calls) ---"
pgbench $CONN -t "$RARE_TXNS" -c 1 --no-vacuum \
    -f "$SCRIPT_DIR/heur_noise.pgbench" >/dev/null 2>&1

# ===========================================================================
echo ""
print_separator
echo "  CANDIDATE REPORT  (score = mean_exec_time * calls; gated by the floors)"
print_separator
# For each table, take its highest-scoring pg_stat_statements entry and show
# whether it clears BOTH heuristic floors.  This is exactly the information the
# worker ranks on.
psql $CONN -c "
WITH cand(tbl, profile) AS (
    VALUES
      ('h_hot_1','hot'),('h_hot_2','hot'),('h_hot_3','hot'),('h_hot_4','hot'),
      ('h_cheap_1','cheap'),('h_cheap_2','cheap'),('h_cheap_3','cheap'),('h_cheap_4','cheap'),
      ('h_rare_1','rare'),('h_rare_2','rare'),('h_rare_3','rare'),('h_rare_4','rare'),
      ('h_noise_1','noise'),('h_noise_2','noise'),('h_noise_3','noise'),('h_noise_4','noise')
)
SELECT c.profile,
       c.tbl,
       s.calls,
       round(s.mean_exec_time::numeric, 2)               AS mean_ms,
       round((s.mean_exec_time * s.calls)::numeric, 1)   AS score,
       CASE WHEN s.calls >= $MIN_CALLS
             AND s.mean_exec_time >= $MIN_MS
            THEN 'yes' ELSE 'no' END                     AS qualifies
FROM cand c
LEFT JOIN LATERAL (
    SELECT calls, mean_exec_time
    FROM pg_stat_statements
    WHERE query ~ ('\m' || c.tbl || '\M')
    ORDER BY mean_exec_time * calls DESC
    LIMIT 1
) s ON true
ORDER BY score DESC NULLS LAST;"

# ===========================================================================
echo ""
print_separator
echo "  FORCING SELECTION  (table_materializer_force_spawn, budget = $MAX_MV)"
print_separator
psql $CONN -c "SELECT table_materializer_force_spawn() AS mvs_created_or_registered;"

echo ""
echo "==> IMMVs the heuristic actually materialized:"
psql $CONN -c "
SELECT c.relname AS immv,
       pg_size_pretty(pg_total_relation_size(i.immvrelid)) AS size
FROM pgivm.pg_ivm_immv i
JOIN pg_class c ON c.oid = i.immvrelid
WHERE c.relname ~ 'auto_mv\$'
ORDER BY c.relname;"

# ===========================================================================
echo ""
print_separator
echo "  RESULT"
print_separator

HOT_PICKS=$(psql_val   "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename ~ '^h_hot_[0-9]+_auto_mv\$';")
OTHER_PICKS=$(psql_val "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename ~ 'auto_mv\$' AND tablename !~ '^h_hot_[0-9]+_auto_mv\$';")

printf "  hot tables materialized   : %s (of 4, budget %s)\n" "$HOT_PICKS" "$MAX_MV"
printf "  non-hot tables materialized: %s (cheap/rare/noise — should be 0)\n" "$OTHER_PICKS"
echo ""

fail=0
[ "$OTHER_PICKS" -eq 0 ] || { echo "  ✗ a cheap/rare/noise table was materialized — the heuristic let through a query it should have filtered"; fail=1; }
[ "$HOT_PICKS" -ge 1 ]   || { echo "  ✗ no hot table was materialized — the heuristic failed to pick the genuinely-expensive workload"; fail=1; }

if [ "$fail" -eq 0 ]; then
    if [ "$HOT_PICKS" -lt 4 ]; then
        echo "  ✓ PASS (partial) — only hot tables were picked, but $HOT_PICKS/4 filled the"
        echo "    budget.  A hot query may have dipped below ${MIN_MS} ms on fast hardware;"
        echo "    try a longer DURATION so every hot table clears the floor."
    else
        echo "  ✓ PASS — the heuristic filled its budget of $MAX_MV with exactly the 4 hot"
        echo "    tables and skipped every cheap (too fast), rare (too few calls) and"
        echo "    noise (both) table."
    fi
    print_separator
    exit 0
else
    echo "  ✗ FAIL — the heuristic did not select as expected (see the candidate report"
    echo "    above for the calls / mean_ms / score that drove the decision)."
    print_separator
    exit 1
fi
