-- pgbench/init_heuristic.sql
-- Schema for the heuristic-selection stress test (heuristic_stress.sh).
--
-- Creates a LARGE set of tables (16) spanning four deliberately different
-- workload profiles, so we can watch which ones the heuristic picks.  The score
-- the worker ranks by is  mean_exec_time * calls  (HEURISTIC_SCORE_EXPR in
-- table_materializer.c), gated by two thresholds:
--     heuristic_min_calls          (set to 20 below)
--     heuristic_min_exec_time_ms   (set to 5.0 below)
--
--   h_hot_1..4    big + slow query + queried FREQUENTLY   -> high score   -> PICK
--   h_cheap_1..4  small + FAST query + queried frequently -> < min_exec   -> skip
--   h_rare_1..4   big + slow query but queried RARELY     -> < min_calls  -> skip
--   h_noise_1..4  tiny + trivial query + queried rarely   -> fails both   -> skip
--
-- This exercises both heuristic filters (calls floor, exec-time floor) AND the
-- ranking + the max_materialized_views budget in one run.

-- Quiet the "drop cascades to .../does not exist, skipping" chatter from the
-- cleanup block below (dropping a base table cascade-drops its IMMV).
SET client_min_messages = warning;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_ivm;
CREATE EXTENSION IF NOT EXISTS table_materializer;

-- Heuristic thresholds chosen so each category lands cleanly on one side:
--   hot   : many calls (> 20), slow mean (> 5 ms)  -> qualifies
--   cheap : many calls,        fast mean (< 5 ms)  -> filtered by exec-time floor
--   rare  : few calls (< 20),  slow mean           -> filtered by calls floor
--   noise : few calls,         fast mean           -> filtered by both
ALTER SYSTEM SET table_materializer.heuristic_min_calls = 20;
ALTER SYSTEM SET table_materializer.heuristic_min_exec_time_ms = 5.0;
-- Budget: only the top 4 candidates may be materialised.  There are exactly 4
-- "hot" tables, so a correct heuristic fills the budget with them and nothing else.
ALTER SYSTEM SET table_materializer.max_materialized_views = 4;
-- Freeze the BGW so it cannot create IMMVs mid-run; the script drives creation
-- explicitly via table_materializer_force_spawn().
ALTER SYSTEM SET table_materializer.interval_ms = 300000;
SELECT pg_reload_conf();

-- Clean slate: drop the test tables and any auto-created IMMVs from prior runs.
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables
             WHERE schemaname = 'public'
               AND (tablename ~ '^h_(hot|cheap|rare|noise)_[0-9]+$'
                    OR tablename LIKE '%_auto_mv')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename);
    END LOOP;
END $$;

-- Remove stale pg_ivm catalog rows whose backing table is gone.
DELETE FROM pgivm.pg_ivm_immv
WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = immvrelid);

-- ----------------------------------------------------------------
-- hot + rare share the same shape: 300k rows, no index on `grp`, so the
-- category aggregate query (GROUP BY grp) is a slow seq scan (> 5 ms).  They
-- differ ONLY in how often the workload queries them (set by heuristic_stress.sh).
-- ----------------------------------------------------------------
DO $$
DECLARE
    kinds TEXT[] := ARRAY['hot', 'rare'];
    k     TEXT;
    i     INT;
    tbl   TEXT;
BEGIN
    FOREACH k IN ARRAY kinds LOOP
        FOR i IN 1..4 LOOP
            tbl := format('h_%s_%s', k, i);
            EXECUTE format($f$
                CREATE TABLE public.%I (
                    id      BIGSERIAL PRIMARY KEY,
                    grp     INT,
                    val     FLOAT8,
                    payload TEXT
                )$f$, tbl);
            EXECUTE format($f$
                INSERT INTO public.%I (grp, val, payload)
                SELECT g %% 50, random() * 1000, 'row-' || g
                FROM generate_series(1, 300000) g
            $f$, tbl);
        END LOOP;
    END LOOP;
END $$;

-- ----------------------------------------------------------------
-- cheap: small (5k rows) with a PK; the category query is a PK point lookup,
-- so its mean exec time is sub-millisecond -> filtered out despite many calls.
-- ----------------------------------------------------------------
DO $$
DECLARE i INT; tbl TEXT;
BEGIN
    FOR i IN 1..4 LOOP
        tbl := format('h_cheap_%s', i);
        EXECUTE format($f$
            CREATE TABLE public.%I (
                id  BIGINT PRIMARY KEY,
                val FLOAT8
            )$f$, tbl);
        EXECUTE format($f$
            INSERT INTO public.%I (id, val)
            SELECT g, random() * 1000 FROM generate_series(1, 5000) g
        $f$, tbl);
    END LOOP;
END $$;

-- ----------------------------------------------------------------
-- noise: tiny (500 rows); the category query is a trivial COUNT(*) that is both
-- fast and run rarely -> fails both thresholds.
-- ----------------------------------------------------------------
DO $$
DECLARE i INT; tbl TEXT;
BEGIN
    FOR i IN 1..4 LOOP
        tbl := format('h_noise_%s', i);
        EXECUTE format($f$
            CREATE TABLE public.%I (
                id  BIGINT PRIMARY KEY,
                val FLOAT8
            )$f$, tbl);
        EXECUTE format($f$
            INSERT INTO public.%I (id, val)
            SELECT g, random() * 1000 FROM generate_series(1, 500) g
        $f$, tbl);
    END LOOP;
END $$;

-- Fresh planner statistics for all 16 tables.
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables
             WHERE schemaname = 'public'
               AND tablename ~ '^h_(hot|cheap|rare|noise)_[0-9]+$'
    LOOP
        EXECUTE format('ANALYZE public.%I', r.tablename);
    END LOOP;
END $$;

SELECT pg_stat_statements_reset();

\echo ''
\echo 'Heuristic test schema ready (16 tables across 4 profiles):'
SELECT
    CASE
        WHEN tablename LIKE 'h_hot_%'   THEN 'hot   (expect PICK)'
        WHEN tablename LIKE 'h_cheap_%' THEN 'cheap (expect skip: too fast)'
        WHEN tablename LIKE 'h_rare_%'  THEN 'rare  (expect skip: too few calls)'
        ELSE                                 'noise (expect skip: fails both)'
    END                              AS profile,
    count(*)                         AS tables
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename ~ '^h_(hot|cheap|rare|noise)_[0-9]+$'
GROUP BY 1
ORDER BY 1;
