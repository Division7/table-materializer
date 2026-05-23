-- pgbench/init.sql
-- Initialises the test schema used by the workload scripts.
-- Safe to re-run: drops and recreates all test tables.
--
-- Run once before starting pgbench:
--   psql -h localhost -U postgres -d postgres -f pgbench/init.sql

-- Extensions -------------------------------------------------------------------
-- pg_stat_statements and pg_ivm must be loaded before table_materializer.
-- In the Docker Compose setup this is handled by initdb/01-extensions.sql.
-- When running against a local PostgreSQL instance, ensure both extensions
-- appear in shared_preload_libraries (pg_stat_statements) / are pre-installed,
-- then CREATE EXTENSION them here first.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_ivm;
CREATE EXTENSION IF NOT EXISTS table_materializer;

-- Lower heuristic thresholds so queries qualify quickly during tests.
-- Production defaults are heuristic_min_calls=10, heuristic_min_exec_time_ms=100.
ALTER SYSTEM SET table_materializer.heuristic_min_calls = 5;
ALTER SYSTEM SET table_materializer.heuristic_min_exec_time_ms = 1.0;
-- Freeze the BGW interval (max 300000 ms = 5 min) so it cannot auto-create
-- IMMVs during Phase 1 (~120 s).  The benchmark drives creation via
-- table_materializer_force_spawn() instead.
ALTER SYSTEM SET table_materializer.interval_ms = 300000;
SELECT pg_reload_conf();

-- Clean slate
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders     CASCADE;
DROP TABLE IF EXISTS products   CASCADE;
DROP TABLE IF EXISTS customers  CASCADE;
DROP TABLE IF EXISTS events     CASCADE;
DROP TABLE IF EXISTS wide_metrics CASCADE;

-- Drop any auto-created IMMVs from prior runs so the test can re-observe creation.
-- pg_ivm creates IMMVs as regular tables (not matviews), so we drop from pg_tables.
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables
             WHERE schemaname = 'public' AND tablename LIKE '%_auto_mv'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename);
    END LOOP;
END $$;

-- Remove stale entries from the pg_ivm catalog that no longer have a backing table.
DELETE FROM pgivm.pg_ivm_immv
WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = immvrelid);

-- ----------------------------------------------------------------
-- customers — 50 000 rows
-- ----------------------------------------------------------------
CREATE TABLE customers (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    email   TEXT,
    region  TEXT,
    tier    TEXT,
    signup  DATE
);

INSERT INTO customers (name, email, region, tier, signup)
SELECT
    'Customer ' || g,
    'user' || g || '@example.com',
    (ARRAY['north','south','east','west'])[1 + (g % 4)],
    (ARRAY['bronze','silver','gold','platinum'])[1 + (g % 4)],
    '2020-01-01'::date + ((g % 1000) || ' days')::interval
FROM generate_series(1, 50000) g;

-- ----------------------------------------------------------------
-- products — 10 000 rows
-- ----------------------------------------------------------------
CREATE TABLE products (
    id       BIGSERIAL PRIMARY KEY,
    sku      TEXT NOT NULL,
    name     TEXT,
    category TEXT,
    price    NUMERIC(10,2),
    cost     NUMERIC(10,2)
);

INSERT INTO products (sku, name, category, price, cost)
SELECT
    'SKU-' || lpad(g::text, 6, '0'),
    'Product ' || g,
    (ARRAY['electronics','clothing','food','books','sports'])[1 + (g % 5)],
    (10 + (g % 990))::numeric,
    (5  + (g % 495))::numeric
FROM generate_series(1, 10000) g;

-- ----------------------------------------------------------------
-- orders — 200 000 rows; no index on customer_id (deliberate — forces seq scan)
-- ----------------------------------------------------------------
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    status      TEXT,
    total       NUMERIC(12,2)
);

INSERT INTO orders (customer_id, created_at, status, total)
SELECT
    1 + (g % 50000),
    NOW() - (((g % 730) || ' days')::interval),
    (ARRAY['pending','shipped','delivered','cancelled'])[1 + (g % 4)],
    (10 + (g % 10000))::numeric
FROM generate_series(1, 200000) g;

-- ----------------------------------------------------------------
-- order_items — 500 000 rows; no index on order_id (deliberate)
-- ----------------------------------------------------------------
CREATE TABLE order_items (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    qty        INT,
    unit_price NUMERIC(10,2)
);

INSERT INTO order_items (order_id, product_id, qty, unit_price)
SELECT
    1 + (g % 200000),
    1 + (g % 10000),
    1 + (g % 10),
    (10 + (g % 990))::numeric
FROM generate_series(1, 500000) g;

-- ----------------------------------------------------------------
-- wide_metrics — 100 000 rows × 40 columns; wide rows stress sequential scans
-- ----------------------------------------------------------------
CREATE TABLE wide_metrics (
    id         BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ DEFAULT NOW(),
    user_id    INT,
    session_id TEXT,
    m01 FLOAT8, m02 FLOAT8, m03 FLOAT8, m04 FLOAT8, m05 FLOAT8,
    m06 FLOAT8, m07 FLOAT8, m08 FLOAT8, m09 FLOAT8, m10 FLOAT8,
    m11 FLOAT8, m12 FLOAT8, m13 FLOAT8, m14 FLOAT8, m15 FLOAT8,
    m16 FLOAT8, m17 FLOAT8, m18 FLOAT8, m19 FLOAT8, m20 FLOAT8,
    t01 TEXT, t02 TEXT, t03 TEXT, t04 TEXT, t05 TEXT,
    f01 BOOL, f02 BOOL, f03 BOOL, f04 BOOL, f05 BOOL,
    cat_a TEXT, cat_b TEXT, cat_c TEXT, cat_d TEXT,
    score_a NUMERIC(8,4), score_b NUMERIC(8,4), score_c NUMERIC(8,4)
);

INSERT INTO wide_metrics (
    user_id, session_id,
    m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,
    m11,m12,m13,m14,m15,m16,m17,m18,m19,m20,
    t01,t02,t03,t04,t05,
    f01,f02,f03,f04,f05,
    cat_a,cat_b,cat_c,cat_d,
    score_a,score_b,score_c
)
SELECT
    1 + (g % 50000),
    'sess-' || (g % 10000),
    random(),random(),random(),random(),random(),
    random(),random(),random(),random(),random(),
    random(),random(),random(),random(),random(),
    random(),random(),random(),random(),random(),
    'tag-'||(g%100), 'tag-'||(g%200), 'tag-'||(g%300),
    'tag-'||(g%400), 'tag-'||(g%500),
    (g%2=0),(g%3=0),(g%5=0),(g%7=0),(g%11=0),
    (ARRAY['A','B','C','D'])[1+(g%4)],
    (ARRAY['X','Y','Z'])[1+(g%3)],
    (ARRAY['alpha','beta','gamma'])[1+(g%3)],
    (ARRAY['low','mid','high'])[1+(g%3)],
    (random()*100)::numeric(8,4),
    (random()*100)::numeric(8,4),
    (random()*100)::numeric(8,4)
FROM generate_series(1, 100000) g;

-- ----------------------------------------------------------------
-- events — 500 000 rows; high-cardinality stream for aggregation tests
-- ----------------------------------------------------------------
CREATE TABLE events (
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT,
    user_id    INT,
    object_id  INT,
    value      FLOAT8,
    created_at TIMESTAMPTZ
);

INSERT INTO events (event_type, user_id, object_id, value, created_at)
SELECT
    (ARRAY['click','view','purchase','signup','logout'])[1+(g%5)],
    1 + (g % 50000),
    1 + (g % 200000),
    random() * 1000,
    NOW() - (((g % 365) || ' days')::interval)
FROM generate_series(1, 500000) g;

-- Collect fresh statistics so the planner has accurate row-count and
-- distribution estimates before Phase 1 starts.  Without this the planner
-- falls back to default (zero-knowledge) estimates for all six tables.
ANALYZE customers, products, orders, order_items, wide_metrics, events;

-- Reset stats so workload scripts start from a clean slate.
SELECT pg_stat_statements_reset();

\echo ''
\echo 'Schema ready. Row counts:'
SELECT 'customers'   AS tbl, COUNT(*) FROM customers
UNION ALL SELECT 'products',   COUNT(*) FROM products
UNION ALL SELECT 'orders',     COUNT(*) FROM orders
UNION ALL SELECT 'order_items',COUNT(*) FROM order_items
UNION ALL SELECT 'wide_metrics',COUNT(*) FROM wide_metrics
UNION ALL SELECT 'events',     COUNT(*) FROM events;
