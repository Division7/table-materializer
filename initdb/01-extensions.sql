-- initdb/01-extensions.sql
-- Runs once when the PostgreSQL data directory is first initialised.
-- Creates the extensions that the rest of the stack depends on:
--   • pg_stat_statements  (query-stats view; must be in shared_preload_libraries)
--   • pg_ivm              (incrementally-maintained materialized views)
--
-- table_materializer is created later by pgbench/init.sql because it depends
-- on application-level GUCs set there (heuristic_min_calls, etc.).

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_ivm;
