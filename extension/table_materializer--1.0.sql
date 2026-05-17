\echo Use "CREATE EXTENSION table_materializer" to load this file. \quit

CREATE FUNCTION top_expensive_queries()
RETURNS TABLE(rank int, query text, mean_exec_time_ms float8, calls bigint)
AS 'table_materializer', 'top_expensive_queries'
LANGUAGE C STRICT;

CREATE FUNCTION table_materializer_force_spawn()
RETURNS integer
AS 'table_materializer', 'force_spawn_mvs'
LANGUAGE C STRICT;
