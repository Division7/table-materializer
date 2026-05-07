\echo Use "CREATE EXTENSION table_materializer" to load this file. \quit

-- Register the main materialization function
CREATE FUNCTION table_materializer_hello()
    RETURNS text
    AS 'table_materializer', 'table_materializer_hello'
    LANGUAGE C STRICT;
