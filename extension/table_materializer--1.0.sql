\echo Use "CREATE EXTENSION table_materializer" to load this file. \quit

-- Load hooks for this session immediately.
LOAD 'table_materializer';

-- Ensure all future connections to this database also load the hooks.
ALTER DATABASE postgres SET session_preload_libraries = 'table_materializer';
